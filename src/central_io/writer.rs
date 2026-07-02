use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    io::{self, IoSlice},
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::time::Instant;

use primitive::arena::obj_pool::ArcObjPool;

use crate::{
    common::Side,
    control::DeadControl,
    fair_queue,
    protocol::{BodyLen, DataHeader, Header, StreamId, StreamIdMsg},
};

use super::{DataBuf, DeadCentralIo};

const CONTROL_CHANNEL_SIZE: usize = 1024;
const SPLIT_POOL_SHARDS: NonZeroUsize = NonZeroUsize::new(1).unwrap();

const DATA_EXTREME_CAP: usize = 1200;
const DATA_MEDIUM_CAP: usize = 2 * 1024;
const DATA_BULK_CAP: usize = 32 * 1024;

/// A stream is relegated from `MaybeLatencySensitive` (the default, protected)
/// to `MustBulk` once at least two thirds of its recent sends were under
/// [`DATA_QUANTUM`] and it has been idle for this long.
const LATENCY_IDLE: Duration = Duration::from_secs(30);

/// Minimum number of sends before the 2/3-under-QUANTUM ratio is evaluated.
const LATENCY_HISTORY_MIN: usize = 3;

/// Per-stream traffic class, keyed by fair-queue token.
///
/// - `MaybeLatencySensitive` (default): the stream is protected. While any
///   open stream is in this class, every Data dispatch is capped at
///   [`DATA_QUANTUM`] so a later-arriving small stream can preempt the tail.
/// - `MustBulk`: the stream has been relegated. Only when every open stream is
///   `MustBulk` does a dispatch use [`DATA_BULK_CAP`] for throughput.
///
/// The class is computed on demand from per-stream send observations (recent
/// send sizes and last-sent time); it is not stored as a field. Global
/// sensitivity is cheap to query in the common case: `bulk_count` and
/// `next_bulk_transition` are maintained incrementally, and
/// `any_latency_sensitive` is O(1) except when a time-driven transition is due
/// (then it recomputes aggregates, O(open_count), and resets the timer).
#[derive(Debug)]
struct LatencyControl {
    /// Send history per open stream token. `Small`/`Bulk` are tallied
    /// incrementally so the 2/3-under-QUANTUM ratio is a cheap division, and
    /// `last_sent` lets `MustBulk` revert to latency-sensitive if a stream
    /// resumes after the idle window.
    streams: HashMap<fair_queue::Token, StreamObs>,
    /// Number of currently-open streams (`Open` seen, no `Close`/`Fin` yet).
    open_count: usize,
    /// Number of open streams currently classified `MustBulk`.
    bulk_count: usize,
    /// Earliest time at which some non-bulk stream may transition to `MustBulk`
    /// (its `last_sent + LATENCY_IDLE`). `None` when no non-bulk stream could
    /// ever transition (e.g. no history yet). `any_latency_sensitive` is O(1)
    /// while `now < next_bulk_transition`; once `now` crosses it, aggregates
    /// are recomputed and this is reset.
    next_bulk_transition: Option<Instant>,
}

#[derive(Debug)]
struct StreamObs {
    small_count: usize,
    bulk_count: usize,
}
impl StreamObs {
    fn is_bulk(&self) -> bool {
        let total = self.small_count + self.bulk_count;
        if total < LATENCY_HISTORY_MIN {
            return false;
        }
        self.small_count * 3 < total * 2
    }
}

impl LatencyControl {
    fn new() -> Self {
        Self {
            streams: HashMap::new(),
            open_count: 0,
            bulk_count: 0,
            next_bulk_transition: None,
        }
    }
    fn open(&mut self, token: fair_queue::Token, now: Instant) {
        let obs = StreamObs {
            small_count: 0,
            bulk_count: 0,
        };
        self.open_count += 1;
        if obs.is_bulk() {
            self.bulk_count += 1;
        } else {
            self.next_bulk_transition = Some(now + LATENCY_IDLE);
        }
        self.streams.insert(token, obs);
    }
    fn close(&mut self, token: fair_queue::Token) {
        let Some(obs) = self.streams.remove(&token) else {
            return;
        };
        if obs.is_bulk() {
            self.bulk_count = self.bulk_count.strict_sub(1);
        }
        self.open_count = self.open_count.strict_sub(1);
    }
    fn record_send(&mut self, token: fair_queue::Token, size: usize, now: Instant) {
        let Some(obs) = self.streams.get_mut(&token) else {
            return;
        };
        let was_bulk = obs.is_bulk();
        if size <= DATA_MEDIUM_CAP {
            obs.small_count += 1;
        } else {
            obs.bulk_count += 1;
        }
        let is_bulk = obs.is_bulk();
        if was_bulk != is_bulk {
            if is_bulk {
                self.bulk_count += 1;
            } else {
                self.bulk_count = self.bulk_count.strict_sub(1);
            }
        }
        if !is_bulk {
            self.next_bulk_transition = Some(now + LATENCY_IDLE);
        }
    }
    pub fn any_latency_sensitive(&self) -> bool {
        if self.open_count == self.bulk_count {
            return false;
        }
        let t = self.next_bulk_transition.unwrap();
        let now = Instant::now();
        now < t
    }
}

pub async fn run_central_io_writer<W>(
    mut io_writer: CentralIoWriter<W>,
    heartbeat_interval: Duration,
    mut control: WriteControlRx,
    mut data: WriteDataRx,
) -> Result<(), RunCentralIoWriterError>
where
    W: AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            biased;
            res = control.recv() => {
                let msg = res.map_err(RunCentralIoWriterError::Control)?;
                io_writer.send_control(msg).await.map_err(RunCentralIoWriterError::IoWriter)?;
            }
            res = data.recv() => {
                let msg = res.map_err(RunCentralIoWriterError::Control)?;
                io_writer.send_data(msg).await.map_err(RunCentralIoWriterError::IoWriter)?;
            }
            () = tokio::time::sleep(heartbeat_interval) => {
                io_writer.send_heartbeat().await.map_err(RunCentralIoWriterError::IoWriter)?;
            }
        }
    }
}
#[derive(Debug)]
pub enum RunCentralIoWriterError {
    IoWriter(io::Error),
    Control(DeadControl),
}

#[derive(Debug)]
pub struct CentralIoWriter<W> {
    io_writer: W,
    /// Reused staging buffer for coalescing a non-vectored Data frame's fixed
    /// header and body into a single transport write. Grows to at most
    /// `Header::SIZE + DataHeader::SIZE + usize::from(BodyLen::MAX)`.
    frame_buf: Vec<u8>,
}
impl<W> CentralIoWriter<W> {
    pub fn new(io_writer: W) -> Self {
        Self {
            io_writer,
            frame_buf: Vec::new(),
        }
    }
}
impl<W> CentralIoWriter<W>
where
    W: AsyncWrite + Unpin,
{
    pub async fn send_heartbeat(&mut self) -> io::Result<()> {
        let hdr = Header::Heartbeat;
        let hdr = hdr.encode();
        self.io_writer.write_all(&hdr).await?;
        Ok(())
    }
    pub async fn send_control(&mut self, msg: WriteControlMsg) -> io::Result<()> {
        let hdr = match &msg {
            WriteControlMsg::Open(_) => Header::Open,
            WriteControlMsg::Close(_, side) => match side {
                Side::Read => Header::CloseRead,
                Side::Write => Header::CloseWrite,
            },
        };
        let stream_id = match msg {
            WriteControlMsg::Open(stream_id) | WriteControlMsg::Close(stream_id, _) => stream_id,
        };
        self.send_control_(hdr, stream_id).await
    }
    async fn send_control_(&mut self, hdr: Header, stream_id: u32) -> io::Result<()> {
        let stream_id_msg = StreamIdMsg { stream_id };
        let hdr = hdr.encode();
        let stream_id_msg = stream_id_msg.encode();
        let mut concat = hdr.into_iter().chain(stream_id_msg);
        let buf: [u8; Header::SIZE + StreamIdMsg::SIZE] =
            core::array::from_fn(|_| concat.next().unwrap());
        self.io_writer.write_all(&buf).await?;
        Ok(())
    }
    pub async fn send_data(&mut self, msg: WriteDataMsg) -> io::Result<()> {
        let data_buf = match msg.data {
            StreamWriteData::Open => return Ok(()),
            StreamWriteData::Fin => {
                let hdr = Header::CloseWrite;
                return self.send_control_(hdr, msg.stream_id).await;
            }
            StreamWriteData::Data(data_buf) => data_buf,
        };
        let hdr = Header::Data;
        let mut body_offset = 0usize;
        while body_offset != data_buf.len() {
            let body_len = (data_buf.len() - body_offset).min(usize::from(BodyLen::MAX));
            let body_len_u16 = BodyLen::try_from(body_len).unwrap();
            let data_hdr = DataHeader {
                stream_id: msg.stream_id,
                body_len: body_len_u16,
            };
            let hdr = hdr.encode();
            let data_hdr = data_hdr.encode();
            let mut concat = hdr.into_iter().chain(data_hdr);
            let fixed_buf: [u8; Header::SIZE + DataHeader::SIZE] =
                core::array::from_fn(|_| concat.next().unwrap());
            let body = &data_buf[body_offset..body_offset + body_len];
            body_offset += body_len;
            self.write_all_frame(&fixed_buf, body).await?;
        }
        Ok(())
    }
    async fn write_all_frame(&mut self, fixed_header: &[u8], body: &[u8]) -> io::Result<()> {
        if self.io_writer.is_write_vectored() && !fixed_header.is_empty() && !body.is_empty() {
            let mut header_remaining = fixed_header;
            let mut body_remaining = body;
            loop {
                let n = if header_remaining.is_empty() {
                    self.io_writer.write(body_remaining).await?
                } else {
                    let bufs = &mut [IoSlice::new(header_remaining), IoSlice::new(body_remaining)];
                    self.io_writer.write_vectored(bufs).await?
                };
                if n == 0 {
                    return Err(io::ErrorKind::WriteZero.into());
                }
                if n < header_remaining.len() {
                    header_remaining = &header_remaining[n..];
                } else {
                    let consumed_body = n - header_remaining.len();
                    body_remaining = &body_remaining[consumed_body..];
                    header_remaining = &[];
                    if body_remaining.is_empty() {
                        return Ok(());
                    }
                }
            }
        } else {
            if fixed_header.is_empty() || body.is_empty() {
                // Keep the existing single-write behaviour when either part is
                // empty; only coalesce when both are non-empty.
                if !fixed_header.is_empty() {
                    self.io_writer.write_all(fixed_header).await?;
                }
                if !body.is_empty() {
                    self.io_writer.write_all(body).await?;
                }
            } else {
                self.frame_buf.clear();
                self.frame_buf.extend_from_slice(fixed_header);
                self.frame_buf.extend_from_slice(body);
                self.io_writer.write_all(&self.frame_buf).await?;
            }
            Ok(())
        }
    }
}

#[derive(Debug)]
pub struct WriteDataMsg {
    pub stream_id: StreamId,
    pub data: StreamWriteData,
}
#[derive(Debug)]
pub enum StreamWriteData {
    Open,
    Fin,
    Data(DataBuf),
}
pub fn write_data_channel() -> (WriteDataTxPrototype, WriteDataRx) {
    let (tx, rx) = fair_queue::channel();
    let tx = WriteDataTxPrototype { opener: tx };
    let rx = WriteDataRx {
        rx,
        token_to_stream: HashMap::new(),
        heads: BTreeMap::new(),
        head_pick_start: fair_queue::Token(0),
        rx_closed: false,
        split_pool: ArcObjPool::new(None, SPLIT_POOL_SHARDS, Vec::new, |v| v.clear()),
        latency: LatencyControl::new(),
    };
    (tx, rx)
}
#[derive(Debug)]
pub struct WriteDataRx {
    rx: fair_queue::Receiver<WriteDataMsg>,
    token_to_stream: HashMap<fair_queue::Token, StreamId>,
    /// At most one cached message per stream/token. The token maps to a
    /// `HeadEntry` (the logical stream message plus a read offset into its
    /// Data payload) ready to be dispatched.
    heads: BTreeMap<fair_queue::Token, HeadEntry>,
    /// Round-robin cursor for selecting among cached heads.
    head_pick_start: fair_queue::Token,
    /// Set once the underlying fair-queue receiver reports closure.
    rx_closed: bool,
    /// Pool for prefix buffers produced by splitting a large Data head.
    split_pool: ArcObjPool<Vec<u8>>,
    /// Per-stream traffic-class observations, keyed by fair-queue token.
    latency: LatencyControl,
}
#[derive(Debug)]
struct HeadEntry {
    msg: WriteDataMsg,
    /// Bytes already dispatched from this head's Data payload; the next
    /// dispatch resumes at `data[offset..]`. Kept here so the original buffer
    /// is reused in place instead of copying the tail on every split.
    offset: usize,
}
impl WriteDataRx {
    pub async fn recv(&mut self) -> Result<WriteDataMsg, DeadControl> {
        struct WriteDataRecv<'a>(&'a mut WriteDataRx);
        impl Future for WriteDataRecv<'_> {
            type Output = Result<WriteDataMsg, DeadControl>;
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                self.0.poll_recv(cx)
            }
        }
        WriteDataRecv(self).await
    }
    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<WriteDataMsg, DeadControl>> {
        // Drain ready streams into `heads`, caching at most one message per
        // token. `poll_recv_excluding` skips tokens that already have a cached
        // head, so we never read a second message from a token that still has
        // a pending head. Loop until it returns Pending (all ready drained) or
        // closure, so selection sees every currently-ready stream.
        let now = Instant::now();
        while !self.rx_closed {
            let heads = &mut self.heads;
            let res = self
                .rx
                .poll_recv_excluding(cx, |token| heads.contains_key(&token));
            match res {
                Poll::Ready(Some((token, msg))) => {
                    let msg = match msg {
                        fair_queue::ReceiverRecv::Open(value) => {
                            self.token_to_stream.insert(token, value.stream_id);
                            self.latency.open(token, now);
                            value
                        }
                        fair_queue::ReceiverRecv::Value(value) => value,
                        fair_queue::ReceiverRecv::Close => {
                            let Some(stream_id) = self.token_to_stream.remove(&token) else {
                                continue;
                            };
                            self.latency.close(token);
                            WriteDataMsg {
                                stream_id,
                                data: StreamWriteData::Fin,
                            }
                        }
                    };
                    self.heads.insert(token, HeadEntry { msg, offset: 0 });
                    continue;
                }
                Poll::Ready(None) => {
                    self.rx_closed = true;
                    break;
                }
                Poll::Pending => {
                    break;
                }
            }
        }
        // Select the best cached head: smallest (priority_size), then
        // round-robin distance from `head_pick_start`.
        if self.heads.is_empty() {
            if self.rx_closed {
                return Err(DeadControl {}).into();
            }
            return Poll::Pending;
        }
        let chosen = self.pick_head();
        let (_, mut entry) = self.heads.remove_entry(&chosen).unwrap();
        self.head_pick_start = fair_queue::Token(chosen.0.wrapping_add(1));
        if let StreamWriteData::Data(ref data) = entry.msg.data {
            let cap = if self.latency.any_latency_sensitive() {
                if self.heads.is_empty() {
                    DATA_MEDIUM_CAP
                } else {
                    DATA_EXTREME_CAP
                }
            } else {
                DATA_BULK_CAP
            };
            if entry.offset == 0 {
                // First dispatch of this head: record one send using the
                // original data length (not the capped emit size)
                self.latency.record_send(chosen, data.len(), now);
            }
            let remaining = data.len() - entry.offset;
            let emit = remaining.min(cap);
            if emit < remaining {
                // Emit a fresh buffer holding the prefix; the original `data`
                // stays with the reinserted tail, its offset advanced past the
                // emitted prefix.
                let split_at = entry.offset + emit;
                let mut prefix = self.split_pool.take_scoped();
                prefix.clear();
                prefix.extend_from_slice(&data[entry.offset..split_at]);
                let stream_id = entry.msg.stream_id;
                entry.offset = split_at;
                self.heads.insert(chosen, entry);
                return Ok(WriteDataMsg {
                    stream_id,
                    data: StreamWriteData::Data(prefix),
                })
                .into();
            }
            // Emit the final slice of the head. If the offset already consumed
            // some prefix, copy the remaining tail into a fresh buffer so the
            // emitted message owns exactly the remaining bytes.
            if entry.offset != 0 {
                let mut tail = self.split_pool.take_scoped();
                tail.clear();
                tail.extend_from_slice(&data[entry.offset..]);
                entry.msg.data = StreamWriteData::Data(tail);
            }
        }
        Ok(entry.msg).into()
    }
    /// Pick the next token to dispatch from `heads`.
    ///
    /// Selection key: `(priority_size, round_robin_distance_from_head_pick_start)`.
    /// `priority_size`: Open = 0, Fin = 0, Data = remaining data length.
    /// Equal-size tie-breaking is round-robin by distance from
    /// `head_pick_start` (with wraparound), not lowest-token-first.
    fn pick_head(&self) -> fair_queue::Token {
        let start = self.head_pick_start;
        let mut best: Option<(fair_queue::Token, (usize, usize))> = None;
        for (&token, entry) in &self.heads {
            let priority = priority_size(entry);
            let distance = round_robin_distance(start, token);
            let key = (priority, distance);
            match best {
                Some((_, best_key)) if best_key <= key => {}
                _ => best = Some((token, key)),
            }
        }
        best.unwrap().0
    }
}

fn priority_size(entry: &HeadEntry) -> usize {
    match entry.msg.data {
        StreamWriteData::Open => 0,
        StreamWriteData::Fin => 0,
        StreamWriteData::Data(ref data) => data.len() - entry.offset,
    }
}

/// Forward cyclic distance from `start` to `token` in a `usize` wraparound
/// space. Used only as a tie-breaker, so the exact modulus doesn't matter as
/// long as it is consistent and monotonic in round-robin order.
fn round_robin_distance(start: fair_queue::Token, token: fair_queue::Token) -> usize {
    let start = start.0;
    let token = token.0;
    token.wrapping_sub(start)
}
#[derive(Debug, Clone)]
pub struct WriteDataTxPrototype {
    opener: fair_queue::Opener<WriteDataMsg>,
}
impl WriteDataTxPrototype {
    pub async fn derive(&self, stream: StreamId) -> Result<StreamWriteDataTx, DeadCentralIo> {
        Ok(StreamWriteDataTx {
            tx: self
                .opener
                .open(WriteDataMsg {
                    stream_id: stream,
                    data: StreamWriteData::Open,
                })
                .await
                .ok_or(DeadCentralIo { side: Side::Write })?,
            stream_id: stream,
        })
    }
}
#[derive(Debug)]
pub struct StreamWriteDataTx {
    stream_id: StreamId,
    tx: fair_queue::Sender<WriteDataMsg>,
}
// impl StreamWriteDataTx {
//     pub async fn send(&self, data: StreamWriteData) -> Result<(), DeadCentralIo> {
//         let msg = WriteDataMsg {
//             stream_id: self.stream_id,
//             data,
//         };
//         self.tx
//             .send(msg)
//             .await
//             .map_err(|_| DeadCentralIo { side: Side::Write })
//     }
// }
#[derive(Debug)]
pub struct PollStreamWriteDataTx {
    stream_id: StreamId,
    tx: fair_queue::PollSender<WriteDataMsg>,
}
impl From<StreamWriteDataTx> for PollStreamWriteDataTx {
    fn from(value: StreamWriteDataTx) -> Self {
        Self {
            stream_id: value.stream_id,
            tx: value.tx.into(),
        }
    }
}
impl PollStreamWriteDataTx {
    pub fn poll_preserve(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), DeadCentralIo>> {
        self.tx
            .poll_reserve(cx)
            .map_err(|_| DeadCentralIo { side: Side::Write })
    }
    pub fn send_item(&mut self, data: StreamWriteData) -> Result<(), DeadCentralIo> {
        let msg = WriteDataMsg {
            stream_id: self.stream_id,
            data,
        };
        self.tx
            .send_item(msg)
            .map_err(|_| DeadCentralIo { side: Side::Write })
    }
}

#[derive(Debug, Clone)]
pub enum WriteControlMsg {
    Open(StreamId),
    Close(StreamId, Side),
}
pub fn write_control_channel() -> (WriteControlTx, WriteControlRx) {
    let (tx, rx) = tokio::sync::mpsc::channel(CONTROL_CHANNEL_SIZE);
    let tx = WriteControlTx { tx };
    let rx = WriteControlRx { rx };
    (tx, rx)
}
#[derive(Debug, Clone)]
pub struct WriteControlTx {
    tx: tokio::sync::mpsc::Sender<WriteControlMsg>,
}
impl WriteControlTx {
    pub async fn send(&self, msg: WriteControlMsg) -> Result<(), DeadCentralIo> {
        self.tx
            .send(msg)
            .await
            .map_err(|_| DeadCentralIo { side: Side::Write })
    }
    pub async fn closed(&self) {
        self.tx.closed().await
    }
}
#[derive(Debug)]
pub struct WriteControlRx {
    rx: tokio::sync::mpsc::Receiver<WriteControlMsg>,
}
impl WriteControlRx {
    pub async fn recv(&mut self) -> Result<WriteControlMsg, DeadControl> {
        self.rx.recv().await.ok_or(DeadControl {})
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, IoSlice},
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
        time::Duration,
    };

    use primitive::arena::obj_pool::arc_buf_pool;
    use tokio::io::AsyncWrite;

    use super::{
        priority_size, round_robin_distance, write_data_channel, CentralIoWriter, HeadEntry,
        StreamWriteData, StreamWriteDataTx, WriteDataMsg, WriteDataRx, WriteDataTxPrototype,
        DATA_BULK_CAP, DATA_EXTREME_CAP, LATENCY_IDLE,
    };
    use crate::protocol::{BodyLen, DataHeader, Header, StreamId};
    use crate::{central_io::writer::DATA_MEDIUM_CAP, fair_queue};

    /// A mock writer that records every byte and can simulate partial vectored
    /// writes. `max_per_write` caps the number of bytes any single `write` /
    /// `write_vectored` call may consume, exercising the partial-write loop.
    #[derive(Default)]
    struct MockWriter {
        out: Vec<u8>,
        max_per_write: Option<usize>,
        vectored: bool,
        write_vectored_calls: Arc<Mutex<usize>>,
    }
    impl AsyncWrite for MockWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let cap = self.max_per_write.unwrap_or(buf.len()).min(buf.len());
            let cap = cap.max(1);
            let n = cap.min(buf.len());
            let this = unsafe { self.get_unchecked_mut() };
            this.out.extend_from_slice(&buf[..n]);
            Poll::Ready(Ok(n))
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn is_write_vectored(&self) -> bool {
            self.vectored
        }
        fn poll_write_vectored(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let this = unsafe { self.get_unchecked_mut() };
            *this.write_vectored_calls.lock().unwrap() += 1;
            // Concatenate all bufs up to max_per_write bytes.
            let cap = this.max_per_write.unwrap_or(usize::MAX);
            let mut written = 0usize;
            for b in bufs {
                if written >= cap {
                    break;
                }
                let remaining = cap - written;
                let take = b.len().min(remaining);
                this.out.extend_from_slice(&b[..take]);
                written += take;
                if take < b.len() {
                    return Poll::Ready(Ok(written));
                }
            }
            Poll::Ready(Ok(written))
        }
    }

    fn make_data_buf(bytes: &[u8]) -> crate::central_io::DataBuf {
        let pool = arc_buf_pool::<u8>(None, std::num::NonZeroUsize::new(1).unwrap());
        let mut scoped = pool.take_scoped();
        scoped.clear();
        scoped.extend_from_slice(bytes);
        scoped
    }

    fn expected_frame(stream_id: u32, body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut offset = 0usize;
        while offset != body.len() {
            let len = (body.len() - offset).min(usize::from(BodyLen::MAX));
            let data_hdr = DataHeader {
                stream_id,
                body_len: BodyLen::try_from(len).unwrap(),
            };
            out.extend_from_slice(&Header::Data.encode());
            out.extend_from_slice(&data_hdr.encode());
            out.extend_from_slice(&body[offset..offset + len]);
            offset += len;
        }
        out
    }

    #[tokio::test]
    async fn vectored_partial_writes_output_exact_bytes() {
        let calls = Arc::new(Mutex::new(0usize));
        let writer = MockWriter {
            out: Vec::new(),
            max_per_write: Some(3),
            vectored: true,
            write_vectored_calls: Arc::clone(&calls),
        };
        let mut central = CentralIoWriter::new(writer);
        let body = (0u8..200u8).collect::<Vec<u8>>();
        central
            .send_data(WriteDataMsg {
                stream_id: 1,
                data: StreamWriteData::Data(make_data_buf(&body)),
            })
            .await
            .unwrap();
        let got = central.io_writer.out.clone();
        assert_eq!(got, expected_frame(1, &body));
        assert!(*calls.lock().unwrap() > 0, "vectored path was taken");
    }

    #[tokio::test]
    async fn non_vectored_writer_uses_fallback_and_outputs_exact_bytes() {
        let writer = MockWriter {
            out: Vec::new(),
            max_per_write: Some(5),
            vectored: false,
            write_vectored_calls: Arc::new(Mutex::new(0)),
        };
        let mut central = CentralIoWriter::new(writer);
        let body = (0u8..200u8).collect::<Vec<u8>>();
        central
            .send_data(WriteDataMsg {
                stream_id: 7,
                data: StreamWriteData::Data(make_data_buf(&body)),
            })
            .await
            .unwrap();
        assert_eq!(
            central
                .io_writer
                .write_vectored_calls
                .lock()
                .unwrap()
                .clone(),
            0,
            "fallback path should not invoke write_vectored"
        );
        let got = central.io_writer.out.clone();
        assert_eq!(got, expected_frame(7, &body));
    }

    /// On a non-vectored transport a Data frame must be emitted as exactly one
    /// write call whose bytes are the fixed header immediately followed by the
    /// body. The frame_buf is reused across frames, so after two frames it holds
    /// the second frame's contents and the byte output reassembles correctly.
    #[tokio::test]
    async fn non_vectored_data_frame_coalesces_into_single_write() {
        struct CountingWriter {
            out: Vec<u8>,
            write_calls: Arc<Mutex<usize>>,
        }
        impl AsyncWrite for CountingWriter {
            fn poll_write(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                let this = unsafe { self.get_unchecked_mut() };
                *this.write_calls.lock().unwrap() += 1;
                this.out.extend_from_slice(buf);
                Poll::Ready(Ok(buf.len()))
            }
            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn is_write_vectored(&self) -> bool {
                false
            }
        }

        let calls = Arc::new(Mutex::new(0usize));
        let writer = CountingWriter {
            out: Vec::new(),
            write_calls: Arc::clone(&calls),
        };
        let mut central = CentralIoWriter::new(writer);

        let body: Vec<u8> = (0u8..=255).cycle().take(1234).collect();
        central
            .send_data(WriteDataMsg {
                stream_id: 7,
                data: StreamWriteData::Data(make_data_buf(&body)),
            })
            .await
            .unwrap();

        let got = central.io_writer.out.clone();
        assert_eq!(got, expected_frame(7, &body));
        assert_eq!(
            *calls.lock().unwrap(),
            1,
            "non-vectored Data frame should reach transport as exactly one write"
        );
        assert_eq!(
            central.frame_buf,
            expected_frame(7, &body),
            "staging buffer should retain the last coalesced frame"
        );
    }

    #[tokio::test]
    async fn body_larger_than_max_creates_multiple_frames_no_first_chunk_repeat() {
        let writer = MockWriter {
            out: Vec::new(),
            max_per_write: None,
            vectored: true,
            write_vectored_calls: Arc::new(Mutex::new(0)),
        };
        let mut central = CentralIoWriter::new(writer);
        let big_len = usize::from(BodyLen::MAX) * 2 + 10;
        let body = (0u8..big_len as u8)
            .cycle()
            .take(big_len)
            .collect::<Vec<u8>>();
        central
            .send_data(WriteDataMsg {
                stream_id: 42,
                data: StreamWriteData::Data(make_data_buf(&body)),
            })
            .await
            .unwrap();
        let got = central.io_writer.out.clone();
        // Byte-exact comparison already verifies chunk boundaries and that the
        // first chunk isn't repeated.
        assert_eq!(got, expected_frame(42, &body));
        // Walk the emitted stream, decoding each Data frame, to prove there were
        // multiple frames with distinct body slices that reassemble to `body`.
        let mut frames = 0usize;
        let mut reassembled = Vec::new();
        let mut pos = 0usize;
        let data_code = Header::Data.encode()[0];
        while pos < got.len() {
            assert_eq!(got[pos], data_code, "expected Data header");
            let dh_off = pos + Header::SIZE;
            let dh = DataHeader::decode(got[dh_off..dh_off + DataHeader::SIZE].try_into().unwrap());
            let blen = usize::from(dh.body_len);
            assert_eq!(dh.stream_id, 42);
            let body_off = dh_off + DataHeader::SIZE;
            reassembled.extend_from_slice(&got[body_off..body_off + blen]);
            pos = body_off + blen;
            frames += 1;
        }
        assert_eq!(reassembled, body);
        let expected_count = big_len.div_ceil(usize::from(BodyLen::MAX));
        assert_eq!(frames, expected_count);
    }

    // ---- Fairness tests ----

    fn make_data(bytes: &[u8]) -> crate::central_io::DataBuf {
        let pool = arc_buf_pool::<u8>(None, std::num::NonZeroUsize::new(1).unwrap());
        let mut scoped = pool.take_scoped();
        scoped.clear();
        scoped.extend_from_slice(bytes);
        scoped
    }

    /// Open stream `stream_id`, returning the `StreamWriteDataTx` once the
    /// receiver has consumed the Open message. `derive` awaits the opener
    /// response which is only produced when the receiver is polled, so the
    /// two are driven concurrently.
    async fn open_stream(
        tx: &WriteDataTxPrototype,
        rx: &mut WriteDataRx,
        stream_id: StreamId,
    ) -> StreamWriteDataTx {
        let mut stream = None;
        let mut got_open = false;
        tokio::join!(
            async {
                stream = Some(tx.derive(stream_id).await.unwrap());
            },
            async {
                while !got_open {
                    let msg = rx.recv().await.unwrap();
                    assert_eq!(msg.stream_id, stream_id);
                    assert!(matches!(msg.data, StreamWriteData::Open));
                    got_open = true;
                }
            },
        );
        stream.unwrap()
    }

    /// Send a data message on a cloned sender, for use with `tokio::spawn`.
    async fn send_data_owned(
        tx: fair_queue::Sender<WriteDataMsg>,
        stream_id: StreamId,
        bytes: Vec<u8>,
    ) {
        let msg = WriteDataMsg {
            stream_id,
            data: StreamWriteData::Data(make_data(&bytes)),
        };
        tx.send(msg).await.unwrap();
    }

    fn data_len(data: &StreamWriteData) -> usize {
        match data {
            StreamWriteData::Open | StreamWriteData::Fin => 0,
            StreamWriteData::Data(d) => d.len(),
        }
    }

    /// Smaller ready data from stream B is emitted before larger ready data
    /// from stream A.
    #[tokio::test]
    async fn smaller_ready_data_emitted_before_larger() {
        let (tx, mut rx) = write_data_channel();
        let stream_a = open_stream(&tx, &mut rx, 1).await;
        let stream_b = open_stream(&tx, &mut rx, 2).await;

        // Each send completes immediately (queue size 1, fresh stream) and
        // leaves the message queued + the ready tree populated before we recv.
        send_data_owned(stream_a.tx.clone(), stream_a.stream_id, vec![0u8; 100]).await;
        send_data_owned(stream_b.tx.clone(), stream_b.stream_id, vec![1u8; 10]).await;

        let first = rx.recv().await.unwrap();
        assert_eq!(first.stream_id, 2, "smaller (B) should come first");
        assert_eq!(data_len(&first.data), 10);
        let second = rx.recv().await.unwrap();
        assert_eq!(second.stream_id, 1, "larger (A) should come second");
        assert_eq!(data_len(&second.data), 100);
    }

    /// Same-stream order is preserved when stream A has large then small and
    /// stream B has small ready.
    #[tokio::test]
    async fn same_stream_order_preserved_with_interleaving() {
        let (tx, mut rx) = write_data_channel();
        let stream_a = open_stream(&tx, &mut rx, 1).await;
        let stream_b = open_stream(&tx, &mut rx, 2).await;

        // A's large message: send completes immediately (fresh stream, queue
        // size 1) then we drain it.
        send_data_owned(stream_a.tx.clone(), stream_a.stream_id, vec![0u8; 100]).await;
        let a_large = rx.recv().await.unwrap();
        assert_eq!(a_large.stream_id, 1);
        assert_eq!(data_len(&a_large.data), 100);

        // Now A small and B small are both ready. Same-stream FIFO only
        // requires A large -> A small ordering (already guaranteed by the
        // queue-size-1 backpressure above), not that A small beats B small.
        send_data_owned(stream_a.tx.clone(), stream_a.stream_id, vec![2u8; 10]).await;
        send_data_owned(stream_b.tx.clone(), stream_b.stream_id, vec![3u8; 10]).await;

        // Drain both; A small must appear (A large already consumed above).
        let mut saw_a_small = false;
        for _ in 0..2 {
            let msg = rx.recv().await.unwrap();
            if msg.stream_id == 1 {
                saw_a_small = true;
            }
        }
        assert!(saw_a_small, "A small eventually emitted after A large");
    }

    /// Equal-size messages rotate by round-robin order, not lowest-token-first.
    #[tokio::test]
    async fn equal_size_messages_rotate_round_robin() {
        let (tx, mut rx) = write_data_channel();
        let stream_a = open_stream(&tx, &mut rx, 1).await;
        let stream_b = open_stream(&tx, &mut rx, 2).await;
        let stream_c = open_stream(&tx, &mut rx, 3).await;

        send_data_owned(stream_a.tx.clone(), stream_a.stream_id, vec![0u8; 10]).await;
        send_data_owned(stream_b.tx.clone(), stream_b.stream_id, vec![1u8; 10]).await;
        send_data_owned(stream_c.tx.clone(), stream_c.stream_id, vec![2u8; 10]).await;

        // All equal size. First pick is round-robin from head_pick_start=0, so
        // lowest token wins the first round; afterwards the cursor advances
        // past it, so subsequent picks rotate.
        let first = rx.recv().await.unwrap();
        assert_eq!(first.stream_id, 1);
        let second = rx.recv().await.unwrap();
        assert_eq!(second.stream_id, 2, "round-robin advances to B");
        let third = rx.recv().await.unwrap();
        assert_eq!(third.stream_id, 3, "round-robin advances to C");

        // Refill in the same order and confirm rotation continues: cursor is
        // now past C, wraps around to A.
        send_data_owned(stream_a.tx.clone(), stream_a.stream_id, vec![3u8; 10]).await;
        send_data_owned(stream_b.tx.clone(), stream_b.stream_id, vec![4u8; 10]).await;
        send_data_owned(stream_c.tx.clone(), stream_c.stream_id, vec![5u8; 10]).await;

        let fourth = rx.recv().await.unwrap();
        assert_eq!(fourth.stream_id, 1, "round-robin wraps to A");
        let fifth = rx.recv().await.unwrap();
        assert_eq!(fifth.stream_id, 2);
        let sixth = rx.recv().await.unwrap();
        assert_eq!(sixth.stream_id, 3);
    }

    #[test]
    fn priority_size_open_and_fin_are_zero() {
        let open = HeadEntry {
            msg: WriteDataMsg {
                stream_id: 1,
                data: StreamWriteData::Open,
            },
            offset: 0,
        };
        let fin = HeadEntry {
            msg: WriteDataMsg {
                stream_id: 1,
                data: StreamWriteData::Fin,
            },
            offset: 0,
        };
        let data = HeadEntry {
            msg: WriteDataMsg {
                stream_id: 1,
                data: StreamWriteData::Data(make_data(&[0u8; 42])),
            },
            offset: 0,
        };
        assert_eq!(priority_size(&open), 0);
        assert_eq!(priority_size(&fin), 0);
        assert_eq!(priority_size(&data), 42);
    }

    #[test]
    fn round_robin_distance_monotonic_from_start() {
        let start = fair_queue::Token(5);
        assert_eq!(round_robin_distance(start, fair_queue::Token(5)), 0);
        assert_eq!(round_robin_distance(start, fair_queue::Token(6)), 1);
        assert_eq!(round_robin_distance(start, fair_queue::Token(7)), 2);
        assert!(
            round_robin_distance(start, fair_queue::Token(6))
                < round_robin_distance(start, fair_queue::Token(7))
        );
    }

    // ---- Large-head preemption tests ----

    /// A large Data head is split across multiple dispatches and reassembles
    /// to the original bytes when consumed in arrival order.
    #[tokio::test]
    async fn large_data_head_reassembles_after_splitting() {
        let (tx, mut rx) = write_data_channel();
        let stream_a = open_stream(&tx, &mut rx, 1).await;

        // Larger than DATA_BULK_CAP so the head is split across multiple
        // dispatches. With a single freshly-opened stream the stream is
        // latency-sensitive by default, so each dispatch is capped at
        // DATA_QUANTUM.
        let big_len = DATA_BULK_CAP * 4 + 7;
        let body: Vec<u8> = (0u8..big_len as u8).cycle().take(big_len).collect();
        send_data_owned(stream_a.tx.clone(), stream_a.stream_id, body.clone()).await;

        let mut reassembled = Vec::new();
        loop {
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg.stream_id, 1);
            match msg.data {
                StreamWriteData::Data(data) => {
                    reassembled.extend_from_slice(&data);
                }
                StreamWriteData::Fin => break,
                StreamWriteData::Open => {}
            }
            if reassembled.len() >= big_len {
                break;
            }
        }
        assert_eq!(reassembled, body);
    }

    /// A small ready stream preempts a cached large tail: after the large head
    /// is split (latency-sensitive cap on first dispatch), the small stream's
    /// message arrives and is dispatched before the large tail resumes because
    /// its remaining length is smaller.
    #[tokio::test]
    async fn small_ready_stream_preempts_cached_large_tail() {
        let (tx, mut rx) = write_data_channel();
        let stream_a = open_stream(&tx, &mut rx, 1).await;
        let stream_b = open_stream(&tx, &mut rx, 2).await;

        // A's large head arrives first. A is latency-sensitive (freshly opened,
        // default class), so the dispatch is capped at DATA_QUANTUM and leaves
        // a tail cached under A's token.
        let big_len = DATA_BULK_CAP * 3;
        send_data_owned(stream_a.tx.clone(), stream_a.stream_id, vec![0u8; big_len]).await;
        let first = rx.recv().await.unwrap();
        assert_eq!(first.stream_id, 1);
        assert_eq!(data_len(&first.data), DATA_MEDIUM_CAP);

        // B's small message arrives. A's tail is still cached with remaining
        // length > 10, so B (smaller priority) preempts the tail.
        send_data_owned(stream_b.tx.clone(), stream_b.stream_id, vec![1u8; 10]).await;
        let second = rx.recv().await.unwrap();
        assert_eq!(
            second.stream_id, 2,
            "small ready B should preempt A's cached large tail"
        );
        assert_eq!(data_len(&second.data), 10);
    }

    /// `write_all` of a payload larger than `BodyLen::MAX` still reaches the
    /// peer intact end-to-end through the mux stream pair.
    #[tokio::test(flavor = "multi_thread")]
    async fn large_write_all_reaches_peer_intact() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        use crate::{spawn_mux_no_reconnection, Initiation, MuxConfig};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move { listener.accept().await.unwrap() });

        let b = tokio::net::TcpStream::connect(addr).await.unwrap();
        let a = accept.await.unwrap().0;

        let mut spawner = tokio::task::JoinSet::new();
        let (a_r, a_w) = a.into_split();
        let (opener, _) = spawn_mux_no_reconnection(
            a_r,
            a_w,
            MuxConfig {
                initiation: Initiation::Server,
                heartbeat_interval: Duration::from_secs(5),
            },
            &mut spawner,
        );
        let (b_r, b_w) = b.into_split();
        let (_, mut accepter) = spawn_mux_no_reconnection(
            b_r,
            b_w,
            MuxConfig {
                initiation: Initiation::Client,
                heartbeat_interval: Duration::from_secs(5),
            },
            &mut spawner,
        );

        let (a_stream, b_stream) = tokio::join!(opener.open(), accepter.accept());
        let mut a_stream = a_stream.unwrap().1;
        let mut b_stream = b_stream.unwrap().0;

        // Larger than BodyLen::MAX so it spans multiple wire frames and
        // exercises the split/reinsert path.
        let payload: Vec<u8> = (0u8..=255)
            .cycle()
            .take(usize::from(BodyLen::MAX) * 3 + 123)
            .collect();
        let expected = payload.clone();

        let writer = tokio::spawn(async move {
            a_stream.write_all(&payload).await.unwrap();
            a_stream.shutdown().unwrap();
            a_stream
        });
        let mut received = Vec::new();
        b_stream.read_to_end(&mut received).await.unwrap();
        writer.await.unwrap();
        assert_eq!(received, expected);
    }

    // ---- LatencyControl integration tests ----

    /// Send `n` messages of `size` bytes on `stream`, draining each from the
    /// receiver so the LatencyControl `record_send` bookkeeping runs once per
    /// message. The send/recv are driven concurrently because the per-stream
    /// fair queue has depth 1.
    async fn send_and_drain(
        stream: &StreamWriteDataTx,
        rx: &mut WriteDataRx,
        n: usize,
        size: usize,
    ) {
        for _ in 0..n {
            let tx = stream.tx.clone();
            let sid = stream.stream_id;
            let bytes = vec![0u8; size];
            tokio::join!(
                async {
                    tx.send(WriteDataMsg {
                        stream_id: sid,
                        data: StreamWriteData::Data(make_data(&bytes)),
                    })
                    .await
                    .unwrap();
                },
                async {
                    let _ = rx.recv().await.unwrap();
                }
            );
        }
    }

    /// A freshly opened stream is latency-sensitive by default: a large head
    /// is split at DATA_QUANTUM (not DATA_BULK_CAP) because no stream has met
    /// the bulk transition condition.
    #[tokio::test]
    async fn fresh_stream_is_latency_sensitive_caps_at_quantum() {
        let (tx, mut rx) = write_data_channel();
        let stream_a = open_stream(&tx, &mut rx, 1).await;

        // Two sends under QUANTUM so the stream has history but is not idle.
        send_and_drain(&stream_a, &mut rx, 2, DATA_EXTREME_CAP - 1).await;

        // A large head is split at DATA_QUANTUM (latency-sensitive cap), not
        // DATA_BULK_CAP, because the stream is not yet MustBulk (idle window
        // has not elapsed).
        let big_len = DATA_BULK_CAP * 2;
        send_data_owned(stream_a.tx.clone(), stream_a.stream_id, vec![0u8; big_len]).await;
        let first = rx.recv().await.unwrap();
        assert_eq!(first.stream_id, 1);
        assert_eq!(
            data_len(&first.data),
            DATA_MEDIUM_CAP,
            "fresh stream should be latency-sensitive and cap at DATA_QUANTUM"
        );
    }

    /// If the only open stream is `MustBulk` (enough small sends + idle for
    /// LATENCY_IDLE), a large head is split at DATA_BULK_CAP for throughput.
    ///
    /// Uses `tokio::time::pause` so the 30s idle window elapses quickly.
    #[tokio::test(start_paused = true)]
    async fn only_bulk_stream_caps_at_bulk_cap() {
        let (tx, mut rx) = write_data_channel();
        let stream_a = open_stream(&tx, &mut rx, 1).await;

        // 3 small sends: enough history, and >= 2/3 under QUANTUM.
        send_and_drain(&stream_a, &mut rx, 3, DATA_EXTREME_CAP - 1).await;

        // Advance past the idle window so the stream transitions to MustBulk.
        tokio::time::advance(LATENCY_IDLE + Duration::from_millis(10)).await;

        // A large head should now split at DATA_BULK_CAP, not DATA_QUANTUM,
        // because the only open stream is MustBulk (any_latency_sensitive is
        // false).
        let big_len = DATA_BULK_CAP * 2 + 5;
        send_data_owned(stream_a.tx.clone(), stream_a.stream_id, vec![0u8; big_len]).await;
        let first = rx.recv().await.unwrap();
        assert_eq!(first.stream_id, 1);
        assert_eq!(
            data_len(&first.data),
            DATA_BULK_CAP,
            "only-bulk open stream should cap at DATA_BULK_CAP"
        );
    }

    /// Closing the only sensitive stream lets the remaining MustBulk stream
    /// drain at DATA_BULK_CAP. This exercises the close bookkeeping and the
    /// aggregate recompute path.
    #[tokio::test(start_paused = true)]
    async fn closing_sensitive_stream_lets_bulk_drain_at_bulk_cap() {
        let (tx, mut rx) = write_data_channel();
        let stream_a = open_stream(&tx, &mut rx, 1).await;
        let _stream_b = open_stream(&tx, &mut rx, 2).await;

        // A -> MustBulk.
        send_and_drain(&stream_a, &mut rx, 3, DATA_EXTREME_CAP - 1).await;
        tokio::time::advance(LATENCY_IDLE + Duration::from_millis(10)).await;

        // Close B (the only sensitive stream) by dropping its sender.
        drop(_stream_b);

        // Drain any pending messages (Open/Fin) until B's close is observed.
        loop {
            let msg = rx.recv().await.unwrap();
            if msg.stream_id == 2 && matches!(msg.data, StreamWriteData::Fin) {
                break;
            }
        }

        // Now only A (MustBulk) is open: large head should cap at DATA_BULK_CAP.
        let big_len = DATA_BULK_CAP * 2 + 5;
        send_data_owned(stream_a.tx.clone(), stream_a.stream_id, vec![0u8; big_len]).await;
        let first = rx.recv().await.unwrap();
        assert_eq!(first.stream_id, 1);
        assert_eq!(
            data_len(&first.data),
            DATA_BULK_CAP,
            "after closing the only sensitive stream, bulk cap should apply"
        );
    }

    /// Regression for the two `fair_queue` defects (phantom per-clone Drop
    /// marks + spurious `Poll::Pending` aborting the scan): dropping a
    /// `Sender` clone after each send (any helper taking `Sender` by value)
    /// concurrently with the receiver drain. Pre-fix this deadlocks: the
    /// sender task completes all sends while the receiver stalls with two
    /// undelivered messages. Each `recv` is wrapped in a 10 s `timeout` so a
    /// regression fails instead of hanging.
    #[tokio::test(flavor = "multi_thread")]
    async fn clone_drop_per_send_does_not_strand_receiver() {
        let (tx, mut rx) = write_data_channel();
        let stream_a = open_stream(&tx, &mut rx, 1).await;
        let stream_b = open_stream(&tx, &mut rx, 2).await;

        // Byte totals the receiver must observe.
        const A_LEN: usize = 16 * 1024;
        const B_SMALL: usize = 100;
        const B_LARGE: usize = 4096;
        const CYCLES: usize = 6;
        let a_total = A_LEN * CYCLES;
        let b_total = (B_SMALL * 5 + B_LARGE) * CYCLES;

        let a_tx = stream_a.tx.clone();
        let b_tx = stream_b.tx.clone();
        let a_sid = stream_a.stream_id;
        let b_sid = stream_b.stream_id;

        // Sender task: per cycle, one 16 KiB on A then five 100 B and one
        // 4096 B on B. `send_data_owned` takes the Sender by value, so each
        // call drops a clone — the trigger. The per-stream queue depth is
        // small, so once a queue fills the sender task naturally yields to
        // the receiver, interleaving sends with drains — the contention
        // window in which the spurious scan abort strands later-ready
        // tokens.
        let sender = tokio::spawn(async move {
            for _ in 0..CYCLES {
                send_data_owned(a_tx.clone(), a_sid, vec![0u8; A_LEN]).await;
                for _ in 0..5 {
                    send_data_owned(b_tx.clone(), b_sid, vec![1u8; B_SMALL]).await;
                }
                send_data_owned(b_tx.clone(), b_sid, vec![2u8; B_LARGE]).await;
            }
            drop(a_tx);
            drop(b_tx);
        });

        // Concurrent drain until both byte totals arrive.
        let mut a_seen = 0usize;
        let mut b_seen = 0usize;
        while a_seen < a_total || b_seen < b_total {
            let msg = tokio::time::timeout(Duration::from_secs(10), rx.recv())
                .await
                .expect("receiver stalled: messages stranded by fair-queue defect")
                .unwrap();
            if let StreamWriteData::Data(bytes) = msg.data {
                if msg.stream_id == a_sid {
                    a_seen += bytes.len();
                } else if msg.stream_id == b_sid {
                    b_seen += bytes.len();
                }
            }
        }
        assert_eq!(a_seen, a_total);
        assert_eq!(b_seen, b_total);
        sender.await.unwrap();
    }
}
