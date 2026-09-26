use std::{
    future::Future,
    io,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use primitive::arena::obj_pool::ArcObjPool;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader, ReadBuf};

use crate::{
    control::DeadControl,
    protocol::{
        CloseWriteExtMsg, DataHeader, DataHeaderExt, Header, Offset, Side, StreamId, StreamIdMsg,
    },
};

use super::{DataBuf, DeadCentralIo};

const OBJ_POOL_SHARDS: NonZeroUsize = NonZeroUsize::new(4).unwrap();
const CHANNEL_SIZE: usize = 1024;
const READ_BUF_CAPACITY: usize = 64 * 1024;
const RECEIVE_DEADLINE_INTERVALS: u32 = 4;
/// Deadline used by [`LivenessRead`] before `recv_with_steady_deadline` arms
/// it. Never in force for a real read, but finite so `Instant + deadline`
/// cannot overflow.
const UNARMED_DEADLINE: Duration = Duration::from_secs(60 * 60 * 24 * 365);

/// Enforces a *sliding* receive deadline over an [`AsyncRead`]: the read fails
/// with [`io::ErrorKind::TimedOut`] only when no bytes have arrived for
/// `deadline`, measured from the **last byte that did arrive** rather than from
/// the start of the read. A large frame whose bytes trickle in steadily is
/// progress and must not be severed; an idle peer makes no progress and is.
struct LivenessRead<R> {
    inner: R,
    deadline: Duration,
    last_progress: tokio::time::Instant,
    /// Armed only while the inner read is pending and re-created whenever a
    /// byte resets `last_progress`.
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
}
impl<R> LivenessRead<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            deadline: UNARMED_DEADLINE,
            last_progress: tokio::time::Instant::now(),
            sleep: None,
        }
    }
    /// Re-arm the deadline from *now*: the next frame gets a full deadline
    /// window, and any in-flight sleep for the old window is discarded.
    fn set_deadline(&mut self, deadline: Duration) {
        self.deadline = deadline;
        self.last_progress = tokio::time::Instant::now();
        self.sleep = None;
    }
}
impl<R: std::fmt::Debug> std::fmt::Debug for LivenessRead<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LivenessRead")
            .field("inner", &self.inner)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}
impl<R> AsyncRead for LivenessRead<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                if buf.filled().len() > before {
                    self.last_progress = tokio::time::Instant::now();
                    self.sleep = None;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => {
                let (deadline, last_progress) = (self.deadline, self.last_progress);
                let sleep = self.sleep.get_or_insert_with(|| {
                    Box::pin(tokio::time::sleep_until(last_progress + deadline))
                });
                if sleep.as_mut().poll(cx).is_ready() {
                    self.sleep = None;
                    Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "receive deadline - session timed out",
                    )))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

pub async fn run_central_io_reader<R>(
    mut io_reader: CentralIoReader<R>,
    tx: CentralIoReadTx,
    heartbeat_interval: Duration,
    first_receive_deadline: Option<Duration>,
    mut first_receive_tx: Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<(), RunCentralIoReaderError>
where
    R: AsyncRead + Unpin,
{
    let steady_deadline = heartbeat_interval * RECEIVE_DEADLINE_INTERVALS;
    let mut deadline = first_receive_deadline.unwrap_or(steady_deadline);
    loop {
        let msg = io_reader
            .recv_with_steady_deadline(deadline, steady_deadline, &mut first_receive_tx)
            .await
            .map_err(RunCentralIoReaderError::IoReader)?;
        deadline = steady_deadline;
        tx.send(msg)
            .await
            .map_err(RunCentralIoReaderError::Control)?;
    }
}
#[derive(Debug)]
pub enum RunCentralIoReaderError {
    IoReader(io::Error),
    Control(DeadControl),
}

impl From<io::Error> for RunCentralIoReaderError {
    fn from(e: io::Error) -> Self {
        RunCentralIoReaderError::IoReader(e)
    }
}

#[derive(Debug)]
pub struct CentralIoReader<R> {
    io_reader: BufReader<LivenessRead<R>>,
    buf_pool: ArcObjPool<Vec<u8>>,
    frame_reassembly: bool,
}
impl<R> CentralIoReader<R>
where
    R: AsyncRead + Unpin,
{
    pub fn new(io_reader: R, frame_reassembly: bool) -> Self {
        Self {
            io_reader: BufReader::with_capacity(READ_BUF_CAPACITY, LivenessRead::new(io_reader)),
            buf_pool: ArcObjPool::new(None, OBJ_POOL_SHARDS, Vec::new, |v| v.clear()),
            frame_reassembly,
        }
    }
}
impl<R> CentralIoReader<R>
where
    R: AsyncRead + Unpin,
{
    pub async fn recv_with_steady_deadline(
        &mut self,
        mut deadline: Duration,
        steady_deadline: Duration,
        first_receive_tx: &mut Option<tokio::sync::oneshot::Sender<()>>,
    ) -> io::Result<CentralIoReadMsg> {
        self.io_reader.get_mut().set_deadline(deadline);
        loop {
            let res = self.recv_pkt().await?;
            if let Some(tx) = first_receive_tx.take() {
                let _ = tx.send(());
            }
            deadline = steady_deadline;
            self.io_reader.get_mut().set_deadline(deadline);
            if let Some(res) = res {
                return Ok(res);
            }
        }
    }
    async fn recv_pkt(&mut self) -> io::Result<Option<CentralIoReadMsg>> {
        let mut hdr = [0; Header::SIZE];
        self.io_reader.read_exact(&mut hdr).await?;
        let hdr = Header::decode(hdr).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown header: {hdr:?}"),
            )
        })?;
        Ok(match hdr {
            Header::Heartbeat => {
                crate::padding::skip_tail(&mut self.io_reader).await?;
                None
            }
            Header::Open => {
                let stream = self.recv_stream_id().await?;
                crate::padding::skip_tail(&mut self.io_reader).await?;
                crate::live_probe::note_frame_read(crate::live_probe::EgressFrameKind::Control);
                Some(CentralIoReadMsg::Open(stream))
            }
            Header::Data => {
                let (stream, offset, pkt) = self.recv_data().await?;
                crate::live_probe::note_frame_read(crate::live_probe::EgressFrameKind::Data);
                Some(CentralIoReadMsg::Data(stream, offset, pkt))
            }
            Header::CloseRead => {
                let stream = self.recv_stream_id().await?;
                crate::padding::skip_tail(&mut self.io_reader).await?;
                crate::live_probe::note_frame_read(crate::live_probe::EgressFrameKind::Control);
                Some(CentralIoReadMsg::Close(stream, Side::Read, 0))
            }
            Header::CloseWrite => {
                let (stream, final_offset) = self.recv_close_write().await?;
                crate::padding::skip_tail(&mut self.io_reader).await?;
                crate::live_probe::note_frame_read(crate::live_probe::EgressFrameKind::CloseWrite);
                Some(CentralIoReadMsg::Close(stream, Side::Write, final_offset))
            }
        })
    }
    async fn recv_data(&mut self) -> io::Result<(StreamId, Offset, DataBuf)> {
        if self.frame_reassembly {
            let mut hdr = [0; DataHeaderExt::SIZE];
            self.io_reader.read_exact(&mut hdr).await?;
            let hdr = DataHeaderExt::decode(hdr);
            let mut remaining = usize::from(hdr.body_len);
            let mut buf = self.buf_pool.take_scoped();
            buf.reserve(remaining);
            while remaining != 0 {
                let chunk = self.io_reader.fill_buf().await?;
                if chunk.is_empty() {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
                let n = chunk.len().min(remaining);
                buf.extend_from_slice(&chunk[..n]);
                self.io_reader.consume(n);
                remaining -= n;
            }
            Ok((hdr.stream_id, hdr.offset, buf))
        } else {
            let mut hdr = [0; DataHeader::SIZE];
            self.io_reader.read_exact(&mut hdr).await?;
            let hdr = DataHeader::decode(hdr);
            let mut remaining = usize::from(hdr.body_len);
            let mut buf = self.buf_pool.take_scoped();
            buf.reserve(remaining);
            while remaining != 0 {
                let chunk = self.io_reader.fill_buf().await?;
                if chunk.is_empty() {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
                let n = chunk.len().min(remaining);
                buf.extend_from_slice(&chunk[..n]);
                self.io_reader.consume(n);
                remaining -= n;
            }
            Ok((hdr.stream_id, 0, buf))
        }
    }
    async fn recv_close_write(&mut self) -> io::Result<(StreamId, Offset)> {
        if self.frame_reassembly {
            let mut buf = [0; CloseWriteExtMsg::SIZE];
            self.io_reader.read_exact(&mut buf).await?;
            let msg = CloseWriteExtMsg::decode(buf);
            Ok((msg.stream_id, msg.final_offset))
        } else {
            Ok((self.recv_stream_id().await?, 0))
        }
    }
    async fn recv_stream_id(&mut self) -> io::Result<StreamId> {
        let mut hdr = [0; StreamIdMsg::SIZE];
        self.io_reader.read_exact(&mut hdr).await?;
        let hdr = StreamIdMsg::decode(hdr);
        Ok(hdr.stream_id)
    }
}

#[derive(Debug)]
pub enum CentralIoReadMsg {
    Open(StreamId),
    /// `(stream_id, byte_offset, body)`. In mode-off the offset is always 0
    /// and is ignored by the control loop; in mode-on it is the per-stream
    /// byte offset of the first byte in `body`.
    Data(StreamId, Offset, DataBuf),
    /// `(stream_id, side, final_offset)`. `final_offset` is meaningful only
    /// for `Side::Write` in mode-on; in mode-off (or `Side::Read`) it is 0.
    Close(StreamId, Side, Offset),
}
pub fn central_io_read_channel() -> (CentralIoReadTx, CentralIoReadRx) {
    let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_SIZE);
    let tx = CentralIoReadTx { tx };
    let rx = CentralIoReadRx { rx };
    (tx, rx)
}
#[derive(Debug, Clone)]
pub struct CentralIoReadTx {
    tx: tokio::sync::mpsc::Sender<CentralIoReadMsg>,
}
impl CentralIoReadTx {
    pub async fn send(&self, msg: CentralIoReadMsg) -> Result<(), DeadControl> {
        self.tx.send(msg).await.map_err(|_| DeadControl {})
    }
}
#[derive(Debug)]
pub struct CentralIoReadRx {
    rx: tokio::sync::mpsc::Receiver<CentralIoReadMsg>,
}
impl CentralIoReadRx {
    pub async fn recv(&mut self) -> Result<CentralIoReadMsg, DeadCentralIo> {
        self.rx
            .recv()
            .await
            .ok_or(DeadCentralIo { side: Side::Read })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn first_receive_ready_waits_for_complete_open_frame() {
        let (mut client, server) = tokio::io::duplex(64);
        let mut reader = CentralIoReader::new(server, false);
        let (ready_tx, mut ready_rx) = tokio::sync::oneshot::channel();
        let mut first_receive_tx = Some(ready_tx);

        client.write_all(&[0x01]).await.unwrap();

        let mut reader_tasks = tokio::task::JoinSet::new();
        reader_tasks.spawn(async move {
            let _ = reader
                .recv_with_steady_deadline(
                    Duration::from_secs(5),
                    Duration::from_secs(5),
                    &mut first_receive_tx,
                )
                .await;
        });

        match tokio::time::timeout(Duration::from_millis(100), &mut ready_rx).await {
            Err(_elapsed) => {}
            Ok(Ok(())) => panic!("ready_tx fired before complete frame was consumed"),
            Ok(Err(_)) => {}
        }

        client.write_all(&0u32.to_be_bytes()).await.unwrap();
        // The Open frame carries a padding tail; a zero-length tail completes it.
        client.write_all(&0u16.to_be_bytes()).await.unwrap();

        match tokio::time::timeout(Duration::from_secs(1), &mut ready_rx).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => panic!("ready_tx sender dropped without sending"),
            Err(_elapsed) => panic!("ready_tx did not resolve after complete frame"),
        }
        while let Some(result) = reader_tasks.join_next().await {
            result.unwrap();
        }
    }

    /// The receive deadline is a *liveness* probe: it must fire only when the
    /// peer has gone silent, not when a large frame's bytes arrive slowly but
    /// steadily. A frame whose total transfer outlasts the deadline while each
    /// inter-byte gap is well under it is progress and must not be severed.
    #[tokio::test(start_paused = true)]
    async fn a_slowly_but_steadily_arriving_frame_is_not_severed() {
        use crate::protocol::{DataHeader, Header};

        let deadline = Duration::from_millis(100);
        let (mut client, server) = tokio::io::duplex(1 << 16);
        let mut reader = CentralIoReader::new(server, false);
        let mut first_receive_tx = None;

        let body_len: u16 = 4096;
        let mut frame = Vec::new();
        frame.push(Header::Data.encode()[0]);
        frame.extend_from_slice(
            &DataHeader {
                stream_id: 7,
                body_len,
            }
            .encode(),
        );
        frame.extend(std::iter::repeat_n(0xABu8, body_len as usize));

        // Deliver one byte every 10 ms: every gap is far below the deadline,
        // but the whole frame takes tens of deadlines to arrive. The writer is
        // owned by the test's JoinSet so the panic/abort discipline holds.
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move {
            for byte in frame {
                tokio::io::AsyncWriteExt::write_all(&mut client, &[byte])
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        let msg = reader
            .recv_with_steady_deadline(deadline, deadline, &mut first_receive_tx)
            .await
            .expect("a steadily-arriving frame must not trip the receive deadline");
        match msg {
            CentralIoReadMsg::Data(stream, _offset, body) => {
                assert_eq!(stream, 7);
                assert_eq!(body.len(), body_len as usize);
            }
            other => panic!("expected Data, got {other:?}"),
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    }

    /// A Data frame's body buffer is taken from the reader's buffer pool and
    /// reused across frames: after one warm frame, receiving another identical
    /// frame must not allocate. Regression guard for the pooled reuse
    /// (replacing `buf_pool.take_scoped()` with a fresh `Vec` allocates once
    /// per body frame and must fail here). Runs on the current thread so the
    /// per-thread allocation counter measures this test's reads only.
    #[tokio::test]
    async fn data_frames_reuse_the_pooled_body_buffer() {
        use crate::protocol::{DataHeader, Header};

        let (mut client, server) = tokio::io::duplex(1 << 16);
        let mut reader = CentralIoReader::new(server, false);

        let body_len: u16 = 8192;
        let mut frame = Vec::new();
        frame.push(Header::Data.encode()[0]);
        frame.extend_from_slice(
            &DataHeader {
                stream_id: 7,
                body_len,
            }
            .encode(),
        );
        frame.extend(std::iter::repeat_n(0xABu8, body_len as usize));

        // Warm the pool shards (four shards need one frame each before the
        // take/drop rotation aligns) and the BufReader with identical frames.
        // Each warm read pre-fills the BufReader so the measured region
        // performs zero inner I/O polls: the read exercises only frame
        // decoding and the pooled body-buffer acquisition. Without the
        // pre-fill the inner duplex read may occasionally poll Pending and the
        // LivenessRead deadline machinery allocates a Box<Sleep>, which would
        // make an exact allocation count timing-dependent.
        for _ in 0..6 {
            client.write_all(&frame).await.unwrap();
            let _ = reader.io_reader.fill_buf().await.unwrap();
            let msg = reader
                .recv_pkt()
                .await
                .expect("a complete frame must decode");
            let body_len_seen = match msg {
                Some(CentralIoReadMsg::Data(_stream, _offset, body)) => body.len(),
                other => panic!("expected Data, got {other:?}"),
            };
            assert_eq!(body_len_seen, 8192);
        }
        let mut allocated_total = 0usize;
        for _ in 0..4 {
            client.write_all(&frame).await.unwrap();
            let _ = reader.io_reader.fill_buf().await.unwrap();
            let before = crate::test_alloc::thread_alloc_count();
            let msg = reader
                .recv_pkt()
                .await
                .expect("a complete frame must decode");
            let allocated = crate::test_alloc::thread_alloc_count() - before;
            allocated_total += allocated;
            let body_len_seen = match msg {
                Some(CentralIoReadMsg::Data(_stream, _offset, body)) => body.len(),
                other => panic!("expected Data, got {other:?}"),
            };
            assert_eq!(body_len_seen, 8192);
            assert_eq!(
                allocated, 0,
                "a warm 8192-byte Data frame allocated {allocated} times; the body \
                 buffer must be reused from the reader's pool, not allocated per \
                 frame",
            );
        }
        assert_eq!(allocated_total, 0);
    }

    /// A Data frame whose declared body is cut short by EOF is a peer that
    /// died mid-frame. That is `UnexpectedEof`, not `InvalidData`: the bytes
    /// did not decode into an unexpected message, they never arrived.
    #[tokio::test]
    async fn eof_mid_body_is_unexpected_eof() {
        use crate::protocol::{DataHeader, Header};

        let (mut client, server) = tokio::io::duplex(1 << 16);
        let mut reader = CentralIoReader::new(server, false);
        client.write_all(&[Header::Data.encode()[0]]).await.unwrap();
        client
            .write_all(
                &DataHeader {
                    stream_id: 7,
                    body_len: 100,
                }
                .encode(),
            )
            .await
            .unwrap();
        client.write_all(&[0xAB; 10]).await.unwrap();
        drop(client);
        let err = reader.recv_pkt().await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    /// The same truncation boundary in frame-reassembly mode, whose Data
    /// header carries an offset: a short body is still a dead peer
    /// (`UnexpectedEof`), never a decoded-but-unexpected `InvalidData`.
    #[tokio::test]
    async fn eof_mid_body_is_unexpected_eof_in_reassembly_mode() {
        use crate::protocol::{DataHeaderExt, Header};

        let (mut client, server) = tokio::io::duplex(1 << 16);
        let mut reader = CentralIoReader::new(server, true);
        client.write_all(&[Header::Data.encode()[0]]).await.unwrap();
        client
            .write_all(
                &DataHeaderExt {
                    stream_id: 7,
                    body_len: 100,
                    offset: 0,
                }
                .encode(),
            )
            .await
            .unwrap();
        client.write_all(&[0xAB; 10]).await.unwrap();
        drop(client);
        let err = reader.recv_pkt().await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    /// The steady receive deadline is exactly
    /// `heartbeat_interval * RECEIVE_DEADLINE_INTERVALS`: a silent peer must
    /// trip it as the paused clock crosses the fourth interval and not before.
    /// The multi-thread deadline test deliberately leaves the multiple free
    /// (it asserts the error, not the timing), so this pins the constant on
    /// the paused clock, polling the reader task once at each side of the
    /// boundary.
    #[tokio::test(start_paused = true)]
    async fn the_steady_receive_deadline_is_four_heartbeat_intervals() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct ProbeReader<R> {
            inner: R,
            polled: std::sync::Arc<AtomicBool>,
        }
        impl<R: AsyncRead + Unpin> AsyncRead for ProbeReader<R> {
            fn poll_read(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                self.polled.store(true, Ordering::SeqCst);
                Pin::new(&mut self.inner).poll_read(cx, buf)
            }
        }

        let heartbeat_interval = Duration::from_millis(100);
        // The peer half stays alive so the read stays Pending; a dropped peer
        // would surface as EOF, not as the liveness deadline.
        let (_peer, server) = tokio::io::duplex(1 << 16);
        let polled = std::sync::Arc::new(AtomicBool::new(false));
        let reader = ProbeReader {
            inner: server,
            polled: polled.clone(),
        };
        let (tx, _rx) = central_io_read_channel();
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move {
            run_central_io_reader(
                CentralIoReader::new(reader, false),
                tx,
                heartbeat_interval,
                None,
                None,
            )
            .await
        });

        // Let the reader reach its first (pending) read and arm the deadline
        // without advancing the clock.
        let mut spins = 0;
        while !polled.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
            spins += 1;
            assert!(spins < 10_000, "the reader never polled its inner stream");
        }

        // One millisecond short of four intervals: still alive.
        tokio::time::advance(heartbeat_interval * 4 - Duration::from_millis(1)).await;
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert!(
            tokio::time::timeout(Duration::ZERO, tasks.join_next())
                .await
                .is_err(),
            "the receive deadline fired before four heartbeat intervals elapsed"
        );

        // Crossing the fourth interval: the deadline must fire, and the
        // session must report the receive-deadline TimedOut.
        tokio::time::advance(Duration::from_millis(1)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        let joined = tokio::time::timeout(Duration::ZERO, tasks.join_next())
            .await
            .expect("the receive deadline did not fire at four heartbeat intervals");
        match joined
            .expect("the reader task vanished")
            .expect("the reader task panicked")
        {
            Err(RunCentralIoReaderError::IoReader(e)) => {
                assert_eq!(e.kind(), io::ErrorKind::TimedOut, "wrong error kind: {e:?}");
            }
            other => panic!("expected Err(IoReader(TimedOut)), got {other:?}"),
        }
    }
}
