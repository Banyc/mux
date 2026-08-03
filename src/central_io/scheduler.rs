use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::time::Instant;

use primitive::arena::obj_pool::ArcObjPool;

use crate::{
    central_io::DataBuf,
    common::Side,
    control::DeadControl,
    fair_queue,
    protocol::StreamId,
    traffic_class::LatencyControl,
};

use super::DeadCentralIo;

const CONTROL_CHANNEL_SIZE: usize = 1024;
const SPLIT_POOL_SHARDS: NonZeroUsize = NonZeroUsize::new(1).unwrap();

const DATA_EXTREME_CAP: usize = 1200;
const DATA_MEDIUM_CAP: usize = crate::traffic_class::BULK_THRESHOLD;
pub(crate) const DATA_BULK_CAP: usize = 32 * 1024;

#[derive(Debug)]
pub struct WriteDataMsg {
    pub stream_id: StreamId,
    pub data: StreamWriteData,
}
#[derive(Debug)]
pub enum StreamWriteData {
    Open { wire: bool },
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
            if entry.offset == 0 || data.len() >= DATA_BULK_CAP {
                // Record once at first dispatch, and additionally on every
                // dispatch for heads whose original length is at least
                // DATA_BULK_CAP so a sustained bulk transfer accumulates
                // LATENCY_HISTORY_MIN observations and escapes the small caps
                // mid-transfer. Always use the original message length, never
                // the capped emit size. (offset == 0 prevents double-counting
                // a head whose original size is exactly DATA_BULK_CAP; the
                // StreamWriter staging cap is unrelated and may be larger.)
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
        StreamWriteData::Open { .. } => 0,
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
    pub async fn derive(
        &self,
        stream: StreamId,
        wire_open: bool,
    ) -> Result<StreamWriteDataTx, DeadCentralIo> {
        Ok(StreamWriteDataTx {
            tx: self
                .opener
                .open(WriteDataMsg {
                    stream_id: stream,
                    data: StreamWriteData::Open { wire: wire_open },
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
    /// Reserve capacity on the fair queue before sending.  Probes the queue
    /// for a free slot without consuming it; the reserved slot is used by the
    /// subsequent `send_item`.  Preserves the error from the fair queue so the
    /// caller can distinguish a closed central I/O from a full queue.
    pub(crate) fn poll_reserve(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), DeadCentralIo>> {
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
    /// The local reader is closed, so the peer must stop its write half.
    CloseRead(StreamId),
    /// Legacy mode-off whole-stream abort. This may overtake pending data and must never be used as a graceful write FIN.
    ForceCloseWrite(StreamId),
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
    use std::time::Duration;

    use primitive::arena::obj_pool::arc_buf_pool;

    use super::{
        DATA_BULK_CAP, DATA_EXTREME_CAP, DATA_MEDIUM_CAP, HeadEntry, StreamWriteData,
        StreamWriteDataTx, WriteDataMsg, WriteDataRx, WriteDataTxPrototype, priority_size,
        round_robin_distance, write_data_channel,
    };
    use crate::fair_queue;
    use crate::protocol::StreamId;
    use crate::traffic_class::{LATENCY_HISTORY_MAX, LATENCY_IDLE};

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
                stream = Some(tx.derive(stream_id, false).await.unwrap());
            },
            async {
                while !got_open {
                    let msg = rx.recv().await.unwrap();
                    assert_eq!(msg.stream_id, stream_id);
                    assert!(matches!(msg.data, StreamWriteData::Open { .. }));
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
            StreamWriteData::Open { .. } | StreamWriteData::Fin => 0,
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
                data: StreamWriteData::Open { wire: false },
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
                StreamWriteData::Open { .. } => {}
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
            send_and_drain_message(stream, rx, size).await;
        }
    }

    /// Send one message of `size` bytes and drain every dispatch produced from
    /// it. This is needed when `size` is larger than the active cap and splits
    /// into multiple dispatches, because a single `recv` would leave the rest
    /// cached and the next send would queue behind an incomplete head.
    async fn send_and_drain_message(stream: &StreamWriteDataTx, rx: &mut WriteDataRx, size: usize) {
        let tx = stream.tx.clone();
        let sid = stream.stream_id;
        let bytes = vec![0u8; size];
        let mut sent = false;
        let mut seen = 0usize;
        while !sent || seen < size {
            tokio::join!(
                async {
                    if !sent {
                        tx.send(WriteDataMsg {
                            stream_id: sid,
                            data: StreamWriteData::Data(make_data(&bytes)),
                        })
                        .await
                        .unwrap();
                        sent = true;
                    }
                },
                async {
                    if seen < size {
                        let msg = rx.recv().await.unwrap();
                        assert_eq!(msg.stream_id, sid);
                        if let StreamWriteData::Data(data) = msg.data {
                            seen += data.len();
                        }
                    }
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

    // ---- Latency ramp tests ----

    /// A head of exactly DATA_BULK_CAP bytes ramps to MustBulk mid-transfer
    /// and emits the tail under DATA_BULK_CAP. The first three dispatches are
    /// capped at DATA_MEDIUM_CAP (accumulating LATENCY_HISTORY_MIN bulk
    /// observations), and the fourth is the remaining tail.
    /// Regression for the record condition: exactly-capped heads used to skip
    /// recording after the first dispatch and therefore never ramped.
    #[tokio::test]
    async fn exactly_bulk_cap_head_ramps_mid_transfer() {
        let (tx, mut rx) = write_data_channel();
        let stream_a = open_stream(&tx, &mut rx, 1).await;

        // One head of exactly DATA_BULK_CAP bytes. The original length is
        // >= DATA_BULK_CAP, so every dispatch records an observation. After
        // three DATA_MEDIUM_CAP dispatches the stream has reached
        // LATENCY_HISTORY_MIN bulk observations and ramps to MustBulk, so the
        // fourth dispatch emits the rest of the head.
        let big_len = DATA_BULK_CAP;
        send_data_owned(stream_a.tx.clone(), stream_a.stream_id, vec![0u8; big_len]).await;

        let mut sizes = Vec::new();
        let mut seen = 0usize;
        while seen < big_len {
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg.stream_id, 1);
            if let StreamWriteData::Data(data) = msg.data {
                seen += data.len();
                sizes.push(data.len());
            }
        }
        assert!(
            sizes.len() >= 4,
            "expected at least 4 dispatches, got {:?}",
            sizes
        );
        assert_eq!(
            sizes[..4],
            [
                DATA_MEDIUM_CAP,
                DATA_MEDIUM_CAP,
                DATA_MEDIUM_CAP,
                DATA_BULK_CAP - 3 * DATA_MEDIUM_CAP
            ]
        );
    }

    /// A single sustained bulk transfer ramps to MustBulk mid-transfer and
    /// starts using DATA_BULK_CAP for dispatch 4. The first three dispatches
    /// are capped at DATA_MEDIUM_CAP; the fourth is capped at DATA_BULK_CAP.
    #[tokio::test]
    async fn sole_bulk_head_escapes_latency_caps_mid_transfer() {
        let (tx, mut rx) = write_data_channel();
        let stream_a = open_stream(&tx, &mut rx, 1).await;

        // One head larger than DATA_BULK_CAP so every dispatch records an
        // observation. With DATA_MEDIUM_CAP = 2 KiB, the first three emits are
        // 2 KiB each, producing 3 bulk observations. The fourth dispatch is
        // computed after the third record_send, when the stream is MustBulk.
        let big_len = DATA_BULK_CAP * 2;
        send_data_owned(stream_a.tx.clone(), stream_a.stream_id, vec![0u8; big_len]).await;

        let mut sizes = Vec::new();
        let mut seen = 0usize;
        while seen < big_len {
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg.stream_id, 1);
            if let StreamWriteData::Data(data) = msg.data {
                seen += data.len();
                sizes.push(data.len());
            }
        }
        assert!(
            sizes.len() >= 4,
            "expected at least 4 dispatches, got {:?}",
            sizes
        );
        assert_eq!(
            sizes[..4],
            [
                DATA_MEDIUM_CAP,
                DATA_MEDIUM_CAP,
                DATA_MEDIUM_CAP,
                DATA_BULK_CAP
            ]
        );
    }

    /// A new latency-sensitive stream restores small caps over a ramped
    /// MustBulk stream.
    #[tokio::test]
    async fn new_stream_restores_small_caps_over_ramped_bulk_stream() {
        let (tx, mut rx) = write_data_channel();
        let stream_a = open_stream(&tx, &mut rx, 1).await;

        // Ramp A to MustBulk with a large head. The first three dispatches are
        // DATA_MEDIUM_CAP, then the fourth (computed after the third bulk
        // observation) uses DATA_BULK_CAP. Drain until that ramped dispatch is
        // observed.
        let big_len = DATA_BULK_CAP * 2;
        send_data_owned(stream_a.tx.clone(), stream_a.stream_id, vec![0u8; big_len]).await;
        let mut seen = 0usize;
        let mut ramped = false;
        while seen < big_len {
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg.stream_id, 1);
            if let StreamWriteData::Data(data) = msg.data {
                seen += data.len();
                if data.len() == DATA_BULK_CAP {
                    ramped = true;
                    break;
                }
            }
        }
        assert!(ramped, "stream A should have ramped to DATA_BULK_CAP");
        assert!(!rx.latency.any_latency_sensitive());

        // Open a fresh sensitive stream B and send 10 bytes. The presence of
        // any latency-sensitive stream caps every dispatch at DATA_MEDIUM_CAP,
        // so A's next dispatch drops back from DATA_BULK_CAP.
        let stream_b = open_stream(&tx, &mut rx, 2).await;
        send_data_owned(stream_b.tx.clone(), stream_b.stream_id, vec![1u8; 10]).await;

        // B's small message is emitted first (priority), but more importantly
        // A's next dispatch after B arrives is capped at DATA_MEDIUM_CAP.
        let first = rx.recv().await.unwrap();
        assert_eq!(first.stream_id, 2);
        assert_eq!(data_len(&first.data), 10);

        // A's remaining large tail is now emitted under the small cap because
        // B is sensitive.
        let second = rx.recv().await.unwrap();
        assert_eq!(second.stream_id, 1);
        assert_eq!(
            data_len(&second.data),
            DATA_MEDIUM_CAP,
            "ramped bulk stream must drop back to DATA_MEDIUM_CAP when a sensitive stream is open"
        );
    }

    /// MustBulk reverts to latency-sensitive within a bounded number of small
    /// record_send calls thanks to LATENCY_HISTORY_MAX.
    #[tokio::test]
    async fn must_bulk_reverts_after_bounded_small_sends() {
        let (tx, mut rx) = write_data_channel();
        let stream_a = open_stream(&tx, &mut rx, 1).await;

        // Saturate the token with 100 bulk observations. Use a message larger
        // than DATA_BULK_CAP so every dispatch records; drain each message
        // fully so the observations are tallied.
        for _ in 0..100 {
            send_and_drain_message(&stream_a, &mut rx, DATA_BULK_CAP + 1).await;
        }
        assert!(!rx.latency.any_latency_sensitive());

        // Now send small messages. The total number of record_send calls
        // required to flip back is bounded by 2 * LATENCY_HISTORY_MAX because
        // each small send is recorded and the halving keeps recent small sends
        // dominant.
        let mut calls = 0usize;
        while !rx.latency.any_latency_sensitive() {
            send_and_drain_message(&stream_a, &mut rx, DATA_MEDIUM_CAP).await;
            calls += 1;
            assert!(
                calls <= 2 * LATENCY_HISTORY_MAX,
                "MustBulk should revert within 2 * LATENCY_HISTORY_MAX small record_send calls"
            );
        }
    }

    /// Mixed small and medium streams keep a truly bulk stream capped so the
    /// interactive-preemption guarantee holds. B's 4096 B message is split
    /// into DATA_EXTREME_CAP slices and still counts as one observation, so
    /// B remains sensitive and A never dispatches more than DATA_MEDIUM_CAP.
    #[tokio::test(flavor = "multi_thread")]
    async fn mixed_small_and_medium_stream_keeps_bulk_capped() {
        let (tx, mut rx) = write_data_channel();
        let stream_a = open_stream(&tx, &mut rx, 1).await;
        let stream_b = open_stream(&tx, &mut rx, 2).await;

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

        let mut a_seen = 0usize;
        let mut b_seen = 0usize;
        while a_seen < a_total || b_seen < b_total {
            let msg = tokio::time::timeout(Duration::from_secs(10), rx.recv())
                .await
                .expect("receiver stalled")
                .unwrap();
            if let StreamWriteData::Data(bytes) = msg.data {
                if msg.stream_id == a_sid {
                    assert!(
                        bytes.len() <= DATA_MEDIUM_CAP,
                        "stream A must stay capped at DATA_MEDIUM_CAP while B is sensitive"
                    );
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
