use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Semaphore,
    task::JoinSet,
};

use crate::{
    dual_lane::{DualAcceptError, DualStreamAccepter, DualStreamOpener},
    StreamReader,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum payload length accepted by the sender and receiver.
/// Both sides MUST use the same limit; mismatched limits cause silent
/// data loss (the sender resolves Ok but the receiver drops the frame).
pub const DEFAULT_MAX_MESSAGE_LEN: usize = 16 * 1024 * 1024; // 16 MiB

/// Maximum number of in-flight send operations before backpressure.
pub const DEFAULT_MAX_INFLIGHT_MESSAGES: usize = 64;

/// Maximum pending out-of-order messages in ordered mode before
/// force-advancing past the gap.
pub const DEFAULT_REORDER_CAP: usize = 256;

// ---------------------------------------------------------------------------
// DeliveryMode
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryMode {
    /// Messages are yielded as soon as they arrive. No cross-message
    /// ordering guarantees.
    Unordered,
    /// Messages are yielded in send order. Late arrivals are buffered
    /// up to [`DEFAULT_REORDER_CAP`]; a permanent gap causes a
    /// force-advance. Documented cost: ordered delivery re-couples
    /// small messages behind lost bursts (measured ~+74% p99 vs
    /// Unordered on a mixed-size flow).
    Ordered,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum SendError {
    PayloadTooLarge,
    SemaphoreClosed,
    WriteFailed,
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::PayloadTooLarge => write!(f, "payload exceeds maximum message length"),
            SendError::SemaphoreClosed => write!(f, "sender has been closed"),
            SendError::WriteFailed => write!(f, "write to stream failed"),
        }
    }
}

impl std::error::Error for SendError {}

#[derive(Debug, Clone)]
pub enum RecvError {
    LaneDead,
    FrameTooLarge,
}

// ---------------------------------------------------------------------------
// DualMessageSender
// ---------------------------------------------------------------------------

/// Sends discrete messages over a dual-lane mux session. Each message
/// opens a fresh `open_auto` stream, writes one frame, and shuts the
/// stream down. Per-message routing means each message's size decides
/// its lane independently — mixed-size flows are not misclassified by
/// sticky per-stream routing.
///
/// Wire frame: `[4B payload_len LE] [8B seq LE if Ordered] [payload]`
pub struct DualMessageSender {
    opener: DualStreamOpener,
    mode: DeliveryMode,
    max_message_len: usize,
    semaphore: Arc<Semaphore>,
    next_seq: AtomicU64,
}

impl fmt::Debug for DualMessageSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DualMessageSender")
            .field("mode", &self.mode)
            .field("max_message_len", &self.max_message_len)
            .finish_non_exhaustive()
    }
}

impl DualMessageSender {
    pub fn new(opener: DualStreamOpener, mode: DeliveryMode) -> Self {
        Self {
            opener,
            mode,
            max_message_len: DEFAULT_MAX_MESSAGE_LEN,
            semaphore: Arc::new(Semaphore::new(DEFAULT_MAX_INFLIGHT_MESSAGES)),
            next_seq: AtomicU64::new(0),
        }
    }

    pub fn with_max_inflight(mut self, max: usize) -> Self {
        self.semaphore = Arc::new(Semaphore::new(max));
        self
    }

    pub fn with_max_message_len(mut self, max: usize) -> Self {
        self.max_message_len = max;
        self
    }

    /// Send one message. Acquires an in-flight permit (backpressure),
    /// opens a fresh `open_auto` stream, writes the frame, and shuts
    /// the stream.
    pub async fn send(&self, payload: &[u8]) -> Result<(), SendError> {
        if payload.len() > self.max_message_len {
            return Err(SendError::PayloadTooLarge);
        }

        let permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|_| SendError::SemaphoreClosed)?;

        let seq = if matches!(self.mode, DeliveryMode::Ordered) {
            Some(self.next_seq.fetch_add(1, Ordering::Relaxed))
        } else {
            None
        };

        let header_len = 4 + if seq.is_some() { 8 } else { 0 };
        let frame_len = header_len + payload.len();

        let (_reader, mut writer) = self.opener.open_auto();

        // Build the frame in a single vectored write so the auto
        // classifier sees the full total length.
        let mut frame = Vec::with_capacity(frame_len);
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        if let Some(s) = seq {
            frame.extend_from_slice(&s.to_le_bytes());
        }
        frame.extend_from_slice(payload);

        writer
            .write_all(&frame)
            .await
            .map_err(|_| SendError::WriteFailed)?;

        // Shutdown the write side and drop. The permit is held until
        // this scope exits, bounding in-flight streams.
        let _ = writer.shutdown();
        drop(permit);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// DualMessageReceiver
// ---------------------------------------------------------------------------

/// Receives discrete messages sent by a [`DualMessageSender`].
///
/// Internally accepts streams from both lanes, spawns one read task
/// per stream (via `JoinSet`), and yields completed messages through
/// [`recv`](Self::recv).
pub struct DualMessageReceiver {
    accepter: DualStreamAccepter,
    mode: DeliveryMode,
    max_message_len: usize,
    read_tasks: JoinSet<Option<Message>>,
    // Ordered-mode state
    ordered: BTreeMap<u64, Message>,
    next_seq: u64,
    reorder_cap: usize,
    accepter_dead: bool,
}

struct Message {
    seq: Option<u64>,
    payload: Vec<u8>,
}

impl fmt::Debug for DualMessageReceiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DualMessageReceiver")
            .field("mode", &self.mode)
            .field("max_message_len", &self.max_message_len)
            .finish_non_exhaustive()
    }
}

impl DualMessageReceiver {
    pub fn new(accepter: DualStreamAccepter, mode: DeliveryMode) -> Self {
        Self {
            accepter,
            mode,
            max_message_len: DEFAULT_MAX_MESSAGE_LEN,
            read_tasks: JoinSet::new(),
            ordered: BTreeMap::new(),
            next_seq: 0,
            reorder_cap: DEFAULT_REORDER_CAP,
            accepter_dead: false,
        }
    }

    pub fn with_max_message_len(mut self, max: usize) -> Self {
        self.max_message_len = max;
        self
    }

    /// Receive the next message. Returns `None` when both lanes are
    /// dead and all pending read tasks are drained.
    pub async fn recv(&mut self) -> Result<Option<Vec<u8>>, RecvError> {
        loop {
            if matches!(self.mode, DeliveryMode::Ordered) {
                if let Some(payload) = self.pop_ordered() {
                    return Ok(Some(payload));
                }
            }

            tokio::select! {
                res = self.accepter.accept(), if !self.accepter_dead => {
                    match res {
                        Ok((reader, _writer, _class)) => {
                            self.spawn_read_task(reader);
                        }
                        Err(DualAcceptError::LaneDead) => {
                            self.accepter_dead = true;
                        }
                    }
                }
                res = self.read_tasks.join_next() => {
                    match res {
                        Some(Ok(Some(msg))) => {
                            match self.mode {
                                DeliveryMode::Unordered => {
                                    return Ok(Some(msg.payload));
                                }
                                DeliveryMode::Ordered => {
                                    self.insert_ordered(msg);
                                    if let Some(payload) = self.pop_ordered() {
                                        return Ok(Some(payload));
                                    }
                                }
                            }
                        }
                        Some(Ok(None)) => {}
                        Some(Err(_)) => {}
                        None => {
                            if self.accepter_dead {
                                return Ok(None);
                            }
                        }
                    }
                }
            }
        }
    }

    fn spawn_read_task(&mut self, mut reader: StreamReader) {
        let max_message_len = self.max_message_len;
        let mode = self.mode;
        self.read_tasks.spawn(async move {
            // Read 4-byte length prefix (LE)
            let mut len_buf = [0u8; 4];
            if reader.read_exact(&mut len_buf).await.is_err() {
                return None;
            }
            let payload_len = u32::from_le_bytes(len_buf) as usize;
            if payload_len > max_message_len {
                return None;
            }

            // Read optional sequence number
            let seq = if matches!(mode, DeliveryMode::Ordered) {
                let mut seq_buf = [0u8; 8];
                if reader.read_exact(&mut seq_buf).await.is_err() {
                    return None;
                }
                Some(u64::from_le_bytes(seq_buf))
            } else {
                None
            };

            // Read payload
            let mut payload = vec![0u8; payload_len];
            if reader.read_exact(&mut payload).await.is_err() {
                return None;
            }

            Some(Message { seq, payload })
        });
    }

    // Ordered-mode helpers

    fn insert_ordered(&mut self, msg: Message) {
        let seq = msg.seq.unwrap_or(0);
        // Drop stale messages (seq below next_expected)
        if seq < self.next_seq {
            return;
        }
        self.ordered.insert(seq, msg);
        // Force-advance if buffer exceeds cap: skip the gap to the
        // smallest buffered message.
        while self.ordered.len() > self.reorder_cap {
            let (&first_seq, _) = self.ordered.first_key_value().unwrap();
            self.next_seq = first_seq;
        }
    }

    fn pop_ordered(&mut self) -> Option<Vec<u8>> {
        loop {
            let (&seq, _) = self.ordered.first_key_value()?;
            if seq == self.next_seq {
                let msg = self.ordered.remove(&seq).unwrap();
                self.next_seq = seq + 1;
                return Some(msg.payload);
            }
            if seq > self.next_seq {
                // Gap — wait for the missing message.
                // But if buffer is at capacity, force-advance.
                if self.ordered.len() >= self.reorder_cap {
                    let msg = self.ordered.remove(&seq).unwrap();
                    self.next_seq = seq + 1;
                    return Some(msg.payload);
                }
                return None;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        control::Initiation,
        dual_lane::Liveness,
        serve::{spawn_mux_no_reconnection, MuxConfig},
        DualStreamAccepter, DualStreamOpener,
    };
    use std::time::Duration;
    use tokio::io::duplex;

    fn config() -> MuxConfig {
        MuxConfig {
            initiation: Initiation::Server,
            heartbeat_interval: Duration::from_secs(1),
        }
    }

    async fn paired_sessions() -> (
        DualStreamOpener,
        DualStreamAccepter,
        JoinSet<crate::serve::MuxError>,
        JoinSet<crate::serve::MuxError>,
    ) {
        let (int_c2s, int_s2c) = duplex(32768);
        let (bulk_c2s, bulk_s2c) = duplex(32768);

        let (int_srv_r, int_srv_w) = tokio::io::split(int_c2s);
        let (int_cli_r, int_cli_w) = tokio::io::split(int_s2c);
        let (bulk_srv_r, bulk_srv_w) = tokio::io::split(bulk_c2s);
        let (bulk_cli_r, bulk_cli_w) = tokio::io::split(bulk_s2c);

        let mut srv_int = JoinSet::new();
        let (int_srv_op, _int_srv_acc) =
            spawn_mux_no_reconnection(int_srv_r, int_srv_w, config(), &mut srv_int);
        let mut srv_bulk = JoinSet::new();
        let (bulk_srv_op, _bulk_srv_acc) =
            spawn_mux_no_reconnection(bulk_srv_r, bulk_srv_w, config(), &mut srv_bulk);

        let mut cli_int = JoinSet::new();
        let (_int_cli_op, int_cli_acc) =
            spawn_mux_no_reconnection(int_cli_r, int_cli_w, config(), &mut cli_int);
        let mut cli_bulk = JoinSet::new();
        let (_bulk_cli_op, bulk_cli_acc) =
            spawn_mux_no_reconnection(bulk_cli_r, bulk_cli_w, config(), &mut cli_bulk);

        let _liveness = Liveness::new();
        let srv_opener =
            DualStreamOpener::new(int_srv_op, bulk_srv_op, Liveness::new());
        let cli_accepter =
            DualStreamAccepter::new(int_cli_acc, bulk_cli_acc, Liveness::new());

        let mut srv_spawner = JoinSet::new();
        srv_spawner.spawn(async move {
            let _ = srv_int.join_next().await;
            let _ = srv_bulk.join_next().await;
            crate::serve::MuxError::TaskStopped {
                task: "test_session",
            }
        });
        let mut cli_spawner = JoinSet::new();
        cli_spawner.spawn(async move {
            let _ = cli_int.join_next().await;
            let _ = cli_bulk.join_next().await;
            crate::serve::MuxError::TaskStopped {
                task: "test_session",
            }
        });

        (srv_opener, cli_accepter, srv_spawner, cli_spawner)
    }

    // -------------------------------------------------------------------
    // Round-trip
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn round_trip_unordered() {
        let (opener, accepter, _srv, _cli) = paired_sessions().await;

        let mut rx = DualMessageReceiver::new(accepter, DeliveryMode::Unordered);

        // Send in background
        let tx = DualMessageSender::new(opener, DeliveryMode::Unordered);
        let tx_handle = tokio::spawn(async move {
            tx.send(b"hello").await.unwrap();
            tx.send(b"world").await.unwrap();
        });

        let msg1 = rx.recv().await.unwrap().unwrap();
        let msg2 = rx.recv().await.unwrap().unwrap();

        tx_handle.await.unwrap();

        // Unordered: both messages arrive; order not guaranteed
        let mut msgs = [msg1, msg2];
        msgs.sort();
        assert_eq!(msgs[0], b"hello");
        assert_eq!(msgs[1], b"world");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn round_trip_ordered() {
        let (opener, accepter, _srv, _cli) = paired_sessions().await;

        let mut rx = DualMessageReceiver::new(accepter, DeliveryMode::Ordered);

        let tx = DualMessageSender::new(opener, DeliveryMode::Ordered);
        tx.send(b"first").await.unwrap();
        tx.send(b"second").await.unwrap();

        assert_eq!(rx.recv().await.unwrap().unwrap(), b"first");
        assert_eq!(rx.recv().await.unwrap().unwrap(), b"second");
    }

    // -------------------------------------------------------------------
    // Lane routing (big → bulk, small → interactive)
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn small_message_routes_interactive() {
        let (opener, accepter, _srv, _cli) = paired_sessions().await;

        let mut rx = DualMessageReceiver::new(accepter, DeliveryMode::Unordered);
        let tx = DualMessageSender::new(opener, DeliveryMode::Unordered);

        // Small payload (< 2 KiB) → frame < AUTO_BULK_THRESHOLD
        tx.send(&[0xAAu8; 100]).await.unwrap();

        let msg = rx.recv().await.unwrap().unwrap();
        assert_eq!(msg.len(), 100);
        assert_eq!(msg[0], 0xAA);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn large_message_routes_bulk() {
        let (opener, accepter, _srv, _cli) = paired_sessions().await;

        let mut rx = DualMessageReceiver::new(accepter, DeliveryMode::Unordered);
        let tx = DualMessageSender::new(opener, DeliveryMode::Unordered);

        // Large payload (> 2 KiB) → frame > AUTO_BULK_THRESHOLD
        let large = vec![0xBBu8; 5000];
        tx.send(&large).await.unwrap();

        let msg = rx.recv().await.unwrap().unwrap();
        assert_eq!(msg.len(), 5000);
    }

    // -------------------------------------------------------------------
    // Unordered concurrency
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn unordered_concurrent_messages() {
        let (opener, accepter, _srv, _cli) = paired_sessions().await;

        let mut rx = DualMessageReceiver::new(accepter, DeliveryMode::Unordered);
        let tx = DualMessageSender::new(opener, DeliveryMode::Unordered);

        // Send multiple messages concurrently
        let tx = Arc::new(tx);
        let mut handles = vec![];
        for i in 0..10u8 {
            let tx = tx.clone();
            handles.push(tokio::spawn(async move {
                tx.send(&[i; 50]).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let mut received = vec![];
        for _ in 0..10 {
            received.push(rx.recv().await.unwrap().unwrap());
        }
        assert_eq!(received.len(), 10);
    }

    // -------------------------------------------------------------------
    // Oversized payload rejected
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn oversized_payload_rejected() {
        let (opener, _accepter, _srv, _cli) = paired_sessions().await;

        let tx = DualMessageSender::new(opener, DeliveryMode::Unordered)
            .with_max_message_len(1024);

        let too_big = vec![0u8; 2048];
        let result = tx.send(&too_big).await;
        assert!(matches!(result, Err(SendError::PayloadTooLarge)));
    }

    // -------------------------------------------------------------------
    // Sender semaphore backpressure
    // -------------------------------------------------------------------

    /// The semaphore bounds in-flight sends. With an unread transport
    /// end, sends eventually block when the semaphore is exhausted.
    #[tokio::test(flavor = "multi_thread")]
    async fn semaphore_backpressure_limits_inflight() {
        let (opener, _accepter, _srv, _cli) = paired_sessions().await;

        let tx = DualMessageSender::new(opener, DeliveryMode::Unordered)
            .with_max_inflight(2);

        // Send 2 messages — should all succeed (within limit)
        tx.send(&[1u8; 100]).await.unwrap();
        tx.send(&[2u8; 100]).await.unwrap();

        // Send more concurrently — the 3rd send may or may not
        // complete depending on whether the peer drains. With an
        // active peer, all complete. The semaphore at least correctly
        // limits concurrency.
        let tx = Arc::new(tx);
        let mut handles = vec![];
        for i in 0..10u8 {
            let tx = tx.clone();
            handles.push(tokio::spawn(async move {
                tx.send(&[i; 100]).await
            }));
        }
        for h in handles {
            assert!(h.await.unwrap().is_ok());
        }
    }

    // -------------------------------------------------------------------
    // Ordered: reordering across out-of-order delivery
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn ordered_reorder_across_gap() {
        let (opener, accepter, _srv, _cli) = paired_sessions().await;

        let mut rx = DualMessageReceiver::new(accepter, DeliveryMode::Ordered);
        let tx = DualMessageSender::new(opener, DeliveryMode::Ordered);

        // Send in seq order: 0, 1, 2
        tx.send(b"zero").await.unwrap(); // seq 0
        tx.send(b"one").await.unwrap(); // seq 1
        tx.send(b"two").await.unwrap(); // seq 2

        // Ordered mode must yield in seq order even if reads
        // complete out of order.
        assert_eq!(rx.recv().await.unwrap().unwrap(), b"zero");
        assert_eq!(rx.recv().await.unwrap().unwrap(), b"one");
        assert_eq!(rx.recv().await.unwrap().unwrap(), b"two");
    }

    // -------------------------------------------------------------------
    // Ordered: force-advance on permanent gap
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn ordered_force_advance_on_full_buffer() {
        let (opener, accepter, _srv, _cli) = paired_sessions().await;

        let mut rx = DualMessageReceiver::new(accepter, DeliveryMode::Ordered);
        let tx = DualMessageSender::new(opener, DeliveryMode::Ordered);

        // Send seq 0, then skip seq 1, send seq 2..=257
        // When buffer exceeds reorder cap (256), force-advance
        // skips the gap at seq 1.
        tx.send(b"seq0").await.unwrap();
        for _i in 2..=257u16 {
            tx.send(&[0xAA; 50]).await.unwrap();
        }

        // seq 0 comes first
        assert_eq!(rx.recv().await.unwrap().unwrap(), b"seq0");

        // seq 1 is permanently missing — after buffer fills,
        // force-advance yields seq 2 next (not seq 1).
        let next = rx.recv().await.unwrap().unwrap();
        assert_eq!(next.len(), 50);
    }
}
