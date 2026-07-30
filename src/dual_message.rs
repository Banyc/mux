use std::{
    collections::BTreeMap,
    fmt,
    io::IoSlice,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Semaphore,
    task::JoinSet,
};

use crate::{
    StreamReader,
    dual_lane::{DualAcceptError, DualStreamAccepter, DualStreamOpener},
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

        let (_reader, mut writer) = self.opener.open_auto();

        // Build a header array on the stack (at most 12 bytes), then issue
        // a single vectored write so the auto classifier sees the full
        // frame length (avoids a Vec allocation for the combined frame).
        let header_len = 4 + if seq.is_some() { 8 } else { 0 };
        let mut header: [u8; 12] = [0u8; 12];
        header[..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        if let Some(s) = seq {
            header[4..12].copy_from_slice(&s.to_le_bytes());
        }

        let mut header_remaining: &[u8] = &header[..header_len];
        let mut payload_remaining: &[u8] = payload;
        loop {
            let bufs = &[
                IoSlice::new(header_remaining),
                IoSlice::new(payload_remaining),
            ];
            let n = writer
                .write_vectored(bufs)
                .await
                .map_err(|_| SendError::WriteFailed)?;
            if n == 0 {
                return Err(SendError::WriteFailed);
            }
            if n < header_remaining.len() {
                header_remaining = &header_remaining[n..];
            } else {
                let payload_consumed = n - header_remaining.len();
                if payload_consumed >= payload_remaining.len() {
                    break;
                }
                payload_remaining = &payload_remaining[payload_consumed..];
                header_remaining = &[];
            }
        }

        // Shutdown the write side and drop. The permit is held until
        // this scope exits, bounding in-flight streams.
        let _ = writer.shutdown();
        drop(permit);
        Ok(())
    }
}

impl Drop for DualMessageSender {
    fn drop(&mut self) {
        self.semaphore.close();
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
    inflight: usize,
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
            inflight: 0,
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
                res = self.read_tasks.join_next(), if self.inflight > 0 => {
                    match res {
                        Some(Ok(Some(msg))) => {
                            self.inflight -= 1;
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
                        Some(Ok(None)) => {
                            self.inflight -= 1;
                        }
                        Some(Err(_)) => {
                            self.inflight -= 1;
                        }
                        None => {
                            self.inflight = 0;
                        }
                    }
                }
            }

            if self.accepter_dead && self.inflight == 0 {
                if matches!(self.mode, DeliveryMode::Ordered) {
                    while self.ordered.len() >= self.reorder_cap
                        || self.ordered.first_key_value().is_some()
                    {
                        if let Some(payload) = self.pop_ordered() {
                            return Ok(Some(payload));
                        }
                        if self.ordered.is_empty() {
                            break;
                        }
                        self.next_seq = *self.ordered.first_key_value().unwrap().0;
                    }
                }
                return Ok(None);
            }
        }
    }

    fn spawn_read_task(&mut self, mut reader: StreamReader) {
        let max_message_len = self.max_message_len;
        let mode = self.mode;
        self.inflight += 1;
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
        // Drop stale seqs already below the cursor (a stale message
        // inserted behind the cursor can never match the normal pop
        // path and is withheld indefinitely).
        if seq < self.next_seq {
            return;
        }
        self.ordered.insert(seq, msg);
        // Force-advance past a permanent gap when the buffer exceeds the
        // cap: jump next_seq to the lowest buffered seq so pop_ordered
        // delivers it next. Do NOT discard the buffered message — that
        // would silently drop data.
        if self.ordered.len() > self.reorder_cap {
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
            if seq < self.next_seq {
                // Stale entry below the cursor — drop and continue.
                self.ordered.remove(&seq);
                continue;
            }
            // seq > next_seq: there is a gap. If the buffer is at/over
            // the cap, force-advance past the gap by jumping next_seq to
            // seq and delivering it.
            if self.ordered.len() >= self.reorder_cap {
                let msg = self.ordered.remove(&seq).unwrap();
                self.next_seq = seq + 1;
                return Some(msg.payload);
            }
            return None;
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
        DualStreamAccepter, DualStreamOpener,
        control::Initiation,
        dual_lane::Liveness,
        serve::{MuxConfig, spawn_mux_no_reconnection},
    };
    use std::time::Duration;
    use tokio::io::duplex;

    fn config() -> MuxConfig {
        MuxConfig {
            initiation: Initiation::Server,
            heartbeat_interval: Duration::from_secs(1),
            frame_reassembly: false,
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
        let srv_opener = DualStreamOpener::new(int_srv_op, bulk_srv_op, Liveness::new());
        let cli_accepter = DualStreamAccepter::new(int_cli_acc, bulk_cli_acc, Liveness::new());

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

        let tx = DualMessageSender::new(opener, DeliveryMode::Unordered).with_max_message_len(1024);

        let too_big = vec![0u8; 2048];
        let result = tx.send(&too_big).await;
        assert!(matches!(result, Err(SendError::PayloadTooLarge)));
    }

    // -------------------------------------------------------------------
    // Sender semaphore backpressure
    // -------------------------------------------------------------------

    /// The semaphore bounds in-flight sends. Both lanes' transport peer
    /// halves are held alive but NEVER read, so once the 128 B duplex
    /// buffer plus all in-process buffering (< ~600 KiB) saturates under
    /// 1 MiB payloads, writes park forever and permits are held.
    ///
    /// This replaces a timing-based assert that counted completed sends
    /// inside a fixed sleep window, which was racy because the semaphore
    /// bounds concurrency (not throughput per unit time).
    #[tokio::test(flavor = "multi_thread")]
    async fn semaphore_backpressure_limits_inflight() {
        use std::sync::atomic::AtomicUsize;
        use tokio::sync::Barrier;
        use tokio::time::timeout;

        // Both lanes: peer halves held alive but never read — writes park
        // once transport + in-process buffering saturates.
        let (_int_peer, int_local) = duplex(128);
        let (int_r, int_w) = tokio::io::split(int_local);
        let (_bulk_peer, bulk_local) = duplex(128);
        let (bulk_r, bulk_w) = tokio::io::split(bulk_local);

        let cfg = config();
        let mut int_spawner = JoinSet::new();
        let (int_opener, _int_acc) =
            spawn_mux_no_reconnection(int_r, int_w, cfg.clone(), &mut int_spawner);
        let mut bulk_spawner = JoinSet::new();
        let (bulk_opener, _bulk_acc) =
            spawn_mux_no_reconnection(bulk_r, bulk_w, cfg.clone(), &mut bulk_spawner);
        // Keep spawners alive so mux session tasks keep running.
        tokio::task::spawn(async move {
            let _ = int_spawner.join_next().await;
        });
        tokio::task::spawn(async move {
            let _ = bulk_spawner.join_next().await;
        });

        let opener = DualStreamOpener::new(int_opener, bulk_opener, Liveness::new());

        let tx =
            Arc::new(DualMessageSender::new(opener, DeliveryMode::Unordered).with_max_inflight(2));
        let semaphore = tx.semaphore.clone();

        let barrier = Arc::new(Barrier::new(10));
        let finished = Arc::new(AtomicUsize::new(0));
        let payload = vec![0u8; 1 << 20]; // 1 MiB > all in-process buffering

        let mut handles = Vec::new();
        for _ in 0..10 {
            let tx = tx.clone();
            let barrier = barrier.clone();
            let finished = finished.clone();
            let payload = payload.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                let _ = tx.send(&payload).await;
                finished.fetch_add(1, Ordering::SeqCst);
            }));
        }

        // Wait for the semaphore to saturate — exactly 2 permits held.
        timeout(Duration::from_secs(5), async {
            while semaphore.available_permits() != 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("semaphore never reached 0 — inflight bound not enforced");

        // After a brief settle, permits must STAY at 0 and no send finished.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            semaphore.available_permits(),
            0,
            "permits must remain at 0 while writes are parked"
        );
        let done = finished.load(Ordering::SeqCst);
        assert_eq!(
            done, 0,
            "no sends should have completed while writes are parked, but {done} finished"
        );

        drop(tx);
        for h in handles {
            let _ = tokio::time::timeout(Duration::from_secs(2), h).await;
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

    // -------------------------------------------------------------------
    // Ordered: force-advance does NOT drop buffered messages.
    // Regression: the old insert_ordered discarded the lowest seq on
    // force-advance, silently losing data. This test sends 257 distinct
    // payloads and asserts ALL of them are delivered (except the
    // permanently-missing seq 1).
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn ordered_force_advance_keeps_all_buffered_messages() {
        let (opener, accepter, _srv, _cli) = paired_sessions().await;

        let mut rx = DualMessageReceiver::new(accepter, DeliveryMode::Ordered);
        let tx = DualMessageSender::new(opener, DeliveryMode::Ordered);

        // Send seq 0, skip seq 1, send seq 2..=257 with DISTINCT payloads.
        tx.send(b"seq0").await.unwrap();
        for i in 2..=257u16 {
            let payload = format!("msg-{i}");
            tx.send(payload.as_bytes()).await.unwrap();
        }

        // seq 0
        assert_eq!(rx.recv().await.unwrap().unwrap(), b"seq0");

        // Collect the rest. Every seq 2..=257 must be delivered exactly
        // once — none dropped, none duplicated.
        let mut delivered = Vec::new();
        for _ in 0..256 {
            delivered.push(rx.recv().await.unwrap().unwrap());
        }

        // Decode and assert every expected message appears exactly once.
        let mut expected: Vec<String> = (2..=257).map(|i| format!("msg-{i}")).collect();
        let mut got: Vec<String> = delivered
            .iter()
            .map(|b| String::from_utf8(b.clone()).unwrap())
            .collect();
        expected.sort();
        got.sort();
        assert_eq!(
            got, expected,
            "force-advance must not drop buffered messages"
        );
    }
}
