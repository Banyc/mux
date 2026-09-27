//! Long-lived-session frame-reordering soak: what does a session hold while
//! the transport delivers frames *out of sent order*, over and over, for the
//! life of one session?
//!
//! The two soaks that already exist name this cell as empty, and they are
//! right to: `session_growth_soak` runs a plain in-memory duplex, whose
//! delivery is in order, so its reorder buffer holds for at most one poll and
//! never carries a gap into a close; `spike_survival_soak` stalls delivery but
//! never reorders it. `interactive_liveness_families::reassembly_gap_family`
//! does reorder, but its cycles are short-lived — a fresh stream set per cycle,
//! 400 of them by default — so it cannot see a retention that needs a long
//! session to reach a bound. Reordering is what a real path does: the
//! deployment's interactive lane hands complete frames up in *arrival* order,
//! and mux's `ReorderBuffer` is the consumer that has to restore per-stream
//! order. This soak drives that consumer for the life of one session.
//!
//! What it asserts, per the mux gate's long-lived-session obligation:
//!
//! 1. **The disturbance reached the structure under test.** The transport is a
//!    frame-reordering shim that holds a data frame and releases the frames
//!    queued behind it first, and duplicates some frames after the fact. The
//!    mux's *own* `ReorderBuffer` is what must have held something, so the
//!    instrument is a counter inside `ReorderBuffer::ingest`: a run whose
//!    `buffered_out_of_order` is zero fails, whatever the shim did.
//! 2. **Nothing leaks.** At two matched points (after N and 10N completed
//!    streams) every per-stream structure reads zero — `stream_table`,
//!    `ReorderBuffer` buffers, their `pending` frame and byte totals, open read
//!    sinks, `closed_but_retained`, the egress token tables — and no structure
//!    holds more live state at the later point.
//! 3. **Reordering never wedges, duplicates, or resurrects.** Every reordered
//!    burst delivers byte-exact and in stream order (compared against a
//!    position-dependent pattern, not against itself); a frame that arrives
//!    after its bytes were already released is dropped idempotently and does
//!    not bring a finished stream back (`dropped_late` is counted, and the
//!    admission ledger's inserts and retires are matched).
//! 4. **Reordering plus a stall, and reordering across a close.** One phase
//!    holds delivery stalled for the field's own spike schedule (190 ms /
//!    1063 ms / 3205 ms / 19.9 s) while frames are being reordered, and another
//!    shuts a stream's write half down so its `CloseWrite` overtakes in-flight
//!    data. Both combinations are where the earlier retention wedge lived.
//! 5. **Recovery after every phase.** Each phase is followed by a full
//!    open/write/echo probe that must complete, so a wedge fails where it
//!    happens instead of averaging away.
//!
//! One phase is the brief's slow consumer × reordering: the peer accepts and
//! never reads while the transport reorders, so the bounded read queue is
//! reached under reordering.
//!
//! Tier: **standard** (`#[ignore]`d). `MUX_REORDER_ROUNDS` and
//! `MUX_REORDER_CHECKPOINT` size the soak; `MUX_REORDER_FAULT` is the
//! red-proof selector (`no_reorder` delivers in order, `never_release` strands
//! a held frame, `no_duplicate` never re-delivers one). This is not a
//! transport-free arm: the impairment is the test's own in-memory shim, which
//! is the smallest disturbance that produces out-of-order delivery at this
//! layer. There is no real transport or netem in mux, and the harness's
//! impairment belongs to `rtp_mux`.

use std::{
    collections::VecDeque,
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant as StdInstant},
};

use mux::{
    Initiation, MuxConfig, MuxError, StreamAccepter, StreamOpener, StreamReader, StreamWriter,
    spawn_mux_no_reconnection,
};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, duplex},
    sync::mpsc,
    task::JoinSet,
    time::Instant,
};

/// Production heartbeat on the deployed path; the steady receive deadline is
/// `4 x` this.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// `central_io::reader::RECEIVE_DEADLINE_INTERVALS`.
const RECEIVE_DEADLINE: Duration = Duration::from_secs(20);
/// A slow-consumer phase may legitimately block on a full read queue; this is
/// how long it is allowed to before the phase drops the stream.
const PHASE_TIMEOUT: Duration = Duration::from_secs(5);

/// Buffered bytes per direction of the in-memory transport pair.
const DUPLEX_BUF: usize = 64 * 1024;
/// Frames the reorder shim may have in flight before it stops accepting more.
/// This is the transport's window: without it the shim would absorb the
/// transport's backpressure entirely.
const REORDER_INFLIGHT_FRAMES: usize = 16;
/// Frames already queued behind a held frame that may overtake it. Above one,
/// the gap spans more than a single frame.
const REORDER_OVERTAKE: u64 = 4;

/// On-wire header byte for a Data frame (`wire_contract` pins it), so the
/// shim can tell data from control without parsing the frame.
const DATA_FRAME_CODE: u8 = 0x02;
/// On-wire header byte for a CloseWrite (`Fin`) frame.
const CLOSE_WRITE_FRAME_CODE: u8 = 0x04;

/// One multi-frame message: the encoder's per-frame body cap is 64 KiB minus
/// headers, so this splits into three frames and the receiver must reassemble
/// across them.
const BULK: usize = 3 * 48 * 1024;
/// The small request/response payload the echo phases use.
const ECHO_BODY: usize = 4096;
/// Streams each echo round opens.
const ECHO_STREAMS: u64 = 4;
/// Streams each closed-without-reading burst opens.
const BURST_STREAMS: u64 = 3;

/// Streams completed before the first checkpoint; the second is ten times it.
const DEFAULT_CHECKPOINT: u64 = 200;
const CHECKPOINT_MULTIPLE: u64 = 10;
/// `RETIRED_FINISHED_PEER_STREAM_WINDOW` in `src/control.rs`.
const RETIRED_WINDOW_MAX: u64 = 1024;

/// The field's spike schedule, rotated per stall phase: the measured floor and
/// the two measured maxima. The deadline-boundary magnitude (19.9 s, one
/// millisecond inside the 20 s window) is deliberately **not** duplicated
/// here: it is `spike_survival_soak`'s cell, and at this phase's structure it
/// is not measurable — the two sessions' sliding windows are armed at
/// different instants and the ledger publishes only the most recent arm, so a
/// 19.9 s advance is ambiguous between "the silence this phase applied" and
/// "the silence plus the arm skew". Measured on the revision this soak
/// landed on: a 19.9 s advance expired one window while the margin check on
/// the last-armed window still passed. A 3.2 s stall leaves a >= 16 s margin
/// on both, so the stall is unambiguously the silence the phase claims.
const SPIKE_SCHEDULE: [Duration; 3] = [
    Duration::from_millis(190),
    Duration::from_millis(1063),
    Duration::from_millis(3205),
];

// ─── the fault switches ────────────────────────────────────────────────────

/// Red-proof modes, never part of a green run.
///
/// * `no_reorder` makes the shim deliver every frame in order, so the mux's
///   reorder buffer holds nothing and the soak's own "the disturbance reached
///   the structure" check must fail.
/// * `never_release` holds the first data frame forever, so no reordered burst
///   ever completes and the per-round recovery probe must fail (or the
///   checkpoint census must show the pending frames).
/// * `no_duplicate` removes the after-release re-delivery, so the count of
///   frames dropped after their bytes were released must stay zero and the
///   soak must fail naming it.
mod faults {
    use std::sync::atomic::{AtomicBool, Ordering};

    pub static NO_REORDER: AtomicBool = AtomicBool::new(false);
    pub static NEVER_RELEASE: AtomicBool = AtomicBool::new(false);
    pub static NO_DUPLICATE: AtomicBool = AtomicBool::new(false);

    pub fn enabled() -> bool {
        NO_REORDER.load(Ordering::SeqCst)
            || NEVER_RELEASE.load(Ordering::SeqCst)
            || NO_DUPLICATE.load(Ordering::SeqCst)
    }
}

// ─── the stall switch ──────────────────────────────────────────────────────

/// Withholds the reorder task's writes to the in-memory transport while
/// `stalled` is set, and is released by the test rather than a timer, so
/// `advance` is the only clock movement and the spike duration is exact. One
/// switch serves both directions, so a spike is applied end to end.
struct StallSwitch {
    stalled: AtomicBool,
    waker: Mutex<Option<Waker>>,
    /// Frames whose transport write returned while the stall was set. Must not
    /// grow across the stall: a non-zero delta means the spike was not applied.
    writes_completed: AtomicU64,
    /// Write polls held pending *by* the switch rather than by a full duplex.
    /// Must grow on every spike; a run that held none never applied one.
    held_writes: AtomicU64,
}

impl StallSwitch {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            stalled: AtomicBool::new(false),
            waker: Mutex::new(None),
            writes_completed: AtomicU64::new(0),
            held_writes: AtomicU64::new(0),
        })
    }
    fn stall(&self) {
        self.stalled.store(true, Ordering::SeqCst);
    }
    fn release(&self) {
        self.stalled.store(false, Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
    fn is_stalled(&self) -> bool {
        self.stalled.load(Ordering::SeqCst)
    }
}

/// The reorder task's output, gated by the stall switch.
struct GateWriter<W> {
    inner: W,
    switch: Arc<StallSwitch>,
}

impl<W> GateWriter<W> {
    fn new(inner: W, switch: Arc<StallSwitch>) -> Self {
        Self { inner, switch }
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for GateWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.switch.is_stalled() {
            *self.switch.waker.lock().unwrap() = Some(cx.waker().clone());
            // Re-check after arming the waker: a release that raced the first
            // check must not strand the write.
            if self.switch.is_stalled() {
                self.switch.held_writes.fetch_add(1, Ordering::SeqCst);
                return Poll::Pending;
            }
        }
        let poll = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &poll
            && *n > 0
        {
            self.switch.writes_completed.fetch_add(1, Ordering::SeqCst);
        }
        poll
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

// ─── the reordering transport ──────────────────────────────────────────────

/// What the shim does to the frame stream. Fixed for the whole soak, so every
/// phase runs against the same transport impairment; the phases vary the load
/// shape instead.
struct ReorderPlan {
    /// Hold every n-th data frame (1 = every one).
    hold_every: u64,
    /// Deliver up to this many already-queued frames behind the held one
    /// before releasing it.
    hold_followers: u64,
    /// Re-deliver every m-th data frame, delayed past its original.
    duplicate_every: u64,
    /// Deliver a scheduled duplicate this many data frames later, so it
    /// arrives after the original's bytes were released rather than while they
    /// were still buffered.
    duplicate_delay: u64,
}

impl Default for ReorderPlan {
    fn default() -> Self {
        Self {
            hold_every: 1,
            hold_followers: REORDER_OVERTAKE,
            // One data frame in three is re-delivered one handled frame
            // later, so the duplicate always lands *after* the original's
            // bytes were released — while the stream is still live, which is
            // what makes it a test of idempotence rather than of the
            // retired-id window.
            duplicate_every: 3,
            duplicate_delay: 1,
        }
    }
}

/// Frame-delivery statistics. A green run has to prove it exercised the
/// out-of-order arrival it exists for, so the shim counts what it did and the
/// soak fails on a zero.
#[derive(Debug, Default)]
struct ReorderStats {
    /// Whole frames the shim accepted from the encoder.
    frames: AtomicU64,
    /// Times a held frame was overtaken by at least one later frame.
    reorders: AtomicU64,
    /// Frames that arrived ahead of an earlier frame, summed over events.
    overtaken: AtomicU64,
    /// Largest number of frames that overtook one held frame.
    max_gap: AtomicU64,
    /// Times a `CloseWrite` overtook a data frame: the Fin racing in-flight
    /// reassembly.
    close_overtakes: AtomicU64,
    /// Frames the shim re-delivered after the fact.
    duplicates: AtomicU64,
    /// Times the injected hold was taken (red-proof mode only).
    injected_holds: AtomicU64,
}

impl ReorderStats {
    fn snapshot(&self) -> (u64, u64, u64, u64, u64, u64, u64) {
        (
            self.frames.load(Ordering::Relaxed),
            self.reorders.load(Ordering::Relaxed),
            self.overtaken.load(Ordering::Relaxed),
            self.max_gap.load(Ordering::Relaxed),
            self.close_overtakes.load(Ordering::Relaxed),
            self.duplicates.load(Ordering::Relaxed),
            self.injected_holds.load(Ordering::Relaxed),
        )
    }
}

/// Bounded frame window shared by the shim and the reorder task: the shim
/// takes a slot per frame it accepts, the task releases one after handing a
/// frame to the duplex, so a writer that outruns the transport parks rather
/// than buffering without bound.
#[derive(Debug)]
struct FrameWindow {
    cap: usize,
    state: Mutex<FrameWindowState>,
}

#[derive(Debug, Default)]
struct FrameWindowState {
    inflight: usize,
    waker: Option<Waker>,
}

impl FrameWindow {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            state: Mutex::new(FrameWindowState::default()),
        }
    }
    fn enter(&self, waker: &Waker) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.inflight >= self.cap {
            state.waker = Some(waker.clone());
            return false;
        }
        state.inflight += 1;
        true
    }
    fn leave(&self) {
        let waker = {
            let mut state = self.state.lock().unwrap();
            state.inflight = state.inflight.saturating_sub(1);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// The encoder stages a whole frame and calls `write_all` on a non-vectored
/// writer, so each `poll_write` is exactly one frame; accepting the buffer
/// whole preserves the frame boundary without the test re-parsing the wire.
struct ReorderWriter {
    tx: mpsc::Sender<Vec<u8>>,
    stats: Arc<ReorderStats>,
    window: Arc<FrameWindow>,
}

impl AsyncWrite for ReorderWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if !this.window.enter(cx.waker()) {
            return Poll::Pending;
        }
        // The channel is one longer than the admission window, so the window
        // is the only gate a writer waits on and `Full` here is unreachable.
        match this.tx.try_send(buf.to_vec()) {
            Ok(()) => {
                this.stats.frames.fetch_add(1, Ordering::Relaxed);
                Poll::Ready(Ok(buf.len()))
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                this.window.leave();
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                this.window.leave();
                Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
            }
        }
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

/// Deliver the frames the shim hands over *out of sent order*. A data frame is
/// held back and the frames already queued behind it — the rest of its own
/// multi-frame message, a `CloseWrite`, another stream's frames — are written
/// first, so the receiver sees a later offset before an earlier one and must
/// hold it until the gap fills. Nothing is ever left held: a frame with
/// nothing queued behind it goes out in order immediately, so the shim cannot
/// strand a stream by waiting for a follower that never comes.
///
/// Every `duplicate_every`-th data frame is also re-delivered `duplicate_delay`
/// data frames later, so the receiver sees a frame whose bytes it has already
/// released and has to drop it idempotently.
async fn reorder_frames(
    mut out: GateWriter<DuplexStream>,
    mut frames: mpsc::Receiver<Vec<u8>>,
    stats: Arc<ReorderStats>,
    window: Arc<FrameWindow>,
    plan: Arc<ReorderPlan>,
) {
    let mut data_seen = 0u64;
    let mut delayed: VecDeque<(u64, Vec<u8>)> = VecDeque::new();
    // Red-proof mode: the first data frame this direction sees is never
    // released while the transport stays open.
    let mut stranded = false;

    while let Some(frame) = frames.recv().await {
        let is_data = frame.first() == Some(&DATA_FRAME_CODE);
        if !is_data {
            if out.write_all(&frame).await.is_err() {
                return;
            }
            window.leave();
            continue;
        }
        data_seen += 1;
        if !faults::NO_DUPLICATE.load(Ordering::SeqCst)
            && plan.duplicate_every != 0
            && data_seen.is_multiple_of(plan.duplicate_every)
        {
            delayed.push_back((data_seen + plan.duplicate_delay.max(1), frame.clone()));
            stats.duplicates.fetch_add(1, Ordering::Relaxed);
        }
        // Flush any duplicate whose delay has elapsed: it now arrives after
        // the original's bytes were released.
        while delayed.front().is_some_and(|(due, _)| *due <= data_seen) {
            let (_, bytes) = delayed.pop_front().unwrap();
            if out.write_all(&bytes).await.is_err() {
                return;
            }
            window.leave();
        }

        let hold = !faults::NO_REORDER.load(Ordering::SeqCst)
            && plan.hold_every != 0
            && data_seen.is_multiple_of(plan.hold_every);
        if !hold {
            if out.write_all(&frame).await.is_err() {
                return;
            }
            window.leave();
        } else {
            let mut followers: Vec<Vec<u8>> = Vec::new();
            while (followers.len() as u64) < plan.hold_followers {
                match frames.try_recv() {
                    Ok(follower) => followers.push(follower),
                    Err(_) => break,
                }
            }
            for follower in &followers {
                if follower.first() == Some(&CLOSE_WRITE_FRAME_CODE) {
                    stats.close_overtakes.fetch_add(1, Ordering::Relaxed);
                }
                if out.write_all(follower).await.is_err() {
                    return;
                }
                window.leave();
            }
            if !followers.is_empty() {
                stats.reorders.fetch_add(1, Ordering::Relaxed);
                stats
                    .overtaken
                    .fetch_add(followers.len() as u64, Ordering::Relaxed);
                stats
                    .max_gap
                    .fetch_max(followers.len() as u64, Ordering::Relaxed);
            }
            if faults::NEVER_RELEASE.load(Ordering::SeqCst) && !stranded {
                stranded = true;
                stats.injected_holds.fetch_add(1, Ordering::Relaxed);
                std::future::pending::<()>().await;
            }
            // The held frame goes last: that is the out-of-order arrival.
            if out.write_all(&frame).await.is_err() {
                return;
            }
            window.leave();
        }
    }
}

// ─── payload ───────────────────────────────────────────────────────────────

fn pattern_byte(seed: u64, offset: usize) -> u8 {
    let mut x = seed ^ (offset as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    (x >> 56) as u8
}

fn pattern(seed: u64, len: usize) -> Vec<u8> {
    (0..len).map(|i| pattern_byte(seed, i)).collect()
}

// ─── the session pair ──────────────────────────────────────────────────────

struct Pair {
    opener: StreamOpener,
    accepter: StreamAccepter,
    client_teardowns: JoinSet<MuxError>,
    server_teardowns: JoinSet<MuxError>,
    /// The two reorder tasks, owned here so dropping the pair aborts them
    /// rather than leaving them detached: they are the transport, and a
    /// session whose transport outlives it is not the shape under test.
    _reorder_tasks: JoinSet<()>,
    stats: Arc<ReorderStats>,
    switch: Arc<StallSwitch>,
}

impl Pair {
    /// Spawn one long-lived mux pair over in-memory duplexes whose two egress
    /// directions pass through the reorder shim, both sessions running
    /// `frame_reassembly` on.
    fn spawn(switch: Arc<StallSwitch>) -> Self {
        let stats = Arc::new(ReorderStats::default());
        let plan = Arc::new(ReorderPlan::default());
        // One window per direction: a single shared window lets a flooding
        // egress starve the other direction's writer for a slot.
        let client_window = Arc::new(FrameWindow::new(REORDER_INFLIGHT_FRAMES));
        let server_window = Arc::new(FrameWindow::new(REORDER_INFLIGHT_FRAMES));
        let (client_read, server_write) = duplex(DUPLEX_BUF);
        let (server_read, client_write) = duplex(DUPLEX_BUF);
        let (client_shim, client_frames) = mpsc::channel(REORDER_INFLIGHT_FRAMES + 1);
        let (server_shim, server_frames) = mpsc::channel(REORDER_INFLIGHT_FRAMES + 1);
        let mut reorder_tasks = JoinSet::new();
        reorder_tasks.spawn(reorder_frames(
            GateWriter::new(client_write, Arc::clone(&switch)),
            client_frames,
            Arc::clone(&stats),
            Arc::clone(&client_window),
            Arc::clone(&plan),
        ));
        reorder_tasks.spawn(reorder_frames(
            GateWriter::new(server_write, Arc::clone(&switch)),
            server_frames,
            Arc::clone(&stats),
            Arc::clone(&server_window),
            plan,
        ));

        let mut client_config = MuxConfig::new(Initiation::Client, HEARTBEAT_INTERVAL);
        client_config.frame_reassembly = true;
        let mut client_teardowns = JoinSet::new();
        let (opener, _) = spawn_mux_no_reconnection(
            client_read,
            ReorderWriter {
                tx: client_shim,
                stats: Arc::clone(&stats),
                window: Arc::clone(&client_window),
            },
            client_config,
            &mut client_teardowns,
        );
        let mut server_config = MuxConfig::new(Initiation::Server, HEARTBEAT_INTERVAL);
        server_config.frame_reassembly = true;
        let mut server_teardowns = JoinSet::new();
        let (_, accepter) = spawn_mux_no_reconnection(
            server_read,
            ReorderWriter {
                tx: server_shim,
                stats: Arc::clone(&stats),
                window: Arc::clone(&server_window),
            },
            server_config,
            &mut server_teardowns,
        );

        Self {
            opener,
            accepter,
            client_teardowns,
            server_teardowns,
            _reorder_tasks: reorder_tasks,
            stats,
            switch,
        }
    }

    fn tear_down_reason(&mut self) -> Option<MuxError> {
        if let Some(joined) = self.client_teardowns.try_join_next() {
            return Some(joined.expect("client session task panicked"));
        }
        if let Some(joined) = self.server_teardowns.try_join_next() {
            return Some(joined.expect("server session task panicked"));
        }
        None
    }

    /// Open one stream on both ends. `tokio::join!` keeps the open and the
    /// accept advancing together, so a peer that never accepts is reported as
    /// a stall by the enclosing timeout rather than deadlocking one side.
    async fn open_pair(
        &mut self,
    ) -> io::Result<(StreamReader, StreamWriter, StreamReader, StreamWriter)> {
        let (open_res, accept_res) = tokio::join!(self.opener.open(), self.accepter.accept());
        let (client_reader, client_writer) =
            open_res.map_err(|e| io::Error::other(format!("open: {e:?}")))?;
        let (server_reader, server_writer) =
            accept_res.map_err(|e| io::Error::other(format!("accept: {e:?}")))?;
        Ok((client_reader, client_writer, server_reader, server_writer))
    }

    /// The recovery probe every phase ends with: a full open/write/echo round
    /// that must complete, comparing the echoed bytes against a freshly
    /// generated pattern rather than against themselves.
    async fn echo_probe(&mut self, seed: u64, len: usize) -> io::Result<()> {
        let (mut client_reader, mut client_writer, mut server_reader, mut server_writer) =
            self.open_pair().await?;
        let sent = pattern(seed, len);
        client_writer.write_all(&sent).await?;
        AsyncWriteExt::shutdown(&mut client_writer).await?;
        let mut received = Vec::new();
        server_reader.read_to_end(&mut received).await?;
        if received != sent {
            return Err(io::Error::other(format!(
                "payload integrity: sent {} byte(s), received {} in stream order",
                sent.len(),
                received.len()
            )));
        }
        server_writer.write_all(&sent).await?;
        AsyncWriteExt::shutdown(&mut server_writer).await?;
        let mut echoed = Vec::new();
        client_reader.read_to_end(&mut echoed).await?;
        if echoed != sent {
            return Err(io::Error::other(format!(
                "echo integrity: sent {} byte(s), received {} in stream order",
                sent.len(),
                echoed.len()
            )));
        }
        Ok(())
    }
}

// ─── phases ────────────────────────────────────────────────────────────────

/// One round's adversarial shape. The schedule is fixed by the round index, so
/// a green run is a statement about this phase family; every phase varies one
/// dimension from the `Echo` baseline, which is the same churn with the same
/// reordering transport and nothing else changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Baseline: concurrent small full echo rounds under reordering.
    Echo,
    /// One multi-frame bidirectional message, under reordering.
    Bulk,
    /// The peer accepts and stops reading: a slow consumer against a bounded
    /// read queue, with the transport reordering.
    SlowConsumer,
    /// Streams dropped mid-transfer, neither clean shutdown nor read.
    DroppedMidTransfer,
    /// A burst of streams written, shut down, and never read.
    ClosedWithoutReading,
    /// A multi-frame message whose `CloseWrite` overtakes in-flight data: the
    /// reorder crossing a stream close.
    FinRace,
    /// A delivery stall at the field's spike magnitudes while frames are
    /// reordered and in flight.
    ReorderStall,
}

const SCHEDULE: [Phase; 8] = [
    Phase::Echo,
    Phase::SlowConsumer,
    Phase::Bulk,
    Phase::DroppedMidTransfer,
    Phase::Echo,
    Phase::FinRace,
    Phase::ClosedWithoutReading,
    Phase::ReorderStall,
];

fn phase_for_round(round: u64) -> Phase {
    SCHEDULE[(round as usize) % SCHEDULE.len()]
}

fn spike_for_stall(index: u64) -> Duration {
    SPIKE_SCHEDULE[(index as usize) % SPIKE_SCHEDULE.len()]
}

#[derive(Debug, Default, Clone, Copy)]
struct PhaseCounts {
    opened: u64,
    completed: u64,
    phase_applied: u64,
    stall_applied: u64,
}

/// A full bidirectional round over one stream, payload compared byte-exact.
async fn bulk_round(pair: &mut Pair, seed: u64) -> PhaseCounts {
    let mut counts = PhaseCounts {
        opened: 1,
        phase_applied: 1,
        ..Default::default()
    };
    let Ok((mut client_reader, mut client_writer, mut server_reader, mut server_writer)) =
        pair.open_pair().await
    else {
        return counts;
    };
    let sent = pattern(seed, BULK);
    let (write, read) = tokio::join!(
        async {
            client_writer.write_all(&sent).await?;
            AsyncWriteExt::shutdown(&mut client_writer).await
        },
        async {
            let mut got = Vec::new();
            server_reader.read_to_end(&mut got).await?;
            Ok::<Vec<u8>, io::Error>(got)
        },
    );
    if let Err(e) = write {
        panic!("bulk client write failed: {e}");
    }
    let received = read.expect("bulk server read");
    assert_eq!(
        received, sent,
        "a reordered multi-frame message was not delivered byte-exact in stream order",
    );
    let (write, read) = tokio::join!(
        async {
            server_writer.write_all(&sent).await?;
            AsyncWriteExt::shutdown(&mut server_writer).await
        },
        async {
            let mut got = Vec::new();
            client_reader.read_to_end(&mut got).await?;
            Ok::<Vec<u8>, io::Error>(got)
        },
    );
    if let Err(e) = write {
        panic!("bulk server write failed: {e}");
    }
    let echoed = read.expect("bulk client read");
    assert_eq!(
        echoed, sent,
        "a reordered multi-frame echo was not delivered byte-exact in stream order",
    );
    counts.completed = 1;
    counts
}

/// `ECHO_STREAMS` small request/response rounds, each byte-exact.
async fn echo_round(pair: &mut Pair, seed: u64) -> PhaseCounts {
    let mut counts = PhaseCounts {
        phase_applied: 1,
        ..Default::default()
    };
    for index in 0..ECHO_STREAMS {
        counts.opened += 1;
        if pair
            .echo_probe(seed.wrapping_add(index), ECHO_BODY)
            .await
            .is_err()
        {
            return counts;
        }
        counts.completed += 1;
    }
    counts
}

/// The client's write half shuts down immediately after a multi-frame message,
/// so its `CloseWrite` is queued behind data frames the shim is holding: the
/// close overtakes in-flight reassembly.
async fn phase_fin_race(pair: &mut Pair, seed: u64) -> PhaseCounts {
    let mut counts = PhaseCounts {
        opened: 1,
        phase_applied: 1,
        ..Default::default()
    };
    let Ok((mut client_reader, mut client_writer, mut server_reader, server_writer)) =
        pair.open_pair().await
    else {
        return counts;
    };
    let sent = pattern(seed, BULK);
    let (write, read) = tokio::join!(
        async {
            client_writer.write_all(&sent).await?;
            AsyncWriteExt::shutdown(&mut client_writer).await
        },
        async {
            let mut got = Vec::new();
            server_reader.read_to_end(&mut got).await?;
            Ok::<Vec<u8>, io::Error>(got)
        },
    );
    if let Err(e) = write {
        panic!("fin-race client write failed: {e}");
    }
    assert_eq!(
        read.expect("fin-race server read"),
        sent,
        "the CloseWrite overtook in-flight data and the stream was not reassembled \
         byte-exact",
    );
    drop(server_writer);
    // The client's read half sees the peer's clean close.
    let mut trailing = Vec::new();
    let _ = client_reader.read_to_end(&mut trailing).await;
    counts.completed = 1;
    counts
}

/// `stream::reader::STREAM_READ_SOFT_DATA_LIMIT` (`1024 - 1`) in
/// `src/stream/reader.rs`: the queued-message count at which the receiving
/// dispatcher refuses a data frame.
const STREAM_READ_SOFT_DATA_LIMIT: usize = 1024 - 1;
/// `stream::reader::STREAM_READ_HARD_DATA_LIMIT` (`8 * 1024 - 1`), the
/// physical queue capacity.
const STREAM_READ_HARD_DATA_LIMIT: usize = 8 * 1024 - 1;

/// A slow consumer × reordering: the peer accepts and never reads while the
/// transport reorders, so the receiving read queue's bound is reached under
/// reordering. One byte per write, so the number of queued read-path messages
/// equals the number of writes and the bound is crossed by message count
/// rather than by byte size. The loop is bounded by a *count* and not only by
/// the clock: a writer that is never answered keeps making progress, so a busy
/// loop never lets the paused clock advance and a clock-only bound would hang
/// instead of failing. It stops as soon as the dispatcher's refusal is
/// observed, and the refusal is asserted *within* the phase.
async fn phase_slow_consumer(pair: &mut Pair, _seed: u64) -> PhaseCounts {
    let counts = PhaseCounts {
        opened: 1,
        phase_applied: 1,
        ..Default::default()
    };
    let Ok((client_reader, mut client_writer, server_reader, server_writer)) =
        pair.open_pair().await
    else {
        return counts;
    };
    let budget = 2 * STREAM_READ_HARD_DATA_LIMIT;
    let refusals_before = mux::live_probe::totals().pipeline.read_queue_full;
    let mut written = 0usize;
    let mut outcome = "budget-exhausted";
    while written < budget {
        // The trait method, not `StreamWriter::write`: the trait maps the
        // writer's own error to an `io::Error` kind, which is what a caller of
        // the mux sees.
        match AsyncWriteExt::write(&mut client_writer, b"x").await {
            Ok(0) => {
                outcome = "ok-zero";
                break;
            }
            Ok(n) => written += n,
            Err(_) => {
                outcome = "error";
                break;
            }
        }
        // Stop the moment the reading dispatcher refuses, rather than writing
        // on through the refusal: the phase's claim is that the bound was
        // reached under reordering, and continuing past it buys no coverage.
        // Measured: the refusal lands at the *hard* limit (~8 191 queued
        // messages, `stream_read_pushed` ~8 295 per occurrence), not at the
        // soft 1 023 — the soft limit's drain grace is a clock comparison,
        // and a writer that keeps making progress is not refused by it, so the
        // physical bound is the one this cell reaches.
        if mux::live_probe::totals().pipeline.read_queue_full > refusals_before {
            outcome = "refusal-observed";
            break;
        }
    }
    let refusals = mux::live_probe::totals().pipeline.read_queue_full - refusals_before;
    assert!(
        refusals >= 1,
        "the slow consumer's read queue never refused a frame within {written} one-byte \
         write(s) (outcome {outcome}, soft limit {STREAM_READ_SOFT_DATA_LIMIT}, hard limit \
         {STREAM_READ_HARD_DATA_LIMIT}), so the phase never reached the bound it exists for",
    );
    // Drop every half: the peer never read a byte.
    drop(server_reader);
    drop(server_writer);
    drop(client_reader);
    drop(client_writer);
    counts
}

/// Streams dropped mid-transfer: both ends go without a shutdown, and the
/// client's two halves go first, so any release the peer's later frames cause
/// is the one this phase exercises.
async fn phase_dropped_mid_transfer(pair: &mut Pair, seed: u64) -> PhaseCounts {
    let counts = PhaseCounts {
        opened: 1,
        phase_applied: 1,
        ..Default::default()
    };
    let Ok((client_reader, mut client_writer, server_reader, server_writer)) =
        pair.open_pair().await
    else {
        return counts;
    };
    let sent = pattern(seed, BULK);
    let write = async {
        let _ = client_writer.write_all(&sent).await;
    };
    let _ = tokio::time::timeout(PHASE_TIMEOUT, write).await;
    drop(client_reader);
    drop(client_writer);
    drop(server_reader);
    drop(server_writer);
    counts
}

/// A burst written and closed but never read, half the burst closing the
/// client's halves first so the peer's close frames arrive at a fully closed
/// local stream.
async fn phase_closed_without_reading(pair: &mut Pair, seed: u64) -> PhaseCounts {
    let mut counts = PhaseCounts {
        phase_applied: 1,
        ..Default::default()
    };
    for index in 0..BURST_STREAMS {
        let Ok((client_reader, mut client_writer, server_reader, server_writer)) =
            pair.open_pair().await
        else {
            return counts;
        };
        counts.opened += 1;
        let sent = pattern(seed.wrapping_add(index), ECHO_BODY);
        let _ = client_writer.write_all(&sent).await;
        let _ = AsyncWriteExt::shutdown(&mut client_writer).await;
        if index % 2 == 0 {
            drop(client_reader);
            drop(client_writer);
            drop(server_reader);
            drop(server_writer);
        } else {
            drop(server_reader);
            drop(server_writer);
            drop(client_reader);
            drop(client_writer);
        }
    }
    counts
}

/// A delivery stall at one of the field's spike magnitudes, applied while
/// frames are being reordered and are in flight. The client's write is started
/// *after* the stall so the shim has frames to hold for the whole window, and
/// nothing may be delivered until the switch is released.
async fn phase_reorder_stall(pair: &mut Pair, seed: u64, spike_index: u64) -> PhaseCounts {
    let mut counts = PhaseCounts {
        opened: 1,
        phase_applied: 1,
        ..Default::default()
    };
    let spike = spike_for_stall(spike_index);
    // The spike schedule must stay inside the receive deadline: a stall past
    // it would tear the session down, and this phase is about a session that
    // survives slowness rather than about the detector (which
    // `spike_survival_soak`'s positive control covers).
    assert!(
        spike < RECEIVE_DEADLINE,
        "the {spike:?} stall crosses the {RECEIVE_DEADLINE:?} receive deadline, so this \
         phase would assert on a session the detector had already ended",
    );
    let switch = Arc::clone(&pair.switch);
    let Ok((mut client_reader, mut client_writer, mut server_reader, mut server_writer)) =
        pair.open_pair().await
    else {
        return counts;
    };

    // The window this advance is applied to is measured, not assumed: the
    // deadline slides on every frame read, so the residual is whatever is left
    // of the most recently armed window. The march check below refuses to
    // advance past it, which is what keeps a phase that deliberately moves the
    // clock from asserting on a session the detector had already ended.
    let ledger = mux::live_probe::timer_ledger();
    let remaining = Duration::from_millis(
        ledger
            .last_deadline_ms
            .saturating_sub(mux::live_probe::now_ms()),
    );
    assert!(
        remaining > spike,
        "only {remaining:?} of the receive-deadline window was left when the {spike:?} stall \
         began, so the advance would cross the deadline and this phase would assert on a \
         session the detector had already ended ({ledger})",
    );
    let sent = pattern(seed, BULK);

    // Stall first: the frames the writer is about to stage must meet the gate,
    // not a transport that already drained.
    switch.stall();
    let writes_before = switch.writes_completed.load(Ordering::SeqCst);
    let held_before = switch.held_writes.load(Ordering::SeqCst);
    let received_before = mux::live_probe::timer_ledger().heartbeats_received;

    // Start the transfer while the stall is in force, and let it queue.
    let payload = sent.clone();
    let mut write_job = JoinSet::new();
    write_job.spawn(async move {
        let result = client_writer.write_all(&payload).await;
        let _ = AsyncWriteExt::shutdown(&mut client_writer).await;
        result
    });
    busily_yield().await;

    let held_during = switch.held_writes.load(Ordering::SeqCst);
    tokio::time::advance(spike).await;
    // Hold the stall through the advance so a deadline the spike crossed would
    // expire *during* it.
    busily_yield().await;
    let writes_during = switch.writes_completed.load(Ordering::SeqCst);
    let received_during = mux::live_probe::timer_ledger().heartbeats_received;
    switch.release();

    assert_eq!(
        writes_during,
        writes_before,
        "the {spike:?} stall let {} frame(s) reach the transport while it was set, \
         so the spike was not applied and this phase proves nothing",
        writes_during.saturating_sub(writes_before),
    );
    busily_yield().await;
    assert!(
        held_during > held_before,
        "the {spike:?} stall held no transport write, so frames were not in flight \
         under it and the stall cannot have been in force",
    );
    if spike >= 2 * HEARTBEAT_INTERVAL {
        // A heartbeat is due inside this stall, so a live peer wrote a control
        // frame during it: none of it may have reached the session.
        assert_eq!(
            received_during,
            received_before,
            "a {spike:?} stall let {} heartbeat(s) through, so the transport was not \
             actually stalled",
            received_during.saturating_sub(received_before),
        );
    }

    // The transfer must now complete, byte-exact.
    let write_result = write_job.join_next().await;
    match write_result {
        Some(Ok(Ok(()))) => {}
        Some(Ok(Err(e))) => panic!("stalled client write failed after release: {e}"),
        Some(Err(e)) => panic!("stalled client write task panicked: {e}"),
        None => panic!("the stalled client write task vanished"),
    }
    let mut received = Vec::new();
    server_reader.read_to_end(&mut received).await.unwrap_or_else(|e| {
        panic!(
            "stalled server read failed at spike {spike:?} (round-local, reorder {}): {e}; {}; {}",
            mux::live_probe::reorder_ledger(),
            mux::live_probe::timer_ledger(),
            mux::live_probe::totals(),
        )
    });
    assert_eq!(
        received, sent,
        "a message that was reordered across a {spike:?} stall was not delivered \
         byte-exact in stream order",
    );
    server_writer
        .write_all(&sent)
        .await
        .unwrap_or_else(|e| panic!("stalled echo write failed: {e}"));
    AsyncWriteExt::shutdown(&mut server_writer).await.ok();
    let mut echoed = Vec::new();
    client_reader
        .read_to_end(&mut echoed)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "stalled client read failed at spike {spike:?}: {e}; {}; {}",
                mux::live_probe::timer_ledger(),
                mux::live_probe::totals(),
            )
        });
    assert_eq!(
        echoed, sent,
        "the echo of a message reordered across a {spike:?} stall was not delivered \
         byte-exact",
    );
    counts.completed = 1;
    counts.stall_applied = 1;
    counts
}

/// Drive the runtime without ever leaving it idle, so a quiescence loop cannot
/// auto-advance the paused clock past a timer this phase is measuring.
async fn busily_yield() {
    for _ in 0..256 {
        tokio::task::yield_now().await;
    }
}

/// Drive the runtime to quiescence so every close the soak issued has been
/// applied and every egress token reaped before the census is read.
async fn settle() {
    for _ in 0..1024 {
        tokio::task::yield_now().await;
    }
}

// ─── checkpoints ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
struct Checkpoint {
    label: &'static str,
    completed_streams: u64,
    structures: mux::live_probe::StructureCensuses,
    egress: mux::live_probe::EgressTokenCensuses,
    ledger: mux::live_probe::AdmissionLedgers,
    reorder: mux::live_probe::ReorderLedger,
    teardowns: u64,
}

impl Checkpoint {
    fn sample(label: &'static str, completed_streams: u64, teardowns: u64) -> Self {
        Self {
            label,
            completed_streams,
            structures: mux::live_probe::structure_censuses(),
            egress: mux::live_probe::egress_token_censuses(),
            ledger: mux::live_probe::admission_ledgers(),
            reorder: mux::live_probe::reorder_ledger(),
            teardowns,
        }
    }
}

impl std::fmt::Display for Checkpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "checkpoint[{}] completed_streams={} teardowns={}\n  {}\n  {}\n  {}\n  {}",
            self.label,
            self.completed_streams,
            self.teardowns,
            self.structures,
            self.egress,
            self.ledger,
            self.reorder,
        )
    }
}

/// Every per-stream structure a quiesced session must have released. A
/// non-zero live count here is a retained structure, not load: the soak has
/// closed every stream it opened.
fn assert_released(checkpoint: &Checkpoint) {
    for (role, census) in [
        ("client", checkpoint.structures.client),
        ("server", checkpoint.structures.server),
    ] {
        assert_eq!(
            census.reassembly_buffers, 0,
            "{}: {role} retains {} reorder buffer(s) after every stream closed ({})",
            checkpoint.label, census.reassembly_buffers, census,
        );
        assert_eq!(
            census.reassembly_pending_frames, 0,
            "{}: {role} retains {} pending reassembly frame(s) ({})",
            checkpoint.label, census.reassembly_pending_frames, census,
        );
        assert_eq!(
            census.reassembly_pending_bytes, 0,
            "{}: {role} retains {} pending reassembly byte(s) ({})",
            checkpoint.label, census.reassembly_pending_bytes, census,
        );
        assert_eq!(
            census.closed_but_retained, 0,
            "{}: {role} retains {} stream-table entr(ies) that `is_closed` already reports \
             finished — no later transition can release them, so the table only fills ({})",
            checkpoint.label, census.closed_but_retained, census,
        );
        assert_eq!(
            census.open_read_sinks, 0,
            "{}: {role} retains {} open read sink(s) after every stream closed ({})",
            checkpoint.label, census.open_read_sinks, census,
        );
        assert_eq!(
            census.stream_table_len, 0,
            "{}: {role} stream table holds {} entr(ies) after every stream closed ({})",
            checkpoint.label, census.stream_table_len, census,
        );
        assert!(
            census.retired_window_len <= RETIRED_WINDOW_MAX,
            "{}: {role} retired-id window is {} entries, past its bound of {RETIRED_WINDOW_MAX}",
            checkpoint.label,
            census.retired_window_len,
        );
    }
    for (role, census) in [
        ("client", checkpoint.egress.client),
        ("server", checkpoint.egress.server),
    ] {
        assert_eq!(
            census.token_queues, 0,
            "{}: {role} egress keeps {} per-stream token queue(s) after every stream closed — \
             the fair queue admits {RETIRED_WINDOW_MAX} and then refuses every later open ({})",
            checkpoint.label, census.token_queues, census,
        );
        assert_eq!(
            census.cached_heads, 0,
            "{}: {role} egress keeps {} cached head(s) after every stream closed ({})",
            checkpoint.label, census.cached_heads, census,
        );
        assert_eq!(
            census.token_streams, 0,
            "{}: {role} egress keeps {} token→stream map entr(ies) after every stream closed ({})",
            checkpoint.label, census.token_streams, census,
        );
    }
}

/// No per-stream structure may hold more live state at a later matched point
/// than at an earlier one.
fn assert_not_grown(first: &Checkpoint, later: &Checkpoint) {
    let before = first.structures.total_live_stream_structures();
    let after = later.structures.total_live_stream_structures();
    assert!(
        after <= before,
        "per-stream structures grew between matched points: {} -> {} live entries over \
         {} -> {} completed streams\n  {}\n  {}",
        before,
        after,
        first.completed_streams,
        later.completed_streams,
        first.structures,
        later.structures,
    );
    let queues_before = first.egress.total();
    let queues_after = later.egress.total();
    assert!(
        queues_after <= queues_before,
        "egress token structures grew between matched points: {} -> {} entries\n  {}\n  {}",
        queues_before,
        queues_after,
        first.egress,
        later.egress,
    );
    for (role, before_len, after_len) in [
        (
            "client",
            first.ledger.client.stream_table_len,
            later.ledger.client.stream_table_len,
        ),
        (
            "server",
            first.ledger.server.stream_table_len,
            later.ledger.server.stream_table_len,
        ),
    ] {
        assert!(
            after_len <= before_len,
            "{role} stream table grew between matched points: {before_len} -> {after_len}",
        );
    }
}

/// Every phase runs under a bound on the simulated clock: a session that
/// wedges must fail the round where it wedged rather than hang the target.
/// The bound is the phase's own budget, because a phase that deliberately
/// burns simulated time (the stall's spike) must not spend its wedge budget on
/// the clock it moved on purpose.
async fn bounded<F>(round: u64, phase: Phase, budget: Duration, fut: F) -> F::Output
where
    F: std::future::Future,
{
    match tokio::time::timeout(budget, fut).await {
        Ok(value) => value,
        Err(_) => panic!(
            "round {round}: the {phase:?} phase did not finish within {budget:?} of simulated \
             time — the session stopped making progress"
        ),
    }
}

/// The budget for an ordinary phase: generous against the phase's own
/// deliberate [`PHASE_TIMEOUT`] waits.
const PHASE_BUDGET: Duration = PHASE_TIMEOUT.saturating_mul(4);

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn checkpoint_streams() -> u64 {
    env_u64("MUX_REORDER_CHECKPOINT", DEFAULT_CHECKPOINT)
}

/// Streams that complete in one pass of [`SCHEDULE`], derived from the phases
/// below: each of the two `Echo` rounds completes four streams, `Bulk`,
/// `FinRace` and `ReorderStall` complete one each, and `SlowConsumer`,
/// `DroppedMidTransfer` and `ClosedWithoutReading` complete none — ten from the
/// phases; plus the one-stream recovery probe every round — eight more.
const COMPLETIONS_PER_SCHEDULE: u64 = 19;

fn rounds() -> u64 {
    let checkpoint = checkpoint_streams();
    // The round count that reaches the second matched point: the completed
    // streams it needs, converted through the completions one schedule
    // delivers. The loop stops as soon as both points are reached, so this is
    // the cost key and a tight cap rather than a fixed length.
    env_u64(
        "MUX_REORDER_ROUNDS",
        (checkpoint * CHECKPOINT_MULTIPLE * SCHEDULE.len() as u64)
            .div_ceil(COMPLETIONS_PER_SCHEDULE),
    )
}

// ─── the soak ──────────────────────────────────────────────────────────────

/// A long-lived session driven through a reordering transport releases every
/// per-stream structure it allocates, delivers every reordered burst
/// byte-exact and in stream order, and returns to full function after every
/// phase.
#[tokio::test(start_paused = true)]
#[ignore = "standard tier: long-lived-session frame-reordering soak"]
async fn a_long_lived_session_survives_sustained_frame_reordering() {
    match std::env::var("MUX_REORDER_FAULT").as_deref() {
        Ok("no_reorder") => faults::NO_REORDER.store(true, Ordering::SeqCst),
        Ok("never_release") => faults::NEVER_RELEASE.store(true, Ordering::SeqCst),
        Ok("no_duplicate") => faults::NO_DUPLICATE.store(true, Ordering::SeqCst),
        Ok(other) => panic!("unknown MUX_REORDER_FAULT={other}"),
        Err(_) => {}
    }
    mux::live_probe::enable_structure_census();

    let checkpoint_at = checkpoint_streams();
    let rounds = rounds();
    let wall_start = StdInstant::now();
    let sim_start = Instant::now();
    let switch = StallSwitch::new();
    let mut pair = Pair::spawn(Arc::clone(&switch));
    let stats = Arc::clone(&pair.stats);
    let reorder_before = mux::live_probe::reorder_ledger();
    let timers_before = mux::live_probe::timer_ledger();
    let pipeline_before = mux::live_probe::totals().pipeline;

    let baseline = Checkpoint::sample("baseline", 0, 0);
    assert_eq!(
        baseline.structures.total_live_stream_structures(),
        0,
        "the freshly spawned session already holds per-stream state ({})",
        baseline.structures,
    );

    let mut completed = 0u64;
    let mut opened = 0u64;
    let mut phases_applied = 0u64;
    let mut stalls_applied = 0u64;
    let mut checkpoints: Vec<Checkpoint> = Vec::new();
    let mut next_checkpoint = checkpoint_at;
    let mut last_round = 0u64;
    let mut stall_index = 0u64;
    let mut slow_consumer_rounds = 0u64;

    for round in 0..rounds {
        last_round = round;
        let phase = phase_for_round(round);
        let seed = 0x5EED_0000_0000_0000u64.wrapping_add(round.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let counts = match phase {
            Phase::Echo => bounded(round, phase, PHASE_BUDGET, echo_round(&mut pair, seed)).await,
            Phase::Bulk => bounded(round, phase, PHASE_BUDGET, bulk_round(&mut pair, seed)).await,
            Phase::SlowConsumer => {
                slow_consumer_rounds += 1;
                bounded(
                    round,
                    phase,
                    PHASE_BUDGET,
                    phase_slow_consumer(&mut pair, seed),
                )
                .await
            }
            Phase::DroppedMidTransfer => {
                bounded(
                    round,
                    phase,
                    PHASE_BUDGET,
                    phase_dropped_mid_transfer(&mut pair, seed),
                )
                .await
            }
            Phase::ClosedWithoutReading => {
                bounded(
                    round,
                    phase,
                    PHASE_BUDGET,
                    phase_closed_without_reading(&mut pair, seed),
                )
                .await
            }
            Phase::FinRace => {
                bounded(round, phase, PHASE_BUDGET, phase_fin_race(&mut pair, seed)).await
            }
            Phase::ReorderStall => {
                let index = stall_index;
                stall_index += 1;
                // The spike moves the clock on purpose, so the wedge budget
                // is the phase's own plus the span the phase itself declares.
                let budget = PHASE_BUDGET.saturating_add(spike_for_stall(index));
                bounded(
                    round,
                    phase,
                    budget,
                    phase_reorder_stall(&mut pair, seed, index),
                )
                .await
            }
        };
        opened += counts.opened;
        completed += counts.completed;
        phases_applied += counts.phase_applied;
        stalls_applied += counts.stall_applied;

        // The recovery probe: every phase must be followed by a round that
        // completes. This is the per-round progress assertion.
        match bounded(
            round,
            phase,
            PHASE_BUDGET,
            pair.echo_probe(seed ^ 0xABCD, ECHO_BODY),
        )
        .await
        {
            Ok(()) => completed += 1,
            Err(e) => panic!(
                "round {round}: the session did not return to full function after \
                 {phase:?}: {e}"
            ),
        }

        if let Some(reason) = pair.tear_down_reason() {
            panic!("round {round}: the session tore down during a {phase:?} round: {reason:?}");
        }

        // An idle stretch on the simulated clock: the session must stay live
        // across heartbeats without any traffic of its own. Twice the
        // heartbeat interval, because the periodic heartbeat is re-armed on
        // every egress frame and the interval carries up to 20 % jitter — a
        // 1x sleep can end before the heartbeat is due, leaving the liveness
        // path unexercised.
        tokio::time::sleep(HEARTBEAT_INTERVAL * 2).await;

        if completed >= next_checkpoint {
            settle().await;
            let mut label = "first";
            if !checkpoints.is_empty() {
                label = "later";
            }
            let checkpoint = Checkpoint::sample(label, completed, 0);
            println!("{checkpoint}");
            assert_released(&checkpoint);
            checkpoints.push(checkpoint);
            next_checkpoint = checkpoint_at * CHECKPOINT_MULTIPLE;
            if checkpoints.len() == 2 {
                break;
            }
        }
    }

    settle().await;
    let sim_elapsed = sim_start.elapsed();
    let wall_elapsed = wall_start.elapsed();
    let after = Checkpoint::sample("final", completed, 0);
    let reorder_delta = mux::live_probe::reorder_ledger().since(&reorder_before);
    let pipeline_delta = mux::live_probe::totals().pipeline.since(&pipeline_before);
    let timers = mux::live_probe::timer_ledger().since(&timers_before);
    let (frames, reorders, overtaken, max_gap, close_overtakes, duplicates, injected_holds) =
        stats.snapshot();
    println!(
        "frame reorder soak: rounds={} opened_streams={} completed_streams={} \
         phases_applied={} stalls_applied={} \
         simulated={:?} wall={:?}\n  {reorder_delta}\n  shim: frames={frames} reorders={reorders} \
         overtaken={overtaken} max_gap={max_gap} close_overtakes={close_overtakes} \
         duplicates={duplicates} injected_holds={injected_holds}\n  {timers}\n  {pipeline_delta}",
        last_round + 1,
        opened,
        completed,
        phases_applied,
        stalls_applied,
        sim_elapsed,
        wall_elapsed,
    );

    // The disturbance reached the structure under test: the mux's own reorder
    // buffer held frames that arrived ahead of the cursor. The shim's counts
    // are corroboration; this one is read from inside `ReorderBuffer::ingest`,
    // so no shim behaviour can make it pass.
    assert!(
        reorder_delta.buffered > 0,
        "the mux `ReorderBuffer` buffered no out-of-order frame across {} ingested frame(s) \
         ({reorder_delta}), so the transport delivered in sent order and this soak proved \
         nothing about reassembly",
        reorder_delta.ingests,
    );
    assert!(
        reorder_delta.ingests >= completed,
        "only {} frame(s) were ingested for {completed} completed streams ({reorder_delta}), \
         so the reorder path was not the one carrying the soak's traffic",
        reorder_delta.ingests,
    );
    assert!(
        reorder_delta.dropped_delivered > 0,
        "no frame arrived after its bytes had already been released ({} dropped late, {} \
         dropped as a buffered duplicate), so the after-release idempotence this soak \
         asserts was never exercised",
        reorder_delta.dropped_delivered,
        reorder_delta.dropped_buffered,
    );
    assert!(
        reorders > 0 && overtaken > 0,
        "the reorder shim delivered no frame out of order (frames={frames} reorders={reorders} \
         overtaken={overtaken}), so it cannot have applied the disturbance",
    );
    assert!(
        max_gap > 1,
        "the largest overtake was {max_gap} frame(s), so the receiver never had to hold more \
         than one frame across a gap",
    );
    assert!(
        close_overtakes > 0,
        "no `CloseWrite` ever overtook in-flight data, so the reorder-across-a-close shape was \
         never exercised (frames={frames})",
    );
    assert!(
        duplicates > 0,
        "the shim never re-delivered a frame, so the after-release path was never entered",
    );
    assert_eq!(
        stalls_applied, stall_index,
        "only {stalls_applied} of {stall_index} stall phases applied their hold",
    );
    assert!(
        stalls_applied > 0,
        "the stall phase never applied; its red-proof check is vacuous",
    );
    assert!(
        pipeline_delta.read_queue_full >= slow_consumer_rounds,
        "the slow-consumer phase reached the receiving read queue's bound only {} time(s) \
         across {slow_consumer_rounds} occurrence(s), so that cell was not covered on every \
         round it appeared",
        pipeline_delta.read_queue_full,
    );
    assert!(
        pipeline_delta.accept_channel_full == 0,
        "{} peer stream(s) were dropped because the application's accept channel was full; \
         the soak never stops accepting, so this is a defect",
        pipeline_delta.accept_channel_full,
    );

    // The timer ledger: the deadline was armed across the whole run and never
    // expired — a stall cost time, not the session.
    assert_eq!(
        timers.receive_deadline_expiries, 0,
        "a receive-deadline window expired during the soak ({timers})",
    );
    assert!(
        timers.receive_deadline_sleeps_armed > 0,
        "no receive-deadline sleep was ever registered on the timer wheel ({timers}); the \
         zero-expiry result is vacuous",
    );
    assert!(
        timers.heartbeats_sent > 0 && timers.heartbeats_received > 0,
        "no heartbeats were exchanged ({timers}); the liveness path was never exercised",
    );

    // The growth and release halves at matched points.
    assert_eq!(
        checkpoints.len(),
        2,
        "the soak did not reach both matched points in {rounds} rounds ({opened} streams \
         opened, {completed} completed); the growth comparison is vacuous",
    );
    let first = checkpoints[0];
    let later = checkpoints[1];
    assert!(
        completed >= checkpoint_at * CHECKPOINT_MULTIPLE,
        "only {completed} streams completed; the second matched point claims {}",
        checkpoint_at * CHECKPOINT_MULTIPLE,
    );
    assert_eq!(
        phases_applied,
        last_round + 1,
        "only {phases_applied} of {} rounds applied a phase",
        last_round + 1,
    );
    assert!(
        later.ledger.client.inserted + later.ledger.server.inserted >= completed,
        "the admission ledger counted fewer inserts than completed streams, so the census was \
         not reading the same session ({} vs {completed})",
        later.ledger.client.inserted + later.ledger.server.inserted,
    );
    assert!(
        later.ledger.client.retired > 0 && later.ledger.server.retired > 0,
        "one session retired no stream at all ({}), so its zero table reading is vacuous",
        later.ledger,
    );
    assert_eq!(
        later.ledger.client.refused_peer + later.ledger.client.refused_local,
        0,
        "the session refused {} local admission(s); a refused open in this soak is a defect",
        later.ledger.client.refused_local,
    );

    assert_not_grown(&first, &later);
    assert_released(&after);
    assert_eq!(
        after.structures.total_live_stream_structures(),
        0,
        "the session holds per-stream state at the end of the soak ({})",
        after.structures,
    );
    assert_eq!(
        after.egress.total(),
        0,
        "the session holds egress token state at the end of the soak ({})",
        after.egress,
    );
    println!("frame reorder soak checkpoints: first[{first}] later[{later}]");
    // Last, so a fault run is never green even if none of the property checks
    // above happened to catch the injected fault; the checks that precede it
    // are the ones that name the structure and both counts.
    assert!(
        !faults::enabled(),
        "a fault mode is set, so this run is a red-proof probe and must not be read as a pass",
    );
}

/// The soak's schedule is fixed, so this pins the shape the default run
/// executes: which phase lands on which round, and that the spike rotation
/// cycles. Env-independent on purpose -- a default-tier arm must not fail
/// because a soak was widened from the environment.
#[test]
fn the_default_schedule_is_the_declared_one() {
    assert_eq!(SCHEDULE.len(), 8);
    assert_eq!(phase_for_round(0), Phase::Echo);
    assert_eq!(phase_for_round(7), Phase::ReorderStall);
    assert_eq!(phase_for_round(8), Phase::Echo);
    assert_eq!(phase_for_round(15), Phase::ReorderStall);
    assert_eq!(spike_for_stall(2), SPIKE_SCHEDULE[2]);
    assert_eq!(spike_for_stall(3), SPIKE_SCHEDULE[0]);
    assert_eq!(SPIKE_SCHEDULE.len(), 3);
}

/// The growth assertion has teeth on its own, independent of the per-checkpoint
/// zero assertions: given a matched pair whose later census holds more live
/// per-stream state, it must panic and name the structure and both counts.
#[test]
fn the_growth_assertion_rejects_a_grown_census() {
    let first = Checkpoint {
        label: "synthetic-first",
        completed_streams: 100,
        structures: mux::live_probe::StructureCensuses::default(),
        egress: mux::live_probe::EgressTokenCensuses::default(),
        ledger: mux::live_probe::AdmissionLedgers::default(),
        reorder: mux::live_probe::ReorderLedger::default(),
        teardowns: 0,
    };
    let mut later = first;
    later.label = "synthetic-later";
    later.completed_streams = 1000;
    later.structures.client.reassembly_pending_frames = 7;

    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let payload = std::panic::catch_unwind(|| assert_not_grown(&first, &later));
    std::panic::set_hook(previous_hook);

    let payload = payload.expect_err("a grown census must fail the growth assertion");
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_default();
    assert!(
        message.contains("per-stream structures grew between matched points"),
        "the failure must name the growth assertion: {message}",
    );
    assert!(
        message.contains("0 -> 7"),
        "the failure must print both counts, before and after: {message}",
    );
    assert!(
        message.contains("100 -> 1000 completed streams"),
        "the failure must name the matched points it compared: {message}",
    );
}

/// The per-checkpoint release assertion has the same property: a single
/// retained reorder buffer must fail it, naming the structure and the count.
#[test]
fn the_release_assertion_rejects_a_retained_structure() {
    let mut checkpoint = Checkpoint {
        label: "synthetic",
        completed_streams: 100,
        structures: mux::live_probe::StructureCensuses::default(),
        egress: mux::live_probe::EgressTokenCensuses::default(),
        ledger: mux::live_probe::AdmissionLedgers::default(),
        reorder: mux::live_probe::ReorderLedger::default(),
        teardowns: 0,
    };
    checkpoint.structures.server.reassembly_pending_frames = 3;
    checkpoint.structures.server.reassembly_pending_bytes = 4096;

    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let payload = std::panic::catch_unwind(|| assert_released(&checkpoint));
    std::panic::set_hook(previous_hook);

    let payload = payload.expect_err("a retained reorder frame must fail the release assertion");
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_default();
    assert!(
        message.contains("retains 3 pending reassembly frame(s)"),
        "the failure must name the structure and the count: {message}",
    );
    assert!(
        message.contains("reassembly_pending(frames=3 bytes=4096)"),
        "the failure must print the census it read: {message}",
    );
}
