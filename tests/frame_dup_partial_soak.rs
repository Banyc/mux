//! Long-lived-session soak for the two delivery shapes the reordering soak
//! names as *not covered*: a frame **duplicated** while a later frame is held
//! in a reorder buffer, and a frame **split** across transport segments — a
//! segment that is not a whole frame.
//!
//! `tests/frame_reorder_soak.rs` records both as empty cells against its own
//! schedule: `dropped_dup_buffered` stayed 0 (its duplicate always lands
//! *after* the original's bytes were released, so only the after-release
//! allocation is asserted), and its transport writes one whole frame per
//! `poll_write`, so a frame boundary never lands inside a transport segment.
//! `session_growth_soak` and `spike_survival_soak` cannot disturb delivery at
//! all. This soak drives the same one-session, fixed-schedule shape with a
//! transport that applies exactly those two disturbances, and asserts the
//! structures they touch.
//!
//! What it asserts:
//!
//! 1. **The disturbance landed, in the structure under test, not only in the
//!    shim.** `dropped_dup_buffered` (a duplicate of a frame *still buffered
//!    ahead of the cursor*) must fire — the cell the reordering soak records
//!    as zero; `dropped_late` must fire for after-release duplicates; and
//!    `partial_bodies` (a frame body consumed across more than one transport
//!    segment, counted in the reader's body loop, for a frame that fits in one
//!    segment) must fire. Each has a matching shim-side counter asserted
//!    non-zero, so a run whose disturbance never applied fails.
//! 2. **A duplicate never doubles the stream.** Every phase's payload is
//!    compared byte-exact against a freshly generated position-dependent
//!    pattern, so a duplicate delivered *into* the stream shows as a repeated
//!    subsequence and fails; the mux's own counters show it was dropped.
//! 3. **Nothing leaks or resurrects.** At two matched points (after N and 10N
//!    completed streams) `stream_table`, the reorder buffers' `pending` frames
//!    and bytes, `open_read_sinks`, `closed_but_retained` and the egress token
//!    tables all read zero, and no structure holds more live state later. A
//!    duplicate re-delivered after its stream retired must not materialise a
//!    phantom stream.
//! 4. **Progress every round.** Each phase is followed by a full open/write/
//!    echo recovery probe that must complete, so a mid-soak wedge fails where
//!    it happens instead of averaging away.
//! 5. **Combinations.** Duplication × a slow consumer (the peer stops reading),
//!    a split whose tail is withheld across the field's 190 ms / 1063 ms /
//!    3205 ms spike, and duplication crossing a stream close.
//!
//! A segment that carries **more** than one whole frame is *not* a new cell:
//! the reader is a `BufReader` over a byte stream, so a segment holding several
//! whole frames is what a reader sees whenever the writer is ahead, in every
//! duplex-based soak in this crate. It is recorded as a cell already covered by
//! the existing soaks rather than one this soak closes; there is deliberately no
//! join injection here, because injecting it changed the shim's pacing enough
//! to suppress the reordering this soak exists to apply.
//!
//! Tier: **standard** (`#[ignore]`d). `MUX_DUP_ROUNDS` and
//! `MUX_DUP_CHECKPOINT` size the soak; `MUX_DUP_FAULT` is the red-proof
//! selector (`no_hold` removes the reorder hold, `no_duplicate` removes both
//! duplicate injections, `no_split` removes the split injection).

use std::{
    collections::VecDeque,
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant as StdInstant},
};

use mux::{
    Initiation, MuxConfig, MuxError, StreamAccepter, StreamOpener, StreamReader, StreamWriter,
    spawn_mux_no_reconnection,
};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, duplex},
    sync::{Notify, mpsc},
    task::JoinSet,
    time::Instant,
};

/// Production heartbeat on the deployed path; the steady receive deadline is
/// `4 x` this.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// `central_io::reader::RECEIVE_DEADLINE_INTERVALS`.
const RECEIVE_DEADLINE: Duration = Duration::from_secs(20);
/// A slow-consumer phase may legitimately block on a full read queue.
const PHASE_TIMEOUT: Duration = Duration::from_secs(5);

/// Buffered bytes per direction of the in-memory transport pair. A whole data
/// frame is at most `64 KiB` (`REASSEMBLY_MAX_BODY` plus its two headers), so
/// the largest frame exactly fills this; a frame smaller than the maximum can
/// therefore always be delivered in one segment, which is what makes the
/// reader's `partial_bodies` reading attributable to the split injection.
const DUPLEX_BUF: usize = 64 * 1024;
/// Frames the shim may have in flight before it stops accepting more.
const INFLIGHT_FRAMES: usize = 16;
/// Frames already queued behind a held frame that may overtake it.
const HOLD_FOLLOWERS: u64 = 4;

/// On-wire header byte for a Data frame (`wire_contract` pins it).
const DATA_FRAME_CODE: u8 = 0x02;

/// One multi-frame message: three frames, the last shorter than the maximum.
const BULK: usize = 3 * 48 * 1024;
/// The small request/response payload the echo phases use: one data frame,
/// comfortably below the transport's per-write capacity.
const ECHO_BODY: usize = 4096;
/// Streams each echo round opens.
const ECHO_STREAMS: u64 = 3;

/// Streams completed before the first checkpoint; the second is ten times it.
const DEFAULT_CHECKPOINT: u64 = 50;
const CHECKPOINT_MULTIPLE: u64 = 10;
/// `RETIRED_FINISHED_PEER_STREAM_WINDOW` in `src/control.rs`.
const RETIRED_WINDOW_MAX: u64 = 1024;

/// The field's spike schedule, rotated per stall phase.
const SPIKE_SCHEDULE: [Duration; 3] = [
    Duration::from_millis(190),
    Duration::from_millis(1063),
    Duration::from_millis(3205),
];

/// `stream::reader::STREAM_READ_HARD_DATA_LIMIT` (`8 * 1024 - 1`).
const STREAM_READ_HARD_DATA_LIMIT: usize = 8 * 1024 - 1;

// ─── the fault switches ────────────────────────────────────────────────────

/// Red-proof modes, never part of a green run.
///
/// * `no_hold` stops the shim holding a data frame, so nothing is ever
///   buffered ahead of the cursor and the soak must fail naming
///   `buffered_out_of_order`.
/// * `no_duplicate` stops both duplicate injections, so `dropped_dup_buffered`
///   and the shim's own duplicate counters must stay zero and the soak must
///   fail naming them.
/// * `no_split` stops the split injection, so no frame boundary lands inside a
///   transport segment and `partial_bodies` must stay zero.
mod faults {
    use std::sync::atomic::{AtomicBool, Ordering};

    pub static NO_HOLD: AtomicBool = AtomicBool::new(false);
    pub static NO_DUPLICATE: AtomicBool = AtomicBool::new(false);
    pub static NO_SPLIT: AtomicBool = AtomicBool::new(false);

    pub fn enabled() -> bool {
        NO_HOLD.load(Ordering::SeqCst)
            || NO_DUPLICATE.load(Ordering::SeqCst)
            || NO_SPLIT.load(Ordering::SeqCst)
    }
}

// ─── the split gate ────────────────────────────────────────────────────────

/// Withholds the second half of one split frame until the test releases it, so
/// the simulated spike between the halves is advanced by the test's own clock
/// move rather than by a timer. `armed` is one-shot and consumed by the next
/// data frame the shim writes, which is the phase's own tail frame because the
/// phases are sequential in that direction.
struct SplitGate {
    armed: AtomicBool,
    half_written: Notify,
    resume: Notify,
    /// Splits whose second half was withheld for the test.
    withheld: AtomicU64,
}

impl SplitGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            armed: AtomicBool::new(false),
            half_written: Notify::new(),
            resume: Notify::new(),
            withheld: AtomicU64::new(0),
        })
    }
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

// ─── the transport shim ────────────────────────────────────────────────────

/// Frame-delivery statistics. A green run has to prove it exercised the
/// shapes it exists for, so the shim counts what it did and the soak fails on
/// a zero.
#[derive(Debug, Default)]
struct Stats {
    /// Whole data frames the shim accepted from the encoder.
    data_frames: AtomicU64,
    /// Times a held frame was overtaken by at least one later frame.
    reorders: AtomicU64,
    /// Frames that arrived ahead of an earlier frame, summed over events.
    overtaken: AtomicU64,
    /// Duplicates re-delivered of a frame a held frame was still waiting on.
    dup_while_held: AtomicU64,
    /// Duplicates re-delivered after the fact (after-release copies).
    dup_after_release: AtomicU64,
    /// Frames written as two segments with a boundary between the halves.
    splits: AtomicU64,
    /// Splits whose second half was withheld across a test-driven spike.
    gated_splits: AtomicU64,
    /// Frames the test injected directly (the duplicate across a close).
    injected: AtomicU64,
}

/// What the shim does to the frame stream. Fixed for the whole soak, so every
/// phase runs against the same transport impairment; the phases vary the load
/// shape instead.
struct SegmentPlan {
    /// Hold every n-th data frame (1 = every one).
    hold_every: u64,
    /// Re-deliver every m-th data frame, delayed past its original.
    duplicate_every: u64,
    /// Deliver a scheduled duplicate this many data frames later.
    duplicate_delay: u64,
}

impl Default for SegmentPlan {
    fn default() -> Self {
        Self {
            // Holding every data frame, and duplicating the frame queued behind
            // it, is what makes `dropped_dup_buffered` reachable: the follower
            // is buffered (its predecessor is held) at the instant its copy
            // arrives, so the duplicate is dropped *while a gap is
            // outstanding* rather than after release.
            hold_every: 1,
            duplicate_every: 2,
            duplicate_delay: 2,
        }
    }
}

/// Bounded frame window shared by the shim and the reorder task.
#[derive(Debug)]
struct FrameWindow {
    cap: usize,
    state: Mutex<FrameWindowState>,
}

#[derive(Debug, Default)]
struct FrameWindowState {
    inflight: usize,
    waker: Option<std::task::Waker>,
}

impl FrameWindow {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            state: Mutex::new(FrameWindowState::default()),
        }
    }
    fn enter(&self, waker: &std::task::Waker) -> bool {
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
struct FrameWriter {
    tx: mpsc::Sender<Vec<u8>>,
    window: Arc<FrameWindow>,
}

impl AsyncWrite for FrameWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if !this.window.enter(cx.waker()) {
            return Poll::Pending;
        }
        match this.tx.try_send(buf.to_vec()) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
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

/// One direction's disturbance: it holds a data frame and lets the frames
/// queued behind it overtake, re-delivers duplicates both while the held frame
/// is outstanding and after the fact, splits a frame across two transport
/// segments, and joins two frames into one segment. Nothing is ever left held:
/// a frame with nothing queued behind it goes out in order immediately.
struct Shim {
    out: DuplexStream,
    frames: mpsc::Receiver<Vec<u8>>,
    inject: mpsc::Receiver<Vec<u8>>,
    capture: Arc<Mutex<Option<Vec<u8>>>>,
    capture_on: Arc<AtomicBool>,
    gate: Arc<SplitGate>,
    stats: Arc<Stats>,
    window: Arc<FrameWindow>,
    plan: Arc<SegmentPlan>,
}

enum Src {
    Frame(Vec<u8>),
    Inject(Vec<u8>),
    End,
}

impl Shim {
    async fn run(mut self) {
        let mut data_seen: u64 = 0;
        let mut delayed: VecDeque<(u64, Vec<u8>)> = VecDeque::new();
        loop {
            let src = tokio::select! {
                biased;
                Some(f) = self.inject.recv() => Src::Inject(f),
                maybe = self.frames.recv() => match maybe {
                    Some(f) => Src::Frame(f),
                    None => Src::End,
                },
            };
            let frame = match src {
                Src::End => return,
                Src::Inject(f) => {
                    self.stats.injected.fetch_add(1, Ordering::Relaxed);
                    if self.out.write_all(&f).await.is_err() {
                        return;
                    }
                    continue;
                }
                Src::Frame(f) => f,
            };
            if frame.first() != Some(&DATA_FRAME_CODE) {
                // A control frame is never split or duplicated: it has
                // no offset to duplicate idempotently.
                if self.out.write_all(&frame).await.is_err() {
                    return;
                }
                self.window.leave();
                continue;
            }
            data_seen += 1;
            self.stats.data_frames.fetch_add(1, Ordering::Relaxed);
            if self.capture_on.load(Ordering::SeqCst) {
                let mut slot = self.capture.lock().unwrap();
                if slot.is_none() {
                    *slot = Some(frame.clone());
                }
            }
            let unit = frame;
            if !faults::NO_DUPLICATE.load(Ordering::SeqCst)
                && self.plan.duplicate_every != 0
                && data_seen.is_multiple_of(self.plan.duplicate_every)
            {
                delayed.push_back((data_seen + self.plan.duplicate_delay.max(1), unit.clone()));
                self.stats.dup_after_release.fetch_add(1, Ordering::Relaxed);
            }

            let hold = !faults::NO_HOLD.load(Ordering::SeqCst)
                && self.plan.hold_every != 0
                && data_seen.is_multiple_of(self.plan.hold_every);
            if !hold {
                if self.write_frame(&unit, data_seen).await.is_err() {
                    return;
                }
                self.window.leave();
            } else {
                // Gather the frames already queued behind the held one. The
                // writer is usually several frames ahead (the message encoder
                // stages its whole frame run before yielding), so the queue is
                // drained without waiting; a bounded couple of yields is only
                // paid when it is momentarily empty, so this loop is not a
                // per-frame scheduling cost.
                let mut followers: Vec<Vec<u8>> = Vec::new();
                let mut stalls = 0u32;
                while (followers.len() as u64) < HOLD_FOLLOWERS {
                    match self.frames.try_recv() {
                        Ok(follower) => followers.push(follower),
                        Err(mpsc::error::TryRecvError::Empty) if stalls < 2 => {
                            stalls += 1;
                            tokio::task::yield_now().await;
                        }
                        Err(_) => break,
                    }
                }
                if followers.is_empty() {
                    if self.write_frame(&unit, data_seen).await.is_err() {
                        return;
                    }
                    self.window.leave();
                } else {
                    // The first follower goes out while the held frame is still
                    // outstanding, so the receiver must buffer it.
                    if self.write_frame(&followers[0], data_seen).await.is_err() {
                        return;
                    }
                    self.window.leave();
                    if followers[0].first() == Some(&DATA_FRAME_CODE)
                        && !faults::NO_DUPLICATE.load(Ordering::SeqCst)
                    {
                        // The duplicate of a frame the receiver is still
                        // holding: it lands *while a gap is outstanding*, which
                        // is the cell the reordering soak records as zero.
                        self.stats.dup_while_held.fetch_add(1, Ordering::Relaxed);
                        if self.write_frame(&followers[0], data_seen).await.is_err() {
                            return;
                        }
                    }
                    for follower in &followers[1..] {
                        if follower.first() == Some(&DATA_FRAME_CODE) {
                            if self.write_frame(follower, data_seen).await.is_err() {
                                return;
                            }
                        } else if self.out.write_all(follower).await.is_err() {
                            return;
                        }
                        self.window.leave();
                    }
                    self.stats.reorders.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .overtaken
                        .fetch_add(followers.len() as u64, Ordering::Relaxed);
                    if self.write_frame(&unit, data_seen).await.is_err() {
                        return;
                    }
                    self.window.leave();
                }
            }
            while delayed.front().is_some_and(|(due, _)| *due <= data_seen) {
                let (_, bytes) = delayed.pop_front().unwrap();
                // The copy is written whole: a duplicate that is itself split
                // would make the two disturbances indistinguishable.
                if self.out.write_all(&bytes).await.is_err() {
                    return;
                }
            }
        }
    }

    /// Write one frame as two segments with a boundary between them, or whole
    /// when the split injection is off. A frame whose second half is armed for
    /// the test is withheld until the test resumes it.
    async fn write_frame(&mut self, bytes: &[u8], index: u64) -> io::Result<()> {
        if faults::NO_SPLIT.load(Ordering::SeqCst) || bytes.len() < 2 {
            return self.out.write_all(bytes).await;
        }
        let span = bytes.len() - 1;
        let at = 1 + ((index.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 11) as usize) % span;
        self.out.write_all(&bytes[..at]).await?;
        self.stats.splits.fetch_add(1, Ordering::Relaxed);
        if self.gate.armed.swap(false, Ordering::SeqCst) {
            self.stats.gated_splits.fetch_add(1, Ordering::Relaxed);
            self.gate.withheld.fetch_add(1, Ordering::Relaxed);
            self.gate.half_written.notify_one();
            self.gate.resume.notified().await;
        } else {
            tokio::task::yield_now().await;
        }
        self.out.write_all(&bytes[at..]).await
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
    _shims: JoinSet<()>,
    stats: Arc<Stats>,
    gate: Arc<SplitGate>,
    inject_tx: mpsc::Sender<Vec<u8>>,
    capture: Arc<Mutex<Option<Vec<u8>>>>,
    capture_on: Arc<AtomicBool>,
}

impl Pair {
    /// Spawn one long-lived mux pair over in-memory duplexes whose two egress
    /// directions pass through the segment shim, both sessions running
    /// `frame_reassembly` on.
    fn spawn() -> Self {
        let stats = Arc::new(Stats::default());
        let plan = Arc::new(SegmentPlan::default());
        let gate = SplitGate::new();
        let client_window = Arc::new(FrameWindow::new(INFLIGHT_FRAMES));
        let server_window = Arc::new(FrameWindow::new(INFLIGHT_FRAMES));
        let capture: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
        let capture_on = Arc::new(AtomicBool::new(false));

        let (client_read, server_write) = duplex(DUPLEX_BUF);
        let (server_read, client_write) = duplex(DUPLEX_BUF);
        let (client_shim, client_frames) = mpsc::channel(INFLIGHT_FRAMES + 1);
        let (server_shim, server_frames) = mpsc::channel(INFLIGHT_FRAMES + 1);
        let (client_inject_tx, client_inject) = mpsc::channel(4);
        let (_server_inject_tx, server_inject) = mpsc::channel(4);

        let mut shims = JoinSet::new();
        shims.spawn(
            Shim {
                out: client_write,
                frames: client_frames,
                inject: client_inject,
                capture: Arc::clone(&capture),
                capture_on: Arc::clone(&capture_on),
                gate: Arc::clone(&gate),
                stats: Arc::clone(&stats),
                window: Arc::clone(&client_window),
                plan: Arc::clone(&plan),
            }
            .run(),
        );
        shims.spawn(
            Shim {
                out: server_write,
                frames: server_frames,
                inject: server_inject,
                capture: Arc::clone(&capture),
                capture_on: Arc::clone(&capture_on),
                gate: SplitGate::new(),
                stats: Arc::clone(&stats),
                window: Arc::clone(&server_window),
                plan,
            }
            .run(),
        );

        let mut client_config = MuxConfig::new(Initiation::Client, HEARTBEAT_INTERVAL);
        client_config.frame_reassembly = true;
        let mut client_teardowns = JoinSet::new();
        let (opener, _) = spawn_mux_no_reconnection(
            client_read,
            FrameWriter {
                tx: client_shim,
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
            FrameWriter {
                tx: server_shim,
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
            _shims: shims,
            stats,
            gate,
            inject_tx: client_inject_tx,
            capture,
            capture_on,
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
    /// comparing the echoed bytes against a freshly generated pattern.
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
                "payload integrity: sent {} byte(s), received {} — a duplicate was \
                 delivered into the stream",
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

/// One round's load shape. Every phase varies one dimension from the `Echo`
/// baseline — small concurrent full echo rounds over the same disturbed
/// transport — and the impairment itself is constant across the whole run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Baseline: concurrent small full echo rounds.
    Echo,
    /// A multi-frame message whose first frame is held while its own successor
    /// is held too and duplicated.
    DupWhileBuffered,
    /// One multi-frame message per direction, with every frame split across
    /// two transport segments.
    SplitBody,
    /// A peer accepts and stops reading while the transport duplicates.
    DupSlowConsumer,
    /// A single-frame message whose only frame is split, with the second half
    /// withheld across the field's spike schedule: the stream's tail stays
    /// incomplete for the whole window.
    SplitTailStall,
    /// A finished stream's data frame re-delivered after the entry retired.
    DupAcrossClose,
}

const SCHEDULE: [Phase; 8] = [
    Phase::Echo,
    Phase::DupWhileBuffered,
    Phase::SplitBody,
    Phase::DupSlowConsumer,
    Phase::Echo,
    Phase::SplitTailStall,
    Phase::SplitBody,
    Phase::DupAcrossClose,
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

/// A full bidirectional multi-frame round, payload compared byte-exact. The
/// held frame's own successor is duplicated while it is still buffered, so the
/// phase asserts the mux's buffered-duplicate counter grew during it: the cell
/// is asserted on every occurrence, not only in the run total.
async fn bulk_round(pair: &mut Pair, seed: u64, require_buffered_dup: bool) -> PhaseCounts {
    let buffered_before = mux::live_probe::reorder_ledger().dropped_buffered;
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
    assert_eq!(
        read.expect("bulk server read"),
        sent,
        "a multi-frame message delivered under duplication and splitting was not \
         byte-exact in stream order",
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
    assert_eq!(
        read.expect("bulk client read"),
        sent,
        "the echo of a message delivered under duplication and splitting was not \
         byte-exact",
    );
    counts.completed = 1;
    if require_buffered_dup {
        let after = mux::live_probe::reorder_ledger().dropped_buffered - buffered_before;
        assert!(
            after >= 1,
            "the duplicate-while-buffered phase dropped no frame that was still buffered \
             ahead of the cursor ({after} in this phase), so the cell this soak exists to \
             close was not exercised on this occurrence",
        );
    }
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

/// The peer accepts and never reads while the transport duplicates and holds,
/// so the receiving read queue's bound is reached under duplication. One byte
/// per write, so the queued-message count equals the write count and the bound
/// is crossed by message count rather than byte size. Bounded by a count as
/// well as the clock, because a writer that keeps making progress never lets a
/// paused clock advance.
async fn phase_dup_slow_consumer(pair: &mut Pair, seed: u64) -> PhaseCounts {
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
    let refusals_before = mux::live_probe::totals().pipeline.read_queue_full;
    let reorder_before = mux::live_probe::reorder_ledger();
    let budget = 2 * STREAM_READ_HARD_DATA_LIMIT;
    let mut written = 0usize;
    while written < budget {
        let byte = pattern_byte(seed, written);
        match AsyncWriteExt::write(&mut client_writer, &[byte]).await {
            Ok(0) => break,
            Ok(n) => written += n,
            Err(_) => break,
        }
        if mux::live_probe::totals().pipeline.read_queue_full > refusals_before {
            break;
        }
    }
    let refusals = mux::live_probe::totals().pipeline.read_queue_full - refusals_before;
    let reorder = mux::live_probe::reorder_ledger().since(&reorder_before);
    assert!(
        refusals >= 1,
        "the slow consumer's read queue never refused a frame within {written} one-byte \
         write(s), so this cell was not covered on the round it appeared",
    );
    assert!(
        reorder.dropped_buffered + reorder.dropped_delivered >= 1,
        "the slow-consumer phase saw no duplicate dropped at all ({reorder}), so the \
         duplication × slow-consumer combination was not applied",
    );
    drop(server_reader);
    drop(server_writer);
    drop(client_reader);
    drop(client_writer);
    counts
}

/// A single-frame message whose tail frame is split, with the second half
/// withheld across one spike of the field's schedule. While the spike is being
/// advanced the stream's tail is incomplete: the `CloseWrite` has already
/// reached the receiver, so it knows the message's final offset but holds only
/// the first half of the bytes.
async fn phase_split_tail_stall(pair: &mut Pair, seed: u64, spike_index: u64) -> PhaseCounts {
    let mut counts = PhaseCounts {
        opened: 1,
        phase_applied: 1,
        ..Default::default()
    };
    let spike = spike_for_stall(spike_index);
    assert!(
        spike < RECEIVE_DEADLINE,
        "the {spike:?} stall crosses the {RECEIVE_DEADLINE:?} receive deadline, so this \
         phase would assert on a session the detector had already ended",
    );
    let ledger = mux::live_probe::timer_ledger();
    let remaining = Duration::from_millis(
        ledger
            .last_deadline_ms
            .saturating_sub(mux::live_probe::now_ms()),
    );
    assert!(
        remaining > spike,
        "only {remaining:?} of the receive-deadline window was left when the {spike:?} \
         stall began, so the advance would cross the deadline ({ledger})",
    );
    let Ok((mut client_reader, mut client_writer, mut server_reader, mut server_writer)) =
        pair.open_pair().await
    else {
        return counts;
    };
    let sent = pattern(seed, ECHO_BODY);
    let partial_before = mux::live_probe::reorder_ledger().partial_bodies;
    // Arm the gate, then write: the shim withholds the second half of the tail
    // frame until the test resumes it.
    pair.gate.arm();
    let withheld_before = pair.gate.withheld.load(Ordering::SeqCst);
    let mut write_job = JoinSet::new();
    let payload = sent.clone();
    write_job.spawn(async move {
        let result = client_writer.write_all(&payload).await;
        let _ = AsyncWriteExt::shutdown(&mut client_writer).await;
        result
    });
    // The wait is bounded on the simulated clock: a run whose split injection
    // never fired must fail *naming the split counter*, not hang until the
    // phase's own wedge budget expires.
    if tokio::time::timeout(Duration::from_millis(50), pair.gate.half_written.notified())
        .await
        .is_err()
    {
        panic!(
            "the gated split withheld no second half within 50 ms of simulated time, so the \
             stream's tail was never incomplete across the spike ({} split frame(s) so far, \
             {} partial bod(y|ies))",
            pair.stats.splits.load(Ordering::SeqCst),
            mux::live_probe::reorder_ledger().partial_bodies,
        );
    }
    assert!(
        pair.gate.withheld.load(Ordering::SeqCst) > withheld_before,
        "the gated split never withheld a second half, so the tail was not incomplete \
         across the spike",
    );
    // The tail is withheld now: hold the clock still for the whole spike.
    busily_yield().await;
    tokio::time::advance(spike).await;
    busily_yield().await;
    pair.gate.resume.notify_one();

    match write_job.join_next().await {
        Some(Ok(Ok(()))) => {}
        Some(Ok(Err(e))) => panic!("split tail write failed: {e}"),
        Some(Err(e)) => panic!("split tail write task panicked: {e}"),
        None => panic!("the split tail write task vanished"),
    }
    let mut received = Vec::new();
    server_reader
        .read_to_end(&mut received)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "server read of a tail split across a {spike:?} stall failed: {e}; {}; {}",
                mux::live_probe::reorder_ledger(),
                mux::live_probe::timer_ledger(),
            )
        });
    assert_eq!(
        received, sent,
        "a message whose tail was split across a {spike:?} stall was not delivered \
         byte-exact in stream order",
    );
    server_writer
        .write_all(&sent)
        .await
        .unwrap_or_else(|e| panic!("split tail echo write failed: {e}"));
    AsyncWriteExt::shutdown(&mut server_writer).await.ok();
    let mut echoed = Vec::new();
    client_reader
        .read_to_end(&mut echoed)
        .await
        .unwrap_or_else(|e| panic!("split tail echo read failed: {e}"));
    assert_eq!(
        echoed, sent,
        "the echo of a split-tail message was not byte-exact"
    );
    let partial = mux::live_probe::reorder_ledger().partial_bodies - partial_before;
    assert!(
        partial >= 1,
        "the tail frame split across the {spike:?} stall was not consumed across more than \
         one transport segment ({partial} partial bod(y|ies) in this phase), so the split \
         never reached the reader",
    );
    counts.completed = 1;
    counts.stall_applied = 1;
    counts
}

/// A stream written, closed, read to the end on both halves, and then dropped,
/// so its entry retires; then its first data frame is injected again. The
/// duplicate must not materialise a phantom stream: nothing may be in the
/// table and nothing may be retained afterwards.
async fn phase_dup_across_close(pair: &mut Pair, seed: u64) -> PhaseCounts {
    let mut counts = PhaseCounts {
        opened: 1,
        phase_applied: 1,
        ..Default::default()
    };
    assert_eq!(
        mux::live_probe::structure_censuses().total_live_stream_structures(),
        0,
        "the session held per-stream state before the duplicate-across-close phase \
         began ({})",
        mux::live_probe::structure_censuses(),
    );
    *pair.capture.lock().unwrap() = None;
    pair.capture_on.store(true, Ordering::SeqCst);
    let Ok((mut client_reader, mut client_writer, mut server_reader, mut server_writer)) =
        pair.open_pair().await
    else {
        pair.capture_on.store(false, Ordering::SeqCst);
        return counts;
    };
    let sent = pattern(seed, ECHO_BODY);
    client_writer.write_all(&sent).await.unwrap();
    AsyncWriteExt::shutdown(&mut client_writer).await.unwrap();
    let mut got = Vec::new();
    server_reader.read_to_end(&mut got).await.unwrap();
    assert_eq!(got, sent);
    server_writer.write_all(&sent).await.unwrap();
    AsyncWriteExt::shutdown(&mut server_writer).await.unwrap();
    let mut echo = Vec::new();
    client_reader.read_to_end(&mut echo).await.unwrap();
    assert_eq!(echo, sent);
    drop(client_reader);
    drop(client_writer);
    drop(server_reader);
    drop(server_writer);
    settle().await;
    // The peer's entry for this stream must be gone before the duplicate is
    // injected: the guard under test is the one that answers a frame for a
    // stream that is no longer in the table.
    let census = mux::live_probe::structure_censuses();
    assert_eq!(
        census.total_live_stream_structures(),
        0,
        "the finished stream had not retired when the duplicate across its close was \
         injected ({census}), so the frame would be dropped by the table rather than by \
         the retired-id guard",
    );
    let capture = pair.capture.lock().unwrap().take();
    pair.capture_on.store(false, Ordering::SeqCst);
    let Some(bytes) = capture else {
        panic!("no data frame was captured for the duplicate-across-close phase");
    };
    pair.inject_tx
        .send(bytes)
        .await
        .expect("the shim's inject channel is closed");
    settle().await;
    let after = mux::live_probe::structure_censuses();
    assert_eq!(
        after.total_live_stream_structures(),
        0,
        "a data frame re-delivered after its stream retired materialised state that \
         outlived the stream ({after}); the duplicate resurrected a phantom stream",
    );
    counts.completed = 1;
    counts
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
}

impl Checkpoint {
    fn sample(label: &'static str, completed_streams: u64) -> Self {
        Self {
            label,
            completed_streams,
            structures: mux::live_probe::structure_censuses(),
            egress: mux::live_probe::egress_token_censuses(),
            ledger: mux::live_probe::admission_ledgers(),
            reorder: mux::live_probe::reorder_ledger(),
        }
    }
}

impl std::fmt::Display for Checkpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "checkpoint[{}] completed_streams={}\n  {}\n  {}\n  {}\n  {}",
            self.label,
            self.completed_streams,
            self.structures,
            self.egress,
            self.ledger,
            self.reorder,
        )
    }
}

/// Every per-stream structure a quiesced session must have released.
fn assert_released(checkpoint: &Checkpoint) {
    for (role, census) in [
        ("client", checkpoint.structures.client),
        ("server", checkpoint.structures.server),
    ] {
        assert_eq!(
            census.reassembly_buffers, 0,
            "{}: {role} retains {} reorder buffer(s) after every stream closed ({census})",
            checkpoint.label, census.reassembly_buffers,
        );
        assert_eq!(
            census.reassembly_pending_frames, 0,
            "{}: {role} retains {} pending reassembly frame(s) ({census})",
            checkpoint.label, census.reassembly_pending_frames,
        );
        assert_eq!(
            census.reassembly_pending_bytes, 0,
            "{}: {role} retains {} pending reassembly byte(s) ({census})",
            checkpoint.label, census.reassembly_pending_bytes,
        );
        assert_eq!(
            census.closed_but_retained, 0,
            "{}: {role} retains {} stream-table entr(ies) that `is_closed` already reports \
             finished ({census})",
            checkpoint.label, census.closed_but_retained,
        );
        assert_eq!(
            census.open_read_sinks, 0,
            "{}: {role} retains {} open read sink(s) after every stream closed ({census})",
            checkpoint.label, census.open_read_sinks,
        );
        assert_eq!(
            census.stream_table_len, 0,
            "{}: {role} stream table holds {} entr(ies) after every stream closed ({census})",
            checkpoint.label, census.stream_table_len,
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
            "{}: {role} egress keeps {} per-stream token queue(s) after every stream closed \
             ({census})",
            checkpoint.label, census.token_queues,
        );
        assert_eq!(
            census.cached_heads, 0,
            "{}: {role} egress keeps {} cached head(s) after every stream closed ({census})",
            checkpoint.label, census.cached_heads,
        );
        assert_eq!(
            census.token_streams, 0,
            "{}: {role} egress keeps {} token→stream map entr(ies) after every stream closed \
             ({census})",
            checkpoint.label, census.token_streams,
        );
    }
}

/// No per-stream structure may hold more live state at a later matched point.
fn assert_not_grown(first: &Checkpoint, later: &Checkpoint) {
    let before = first.structures.total_live_stream_structures();
    let after = later.structures.total_live_stream_structures();
    assert!(
        after <= before,
        "per-stream structures grew between matched points: {before} -> {after} live entries \
         over {} -> {} completed streams\n  {}\n  {}",
        first.completed_streams,
        later.completed_streams,
        first.structures,
        later.structures,
    );
    assert!(
        later.egress.total() <= first.egress.total(),
        "egress token structures grew between matched points: {} -> {} entries\n  {}\n  {}",
        first.egress.total(),
        later.egress.total(),
        first.egress,
        later.egress,
    );
}

/// Every phase runs under a bound on the simulated clock.
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

const PHASE_BUDGET: Duration = PHASE_TIMEOUT.saturating_mul(4);

/// Streams that complete in one pass of [`SCHEDULE`], derived from the phases:
/// two `Echo` rounds at three streams each, two `SplitBody` bulk rounds, and
/// one each from `DupWhileBuffered`, `SplitTailStall` and `DupAcrossClose` is
/// eleven; `DupSlowConsumer` completes none; the per-round recovery probe adds
/// eight.
const COMPLETIONS_PER_SCHEDULE: u64 = 19;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn checkpoint_streams() -> u64 {
    env_u64("MUX_DUP_CHECKPOINT", DEFAULT_CHECKPOINT)
}

fn rounds() -> u64 {
    let checkpoint = checkpoint_streams();
    env_u64(
        "MUX_DUP_ROUNDS",
        (checkpoint * CHECKPOINT_MULTIPLE * SCHEDULE.len() as u64)
            .div_ceil(COMPLETIONS_PER_SCHEDULE),
    )
}

/// Drive the runtime without leaving it idle, so a quiescence loop cannot
/// auto-advance the paused clock past a timer a phase is measuring.
async fn busily_yield() {
    for _ in 0..256 {
        tokio::task::yield_now().await;
    }
}

/// Drive the runtime to quiescence so every close the soak issued has been
/// applied and every egress token reaped before the census is read, then let
/// the paused clock cross a heartbeat so the liveness path runs.
async fn settle() {
    for _ in 0..1024 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(HEARTBEAT_INTERVAL * 2).await;
    for _ in 0..1024 {
        tokio::task::yield_now().await;
    }
}

// ─── the soak ──────────────────────────────────────────────────────────────

/// A long-lived session driven over a transport that duplicates frames while
/// they are held and splits frames across segments releases every per-stream
/// structure it allocates, never lets a duplicate into the delivered stream,
/// never resurrects a retired stream, and returns to full function after every
/// phase.
#[tokio::test(start_paused = true)]
#[ignore = "standard tier: long-lived-session frame duplication and partial delivery soak"]
async fn a_long_lived_session_survives_duplicated_and_partial_frames() {
    match std::env::var("MUX_DUP_FAULT").as_deref() {
        Ok("no_hold") => faults::NO_HOLD.store(true, Ordering::SeqCst),
        Ok("no_duplicate") => faults::NO_DUPLICATE.store(true, Ordering::SeqCst),
        Ok("no_split") => faults::NO_SPLIT.store(true, Ordering::SeqCst),
        Ok(other) => panic!("unknown MUX_DUP_FAULT={other}"),
        Err(_) => {}
    }
    mux::live_probe::enable_structure_census();

    let checkpoint_at = checkpoint_streams();
    let rounds = rounds();
    let wall_start = StdInstant::now();
    let sim_start = Instant::now();
    let mut pair = Pair::spawn();
    let stats = Arc::clone(&pair.stats);
    let gate = Arc::clone(&pair.gate);
    let reorder_before = mux::live_probe::reorder_ledger();
    let timers_before = mux::live_probe::timer_ledger();
    let pipeline_before = mux::live_probe::totals().pipeline;

    let baseline = Checkpoint::sample("baseline", 0);
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
    let mut close_dup_rounds = 0u64;

    for round in 0..rounds {
        last_round = round;
        let phase = phase_for_round(round);
        let seed = 0x5EED_D0D0_0000_0000u64.wrapping_add(round.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let counts = match phase {
            Phase::Echo => bounded(round, phase, PHASE_BUDGET, echo_round(&mut pair, seed)).await,
            // `DupWhileBuffered` asserts its own cell on every occurrence;
            // `SplitBody` is the same shape without that requirement, so the
            // two phases differ in exactly one dimension: whether the held
            // frame's successor is duplicated while it is still buffered.
            Phase::DupWhileBuffered => {
                bounded(
                    round,
                    phase,
                    PHASE_BUDGET,
                    bulk_round(&mut pair, seed, true),
                )
                .await
            }
            Phase::SplitBody => {
                bounded(
                    round,
                    phase,
                    PHASE_BUDGET,
                    bulk_round(&mut pair, seed, false),
                )
                .await
            }
            Phase::DupSlowConsumer => {
                slow_consumer_rounds += 1;
                bounded(
                    round,
                    phase,
                    PHASE_BUDGET,
                    phase_dup_slow_consumer(&mut pair, seed),
                )
                .await
            }
            Phase::SplitTailStall => {
                let index = stall_index;
                stall_index += 1;
                let budget = PHASE_BUDGET.saturating_add(spike_for_stall(index));
                bounded(
                    round,
                    phase,
                    budget,
                    phase_split_tail_stall(&mut pair, seed, index),
                )
                .await
            }
            Phase::DupAcrossClose => {
                close_dup_rounds += 1;
                // Two quiescing waits, each crossing a heartbeat so the
                // liveness path runs: the phase's own budget covers them.
                let budget = PHASE_BUDGET.saturating_add(HEARTBEAT_INTERVAL * 4);
                bounded(
                    round,
                    phase,
                    budget,
                    phase_dup_across_close(&mut pair, seed),
                )
                .await
            }
        };
        opened += counts.opened;
        completed += counts.completed;
        phases_applied += counts.phase_applied;
        stalls_applied += counts.stall_applied;

        // The per-round progress assertion: every phase must be followed by a
        // round that completes.
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
                "round {round}: the session did not return to full function after {phase:?}: {e}"
            ),
        }

        if let Some(reason) = pair.tear_down_reason() {
            panic!("round {round}: the session tore down during a {phase:?} round: {reason:?}");
        }

        tokio::time::sleep(HEARTBEAT_INTERVAL * 2).await;

        if completed >= next_checkpoint {
            settle().await;
            let mut label = "first";
            if !checkpoints.is_empty() {
                label = "later";
            }
            let checkpoint = Checkpoint::sample(label, completed);
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
    let after = Checkpoint::sample("final", completed);
    let reorder_delta = mux::live_probe::reorder_ledger().since(&reorder_before);
    let pipeline_delta = mux::live_probe::totals().pipeline.since(&pipeline_before);
    let timers = mux::live_probe::timer_ledger().since(&timers_before);
    let stats = stats.as_ref();
    let data_frames = stats.data_frames.load(Ordering::Relaxed);
    let reorders = stats.reorders.load(Ordering::Relaxed);
    let overtaken = stats.overtaken.load(Ordering::Relaxed);
    let dup_while_held = stats.dup_while_held.load(Ordering::Relaxed);
    let dup_after_release = stats.dup_after_release.load(Ordering::Relaxed);
    let splits = stats.splits.load(Ordering::Relaxed);
    let injected = stats.injected.load(Ordering::Relaxed);
    let gated_splits = stats.gated_splits.load(Ordering::Relaxed);
    println!(
        "frame dup/partial soak: rounds={} opened_streams={} completed_streams={} \
         phases_applied={} stalls_applied={} simulated={:?} wall={:?}\n  {reorder_delta}\n  \
         shim: data_frames={data_frames} reorders={reorders} overtaken={overtaken} \
         dup_while_held={dup_while_held} dup_after_release={dup_after_release} splits={splits} \
         gated_splits={gated_splits} injected={injected}\n  \
         {timers}\n  {pipeline_delta}",
        last_round + 1,
        opened,
        completed,
        phases_applied,
        stalls_applied,
        sim_elapsed,
        wall_elapsed,
    );

    // (1) The disturbance reached the structure under test. These are read from
    // inside the mux, so no shim behaviour can make them pass.
    assert!(
        reorder_delta.buffered > 0,
        "the mux `ReorderBuffer` buffered no out-of-order frame across {} ingested frame(s) \
         ({reorder_delta}), so nothing was ever held and the duplicate-while-buffered path \
         was unreachable",
        reorder_delta.ingests,
    );
    assert!(
        reorder_delta.dropped_buffered > 0,
        "no frame duplicated one already buffered ahead of the cursor ({} dropped as a \
         buffered duplicate, {} dropped after release) across {dup_while_held} injected \
         duplicate(s) held against a buffered frame: the duplication-while-buffered cell is \
         still empty",
        reorder_delta.dropped_buffered,
        reorder_delta.dropped_delivered,
    );
    assert!(
        reorder_delta.dropped_delivered > 0,
        "no frame arrived after its bytes had already been released ({} dropped late, {} \
         dropped as a buffered duplicate) across {dup_after_release} after-release \
         duplicate(s) injected",
        reorder_delta.dropped_delivered,
        reorder_delta.dropped_buffered,
    );
    assert!(
        reorder_delta.partial_bodies > 0,
        "no frame's body was consumed across more than one transport segment across {} \
         ingested frame(s) and {splits} split frame(s): a split frame never reached the \
         reader, so the partial-delivery cell is still empty. (This reading has a small \
         natural floor — a pipe that runs full cuts a frame whatever the writer does — so \
         the attributable count is the shim's {splits} split(s) and {gated_splits} \
         withheld second half(ves), not this total.)",
        reorder_delta.ingests,
    );
    assert!(
        reorder_delta.ingests >= completed,
        "only {} frame(s) were ingested for {completed} completed streams ({reorder_delta}), \
         so the reassembly path was not the one carrying the soak's traffic",
        reorder_delta.ingests,
    );
    // (1b) The shim applied the disturbance. A run whose injection never fired
    // fails here rather than passing on a zero.
    assert!(
        dup_while_held > 0,
        "the shim never duplicated a frame while a held frame was outstanding, so the \
         buffered-duplicate path was never injected",
    );
    assert!(
        dup_after_release > 0,
        "the shim scheduled no after-release duplicate, so the late-duplicate path was never \
         injected",
    );
    assert!(
        splits > 0,
        "the shim never split a frame across two segments, so no segment boundary landed \
         inside a frame",
    );
    assert!(
        gated_splits >= 1 && stalls_applied == stall_index,
        "only {gated_splits} split(s) were withheld across a spike and only {stalls_applied} \
         of {stall_index} stall phase(s) applied their hold",
    );
    assert!(
        injected >= close_dup_rounds,
        "only {injected} frame(s) were injected for {close_dup_rounds} duplicate-across-close \
         round(s), so that combination was not applied on every occurrence",
    );
    assert!(
        reorders > 0 && overtaken > 0,
        "the shim delivered no frame out of order (reorders={reorders} overtaken={overtaken})",
    );
    assert!(
        pipeline_delta.read_queue_full >= slow_consumer_rounds,
        "the slow-consumer phase reached the receiving read queue's bound only {} time(s) \
         across {slow_consumer_rounds} occurrence(s)",
        pipeline_delta.read_queue_full,
    );
    assert!(
        pipeline_delta.accept_channel_full == 0,
        "{} peer stream(s) were dropped because the application's accept channel was full; \
         the soak never stops accepting, so this is a defect",
        pipeline_delta.accept_channel_full,
    );

    // The timer ledger: the deadline was armed across the whole run and never
    // expired — a split held across a spike cost time, not the session.
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

    // (3) The matched points.
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
        "the admission ledger counted fewer inserts than completed streams ({} vs {completed})",
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
    assert!(
        gate.withheld.load(Ordering::SeqCst) >= 1,
        "the split gate never withheld a second half, so no stream's tail was incomplete \
         across a spike",
    );
    println!("frame dup/partial soak checkpoints: first[{first}] later[{later}]");
    assert!(
        !faults::enabled(),
        "a fault mode is set, so this run is a red-proof probe and must not be read as a pass",
    );
}

/// The soak's schedule is fixed, so this pins the shape the default run
/// executes. Env-independent on purpose.
#[test]
fn the_default_schedule_is_the_declared_one() {
    assert_eq!(SCHEDULE.len(), 8);
    assert_eq!(phase_for_round(0), Phase::Echo);
    assert_eq!(phase_for_round(1), Phase::DupWhileBuffered);
    assert_eq!(phase_for_round(7), Phase::DupAcrossClose);
    assert_eq!(phase_for_round(8), Phase::Echo);
    assert_eq!(spike_for_stall(2), SPIKE_SCHEDULE[2]);
    assert_eq!(spike_for_stall(3), SPIKE_SCHEDULE[0]);
    assert_eq!(SPIKE_SCHEDULE.len(), 3);
    assert_eq!(COMPLETIONS_PER_SCHEDULE, 19);
}

/// The growth assertion has teeth on its own: given a matched pair whose later
/// census holds more live per-stream state, it must panic and name the
/// structure and both counts.
#[test]
fn the_growth_assertion_rejects_a_grown_census() {
    let first = Checkpoint {
        label: "synthetic-first",
        completed_streams: 100,
        structures: mux::live_probe::StructureCensuses::default(),
        egress: mux::live_probe::EgressTokenCensuses::default(),
        ledger: mux::live_probe::AdmissionLedgers::default(),
        reorder: mux::live_probe::ReorderLedger::default(),
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

/// The per-checkpoint release assertion has the same property: one retained
/// reorder buffer must fail it, naming the structure and the count.
#[test]
fn the_release_assertion_rejects_a_retained_structure() {
    let mut checkpoint = Checkpoint {
        label: "synthetic",
        completed_streams: 100,
        structures: mux::live_probe::StructureCensuses::default(),
        egress: mux::live_probe::EgressTokenCensuses::default(),
        ledger: mux::live_probe::AdmissionLedgers::default(),
        reorder: mux::live_probe::ReorderLedger::default(),
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
}
