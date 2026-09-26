//! Interactive-path liveness: schedule families the soak does not reach.
//!
//! `interactive_liveness_soak` varies its *volume* and its per-cycle stream
//! counts, but its injection schedule is fixed by the cycle index
//! (`cycle % 7`, `% 11`, `% 13`), so the *timing* of every injection relative
//! to message boundaries, the number of concurrent sessions, and the
//! control-frame races are all held constant across seeds. A green soak
//! therefore excludes liveness defects in one schedule family only.
//!
//! These families vary the axes the soak holds fixed. Each is `#[ignore]`d
//! (tier `standard`) and asserts the same properties as the soak: byte
//! conservation, per-job completion, and a per-cycle bound with a stall
//! reported distinctly from a late cycle.
//!
//! * `quiet_egress_tail_family` — exactly one stream at a time, with a quiet
//!   gap between every message so the egress scheduler is observed parking
//!   and must then be woken for the *next* publish; the gap is also placed
//!   immediately before a `shutdown` (the FIN) and before a dropped reader.
//!   This is the timing axis: the soak's injections fire at fixed cycle
//!   phases, never at a message boundary of a quiet stream.
//! * `concurrent_sessions_family` — three independent mux session pairs in
//!   one process, each with its own central I/O, one of them deliberately
//!   idle on alternate cycles, so a wake lost in one session cannot be masked
//!   by another session's activity.
//! * `control_race_family` — `Open`/`CloseRead`/`CloseWrite` churn racing
//!   in-flight data: open-then-immediately-abandon, a reader dropped while
//!   the peer is still staging (CloseRead against data), and a writer dropped
//!   on its own staged tail (the FIN against pending data).
//! * `reassembly_gap_family` — both sessions run `frame_reassembly` on over a
//!   transport whose two directions deliver frames out of sent order, so the
//!   reassembly cursor's liveness is observed at all: interactive ping-pong
//!   interleaved with a multi-frame bulk message on one session, a message
//!   whose frames arrive out of order and across multi-frame gaps, a `Fin`
//!   racing an in-flight message, and a reader dropped mid-reassembly. Every
//!   other family (and the soak) runs the stock wire, so no other instrument
//!   in this crate reaches this path.
//!
//! Run one family (or all, `--ignored` runs the whole target):
//!
//! ```sh
//! MUX_FAMILY_CYCLES=N MUX_FAMILY_SEED=S cargo test --release -p mux \
//!   --test interactive_liveness_families -- --ignored --nocapture
//! ```
//!
//! Each family prints one summary line; a stall prints the in-flight jobs with
//! their staged/received byte counts and panics.

use std::{
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use mux::{
    Initiation, MuxConfig, StreamAccepter, StreamOpener, StreamReader, StreamWriter,
    spawn_mux_no_reconnection,
};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, duplex},
    sync::mpsc,
    task::JoinSet,
    time::{Instant, timeout},
};

/// Same bound as the soak: single-digit ms on loopback, so a live but slow
/// host does not read as a stall while a lost wake is still caught.
const CYCLE_BOUND: Duration = Duration::from_millis(2_000);

const DEFAULT_CYCLES: u64 = 400;

/// Buffered bytes per direction of the in-memory transport pair.
const DUPLEX_BUF: usize = 64 * 1024;

/// A cycle that has not completed within this long *and* has made no
/// observable progress is a hang, not a late cycle: the soak's second bound.
const HANG_BOUND: Duration = Duration::from_secs(10);

/// Frames already staged behind a held frame that may overtake it. The
/// reorderer never waits for one: it takes whatever the encoder has already
/// queued behind the frame it holds, then releases that frame, so a
/// multi-frame message arrives with its later frames first and the receiver's
/// reorder buffer has to hold them until the gap fills. Above one, the gap
/// spans more than a single frame.
const REORDER_OVERTAKE: u64 = 3;

/// The multi-frame message the reassembly family sends: one `write_all` well
/// above the encoder's per-frame body cap, so the sender splits it and the
/// receiver must hold the later frames until the earlier ones arrive.
const REASSEMBLY_BULK: usize = 3 * 48 * 1024;

/// The prefix of a multi-frame message the server reads before dropping its
/// reader, leaving the rest of the message in flight or still reassembling.
const DROP_READER_PREFIX: usize = 16 * 1024;

/// Frames the reorder shim may have in flight before it stops accepting more.
/// This is the transport's window: without it the shim would absorb the
/// transport's backpressure entirely, letting a writer stage megabytes into
/// the reorder task's buffer — not what a transport does, and not free, since
/// a peer's close would then race a flood the peer never had to face.
const REORDER_INFLIGHT_FRAMES: usize = 8;

// ─── payload ───────────────────────────────────────────────────────────────

fn pattern_byte(seed: u64, offset: usize) -> u8 {
    let mut x = seed ^ (offset as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    (x >> 56) as u8
}

fn fill_pattern(seed: u64, offset: usize, out: &mut [u8]) {
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = pattern_byte(seed, offset + i);
    }
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn cycles() -> u64 {
    env_u64("MUX_FAMILY_CYCLES", DEFAULT_CYCLES)
}

fn base_seed() -> u64 {
    env_u64("MUX_FAMILY_SEED", 0x5EED_2244_ABCD_0001)
}

// ─── accounting ────────────────────────────────────────────────────────────

/// In-flight job labels with per-job staged/received byte counts, so a stall
/// names which side is short of bytes instead of leaving it a guess.
#[derive(Debug, Default)]
struct Inflight {
    map: Mutex<std::collections::HashMap<u64, (String, Arc<JobMeter>)>>,
    next: AtomicU64,
}

#[derive(Debug, Default)]
struct JobMeter {
    staged: AtomicU64,
    received: AtomicU64,
}

impl Inflight {
    fn enter(&self, label: String, meter: Arc<JobMeter>) -> u64 {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.map.lock().unwrap().insert(id, (label, meter));
        id
    }
    fn leave(&self, id: u64) {
        self.map.lock().unwrap().remove(&id);
    }
    fn snapshot(&self) -> Vec<String> {
        let mut labels: Vec<String> = self
            .map
            .lock()
            .unwrap()
            .values()
            .map(|(label, meter)| {
                format!(
                    "{label} staged={} received={}",
                    meter.staged.load(Ordering::Relaxed),
                    meter.received.load(Ordering::Relaxed)
                )
            })
            .collect();
        labels.sort();
        labels
    }
}

/// One cycle's jobs, driven under the cycle bound. A cycle that does not
/// complete inside the bound is a stall and its in-flight labels are printed:
/// the same verdict shape the soak uses, without the soak's replay machinery.
struct Cycle {
    jobs: JoinSet<Result<(), String>>,
    inflight: Arc<Inflight>,
    completed: Arc<AtomicU64>,
}

impl Cycle {
    fn new(inflight: Arc<Inflight>, completed: Arc<AtomicU64>) -> Self {
        Self {
            jobs: JoinSet::new(),
            inflight,
            completed,
        }
    }

    /// Spawn a job that owns `meter` (so it can record its own staged and
    /// received bytes) and is labelled in the in-flight map while it runs.
    fn spawn<F, Fut>(&mut self, label: String, future: F)
    where
        F: FnOnce(Arc<JobMeter>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), String>> + Send + 'static,
    {
        let meter = Arc::new(JobMeter::default());
        let id = self.inflight.enter(label, Arc::clone(&meter));
        let tracker = Arc::clone(&self.inflight);
        let completed = Arc::clone(&self.completed);
        self.jobs.spawn(async move {
            let result = future(meter).await;
            tracker.leave(id);
            completed.fetch_add(1, Ordering::Relaxed);
            result
        });
    }

    /// Drain every job, or report a stall. `first` keeps the first job error so
    /// a payload mismatch is never hidden behind a later success.
    async fn run(mut self, label: &str) -> Result<(), String> {
        let inflight = Arc::clone(&self.inflight);
        let jobs = &mut self.jobs;
        let drain = async {
            let mut first: Option<String> = None;
            while let Some(result) = jobs.join_next().await {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(message)) => {
                        first.get_or_insert(message);
                    }
                    Err(_) => {
                        first.get_or_insert("job panicked/aborted".to_owned());
                    }
                }
            }
            first
        };
        match timeout(CYCLE_BOUND, drain).await {
            Ok(None) => Ok(()),
            Ok(Some(message)) => Err(format!("{label} failed: {message}")),
            Err(_) => Err(format!(
                "{label} STALLED: not all jobs finished within {CYCLE_BOUND:?}; \
                 in-flight: {:?}",
                inflight.snapshot()
            )),
        }
    }

    /// The soak's two-stage verdict: complete inside `CYCLE_BOUND` to pass,
    /// complete after it to be **late**, never complete to be a **hang** —
    /// with the jobs that did finish counted, so a partially stalled cycle is
    /// not read as a total one.
    async fn run_verdict(mut self) -> CycleVerdict {
        let inflight = Arc::clone(&self.inflight);
        let completed = &self.completed;
        let jobs = &mut self.jobs;
        let before = completed.load(Ordering::Relaxed);
        let start = Instant::now();
        let mut first: Option<String> = None;
        let mut finished = false;
        for window in [CYCLE_BOUND, HANG_BOUND.saturating_sub(CYCLE_BOUND)] {
            let drain = async {
                while let Some(result) = jobs.join_next().await {
                    completed.fetch_add(1, Ordering::Relaxed);
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(message)) => {
                            if first.is_none() {
                                first = Some(message);
                            }
                        }
                        Err(_) => {
                            if first.is_none() {
                                first = Some("job panicked/aborted".to_owned());
                            }
                        }
                    }
                }
            };
            if timeout(window, drain).await.is_ok() {
                finished = true;
                break;
            }
        }
        let elapsed = start.elapsed();
        if finished {
            if elapsed <= CYCLE_BOUND {
                return match first {
                    Some(message) => CycleVerdict::Failed { elapsed, message },
                    None => CycleVerdict::Pass,
                };
            }
            return CycleVerdict::Late {
                elapsed,
                message: first,
            };
        }
        CycleVerdict::Hang {
            elapsed,
            finished: completed.load(Ordering::Relaxed) - before,
            message: first,
            in_flight: inflight.snapshot(),
        }
    }
}

/// One cycle's verdict, in the soak's shape: a cycle that finishes inside the
/// bound passes (or fails on a job error), one that finishes after it is
/// **late**, and one that never finishes is a **hang**.
#[derive(Debug)]
enum CycleVerdict {
    Pass,
    Failed {
        elapsed: Duration,
        message: String,
    },
    Late {
        elapsed: Duration,
        message: Option<String>,
    },
    Hang {
        elapsed: Duration,
        finished: u64,
        message: Option<String>,
        in_flight: Vec<String>,
    },
}

// ─── session helpers ───────────────────────────────────────────────────────

struct Endpoint {
    opener: StreamOpener,
    accepter: StreamAccepter,
}

/// Spawn one mux pair over an in-memory duplex. The session tasks live in
/// `session`; a session that dies shows up as a job error (every job's write
/// or read fails), which the cycle verdict reports as a failure.
fn spawn_pair(session: &mut JoinSet<mux::MuxError>, frame_reassembly: bool) -> Endpoint {
    let (client_read, server_write) = duplex(DUPLEX_BUF);
    let (server_read, client_write) = duplex(DUPLEX_BUF);
    let common = |initiation| MuxConfig {
        initiation,
        heartbeat_interval: Duration::from_secs(60),
        frame_reassembly,
    };
    let (opener, _client_accepter) = spawn_mux_no_reconnection(
        client_read,
        client_write,
        common(Initiation::Client),
        session,
    );
    let (_server_opener, accepter) = spawn_mux_no_reconnection(
        server_read,
        server_write,
        common(Initiation::Server),
        session,
    );
    Endpoint { opener, accepter }
}

/// Read exactly `buf.len()` bytes, or a clean EOF only on the message
/// boundary: a partial message is an error, never a silent truncation.
async fn read_exact_or_eof(reader: &mut StreamReader, buf: &mut [u8]) -> io::Result<bool> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = reader.read(&mut buf[filled..]).await?;
        if n == 0 {
            if filled == 0 {
                return Ok(false);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "truncated message: {filled} of {} bytes before EOF",
                    buf.len()
                ),
            ));
        }
        filled += n;
    }
    Ok(true)
}

// ─── out-of-order frame delivery ───────────────────────────────────────────
//
// `frame_reassembly` exists because the deployment's interactive lane hands
// complete frames up in *arrival* order, not sent order; mux's reassembly
// buffer is the consumer that restores per-stream order. The families above
// (and the soak) run the stock wire, so none of them ever reaches that
// consumer. These helpers are the transport those families lack: a
// frame-reordering shim the sessions write through.

/// On-wire header byte for a Data frame. The default-tier wire contract pins
/// it (`wire_contract` asserts `Header::Data.encode() == [0x02]`), so the
/// reorderer can tell a data frame from a control frame by its first byte
/// without parsing the frame — it needs the distinction only to know which
/// frames it is legal to hold back.
const DATA_FRAME_CODE: u8 = 0x02;

/// On-wire header byte for a CloseWrite (`Fin`) frame. A `Fin` overtaking an
/// in-flight data frame is the shape the reassembly cursor has to survive.
const CLOSE_WRITE_FRAME_CODE: u8 = 0x04;

/// Frame-delivery statistics. A green run has to prove it exercised the
/// out-of-order arrival it exists for, so the reorderer counts what it did and
/// the family prints it and refuses to pass on `reorders == 0`.
#[derive(Debug, Default)]
struct ReorderStats {
    /// Whole frames the shim accepted from the encoder.
    frames: AtomicU64,
    /// Times a held frame was overtaken by at least one later frame: the
    /// number of out-of-order arrivals the receiver had to reassemble.
    reorders: AtomicU64,
    /// Frames that arrived ahead of an earlier frame, summed over those
    /// reorder events.
    overtaken: AtomicU64,
    /// Largest number of frames that overtook one held frame: above one, the
    /// receiver's reorder buffer had to hold more than a single frame.
    max_gap: AtomicU64,
    /// Times a `CloseWrite` (`Fin`) overtook a data frame of its own session:
    /// the Fin racing in-flight reassembly, counted rather than assumed.
    close_overtakes: AtomicU64,
    /// Times the injected hold was taken (red-proof mode only).
    injected_holds: AtomicU64,
}

impl ReorderStats {
    fn snapshot(&self) -> (u64, u64, u64, u64, u64, u64) {
        (
            self.frames.load(Ordering::Relaxed),
            self.reorders.load(Ordering::Relaxed),
            self.overtaken.load(Ordering::Relaxed),
            self.max_gap.load(Ordering::Relaxed),
            self.close_overtakes.load(Ordering::Relaxed),
            self.injected_holds.load(Ordering::Relaxed),
        )
    }
}

/// Bounded frame window shared by the shim and the reorder task: the shim
/// takes a slot per frame it accepts, the task releases one after handing a
/// frame to the duplex. A writer that outruns the transport is therefore
/// parked rather than buffered without bound.
#[derive(Debug)]
struct FrameWindow {
    cap: usize,
    state: Mutex<FrameWindowState>,
}

#[derive(Debug, Default)]
struct FrameWindowState {
    inflight: usize,
    /// The writer parked for a slot. One writer per direction, so one waker is
    /// enough.
    waker: Option<Waker>,
}

impl FrameWindow {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            state: Mutex::new(FrameWindowState::default()),
        }
    }
    /// Take a slot, or register `waker` and report that there is none.
    fn enter(&self, waker: &Waker) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.inflight >= self.cap {
            state.waker = Some(waker.clone());
            return false;
        }
        state.inflight += 1;
        true
    }
    /// Release a slot, waking the writer parked for one.
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
/// writer, so each `poll_write` is exactly one frame. Accepting the buffer
/// whole and forwarding it to a reorder task preserves the frame boundary the
/// encoder already has, without the test re-parsing the wire format.
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
        // The channel is one longer than the admission window, so the window is
        // the only gate a writer waits on and `Full` here is unreachable. It is
        // still handled: a self-wake retries rather than dropping a frame, and
        // it cannot spin because the reorder task frees a slot on every pass.
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
    /// The encoder has to take its one-`write_all`-per-frame path: the shim's
    /// frame boundaries *are* those calls.
    fn is_write_vectored(&self) -> bool {
        false
    }
}

/// Deliver the frames a reorder task receives out of sent order. A data frame
/// is held back and the frames already queued behind it — the rest of its own
/// multi-frame message, and whatever the other streams staged meanwhile — are
/// written first, so the receiver sees a later offset before an earlier one
/// and has to hold it until the gap fills. Nothing is ever left held: a frame
/// with nothing queued behind it goes out in order immediately, so the shim
/// cannot strand a stream by waiting for a follower that never comes.
///
/// `strand` is the red-proof mode, never a normal one: the n-th data frame
/// this direction sees is never released while the transport stays open, so
/// the receiver's reassembly cursor can never advance past the gap.
async fn reorder_frames(
    mut out: DuplexStream,
    mut frames: mpsc::Receiver<Vec<u8>>,
    stats: Arc<ReorderStats>,
    window: Arc<FrameWindow>,
    strand: Option<u64>,
) {
    let mut data_seen = 0u64;
    while let Some(frame) = frames.recv().await {
        if frame.first() != Some(&DATA_FRAME_CODE) {
            let written = out.write_all(&frame).await;
            window.leave();
            if written.is_err() {
                return;
            }
            continue;
        }
        data_seen += 1;
        if strand == Some(data_seen) {
            stats.injected_holds.fetch_add(1, Ordering::Relaxed);
            std::future::pending::<()>().await;
        }
        let mut followers: Vec<Vec<u8>> = Vec::new();
        while (followers.len() as u64) < REORDER_OVERTAKE {
            match frames.try_recv() {
                Ok(follower) => followers.push(follower),
                Err(_) => break,
            }
        }
        for follower in &followers {
            if follower.first() == Some(&CLOSE_WRITE_FRAME_CODE) {
                stats.close_overtakes.fetch_add(1, Ordering::Relaxed);
            }
            let written = out.write_all(follower).await;
            window.leave();
            if written.is_err() {
                return;
            }
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
        let written = out.write_all(&frame).await;
        window.leave();
        if written.is_err() {
            return;
        }
    }
}

/// An endpoint whose two directions each deliver frames out of sent order,
/// with both sessions running `frame_reassembly` on — the configuration the
/// reordering exists to exercise.
struct ReorderedEndpoint {
    endpoint: Endpoint,
    stats: Arc<ReorderStats>,
    /// The two reorder tasks, owned here so dropping the endpoint aborts them
    /// rather than leaving them detached.
    tasks: JoinSet<()>,
}

/// Spawn one mux pair over in-memory duplexes whose egress directions pass
/// through [`reorder_frames`]. The reorderer stands in for the deployment's
/// frame fast-forward; mux's reassembly path is the consumer that has to
/// restore per-stream order.
fn spawn_pair_reordered(
    session: &mut JoinSet<mux::MuxError>,
    strand: Option<u64>,
) -> ReorderedEndpoint {
    let stats = Arc::new(ReorderStats::default());
    // One window per direction: a single shared window lets a flooding egress
    // starve the *other* direction's writer for a slot, which is a deadlock
    // the test invented rather than found — the peer's own control frames
    // (a CloseRead) have to keep flowing while one direction is jammed.
    let client_window = Arc::new(FrameWindow::new(REORDER_INFLIGHT_FRAMES));
    let server_window = Arc::new(FrameWindow::new(REORDER_INFLIGHT_FRAMES));
    let (client_read, server_write) = duplex(DUPLEX_BUF);
    let (server_read, client_write) = duplex(DUPLEX_BUF);
    // The shim buffers into the reorder task's channel, so the write halves
    // the sessions see are the channels and the task owns the duplex half.
    // Named bounded capacities, one longer than the admission window, so the
    // window is the only gate a writer ever waits on and the channel can never
    // be the thing that refuses a frame.
    let (client_shim, client_frames) = mpsc::channel(REORDER_INFLIGHT_FRAMES + 1);
    let (server_shim, server_frames) = mpsc::channel(REORDER_INFLIGHT_FRAMES + 1);
    let mut tasks = JoinSet::new();
    tasks.spawn(reorder_frames(
        client_write,
        client_frames,
        Arc::clone(&stats),
        Arc::clone(&client_window),
        strand,
    ));
    tasks.spawn(reorder_frames(
        server_write,
        server_frames,
        Arc::clone(&stats),
        Arc::clone(&server_window),
        strand,
    ));
    let common = |initiation| MuxConfig {
        initiation,
        heartbeat_interval: Duration::from_secs(60),
        frame_reassembly: true,
    };
    let (opener, _client_accepter) = spawn_mux_no_reconnection(
        client_read,
        ReorderWriter {
            tx: client_shim,
            stats: Arc::clone(&stats),
            window: Arc::clone(&client_window),
        },
        common(Initiation::Client),
        session,
    );
    let (_server_opener, accepter) = spawn_mux_no_reconnection(
        server_read,
        ReorderWriter {
            tx: server_shim,
            stats: Arc::clone(&stats),
            window: Arc::clone(&server_window),
        },
        common(Initiation::Server),
        session,
    );
    ReorderedEndpoint {
        endpoint: Endpoint { opener, accepter },
        stats,
        tasks,
    }
}

// ─── family 1: quiet egress, message-boundary injection timing ─────────────

/// Hand the runtime a chance to park the egress scheduler between publishes:
/// with no queued messages the writer task's `select!` returns `Pending`, so
/// the *next* publish is the one that has to wake it.
async fn quiet_gap() {
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_micros(200)).await;
}

async fn quiet_echo_rounds(
    mut writer: StreamWriter,
    mut reader: StreamReader,
    meter: Arc<JobMeter>,
    seed: u64,
    rounds: usize,
    len: usize,
) -> Result<(), String> {
    let mut request = vec![0u8; len];
    let mut echo = vec![0u8; len];
    for round in 0..rounds {
        fill_pattern(seed.wrapping_add(round as u64), 0, &mut request);
        writer
            .write_all(&request)
            .await
            .map_err(|e| format!("quiet write: {e:?}"))?;
        meter.staged.fetch_add(len as u64, Ordering::Relaxed);
        read_exact_or_eof(&mut reader, &mut echo)
            .await
            .map_err(|e| format!("quiet read: {e:?}"))?;
        if echo != request {
            return Err(format!("quiet echo mismatch on round {round}"));
        }
        meter.received.fetch_add(len as u64, Ordering::Relaxed);
        // The quiet gap: the next write is a publish after the scheduler has
        // been given the chance to park.
        quiet_gap().await;
    }
    writer
        .shutdown()
        .map_err(|e| format!("quiet shutdown: {e:?}"))?;
    Ok(())
}

async fn quiet_echo_server(
    mut writer: StreamWriter,
    mut reader: StreamReader,
    meter: Arc<JobMeter>,
    len: usize,
) -> Result<(), String> {
    let mut message = vec![0u8; len];
    while read_exact_or_eof(&mut reader, &mut message)
        .await
        .map_err(|e| format!("quiet echo read: {e:?}"))?
    {
        meter.received.fetch_add(len as u64, Ordering::Relaxed);
        writer
            .write_all(&message)
            .await
            .map_err(|e| format!("quiet echo write: {e:?}"))?;
        meter.staged.fetch_add(len as u64, Ordering::Relaxed);
    }
    Ok(())
}

/// The tail shape: a lone message published after a quiet gap, then a
/// shutdown. The FIN is published by the writer's drop while the message it
/// follows may still be staged, and the lookup for it must find the scheduler
/// parked.
async fn quiet_tail_client(
    mut writer: StreamWriter,
    mut reader: StreamReader,
    meter: Arc<JobMeter>,
    seed: u64,
    payload: usize,
) -> Result<(), String> {
    quiet_gap().await;
    let mut buf = vec![0u8; payload];
    fill_pattern(seed, 0, &mut buf);
    writer
        .write_all(&buf)
        .await
        .map_err(|e| format!("tail write: {e:?}"))?;
    meter.staged.fetch_add(payload as u64, Ordering::Relaxed);
    writer
        .shutdown()
        .map_err(|e| format!("tail shutdown: {e:?}"))?;
    let mut trailing = [0u8; 1];
    let n = reader
        .read(&mut trailing)
        .await
        .map_err(|e| format!("tail eof: {e:?}"))?;
    if n != 0 {
        return Err("tail: peer data after FIN".to_owned());
    }
    Ok(())
}

async fn quiet_tail_server(
    mut reader: StreamReader,
    writer: StreamWriter,
    meter: Arc<JobMeter>,
    seed: u64,
    payload: usize,
) -> Result<(), String> {
    // The server's write half is dropped inside this job, so its FIN is the
    // same publish the client's trailing read is waiting on.
    drop(writer);
    let mut buf = vec![0u8; payload];
    if !read_exact_or_eof(&mut reader, &mut buf)
        .await
        .map_err(|e| format!("tail verify read: {e:?}"))?
    {
        return Err("tail verify: EOF before the payload".to_owned());
    }
    for (i, byte) in buf.iter().enumerate() {
        if *byte != pattern_byte(seed, i) {
            return Err(format!("tail payload mismatch at {i}"));
        }
    }
    meter.received.fetch_add(payload as u64, Ordering::Relaxed);
    Ok(())
}

/// Drop the reader after a quiet gap: the CloseRead must reach the peer's
/// writer (whose next write must fail) rather than leave it parked forever.
async fn quiet_drop_reader(mut writer: StreamWriter, reader: StreamReader) -> Result<(), String> {
    drop(reader);
    quiet_gap().await;
    let mut wrote = 0u64;
    while writer.write_all(&[0xA5u8; 64]).await.is_ok() {
        wrote += 1;
        if wrote > 100_000 {
            return Err("quiet drop-reader: writer never observed the peer close".to_owned());
        }
    }
    Ok(())
}

async fn quiet_peer_writer(
    mut writer: StreamWriter,
    mut reader: StreamReader,
) -> Result<(), String> {
    let mut scratch = [0u8; 256];
    let mut wrote = 0u64;
    while writer.write_all(&[0x5Au8; 64]).await.is_ok() {
        wrote += 1;
        if wrote > 100_000 {
            return Err("quiet drop-reader peer: writer never failed".to_owned());
        }
        let _ = reader.read(&mut scratch).await;
    }
    Ok(())
}

/// One stream at a time, with a quiet gap between every message, before the
/// tail FIN, and before a dropped reader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standard tier: quiet-egress message-boundary liveness family"]
async fn quiet_egress_tail_family() {
    let total = cycles();
    let mut session = JoinSet::new();
    let mut endpoint = spawn_pair(&mut session, false);
    let inflight = Arc::new(Inflight::default());
    let completed = Arc::new(AtomicU64::new(0));
    let mut rng = base_seed();
    let mut seed = base_seed() ^ 0x00C0_FFEE_1234_5678;
    let mut passes = 0u64;
    let mut worst = Duration::ZERO;

    for cycle in 0..total {
        let start = Instant::now();
        let mut jobs = Cycle::new(Arc::clone(&inflight), Arc::clone(&completed));
        let rounds = 2 + (splitmix64(&mut rng) % 4) as usize;
        let len = if splitmix64(&mut rng).is_multiple_of(2) {
            16
        } else {
            64
        };
        seed = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let (client_reader, client_writer) = endpoint.opener.open().await.unwrap();
        let (server_reader, server_writer) = endpoint.accepter.accept().await.unwrap();
        jobs.spawn(
            format!("c{cycle} quiet-client rounds={rounds} len={len}"),
            move |meter| quiet_echo_rounds(client_writer, client_reader, meter, seed, rounds, len),
        );
        jobs.spawn(format!("c{cycle} quiet-server len={len}"), move |meter| {
            quiet_echo_server(server_writer, server_reader, meter, len)
        });

        // The tail shape, every cycle: a lone message then the FIN, both
        // published right after a quiet gap.
        let payload = 32 + (splitmix64(&mut rng) % 96) as usize;
        let (client_reader, client_writer) = endpoint.opener.open().await.unwrap();
        let (server_reader, server_writer) = endpoint.accepter.accept().await.unwrap();
        seed = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        jobs.spawn(
            format!("c{cycle} tail-client payload={payload}"),
            move |meter| quiet_tail_client(client_writer, client_reader, meter, seed, payload),
        );
        jobs.spawn(
            format!("c{cycle} tail-server payload={payload}"),
            move |meter| quiet_tail_server(server_reader, server_writer, meter, seed, payload),
        );

        // Every third cycle also races a dropped reader against the peer's
        // writer, after a quiet gap.
        if cycle.is_multiple_of(3) {
            let (client_reader, client_writer) = endpoint.opener.open().await.unwrap();
            let (server_reader, server_writer) = endpoint.accepter.accept().await.unwrap();
            jobs.spawn(format!("c{cycle} drop-reader-client"), move |_meter| {
                quiet_drop_reader(client_writer, client_reader)
            });
            jobs.spawn(format!("c{cycle} drop-reader-server"), move |_meter| {
                quiet_peer_writer(server_writer, server_reader)
            });
        }

        match jobs.run(&format!("cycle {cycle}")).await {
            Ok(()) => {
                passes += 1;
                worst = worst.max(start.elapsed());
            }
            Err(message) => panic!("quiet egress family failed: {message}"),
        }
    }

    println!(
        "quiet_egress_tail_family: cycles={passes}/{total} worst_cycle={worst:?} \
         jobs={}",
        completed.load(Ordering::Relaxed)
    );
    assert!(passes > 0, "family ran no cycles");
}

// ─── family 2: independent sessions, one quiet ─────────────────────────────

/// The four halves of one bidirectional stream, grouped so a family job takes
/// them as one value rather than four positional arguments.
struct Halves {
    client_reader: StreamReader,
    client_writer: StreamWriter,
    server_reader: StreamReader,
    server_writer: StreamWriter,
}

async fn session_ping_pong(
    halves: Halves,
    meter: Arc<JobMeter>,
    seed: u64,
    rounds: usize,
    len: usize,
) -> Result<(), String> {
    let Halves {
        mut client_reader,
        mut client_writer,
        mut server_reader,
        mut server_writer,
    } = halves;
    let mut request = vec![0u8; len];
    let mut echo = vec![0u8; len];
    for round in 0..rounds {
        fill_pattern(seed.wrapping_add(round as u64), 0, &mut request);
        client_writer
            .write_all(&request)
            .await
            .map_err(|e| format!("write: {e:?}"))?;
        meter.staged.fetch_add(len as u64, Ordering::Relaxed);
        read_exact_or_eof(&mut server_reader, &mut echo)
            .await
            .map_err(|e| format!("server read: {e:?}"))?;
        if echo != request {
            return Err(format!("concurrent echo mismatch on round {round}"));
        }
        server_writer
            .write_all(&echo)
            .await
            .map_err(|e| format!("echo: {e:?}"))?;
        read_exact_or_eof(&mut client_reader, &mut echo)
            .await
            .map_err(|e| format!("client read: {e:?}"))?;
        if echo != request {
            return Err(format!("concurrent echo mismatch back on round {round}"));
        }
        meter.received.fetch_add(len as u64, Ordering::Relaxed);
    }
    client_writer
        .shutdown()
        .map_err(|e| format!("shutdown: {e:?}"))?;
    Ok(())
}

/// Three pairs, each with its own central I/O. One of them is deliberately
/// idle on alternate cycles, so a wake lost in an idle session cannot be
/// covered by another session's activity.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standard tier: multiple concurrent sessions sharing one runtime"]
async fn concurrent_sessions_family() {
    let total = cycles();
    let mut session = JoinSet::new();
    let mut pairs: Vec<Endpoint> = (0..3).map(|_| spawn_pair(&mut session, false)).collect();
    let inflight = Arc::new(Inflight::default());
    let completed = Arc::new(AtomicU64::new(0));
    let mut rng = base_seed();
    let mut passes = 0u64;
    let mut worst = Duration::ZERO;

    for cycle in 0..total {
        let start = Instant::now();
        let mut jobs = Cycle::new(Arc::clone(&inflight), Arc::clone(&completed));
        for (index, endpoint) in pairs.iter_mut().enumerate() {
            // Pair 2 is the idle one on odd cycles: it must still be reachable
            // by the next cycle's streams.
            if index == 2 && !cycle.is_multiple_of(2) {
                continue;
            }
            let rounds = 2 + (splitmix64(&mut rng) % 3) as usize;
            let len = if splitmix64(&mut rng).is_multiple_of(2) {
                16
            } else {
                128
            };
            let seed = base_seed()
                .wrapping_add(cycle)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(index as u64);
            let (client_reader, client_writer) = endpoint.opener.open().await.unwrap();
            let (server_reader, server_writer) = endpoint.accepter.accept().await.unwrap();
            jobs.spawn(
                format!("c{cycle} session#{index} rounds={rounds} len={len}"),
                move |meter| {
                    session_ping_pong(
                        Halves {
                            client_reader,
                            client_writer,
                            server_reader,
                            server_writer,
                        },
                        meter,
                        seed,
                        rounds,
                        len,
                    )
                },
            );
        }
        match jobs.run(&format!("cycle {cycle}")).await {
            Ok(()) => {
                passes += 1;
                worst = worst.max(start.elapsed());
            }
            Err(message) => panic!("concurrent sessions family failed: {message}"),
        }
    }

    println!(
        "concurrent_sessions_family: cycles={passes}/{total} worst_cycle={worst:?} \
         jobs={}",
        completed.load(Ordering::Relaxed)
    );
    assert!(passes > 0, "family ran no cycles");
}

// ─── family 3: control-frame races against in-flight data ──────────────────

async fn open_and_abandon(reader: StreamReader, writer: StreamWriter) -> Result<(), String> {
    // Abandon half-open immediately: the Open announcement and both close
    // frames race each other with no data at all.
    drop(writer);
    drop(reader);
    Ok(())
}

async fn reader_dropped_mid_bulk(
    mut writer: StreamWriter,
    reader: StreamReader,
    meter: Arc<JobMeter>,
    total: usize,
) -> Result<(), String> {
    // Stage a prefix, then drop the read half while the peer is still
    // staging: CloseRead races the in-flight data.
    let chunk = vec![0x33u8; 4096];
    let mut staged = 0;
    while staged < total / 4 {
        writer
            .write_all(&chunk)
            .await
            .map_err(|e| format!("bulk-race write: {e:?}"))?;
        meter
            .staged
            .fetch_add(chunk.len() as u64, Ordering::Relaxed);
        staged += chunk.len();
    }
    drop(reader);
    let mut wrote = 0u64;
    while writer.write_all(&chunk).await.is_ok() {
        wrote += 1;
        if wrote > 50_000 {
            return Err("bulk-race: writer never observed the peer close".to_owned());
        }
    }
    Ok(())
}

async fn reader_gone_peer_writer(
    mut writer: StreamWriter,
    mut reader: StreamReader,
) -> Result<(), String> {
    let mut scratch = [0u8; 4096];
    let mut wrote = 0u64;
    while writer.write_all(&[0x44u8; 4096]).await.is_ok() {
        wrote += 1;
        if wrote > 50_000 {
            return Err("bulk-race peer: writer never failed".to_owned());
        }
        let _ = reader.read(&mut scratch).await;
    }
    Ok(())
}

async fn aborted_writer_after_staging(
    mut writer: StreamWriter,
    meter: Arc<JobMeter>,
    seed: u64,
    payload: usize,
) -> Result<(), String> {
    // Stage the whole payload and drop the writer without an explicit
    // shutdown: the FIN is published by the drop, racing its own staged tail.
    let mut buf = vec![0u8; payload];
    fill_pattern(seed, 0, &mut buf);
    writer
        .write_all(&buf)
        .await
        .map_err(|e| format!("abort write: {e:?}"))?;
    meter.staged.fetch_add(payload as u64, Ordering::Relaxed);
    drop(writer);
    Ok(())
}

async fn aborted_peer_reader(
    mut reader: StreamReader,
    meter: Arc<JobMeter>,
    seed: u64,
    payload: usize,
) -> Result<(), String> {
    // The peer's writer was dropped: every staged byte must still arrive, and
    // then a clean EOF.
    let mut buf = vec![0u8; 4096];
    let mut offset = 0usize;
    loop {
        let n = reader
            .read(&mut buf)
            .await
            .map_err(|e| format!("abort peer read at {offset}: {e:?}"))?;
        if n == 0 {
            break;
        }
        for (i, byte) in buf[..n].iter().enumerate() {
            if *byte != pattern_byte(seed, offset + i) {
                return Err(format!("abort payload mismatch at {}", offset + i));
            }
        }
        offset += n;
        meter.received.fetch_add(n as u64, Ordering::Relaxed);
    }
    if offset != payload {
        return Err(format!(
            "abort truncated: received {offset} of {payload} bytes"
        ));
    }
    Ok(())
}

/// `Open`/`CloseRead`/`CloseWrite` churn racing in-flight data. The soak sends
/// its control frames in one order (write, then shutdown) against a peer that
/// has already drained; these do not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standard tier: control-frame races against in-flight data"]
async fn control_race_family() {
    let total = cycles();
    let mut session = JoinSet::new();
    let mut endpoint = spawn_pair(&mut session, false);
    let inflight = Arc::new(Inflight::default());
    let completed = Arc::new(AtomicU64::new(0));
    let mut rng = base_seed();
    let mut passes = 0u64;
    let mut worst = Duration::ZERO;

    for cycle in 0..total {
        let start = Instant::now();
        let mut jobs = Cycle::new(Arc::clone(&inflight), Arc::clone(&completed));

        // Open churn: `Open` racing both close directions with no data.
        for churn in 0..2 {
            let (client_reader, client_writer) = endpoint.opener.open().await.unwrap();
            let _ = endpoint.accepter.accept().await.unwrap();
            jobs.spawn(format!("c{cycle} churn#{churn}"), move |_meter| {
                open_and_abandon(client_reader, client_writer)
            });
        }

        // A reader dropped while the peer is mid-bulk: CloseRead against data.
        {
            let (client_reader, client_writer) = endpoint.opener.open().await.unwrap();
            let (server_reader, server_writer) = endpoint.accepter.accept().await.unwrap();
            jobs.spawn(format!("c{cycle} bulk-race-client"), move |meter| {
                reader_dropped_mid_bulk(client_writer, client_reader, meter, 64 * 1024)
            });
            jobs.spawn(format!("c{cycle} bulk-race-server"), move |_meter| {
                reader_gone_peer_writer(server_writer, server_reader)
            });
        }

        // A writer dropped on its own staged tail: the FIN racing pending data.
        {
            let payload = 4096 + (splitmix64(&mut rng) % 1024) as usize;
            let seed = base_seed()
                .wrapping_add(cycle)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let (client_reader, client_writer) = endpoint.opener.open().await.unwrap();
            let (server_reader, _server_writer) = endpoint.accepter.accept().await.unwrap();
            drop(client_reader);
            jobs.spawn(
                format!("c{cycle} abort-client payload={payload}"),
                move |meter| aborted_writer_after_staging(client_writer, meter, seed, payload),
            );
            jobs.spawn(
                format!("c{cycle} abort-server payload={payload}"),
                move |meter| aborted_peer_reader(server_reader, meter, seed, payload),
            );
        }

        match jobs.run(&format!("cycle {cycle}")).await {
            Ok(()) => {
                passes += 1;
                worst = worst.max(start.elapsed());
            }
            Err(message) => panic!("control race family failed: {message}"),
        }
    }

    println!(
        "control_race_family: cycles={passes}/{total} worst_cycle={worst:?} \
         jobs={}",
        completed.load(Ordering::Relaxed)
    );
    assert!(passes > 0, "family ran no cycles");
}

// ─── family 4: reassembly liveness under out-of-order frame delivery ───────

/// Runtime-liveness heartbeat, the soak's instrument: a task ticks every
/// 10 ms and the age at a stall verdict separates a genuinely parked session
/// (the runtime kept scheduling) from host starvation (the runtime itself
/// stopped). It asserts nothing.
#[derive(Debug)]
struct Heartbeat {
    start: std::time::Instant,
    last_tick_ms: AtomicU64,
    max_gap_ms: AtomicU64,
    ticks: AtomicU64,
}

impl Heartbeat {
    fn new() -> Self {
        Self {
            start: std::time::Instant::now(),
            last_tick_ms: AtomicU64::new(0),
            max_gap_ms: AtomicU64::new(0),
            ticks: AtomicU64::new(0),
        }
    }
    fn elapsed_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
    fn tick(&self) {
        let now = self.elapsed_ms();
        let previous = self.last_tick_ms.swap(now, Ordering::Relaxed);
        let gap = now.saturating_sub(previous);
        self.max_gap_ms.fetch_max(gap, Ordering::Relaxed);
        self.ticks.fetch_add(1, Ordering::Relaxed);
    }
    fn report(&self) -> String {
        let age = self
            .elapsed_ms()
            .saturating_sub(self.last_tick_ms.load(Ordering::Relaxed));
        format!(
            "heartbeat_age={age}ms max_gap={}ms ticks={}",
            self.max_gap_ms.load(Ordering::Relaxed),
            self.ticks.load(Ordering::Relaxed),
        )
    }
}

async fn heartbeat_task(heartbeat: Arc<Heartbeat>) {
    loop {
        tokio::time::sleep(Duration::from_millis(10)).await;
        heartbeat.tick();
    }
}

/// The soak's post-stall probe: a fresh one-round ping-pong on the same
/// session. If it completes, the session is alive and the stall was
/// stream-local; if it hangs, the session itself is wedged. Every step is
/// bounded, including the setup: the session this probe is asking about may be
/// the wedged one, so an unbounded probe would hang instead of reporting.
async fn probe_after_stall(endpoint: &mut Endpoint) -> Result<(), String> {
    let probe_bound = Duration::from_secs(2);
    let (mut client_reader, mut client_writer) = timeout(probe_bound, endpoint.opener.open())
        .await
        .map_err(|_| "probe open hung: the session itself is wedged".to_owned())?
        .map_err(|e| format!("probe open: {e:?}"))?;
    let (mut server_reader, mut server_writer) = timeout(probe_bound, endpoint.accepter.accept())
        .await
        .map_err(|_| {
            "probe accept hung after the probe open: the session itself is wedged".to_owned()
        })?
        .map_err(|e| format!("probe accept: {e:?}"))?;
    let client = async move {
        client_writer
            .write_all(b"probe")
            .await
            .map_err(|e| format!("probe write: {e:?}"))?;
        client_writer
            .shutdown()
            .map_err(|e| format!("probe shutdown: {e:?}"))?;
        let mut echo = [0u8; 5];
        client_reader
            .read_exact(&mut echo)
            .await
            .map_err(|e| format!("probe echo read: {e:?}"))?;
        let mut trailing = [0u8; 1];
        let n = client_reader
            .read(&mut trailing)
            .await
            .map_err(|e| format!("probe eof read: {e:?}"))?;
        if n != 0 {
            return Err("probe: peer data after FIN".to_owned());
        }
        Ok::<(), String>(())
    };
    let server = async move {
        let mut request = [0u8; 5];
        server_reader
            .read_exact(&mut request)
            .await
            .map_err(|e| format!("probe request read: {e:?}"))?;
        server_writer
            .write_all(&request)
            .await
            .map_err(|e| format!("probe echo write: {e:?}"))?;
        Ok::<(), String>(())
    };
    match timeout(probe_bound, async {
        let (client, server) = tokio::join!(client, server);
        client.and(server)
    })
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(message)) => Err(message),
        Err(_) => Err("probe hung: the session itself is wedged".to_owned()),
    }
}

/// Drop the read half once a prefix of a multi-frame message has been
/// reassembled: the rest of the message is still in flight or still held in
/// the reorder buffer, so the peer's write half must be told rather than left
/// staging into a stream nobody will read.
async fn reader_dropped_mid_reassembly(
    mut reader: StreamReader,
    meter: Arc<JobMeter>,
    prefix: usize,
) -> Result<(), String> {
    let mut buf = vec![0u8; prefix];
    if !read_exact_or_eof(&mut reader, &mut buf)
        .await
        .map_err(|e| format!("reassembly prefix read: {e:?}"))?
    {
        return Err(format!(
            "reassembly prefix: clean EOF before {prefix} bytes"
        ));
    }
    meter.received.fetch_add(prefix as u64, Ordering::Relaxed);
    drop(reader);
    Ok(())
}

/// Keep staging a multi-frame message until the peer's read-side close reaches
/// this write half. The peer's close is the only thing that ends the loop, so
/// a close that never arrives parks here — and the cycle bound reports that
/// park as a stall rather than this shape papering over it with a bound of its
/// own. The bounded frame window keeps what it stages finite while it waits.
async fn writer_until_peer_close(
    mut writer: StreamWriter,
    meter: Arc<JobMeter>,
) -> Result<(), String> {
    let chunk = [0x77u8; 16 * 1024];
    while writer.write_all(&chunk).await.is_ok() {
        meter
            .staged
            .fetch_add(chunk.len() as u64, Ordering::Relaxed);
    }
    Ok(())
}

/// The four halves of one freshly opened stream pair, grouped so the shapes
/// below take them as one value.
struct Pair {
    client_reader: StreamReader,
    client_writer: StreamWriter,
    server_reader: StreamReader,
    server_writer: StreamWriter,
}

/// Open one stream pair under the cycle bound. Stream setup is part of the
/// cycle: an `open` or an `accept` that never completes stalls the cycle just
/// as much as a job that never finishes, and a bound that starts counting only
/// after the setup would report neither. `open` and `accept` are bounded
/// separately so a stall names which side parked.
async fn open_pair(
    opener: &mut StreamOpener,
    accepter: &mut StreamAccepter,
    bound: Duration,
) -> Result<Pair, String> {
    let (client_reader, client_writer) = timeout(bound, opener.open())
        .await
        .map_err(|_| format!("open did not complete within {bound:?}"))?
        .map_err(|e| format!("open: {e:?}"))?;
    let (server_reader, server_writer) = timeout(bound, accepter.accept())
        .await
        .map_err(|_| format!("accept did not complete within {bound:?} after the open did"))?
        .map_err(|e| format!("accept: {e:?}"))?;
    Ok(Pair {
        client_reader,
        client_writer,
        server_reader,
        server_writer,
    })
}

/// Open one pair for a shape, reporting a setup that never completes as the
/// cycle failure it is.
async fn open_or_report(
    endpoint: &mut Endpoint,
    inflight: &Inflight,
    heartbeat: &Heartbeat,
    egress_before: &mux::live_probe::Totals,
    cycle: u64,
    shape: &str,
) -> Pair {
    match open_pair(&mut endpoint.opener, &mut endpoint.accepter, CYCLE_BOUND).await {
        Ok(pair) => pair,
        Err(detail) => {
            report_failure(
                endpoint,
                inflight,
                heartbeat,
                egress_before,
                format!("cycle {cycle} stream setup for {shape} did not complete: {detail}"),
            )
            .await
        }
    }
}

/// Report a cycle that did not complete, with the evidence the soak prints at
/// a stall: the jobs still in flight with their staged/received byte counts,
/// the runtime heartbeat, the egress/ingress counter delta for this cycle, the
/// per-stream byte ledger, and whether a fresh stream on the same session
/// still works.
async fn report_failure(
    endpoint: &mut Endpoint,
    inflight: &Inflight,
    heartbeat: &Heartbeat,
    egress_before: &mux::live_probe::Totals,
    detail: String,
) -> ! {
    let probe = probe_after_stall(endpoint).await;
    panic!(
        "reassembly gap family failed: {detail}; in-flight: {:?}; {}; egress delta: {}; \
         stream trace: {}; post-stall probe: {}",
        inflight.snapshot(),
        heartbeat.report(),
        mux::live_probe::totals().since(egress_before),
        mux::live_probe::stream_trace_report(),
        match probe {
            Ok(()) => "a fresh one-round ping-pong completed (stream-local stall)".to_owned(),
            Err(message) => format!("failed: {message}"),
        },
    );
}

/// Frame-reassembly liveness. Every other instrument in this crate (the soak
/// and the three families above) runs the stock wire with `frame_reassembly`
/// off, so a lost wakeup, a stalled close, or a dropped reader *while the
/// reorder buffer holds a gap* would be invisible to all of them.
///
/// This family runs both sessions with `frame_reassembly` on over a transport
/// whose two directions deliver frames out of sent order (the deployment's
/// frame fast-forward), and asserts the soak's properties — every staged byte
/// received in order, every job complete, every cycle inside the bound with a
/// never-completing cycle reported as a hang — over the shapes the reassembly
/// path exists for:
///
/// * interactive ping-pong interleaved with a multi-frame bulk message on one
///   session;
/// * a message whose frames arrive out of order, and across multi-frame gaps;
/// * `Fin` racing an in-flight message (the extended `CloseWrite` final offset
///   arriving while the last data frame is still buffered);
/// * the reader dropped mid-reassembly with a gap outstanding.
///
/// `MUX_FAMILY_CYCLES` and `MUX_FAMILY_SEED` widen the run. `MUX_FAMILY_STRAND=n`
/// is the red-proof mode, not a normal one: the n-th data frame in each
/// direction is never released, which must turn the family red.
///
/// Current status: green at the default 400 cycles, **red at >=13 000**. Every
/// run of 14 000 cycles wedges the session between cycle 11 390 and 12 528 —
/// the client's `open` completes but the peer's `accept` never does, because
/// the receiver's stream table has grown to `MAX_CONCURRENT_STREAMS` under
/// `frame_reassembly` and admission is then refused silently for the life of
/// the session. The same shapes with `frame_reassembly = false` keep the table
/// flat, so a green 400-cycle run clears these shapes at that length, not the
/// reassembly path. See `GATE.md` for the reproduction command and the
/// evidence; the leak itself is not fixed here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standard tier: frame-reassembly liveness under out-of-order frame delivery"]
async fn reassembly_gap_family() {
    let total = cycles();
    let strand = match env_u64("MUX_FAMILY_STRAND", 0) {
        0 => None,
        n => Some(n),
    };
    let mut session = JoinSet::new();
    let mut endpoint = spawn_pair_reordered(&mut session, strand);
    let stats = Arc::clone(&endpoint.stats);
    let inflight = Arc::new(Inflight::default());
    let completed = Arc::new(AtomicU64::new(0));
    let heartbeat = Arc::new(Heartbeat::new());
    let mut monitors = JoinSet::new();
    monitors.spawn(heartbeat_task(Arc::clone(&heartbeat)));

    // The stall verdict names the stage a stream parked in; the per-stream
    // ledger is reset per cycle so a stall reads this cycle's streams, and the
    // egress delta is the parked cycle's own contribution.
    mux::live_probe::enable_stream_trace();

    let mut rng = base_seed();
    let mut seed = base_seed() ^ 0x00C0_FFEE_1234_5678;
    let mut passes = 0u64;
    let mut worst = Duration::ZERO;
    let mut interleaved = 0u64;
    let mut fin_races = 0u64;
    let mut dropped_readers = 0u64;

    for cycle in 0..total {
        let start = Instant::now();
        let egress_before = mux::live_probe::totals();
        mux::live_probe::reset_stream_trace();
        let mut jobs = Cycle::new(Arc::clone(&inflight), Arc::clone(&completed));

        // Interactive ping-pong interleaved with a multi-frame bulk message on
        // the same session: the reassembly path's whole purpose is that the
        // bulk stream's reordered frames do not stall the interactive stream.
        {
            let rounds = 2 + (splitmix64(&mut rng) % 3) as usize;
            seed = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let Pair {
                client_reader,
                client_writer,
                server_reader,
                server_writer,
            } = open_or_report(
                &mut endpoint.endpoint,
                &inflight,
                &heartbeat,
                &egress_before,
                cycle,
                "interleaved ping",
            )
            .await;
            jobs.spawn(
                format!("c{cycle} interleaved ping rounds={rounds}"),
                move |meter| {
                    session_ping_pong(
                        Halves {
                            client_reader,
                            client_writer,
                            server_reader,
                            server_writer,
                        },
                        meter,
                        seed,
                        rounds,
                        16,
                    )
                },
            );
            let bulk_seed = seed.wrapping_add(0x51ED_2701_1111_1111);
            let Pair {
                client_reader,
                client_writer,
                server_reader,
                server_writer: _server_writer,
            } = open_or_report(
                &mut endpoint.endpoint,
                &inflight,
                &heartbeat,
                &egress_before,
                cycle,
                "interleaved bulk",
            )
            .await;
            drop(client_reader);
            jobs.spawn(
                format!("c{cycle} interleaved bulk-client payload={REASSEMBLY_BULK}"),
                move |meter| {
                    aborted_writer_after_staging(client_writer, meter, bulk_seed, REASSEMBLY_BULK)
                },
            );
            jobs.spawn(
                format!("c{cycle} interleaved bulk-server payload={REASSEMBLY_BULK}"),
                move |meter| aborted_peer_reader(server_reader, meter, bulk_seed, REASSEMBLY_BULK),
            );
            interleaved += 1;
        }

        // `Fin` racing an in-flight multi-frame message, alone, so the race is
        // not diluted by the concurrent ping-pong.
        if cycle.is_multiple_of(2) {
            seed = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let Pair {
                client_reader,
                client_writer,
                server_reader,
                server_writer: _server_writer,
            } = open_or_report(
                &mut endpoint.endpoint,
                &inflight,
                &heartbeat,
                &egress_before,
                cycle,
                "fin-race bulk",
            )
            .await;
            drop(client_reader);
            jobs.spawn(
                format!("c{cycle} fin-race bulk-client payload={REASSEMBLY_BULK}"),
                move |meter| {
                    aborted_writer_after_staging(client_writer, meter, seed, REASSEMBLY_BULK)
                },
            );
            jobs.spawn(
                format!("c{cycle} fin-race bulk-server payload={REASSEMBLY_BULK}"),
                move |meter| aborted_peer_reader(server_reader, meter, seed, REASSEMBLY_BULK),
            );
            fin_races += 1;
        }

        // The reader dropped mid-reassembly: a prefix has been reassembled,
        // the rest of the message is still in flight or still held in the
        // reorder buffer, and the peer's write half must be told.
        if cycle.is_multiple_of(3) {
            let Pair {
                client_reader,
                client_writer,
                server_reader,
                server_writer,
            } = open_or_report(
                &mut endpoint.endpoint,
                &inflight,
                &heartbeat,
                &egress_before,
                cycle,
                "drop-reader bulk",
            )
            .await;
            drop(client_reader);
            drop(server_writer);
            jobs.spawn(format!("c{cycle} drop-reader bulk-client"), move |meter| {
                writer_until_peer_close(client_writer, meter)
            });
            jobs.spawn(
                format!("c{cycle} drop-reader bulk-server prefix={DROP_READER_PREFIX}"),
                move |meter| {
                    reader_dropped_mid_reassembly(server_reader, meter, DROP_READER_PREFIX)
                },
            );
            dropped_readers += 1;
        }

        let detail = match jobs.run_verdict().await {
            CycleVerdict::Pass => {
                passes += 1;
                worst = worst.max(start.elapsed());
                continue;
            }
            CycleVerdict::Failed { elapsed, message } => {
                format!("cycle {cycle} FAILED after {elapsed:?}: {message}")
            }
            CycleVerdict::Late { elapsed, message } => {
                let detail = message
                    .map(|message| format!("; job error: {message}"))
                    .unwrap_or_default();
                format!("cycle {cycle} completed LATE: {elapsed:?} > {CYCLE_BOUND:?}{detail}")
            }
            CycleVerdict::Hang {
                elapsed,
                finished,
                message,
                in_flight,
            } => {
                let detail = message
                    .map(|message| format!("; job error: {message}"))
                    .unwrap_or_default();
                format!(
                    "cycle {cycle} HUNG: never completed within {elapsed:?}, {finished} job(s) \
                     finished{detail}; in-flight: {in_flight:?}"
                )
            }
        };
        // A stall is the finding: report what parked and whether the session
        // survived it, rather than only that a cycle did not complete.
        report_failure(
            &mut endpoint.endpoint,
            &inflight,
            &heartbeat,
            &egress_before,
            detail,
        )
        .await;
    }

    let (frames, reorders, overtaken, max_gap, close_overtakes, injected_holds) = stats.snapshot();
    println!(
        "reassembly_gap_family: cycles={passes}/{total} interleaved={interleaved} \
         fin_races={fin_races} dropped_readers={dropped_readers} worst_cycle={worst:?} \
         jobs={} frames={frames} reorders={reorders} overtaken={overtaken} \
         max_gap={max_gap} close_overtakes={close_overtakes} injected_holds={injected_holds}; {}",
        completed.load(Ordering::Relaxed),
        heartbeat.report(),
    );
    endpoint.tasks.abort_all();
    monitors.abort_all();
    assert!(passes > 0, "family ran no cycles");
    // A green run only means anything if the transport really delivered frames
    // out of order: with no reorder the family never reaches the reorder
    // buffer's gap-filling path and proves nothing about it.
    assert!(
        reorders > 0,
        "the reorderer delivered no frame out of order, so the family did not \
         exercise reassembly at all"
    );
}
