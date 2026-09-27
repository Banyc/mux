//! Long-lived-session growth soak: does anything a session holds *per stream*
//! grow, or wedge, over a long session with heavy churn and adversarial
//! traffic?
//!
//! The operator's client multiplexes everything over one long-lived mux
//! session, and the two worst defects this workspace has found — the
//! fair-queue lost wakeup and the frame-reassembly stream-table retention —
//! were both invisible to a short session. The retention one was not a slow
//! leak but a permanent wedge: the receiver's stream table filled with entries
//! that `is_closed()` already reported finished, and `MuxControl::open` then
//! refused every later stream for the life of the session.
//!
//! This soak asks a *different* question from `spike_survival_soak` (which
//! asks whether a spike is survivable) on the same instrument: with streams
//! opening and closing continuously, does the live count of any per-stream
//! structure grow, and does the session always return to full function?
//!
//! Evidence, not impression:
//!
//! - `live_probe::structure_censuses` reports, per session role, the live
//!   count of every per-stream structure the control loop holds: stream-table
//!   entries, reorder buffers, their pending frame and byte totals, open read
//!   sinks, entries already `is_closed()` (the retention signature), and the
//!   retired-peer-id window.
//! - `live_probe::egress_token_censuses` reports, per role, the live size of
//!   the egress path's token-keyed maps: the fair queue's per-token queue
//!   table and ready map, the cached heads, and the token→stream and deficit
//!   maps. The fair queue admits exactly `MAX_QUEUE_COUNT` queues and stops
//!   announcing opens once full, so an unreaped token queue is a permanent
//!   admission loss, not a slow leak.
//! - `live_probe::admission_ledgers` reports, per role, the table's insert and
//!   retire totals, so a saturated table can be read as steady state or as
//!   monotone growth.
//!
//! The soak runs `frame_reassembly = true` — the mode the retention defect was
//! specific to — over a plain in-memory duplex. Frames arrive in order there,
//! so the reorder buffer's *gap* dimension is not this arm's: that is
//! `interactive_liveness_families::reassembly_gap_family`'s. What this arm
//! covers is the *release* question: every frame is ingested into and drained
//! out of a reorder buffer, every stream is closed in a different order, and
//! the census must return to zero.
//!
//! Tier: **standard** (`#[ignore]`d). `MUX_GROWTH_ROUNDS` and
//! `MUX_GROWTH_CHECKPOINT` size the soak. `MUX_GROWTH_FAULT=leak_stream`
//! reinjects a retention from the test's own side (see [`faults`]).

use std::{
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
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf},
    task::JoinSet,
    time::Instant,
};

/// Production heartbeat on the deployed path; the steady receive deadline is
/// `4 x` this, so an idle round of this length exercises the heartbeat and
/// resets the deadline without crossing it.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// A slow-consumer phase may legitimately block on a full read queue; this is
/// how long it is allowed to before the phase gives up and drops the stream.
const PHASE_TIMEOUT: Duration = Duration::from_secs(5);
/// The field's mid-range spike, applied by the stall phase.
const SPIKE: Duration = Duration::from_millis(1063);

/// Streams completed before the first checkpoint, and the multiple at which
/// the second is taken. Matched points: a structure whose live count at the
/// second exceeds its count at the first grew with the session.
const DEFAULT_CHECKPOINT: u64 = 200;
const CHECKPOINT_MULTIPLE: u64 = 10;
/// Streams each echo round opens concurrently.
const ECHO_STREAMS: u64 = 4;
/// Streams each closed-without-reading burst opens.
const BURST_STREAMS: u64 = 3;

/// `RETIRED_FINISHED_PEER_STREAM_WINDOW` in `src/control.rs`.
const RETIRED_WINDOW_MAX: u64 = 1024;

// ─── the fault switch ──────────────────────────────────────────────────────

/// Red-proof modes. `leak_stream` makes each echo round leak one stream's
/// peer-side half, so its table entry is never released and the growth
/// assertion must fail naming the structure and both counts. `no_stall`
/// disables the stall phase's hold, so the "the spike was applied" check must
/// fail. Both are test-side faults: they remove a property the soak relies on
/// rather than re-expressing it.
mod faults {
    use std::sync::atomic::{AtomicBool, Ordering};

    pub static LEAK_STREAM: AtomicBool = AtomicBool::new(false);
    pub static NO_STALL: AtomicBool = AtomicBool::new(false);

    pub fn enabled() -> bool {
        LEAK_STREAM.load(Ordering::SeqCst) || NO_STALL.load(Ordering::SeqCst)
    }
}

// ─── the stall gate ────────────────────────────────────────────────────────

struct StallSwitch {
    stalled: AtomicBool,
    waker: Mutex<Option<Waker>>,
    delivered_while_stalled: AtomicU64,
    held_polls: AtomicU64,
}

impl StallSwitch {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            stalled: AtomicBool::new(false),
            waker: Mutex::new(None),
            delivered_while_stalled: AtomicU64::new(0),
            held_polls: AtomicU64::new(0),
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

struct StallGate<R> {
    inner: R,
    switch: Arc<StallSwitch>,
}

impl<R: AsyncRead + Unpin> AsyncRead for StallGate<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.switch.is_stalled() && !faults::NO_STALL.load(Ordering::SeqCst) {
            *self.switch.waker.lock().unwrap() = Some(cx.waker().clone());
            if self.switch.is_stalled() {
                self.switch.held_polls.fetch_add(1, Ordering::SeqCst);
                return Poll::Pending;
            }
        }
        let before = buf.filled().len();
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        if self.switch.is_stalled() && buf.filled().len() > before {
            self.switch
                .delivered_while_stalled
                .fetch_add((buf.filled().len() - before) as u64, Ordering::SeqCst);
        }
        poll
    }
}

// ─── the session pair ──────────────────────────────────────────────────────

struct Pair {
    opener: StreamOpener,
    accepter: StreamAccepter,
    teardowns: JoinSet<MuxError>,
    _server_teardowns: JoinSet<MuxError>,
}

impl Pair {
    fn spawn(switch: Arc<StallSwitch>) -> Self {
        let (a, b) = tokio::io::duplex(1 << 20);
        let (client_read, client_write) = tokio::io::split(a);
        let (server_read, server_write) = tokio::io::split(b);
        let client_read = StallGate {
            inner: client_read,
            switch: Arc::clone(&switch),
        };
        let server_read = StallGate {
            inner: server_read,
            switch,
        };

        let mut client_config = MuxConfig::new(Initiation::Client, HEARTBEAT_INTERVAL);
        client_config.frame_reassembly = true;
        let mut teardowns = JoinSet::new();
        let (opener, _) =
            spawn_mux_no_reconnection(client_read, client_write, client_config, &mut teardowns);

        let mut server_config = MuxConfig::new(Initiation::Server, HEARTBEAT_INTERVAL);
        server_config.frame_reassembly = true;
        let mut server_teardowns = JoinSet::new();
        let (_, accepter) = spawn_mux_no_reconnection(
            server_read,
            server_write,
            server_config,
            &mut server_teardowns,
        );

        Self {
            opener,
            accepter,
            teardowns,
            _server_teardowns: server_teardowns,
        }
    }

    fn tear_down_reason(&mut self) -> Option<MuxError> {
        if let Some(joined) = self.teardowns.try_join_next() {
            return Some(joined.expect("client session task panicked"));
        }
        if let Some(joined) = self._server_teardowns.try_join_next() {
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
    /// that must complete. Progress is asserted per round, so a stall in the
    /// middle of the soak fails where it happens instead of being averaged.
    async fn echo_probe(&mut self, payload: &[u8], leak: bool) -> io::Result<()> {
        let (mut client_reader, mut client_writer, mut server_reader, mut server_writer) =
            self.open_pair().await?;
        client_writer.write_all(payload).await?;
        AsyncWriteExt::shutdown(&mut client_writer).await?;
        let mut received = Vec::new();
        server_reader.read_to_end(&mut received).await?;
        if received != payload {
            return Err(io::Error::other("payload integrity"));
        }
        server_writer.write_all(&received).await?;
        AsyncWriteExt::shutdown(&mut server_writer).await?;
        let mut echoed = Vec::new();
        client_reader.read_to_end(&mut echoed).await?;
        if echoed != payload {
            return Err(io::Error::other("echo integrity"));
        }
        if leak {
            // Fault mode: the peer's two halves are leaked, so the stream's
            // local read and write closes never fire and its table entry is
            // retained for the life of the session.
            Box::leak(Box::new(server_reader));
            Box::leak(Box::new(server_writer));
        }
        Ok(())
    }

    /// An echo phase: `ECHO_STREAMS` concurrent full request/response rounds.
    async fn echo_round(&mut self, payload: &[u8]) -> io::Result<u64> {
        let mut completed = 0;
        for _ in 0..ECHO_STREAMS {
            self.echo_probe(payload, false).await?;
            completed += 1;
        }
        Ok(completed)
    }
}

// ─── phases ────────────────────────────────────────────────────────────────

/// One round's adversarial shape. The schedule is fixed by the round index, so
/// a green run is a statement about this phase family, not about a random
/// draw; the recovery probe inside every phase is what makes the schedule
/// "one varying dimension per arm" rather than a confounded composite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Baseline churn: concurrent full echo rounds.
    Echo,
    /// The peer accepts and then stops reading: a slow consumer against a
    /// bounded read queue. The shape that leaked before.
    SlowConsumer,
    /// Streams dropped mid-transfer, with neither a clean shutdown nor a read.
    DroppedMidTransfer,
    /// A burst of streams that are written, closed, and never read.
    ClosedWithoutReading,
    /// A transport stall at the field's mid-range magnitude, then recovery.
    Spike,
}

const SCHEDULE: [Phase; 8] = [
    Phase::Echo,
    Phase::SlowConsumer,
    Phase::Echo,
    Phase::DroppedMidTransfer,
    Phase::ClosedWithoutReading,
    Phase::Echo,
    Phase::Spike,
    Phase::Echo,
];

fn phase_for_round(round: u64) -> Phase {
    SCHEDULE[(round as usize) % SCHEDULE.len()]
}

#[derive(Debug, Default, Clone, Copy)]
struct PhaseCounts {
    opened: u64,
    completed: u64,
    phase_applied: u64,
}

/// A slow consumer: the peer accepts and holds its two halves without ever
/// reading, so the writer fills the bounded read queue and parks. The phase
/// times out (on the simulated clock) rather than hanging, then drops the
/// stream — the peer that stopped reading is released, not waited for.
async fn phase_slow_consumer(pair: &mut Pair, payload: &[u8]) -> PhaseCounts {
    let mut counts = PhaseCounts {
        phase_applied: 1,
        ..Default::default()
    };
    let Ok((client_reader, mut client_writer, server_reader, server_writer)) =
        pair.open_pair().await
    else {
        return counts;
    };
    counts.opened += 1;
    let write = async {
        let _ = client_writer.write_all(payload).await;
        let _ = AsyncWriteExt::shutdown(&mut client_writer).await;
    };
    let _ = tokio::time::timeout(PHASE_TIMEOUT, write).await;
    // Drop every half: the peer never read a byte.
    drop(server_reader);
    drop(server_writer);
    drop(client_reader);
    drop(client_writer);
    counts
}

/// Streams dropped mid-transfer: the client has begun writing and both ends
/// are dropped with no shutdown, so the peer's `CloseWrite` never carries a
/// final offset and the reader is dropped mid-flight. The client's two halves
/// go first, so any release the peer's later frames can still cause is the one
/// this phase is here to exercise.
async fn phase_dropped_mid_transfer(pair: &mut Pair, payload: &[u8]) -> PhaseCounts {
    let mut counts = PhaseCounts {
        phase_applied: 1,
        ..Default::default()
    };
    let Ok((client_reader, client_writer, server_reader, server_writer)) = pair.open_pair().await
    else {
        return counts;
    };
    counts.opened += 1;
    let mut writer = client_writer;
    let write = async {
        let _ = writer.write_all(payload).await;
    };
    let _ = tokio::time::timeout(PHASE_TIMEOUT, write).await;
    // The client's read and write halves close first; the peer's frames then
    // arrive at a stream whose every local side is already closed, which is
    // the ordering in which the peer's final `CloseWrite` is the only event
    // left that can release the entry.
    drop(client_reader);
    drop(writer);
    drop(server_reader);
    drop(server_writer);
    counts
}

/// A burst of streams that are written and closed but never read: the client's
/// halves are dropped immediately after it has written and shut down, and the
/// peer's halves are dropped with no read at all. Half the burst drops the
/// client's halves first, so the peer's `CloseRead`/`CloseWrite` arrive at a
/// stream with all its local sides closed — the ordering the frame-reassembly
/// retention defect needed — and half drops the peer's first, so a release
/// driven by a later local close is covered too.
async fn phase_closed_without_reading(pair: &mut Pair, payload: &[u8]) -> PhaseCounts {
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
        let _ = client_writer.write_all(payload).await;
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

// ─── checkpoints ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
struct Checkpoint {
    label: &'static str,
    completed_streams: u64,
    structures: mux::live_probe::StructureCensuses,
    egress: mux::live_probe::EgressTokenCensuses,
    ledger: mux::live_probe::AdmissionLedgers,
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
            teardowns,
        }
    }
}

impl std::fmt::Display for Checkpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "checkpoint[{}] completed_streams={} teardowns={}\n  {}\n  {}\n  {}",
            self.label,
            self.completed_streams,
            self.teardowns,
            self.structures,
            self.egress,
            self.ledger,
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

/// The growth half: no per-stream structure may hold more live state at a
/// later matched point than at an earlier one. `live_stream_structures` sums
/// every field except the bounded retired-id window, so a leak in any of them
/// (or a new one that is added to the struct) is caught by the sum even if it
/// is not named by a field-specific assertion yet.
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

fn rounds() -> u64 {
    let checkpoint = checkpoint_streams();
    // The fixed schedule completes 3 streams per round on average (two echo
    // rounds per eight, four streams each, plus the one-stream recovery probe
    // every round), so this is the round count that reaches the second matched
    // point, plus one schedule's worth of headroom.
    let derived = checkpoint * CHECKPOINT_MULTIPLE / 3 + SCHEDULE.len() as u64;
    std::env::var("MUX_GROWTH_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(derived)
}

/// Every phase runs under a bound on the simulated clock: a session that
/// wedges must fail the round where it wedged rather than hang the target.
/// The bound is generous against the phase's own deliberate
/// [`PHASE_TIMEOUT`], and the failure names the phase and the round.
async fn bounded<F>(round: u64, phase: Phase, fut: F) -> F::Output
where
    F: std::future::Future,
{
    match tokio::time::timeout(PHASE_TIMEOUT * 4, fut).await {
        Ok(value) => value,
        Err(_) => panic!(
            "round {round}: the {phase:?} phase did not finish within {:?} of simulated 
             time — the session stopped making progress",
            PHASE_TIMEOUT * 4
        ),
    }
}

fn checkpoint_streams() -> u64 {
    std::env::var("MUX_GROWTH_CHECKPOINT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_CHECKPOINT)
}

/// The growth assertion has teeth on its own, independent of the per-checkpoint
/// zero assertions: given a matched pair whose later census holds more live
/// per-stream state, it must panic and name the structure and both counts.
/// This is the vacuity half of [`assert_not_grown`], kept in the suite so the
/// assertion cannot be weakened into one that always passes.
#[test]
fn the_growth_assertion_rejects_a_grown_census() {
    let first = Checkpoint {
        label: "synthetic-first",
        completed_streams: 100,
        structures: mux::live_probe::StructureCensuses::default(),
        egress: mux::live_probe::EgressTokenCensuses::default(),
        ledger: mux::live_probe::AdmissionLedgers::default(),
        teardowns: 0,
    };
    let mut later = first;
    later.label = "synthetic-later";
    later.completed_streams = 1000;
    later.structures.client.reassembly_buffers = 7;

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
        teardowns: 0,
    };
    checkpoint.structures.server.closed_but_retained = 1;
    checkpoint.structures.server.stream_table_len = 1;

    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let payload = std::panic::catch_unwind(|| assert_released(&checkpoint));
    std::panic::set_hook(previous_hook);

    let payload = payload.expect_err("a retained closed entry must fail the release assertion");
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_default();
    assert!(
        message.contains("retains 1 stream-table entr(ies) that `is_closed` already reports"),
        "the failure must name the retention and the structure: {message}",
    );
    assert!(
        message.contains("closed_but_retained=1"),
        "the failure must print the census it read: {message}",
    );
}

/// Drive the runtime to quiescence so every close the soak issued has been
/// applied and every egress token reaped before the census is read.
async fn settle() {
    for _ in 0..1024 {
        tokio::task::yield_now().await;
    }
}

// ─── the soak ──────────────────────────────────────────────────────────────

/// A long-lived session with heavy churn and adversarial phases releases every
/// per-stream structure it allocates, and returns to full function after every
/// phase.
#[tokio::test(start_paused = true)]
#[ignore = "standard tier: long-lived-session churn/retention soak"]
async fn a_long_lived_session_releases_every_per_stream_structure() {
    if std::env::var("MUX_GROWTH_FAULT").as_deref() == Ok("leak_stream") {
        faults::LEAK_STREAM.store(true, Ordering::SeqCst);
    }
    if std::env::var("MUX_GROWTH_FAULT").as_deref() == Ok("no_stall") {
        faults::NO_STALL.store(true, Ordering::SeqCst);
    }
    mux::live_probe::enable_structure_census();

    let checkpoint_at = checkpoint_streams();
    let rounds = rounds();
    let wall_start = StdInstant::now();
    let sim_start = Instant::now();

    let switch = StallSwitch::new();
    let mut pair = Pair::spawn(Arc::clone(&switch));
    let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    let big_payload: Vec<u8> = (0..64 * 1024u32).map(|i| (i % 251) as u8).collect();

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

    for round in 0..rounds {
        last_round = round;
        let phase = phase_for_round(round);
        let counts = match phase {
            Phase::Echo => {
                let completed_here = bounded(round, phase, pair.echo_round(&payload))
                    .await
                    .unwrap_or_else(|e| panic!("round {round}: echo phase failed: {e}"));
                PhaseCounts {
                    opened: ECHO_STREAMS,
                    completed: completed_here,
                    phase_applied: 1,
                }
            }
            Phase::SlowConsumer => {
                bounded(round, phase, phase_slow_consumer(&mut pair, &big_payload)).await
            }
            Phase::DroppedMidTransfer => {
                bounded(
                    round,
                    phase,
                    phase_dropped_mid_transfer(&mut pair, &big_payload),
                )
                .await
            }
            Phase::ClosedWithoutReading => {
                bounded(
                    round,
                    phase,
                    phase_closed_without_reading(&mut pair, &payload),
                )
                .await
            }
            Phase::Spike => {
                bounded(round, phase, async {
                    settle().await;
                    switch.stall();
                    let withheld_before = switch.delivered_while_stalled.load(Ordering::SeqCst);
                    let held_before = switch.held_polls.load(Ordering::SeqCst);
                    tokio::time::advance(SPIKE).await;
                    settle().await;
                    let withheld_during = switch.delivered_while_stalled.load(Ordering::SeqCst);
                    let held_during = switch.held_polls.load(Ordering::SeqCst);
                    switch.release();
                    settle().await;
                    assert_eq!(
                        withheld_during,
                        withheld_before,
                        "round {round}: the {SPIKE:?} stall delivered {} byte(s) while 
                         stalled, so the stall was not applied",
                        withheld_during - withheld_before,
                    );
                    if SPIKE >= 2 * HEARTBEAT_INTERVAL {
                        assert!(
                            held_during > held_before,
                            "round {round}: the {SPIKE:?} stall held no read, so it cannot 
                             have been in force",
                        );
                    }
                    PhaseCounts {
                        phase_applied: 1,
                        ..Default::default()
                    }
                })
                .await
            }
        };
        stalls_applied += u64::from(phase == Phase::Spike);
        opened += counts.opened;
        completed += counts.completed;
        phases_applied += counts.phase_applied;

        // The recovery probe: every phase must be followed by a round that
        // completes. This is the per-round progress assertion.
        bounded(
            round,
            phase,
            pair.echo_probe(&payload, faults::LEAK_STREAM.load(Ordering::SeqCst)),
        )
        .await
        .unwrap_or_else(|e| {
            panic!(
                "round {round}: the session did not return to full function after \
                 {phase:?}: {e}"
            )
        });
        completed += 1;

        if let Some(reason) = pair.tear_down_reason() {
            panic!("round {round}: the session tore down during a {phase:?} round: {reason:?}");
        }

        // An idle stretch on the simulated clock: the session must stay live
        // across heartbeats without any traffic of its own.
        tokio::time::sleep(HEARTBEAT_INTERVAL).await;

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

    let sim_elapsed = sim_start.elapsed();
    let wall_elapsed = wall_start.elapsed();
    let after = Checkpoint::sample("final", completed, 0);
    println!(
        "growth soak: rounds={} opened_streams={} completed_streams={} phases_applied={} \
         stalls_applied={} simulated={:?} wall={:?}",
        last_round + 1,
        opened,
        completed,
        phases_applied,
        stalls_applied,
        sim_elapsed,
        wall_elapsed,
    );

    // Instrument sanity: the run must actually have churned streams and
    // applied its phases, or the zero readings above are vacuous.
    assert_eq!(
        checkpoints.len(),
        2,
        "the soak did not reach both matched points in {rounds} rounds \
         ({opened} streams opened, {completed} completed); the growth comparison is vacuous",
    );
    let first = checkpoints[0];
    let later = checkpoints[1];
    assert!(
        completed >= checkpoint_at * CHECKPOINT_MULTIPLE,
        "only {completed} streams completed; the second matched point claims \
         {}",
        checkpoint_at * CHECKPOINT_MULTIPLE,
    );
    assert_eq!(
        phases_applied,
        last_round + 1,
        "only {phases_applied} of {} rounds applied a phase",
        last_round + 1,
    );
    assert!(
        stalls_applied > 0,
        "the stall phase never applied; its red-proof check is vacuous",
    );
    assert!(
        later.ledger.client.inserted + later.ledger.server.inserted >= completed,
        "the admission ledger counted fewer inserts than completed streams, so the census \
         was not reading the same session ({} vs {completed})",
        later.ledger.client.inserted + later.ledger.server.inserted,
    );
    assert!(
        later.ledger.client.retired > 0 && later.ledger.server.retired > 0,
        "one session retired no stream at all ({}), so its zero table reading is vacuous",
        later.ledger,
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
    for (role, census) in [
        ("client", first.structures.client),
        ("client", later.structures.client),
        ("server", first.structures.server),
        ("server", later.structures.server),
    ] {
        assert!(
            census.retired_window_len <= RETIRED_WINDOW_MAX,
            "{role} retired-id window past its bound: {census}",
        );
    }
    println!("growth soak checkpoints: first[{}] later[{}]", first, later,);
    // Last, so a fault run is never green even if none of the property checks
    // above happened to catch the injected fault; the checks that precede it
    // are the ones that name the structure and both counts.
    assert!(
        !faults::enabled(),
        "a fault mode is set, so this run is a red-proof probe and must not be read as a pass",
    );
}
