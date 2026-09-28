//! Combined-horizon field soak: the deployed session's whole shape at once.
//!
//! Every other soak in this crate varies **one** dimension from a stated
//! baseline. This arm is a labelled **composite**: it is the field's session
//! shape with no dimension held out, so it can reach a defect that lives in
//! the *interaction* of dimensions no single-dimension arm can reach.
//!
//! The operator's client multiplexes everything over one long-lived mux
//! session on a path whose measured floor is ~190 ms and whose measured worst
//! spikes are 1063 ms and 3205 ms, with one 19.9 s silence observed. That
//! session carries a persistent interactive lane (a request/response stream
//! that is never closed) while short-lived streams open and close throughout.
//! The field's own shape, therefore, is:
//!
//! * the field's **spike schedule** — all four measured magnitudes, including
//!   the 19.9 s boundary case, which is 100 ms short of the 20 s receive
//!   deadline;
//! * **sustained churn** — short-lived streams opening, staged, closed and
//!   released on every round, so the stream table and the egress token table
//!   both cycle;
//! * a **persistent interactive lane in flight across every spike** — the
//!   operator's request/response stream is staged before the stall and its
//!   reply is consumed after release, so a spike always lands on live
//!   interactive traffic rather than on a quiesced session;
//! * **per-stream structure censuses at matched points**, so growth and
//!   retention are read as a trend across the horizon and not as one number.
//!
//! What this arm adds over the set it sits beside, stated precisely:
//!
//! * `spike_survival_soak` applies the 19.9 s spike, but opens its stream
//!   *after* the stall is released and takes no census, so it cannot see a
//!   structure retained across that spike.
//! * `session_growth_soak` censuses per-stream structures, but applies only
//!   the 1063 ms magnitude, and its spike phase is a dedicated round with a
//!   recovery probe *after* release — no live traffic is in flight across it.
//! * `frame_reorder_soak` / `frame_dup_partial_soak` census and hold traffic
//!   in flight across a stall, but their schedule deliberately stops at
//!   3205 ms; their own module doc records why (the two sessions' sliding
//!   windows are armed at different instants and the ledger publishes only the
//!   most recent arm, so a 19.9 s advance would be ambiguous).
//! * `interactive_liveness_soak` drives the persistent interactive lane hard,
//!   but applies no transport stall at all.
//!
//! The cell this arm holds is therefore the one no arm holds: **a per-stream
//! census across the 19.9 s boundary spike, taken while live interactive
//! traffic and churn are in flight across it.** The 19.9 s advance is made
//! unambiguous here by the paused clock: the churn staging, the operator
//! request and both readers' arms all happen at one instant, so the margin
//! check below reads the window the stall is actually applied to rather than
//! an arm from an earlier round.
//!
//! The census is a table walk, so it is off unless a soak enables it; the
//! horizon is simulated (a paused clock), so the wall cost is the work, not
//! the horizon.
//!
//! Tier: **standard** (`#[ignore]`d, asserting). `MUX_FIELD_ROUNDS` sizes the
//! horizon and `MUX_FIELD_CHECKPOINT` places its matched points.
//! `MUX_FIELD_FAULT=no_stall` disables the gate's hold, so the "the spike was
//! applied" check must fail.

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

/// Production heartbeat on the deployed dual-lane path; the steady receive
/// deadline is `4 x` this.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// `central_io::reader::RECEIVE_DEADLINE_INTERVALS` x [`HEARTBEAT_INTERVAL`].
const RECEIVE_DEADLINE: Duration = Duration::from_secs(20);
/// The field's boundary silence: 100 ms short of the receive deadline.
const DEADLINE_BOUNDARY_SPIKE: Duration = Duration::from_millis(19_900);

/// The field's measured floor and maxima, plus the one observed silence. All
/// four magnitudes are applied here; the first three live in the reorder,
/// duplication and growth soaks and the last in `spike_survival_soak` alone.
const SPIKE_SCHEDULE: [Duration; 4] = [
    Duration::from_millis(190),
    Duration::from_millis(1063),
    Duration::from_millis(3205),
    DEADLINE_BOUNDARY_SPIKE,
];

/// The horizon: 48 rounds at four matched points. On this schedule the
/// horizon is ~1 200 s of simulated session time (see the measured reading the
/// arm prints).
const DEFAULT_ROUNDS: u64 = 48;
const DEFAULT_CHECKPOINT: u64 = 12;
/// The round-end idle: three heartbeat intervals of simulated session time
/// with no traffic of the arm's own. The heartbeats inside it re-arm each
/// session's receive-deadline window well inside its 20 s bound, so the idle
/// ages the session rather than crossing its own liveness timer.
const IDLE_WINDOW: Duration = Duration::from_secs(15);
/// Short-lived streams opened and staged every round.
const CHURN_STREAMS: u64 = 2;
/// The persistent operator lane's request size.
const OP_LEN: usize = 96;
/// Churn payload sizes: small for the ordinary shapes, past the read queue's
/// bound for the slow consumer.
const SMALL: usize = 4 * 1024;
const LARGE: usize = 64 * 1024;
/// `RETIRED_FINISHED_PEER_STREAM_WINDOW` in `src/control.rs`.
const RETIRED_WINDOW_MAX: u64 = 1024;
/// A phase that wedges must fail the round where it wedged rather than hang
/// the target.
const PHASE_BUDGET: Duration = Duration::from_secs(30);
/// The slow-consumer shape's release bound: past the point a parked write could
/// still be a live one, so the round moves on instead of hanging on it.
const LARGE_DRAIN_BUDGET: Duration = Duration::from_secs(10);

/// Red-proof switch: when set, the gate ignores the stall so the soak's
/// "the spike was applied" checks must fail. A test-side fault that removes
/// the property the arm relies on, rather than re-expressing it.
static FAULT_NO_STALL: AtomicBool = AtomicBool::new(false);

// ─── the stall gate ────────────────────────────────────────────────────────

/// A read gate that withholds delivery while `stalled` is set. It is released
/// by the test, not by a timer, so `advance` is the only clock movement and
/// the spike duration is exact. One switch serves both directions, so a spike
/// is applied end to end.
struct StallSwitch {
    stalled: AtomicBool,
    waker: Mutex<Option<Waker>>,
    /// Bytes delivered while the stall was set. Must stay zero: a non-zero
    /// value means the spike was not actually applied.
    delivered_while_stalled: AtomicU64,
    /// Read polls held pending *by the stall* rather than by an empty inner
    /// stream. Must increase on every spike; a run with no holds never
    /// applied one.
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

impl<R> StallGate<R> {
    fn new(inner: R, switch: Arc<StallSwitch>) -> Self {
        Self { inner, switch }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for StallGate<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.switch.is_stalled() && !FAULT_NO_STALL.load(Ordering::SeqCst) {
            *self.switch.waker.lock().unwrap() = Some(cx.waker().clone());
            // Re-check after arming the waker: a release that raced the first
            // check must not strand the read.
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
    server_teardowns: JoinSet<MuxError>,
}

impl Pair {
    /// Spawn one long-lived session across a gated in-memory duplex, stalling
    /// both delivery directions with one atomic switch so a spike is applied
    /// end to end. `frame_reassembly = true` is the mode the frame-reassembly
    /// retention defect was specific to, and the mode the deployed
    /// frame-delivery lane runs.
    fn spawn(switch: Arc<StallSwitch>) -> Self {
        let (a, b) = tokio::io::duplex(1 << 20);
        let (client_read, client_write) = tokio::io::split(a);
        let (server_read, server_write) = tokio::io::split(b);
        let client_read = StallGate::new(client_read, Arc::clone(&switch));
        let server_read = StallGate::new(server_read, switch);

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
            server_teardowns,
        }
    }

    fn teardowns(&mut self) -> Vec<MuxError> {
        let mut out = Vec::new();
        while let Some(joined) = self.teardowns.try_join_next() {
            out.push(joined.expect("client session task panicked"));
        }
        while let Some(joined) = self.server_teardowns.try_join_next() {
            out.push(joined.expect("server session task panicked"));
        }
        out
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
}

// ─── the persistent operator lane ──────────────────────────────────────────

/// The server half of the operator lane: echo every `OP_LEN`-byte request.
/// This is the "Minecraft-shaped" interactive lane the client keeps open for
/// the life of the session; it is never closed mid-horizon.
async fn operator_echo(mut reader: StreamReader, mut writer: StreamWriter) {
    let mut message = vec![0u8; OP_LEN];
    loop {
        let mut filled = 0;
        while filled < OP_LEN {
            match reader.read(&mut message[filled..]).await {
                Ok(0) => return,
                Ok(n) => filled += n,
                Err(_) => return,
            }
        }
        if writer.write_all(&message).await.is_err() {
            return;
        }
    }
}

/// Stage one operator request without reading its reply: the request sits in
/// flight while the spike is applied.
async fn stage_operator(writer: &mut StreamWriter, round: u64) -> io::Result<Vec<u8>> {
    let mut request = vec![0u8; OP_LEN];
    fill_pattern(round, 0, &mut request);
    writer.write_all(&request).await?;
    Ok(request)
}

/// Consume the reply the peer owed for the staged request and verify it
/// byte-for-byte.
async fn read_operator_reply(
    reader: &mut StreamReader,
    round: u64,
    request: &[u8],
) -> io::Result<()> {
    let mut echo = vec![0u8; OP_LEN];
    reader.read_exact(&mut echo).await?;
    if echo != request {
        return Err(io::Error::other(format!(
            "operator echo mismatch on round {round}: sent {:?} got {:?}",
            &request[..request.len().min(8)],
            &echo[..echo.len().min(8)],
        )));
    }
    Ok(())
}

// ─── churn ─────────────────────────────────────────────────────────────────

/// One round's short-lived-stream shape. The schedule is fixed by the round
/// index, so a green run is a statement about this shape family rather than
/// about a random draw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// Full request/response on two concurrent streams.
    Echo,
    /// A 32 KiB transfer in one direction, verified end to end.
    Bulk,
    /// The peer accepts and then never reads: a slow consumer against a
    /// bounded read queue. The shape that leaked before.
    SlowConsumer,
    /// A partial write with no shutdown and both ends dropped.
    DroppedMidTransfer,
    /// Written, shut down, and never read: the closing order alternates.
    ClosedWithoutReading,
}

const SCHEDULE: [Shape; 8] = [
    Shape::Echo,
    Shape::Bulk,
    Shape::Echo,
    Shape::SlowConsumer,
    Shape::DroppedMidTransfer,
    Shape::Echo,
    Shape::ClosedWithoutReading,
    Shape::Bulk,
];

fn shape_for_round(round: u64) -> Shape {
    SCHEDULE[(round as usize) % SCHEDULE.len()]
}

fn spike_for_round(round: u64) -> Duration {
    SPIKE_SCHEDULE[(round as usize) % SPIKE_SCHEDULE.len()]
}

/// One churn stream, open and holding its halves. The write itself runs in the
/// round's [`JoinSet`], so its completion and its panics are observed rather
/// than detached.
struct Opened {
    shape: Shape,
    client_reader: Option<StreamReader>,
    client_writer: Option<StreamWriter>,
    server_reader: Option<StreamReader>,
    server_writer: Option<StreamWriter>,
    payload: Vec<u8>,
    seed: u64,
}

impl Opened {
    /// Release every half a shape has deliberately finished with.
    fn drop_all(&mut self) {
        self.client_reader.take();
        self.client_writer.take();
        self.server_reader.take();
        self.server_writer.take();
    }
}

fn fill_pattern(seed: u64, offset: usize, buf: &mut [u8]) {
    for (index, byte) in buf.iter_mut().enumerate() {
        *byte = (seed.wrapping_add((offset + index) as u64) % 251) as u8;
    }
}

fn pattern_vec(seed: u64, len: usize) -> Vec<u8> {
    let mut buf = vec![0u8; len];
    fill_pattern(seed, 0, &mut buf);
    buf
}

/// Open the round's churn streams on the live path, holding every half. The
/// writes are staged later, after the stall is set, so the streams are open
/// and mid-transfer across the spike rather than drained before it.
async fn open_churn(pair: &mut Pair, shape: Shape, round: u64) -> io::Result<Vec<Opened>> {
    let mut opened = Vec::new();
    for index in 0..CHURN_STREAMS {
        let (client_reader, client_writer, server_reader, server_writer) = pair.open_pair().await?;
        let seed = round
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(index);
        let len = match shape {
            Shape::SlowConsumer => LARGE,
            Shape::Bulk => SMALL * 8,
            _ => SMALL,
        };
        let payload = pattern_vec(seed, len);
        opened.push(Opened {
            shape,
            client_reader: Some(client_reader),
            client_writer: Some(client_writer),
            server_reader: Some(server_reader),
            server_writer: Some(server_writer),
            payload,
            seed,
        });
    }
    Ok(opened)
}

/// Stage each opened churn stream's write in the round's [`JoinSet`], while the
/// stall is set: with the gate holding the peer's read, the write is what the
/// spike strands. The dropped and closed shapes then release their local
/// halves, so the peer's frames arrive at a stream whose every local side is
/// closed — the ordering the retention defect needed.
async fn stage_writes(opened: &mut [Opened], jobs: &mut JoinSet<io::Result<()>>) {
    for stream in opened.iter_mut() {
        let mut writer = stream.client_writer.take().expect("client writer");
        let payload = stream.payload.clone();
        let shape = stream.shape;
        jobs.spawn(async move {
            if shape == Shape::DroppedMidTransfer {
                // Half the bytes, no shutdown: the peer's `CloseWrite` never
                // carries a final offset.
                writer.write_all(&payload[..payload.len() / 2]).await?;
                return Ok(());
            }
            writer.write_all(&payload).await?;
            AsyncWriteExt::shutdown(&mut writer).await?;
            Ok(())
        });
        if matches!(
            shape,
            Shape::DroppedMidTransfer | Shape::ClosedWithoutReading
        ) {
            stream.client_reader.take();
            stream.server_reader.take();
            stream.server_writer.take();
        }
    }
}

/// Join every staged write in the round's set, surfacing the first failure.
async fn drain_writes(jobs: &mut JoinSet<io::Result<()>>) -> io::Result<()> {
    let mut first: Option<io::Error> = None;
    while let Some(joined) = jobs.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                first.get_or_insert(error);
            }
            Err(join_error) => {
                first.get_or_insert(io::Error::other(format!("write task: {join_error}")));
            }
        }
    }
    match first {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Verify one churn stream after its write: the peer received every byte in
/// order, and (for the echo shape) the echo came back intact. A shape that
/// deliberately drops its halves is released here instead.
async fn drain_churn(stream: &mut Opened, round: u64) -> io::Result<()> {
    match stream.shape {
        Shape::DroppedMidTransfer | Shape::ClosedWithoutReading | Shape::SlowConsumer => {
            stream.drop_all();
            Ok(())
        }
        Shape::Echo | Shape::Bulk => {
            let mut server_reader = stream.server_reader.take().expect("server reader");
            let mut received = Vec::new();
            server_reader.read_to_end(&mut received).await?;
            if received != stream.payload {
                return Err(io::Error::other(format!(
                    "round {round}: {} payload mismatch: {} of {} bytes",
                    shape_name(stream.shape),
                    received.len(),
                    stream.payload.len(),
                )));
            }
            // Verify the pattern independently of the exact-equality check
            // above, so a truncation and a corruption are not the same reading.
            let expected = pattern_vec(stream.seed, stream.payload.len());
            if received != expected {
                return Err(io::Error::other(format!(
                    "round {round}: {} pattern mismatch",
                    shape_name(stream.shape)
                )));
            }
            if stream.shape == Shape::Echo {
                let mut server_writer = stream.server_writer.take().expect("server writer");
                server_writer.write_all(&received).await?;
                AsyncWriteExt::shutdown(&mut server_writer).await?;
                let mut client_reader = stream.client_reader.take().expect("client reader");
                let mut echoed = Vec::new();
                client_reader.read_to_end(&mut echoed).await?;
                if echoed != stream.payload {
                    return Err(io::Error::other(format!(
                        "round {round}: echo integrity: {} of {} bytes",
                        echoed.len(),
                        stream.payload.len(),
                    )));
                }
            }
            Ok(())
        }
    }
}

fn shape_name(shape: Shape) -> &'static str {
    match shape {
        Shape::Echo => "echo",
        Shape::Bulk => "bulk",
        Shape::SlowConsumer => "slow-consumer",
        Shape::DroppedMidTransfer => "dropped-mid-transfer",
        Shape::ClosedWithoutReading => "closed-without-reading",
    }
}

// ─── checkpoints ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
struct Checkpoint {
    label: &'static str,
    round: u64,
    completed: u64,
    structures: mux::live_probe::StructureCensuses,
    egress: mux::live_probe::EgressTokenCensuses,
    ledger: mux::live_probe::AdmissionLedgers,
}

impl Checkpoint {
    fn sample(label: &'static str, round: u64, completed: u64) -> Self {
        Self {
            label,
            round,
            completed,
            structures: mux::live_probe::structure_censuses(),
            egress: mux::live_probe::egress_token_censuses(),
            ledger: mux::live_probe::admission_ledgers(),
        }
    }
}

impl std::fmt::Display for Checkpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "checkpoint[{}] round={} completed_churn_streams={}\n  {}\n  {}\n  {}",
            self.label, self.round, self.completed, self.structures, self.egress, self.ledger,
        )
    }
}

/// The census a quiesced round must read: every churn stream released, and the
/// persistent operator lane exactly the size it was at the floor. The floor is
/// **measured** (sampled with only the operator lane open) rather than
/// hand-derived, so a structure the operator lane itself holds is not mistaken
/// for a leak; the vacuity guard below proves the floor is non-zero, so this is
/// not "assert everything is zero" wearing a different name.
fn assert_matches_floor(floor: &Checkpoint, checkpoint: &Checkpoint) {
    for (role, before, now) in [
        (
            "client",
            floor.structures.client,
            checkpoint.structures.client,
        ),
        (
            "server",
            floor.structures.server,
            checkpoint.structures.server,
        ),
    ] {
        assert_eq!(
            now.closed_but_retained, 0,
            "{}: {role} retains {} stream-table entr(ies) that `is_closed` already reports \
             finished — no later transition can release them, so the table only fills ({now})",
            checkpoint.label, now.closed_but_retained,
        );
        assert_eq!(
            now.reassembly_pending_frames, 0,
            "{}: {role} retains {} pending reassembly frame(s) ({now})",
            checkpoint.label, now.reassembly_pending_frames,
        );
        assert_eq!(
            now.reassembly_pending_bytes, 0,
            "{}: {role} retains {} pending reassembly byte(s) ({now})",
            checkpoint.label, now.reassembly_pending_bytes,
        );
        assert_eq!(
            now.stream_table_len, before.stream_table_len,
            "{}: {role} stream table is {} entries against a floor of {} — the churn streams \
             opened this horizon were not all released ({now})",
            checkpoint.label, now.stream_table_len, before.stream_table_len,
        );
        assert_eq!(
            now.reassembly_buffers, before.reassembly_buffers,
            "{}: {role} holds {} reorder buffer(s) against a floor of {} ({now})",
            checkpoint.label, now.reassembly_buffers, before.reassembly_buffers,
        );
        assert_eq!(
            now.open_read_sinks, before.open_read_sinks,
            "{}: {role} holds {} open read sink(s) against a floor of {} ({now})",
            checkpoint.label, now.open_read_sinks, before.open_read_sinks,
        );
        assert!(
            now.retired_window_len <= RETIRED_WINDOW_MAX,
            "{}: {role} retired-id window is {} entries, past its bound of {RETIRED_WINDOW_MAX}",
            checkpoint.label,
            now.retired_window_len,
        );
    }
    for (role, before, now) in [
        ("client", floor.egress.client, checkpoint.egress.client),
        ("server", floor.egress.server, checkpoint.egress.server),
    ] {
        assert_eq!(
            now.token_queues, before.token_queues,
            "{}: {role} egress keeps {} per-stream token queue(s) against a floor of {} — the \
             fair queue admits a bounded number and then refuses every later open ({now})",
            checkpoint.label, now.token_queues, before.token_queues,
        );
        assert_eq!(
            now.token_streams, before.token_streams,
            "{}: {role} egress keeps {} token→stream map entr(ies) against a floor of {} ({now})",
            checkpoint.label, now.token_streams, before.token_streams,
        );
        assert_eq!(
            now.token_deficits, before.token_deficits,
            "{}: {role} egress keeps {} deficit entr(ies) against a floor of {} ({now})",
            checkpoint.label, now.token_deficits, before.token_deficits,
        );
        assert!(
            now.cached_heads <= before.cached_heads,
            "{}: {role} egress cached {} head(s), past its floor of {} ({now})",
            checkpoint.label,
            now.cached_heads,
            before.cached_heads,
        );
    }
}

/// The trend half: no per-stream structure may hold more live state at a later
/// matched point than at an earlier one. `total_live_stream_structures` sums
/// every field except the bounded retired-id window, so a leak in any of them
/// (or a new one added to the struct) is caught by the sum even before a
/// field-specific assertion names it.
fn assert_not_grown(first: &Checkpoint, later: &Checkpoint) {
    let before = first.structures.total_live_stream_structures();
    let after = later.structures.total_live_stream_structures();
    assert!(
        after <= before,
        "per-stream structures grew between matched points: {before} -> {after} live entries \
         over rounds {} -> {}\n  {}\n  {}",
        first.round,
        later.round,
        first.structures,
        later.structures,
    );
    let queues_before = first.egress.total();
    let queues_after = later.egress.total();
    assert!(
        queues_after <= queues_before,
        "egress token structures grew between matched points: {queues_before} -> {queues_after} \
         entries\n  {}\n  {}",
        first.egress,
        later.egress,
    );
}

fn rounds() -> u64 {
    std::env::var("MUX_FIELD_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_ROUNDS)
}

fn checkpoint_rounds() -> u64 {
    std::env::var("MUX_FIELD_CHECKPOINT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_CHECKPOINT)
        .max(1)
}

/// Drive the runtime to quiescence so every parked central reader has created
/// its deadline sleep on the timer wheel. Without this the advance that
/// follows can move the clock past a window the reader has not yet armed, and
/// the arm would prove nothing about the deadline.
async fn settle() {
    for _ in 0..256 {
        tokio::task::yield_now().await;
    }
}

async fn bounded<F>(round: u64, what: &str, fut: F) -> F::Output
where
    F: std::future::Future,
{
    match tokio::time::timeout(PHASE_BUDGET, fut).await {
        Ok(value) => value,
        Err(_) => panic!(
            "round {round}: {what} did not finish within {PHASE_BUDGET:?} of simulated time — \
             the session stopped making progress"
        ),
    }
}

// ─── the soak ──────────────────────────────────────────────────────────────

/// The field's whole session shape over one long horizon: the full spike
/// schedule, sustained churn, and a persistent interactive lane in flight
/// across every spike, with per-stream censuses at matched points.
#[tokio::test(start_paused = true)]
#[ignore = "standard tier: combined-horizon field-shaped session soak"]
async fn a_field_shaped_horizon_does_not_grow_a_session_or_lose_a_wake() {
    if std::env::var("MUX_FIELD_FAULT").as_deref() == Ok("no_stall") {
        FAULT_NO_STALL.store(true, Ordering::SeqCst);
    }
    mux::live_probe::enable_structure_census();

    let rounds = rounds();
    let checkpoint_every = checkpoint_rounds();
    let wall_start = StdInstant::now();
    let sim_start = Instant::now();

    let switch = StallSwitch::new();
    let mut pair = Pair::spawn(Arc::clone(&switch));

    // The persistent operator lane: one request/response stream held open for
    // the whole horizon, echoed by a server task. It is never closed
    // mid-horizon, which is the deployed client's shape.
    let (mut op_reader, mut op_writer, op_server_reader, op_server_writer) =
        bounded(0, "operator lane open", pair.open_pair())
            .await
            .expect("operator lane open");
    let mut echo_jobs = JoinSet::new();
    echo_jobs.spawn(operator_echo(op_server_reader, op_server_writer));

    let warm_request = bounded(
        0,
        "operator warm round",
        stage_operator(&mut op_writer, u64::MAX),
    )
    .await
    .expect("operator warm stage");
    bounded(
        0,
        "operator warm reply",
        read_operator_reply(&mut op_reader, u64::MAX, &warm_request),
    )
    .await
    .expect("operator warm reply");

    // The measured floor: only the operator lane is open, so every churn
    // stream this horizon opens must return the census to exactly this reading.
    let floor = Checkpoint::sample("floor", 0, 0);
    println!("{floor}");
    assert!(
        floor.structures.client.stream_table_len >= 1
            && floor.structures.server.stream_table_len >= 1,
        "the operator lane is not live on both roles, so the floor is zero and every matched \
         point would pass vacuously ({})",
        floor.structures,
    );
    assert!(
        floor.egress.client.token_queues >= 1 && floor.egress.server.token_queues >= 1,
        "the operator lane holds no egress token, so the token-table assertion is vacuous ({})",
        floor.egress,
    );
    assert_matches_floor(&floor, &floor);

    let ledger_before = mux::live_probe::timer_ledger();
    let mut checkpoints: Vec<Checkpoint> = Vec::new();
    let mut completed = 0u64;
    let mut opened = 0u64;
    let mut boundary_spikes = 0u64;
    let mut stalls_applied = 0u64;

    for round in 0..rounds {
        let shape = shape_for_round(round);
        let spike = spike_for_round(round);

        // 0. Synchronise both sessions' receive-deadline windows with one full
        //    operator round trip on the paused clock: the client's window is
        //    re-armed by reading this reply and the server's by reading this
        //    request, at the same simulated instant. Without it the two
        //    windows are armed a round apart (the client's by the previous
        //    round's reply, the server's by this round's open), and a 19.9 s
        //    advance would cross whichever was armed earlier — the ambiguity
        //    `frame_reorder_soak` records as its reason for stopping at
        //    3205 ms.
        let sync_request = stage_operator(&mut op_writer, round ^ 0xA5A5_A5A5_A5A5_A5A5)
            .await
            .unwrap_or_else(|e| panic!("round {round}: operator sync stage failed: {e}"));
        bounded(
            round,
            "operator sync reply",
            read_operator_reply(&mut op_reader, round, &sync_request),
        )
        .await
        .unwrap_or_else(|e| panic!("round {round}: operator sync round trip failed: {e}"));

        // 1. Quiesce, then open the churn streams on the live path so their
        //    open round trips complete before the stall.
        settle().await;
        let mut opened_streams = bounded(round, "churn open", open_churn(&mut pair, shape, round))
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "round {round}: {} churn open failed: {e}",
                    shape_name(shape)
                )
            });
        opened += opened_streams.len() as u64;

        // 2. Measure the receive-deadline margin the stall is applied to. On
        //    the paused clock every arm happens at one instant, so this is the
        //    window the spike crosses rather than a stale arm from an earlier
        //    round — which is what makes the 19.9 s boundary case unambiguous.
        settle().await;
        let ledger = mux::live_probe::timer_ledger();
        let remaining = Duration::from_millis(
            ledger
                .last_deadline_ms
                .saturating_sub(mux::live_probe::now_ms()),
        );
        assert!(
            remaining + Duration::from_millis(2) >= RECEIVE_DEADLINE,
            "round {round}: only {remaining:?} of the {RECEIVE_DEADLINE:?} receive-deadline \
             window was left on the most recently armed session, so the two sessions' windows \
             were not synchronised and the {spike:?} advance would be ambiguous ({ledger})",
        );
        assert!(
            remaining > spike,
            "round {round}: only {remaining:?} of the {RECEIVE_DEADLINE:?} receive-deadline \
             window was left when the {spike:?} stall began, so the advance would cross the \
             deadline and this round would assert on a session the detector had already ended \
             ({ledger})",
        );

        // 3. Stall first, then stage the churn writes and the operator
        //    request, so both are in flight across the spike.
        switch.stall();
        let withheld_before = switch.delivered_while_stalled.load(Ordering::SeqCst);
        let held_before = switch.held_polls.load(Ordering::SeqCst);
        let received_before = mux::live_probe::timer_ledger().heartbeats_received;
        settle().await;

        let mut write_jobs = JoinSet::new();
        stage_writes(&mut opened_streams, &mut write_jobs).await;
        let request = stage_operator(&mut op_writer, round)
            .await
            .unwrap_or_else(|e| panic!("round {round}: operator request staging failed: {e}"));

        // 4. Advance the spike with the stall still in force.
        settle().await;
        let held_during_staging = switch.held_polls.load(Ordering::SeqCst);
        tokio::time::advance(spike).await;
        settle().await;
        let withheld_during = switch.delivered_while_stalled.load(Ordering::SeqCst);
        let held_during = switch.held_polls.load(Ordering::SeqCst);
        let received_during = mux::live_probe::timer_ledger().heartbeats_received;

        assert_eq!(
            withheld_during,
            withheld_before,
            "round {round}: the {spike:?} spike delivered {} byte(s) while stalled, so the \
             spike was not applied and this round proves nothing",
            withheld_during - withheld_before,
        );
        assert!(
            held_during > held_before,
            "round {round}: the {spike:?} spike held no read, so it cannot have been in force \
             (held before {held_before}, after staging {held_during_staging}, after advance \
             {held_during})",
        );
        if spike >= 2 * HEARTBEAT_INTERVAL {
            // A heartbeat is due within this stall, so a live peer wrote into
            // the transport during it: none of it may have reached the session.
            assert_eq!(
                received_during,
                received_before,
                "round {round}: a {spike:?} stall let {} heartbeat(s) through, so the transport \
                 was not actually stalled",
                received_during - received_before,
            );
        }
        switch.release();
        settle().await;

        // 5. The interactive reply the peer owed for the request that was in
        //    flight across the spike. This is the persistent operator lane's
        //    per-round progress assertion.
        bounded(
            round,
            "operator reply",
            read_operator_reply(&mut op_reader, round, &request),
        )
        .await
        .unwrap_or_else(|e| {
            panic!(
                "round {round}: the operator lane did not recover after the {spike:?} spike: {e}; \
                 teardowns={:?}; {}",
                pair.teardowns(),
                mux::live_probe::timer_ledger(),
            )
        });

        // 6. Join the churn writes, whose shape decides whether that means
        //    "every byte landed" or "release the parked writer".
        match shape {
            Shape::SlowConsumer | Shape::DroppedMidTransfer | Shape::ClosedWithoutReading => {
                let _ =
                    tokio::time::timeout(LARGE_DRAIN_BUDGET, drain_writes(&mut write_jobs)).await;
                write_jobs.abort_all();
            }
            Shape::Echo | Shape::Bulk => {
                bounded(round, "churn writes", drain_writes(&mut write_jobs))
                    .await
                    .unwrap_or_else(|e| {
                        panic!(
                            "round {round}: a {} churn write failed: {e}",
                            shape_name(shape)
                        )
                    });
            }
        }

        // 7. Drain the churn streams: byte conservation and release.
        for stream in opened_streams.iter_mut() {
            bounded(round, shape_name(stream.shape), drain_churn(stream, round))
                .await
                .unwrap_or_else(|e| {
                    panic!(
                        "round {round}: the {} churn stream did not recover after the {spike:?} \
                         spike: {e}",
                        shape_name(stream.shape)
                    )
                });
        }
        completed += opened_streams.len() as u64;
        drop(opened_streams);

        // 8. The liveness ledger for this round.
        let after = mux::live_probe::timer_ledger();
        assert_eq!(
            after.receive_deadline_expiries, ledger.receive_deadline_expiries,
            "round {round}: a receive-deadline window expired during a {spike:?} spike the \
             session survived ({after})",
        );
        stalls_applied += 1;
        if spike == DEADLINE_BOUNDARY_SPIKE {
            boundary_spikes += 1;
        }

        // 9. The session must still be live.
        let teardowns = pair.teardowns();
        assert!(
            teardowns.is_empty(),
            "round {round}: a {spike:?} spike (receive deadline {RECEIVE_DEADLINE:?}) tore the \
             session down: {teardowns:?}",
        );

        // 10. An idle stretch on the simulated clock: the session must stay
        //     live across heartbeats with no traffic of its own.
        tokio::time::sleep(IDLE_WINDOW).await;

        // 11. The matched point.
        if (round + 1) % checkpoint_every == 0 {
            settle().await;
            let label = if checkpoints.is_empty() {
                "first"
            } else {
                "later"
            };
            let checkpoint = Checkpoint::sample(label, round + 1, completed);
            println!("{checkpoint}");
            assert_matches_floor(&floor, &checkpoint);
            checkpoints.push(checkpoint);
        }
    }

    let sim_elapsed = sim_start.elapsed();
    let wall_elapsed = wall_start.elapsed();
    let after = Checkpoint::sample("final", rounds, completed);
    println!(
        "field horizon: rounds={rounds} churn_streams_opened={opened} \
         churn_streams_completed={completed} stalls_applied={stalls_applied} \
         boundary_spikes={boundary_spikes} simulated={sim_elapsed:?} wall={wall_elapsed:?}",
    );
    println!(
        "field horizon ledger: {}",
        mux::live_probe::timer_ledger().since(&ledger_before)
    );

    // Instrument sanity. A run that churned nothing, applied no stall, or
    // never crossed the boundary magnitude would pass the census and the
    // zero-expiry assertions vacuously.
    assert!(
        !checkpoints.is_empty(),
        "the horizon of {rounds} rounds with a checkpoint every {checkpoint_every} reached no \
         matched point; the growth comparison is vacuous",
    );
    assert!(
        opened >= CHURN_STREAMS * rounds,
        "only {opened} churn streams opened over {rounds} rounds, so the churn path was not \
         driven",
    );
    assert_eq!(
        stalls_applied, rounds,
        "only {stalls_applied} of {rounds} rounds applied a spike",
    );
    assert!(
        boundary_spikes > 0,
        "the {DEADLINE_BOUNDARY_SPIKE:?} boundary spike was never applied; the cell this arm \
         exists for was not reached",
    );
    assert!(
        after.ledger.client.inserted + after.ledger.server.inserted > completed,
        "the admission ledger counted fewer inserts than the churn streams completed plus the \
         operator lane, so the census was not reading the same session ({} vs {completed})",
        after.ledger.client.inserted + after.ledger.server.inserted,
    );
    assert!(
        after.ledger.server.retired > 0,
        "the server retired no stream at all, so its matched-point reading is vacuous ({})",
        after.ledger,
    );
    let ledger_delta = mux::live_probe::timer_ledger().since(&ledger_before);
    assert!(
        ledger_delta.receive_deadline_sleeps_armed > 0 && ledger_delta.heartbeats_received > 0,
        "no receive-deadline sleep and no heartbeat were ever observed over the horizon \
         ({ledger_delta}), so the zero-expiry result is vacuous",
    );
    assert_eq!(
        ledger_delta.receive_deadline_expiries, 0,
        "a receive-deadline window expired during a spike the session survived ({ledger_delta})",
    );
    assert!(
        switch.held_polls.load(Ordering::SeqCst) > 0,
        "the gate never held a read across the whole horizon; the stall was never in force",
    );
    assert!(
        echo_jobs.try_join_next().is_none(),
        "the operator lane's echo task exited during the horizon: the persistent interactive \
         lane did not survive it",
    );

    // The census at the final matched point and the trend across all of them.
    assert_matches_floor(&floor, &after);
    for window in checkpoints.windows(2) {
        assert_not_grown(&window[0], &window[1]);
    }
    assert!(
        checkpoints.len() >= 2,
        "only {} matched point(s) were taken over {rounds} rounds; a trend needs at least two",
        checkpoints.len(),
    );
    println!(
        "field horizon checkpoints: {}",
        checkpoints
            .iter()
            .map(|c| format!("[{} r{} c{}]", c.label, c.round, c.completed))
            .collect::<Vec<_>>()
            .join(" ")
    );

    // Last, so a fault run is never green even if none of the property checks
    // above happened to catch the injected fault.
    assert!(
        !FAULT_NO_STALL.load(Ordering::SeqCst),
        "a fault mode is set, so this run is a red-proof probe and must not be read as a pass",
    );
}
