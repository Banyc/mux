//! Does the mux *receive* path amplify a withheld delivery?
//!
//! The question this soak answers is a narrow one: when one delivery event for
//! one flow is withheld for `D`, how much **more** than `D` does that flow's
//! next message take to reach the application? A mux that adds nothing answers
//! `D + 0`; a mux whose scheduler or reader only revisits a flow on the *next*
//! arrival answers `D + (the gap to that arrival)`, which is unbounded with
//! respect to `D` and is the shape a per-flow tens-of-milliseconds stall has.
//!
//! The instrument is deliberately sharper than a latency measurement. The
//! runtime's clock is paused, so **nothing moves simulated time except the
//! `advance(D)` this soak performs itself**. The soak therefore asserts the
//! property directly rather than statistically:
//!
//! 1. **Every other flow's message arrives with the clock untouched.** After
//!    the four flows have staged one message each, the three flows whose frame
//!    was not withheld must reach their readers with *zero* elapsed simulated
//!    time (asserted as `Instant::now()` equality, not as a percentile). This
//!    is the cross-flow independence the reorder buffer provides; a scheduler
//!    that only revisits a ready flow on the next arrival cannot pass it.
//! 2. **The withheld flow's message arrives on the release and on nothing
//!    else.** Its latency must be at least `D` — the proof the withhold really
//!    landed on the armed flow's frame, so the measurement is not vacuous —
//!    and at most `D + one millisecond`. The lower bound is what makes the
//!    upper one mean something: a soak that withheld the wrong frame would
//!    fail the lower bound rather than pass the upper one by accident.
//! 3. **Progress is asserted on every round, never averaged.** A round whose
//!    messages do not arrive is a red naming the stalled flow, so a stall that
//!    happens in one round out of two hundred cannot be diluted by the other
//!    199.
//! 4. **The disturbance fired.** Each phase counts the frames its transport
//!    actually withheld and fails on zero, so a green run cannot be a run in
//!    which the impairment never happened.
//!
//! Four phases, each varying **one** dimension from the `SmallWithhold`
//! baseline (the deployed interactive-lane shape: `frame_reassembly` on, four
//! concurrent long-lived streams, small messages, round-robin withhold):
//!
//! * `SmallWithhold` — the baseline.
//! * `MultiFrameWithhold` — varies the **message shape**: a 200 KiB message
//!   split across many frames, with a middle frame withheld, so the receiver's
//!   `ReorderBuffer` holds the tail across the gap. This is the reassembly
//!   candidate.
//! * `StockWireWithhold` — varies the **wire mode**: `frame_reassembly` off, so
//!   the withheld frame blocks every later frame behind it on the byte stream.
//!   That head-of-line cost is the *transport's*, not the mux's, so this phase
//!   asserts only the per-flow bound and not the cross-flow freedom.
//! * `ZeroCostControl` — varies the **impairment**: no withhold at all. Every
//!   flow's message must arrive with the clock untouched, which is the mux's
//!   own receive-path cost measured with no disturbance in the way.
//!
//! Tier: **standard** (`#[ignore]`d, asserting). `MUX_RECEIVE_STALL_ROUNDS`
//! scales the baseline phase's round count; `MUX_RECEIVE_STALL_FAULT` is the
//! red-proof selector — `no_withhold` never withholds a frame (the
//! disturbance-fired assertion must fail) and `arrival_gated` releases the held
//! frame only when the *next* frame arrives, i.e. it makes the transport itself
//! arrival-gated (the per-round progress assertion must fail, which is what
//! proves the instrument detects the shape this soak exists for).
//!
//! What this soak cannot catch, stated rather than implied: it holds no real
//! transport, so it cannot see a stall produced by the transport's own repair
//! ladder or by a real link's loss/jitter; it withholds no control frame
//! (an `Open`/`CloseWrite` withhold is a stream-lifecycle impairment, covered
//! by the reorder and duplication soaks); it uses one interleaving per round,
//! not a search over interleavings; and its latency resolution is the paused
//! clock's, which is exact for *task-scheduling* cost (zero) but says nothing
//! about wall-clock cost on a loaded host.

use std::{
    collections::{HashMap, VecDeque},
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use mux::{
    Initiation, MuxConfig, MuxError, StreamAccepter, StreamOpener, StreamReader, StreamWriter,
    spawn_mux_no_reconnection,
};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, duplex},
    sync::mpsc,
    task::JoinSet,
    time::{Instant, advance},
};

// ─── constants ─────────────────────────────────────────────────────────────

/// Production heartbeat on the deployed path; the steady receive deadline is
/// `4 x` this.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// Buffered bytes per direction of the in-memory transport pair.
const DUPLEX_BUF: usize = 1 << 16;
/// Concurrent long-lived streams, i.e. the production flow count.
const FLOWS: usize = 4;
/// The withheld delivery interval, rotated per round; index 0 is the control
/// (no withhold at all), so every phase carries its own zero-cost baseline.
const WITHHOLD_SCHEDULE: [Duration; 4] = [
    Duration::ZERO,
    Duration::from_millis(30),
    Duration::from_millis(50),
    Duration::from_millis(100),
];
/// Baseline phase rounds. The withheld-event count is `3/4` of this.
const DEFAULT_ROUNDS: usize = 200;
/// The multi-frame phase's rounds; each carries a 200 KiB message.
const MULTI_ROUNDS: usize = 40;
/// Stock-wire phase rounds.
const STOCK_ROUNDS: usize = 100;
/// Fill bytes appended after the `[stamp u64][flow u32][round u32]` header.
const SMALL_FILL: usize = 64;
/// One multi-frame message: large enough to split into many frames under every
/// dispatch cap the scheduler can choose (1 200 .. 32 KiB).
const MULTI_FILL: usize = 200 * 1024;
/// How much more than the withheld interval a message may take. On a paused
/// clock the only way to spend simulated time is a timer or an `advance`, so
/// this is slack for the measurement, not for the mux.
const EXCESS_BOUND: Duration = Duration::from_millis(1);
/// How many scheduler yields a round may need before its messages are declared
/// stalled. Every hop of the pipeline is woken by the previous one, so a round
/// that arrives at all arrives in a handful; the budget exists so a flow that
/// needs a *further arrival* (the defect this soak hunts) fails instead of
/// hanging.
const YIELD_BUDGET: usize = 20_000;
/// The multi-frame phase's message, as `[len u32][stamp u64][flow u32][round
/// u32][fill]`.
const MULTI_MSG_BYTES: usize = 4 + 16 + MULTI_FILL;
/// The phase is only a *multi-frame* shape if its message splits into several
/// frames under the largest dispatch cap the scheduler can choose
/// (`DATA_BULK_CAP`, 32 KiB). Asserted at compile time, so the phase cannot
/// quietly become a single-frame one and stop covering the reassembly gap.
const _: () = assert!(MULTI_MSG_BYTES > 3 * 32 * 1024);

/// On-wire header byte for a Data frame (`wire_contract` pins it).
const DATA_FRAME_CODE: u8 = 0x02;

// ─── red-proof selectors ───────────────────────────────────────────────────

mod faults {
    use std::sync::atomic::{AtomicU8, Ordering};

    pub const NO_WITHHOLD: u8 = 1;
    pub const ARRIVAL_GATED: u8 = 2;
    pub const SHORT_RELEASE: u8 = 3;

    static MODE: AtomicU8 = AtomicU8::new(255);

    pub fn mode() -> u8 {
        match MODE.load(Ordering::Relaxed) {
            255 => {
                let parsed = match std::env::var("MUX_RECEIVE_STALL_FAULT").as_deref() {
                    Ok("no_withhold") => NO_WITHHOLD,
                    Ok("arrival_gated") => ARRIVAL_GATED,
                    Ok("short_release") => SHORT_RELEASE,
                    Ok("") | Err(_) => 0,
                    Ok(other) => panic!("unknown MUX_RECEIVE_STALL_FAULT={other}"),
                };
                MODE.store(parsed, Ordering::Relaxed);
                parsed
            }
            m => m,
        }
    }

    pub fn arrival_gated() -> bool {
        mode() == ARRIVAL_GATED
    }

    pub fn short_release() -> bool {
        mode() == SHORT_RELEASE
    }

    pub fn enabled() -> bool {
        mode() != 0
    }
}

/// Rounds for the baseline phase: `MUX_RECEIVE_STALL_ROUNDS` or the default.
fn rounds() -> usize {
    match std::env::var("MUX_RECEIVE_STALL_ROUNDS") {
        Ok(v) => v
            .parse()
            .expect("MUX_RECEIVE_STALL_ROUNDS must be a number"),
        Err(_) => DEFAULT_ROUNDS,
    }
}

// ─── the withholding transport ─────────────────────────────────────────────

/// Which frame the transport should withhold. `flow` is the index of the
/// stream id in first-appearance order; the frame is the first one for that
/// flow at or after `offset` (frame-reassembly on, where every Data frame
/// carries its stream offset) or the `ordinal`-th Data frame of that flow
/// (stock wire, where the frame order is the byte-stream order).
#[derive(Debug, Clone, Copy)]
struct Arm {
    flow: usize,
    offset: Option<u32>,
    ordinal: Option<u64>,
}

/// The withholding plan, shared between the test and the transport task.
#[derive(Debug)]
struct WithholdPlan {
    /// True when the transport is a *frame* transport (`frame_reassembly` on):
    /// a held frame is passed by later frames. False when the transport is a
    /// byte stream, where a held frame blocks everything queued behind it —
    /// that head-of-line cost is the transport's, not the mux's.
    forward_past_hold: bool,
    arm: Mutex<Option<Arm>>,
    /// Frames this transport actually withheld. Zero at the end is a run whose
    /// disturbance never happened.
    withheld: AtomicU64,
    /// Set by the test to release the held frame; cleared when the release is
    /// consumed, so a stale notification cannot release a later withhold.
    released: AtomicBool,
    release: tokio::sync::Notify,
}

impl WithholdPlan {
    fn new(forward_past_hold: bool) -> Arc<Self> {
        Arc::new(Self {
            forward_past_hold,
            arm: Mutex::new(None),
            withheld: AtomicU64::new(0),
            released: AtomicBool::new(false),
            release: tokio::sync::Notify::new(),
        })
    }

    fn arm(&self, arm: Arm) {
        self.released.store(false, Ordering::SeqCst);
        *self.arm.lock().unwrap() = Some(arm);
    }

    fn disarm(&self) {
        self.released.store(false, Ordering::SeqCst);
        *self.arm.lock().unwrap() = None;
    }

    fn set_release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release.notify_one();
    }

    /// Take the arm if this frame is the one it names. One-shot: an arm fires
    /// once, so a phase's withheld-frame count is exactly its round count.
    fn take_if_match(
        &self,
        flow: Option<usize>,
        offset: Option<u32>,
        ordinal: Option<u64>,
    ) -> bool {
        if faults::mode() == faults::NO_WITHHOLD {
            return false;
        }
        let mut arm = self.arm.lock().unwrap();
        let Some(candidate) = *arm else {
            return false;
        };
        if Some(candidate.flow) != flow {
            return false;
        }
        let matched = match (candidate.offset, candidate.ordinal) {
            (Some(want), _) => offset.is_some_and(|have| have >= want),
            (None, Some(want)) => ordinal == Some(want),
            (None, None) => false,
        };
        if matched {
            *arm = None;
        }
        matched
    }
}

/// The transport's egress: accepts one whole frame per `poll_write` (the
/// encoder is told this writer is not vectored, so a frame is written whole)
/// and hands it to the pump task.
struct FrameWriter {
    tx: mpsc::Sender<Vec<u8>>,
}

impl AsyncWrite for FrameWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.tx.try_send(buf.to_vec()) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(mpsc::error::TrySendError::Full(_)) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
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
    /// Force the encoder down its non-vectored path, where a frame's fixed
    /// header and body are concatenated into one buffer and written once. That
    /// is what makes each `poll_write` exactly one frame.
    fn is_write_vectored(&self) -> bool {
        false
    }
}

/// Forward frames from the encoder into the in-memory link, withholding the
/// one frame the plan names.
async fn pump(mut out: DuplexStream, mut frames: mpsc::Receiver<Vec<u8>>, plan: Arc<WithholdPlan>) {
    // Stream ids in first-appearance order: the flow index a test arm names.
    let mut order: Vec<u32> = Vec::new();
    // Data frames seen per stream id: the stock wire's frame ordinal.
    let mut counts: HashMap<u32, u64> = HashMap::new();
    let mut held: Option<Vec<u8>> = None;
    let mut held_sid: Option<u32> = None;
    let mut backlog: VecDeque<Vec<u8>> = VecDeque::new();

    loop {
        tokio::select! {
            biased;
            _ = plan.release.notified(), if held.is_some() && !faults::arrival_gated() => {
                if plan.released.swap(false, Ordering::SeqCst) {
                    let Some(frame) = held.take() else { continue };
                    held_sid = None;
                    if out.write_all(&frame).await.is_err() {
                        return;
                    }
                    while let Some(frame) = backlog.pop_front() {
                        if out.write_all(&frame).await.is_err() {
                            return;
                        }
                    }
                }
            }
            maybe = frames.recv() => {
                let Some(frame) = maybe else { return };
                let code = frame.first().copied().unwrap_or(0);
                // Only Open/Data/CloseRead/CloseWrite carry a stream id; a
                // heartbeat's bytes after the code are a padding length.
                let sid = if (1..=4).contains(&code) && frame.len() >= 5 {
                    Some(u32::from_be_bytes(frame[1..5].try_into().unwrap()))
                } else {
                    None
                };
                let flow = sid.map(|sid| match order.iter().position(|s| *s == sid) {
                    Some(index) => index,
                    None => {
                        order.push(sid);
                        order.len() - 1
                    }
                });
                let is_data = code == DATA_FRAME_CODE && frame.len() >= 11;
                let ordinal = if is_data {
                    let sid = sid.unwrap();
                    let count = counts.entry(sid).or_insert(0);
                    let ordinal = *count;
                    *count += 1;
                    Some(ordinal)
                } else {
                    None
                };
                let offset = if is_data {
                    Some(u32::from_be_bytes(frame[7..11].try_into().unwrap()))
                } else {
                    None
                };
                if plan.take_if_match(flow, offset, ordinal) {
                    plan.withheld.fetch_add(1, Ordering::SeqCst);
                    held_sid = sid;
                    held = Some(frame);
                    continue;
                }
                if held.is_some() {
                    if plan.forward_past_hold {
                        if faults::arrival_gated() && sid == held_sid {
                            // The red-proof: the *same flow's* next arrival
                            // releases the held frame, which is the
                            // arrival-dependence shape under test.
                            let frame_held = held.take().unwrap();
                            held_sid = None;
                            if out.write_all(&frame_held).await.is_err() {
                                return;
                            }
                        } else {
                            // A frame transport: the held frame stays held and
                            // this one passes it.
                            if out.write_all(&frame).await.is_err() {
                                return;
                            }
                            continue;
                        }
                    } else {
                        // A byte-stream transport: everything queues behind the
                        // hold, which is the transport's own head-of-line cost.
                        backlog.push_back(frame);
                        continue;
                    }
                }
                if out.write_all(&frame).await.is_err() {
                    return;
                }
            }
        }
    }
}

// ─── messages and samples ──────────────────────────────────────────────────

/// `[len u32 LE][stamp u64 LE][flow u32 LE][round u32 LE][fill]`, where the
/// stamp is nanoseconds since the soak's `base`.
fn encode_msg(base: Instant, flow: u32, round: u32, fill: usize) -> Vec<u8> {
    let mut body = Vec::with_capacity(16 + fill);
    body.extend_from_slice(&(base.elapsed().as_nanos() as u64).to_le_bytes());
    body.extend_from_slice(&flow.to_le_bytes());
    body.extend_from_slice(&round.to_le_bytes());
    body.resize(16 + fill, 0x5A);
    let mut msg = Vec::with_capacity(4 + body.len());
    msg.extend_from_slice(&(body.len() as u32).to_le_bytes());
    msg.extend_from_slice(&body);
    msg
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    flow: usize,
    latency: Duration,
}

/// Read one flow's messages for the life of the soak. Asserts per-flow order
/// and flow identity on every message, so a reorder or a cross-flow mix-up is
/// a failure rather than a latency reading.
async fn read_flow(flow: usize, mut reader: StreamReader, base: Instant, tx: mpsc::Sender<Sample>) {
    let mut hdr = [0u8; 4];
    let mut round = 0u32;
    loop {
        if reader.read_exact(&mut hdr).await.is_err() {
            return;
        }
        let len = u32::from_le_bytes(hdr) as usize;
        assert!(len >= 16, "flow {flow}: short message body ({len} bytes)");
        let mut body = vec![0u8; len];
        if reader.read_exact(&mut body).await.is_err() {
            return;
        }
        let now = base.elapsed().as_nanos() as u64;
        let stamp = u64::from_le_bytes(body[..8].try_into().unwrap());
        let got_flow = u32::from_le_bytes(body[8..12].try_into().unwrap()) as usize;
        let got_round = u32::from_le_bytes(body[12..16].try_into().unwrap());
        assert_eq!(
            got_flow, flow,
            "flow {flow} received a message stamped for flow {got_flow}"
        );
        assert_eq!(
            got_round, round,
            "flow {flow} received round {got_round} where round {round} was expected: a \
             message was lost, duplicated or reordered within one stream"
        );
        round += 1;
        let sample = Sample {
            flow,
            latency: Duration::from_nanos(now.saturating_sub(stamp)),
        };
        if tx.send(sample).await.is_err() {
            return;
        }
    }
}

// ─── the session pair ──────────────────────────────────────────────────────

struct Pair {
    opener: StreamOpener,
    accepter: StreamAccepter,
    client_teardowns: JoinSet<MuxError>,
    server_teardowns: JoinSet<MuxError>,
    _pumps: JoinSet<()>,
    c2s: Arc<WithholdPlan>,
}

impl Pair {
    fn spawn(frame_reassembly: bool) -> Self {
        let (client_read, server_write) = duplex(DUPLEX_BUF);
        let (server_read, client_write) = duplex(DUPLEX_BUF);
        let (client_frames, client_rx) = mpsc::channel(256);
        let (server_frames, server_rx) = mpsc::channel(256);
        let c2s = WithholdPlan::new(frame_reassembly);
        let s2c = WithholdPlan::new(frame_reassembly);
        let mut pumps = JoinSet::new();
        pumps.spawn(pump(client_write, client_rx, Arc::clone(&c2s)));
        pumps.spawn(pump(server_write, server_rx, Arc::clone(&s2c)));

        let mut client_config = MuxConfig::new(Initiation::Client, HEARTBEAT_INTERVAL);
        client_config.frame_reassembly = frame_reassembly;
        let mut client_teardowns = JoinSet::new();
        let (opener, _) = spawn_mux_no_reconnection(
            client_read,
            FrameWriter { tx: client_frames },
            client_config,
            &mut client_teardowns,
        );
        let mut server_config = MuxConfig::new(Initiation::Server, HEARTBEAT_INTERVAL);
        server_config.frame_reassembly = frame_reassembly;
        let mut server_teardowns = JoinSet::new();
        let (_, accepter) = spawn_mux_no_reconnection(
            server_read,
            FrameWriter { tx: server_frames },
            server_config,
            &mut server_teardowns,
        );
        Self {
            opener,
            accepter,
            client_teardowns,
            server_teardowns,
            _pumps: pumps,
            c2s,
        }
    }

    fn assert_alive(&mut self, phase: &str, round: usize) {
        if let Some(joined) = self.client_teardowns.try_join_next() {
            let err = joined.expect("client session task panicked");
            panic!("{phase} round {round}: the client session died: {err:?}");
        }
        if let Some(joined) = self.server_teardowns.try_join_next() {
            let err = joined.expect("server session task panicked");
            panic!("{phase} round {round}: the server session died: {err:?}");
        }
    }
}

/// Drive the runtime until `want` of the expected samples have arrived,
/// **without moving the paused clock**. A round that needs an `advance` or a
/// further arrival exhausts the budget and fails naming the flow.
async fn collect(rx: &mut mpsc::Receiver<Sample>, want: usize, already: &mut Vec<Sample>) -> usize {
    let mut yields = 0usize;
    while already.len() < want {
        match rx.try_recv() {
            Ok(sample) => already.push(sample),
            Err(mpsc::error::TryRecvError::Empty) => {
                if yields >= YIELD_BUDGET {
                    let flows: Vec<usize> = already.iter().map(|s| s.flow).collect();
                    panic!(
                        "only {} of {want} expected messages arrived after {YIELD_BUDGET} \
                         scheduler yields with the clock untouched (arrived from flows {flows:?}); \
                         the receive path is waiting on something other than the withheld \
                         frame's release",
                        already.len()
                    );
                }
                yields += 1;
                tokio::task::yield_now().await;
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                panic!("the reader tasks ended before {want} messages arrived")
            }
        }
    }
    already.len()
}

// ─── phases ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Baseline: deployed interactive-lane shape, small messages.
    SmallWithhold,
    /// One dimension varied: the message shape (many frames per message).
    MultiFrameWithhold,
    /// One dimension varied: the wire mode (stock, byte-stream transport).
    StockWireWithhold,
    /// One dimension varied: the impairment (no withhold at all).
    ZeroCostControl,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Phase::SmallWithhold => "SmallWithhold",
            Phase::MultiFrameWithhold => "MultiFrameWithhold",
            Phase::StockWireWithhold => "StockWireWithhold",
            Phase::ZeroCostControl => "ZeroCostControl",
        }
    }

    fn frame_reassembly(self) -> bool {
        self != Phase::StockWireWithhold
    }

    fn fill(self) -> usize {
        match self {
            Phase::MultiFrameWithhold => MULTI_FILL,
            _ => SMALL_FILL,
        }
    }

    fn rounds(self, baseline: usize) -> usize {
        match self {
            Phase::MultiFrameWithhold => MULTI_ROUNDS,
            Phase::StockWireWithhold => STOCK_ROUNDS,
            _ => baseline,
        }
    }

    /// The withheld interval this round applies; `None` is the zero-cost
    /// control round, which withholds nothing.
    /// What this phase means to apply this round, given the active red-proof
    /// fault. The disturbance-fired assertion compares the frames the
    /// transport really withheld against *this*, so `no_withhold` — which
    /// makes the transport ignore the arm — cannot also remove the expectation
    /// that catches it. `arrival_gated`'s oracle is the per-round progress
    /// assertion, so its intent is scoped to the baseline phase where one
    /// message is one frame and the gate's meaning is unambiguous.
    fn asked(self, round: usize) -> Option<Duration> {
        if self == Phase::ZeroCostControl {
            return None;
        }
        if faults::arrival_gated() && self != Phase::SmallWithhold {
            return None;
        }
        let d = WITHHOLD_SCHEDULE[round % WITHHOLD_SCHEDULE.len()];
        (d != Duration::ZERO).then_some(d)
    }

    /// What this round's *test* applies. `no_withhold` removes the disturbance
    /// entirely (the transport ignores the arm), so no round-level disturbance
    /// assertion runs and the disturbance-fired count below is the oracle that
    /// speaks.
    fn applies(self, round: usize) -> Option<Duration> {
        if faults::mode() == faults::NO_WITHHOLD {
            return None;
        }
        self.asked(round)
    }

    /// The flow whose frame is withheld this round, and the byte offset (on a
    /// frame transport) or Data-frame ordinal (on the stock wire) of the frame
    /// to withhold.
    fn arm(self, round: usize, fill: usize) -> Arm {
        let flow = round % FLOWS;
        let msg_bytes = 4 + 16 + fill;
        if self.frame_reassembly() {
            // The k-th message on this flow starts at k * msg_bytes. A small
            // message is one frame, so its start offset is the frame to
            // withhold; a multi-frame message's start offset is its first
            // frame, and `fill / 2` picks the first frame boundary at or past
            // its middle, so the reorder buffer holds a tail across a real gap
            // (frames on both sides of the withheld one). The comparison is
            // "at or after", which holds whatever dispatch cap the scheduler
            // chooses, so the arm is independent of that cap.
            let base = (round * msg_bytes) as u64;
            let want = if self == Phase::MultiFrameWithhold {
                base + (fill / 2) as u64
            } else {
                base
            };
            Arm {
                flow,
                offset: Some(want.min(u32::MAX as u64) as u32),
                ordinal: None,
            }
        } else {
            Arm {
                flow,
                offset: None,
                ordinal: Some(round as u64),
            }
        }
    }
}

/// What one phase measured, for the report and the GATE.md cost.
#[derive(Debug, Default)]
struct PhaseReport {
    rounds: usize,
    withheld: u64,
    messages: usize,
    max_other_clock: Duration,
    max_other_excess: Duration,
    max_armed_excess: Duration,
    /// The simulated time this phase advanced the clock by, itself. It is
    /// compared against the phase's whole elapsed time at the end, so a timer
    /// that moved the paused clock outside the soak's control is a failure
    /// rather than an unexplained reading.
    advanced: Duration,
}

async fn run_phase(phase: Phase, baseline_rounds: usize, base: Instant) -> PhaseReport {
    let mut report = PhaseReport::default();
    let mut pair = Pair::spawn(phase.frame_reassembly());
    let fill = phase.fill();
    let rounds = phase.rounds(baseline_rounds);

    // One long-lived stream per flow, both ends, in open order.
    let mut writers: Vec<StreamWriter> = Vec::with_capacity(FLOWS);
    let mut openers = Vec::with_capacity(FLOWS);
    for _ in 0..FLOWS {
        let (open_result, accept_result) = tokio::join!(pair.opener.open(), pair.accepter.accept());
        let (_client_reader, client_writer) = open_result.expect("client open");
        let (server_reader, _server_writer) = accept_result.expect("server accept");
        writers.push(client_writer);
        openers.push(server_reader);
    }
    let (tx, mut rx) = mpsc::channel(FLOWS * 4);
    let mut readers = JoinSet::new();
    for (flow, reader) in openers.into_iter().enumerate() {
        readers.spawn(read_flow(flow, reader, base, tx.clone()));
    }
    drop(tx);
    // Let the four opens settle so their frames are not mistaken for a round's.
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }

    for round in 0..rounds {
        report.rounds += 1;
        let withhold = phase.applies(round);
        let armed = round % FLOWS;
        match withhold {
            Some(_) => pair.c2s.arm(phase.arm(round, fill)),
            None => pair.c2s.disarm(),
        }
        let before = Instant::now();
        for (flow, writer) in writers.iter_mut().enumerate() {
            let msg = encode_msg(base, flow as u32, round as u32, fill);
            writer.write_all(&msg).await.unwrap_or_else(|e| {
                panic!("{} round {round}: write on flow {flow}: {e}", phase.name())
            });
        }

        let mut samples = Vec::with_capacity(FLOWS);
        if let Some(d) = withhold {
            // On a *frame* transport a withheld frame is passed by later
            // frames, so the other flows' messages must arrive before the
            // release with the clock untouched. On the stock wire the withheld
            // frame heads a byte stream, so everything queued behind it waits
            // too: that head-of-line cost is the transport's, and this phase
            // asserts only the per-flow bound, on every flow.
            let pre_release_expectation = phase.frame_reassembly();
            if pre_release_expectation {
                collect(&mut rx, FLOWS - 1, &mut samples).await;
                let moved = Instant::now().duration_since(before);
                report.max_other_clock = report.max_other_clock.max(moved);
                assert_eq!(
                    moved,
                    Duration::ZERO,
                    "{} round {round}: the clock moved {moved:?} while the undisturbed flows' \
                     messages were delivered - a receive-path wakeup needed a timer",
                    phase.name()
                );
                for sample in &samples {
                    assert_ne!(
                        sample.flow,
                        armed,
                        "{} round {round}: the withheld flow arrived before its release",
                        phase.name()
                    );
                    report.max_other_excess = report.max_other_excess.max(sample.latency);
                    assert!(
                        sample.latency <= EXCESS_BOUND,
                        "{} round {round}: undisturbed flow {} took {:?} with the clock \
                         untouched (bound {EXCESS_BOUND:?})",
                        phase.name(),
                        sample.flow,
                        sample.latency
                    );
                }
            }
            let released_after = if faults::short_release() { d / 2 } else { d };
            advance(released_after).await;
            report.advanced += released_after;
            pair.c2s.set_release();
            collect(&mut rx, FLOWS, &mut samples).await;
            let armed_sample = samples
                .iter()
                .find(|s| s.flow == armed)
                .expect("the withheld flow's sample is missing");
            report.messages += FLOWS;
            assert!(
                armed_sample.latency >= d,
                "{} round {round}: the message on the armed flow {armed} took {:?}, less than \
                 the {d:?} its frame was withheld - the withhold did not land on this flow, so \
                 this round's upper bound is vacuous",
                phase.name(),
                armed_sample.latency
            );
            for sample in &samples {
                if !pre_release_expectation {
                    // Behind the hold on a byte stream, every flow pays it.
                    assert!(
                        sample.latency >= d,
                        "{} round {round}: flow {} took {:?}, less than the {d:?} its frame \
                         was queued behind a hold",
                        phase.name(),
                        sample.flow,
                        sample.latency
                    );
                } else if sample.flow != armed {
                    continue;
                }
                let excess = sample.latency.saturating_sub(d);
                report.max_armed_excess = report.max_armed_excess.max(excess);
                assert!(
                    excess <= EXCESS_BOUND,
                    "{} round {round}: flow {} took {:?} for a {d:?} withhold ({excess:?} more \
                     than the withhold, bound {EXCESS_BOUND:?}) - the receive path amplified the \
                     withheld delivery",
                    phase.name(),
                    sample.flow,
                    sample.latency
                );
            }
        } else {
            collect(&mut rx, FLOWS, &mut samples).await;
            let moved = Instant::now().duration_since(before);
            report.max_other_clock = report.max_other_clock.max(moved);
            assert_eq!(
                moved,
                Duration::ZERO,
                "{} round {round}: the clock moved {moved:?} with nothing withheld - the mux's \
                 own receive path spent simulated time",
                phase.name()
            );
            report.messages += FLOWS;
            for sample in &samples {
                report.max_other_excess = report.max_other_excess.max(sample.latency);
                assert!(
                    sample.latency <= EXCESS_BOUND,
                    "{} round {round}: flow {} took {:?} with nothing withheld",
                    phase.name(),
                    sample.flow,
                    sample.latency
                );
            }
        }
        pair.assert_alive(phase.name(), round);
    }

    let withheld = pair.c2s.withheld.load(Ordering::SeqCst);
    report.withheld = withheld;
    // The disturbance reached the transport, or this phase measured nothing.
    if phase != Phase::ZeroCostControl {
        let expected = (0..rounds).filter(|r| phase.asked(*r).is_some()).count() as u64;
        assert_eq!(
            withheld,
            expected,
            "{}: the transport withheld {withheld} frames, not the {expected} this phase's \
             schedule names - the impairment did not reach the structure under test",
            phase.name()
        );
    } else {
        assert_eq!(
            withheld,
            0,
            "{}: a frame was withheld in the zero-cost control",
            phase.name()
        );
    }

    pair.client_teardowns.abort_all();
    pair.server_teardowns.abort_all();
    pair._pumps.abort_all();
    readers.abort_all();
    report
}

/// The soak. See the module header for what each phase covers and what the
/// whole arm cannot catch.
#[tokio::test(start_paused = true)]
#[ignore = "standard tier: paused-clock stalled-delivery soak, ~1 s"]
async fn a_withheld_delivery_costs_the_withhold_and_nothing_else() {
    assert!(
        !faults::enabled()
            || faults::mode() == faults::NO_WITHHOLD
            || faults::mode() == faults::ARRIVAL_GATED
            || faults::mode() == faults::SHORT_RELEASE,
        "unknown MUX_RECEIVE_STALL_FAULT"
    );
    let base = Instant::now();
    let baseline = rounds();
    let phases = [
        Phase::SmallWithhold,
        Phase::MultiFrameWithhold,
        Phase::StockWireWithhold,
        Phase::ZeroCostControl,
    ];
    let mut reports = Vec::new();
    for phase in phases {
        let report = run_phase(phase, baseline, base).await;
        println!(
            "receive-stall phase={} rounds={} withheld={} messages={} max_undisturbed_clock={:?} \
             max_undisturbed_excess={:?} max_withhold_excess={:?}",
            phase.name(),
            report.rounds,
            report.withheld,
            report.messages,
            report.max_other_clock,
            report.max_other_excess,
            report.max_armed_excess,
        );
        reports.push((phase, report));
    }
    let withheld: u64 = reports.iter().map(|(_, r)| r.withheld).sum();
    let messages: usize = reports.iter().map(|(_, r)| r.messages).sum();
    let advanced: Duration = reports.iter().map(|(_, r)| r.advanced).sum();
    let max_excess = reports
        .iter()
        .map(|(_, r)| r.max_armed_excess.max(r.max_other_excess))
        .max()
        .unwrap();
    println!(
        "receive-stall total: withheld_frames={withheld} messages={messages} \
         max_receive_path_excess_over_withhold={max_excess:?} \
         simulated_window={:?} advanced_by_the_soak_itself={advanced:?} rounds={}",
        base.elapsed(),
        reports.iter().map(|(_, r)| r.rounds).sum::<usize>()
    );
    // The clock is the instrument. Nothing but this soak's own `advance` may
    // move it: an equal reading is what makes "0 ns of receive-path excess" a
    // statement about the mux rather than about a timer that happened to fire.
    assert_eq!(
        base.elapsed(),
        advanced,
        "the paused clock moved {:?} more than the {advanced:?} this soak advanced it by: a \
         timer (the mux heartbeat, or an auto-advanced sleep) fired outside the soak's control, \
         so the zero-excess reading below is not attributable",
        base.elapsed().saturating_sub(advanced)
    );
    assert!(
        withheld >= 200,
        "only {withheld} withheld deliveries: the run is too short to exclude a per-delivery \
         defect rate of a few percent (rule of three: 3/N)"
    );
}
