//! Does an *ingress* event wait behind an *egress* transport write?
//!
//! `MuxControl::open` acquires the egress fair-queue token and only then
//! inserts its stream-table entry, so a refused open leaves no entry behind.
//! The token is minted by the consumer that drains the egress receiver:
//!
//! `handle_central_read` (`src/control.rs`) -> `accept_peer_stream` ->
//! `open_stream` -> `MuxControl::open` -> `WriteDataTxFactory::for_stream`
//! (`src/central_io/scheduler.rs`) -> `QueueRegistrar::open`
//! (`src/fair_queue.rs`).
//!
//! If that drain runs only when a transport write returns, the ingress path is
//! parked for the write's whole duration. This arm measures what an ingress
//! event pays when it arrives during such a write, on a paused clock: the
//! client's write half parks inside `poll_write` for `D` simulated
//! milliseconds (the field's 190 ms / 1063 ms / 3205 ms), the transport stamps
//! that write's begin and end in simulated time, and the ingress event is
//! issued while the write is provably in flight.
//!
//! The property asserted is not a latency bound but a **state**: the ingress
//! event is serviced with the stalled write *still in flight*, and with the
//! paused clock exactly where the write began. A fix that merely shortened the
//! wait would pass a bound; it would not pass this.
//!
//! Four shapes, one varying dimension from the `SteadyOpen` baseline:
//!
//! * `SteadyOpen` — baseline: no transport write is stalled, and the ingress
//!   event is a peer open. The accept is serviced with the clock untouched:
//!   the mux's own stream-open cost.
//! * `OpenUnderStall` — varies the **write duration**: `D` is rotated through
//!   the field's own numbers.
//! * `DataUnderStallNoOpen` — the control. The write stalls and a data frame on
//!   an *established* stream arrives, with no stream introduced:
//!   `accept_peer_stream` returns before it reaches the egress, so this frame
//!   must be delivered while the writer is parked. It isolates the blocking
//!   `open` from the write.
//! * `DataUnderStallWithOpen` — the same data frame, but a peer open is
//!   introduced first, so the control loop is inside `accept_peer_stream` when
//!   the frame arrives and the frame queues behind it.
//!
//! Every stalled round asserts its own sanity: the transport really entered the
//! stalled write (`writes_stalled` advanced by exactly one), `in_flight` was
//! observed true at the instant the ingress event was issued and still true
//! when it was serviced, the transport's recorded simulated begin equals the
//! issue instant, and the run's whole elapsed simulated time equals the sum of
//! the stalls the arm itself armed — so a timer outside the arm firing is a
//! failure rather than an unexplained reading.
//!
//! Tier: **standard** (`#[ignore]`d, asserting). `MUX_EGRESS_STALL_ROUNDS`
//! scales each shape's round count.
//!
//! Vacuity: the property is removed by deleting the drain from the egress
//! writer's in-flight write (the `answering` branch of `write_answering_opens`
//! in `src/central_io/encoder.rs`), which is the shape this arm was written
//! against: the same command then fails naming the observed wait
//! (`a peer open ... was serviced after only {wait:?}` is what a *decoupled*
//! run prints; with the drain removed it prints the write's full duration).
//!
//! What this arm cannot catch, stated rather than implied: the transport is an
//! in-memory `duplex`, not a socket, and the stall is the arm's own `Sleep`
//! rather than a real blocked `sendmsg` — so it establishes the mux-internal
//! coupling (which task must run for the token to be minted) and *not* that a
//! real transport write blocks for `D`. `tokio::io::duplex` also has one shared
//! buffer per direction, so a stalled *reader* could back-pressure this
//! writer's transport write in a way a socket pair would not; that reverse
//! coupling is the shim's, not the mux's, and the mux layer's own answer is
//! established from the code: the writer awaits only its producer channels and
//! the transport, so it never waits on the ingress task's *progress*. The
//! heartbeat is deliberately far out (its cadence and the receive deadline are
//! `spike_survival_soak`'s cell), and no impairment but the write stall is
//! applied.

use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
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
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, duplex},
    sync::{Notify, mpsc},
    task::JoinSet,
    time::{Instant, Sleep, sleep},
};

// --- constants -------------------------------------------------------------

/// The heartbeat is set far out on purpose. This arm's cell is the egress
/// transport write's own duration, and the only clock movement it wants is the
/// stalled write's timer; a production-cadence heartbeat puts its own sleeps on
/// the paused clock (and the writer re-creates that sleep on every dispatched
/// frame, so under traffic the cadence slips), which lets the clock
/// auto-advance past the arm's own instant. The heartbeat and receive-deadline
/// cells belong to `spike_survival_soak`; this arm declares them empty.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(3_600);
/// Buffered bytes per direction of the in-memory transport.
const DUPLEX_BUF: usize = 1 << 20;
/// The field's measured floor and maxima: the write durations the egress
/// transport is stalled for.
const STALL_SCHEDULE: [Duration; 3] = [
    Duration::from_millis(190),
    Duration::from_millis(1063),
    Duration::from_millis(3205),
];
/// Default rounds per shape; the stalled shapes rotate the schedule, so this is
/// the per-duration trial count `rounds / 3`.
const DEFAULT_ROUNDS: usize = 30;
/// How much simulated time a serviced ingress event may carry. On a paused
/// clock the only way to spend simulated time is a timer, so this is slack for
/// task wakeups, not for the mux.
const SLACK: Duration = Duration::from_millis(1);
/// Scheduler yields allowed while waiting for a frame to travel session ->
/// transport -> peer session without moving the clock.
const TRAVEL_YIELDS: usize = 4_096;
/// Trigger payload written on the client's own stream. The transport arms on a
/// write carrying these bytes, so the arm lands on the round's own frame and
/// not on a heartbeat.
const TRIGGER: &[u8] = b"egress-write-ingress-coupling-trigger";
/// Measured payload the server writes on the established measured stream.
const MEASURED: &[u8] = b"egress-write-ingress-coupling-measured";
/// Bound on the measured-delivery channel. One sample per round is produced
/// and drained, so this is headroom for the shape boundary, not a load.
const SAMPLE_CAPACITY: usize = 16;

fn shape_rounds() -> usize {
    match std::env::var("MUX_EGRESS_STALL_ROUNDS") {
        Ok(v) => v.parse().expect("MUX_EGRESS_STALL_ROUNDS must be a number"),
        Err(_) => DEFAULT_ROUNDS,
    }
}

// --- the write-stalling transport -----------------------------------------

/// Shared state of the client's egress transport. The stalled write's begin and
/// end are stamped in *simulated* milliseconds by the transport itself, so the
/// arm never has to infer them from a scheduler observation.
#[derive(Debug)]
struct StallState {
    base: Instant,
    /// One-shot: the next trigger-carrying `poll_write` after this is set parks
    /// for the configured duration.
    armed: AtomicBool,
    duration_ms: AtomicU64,
    in_flight: AtomicBool,
    /// Stalled writes actually entered. Must advance by exactly one per round.
    writes_stalled: AtomicU64,
    begin_ms: AtomicU64,
    end_ms: AtomicU64,
    began: Notify,
    ended: Notify,
}

impl StallState {
    fn new(base: Instant) -> Arc<Self> {
        Arc::new(Self {
            base,
            armed: AtomicBool::new(false),
            duration_ms: AtomicU64::new(0),
            in_flight: AtomicBool::new(false),
            writes_stalled: AtomicU64::new(0),
            begin_ms: AtomicU64::new(0),
            end_ms: AtomicU64::new(0),
            began: Notify::new(),
            ended: Notify::new(),
        })
    }

    fn arm(&self, d: Duration) {
        self.duration_ms
            .store(d.as_millis() as u64, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
    }

    fn elapsed_ms(&self) -> u64 {
        self.base.elapsed().as_millis() as u64
    }

    fn in_flight(&self) -> bool {
        self.in_flight.load(Ordering::SeqCst)
    }

    async fn wait_begin(&self) {
        while !self.in_flight() {
            self.began.notified().await;
        }
    }

    async fn wait_end(&self) {
        while self.in_flight() {
            self.ended.notified().await;
        }
    }
}

/// Wraps the client's write half. A write carrying [`TRIGGER`] parks for the
/// armed duration before the bytes reach the transport; every other write — and
/// every trigger write with nothing armed — passes straight through.
struct WriteStall<W> {
    inner: W,
    state: Arc<StallState>,
    parked: bool,
    delay: Option<Pin<Box<Sleep>>>,
}

impl<W> WriteStall<W> {
    fn new(inner: W, state: Arc<StallState>) -> Self {
        Self {
            inner,
            state,
            parked: false,
            delay: None,
        }
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for WriteStall<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let is_trigger =
            buf.len() >= TRIGGER.len() && buf.windows(TRIGGER.len()).any(|w| w == TRIGGER);
        if !this.parked && is_trigger && this.state.armed.swap(false, Ordering::SeqCst) {
            let d = Duration::from_millis(this.state.duration_ms.load(Ordering::SeqCst));
            this.state.writes_stalled.fetch_add(1, Ordering::SeqCst);
            this.state.in_flight.store(true, Ordering::SeqCst);
            this.state
                .begin_ms
                .store(this.state.elapsed_ms(), Ordering::SeqCst);
            this.state.began.notify_one();
            this.parked = true;
            let mut delay = Box::pin(sleep(d));
            // Arm the wheel in this poll. A `Sleep` registers its timeout only
            // when it is first polled; a sleep created here but first polled
            // later leaves the runtime with **no deadline at all**, and the
            // paused clock then jumps to whatever timer is next (measured: it
            // jumped ~7 h to the receive deadline and tore the session down).
            if delay.as_mut().poll(cx).is_pending() {
                this.delay = Some(delay);
                return Poll::Pending;
            }
            // A positive delay that is already due: finish the write without
            // spending another poll.
            this.parked = false;
            this.state.in_flight.store(false, Ordering::SeqCst);
            this.state
                .end_ms
                .store(this.state.elapsed_ms(), Ordering::SeqCst);
            this.state.ended.notify_one();
        }
        if this.parked {
            if let Some(delay) = this.delay.as_mut() {
                std::task::ready!(delay.as_mut().poll(cx));
                this.delay = None;
            }
            this.parked = false;
            this.state.in_flight.store(false, Ordering::SeqCst);
            this.state
                .end_ms
                .store(this.state.elapsed_ms(), Ordering::SeqCst);
            this.state.ended.notify_one();
        }
        Pin::new(&mut this.inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }

    /// Force the encoder down its non-vectored path, where a frame's fixed
    /// header and body are concatenated and written once, so the armed write is
    /// exactly one frame rather than a vectored pair.
    fn is_write_vectored(&self) -> bool {
        false
    }
}

// --- session pair ----------------------------------------------------------

struct Pair {
    /// Kept alive: the client's opener and the server's accepter are the
    /// session's idle request handles, and holding them mirrors production,
    /// where the application's halves outlive every shape.
    _client_opener: StreamOpener,
    client_accepter: StreamAccepter,
    server_opener: StreamOpener,
    _server_accepter: StreamAccepter,
    /// Client side of the trigger stream; the client writes here to put its
    /// egress writer inside a transport write.
    trigger_writer: StreamWriter,
    /// Server side of the measured (server -> client) stream.
    measured_writer: StreamWriter,
    samples: mpsc::Receiver<Instant>,
    client_teardowns: JoinSet<MuxError>,
    server_teardowns: JoinSet<MuxError>,
    /// Streams opened during a shape are kept alive: dropping one emits close
    /// frames that would interleave with the next round's armed write.
    held: Vec<(StreamReader, StreamWriter)>,
    drains: JoinSet<()>,
    /// A peer open driven while the test does other work. A `JoinSet` rather
    /// than a detached task, so it is reaped and its panic is observed.
    open_tasks: JoinSet<Result<(StreamReader, StreamWriter), mux::StreamOpenError>>,
    state: Arc<StallState>,
}

impl Pair {
    async fn spawn(base: Instant) -> Self {
        let state = StallState::new(base);
        let (client, server) = duplex(DUPLEX_BUF);
        let (client_read, client_write) = tokio::io::split(client);
        let (server_read, server_write) = tokio::io::split(server);

        let client_config = MuxConfig::new(Initiation::Client, HEARTBEAT_INTERVAL);
        let mut client_teardowns = JoinSet::new();
        let (client_opener, mut client_accepter) = spawn_mux_no_reconnection(
            client_read,
            WriteStall::new(client_write, Arc::clone(&state)),
            client_config,
            &mut client_teardowns,
        );
        let server_config = MuxConfig::new(Initiation::Server, HEARTBEAT_INTERVAL);
        let mut server_teardowns = JoinSet::new();
        let (server_opener, mut server_accepter) = spawn_mux_no_reconnection(
            server_read,
            server_write,
            server_config,
            &mut server_teardowns,
        );

        let (sample_tx, samples) = mpsc::channel(SAMPLE_CAPACITY);
        let mut drains = JoinSet::new();

        // Client opens the trigger stream C; the server accepts it and drains
        // it so the client's writes never back up on a full duplex.
        let (open_c, accept_c) = tokio::join!(client_opener.open(), server_accepter.accept());
        let (client_c_reader, trigger_writer) = open_c.expect("client opens the trigger stream");
        let (server_c_reader, server_c_writer) =
            accept_c.expect("server accepts the trigger stream");
        drains.spawn(async move {
            let mut server_c_reader = server_c_reader;
            let mut buf = vec![0u8; 256];
            while server_c_reader.read(&mut buf).await.unwrap_or(0) != 0 {}
        });

        // Server opens the measured stream D; the client accepts it and hands
        // every completed message to the arm as a simulated timestamp.
        let (open_d, accept_d) = tokio::join!(server_opener.open(), client_accepter.accept());
        let (server_d_reader, measured_writer) = open_d.expect("server opens the measured stream");
        let (mut client_d_reader, client_d_writer) =
            accept_d.expect("client accepts the measured stream");
        drains.spawn(async move {
            let mut msg = [0u8; MEASURED.len()];
            loop {
                if client_d_reader.read_exact(&mut msg).await.is_err() {
                    return;
                }
                if sample_tx.send(Instant::now()).await.is_err() {
                    return;
                }
            }
        });

        Self {
            _client_opener: client_opener,
            client_accepter,
            server_opener,
            _server_accepter: server_accepter,
            trigger_writer,
            measured_writer,
            samples,
            client_teardowns,
            server_teardowns,
            held: vec![
                (client_c_reader, server_c_writer),
                (server_d_reader, client_d_writer),
            ],
            drains,
            open_tasks: JoinSet::new(),
            state,
        }
    }

    fn assert_alive(&mut self, shape: &str, round: usize) {
        let mut died = Vec::new();
        while let Some(joined) = self.client_teardowns.try_join_next() {
            died.push(format!(
                "client: {:?}",
                joined.expect("client task panicked")
            ));
        }
        while let Some(joined) = self.server_teardowns.try_join_next() {
            died.push(format!(
                "server: {:?}",
                joined.expect("server task panicked")
            ));
        }
        assert!(
            died.is_empty(),
            "{shape} round {round}: a session died: {died:?}",
        );
    }

    /// Arm the transport, put the client's egress writer inside a transport
    /// write, and return the simulated instant that write began — asserting on
    /// the way that the write really was entered and that the clock has not
    /// moved since.
    async fn arm_and_trigger(&mut self, d: Duration, shape: &str, round: usize) -> Instant {
        let before = self.state.writes_stalled.load(Ordering::SeqCst);
        self.state.arm(d);
        self.trigger_writer
            .write_all(TRIGGER)
            .await
            .unwrap_or_else(|e| panic!("{shape} round {round}: trigger write: {e}"));
        self.state.wait_begin().await;
        let stalled = self.state.writes_stalled.load(Ordering::SeqCst);
        assert_eq!(
            stalled,
            before + 1,
            "{shape} round {round}: {} transport write(s) were stalled, not the one this round \
             armed - the disturbance did not land on the egress path",
            stalled - before,
        );
        let t_issue = Instant::now();
        assert!(
            self.state.in_flight(),
            "{shape} round {round}: the armed transport write was not in flight when the ingress \
             event was issued, so this round measures nothing",
        );
        let issue_ms = self.state.elapsed_ms();
        assert_eq!(
            self.state.begin_ms.load(Ordering::SeqCst),
            issue_ms,
            "{shape} round {round}: the ingress event was issued at simulated {issue_ms} ms but \
             the stalled write began at {} ms; the clock moved between them, so the reading below \
             is not attributable to the write",
            self.state.begin_ms.load(Ordering::SeqCst),
        );
        t_issue
    }
}

/// Wait until the control loop has taken the peer's `Open` frame (its
/// `handled_open` counter advanced), i.e. it is committed to `open`. Yields
/// keep the clock pinned, so this cannot advance time.
async fn await_open_handled(before: u64, shape: &str, round: usize) {
    for _ in 0..TRAVEL_YIELDS {
        if mux::live_probe::totals().pipeline.handled_open > before {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!(
        "{shape} round {round}: the peer's Open frame never reached the control loop within \
         {TRAVEL_YIELDS} scheduler yields, so this round never put the ingress path inside \
         accept_peer_stream and would measure the wrong thing",
    );
}

// --- shapes and report -----------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    SteadyOpen,
    OpenUnderStall,
    DataUnderStallNoOpen,
    DataUnderStallWithOpen,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::SteadyOpen => "SteadyOpen",
            Shape::OpenUnderStall => "OpenUnderStall",
            Shape::DataUnderStallNoOpen => "DataUnderStallNoOpen",
            Shape::DataUnderStallWithOpen => "DataUnderStallWithOpen",
        }
    }

    /// The write duration this round applies. `SteadyOpen` stalls nothing; the
    /// rest rotate the field's schedule.
    fn stall(self, round: usize) -> Duration {
        match self {
            Shape::SteadyOpen => Duration::ZERO,
            _ => STALL_SCHEDULE[round % STALL_SCHEDULE.len()],
        }
    }

    fn stalls(self) -> bool {
        self != Shape::SteadyOpen
    }
}

#[derive(Debug, Default)]
struct ShapeReport {
    rounds: usize,
    open_waits: Vec<Duration>,
    data_waits: Vec<Duration>,
    /// Rounds whose ingress event was serviced while the stalled write was
    /// still in flight. This is the property, not a corollary of the wait.
    serviced_in_flight: usize,
    stalls: u64,
    /// Simulated time the shape's own stalled writes account for. The run's
    /// whole elapsed simulated time must equal this, so a timer that moved the
    /// clock outside the arm's control is a failure rather than an unexplained
    /// reading.
    advanced: Duration,
}

/// Assert an ingress event issued during a stalled write was serviced *while
/// that write was still in flight*, and with the clock still at its begin.
fn assert_serviced(
    rep: &mut ShapeReport,
    shape: &str,
    round: usize,
    what: &str,
    d: Duration,
    wait: Duration,
    in_flight: bool,
) {
    assert!(
        in_flight,
        "{shape} round {round}: {what} was not serviced until the {d:?} transport write had \
         returned - the ingress path is gated by the egress write (observed wait {wait:?})",
    );
    assert!(
        wait <= SLACK,
        "{shape} round {round}: {what} cost {wait:?} of simulated time while it queued for a \
         {d:?} transport write; the ingress paid for the write's duration",
    );
    rep.serviced_in_flight += 1;
}

async fn run_shape(shape: Shape, base: Instant) -> ShapeReport {
    let mut report = ShapeReport::default();
    let mut pair = Pair::spawn(base).await;
    let rounds = shape_rounds();

    // Let setup traffic (the two opens and their wire frames) settle.
    for _ in 0..256 {
        tokio::task::yield_now().await;
    }

    for round in 0..rounds {
        report.rounds += 1;
        let d = shape.stall(round);
        let t_issue = if shape.stalls() {
            pair.arm_and_trigger(d, shape.name(), round).await
        } else {
            Instant::now()
        };

        match shape {
            Shape::SteadyOpen | Shape::OpenUnderStall => {
                let opener = pair.server_opener.clone();
                let open_fut = opener.open();
                let accept_fut = pair.client_accepter.accept();
                let (open_res, accept_res) = tokio::join!(open_fut, accept_fut);
                let t_done = Instant::now();
                let in_flight = pair.state.in_flight();
                let (reader, writer) = accept_res
                    .unwrap_or_else(|e| panic!("{} round {round}: accept: {e:?}", shape.name()));
                let (r2, w2) = open_res
                    .unwrap_or_else(|e| panic!("{} round {round}: open: {e:?}", shape.name()));
                pair.held.push((reader, writer));
                pair.held.push((r2, w2));
                let wait = t_done.duration_since(t_issue);
                if shape.stalls() {
                    assert_serviced(
                        &mut report,
                        shape.name(),
                        round,
                        "a peer open",
                        d,
                        wait,
                        in_flight,
                    );
                }
                assert_eq!(
                    Instant::now(),
                    t_issue,
                    "{} round {round}: the clock moved {wait:?} while a peer open was accepted",
                    shape.name(),
                );
                if !shape.stalls() {
                    assert!(
                        wait <= SLACK,
                        "{} round {round}: a peer open cost {wait:?} with no transport write in \
                         flight",
                        shape.name(),
                    );
                }
                report.open_waits.push(wait);
            }
            Shape::DataUnderStallNoOpen => {
                pair.measured_writer
                    .write_all(MEASURED)
                    .await
                    .unwrap_or_else(|e| panic!("{} round {round}: data write: {e}", shape.name()));
                let sample = pair
                    .samples
                    .recv()
                    .await
                    .expect("the measured reader ended");
                let in_flight = pair.state.in_flight();
                let wait = sample.duration_since(t_issue);
                assert_eq!(
                    Instant::now(),
                    t_issue,
                    "{} round {round}: the clock moved {wait:?} while a data frame on an \
                     established stream was delivered",
                    shape.name(),
                );
                assert_serviced(
                    &mut report,
                    shape.name(),
                    round,
                    "a data frame on an established stream",
                    d,
                    wait,
                    in_flight,
                );
                report.data_waits.push(wait);
            }
            Shape::DataUnderStallWithOpen => {
                let opener = pair.server_opener.clone();
                let handled_before = mux::live_probe::totals().pipeline.handled_open;
                pair.open_tasks.spawn(async move { opener.open().await });
                await_open_handled(handled_before, shape.name(), round).await;
                assert_eq!(
                    Instant::now(),
                    t_issue,
                    "{} round {round}: the clock moved while the peer's Open frame reached the \
                     control loop, so the reading below is not attributable to the write",
                    shape.name(),
                );
                assert!(
                    pair.state.in_flight(),
                    "{} round {round}: the stalled write ended before the ingress frame was \
                     written, so this round does not measure a frame queued behind it",
                    shape.name(),
                );
                pair.measured_writer
                    .write_all(MEASURED)
                    .await
                    .unwrap_or_else(|e| panic!("{} round {round}: data write: {e}", shape.name()));
                let data_sample = pair
                    .samples
                    .recv()
                    .await
                    .expect("the measured reader ended");
                let data_in_flight = pair.state.in_flight();
                let accept_done = pair.client_accepter.accept().await;
                let t_accept = Instant::now();
                let accept_in_flight = pair.state.in_flight();
                let (reader, writer) = accept_done
                    .unwrap_or_else(|e| panic!("{} round {round}: accept: {e:?}", shape.name()));
                let (r2, w2) = pair
                    .open_tasks
                    .join_next()
                    .await
                    .expect("the spawned open task vanished")
                    .expect("the spawned open task panicked")
                    .unwrap_or_else(|e| panic!("{} round {round}: open: {e:?}", shape.name()));
                pair.held.push((reader, writer));
                pair.held.push((r2, w2));
                let open_wait = t_accept.duration_since(t_issue);
                let data_wait = data_sample.duration_since(t_issue);
                assert_serviced(
                    &mut report,
                    shape.name(),
                    round,
                    "a data frame queued behind a blocked peer open",
                    d,
                    data_wait,
                    data_in_flight,
                );
                // The accept is checked after the frame, so it reports the same
                // in-flight state; `assert_serviced` is called once per round
                // for the counter's sake.
                assert!(
                    accept_in_flight,
                    "{} round {round}: the peer open was not serviced until the {d:?} transport \
                     write had returned",
                    shape.name(),
                );
                assert!(
                    open_wait <= SLACK,
                    "{} round {round}: a peer open cost {open_wait:?} while it queued for a \
                     {d:?} transport write",
                    shape.name(),
                );
                assert_eq!(
                    Instant::now(),
                    t_issue,
                    "{} round {round}: the clock moved while both ingress events were serviced",
                    shape.name(),
                );
                report.open_waits.push(open_wait);
                report.data_waits.push(data_wait);
            }
        }

        if shape.stalls() {
            pair.state.wait_end().await;
            let end_ms = pair.state.end_ms.load(Ordering::SeqCst);
            let issued_ms = t_issue.duration_since(base).as_millis() as u64;
            assert!(
                end_ms >= issued_ms + d.as_millis() as u64,
                "{} round {round}: the transport's write began at {issued_ms} ms and ended at \
                 {end_ms} ms, which is not the {d:?} this round armed",
                shape.name(),
            );
            report.stalls += 1;
            report.advanced += d;
        }

        pair.assert_alive(shape.name(), round);
        // Let the round's own bookkeeping settle before the next arm.
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
    }

    pair.client_teardowns.abort_all();
    pair.server_teardowns.abort_all();
    pair.drains.abort_all();
    pair.open_tasks.abort_all();
    drop(pair);
    report
}

fn max(waits: &[Duration]) -> Duration {
    waits.iter().copied().max().unwrap_or(Duration::ZERO)
}

fn min(waits: &[Duration]) -> Duration {
    waits.iter().copied().min().unwrap_or(Duration::ZERO)
}

/// The arm. See the module header for what each shape covers and what the whole
/// arm cannot catch.
#[tokio::test(start_paused = true)]
#[ignore = "standard tier: paused-clock egress-write/ingress coupling measurement"]
async fn an_ingress_event_does_not_wait_behind_an_egress_transport_write() {
    let base = Instant::now();
    let shapes = [
        Shape::SteadyOpen,
        Shape::OpenUnderStall,
        Shape::DataUnderStallNoOpen,
        Shape::DataUnderStallWithOpen,
    ];
    let mut reports = Vec::new();
    for shape in shapes {
        let report = run_shape(shape, base).await;
        println!(
            "egress-ingress shape={:<24} rounds={:<3} stalls={:<3} serviced_in_flight={:<3} \
             open_waits={{min={:?} max={:?} n={}}} data_waits={{min={:?} max={:?} n={}}}",
            shape.name(),
            report.rounds,
            report.stalls,
            report.serviced_in_flight,
            min(&report.open_waits),
            max(&report.open_waits),
            report.open_waits.len(),
            min(&report.data_waits),
            max(&report.data_waits),
            report.data_waits.len(),
        );
        reports.push((shape, report));
    }

    let stalled_open: Vec<Duration> = reports
        .iter()
        .filter(|(s, _)| *s == Shape::OpenUnderStall)
        .flat_map(|(_, r)| r.open_waits.iter().copied())
        .collect();
    let stalled_data: Vec<Duration> = reports
        .iter()
        .filter(|(s, _)| *s == Shape::DataUnderStallWithOpen)
        .flat_map(|(_, r)| r.data_waits.iter().copied())
        .collect();
    let serviced: usize = reports.iter().map(|(_, r)| r.serviced_in_flight).sum();
    let stalls: u64 = reports.iter().map(|(_, r)| r.stalls).sum();
    println!(
        "egress-ingress total: simulated_window={:?} stalls_applied={stalls} \
         serviced_in_flight={serviced} peer_open_waits={} max_peer_open_wait={:?} \
         data_behind_open_waits={} max_data_behind_open_wait={:?}",
        base.elapsed(),
        stalled_open.len(),
        max(&stalled_open),
        stalled_data.len(),
        max(&stalled_data),
    );

    assert!(
        stalls > 0 && serviced >= stalls as usize,
        "the run stalled {stalls} transport write(s) but serviced only {serviced} ingress \
         event(s) in flight: too few to report",
    );

    // The clock is the instrument. The only simulated time this arm intends to
    // spend is the stalled writes themselves; anything else is a timer that
    // fired outside its control and would make the readings unattributable.
    let advanced: Duration = reports.iter().map(|(_, r)| r.advanced).sum();
    assert_eq!(
        base.elapsed(),
        advanced,
        "the paused clock moved {:?} more than the {advanced:?} the arm's own stalled writes \
         account for: a timer outside the arm fired, so the readings above are not attributable \
         to the transport write",
        base.elapsed().saturating_sub(advanced),
    );
}
