//! Long-lived-session soak: does any liveness timer fire on *slowness* rather
//! than on a proved-dead path?
//!
//! The operator's client multiplexes everything over one long-lived mux
//! session over a path whose measured floor is ~190 ms and whose measured
//! worst spikes are 1063 ms and 3205 ms. A spike must cost *time*, not the
//! session: a teardown during a finite stall pays a full cold re-establishment
//! that is worse than the spike. The session's only liveness timer is the
//! central reader's sliding receive deadline (4 x the heartbeat interval, so
//! 20 s at the production 5 s heartbeat). This soak holds a live session open
//! for hundreds of simulated seconds and stalls transport *delivery* in both
//! directions for a schedule of durations taken from the field's own numbers,
//! while a second arm crosses the deadline to prove the detector still works.
//!
//! The evidence is a ledger, not an impression: `live_probe::timer_ledger`
//! counts heartbeat frames emitted and consumed and receive-deadline windows
//! armed and expired, and the gate counts the bytes it let through while the
//! stall was set, so a green run cannot be one where the spike was never
//! applied or the deadline was never armed.
//!
//! Tier: **standard** (`#[ignore]`d). `MUX_SPIKE_ROUNDS` sizes the soak; the
//! schedule is fixed, one dimension per arm. `MUX_SPIKE_FAULT=no_stall`
//! disables the gate's hold and must fail the "the spike was applied" check.

use std::{
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use mux::{
    Initiation, MuxConfig, MuxError, StreamAccepter, StreamOpener, spawn_mux_no_reconnection,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf},
    task::JoinSet,
};

/// Production heartbeat on the deployed dual-lane path (`rtp_mux` lane mux
/// config); the steady receive deadline is `4 x` this.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// `central_io::reader::RECEIVE_DEADLINE_INTERVALS`.
const RECEIVE_DEADLINE: Duration = Duration::from_secs(20);

const DEFAULT_ROUNDS: u64 = 120;

/// The spike durations a round applies, in a fixed rotation. The first three
/// are the operator's measured floor and maxima; the last is the largest
/// stall the current deadline admits, one millisecond short of expiry.
const SPIKE_SCHEDULE: [Duration; 4] = [
    Duration::from_millis(190),
    Duration::from_millis(1063),
    Duration::from_millis(3205),
    Duration::from_millis(19_900),
];

fn spike_for_round(round: u64) -> Duration {
    SPIKE_SCHEDULE[(round as usize) % SPIKE_SCHEDULE.len()]
}

/// Red-proof switch: when set, the gate ignores the stall so the soak's
/// non-vacuity check ("nothing was delivered while stalled") must fail.
static FAULT_NO_STALL: AtomicBool = AtomicBool::new(false);

/// A read gate that withholds delivery while `stalled` is set. It is released
/// by the test, not by a timer, so `advance` in the soak is the only clock
/// movement and the spike duration is exact.
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

struct Pair {
    client_opener: StreamOpener,
    server_accepter: StreamAccepter,
    client_teardowns: JoinSet<MuxError>,
    server_teardowns: JoinSet<MuxError>,
}

impl Pair {
    /// Spawn one long-lived session across a gated in-memory duplex, stalling
    /// both delivery directions with one atomic switch so a spike is applied
    /// end to end.
    fn spawn(switch: Arc<StallSwitch>) -> Self {
        let (a, b) = tokio::io::duplex(1 << 20);
        let (client_read, client_write) = tokio::io::split(a);
        let (server_read, server_write) = tokio::io::split(b);
        let client_read = StallGate::new(client_read, Arc::clone(&switch));
        let server_read = StallGate::new(server_read, switch);

        let mut client_teardowns = JoinSet::new();
        let (client_opener, _client_accepter) = spawn_mux_no_reconnection(
            client_read,
            client_write,
            MuxConfig::new(Initiation::Client, HEARTBEAT_INTERVAL),
            &mut client_teardowns,
        );
        let mut server_teardowns = JoinSet::new();
        let (_server_opener, server_accepter) = spawn_mux_no_reconnection(
            server_read,
            server_write,
            MuxConfig::new(Initiation::Server, HEARTBEAT_INTERVAL),
            &mut server_teardowns,
        );
        Self {
            client_opener,
            server_accepter,
            client_teardowns,
            server_teardowns,
        }
    }

    fn teardowns(&mut self) -> Vec<MuxError> {
        let mut out = Vec::new();
        while let Some(joined) = self.client_teardowns.try_join_next() {
            out.push(joined.expect("client session task panicked"));
        }
        while let Some(joined) = self.server_teardowns.try_join_next() {
            out.push(joined.expect("server session task panicked"));
        }
        out
    }

    /// One echo round over a fresh stream: open, payload, echo, compare.
    async fn round(&mut self, payload: &[u8]) -> io::Result<()> {
        let open = self.client_opener.open();
        let accept = self.server_accepter.accept();
        let (open_res, accept_res) = tokio::join!(open, accept);
        let (mut client_reader, mut client_writer) =
            open_res.map_err(|e| io::Error::other(format!("open: {e:?}")))?;
        let (mut server_reader, mut server_writer) =
            accept_res.map_err(|e| io::Error::other(format!("accept: {e:?}")))?;

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
        Ok(())
    }
}

fn rounds() -> u64 {
    std::env::var("MUX_SPIKE_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_ROUNDS)
}

/// The soak: a live session survives the whole field-derived spike schedule.
#[tokio::test(start_paused = true)]
#[ignore = "standard tier: long-lived-session spike soak"]
async fn a_live_session_survives_the_fields_spike_schedule() {
    if std::env::var("MUX_SPIKE_FAULT").as_deref() == Ok("no_stall") {
        FAULT_NO_STALL.store(true, Ordering::SeqCst);
    }
    let rounds = rounds();
    let switch = StallSwitch::new();
    let mut pair = Pair::spawn(Arc::clone(&switch));

    let before = mux::live_probe::timer_ledger();
    let mut completed = 0u64;
    let mut spikes_applied = 0u64;
    let payload = b"interactive request/response payload";

    for round in 0..rounds {
        let spike = spike_for_round(round);
        // Quiesce first: the readers must be parked with an armed deadline
        // before the clock moves, or the advance proves nothing.
        settle_parked().await;
        switch.stall();
        let withheld_before = switch.delivered_while_stalled.load(Ordering::SeqCst);
        let held_before = switch.held_polls.load(Ordering::SeqCst);
        let received_before = mux::live_probe::timer_ledger().heartbeats_received;
        tokio::time::advance(spike).await;
        // Keep the stall in force while the runtime runs: a deadline that the
        // spike crossed must expire *during* the stall, and any frame the peer
        // wrote must be observed as held.
        settle_parked().await;
        let withheld_during = switch.delivered_while_stalled.load(Ordering::SeqCst);
        let held_during = switch.held_polls.load(Ordering::SeqCst);
        let received_during = mux::live_probe::timer_ledger().heartbeats_received;
        switch.release();
        settle_parked().await;

        assert!(
            withheld_during == withheld_before,
            "round {round}: the {spike:?} spike delivered {} bytes while stalled, so the \
             spike was not applied and this run proves nothing",
            withheld_during - withheld_before,
        );
        if spike >= 2 * HEARTBEAT_INTERVAL {
            // A heartbeat is due within this stall, so a live peer wrote into
            // the transport during it: none of it may have reached the session,
            // and the gate must have held at least one read for it.
            assert!(
                held_during > held_before,
                "round {round}: the {spike:?} spike held no read; a heartbeat was due inside \
                 it, so the stall cannot have been in force",
            );
            assert_eq!(
                received_during,
                received_before,
                "round {round}: a {spike:?} stall let {} heartbeat(s) through, so the \
                 transport was not actually stalled",
                received_during - received_before,
            );
        }
        spikes_applied += 1;

        let teardowns = pair.teardowns();
        assert!(
            teardowns.is_empty(),
            "round {round}: a {spike:?} spike (receive deadline {RECEIVE_DEADLINE:?}) tore the \
             session down: {teardowns:?}",
        );

        pair.round(payload).await.unwrap_or_else(|e| {
            panic!("round {round}: traffic did not recover after the {spike:?} spike: {e}")
        });
        completed += 1;
    }

    let after = mux::live_probe::timer_ledger();
    let delta = after.since(&before);
    let teardowns = pair.teardowns();
    println!(
        "spike soak: rounds={completed}/{rounds} spikes_applied={spikes_applied} \
         teardowns={} | {delta}",
        teardowns.len(),
    );
    assert_eq!(completed, rounds, "not every round completed");
    assert_eq!(spikes_applied, rounds, "not every spike was applied");
    assert!(
        teardowns.is_empty(),
        "the session did not survive: {teardowns:?}"
    );
    // Red proof, recorded in GATE.md ("Session survival under latency spikes"):
    // with `central_io::reader::RECEIVE_DEADLINE_INTERVALS` 4 -> 3 the 19.9 s
    // spike crosses the 15 s production deadline and this arm fails at the
    // per-round teardown assert below, naming the deadline:
    //   `round 3: a 19.9s spike (receive deadline 20s) tore the session down:
    //    [IoReader(Custom { kind: TimedOut, error: "receive deadline -
    //    session timed out" }), …]`
    assert_eq!(
        delta.receive_deadline_expiries, 0,
        "a receive-deadline window expired during a spike the session survived",
    );
    // Sanity on the instrument itself: the run must actually have armed the
    // deadline and exchanged the heartbeats that reset it. A run that armed
    // nothing would pass the expiry check vacuously.
    assert!(
        delta.receive_deadline_sleeps_armed > 0,
        "no receive-deadline sleep was ever registered on the timer wheel ({delta}); the \
         zero-expiry result is vacuous",
    );
    assert!(
        switch.held_polls.load(Ordering::SeqCst) > 0,
        "the gate never held a read across the whole soak; the stall was never in force",
    );
    assert!(
        delta.receive_deadline_arms >= rounds,
        "only {} receive-deadline arms across {rounds} rounds: the deadline was not \
         armed and the expiry count is vacuous",
        delta.receive_deadline_arms,
    );
    assert!(
        delta.heartbeats_sent > 0 && delta.heartbeats_received > 0,
        "no heartbeats were exchanged ({delta}); the liveness path was never exercised",
    );
}

/// Drive the runtime to quiescence so every parked central reader has created
/// its deadline sleep on the timer wheel. Without this the advance that
/// follows can move the clock past a window the reader has not yet armed, and
/// the test would prove nothing about the deadline.
async fn settle_parked() {
    for _ in 0..256 {
        tokio::task::yield_now().await;
    }
}

/// The detector is still live. This is the positive half of the soak's
/// zero-expiry claim — without it, "no expiry" could mean "the deadline can
/// never fire".
///
/// The advance is deliberately a full second past the deadline rather than one
/// millisecond: `tokio::time::advance` does not reliably cascade a coarse
/// timer-wheel slot on a 1 ms step at this magnitude (measured: advancing
/// 20.001 s leaves `receive_deadline_expiries=0`, while 21 s gives 2), so a
/// millisecond-exact crossing here would assert the harness's granularity
/// rather than the session's. The exact boundary is pinned at a fine scale by
/// the in-crate `the_steady_receive_deadline_is_four_heartbeat_intervals`.
#[tokio::test(start_paused = true)]
async fn a_stall_past_the_deadline_still_trips_the_detector() {
    let switch = StallSwitch::new();
    let mut pair = Pair::spawn(Arc::clone(&switch));

    // Warm the session so both endpoints have exchanged frames.
    pair.round(b"warm").await.expect("warm round");
    assert!(pair.teardowns().is_empty(), "the warm session died");
    settle_parked().await;
    let before = mux::live_probe::timer_ledger();

    switch.stall();
    tokio::time::advance(RECEIVE_DEADLINE + Duration::from_secs(1)).await;
    // Let the due deadline be processed while the stall is still in force, so
    // the expiry is the deadline firing rather than a re-arm after release.
    settle_parked().await;
    switch.release();
    // Let the reader's deadline error propagate: reader task -> control loop ->
    // the session supervision task that owns both join sets.
    for _ in 0..256 {
        tokio::task::yield_now().await;
    }

    let teardowns = pair.teardowns();
    let delta = mux::live_probe::timer_ledger().since(&before);
    println!("detector: {delta}");
    assert!(
        delta.receive_deadline_expiries > 0,
        "the deadline at t={} ms never expired under a stall past it ({delta})",
        before.last_deadline_ms,
    );
    assert!(
        !teardowns.is_empty(),
        "a stall past the {RECEIVE_DEADLINE:?} receive deadline did not tear the session \
         down: the deadline is not live and the soak's zero-expiry result is vacuous",
    );
    assert!(
        teardowns.iter().any(|e| matches!(
            e,
            MuxError::IoReader(io) if io.to_string().contains("receive deadline")
        )),
        "the teardown was not the receive deadline: {teardowns:?}",
    );
}
