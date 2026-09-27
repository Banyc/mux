//! Request-path capacity saturation: every bounded resource a request can
//! reach must answer its full condition with an outcome the caller can act on
//! — an error, or a documented drop — and never with a wait that outlives the
//! condition that caused it.
//!
//! The defect this generalises from is `mux`'s egress fair-queue token table:
//! it held 1 024 queues while the stream table admitted 8 192, and
//! `MuxControl::open` inserted the stream-table entry before it asked for the
//! token, so reaching the token bound blocked the open for the life of the
//! session instead of failing it. The same question asked of every other
//! bounded thing on the request path is: *what does the caller observe when it
//! is full?* Three answers are possible and only two are acceptable —
//! **backpressure** (an error the caller can act on, or a wait that the
//! consumer's own progress ends), a **hang** (a wait nothing will ever end),
//! and a **silent drop** (no error, no progress, the message simply gone).
//!
//! This file measures one arm per reachable capacity and asserts the observed
//! outcome, so the table in `GATE.md` is a measurement and not a reading of
//! the source. Each arm proves its own capacity was actually reached (a count
//! it prints) before it asserts the outcome; an arm that never filled the
//! resource would pass vacuously.
//!
//! Why one test function and not one per arm: `mux::live_probe`'s censuses,
//! ledgers and counters are **process-global statics**, and the default tier
//! runs one test binary's tests in parallel threads. Two arms running
//! concurrently would read each other's sessions, so the arms that read the
//! probe are serialised by living in one function. Everything here is driven
//! on the paused clock, so the whole file costs milliseconds of wall time
//! however much *simulated* time its bounds span: a stall costs a bounded
//! amount of simulated time and a control-flow assertion, not a wall-clock
//! sleep.

use std::{io, time::Duration};

use mux::{
    Initiation, MuxConfig, MuxError, StreamAccepter, StreamOpener, StreamReader, StreamWriter,
    live_probe, spawn_mux_no_reconnection,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinSet,
};

/// Production heartbeat on the deployed path. Every arm's session uses it so
/// an arm is not accidentally measuring a session whose receive deadline has
/// been retuned.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// The arm-level bound, on the *simulated* clock. A wedged request is the
/// failure these arms exist to catch, so every wait is bounded and the bound
/// is generous against the work the arm does: an arm that hits it names the
/// resource and reports a wait as the observed outcome.
const BOUND: Duration = Duration::from_secs(600);

/// `stream::reader::STREAM_READ_SOFT_DATA_LIMIT` (`1024 - 1`) and
/// `STREAM_READ_HARD_DATA_LIMIT` (`8 * 1024 - 1`) in `src/stream/reader.rs`.
/// The soft limit is the watermark whose 10 ms grace this arm crosses; the
/// hard limit is the queue's physical capacity minus the reserved terminal
/// slot.
const STREAM_READ_SOFT_DATA_LIMIT: usize = 1024 - 1;
const STREAM_READ_HARD_DATA_LIMIT: usize = 8 * 1024 - 1;

/// `stream::accepter::CHANNEL_SIZE` in `src/stream/accepter.rs`.
const ACCEPT_CHANNEL_SIZE: usize = 1024;

// ─── the session pair ──────────────────────────────────────────────────────

struct Pair {
    opener: StreamOpener,
    accepter: StreamAccepter,
    teardowns: JoinSet<MuxError>,
    _server_teardowns: JoinSet<MuxError>,
}

impl Pair {
    /// `frame_reassembly` selects the wire mode. Both are real: off is the
    /// stock default (`MuxConfig::new`) and on is what the deployed client
    /// runs. The two modes reach the receiving stream's read queue through
    /// different functions (`dispatch_data` and `ingest_reassembly`), so an arm
    /// that claims to measure that capacity measures it in each.
    fn spawn(frame_reassembly: bool) -> Self {
        let (a, b) = tokio::io::duplex(1 << 20);
        let (client_read, client_write) = tokio::io::split(a);
        let (server_read, server_write) = tokio::io::split(b);
        let mut client_config = MuxConfig::new(Initiation::Client, HEARTBEAT_INTERVAL);
        client_config.frame_reassembly = frame_reassembly;
        let mut teardowns = JoinSet::new();
        let (opener, _) =
            spawn_mux_no_reconnection(client_read, client_write, client_config, &mut teardowns);
        let mut server_config = MuxConfig::new(Initiation::Server, HEARTBEAT_INTERVAL);
        server_config.frame_reassembly = frame_reassembly;
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

    /// Open one stream on both ends together. `join!` keeps the open and the
    /// accept advancing, so a peer that never accepts is reported by the
    /// enclosing bound rather than deadlocking one side.
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

/// Drive the runtime to quiescence so every close issued has been applied and
/// every egress token reaped before the census is read. The soak uses the same
/// recipe; a census taken mid-close would read a structure that is released a
/// poll later.
async fn settle() {
    for _ in 0..1024 {
        tokio::task::yield_now().await;
    }
}

fn structures() -> live_probe::StructureCensuses {
    live_probe::structure_censuses()
}

fn egress() -> live_probe::EgressTokenCensuses {
    live_probe::egress_token_censuses()
}

// ─── arm 1: the receiving stream's read queue ──────────────────────────────

/// A peer that accepts a stream and stops reading. The receiving dispatcher
/// holds a bounded queue (`STREAM_READ_SOFT_DATA_LIMIT` entries before its
/// grace, `STREAM_READ_HARD_DATA_LIMIT` physical) and severs the stream when
/// the reader neither drains nor progresses. The writer must learn this from
/// an *error*, not from a write that never returns.
///
/// In mode-off — the stock default — the same arm measures the second half of
/// the token-table defect: a severed stream's entry is retired immediately by
/// `dispatch_data`, but its egress token lives in the application's
/// `StreamWriter` until the application drops it. So live tokens are not
/// bounded by stream-table entries, which is why the token table's full
/// condition must have an answer rather than relying on the two bounds being
/// equal. Mode-on's severance takes `reassembly_error_teardown`, which closes
/// the read side and the peer's write side but deliberately leaves the entry
/// for the later close — the property `reassembly_gap_family` owns — so the
/// structural assertion is mode-off's and the capacity assertion is both's.
async fn arm_reader_stops_draining(pair: &mut Pair, frame_reassembly: bool) {
    let full_before = live_probe::totals().pipeline.read_queue_full;
    let (client_reader, mut client_writer, server_reader, server_writer) = pair
        .open_pair()
        .await
        .expect("an arm cannot measure a capacity it could not reach");

    // One byte per write, so the number of queued read-path messages is the
    // number of writes and the watermark is crossed by message count rather
    // than by byte size. The loop is bounded by a *count* and not only by the
    // clock: a writer that is never answered keeps making progress, so a
    // busy loop never lets the paused clock advance and a clock-only bound
    // would hang instead of failing. Twice the physical queue capacity is
    // enough for the refusal to arrive; running past it is itself the failure.
    let mut written = 0usize;
    const WRITE_BUDGET: usize = 2 * STREAM_READ_HARD_DATA_LIMIT;
    let outcome: Result<(), io::Error> = tokio::time::timeout(BOUND, async {
        loop {
            if written >= WRITE_BUDGET {
                break Err(io::Error::other(format!(
                    "the writer wrote {written} bytes without an answer: the receiving read \
                     queue's capacity was reached and neither an error nor progress followed"
                )));
            }
            // The trait method, not `StreamWriter::write`: the trait maps the
            // writer's own error to an `io::Error` kind, which is what a
            // caller of the mux sees.
            match AsyncWriteExt::write(&mut client_writer, b"x").await {
                Ok(0) => break Ok(()),
                Ok(n) => written += n,
                Err(e) => break Err(e),
            }
        }
    })
    .await
    .expect(
        "the write never returned: a reader that stopped draining its bounded queue left its \
         writer waiting on a queue nothing would ever consume",
    );
    let full_after = live_probe::totals().pipeline.read_queue_full;

    println!(
        "arm=read-queue mode={} written={written} outcome={:?} read_queue_full={}->{} \
         hard_limit={STREAM_READ_HARD_DATA_LIMIT}",
        if frame_reassembly {
            "reassembly"
        } else {
            "stock"
        },
        outcome.as_ref().map(|()| "Ok(0)"),
        full_before,
        full_after,
    );

    // Sanity: the capacity was reached. Without this the error below could be
    // attributed to anything.
    assert!(
        full_after - full_before >= 1,
        "the read-queue refusal counter never fired, so no bounded queue was ever \
         actually full and the outcome below proves nothing"
    );
    assert!(
        written >= STREAM_READ_SOFT_DATA_LIMIT,
        "only {written} byte(s) were written, below the {STREAM_READ_SOFT_DATA_LIMIT} \
         soft watermark the refusal keys on: the queue was not filled, so the outcome is \
         not the capacity's"
    );
    let error = outcome.expect_err(
        "the write reported success while the peer's read queue was full and its reader had \
         stopped draining: a full bounded queue must not read as progress",
    );
    assert_eq!(
        error.kind(),
        io::ErrorKind::BrokenPipe,
        "a severed stream must reach its writer as a broken pipe, got {error:?}"
    );
    // The structural half, in mode-off only (see this function's doc): the
    // entry is gone, the token is still held by the accepted writer the
    // application has not dropped. Asserting both in one arm is what makes the
    // composition visible — the token table's occupancy is *live writers*, and
    // the stream table's is *live entries*.
    settle().await;
    let held_structures = structures();
    let held_egress = egress();
    println!(
        "arm=read-queue mode={} after-severance: server[{}] client[{}] egress.server[{}]",
        if frame_reassembly {
            "reassembly"
        } else {
            "stock"
        },
        held_structures.server,
        held_structures.client,
        held_egress.server,
    );
    if !frame_reassembly {
        assert_eq!(
            held_structures.server.stream_table_len, 0,
            "the severed stream's table entry was retained: {}",
            held_structures.server
        );
        assert!(
            held_egress.server.token_queues >= 1,
            "the severed stream's egress token was reaped with its table entry, so live tokens \
             would be bounded by table entries and this arm would not exercise the reason the \
             token table needs its own answer ({})",
            held_egress.server
        );
    }

    // Release the held halves: the token must be reaped, or the fix would have
    // traded a wait for a permanent loss of egress capacity.
    drop((client_reader, server_reader, server_writer));
    drop(client_writer);
    settle().await;
    let released = egress();
    println!(
        "arm=read-queue after-release: egress.server[{}]",
        released.server
    );
    assert_eq!(
        released.server.token_queues, 0,
        "the egress token table did not drain after the severed stream's writer was dropped \
         ({})",
        released.server
    );
}

// ─── arm 2: the application's accept channel ───────────────────────────────

/// A peer that opens streams while the application stops accepting. The
/// receiving side materialises each peer stream and hands it to the
/// application through a bounded accept channel; past that channel's bound the
/// stream is **dropped** — the peer's `open` already returned, so no error can
/// reach the caller that opened it. This arm measures that outcome and asserts
/// the two properties that make the drop survivable rather than a wedge: the
/// session survives it, and the drop is *counted*.
async fn arm_application_stops_accepting(pair: &mut Pair) {
    let pipeline_before = live_probe::totals().pipeline;
    let ledger_before = live_probe::admission_ledgers();

    // The application does not accept while `ACCEPT_CHANNEL_SIZE + 1` peer
    // streams are opened. Holding the client halves keeps every stream open so
    // the peer side cannot release them either.
    let mut held = Vec::new();
    for index in 0..=ACCEPT_CHANNEL_SIZE {
        let opened = tokio::time::timeout(BOUND, pair.opener.open())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "open #{index} did not complete while the application was not accepting: the \
                 client's own open path must not depend on the peer's accept channel"
                )
            });
        let halves = opened.unwrap_or_else(|e| {
            panic!("open #{index} was refused while the stream table admits it: {e:?}")
        });
        held.push(halves);
    }
    settle().await;

    let pipeline_after = live_probe::totals().pipeline;
    let ledger_after = live_probe::admission_ledgers();
    let drops = pipeline_after.accept_channel_full - pipeline_before.accept_channel_full;
    let materialised = (ledger_after.server.inserted - ledger_before.server.inserted)
        - (ledger_after.server.retired - ledger_before.server.retired);
    println!(
        "arm=accept-channel opened={} accept_channel_full={drops} server_materialised_now={} \
         server_refused_table={}",
        held.len(),
        materialised,
        ledger_after.server.refused_peer - ledger_before.server.refused_peer,
    );

    assert!(
        drops >= 1,
        "the accept channel never reported full, so the arm never reached the capacity whose \
         outcome it claims to measure"
    );
    assert_eq!(
        materialised as usize, ACCEPT_CHANNEL_SIZE,
        "the peer materialised a different number of live streams than the accept channel \
         holds: {} != {ACCEPT_CHANNEL_SIZE}",
        materialised,
    );
    assert_eq!(
        ledger_after.server.refused_peer - ledger_before.server.refused_peer,
        0,
        "the stream table refused an admission, so the drop above is the table's bound and \
         not the accept channel's"
    );

    // The session survives the drop: drain the channel, then complete a full
    // request/response round that must work.
    for _ in 0..ACCEPT_CHANNEL_SIZE {
        tokio::time::timeout(BOUND, pair.accepter.accept())
            .await
            .expect("the accept channel was reported full but offered fewer than its capacity")
            .expect("the accept channel closed");
    }
    let (mut client_reader, mut client_writer, mut server_reader, mut server_writer) =
        tokio::time::timeout(BOUND, pair.open_pair())
            .await
            .expect("the session did not return to service after the accept-channel drop")
            .expect("the recovery probe could not open a stream");
    client_writer.write_all(b"recovery").await.unwrap();
    AsyncWriteExt::shutdown(&mut client_writer).await.unwrap();
    let mut received = Vec::new();
    server_reader.read_to_end(&mut received).await.unwrap();
    assert_eq!(received, b"recovery");
    server_writer.write_all(b"recovery").await.unwrap();
    AsyncWriteExt::shutdown(&mut server_writer).await.unwrap();
    let mut echoed = Vec::new();
    client_reader.read_to_end(&mut echoed).await.unwrap();
    assert_eq!(echoed, b"recovery");
    assert!(
        pair.tear_down_reason().is_none(),
        "the session tore down over a dropped peer stream"
    );

    drop(held);
    settle().await;
}

// ─── the battery ───────────────────────────────────────────────────────────

/// Every bounded resource on the request path that this tier can drive to its
/// bound, with the outcome measured for each. See `GATE.md`
/// ("Request-path capacity table") for the full inventory, including the
/// capacities this arm does not reach and the reason.
#[tokio::test(start_paused = true)]
async fn bounded_request_path_capacities_answer_instead_of_waiting() {
    live_probe::enable_structure_census();

    let mut pair = Pair::spawn(false);
    arm_reader_stops_draining(&mut pair, false).await;
    assert!(
        pair.tear_down_reason().is_none(),
        "the session tore down during the stock read-queue arm"
    );

    let mut pair = Pair::spawn(true);
    arm_reader_stops_draining(&mut pair, true).await;
    assert!(
        pair.tear_down_reason().is_none(),
        "the session tore down during the reassembly read-queue arm"
    );

    let mut pair = Pair::spawn(false);
    arm_application_stops_accepting(&mut pair).await;

    // Instrument sanity: the arms above must have moved the counters they
    // read, or the readings were of a session that never ran.
    let totals = live_probe::totals();
    assert!(
        totals.pipeline.read_queue_full >= 1,
        "no read-queue refusal was ever counted across the whole battery"
    );
    assert!(
        totals.pipeline.accept_channel_full >= 1,
        "no accept-channel refusal was ever counted across the whole battery"
    );
    println!("battery complete: {totals}");
}
