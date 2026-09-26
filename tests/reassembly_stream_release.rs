//! Release of a finished stream from the receiver's stream table, end to end.
//!
//! Under `frame_reassembly` a session materialises one table entry per stream
//! the peer opens, and it closes the peer's write half from the reassembly
//! cursor (the final offset the peer's `CloseWrite` carries) rather than from
//! `MuxControl::peer_close`. Whether a finished entry is actually released is
//! therefore only observable through a real session and the admission ledger:
//! on the wire the session looks idle, every staged byte arrives, and the
//! table quietly keeps the entry until admission refuses every later stream.
//!
//! Each iteration drives the order the release defect needs — the receiver
//! drops both halves of the stream first, then the sender drops its read half
//! (its `CloseRead`) and closes its write half last — and waits for both
//! sessions' tables to drain back to their baseline. That wait is the
//! assertion, so a retained entry is reported as the iteration it was retained
//! in, with the ledger that names it. The `frame_reassembly = false` phase is
//! the control: the same order over the stock wire drains at every iteration,
//! which is what makes the mode-on retention a release defect rather than a
//! property of the shapes.
//!
//! Both phases run in one test because the ledger is published per session
//! role in the process, and two tests in one binary would share those slots.

use std::time::Duration;

use mux::{
    Initiation, MuxConfig, StreamAccepter, StreamOpener, live_probe, spawn_mux_no_reconnection,
};
use tokio::{
    io::duplex,
    task::JoinSet,
    time::{Instant, sleep},
};

/// Buffered bytes per direction of the in-memory transport pair.
const TRANSPORT_BUF: usize = 64 * 1024;

/// Streams driven per phase: enough that a per-stream retention cannot be
/// mistaken for a one-off, small enough that the phase stays sub-second.
const ITERATIONS: u64 = 24;

/// How long the sender's `CloseRead` is given to be published before its write
/// half is closed. The wire is in-order, so publishing the close read first is
/// what makes the receiver apply it first and leave the peer's write close as
/// the stream's last frame — the state the release check exists for.
const CLOSE_ORDER_GAP: Duration = Duration::from_millis(5);

/// How long both tables are given to drain after an iteration. Generous
/// against a loaded host, small enough that a retained entry is reported
/// instead of hidden by the test's own bound.
const DRAIN_BOUND: Duration = Duration::from_secs(5);

fn spawn_pair(
    session: &mut JoinSet<mux::MuxError>,
    frame_reassembly: bool,
) -> (StreamOpener, StreamAccepter) {
    let (client_read, server_write) = duplex(TRANSPORT_BUF);
    let (server_read, client_write) = duplex(TRANSPORT_BUF);
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
    (opener, accepter)
}

/// One stream in the order that leaves the peer's write close as its last
/// frame: the receiver closes both halves, then the sender closes its read
/// half, waits, and closes its write half.
async fn drive_one(opener: &mut StreamOpener, accepter: &mut StreamAccepter) {
    let (client_reader, client_writer) = opener.open().await.unwrap();
    let (server_reader, server_writer) = accepter.accept().await.unwrap();
    drop(server_reader);
    drop(server_writer);
    drop(client_reader);
    sleep(CLOSE_ORDER_GAP).await;
    drop(client_writer);
}

/// Wait for both sessions' tables to return to the pre-phase baseline, or
/// report the iteration whose stream was retained.
async fn await_tables_drained(baseline: live_probe::AdmissionLedgers, iteration: u64) {
    let deadline = Instant::now() + DRAIN_BOUND;
    loop {
        let now = live_probe::admission_ledgers();
        if now.server.stream_table_len <= baseline.server.stream_table_len
            && now.client.stream_table_len <= baseline.client.stream_table_len
        {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "phase iteration {iteration}: a finished stream was retained in the \
                 receiver's stream table; ledger now: {}; baseline: {}",
                now, baseline
            );
        }
        sleep(Duration::from_millis(1)).await;
    }
}

/// Drive `ITERATIONS` streams and assert every one of them left both tables,
/// with the insert/retire totals as the cross-check that the streams really
/// were materialised and really were released.
async fn run_phase(frame_reassembly: bool) {
    let mut session = JoinSet::new();
    let (mut opener, mut accepter) = spawn_pair(&mut session, frame_reassembly);
    let baseline = live_probe::admission_ledgers();
    for iteration in 0..ITERATIONS {
        drive_one(&mut opener, &mut accepter).await;
        await_tables_drained(baseline, iteration).await;
    }
    let end = live_probe::admission_ledgers();
    for (role, before, after) in [
        ("server", baseline.server, end.server),
        ("client", baseline.client, end.client),
    ] {
        assert_eq!(
            after.inserted - before.inserted,
            ITERATIONS,
            "phase frame_reassembly={frame_reassembly}: {role} did not materialise \
             one stream per iteration"
        );
        assert_eq!(
            after.retired - before.retired,
            ITERATIONS,
            "phase frame_reassembly={frame_reassembly}: {role} retained a finished \
             stream (inserted {} vs retired {} over the phase)",
            after.inserted - before.inserted,
            after.retired - before.retired,
        );
        assert_eq!(
            after.stream_table_len, before.stream_table_len,
            "phase frame_reassembly={frame_reassembly}: {role} table did not drain"
        );
        // One stream is driven at a time, so a table that drains is never more
        // than one entry above the baseline. This catches an accumulation that
        // happens to drain at the end of a phase but grows while it runs.
        assert!(
            after.max_stream_table_len <= before.max_stream_table_len + 1,
            "phase frame_reassembly={frame_reassembly}: {role} table grew to {} entries \
             while one stream at a time was driven",
            after.max_stream_table_len
        );
    }
    session.abort_all();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn finished_streams_leave_the_peer_table_in_both_wire_modes() {
    // The reassembly path is the one under test; the stock wire is the control
    // that shows the close order itself retains nothing.
    run_phase(true).await;
    run_phase(false).await;
}
