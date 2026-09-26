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
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use mux::{
    Initiation, MuxConfig, StreamAccepter, StreamOpener, StreamReader, StreamWriter,
    spawn_mux_no_reconnection,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, duplex},
    task::JoinSet,
    time::{Instant, timeout},
};

/// Same bound as the soak: single-digit ms on loopback, so a live but slow
/// host does not read as a stall while a lost wake is still caught.
const CYCLE_BOUND: Duration = Duration::from_millis(2_000);

const DEFAULT_CYCLES: u64 = 400;

/// Buffered bytes per direction of the in-memory transport pair.
const DUPLEX_BUF: usize = 64 * 1024;

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
