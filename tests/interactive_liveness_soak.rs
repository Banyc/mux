//! Interactive-path liveness soak.
//!
//! The product's standing priority is M1, interactive tail latency, and the
//! field symptom is multi-second stalls on an otherwise healthy session. The
//! audit programme here closes *decision* defects (threshold inclusivity,
//! equivalence, boundary exactness); it cannot reach *liveness* defects — a
//! lost wakeup, a staged reply cancelled by a teardown guard, a cursor
//! blocked on something a refusal path silently discarded. Those are the
//! shapes that become a multi-second interactive stall, and this soak exists
//! to exercise them.
//!
//! It drives the real egress path of many concurrent streams over a real mux
//! session (the `fair_queue` sender/receiver and `central_io`'s write
//! scheduler, the components that must wake each other) through the shapes
//! that exercise their wake discipline:
//!
//! * interleaved interactive request/response traffic beside bulk transfers,
//! * a writer parked on the fair-queue reserve path while its queue is full,
//! * a writer or reader dropped mid-flight (the `fair_queue` clone-drop ready
//!   mark, and the teardown guard),
//! * `Fin` racing the last bytes of pending data,
//! * streams opened and closed across thousands of cycles so the scan,
//!   rotation and close paths are all hot.
//!
//! The properties asserted are the ones a stall or a loss violates:
//!
//! 1. every byte the sender staged is received, and in order (per-stream
//!    deterministic payload check — a silent drop, a duplicate, a reorder or a
//!    truncation all read as a mismatch);
//! 2. every stream completes — no stream starved behind another;
//! 3. every cycle completes inside `CYCLE_BOUND`, with a **hang** (a cycle
//!    that never completes) reported distinctly from a **late** cycle or a
//!    **partially stalled** one.
//!
//! This is a report-and-assert instrument, not a perf measurement: it is
//! `#[ignore]`d (tier `standard` in `GATE.md`) and its cycle count can be
//! raised with `MUX_SOAK_CYCLES` for a longer soak. A green run is bounded by
//! the number of cycles it ran; see the detection-limit note in `GATE.md`.

use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use mux::{
    Initiation, MuxConfig, MuxError, StreamAccepter, StreamOpener, StreamReader, StreamWriter,
    spawn_mux_no_reconnection,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, duplex},
    task::JoinSet,
    time::{Instant, timeout},
};

/// Buffered bytes in each direction of the in-memory transport pair. Large
/// enough that the central I/O writer is not the bottleneck for the ordinary
/// shapes; the backpressure shape deliberately floods past it.
const DUPLEX_BUF: usize = 64 * 1024;

/// Default number of soak cycles. Each cycle is a fresh batch of streams, so
/// cycles are the trial unit for the detection bound.
const DEFAULT_CYCLES: u64 = 1_500;

/// A cycle is expected to finish in single-digit milliseconds on loopback; the
/// bound is set far above that so a slow-but-live machine does not read as a
/// stall while a lost wakeup is still caught.
const CYCLE_BOUND: Duration = Duration::from_millis(2_000);

/// A cycle that has not completed within this long *and* has made no observable
/// progress is a hang, not a late cycle.
const HANG_BOUND: Duration = Duration::from_secs(10);

// ─── payload ───────────────────────────────────────────────────────────────

/// Deterministic per-(seed, offset) payload byte. Every receiver that verifies
/// by this function detects a lost byte, a duplicate, a reorder or a
/// truncation as a mismatch.
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

// ─── accounting ────────────────────────────────────────────────────────────

/// Runtime-liveness heartbeat. A task wakes every 10 ms and timestamps the
/// tick; the age at a stall verdict and the worst gap over the run separate a
/// genuine park (the runtime kept scheduling while the cycle made no
/// progress) from host starvation (the runtime itself stopped running).
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
    fn age_ms(&self) -> u64 {
        self.elapsed_ms()
            .saturating_sub(self.last_tick_ms.load(Ordering::Relaxed))
    }
    fn report(&self) -> String {
        format!(
            "heartbeat_age={}ms max_gap={}ms ticks={}",
            self.age_ms(),
            self.max_gap_ms.load(Ordering::Relaxed),
            self.ticks.load(Ordering::Relaxed)
        )
    }
}

async fn heartbeat_task(heartbeat: Arc<Heartbeat>) {
    loop {
        tokio::time::sleep(Duration::from_millis(10)).await;
        heartbeat.tick();
    }
}

/// Cross-cycle progress and first-failure record. The progress counter is what
/// separates "the cycle never completed" (a hang) from "the cycle completed
/// late" or "it moved but a stream is stuck".
#[derive(Debug, Default)]
struct Progress {
    staged_bytes: AtomicU64,
    received_bytes: AtomicU64,
    completed_jobs: AtomicU64,
    opened_streams: AtomicU64,
}

/// Per-job byte meter: feeds the global counters and keeps the job's own
/// staged/received totals so a stall names which side is short of bytes.
#[derive(Debug)]
struct JobMeter {
    progress: Arc<Progress>,
    staged: AtomicU64,
    received: AtomicU64,
}

impl JobMeter {
    fn new(progress: Arc<Progress>) -> Self {
        Self {
            progress,
            staged: AtomicU64::new(0),
            received: AtomicU64::new(0),
        }
    }
    fn staged(&self, bytes: u64) {
        self.staged.fetch_add(bytes, Ordering::Relaxed);
        self.progress.staged(bytes);
    }
    fn received(&self, bytes: u64) {
        self.received.fetch_add(bytes, Ordering::Relaxed);
        self.progress.received(bytes);
    }
}

/// In-flight job labels. A stall reports exactly which jobs never finished, so
/// the stalled side (writer parked, reader waiting, echo not returned) is
/// evidence rather than a guess.
#[derive(Debug, Default)]
struct Inflight {
    map: Mutex<std::collections::HashMap<u64, (String, Arc<JobMeter>)>>,
    next: AtomicU64,
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

impl Progress {
    fn staged(&self, bytes: u64) {
        self.staged_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    fn received(&self, bytes: u64) {
        self.received_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    fn completed(&self, jobs: u64) {
        self.completed_jobs.fetch_add(jobs, Ordering::Relaxed);
    }
    fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.staged_bytes.load(Ordering::Relaxed),
            self.received_bytes.load(Ordering::Relaxed),
            self.completed_jobs.load(Ordering::Relaxed),
        )
    }
}

/// First terminal `MuxError` (or a task panic) observed on either session
/// half. A session that dies mid-soak is a loss even if the cycle bookkeeping
/// would eventually time out.
#[derive(Debug, Default)]
struct SessionProbe {
    first: Mutex<Option<String>>,
}

impl SessionProbe {
    fn record(&self, what: String) {
        let mut first = self.first.lock().unwrap();
        if first.is_none() {
            *first = Some(what);
        }
    }
    fn record_error(&self, error: &MuxError) {
        self.record(format!("session error: {error:?}"));
    }
    fn take(&self) -> Option<String> {
        self.first.lock().unwrap().clone()
    }
}

async fn supervise(mut session: JoinSet<MuxError>, probe: Arc<SessionProbe>) {
    while let Some(result) = session.join_next().await {
        match result {
            Ok(error) => probe.record_error(&error),
            Err(join_error) => probe.record(format!("session task panicked/aborted: {join_error}")),
        }
    }
}

// ─── stream helpers ────────────────────────────────────────────────────────

/// Read exactly `buf.len()` bytes, or report a clean EOF only when it lands on
/// the message boundary. A partial message is an error, never a silent
/// truncation.
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

// ─── job shapes ────────────────────────────────────────────────────────────

/// Interactive request/response: the client stages a small message and reads
/// the echo before staging the next, so every round trip crosses the fair
/// queue's wake discipline in both directions and the reader must be woken for
/// each reply.
async fn client_ping_pong(
    mut writer: StreamWriter,
    mut reader: StreamReader,
    progress: Arc<JobMeter>,
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
            .map_err(|e| format!("ping write: {e:?}"))?;
        progress.staged(len as u64);
        read_exact_or_eof(&mut reader, &mut echo)
            .await
            .map_err(|e| format!("ping read: {e:?}"))?;
        if echo != request {
            return Err(format!(
                "interactive echo mismatch on round {round}: sent {:?} got {:?}",
                &request[..request.len().min(8)],
                &echo[..echo.len().min(8)]
            ));
        }
        progress.received(len as u64);
    }
    writer
        .shutdown()
        .map_err(|e| format!("ping shutdown: {e:?}"))?;
    let mut trailing = [0u8; 1];
    let n = reader
        .read(&mut trailing)
        .await
        .map_err(|e| format!("ping eof read: {e:?}"))?;
    if n != 0 {
        return Err("peer sent data after our FIN".to_owned());
    }
    Ok(())
}

async fn server_echo(
    mut writer: StreamWriter,
    mut reader: StreamReader,
    progress: Arc<JobMeter>,
    len: usize,
) -> Result<(), String> {
    let mut message = vec![0u8; len];
    while read_exact_or_eof(&mut reader, &mut message)
        .await
        .map_err(|e| format!("echo read: {e:?}"))?
    {
        progress.received(len as u64);
        writer
            .write_all(&message)
            .await
            .map_err(|e| format!("echo write: {e:?}"))?;
        progress.staged(len as u64);
    }
    Ok(())
}

/// Bulk: stage `total` bytes in `chunk`-sized writes while the peer drains and
/// verifies every byte against the pattern.
async fn client_bulk(
    mut writer: StreamWriter,
    progress: Arc<JobMeter>,
    seed: u64,
    total: usize,
    chunk: usize,
) -> Result<(), String> {
    let mut buf = vec![0u8; chunk.min(total).max(1)];
    let mut offset = 0;
    while offset < total {
        let take = (total - offset).min(buf.len());
        fill_pattern(seed, offset, &mut buf[..take]);
        writer
            .write_all(&buf[..take])
            .await
            .map_err(|e| format!("bulk write at {offset}: {e:?}"))?;
        progress.staged(take as u64);
        offset += take;
    }
    writer
        .shutdown()
        .map_err(|e| format!("bulk shutdown: {e:?}"))?;
    Ok(())
}

async fn server_verify(
    mut reader: StreamReader,
    progress: Arc<JobMeter>,
    seed: u64,
    total: usize,
) -> Result<(), String> {
    let mut buf = vec![0u8; 16 * 1024];
    let mut offset = 0usize;
    loop {
        let n = reader
            .read(&mut buf)
            .await
            .map_err(|e| format!("verify read at {offset}: {e:?}"))?;
        if n == 0 {
            break;
        }
        for (i, byte) in buf[..n].iter().enumerate() {
            let expected = pattern_byte(seed, offset + i);
            if *byte != expected {
                return Err(format!(
                    "payload mismatch at offset {}: expected {expected:#04x}, got {:#04x}",
                    offset + i,
                    *byte
                ));
            }
        }
        offset += n;
        progress.received(n as u64);
    }
    if offset != total {
        return Err(format!(
            "bulk truncated: received {offset} of {total} bytes"
        ));
    }
    Ok(())
}

/// `Fin` racing pending data: stage the payload and shut down immediately, then
/// wait for the peer's clean EOF. This is the shape where a staged tail can be
/// cancelled by the teardown guard.
async fn client_fin_race(
    mut writer: StreamWriter,
    mut reader: StreamReader,
    progress: Arc<JobMeter>,
    seed: u64,
    payload: usize,
) -> Result<(), String> {
    let mut buf = vec![0u8; payload];
    fill_pattern(seed, 0, &mut buf);
    writer
        .write_all(&buf)
        .await
        .map_err(|e| format!("fin-race write: {e:?}"))?;
    progress.staged(payload as u64);
    writer
        .shutdown()
        .map_err(|e| format!("fin-race shutdown: {e:?}"))?;
    let mut trailing = [0u8; 1];
    let n = reader
        .read(&mut trailing)
        .await
        .map_err(|e| format!("fin-race eof: {e:?}"))?;
    if n != 0 {
        return Err("fin-race: peer data after FIN".to_owned());
    }
    Ok(())
}

/// Reader dropped mid-flight: the peer's write half must be told, and the
/// peer's next write must fail promptly rather than park forever.
async fn client_drop_reader(mut writer: StreamWriter, reader: StreamReader) -> Result<(), String> {
    drop(reader);
    let message = [0xA5u8; 256];
    let mut writes = 0u64;
    while writer.write_all(&message).await.is_ok() {
        writes += 1;
        if writes > 200_000 {
            return Err("drop-reader: local writer never observed the peer close".to_owned());
        }
    }
    Ok(())
}

async fn server_dropped_reader(
    mut writer: StreamWriter,
    mut reader: StreamReader,
) -> Result<(), String> {
    let message = [0x5Au8; 256];
    let mut writes = 0u64;
    while writer.write_all(&message).await.is_ok() {
        writes += 1;
        if writes > 200_000 {
            return Err("drop-reader server: reader close never reached the write half".to_owned());
        }
        // Drain whatever the peer managed to stage before its reader dropped,
        // so its writer is not parked on backpressure the whole time.
        let mut scratch = [0u8; 1024];
        let _ = reader.read(&mut scratch).await;
    }
    Ok(())
}

/// Backpressure: the server withholds reads while the client floods, so the
/// session's egress fair queue fills and the client's writer parks on the
/// reserve path. The queued bytes must all arrive once the server reads.
async fn client_flood(
    mut writer: StreamWriter,
    progress: Arc<JobMeter>,
    seed: u64,
    total: usize,
) -> Result<(), String> {
    let mut buf = vec![0u8; 8 * 1024];
    let mut offset = 0;
    while offset < total {
        let take = (total - offset).min(buf.len());
        fill_pattern(seed, offset, &mut buf[..take]);
        writer
            .write_all(&buf[..take])
            .await
            .map_err(|e| format!("flood write at {offset}: {e:?}"))?;
        progress.staged(take as u64);
        offset += take;
    }
    writer
        .shutdown()
        .map_err(|e| format!("flood shutdown: {e:?}"))?;
    Ok(())
}

async fn server_delayed_read(
    reader: StreamReader,
    progress: Arc<JobMeter>,
    seed: u64,
    total: usize,
) -> Result<(), String> {
    tokio::time::sleep(Duration::from_millis(30)).await;
    server_verify(reader, progress, seed, total).await
}

// ─── cycle driver ──────────────────────────────────────────────────────────

/// One cycle's schedule decisions, produced by [`Soak::cycle_shape`].
#[derive(Debug)]
struct Shape {
    interactive: usize,
    rounds: Vec<usize>,
    lens: Vec<usize>,
    ping_seeds: Vec<u64>,
    bulk: usize,
    bulk_seeds: Vec<u64>,
    fin_race: Option<(usize, u64)>,
    drop_reader: bool,
    flood: Option<u64>,
}

struct Soak {
    opener: StreamOpener,
    accepter: StreamAccepter,
    progress: Arc<Progress>,
    inflight: Arc<Inflight>,
    rng: u64,
    cycle_bound: Duration,
    hang_bound: Duration,
    next_seed: u64,
}

#[derive(Debug)]
enum CycleVerdict {
    Pass {
        elapsed: Duration,
    },
    Failed {
        message: String,
        elapsed: Duration,
    },
    Late {
        message: Option<String>,
        elapsed: Duration,
    },
    Partial {
        progressed: u64,
        message: Option<String>,
        elapsed: Duration,
    },
    Hang,
}

impl Soak {
    fn seed(&mut self) -> u64 {
        self.next_seed = self.next_seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        self.next_seed
    }

    /// The per-cycle shape decisions, consuming exactly the RNG calls the
    /// cycle itself makes and in the same order. Factoring them out lets the
    /// driver replay the schedule to a target cycle (diagnostic reproduction)
    /// without executing the earlier cycles' I/O.
    fn cycle_shape(&mut self, cycle: u64) -> Shape {
        let interactive = 3 + (splitmix64(&mut self.rng) % 4) as usize;
        let bulk = if cycle.is_multiple_of(5) { 2 } else { 1 };
        let mut rounds = Vec::with_capacity(interactive);
        let mut lens = Vec::with_capacity(interactive);
        let mut ping_seeds = Vec::with_capacity(interactive);
        for _ in 0..interactive {
            rounds.push(3 + (splitmix64(&mut self.rng) % 4) as usize);
            lens.push(if splitmix64(&mut self.rng).is_multiple_of(2) {
                16
            } else {
                64
            });
            ping_seeds.push(self.seed());
        }
        let mut bulk_seeds = Vec::with_capacity(bulk);
        for _ in 0..bulk {
            bulk_seeds.push(self.seed());
        }
        let fin_race = cycle.is_multiple_of(7).then(|| {
            let payload = 4 * 1024 + (splitmix64(&mut self.rng) % 5) as usize;
            (payload, self.seed())
        });
        let drop_reader = cycle.is_multiple_of(11);
        let flood = cycle.is_multiple_of(13).then(|| self.seed());
        Shape {
            interactive,
            rounds,
            lens,
            ping_seeds,
            bulk,
            bulk_seeds,
            fin_race,
            drop_reader,
            flood,
        }
    }

    /// Advance the schedule past a cycle without running it. Used only by the
    /// diagnostic replay mode, so the target cycle sees the same RNG state it
    /// would have seen in a full run.
    fn skip_cycle(&mut self, cycle: u64) {
        let _ = self.cycle_shape(cycle);
    }

    async fn cycle(&mut self, cycle: u64) -> Result<(), String> {
        let mut jobs: JoinSet<Result<(), String>> = JoinSet::new();
        let progress = Arc::clone(&self.progress);
        let inflight = Arc::clone(&self.inflight);
        let shape = self.cycle_shape(cycle);

        // Spawn with an in-flight label so a stall names the jobs that never
        // finished (writer parked vs reader waiting) instead of leaving it a
        // guess.
        macro_rules! spawn_job {
            ($label:expr, $meter:expr, $fut:expr) => {{
                let label = $label;
                let meter = $meter;
                let id = inflight.enter(label, Arc::clone(&meter));
                let tracker = Arc::clone(&inflight);
                jobs.spawn(async move {
                    let result = $fut.await;
                    tracker.leave(id);
                    result
                });
            }};
        }

        let interactive = shape.interactive;
        let bulk = shape.bulk;

        for i in 0..interactive {
            let (client_reader, client_writer) = self
                .opener
                .open()
                .await
                .map_err(|e| format!("open interactive: {e:?}"))?;
            let (server_reader, server_writer) = self
                .accepter
                .accept()
                .await
                .map_err(|e| format!("accept interactive: {e:?}"))?;
            self.progress.opened_streams.fetch_add(1, Ordering::Relaxed);
            let rounds = shape.rounds[i];
            let len = shape.lens[i];
            let seed = shape.ping_seeds[i];
            let p = Arc::new(JobMeter::new(Arc::clone(&progress)));
            spawn_job!(
                format!("c{cycle} ping-client#{i} rounds={rounds} len={len}"),
                Arc::clone(&p),
                client_ping_pong(client_writer, client_reader, p, seed, rounds, len)
            );
            let p = Arc::new(JobMeter::new(Arc::clone(&progress)));
            spawn_job!(
                format!("c{cycle} ping-server#{i} len={len}"),
                Arc::clone(&p),
                server_echo(server_writer, server_reader, p, len)
            );
        }

        for i in 0..bulk {
            let (client_reader, client_writer) = self
                .opener
                .open()
                .await
                .map_err(|e| format!("open bulk: {e:?}"))?;
            let (server_reader, server_writer) = self
                .accepter
                .accept()
                .await
                .map_err(|e| format!("accept bulk: {e:?}"))?;
            drop(server_writer);
            self.progress.opened_streams.fetch_add(1, Ordering::Relaxed);
            let total = 64 * 1024;
            let chunk = 8 * 1024;
            let seed = shape.bulk_seeds[i];
            let p = Arc::new(JobMeter::new(Arc::clone(&progress)));
            spawn_job!(
                format!("c{cycle} bulk-client#{i} total={total}"),
                Arc::clone(&p),
                client_bulk(client_writer, p, seed, total, chunk)
            );
            let p = Arc::new(JobMeter::new(Arc::clone(&progress)));
            spawn_job!(
                format!("c{cycle} bulk-server#{i} total={total}"),
                Arc::clone(&p),
                server_verify(server_reader, p, seed, total)
            );
            drop(client_reader);
        }

        if let Some((payload, seed)) = shape.fin_race {
            let (client_reader, client_writer) = self
                .opener
                .open()
                .await
                .map_err(|e| format!("open fin-race: {e:?}"))?;
            let (server_reader, server_writer) = self
                .accepter
                .accept()
                .await
                .map_err(|e| format!("accept fin-race: {e:?}"))?;
            self.progress.opened_streams.fetch_add(1, Ordering::Relaxed);
            let p = Arc::new(JobMeter::new(Arc::clone(&progress)));
            spawn_job!(
                format!("c{cycle} fin-race-client payload={payload}"),
                Arc::clone(&p),
                client_fin_race(client_writer, client_reader, p, seed, payload)
            );
            let p = Arc::new(JobMeter::new(Arc::clone(&progress)));
            spawn_job!(
                format!("c{cycle} fin-race-server payload={payload}"),
                Arc::clone(&p),
                server_verify(server_reader, p, seed, payload)
            );
            drop(server_writer);
        }

        if shape.drop_reader {
            let (client_reader, client_writer) = self
                .opener
                .open()
                .await
                .map_err(|e| format!("open drop-reader: {e:?}"))?;
            let (server_reader, server_writer) = self
                .accepter
                .accept()
                .await
                .map_err(|e| format!("accept drop-reader: {e:?}"))?;
            self.progress.opened_streams.fetch_add(1, Ordering::Relaxed);
            let p = Arc::new(JobMeter::new(Arc::clone(&progress)));
            spawn_job!(
                format!("c{cycle} drop-reader-client"),
                Arc::clone(&p),
                client_drop_reader(client_writer, client_reader)
            );
            let p = Arc::new(JobMeter::new(Arc::clone(&progress)));
            spawn_job!(
                format!("c{cycle} drop-reader-server"),
                Arc::clone(&p),
                server_dropped_reader(server_writer, server_reader)
            );
        }

        if let Some(seed) = shape.flood {
            let (client_reader, client_writer) = self
                .opener
                .open()
                .await
                .map_err(|e| format!("open flood: {e:?}"))?;
            let (server_reader, server_writer) = self
                .accepter
                .accept()
                .await
                .map_err(|e| format!("accept flood: {e:?}"))?;
            drop(server_writer);
            self.progress.opened_streams.fetch_add(1, Ordering::Relaxed);
            let total = 512 * 1024;
            let p = Arc::new(JobMeter::new(Arc::clone(&progress)));
            spawn_job!(
                format!("c{cycle} flood-client total={total}"),
                Arc::clone(&p),
                client_flood(client_writer, p, seed, total)
            );
            let p = Arc::new(JobMeter::new(Arc::clone(&progress)));
            spawn_job!(
                format!("c{cycle} flood-server total={total}"),
                Arc::clone(&p),
                server_delayed_read(server_reader, p, seed, total)
            );
            drop(client_reader);
        }

        let expected_jobs = jobs.len() as u64;
        let mut completed = 0u64;
        let mut first_error: Option<String> = None;
        while let Some(result) = jobs.join_next().await {
            completed += 1;
            self.progress.completed(1);
            match result {
                Ok(Ok(())) => {}
                Ok(Err(message)) => {
                    if first_error.is_none() {
                        first_error = Some(message);
                    }
                }
                Err(join_error) => {
                    if first_error.is_none() {
                        first_error = Some(format!("job panicked: {join_error}"));
                    }
                }
            }
        }
        self.progress.completed(expected_jobs - completed);
        if let Some(message) = first_error {
            return Err(message);
        }
        Ok(())
    }

    /// Open one fresh stream on the same session and complete a one-round
    /// ping-pong with a shutdown/EOF handshake. Run after a stall verdict: if
    /// it completes, the session is alive and the stall is stream-local (the
    /// stalled stream's close handshake); if it hangs, the session itself is
    /// wedged.
    async fn probe_after_stall(&mut self) -> Result<(), String> {
        let (mut client_reader, mut client_writer) = self
            .opener
            .open()
            .await
            .map_err(|e| format!("probe open: {e:?}"))?;
        let (mut server_reader, mut server_writer) = self
            .accepter
            .accept()
            .await
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
        match tokio::time::timeout(Duration::from_secs(2), async {
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

    /// Run one cycle under the two-stage deadline: completion inside
    /// `cycle_bound` passes, completion later is `Late`, completion never is
    /// `Hang` when nothing progressed and `Partial` when the cycle moved but a
    /// stream is stuck.
    async fn cycle_verdict(&mut self, cycle: u64) -> CycleVerdict {
        let cycle_bound = self.cycle_bound;
        let hang_window = self.hang_bound.saturating_sub(self.cycle_bound);
        let progress = Arc::clone(&self.progress);
        let before = progress.snapshot().2;
        let start = Instant::now();
        let future = self.cycle(cycle);
        tokio::pin!(future);
        match timeout(cycle_bound, future.as_mut()).await {
            Ok(Ok(())) => {
                return CycleVerdict::Pass {
                    elapsed: start.elapsed(),
                };
            }
            Ok(Err(message)) => {
                return CycleVerdict::Failed {
                    message,
                    elapsed: start.elapsed(),
                };
            }
            Err(_) => {}
        }
        let progressed = progress.snapshot().2 - before;
        match timeout(hang_window, future.as_mut()).await {
            Ok(Ok(())) => {
                return CycleVerdict::Late {
                    message: None,
                    elapsed: start.elapsed(),
                };
            }
            Ok(Err(message)) => {
                return CycleVerdict::Late {
                    message: Some(message),
                    elapsed: start.elapsed(),
                };
            }
            Err(_) => {}
        }
        if progressed == 0 {
            CycleVerdict::Hang
        } else {
            CycleVerdict::Partial {
                progressed,
                message: None,
                elapsed: start.elapsed(),
            }
        }
    }
}

fn cycles() -> u64 {
    std::env::var("MUX_SOAK_CYCLES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_CYCLES)
}

/// Base seed for the per-cycle schedule. The default keeps the gate run
/// reproducible; other seeds widen the schedule family for a longer hunt.
fn base_seed() -> u64 {
    std::env::var("MUX_SOAK_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0x5EED_1234_ABCD_0001)
}

/// Read a `u64` env override, if present and parseable.
fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standard tier: interactive-path liveness soak; run explicitly"]
async fn interactive_path_liveness_soak() {
    let total_cycles = cycles();
    let probe = Arc::new(SessionProbe::default());

    let mut client_session = JoinSet::new();
    let mut server_session = JoinSet::new();
    let (client_read, server_write) = duplex(DUPLEX_BUF);
    let (server_read, client_write) = duplex(DUPLEX_BUF);
    let common = |initiation| MuxConfig {
        initiation,
        heartbeat_interval: Duration::from_secs(60),
        frame_reassembly: false,
    };
    let (opener, _client_accepter) = spawn_mux_no_reconnection(
        client_read,
        client_write,
        common(Initiation::Client),
        &mut client_session,
    );
    let (_server_opener, accepter) = spawn_mux_no_reconnection(
        server_read,
        server_write,
        common(Initiation::Server),
        &mut server_session,
    );

    let mut monitors = JoinSet::new();
    monitors.spawn(supervise(client_session, Arc::clone(&probe)));
    monitors.spawn(supervise(server_session, Arc::clone(&probe)));
    let heartbeat = Arc::new(Heartbeat::new());
    monitors.spawn(heartbeat_task(Arc::clone(&heartbeat)));

    let mut soak = Soak {
        opener,
        accepter,
        progress: Arc::new(Progress::default()),
        inflight: Arc::new(Inflight::default()),
        rng: base_seed(),
        cycle_bound: CYCLE_BOUND,
        hang_bound: HANG_BOUND,
        next_seed: base_seed() ^ 0x00C0_FFEE_1234_5678,
    };

    let mut passes = 0u64;
    let mut worst = Duration::ZERO;
    let mut failure: Option<String> = None;
    let inflight = Arc::clone(&soak.inflight);

    // The per-stream byte ledger only records while the probe is enabled, and
    // is reset per cycle so a stall's report names this cycle's streams.
    mux::live_probe::enable_stream_trace();

    // Egress probe counts are cumulative across both sessions in this
    // process, so a stall reports the delta the stalled cycle added: a park
    // with queued work that first appears inside the stalled cycle is that
    // cycle's defect, not an earlier one's, and a delta of zero proves the
    // stall is downstream of the egress fair queue rather than in it. The
    // snapshot is taken per cycle just below.

    // Diagnostic replay: advance the schedule to `MUX_SOAK_REPLAY_TO` without
    // running the earlier cycles, then either run from there or repeat that
    // single cycle `MUX_SOAK_REPEAT` times (RNG restored each repeat, so the
    // schedule is identical). A full run is unaffected.
    let replay_to = env_u64("MUX_SOAK_REPLAY_TO").unwrap_or(0);
    let repeat = env_u64("MUX_SOAK_REPEAT").unwrap_or(0);
    for cycle in 0..replay_to {
        soak.skip_cycle(cycle);
    }
    let repeating = repeat > 0;
    let saved = (soak.rng, soak.next_seed);
    let planned: Vec<u64> = if repeating {
        vec![replay_to; repeat as usize]
    } else {
        (replay_to..total_cycles).collect()
    };
    let planned_total = planned.len() as u64;

    for cycle in planned {
        if repeating {
            soak.rng = saved.0;
            soak.next_seed = saved.1;
        }
        let egress_before = mux::live_probe::totals();
        mux::live_probe::reset_stream_trace();
        match soak.cycle_verdict(cycle).await {
            CycleVerdict::Pass { elapsed } => {
                passes += 1;
                worst = worst.max(elapsed);
            }
            CycleVerdict::Failed { message, elapsed } => {
                failure = Some(format!("cycle {cycle} failed after {elapsed:?}: {message}"));
                break;
            }
            CycleVerdict::Late { message, elapsed } => {
                let detail = message
                    .map(|message| format!("; cycle error: {message}"))
                    .unwrap_or_default();
                failure = Some(format!(
                    "cycle {cycle} completed late: {elapsed:?} > {CYCLE_BOUND:?}{detail}; \
                     in-flight: {:?}; {}; {}",
                    inflight.snapshot(),
                    heartbeat.report(),
                    mux::live_probe::totals().since(&egress_before)
                ));
                break;
            }
            CycleVerdict::Partial {
                progressed,
                message,
                elapsed,
            } => {
                let detail = message
                    .map(|message| format!("; cycle error: {message}"))
                    .unwrap_or_default();
                failure = Some(format!(
                    "cycle {cycle} stalled: only {progressed} job(s) finished within \
                     {elapsed:?} and the cycle never completed{detail}; in-flight: {:?}; {}; {}; {}",
                    inflight.snapshot(),
                    heartbeat.report(),
                    mux::live_probe::totals().since(&egress_before),
                    mux::live_probe::stream_trace_report()
                ));
                break;
            }
            CycleVerdict::Hang => {
                let (staged, received, jobs) = soak.progress.snapshot();
                failure = Some(format!(
                    "cycle {cycle} hung: no progress for at least {HANG_BOUND:?} \
                     (staged={staged} received={received} completed_jobs={jobs}); \
                     in-flight: {:?}; {}; {}; {}",
                    inflight.snapshot(),
                    heartbeat.report(),
                    mux::live_probe::totals().since(&egress_before),
                    mux::live_probe::stream_trace_report()
                ));
                break;
            }
        }
        if let Some(message) = probe.take() {
            failure = Some(format!("session died during cycle {cycle}: {message}"));
            break;
        }
    }

    let (staged, received, jobs) = soak.progress.snapshot();
    let opened = soak.progress.opened_streams.load(Ordering::Relaxed);
    println!(
        "soak: cycles={passes}/{planned_total} worst_cycle={worst:?} streams_opened={opened} \
         staged_bytes={staged} received_bytes={received} completed_jobs={jobs} {}; {}",
        heartbeat.report(),
        mux::live_probe::totals()
    );

    if staged != received {
        failure.get_or_insert_with(|| {
            format!(
                "byte conservation violated: staged {staged}, received {received} \
                 (silent loss)"
            )
        });
    }

    if failure.is_some() {
        let probe = soak.probe_after_stall().await;
        let detail = match probe {
            Ok(()) => {
                "post-stall probe: a fresh one-round ping-pong completed (stream-local stall)"
                    .to_owned()
            }
            Err(message) => format!("post-stall probe failed: {message}"),
        };
        println!("soak: {detail}");
        if let Some(failure) = failure.as_mut() {
            failure.push_str(&format!("; {detail}"));
        }
    }

    if let Some(message) = probe.take() {
        failure.get_or_insert(message);
    }
    // The sessions own the transport halves and only end when the test scope
    // drops them, so the supervisors are stopped explicitly rather than
    // awaited.
    drop(soak);
    monitors.abort_all();
    while monitors.join_next().await.is_some() {}

    if let Some(message) = failure {
        panic!("interactive-path liveness soak failed: {message}");
    }
    assert!(passes > 0, "soak ran no cycles");
}
