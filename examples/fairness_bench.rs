use std::{
    io,
    sync::Arc,
    time::{Duration, Instant},
};

use clap::Parser;
use mux::{spawn_mux_no_reconnection, Initiation, MuxConfig};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    task::JoinSet,
};

#[derive(Debug, Parser)]
struct Cli {
    /// Bulk transfer size in MiB. Defaults to a value that keeps the bulk
    /// stream running for the whole ping measurement under typical localhost
    /// throughput. Mutually exclusive with `--bulk-kib` (the last non-default
    /// value wins; if both are set, `--bulk-kib` takes precedence when nonzero).
    #[clap(long, default_value_t = 128)]
    bulk_mib: usize,
    /// Bulk transfer size in KiB. When nonzero, overrides `--bulk-mib` so a
    /// short bulk transfer can be paired with a small ping count to model a
    /// low-bandwidth, packet-quantum-limited link.
    #[clap(long, default_value_t = 0)]
    bulk_kib: usize,
    /// Number of ping round-trips to measure under contention.
    #[clap(long, default_value_t = 300)]
    pings: usize,
    /// Cap the bulk client's wire rate to `kbps` kilobits per second using a
    /// rate-limited writer that emits at most `MAX_QUANTUM` bytes per write.
    /// 0 disables rate limiting. Models a low-bandwidth link so the benchmark
    /// exercises the same packet quantum the mux writer uses for contended
    /// traffic.
    #[clap(long, default_value_t = 0)]
    rate_kbps: u64,
}

/// Maximum bytes the rate-limited writer hands to the underlying socket in a
/// single `write` call. Matches the mux writer's
/// `DATA_HEAD_EXTREME_CONTENDED_CAP` (1200 bytes): the QUIC-like packet
/// quantum for latency-sensitive/protected sends.
const MAX_QUANTUM: usize = 1200;

const CHUNK: usize = 64 * 1024;
const PING_SIZE: usize = 64;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Cli::parse();
    let bulk_bytes = if args.bulk_kib != 0 {
        args.bulk_kib * 1024
    } else {
        args.bulk_mib * 1024 * 1024
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_tcp_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        stream
    });
    let client_tcp = TcpStream::connect(addr).await.unwrap();
    let server_tcp = server_tcp_task.await.unwrap();
    client_tcp.set_nodelay(true).unwrap();
    server_tcp.set_nodelay(true).unwrap();

    let mut mux_spawner: JoinSet<mux::MuxError> = JoinSet::new();

    let (c_r, c_w) = client_tcp.into_split();
    let client_config = MuxConfig {
        initiation: Initiation::Client,
        heartbeat_interval: Duration::from_secs(60),
    };
    let (client_opener, _client_accepter) =
        spawn_mux_no_reconnection(c_r, c_w, client_config, &mut mux_spawner);

    let (s_r, s_w) = server_tcp.into_split();
    let server_config = MuxConfig {
        initiation: Initiation::Server,
        heartbeat_interval: Duration::from_secs(60),
    };
    let (_server_opener, server_accepter) =
        spawn_mux_no_reconnection(s_r, s_w, server_config, &mut mux_spawner);

    let mut server_accepter = server_accepter;

    let bulk_mib_s = uncontended_bulk(&client_opener, &mut server_accepter, bulk_bytes).await;
    let (contended_bulk_mib_s, ping_us_p50, ping_us_p95, ping_us_p99, ping_us_max) =
        contended_ping(
            &client_opener,
            &mut server_accepter,
            bulk_bytes,
            args.pings,
            args.rate_kbps,
        )
        .await;

    println!(
        "bulk_mib_s={:.3} contended_bulk_mib_s={:.3} ping_us_p50={} ping_us_p95={} ping_us_p99={} ping_us_max={}",
        bulk_mib_s, contended_bulk_mib_s, ping_us_p50, ping_us_p95, ping_us_p99, ping_us_max
    );
}

async fn uncontended_bulk(
    client_opener: &mux::StreamOpener,
    server_accepter: &mut mux::StreamAccepter,
    bulk_bytes: usize,
) -> f64 {
    let (c_reader, mut c_writer) = client_opener.open().await.unwrap();
    let (mut s_reader, s_writer) = server_accepter.accept().await.unwrap();
    drop(c_reader);
    drop(s_writer);

    let server_task = tokio::spawn(async move {
        let mut buf = vec![0u8; CHUNK];
        let mut remaining = bulk_bytes;
        while remaining > 0 {
            let n = remaining.min(buf.len());
            s_reader.read_exact(&mut buf[..n]).await.unwrap();
            remaining -= n;
        }
    });

    let chunk = vec![0x42u8; CHUNK];
    let start = Instant::now();
    let mut remaining = bulk_bytes;
    while remaining > 0 {
        let n = remaining.min(chunk.len());
        c_writer.write_all(&chunk[..n]).await.unwrap();
        remaining -= n;
    }
    let elapsed = start.elapsed().as_secs_f64();
    server_task.await.unwrap();
    (bulk_bytes as f64) / elapsed / (1024.0 * 1024.0)
}

async fn contended_ping(
    client_opener: &mux::StreamOpener,
    server_accepter: &mut mux::StreamAccepter,
    bulk_bytes: usize,
    pings: usize,
    rate_kbps: u64,
) -> (f64, u128, u128, u128, u128) {
    let (bulk_c_reader, mut bulk_c_writer) = client_opener.open().await.unwrap();
    let (mut bulk_s_reader, bulk_s_writer) = server_accepter.accept().await.unwrap();
    drop(bulk_c_reader);
    drop(bulk_s_writer);

    let (mut ping_c_reader, mut ping_c_writer) = client_opener.open().await.unwrap();
    let (mut ping_s_reader, mut ping_s_writer) = server_accepter.accept().await.unwrap();

    let bulk_server_task = tokio::spawn(async move {
        let mut buf = vec![0u8; CHUNK];
        let mut remaining = bulk_bytes;
        while remaining > 0 {
            let n = remaining.min(buf.len());
            bulk_s_reader.read_exact(&mut buf[..n]).await.unwrap();
            remaining -= n;
        }
    });

    let echo_task = tokio::spawn(async move {
        let mut buf = [0u8; PING_SIZE];
        loop {
            if ping_s_reader.read_exact(&mut buf).await.is_err() {
                break;
            }
            if ping_s_writer.write_all(&buf).await.is_err() {
                break;
            }
        }
    });

    let bulk_client_task: tokio::task::JoinHandle<f64> = tokio::spawn(async move {
        let chunk = vec![0x42u8; CHUNK];
        let start = Instant::now();
        let mut remaining = bulk_bytes;
        // Optionally throttle the bulk writer so it models a low-bandwidth link
        // and emits at most MAX_QUANTUM bytes per write, matching the mux
        // writer's contended packet quantum.
        let limiter = if rate_kbps != 0 {
            Some(Arc::new(Mutex::new(RateLimiter::new(rate_kbps))) as Arc<Mutex<RateLimiter>>)
        } else {
            None
        };
        while remaining > 0 {
            let n = remaining.min(chunk.len());
            write_limited(&mut bulk_c_writer, &chunk[..n], limiter.as_ref())
                .await
                .unwrap();
            remaining -= n;
        }
        let elapsed = start.elapsed().as_secs_f64();
        (bulk_bytes as f64) / elapsed / (1024.0 * 1024.0)
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    if bulk_client_task.is_finished() {
        panic!("bulk finished before ping contention; increase --bulk-mib");
    }

    let ping_data = [0xABu8; PING_SIZE];
    let mut latencies: Vec<u128> = Vec::with_capacity(pings);
    for _ in 0..pings {
        let start = Instant::now();
        ping_c_writer.write_all(&ping_data).await.unwrap();
        let mut buf = [0u8; PING_SIZE];
        ping_c_reader.read_exact(&mut buf).await.unwrap();
        latencies.push(start.elapsed().as_micros());
    }

    let _ = ping_c_writer.shutdown();
    drop(ping_c_reader);
    let _ = echo_task.await;

    let contended_bulk_mib_s = bulk_client_task.await.unwrap();
    bulk_server_task.await.unwrap();

    latencies.sort_unstable();
    let ping_us_p50 = percentile(&latencies, 50);
    let ping_us_p95 = percentile(&latencies, 95);
    let ping_us_p99 = percentile(&latencies, 99);
    let ping_us_max = *latencies.last().unwrap();

    (
        contended_bulk_mib_s,
        ping_us_p50,
        ping_us_p95,
        ping_us_p99,
        ping_us_max,
    )
}

/// Token-bucket rate limiter for the bulk writer. Refills at `kbps` kilobits
/// per second and is consumed by `RateLimitedWriter` before each quantum.
struct RateLimiter {
    /// Available bytes.
    budget: u64,
    /// Bytes added per second.
    bytes_per_sec: u64,
    /// Last refill instant.
    last: Instant,
}

impl RateLimiter {
    fn new(kbps: u64) -> Self {
        let bytes_per_sec = kbps * 1000 / 8;
        Self {
            // Prefill so the first quantum isn't blocked.
            budget: bytes_per_sec,
            bytes_per_sec,
            last: Instant::now(),
        }
    }

    /// Refill the bucket, then sleep until at least `want` bytes are available.
    async fn acquire(&mut self, want: u64) {
        loop {
            let now = Instant::now();
            let elapsed = now.saturating_duration_since(self.last);
            self.budget = self.budget.saturating_add(
                (self.bytes_per_sec as u128 * elapsed.as_nanos() / 1_000_000_000) as u64,
            );
            self.last = now;
            if self.budget >= want {
                self.budget -= want;
                return;
            }
            let deficit = want - self.budget;
            let wait_ns = (deficit as u128 * 1_000_000_000 / self.bytes_per_sec as u128) as u64;
            tokio::time::sleep(Duration::from_nanos(wait_ns.max(1))).await;
        }
    }
}

/// Write `buf` to `writer`, optionally rate-limiting it. When a limiter is
/// present, the write is split into `MAX_QUANTUM`-byte chunks, each preceded
/// by `RateLimiter::acquire` so the wire never sees a write larger than the
/// mux writer's contended packet quantum.
async fn write_limited<W: AsyncWrite + Unpin>(
    writer: &mut W,
    buf: &[u8],
    limiter: Option<&Arc<Mutex<RateLimiter>>>,
) -> io::Result<()> {
    let mut offset = 0usize;
    while offset < buf.len() {
        let n = (buf.len() - offset).min(MAX_QUANTUM);
        if let Some(limiter) = limiter {
            limiter.lock().await.acquire(n as u64).await;
        }
        writer.write_all(&buf[offset..offset + n]).await?;
        offset += n;
    }
    Ok(())
}

fn percentile(sorted: &[u128], p: u32) -> u128 {
    let n = sorted.len();
    let idx = (p as usize * n).div_ceil(100).saturating_sub(1).min(n - 1);
    sorted[idx]
}
