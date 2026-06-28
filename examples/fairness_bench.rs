use std::time::{Duration, Instant};

use clap::Parser;
use mux::{spawn_mux_no_reconnection, Initiation, MuxConfig};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};

#[derive(Debug, Parser)]
struct Cli {
    /// Bulk transfer size in MiB.
    #[clap(long, default_value_t = 128)]
    bulk_mib: usize,
    /// Number of ping round-trips to measure under contention.
    #[clap(long, default_value_t = 300)]
    pings: usize,
}

const CHUNK: usize = 64 * 1024;
const PING_SIZE: usize = 64;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Cli::parse();
    let bulk_bytes = args.bulk_mib * 1024 * 1024;

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
        contended_ping(&client_opener, &mut server_accepter, bulk_bytes, args.pings).await;

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
        while remaining > 0 {
            let n = remaining.min(chunk.len());
            bulk_c_writer.write_all(&chunk[..n]).await.unwrap();
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

fn percentile(sorted: &[u128], p: u32) -> u128 {
    let n = sorted.len();
    let idx = (p as usize * n).div_ceil(100).saturating_sub(1).min(n - 1);
    sorted[idx]
}
