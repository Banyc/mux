#![allow(clippy::disallowed_methods)]

use std::time::Duration;

use mux::{Initiation, MuxConfig, spawn_mux_no_reconnection};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, duplex},
    task::JoinSet,
};

// A large buffer for the duplex pipes so that the central io writers never
// block on writes during the test.
const DUPLEX_BUF: usize = 64 * 1024;

fn spawn_mux_pair(
    frame_reassembly: bool,
) -> (
    mux::StreamOpener,
    mux::StreamAccepter,
    JoinSet<mux::MuxError>,
) {
    let mut tasks = JoinSet::new();
    let (client_read, server_write) = duplex(DUPLEX_BUF);
    let (server_read, client_write) = duplex(DUPLEX_BUF);
    let (client_opener, _client_accepter) = spawn_mux_no_reconnection(
        client_read,
        client_write,
        MuxConfig {
            initiation: Initiation::Client,
            heartbeat_interval: Duration::from_secs(60),
            frame_reassembly,
        },
        &mut tasks,
    );
    let (_server_opener, server_accepter) = spawn_mux_no_reconnection(
        server_read,
        server_write,
        MuxConfig {
            initiation: Initiation::Server,
            heartbeat_interval: Duration::from_secs(60),
            frame_reassembly,
        },
        &mut tasks,
    );
    (client_opener, server_accepter, tasks)
}

#[tokio::test(flavor = "multi_thread")]
async fn manual_shutdown_reaches_peer() {
    for frame_reassembly in [false, true] {
        let (client_opener, mut server_accepter, _spawner) = spawn_mux_pair(frame_reassembly);
        let (_client_reader, mut client_writer) = client_opener.open().await.unwrap();
        let (mut server_reader, _server_writer) = server_accepter.accept().await.unwrap();
        client_writer.shutdown().unwrap();
        let mut buf = [0u8; 8];
        let n = server_reader.read(&mut buf).await.unwrap();
        assert_eq!(n, 0, "peer reader should observe EOF after shutdown");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_writer_does_not_break_peer() {
    for frame_reassembly in [false, true] {
        let (client_opener, mut server_accepter, _spawner) = spawn_mux_pair(frame_reassembly);
        let (_client_reader, client_writer) = client_opener.open().await.unwrap();
        let (mut server_reader, mut server_writer) = server_accepter.accept().await.unwrap();
        drop(client_writer);
        tokio::task::yield_now().await;
        let (mut client_reader2, _client_writer2) = client_opener.open().await.unwrap();
        let (_server_reader2, mut server_writer2) = server_accepter.accept().await.unwrap();
        let payload = b"still alive";
        server_writer2.write_all(payload).await.unwrap();
        server_writer2.shutdown().unwrap();
        let mut got = [0u8; 11];
        client_reader2.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, payload);
        let mut buf = [0u8; 8];
        let n = server_reader.read(&mut buf).await.unwrap();
        assert_eq!(n, 0, "dropping writer should surface as graceful EOF");
        server_writer.shutdown().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn queued_payload_arrives_before_eof() {
    const PAYLOAD_LEN: usize = 256 * 1024 + 37;
    for frame_reassembly in [false, true] {
        let (client_opener, mut server_accepter, _spawner) = spawn_mux_pair(frame_reassembly);
        let (_client_reader, mut client_writer) = client_opener.open().await.unwrap();
        let (mut server_reader, _server_writer) = server_accepter.accept().await.unwrap();
        let payload: Vec<u8> = (0u8..=u8::MAX).cycle().take(PAYLOAD_LEN).collect();
        let expected = payload.clone();
        let writer = tokio::spawn(async move {
            client_writer.write_all(&payload).await.unwrap();
            client_writer.shutdown().unwrap();
        });
        let mut received = Vec::new();
        server_reader.read_to_end(&mut received).await.unwrap();
        writer.await.unwrap();
        assert_eq!(received, expected);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn reader_close_does_not_truncate_opposite_direction_data() {
    for frame_reassembly in [false, true] {
        let (client_opener, mut server_accepter, _spawner) = spawn_mux_pair(frame_reassembly);
        let (client_reader, mut client_writer) = client_opener.open().await.unwrap();
        let (mut server_reader, _server_writer) = server_accepter.accept().await.unwrap();
        let payload = b"close-read does not close-write";
        drop(client_reader);
        client_writer.write_all(payload).await.unwrap();
        client_writer.shutdown().unwrap();
        let mut received = Vec::new();
        server_reader.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, payload);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn peer_close_read_shutdown_error_still_leaves_mux_usable() {
    for frame_reassembly in [false, true] {
        let (client_opener, mut server_accepter, _spawner) = spawn_mux_pair(frame_reassembly);
        let (_client_reader, mut client_writer) = client_opener.open().await.unwrap();
        let (server_reader, _server_writer) = server_accepter.accept().await.unwrap();
        client_writer.write_all(b"already accepted").await.unwrap();
        drop(server_reader);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if client_writer.write(b"x").await.is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("peer CloseRead must reach the local writer");
        assert!(
            client_writer.shutdown().is_err(),
            "shutdown must preserve PeerClosedStream error reporting"
        );
        let (mut client_reader2, _client_writer2) = client_opener.open().await.unwrap();
        let (_server_reader2, mut server_writer2) = server_accepter.accept().await.unwrap();
        server_writer2.write_all(b"mux still alive").await.unwrap();
        server_writer2.shutdown().unwrap();
        let mut received = Vec::new();
        client_reader2.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, b"mux still alive");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_open_then_write_payload_arrives() {
    const NUM_STREAMS: usize = 64;
    const PAYLOAD_LEN: usize = 32;
    for frame_reassembly in [false, true] {
        let (client_opener, mut server_accepter, _spawner) = spawn_mux_pair(frame_reassembly);
        let mut server_handles = tokio::task::JoinSet::new();
        server_handles.spawn(async move {
            let mut expected: Vec<Vec<u8>> = (0..NUM_STREAMS)
                .map(|i| {
                    let mut p = vec![0u8; PAYLOAD_LEN];
                    let seed = i as u8;
                    for (j, b) in p.iter_mut().enumerate() {
                        *b = seed.wrapping_add(j as u8);
                    }
                    p
                })
                .collect();
            for _ in 0..NUM_STREAMS {
                let (mut reader, _writer) = server_accepter.accept().await.unwrap();
                let mut buf = vec![0u8; PAYLOAD_LEN];
                reader.read_exact(&mut buf).await.unwrap();
                let idx = expected
                    .iter()
                    .position(|p| *p == buf)
                    .expect("server received an unexpected/unknown payload");
                expected.swap_remove(idx);
            }
            assert!(expected.is_empty(), "server did not receive all payloads");
        });
        let mut client_handles = tokio::task::JoinSet::new();
        for i in 0..NUM_STREAMS {
            let opener = client_opener.clone();
            client_handles.spawn(async move {
                let (_reader, mut writer) = opener.open().await.unwrap();
                let mut payload = vec![0u8; PAYLOAD_LEN];
                let seed = i as u8;
                for (j, b) in payload.iter_mut().enumerate() {
                    *b = seed.wrapping_add(j as u8);
                }
                writer.write_all(&payload).await.unwrap();
                writer.shutdown().unwrap();
                drop(_reader);
            });
        }
        while let Some(res) = client_handles.join_next().await {
            res.unwrap();
        }
        server_handles.join_next().await.unwrap().unwrap();
    }
}
