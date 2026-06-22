use std::time::Duration;

use mux::{spawn_mux_no_reconnection, Initiation, MuxConfig};
use tokio::{
    io::{duplex, AsyncReadExt, AsyncWriteExt},
    task::JoinSet,
};

// A large buffer for the duplex pipes so that the central io writers never
// block on writes during the test.
const DUPLEX_BUF: usize = 64 * 1024;

// Spins up a pair of mux endpoints connected through tokio duplex pipes and
// returns the client-side opener, the server-side accepter and the join set
// holding the background tasks.
fn spawn_mux_pair() -> (
    mux::StreamOpener,
    mux::StreamAccepter,
    JoinSet<mux::MuxError>,
) {
    let mut spawner = JoinSet::new();

    // (client_read, server_write) and (server_read, client_write) form the two
    // directions of the underlying byte stream.
    let (client_read, server_write) = duplex(DUPLEX_BUF);
    let (server_read, client_write) = duplex(DUPLEX_BUF);

    let (client_opener, _client_accepter) = spawn_mux_no_reconnection(
        client_read,
        client_write,
        MuxConfig {
            initiation: Initiation::Client,
            heartbeat_interval: Duration::from_secs(60),
        },
        &mut spawner,
    );
    let (_server_opener, server_accepter) = spawn_mux_no_reconnection(
        server_read,
        server_write,
        MuxConfig {
            initiation: Initiation::Server,
            heartbeat_interval: Duration::from_secs(60),
        },
        &mut spawner,
    );

    (client_opener, server_accepter, spawner)
}

#[tokio::test(flavor = "multi_thread")]
async fn manual_shutdown_reaches_peer() {
    let (client_opener, mut server_accepter, _spawner) = spawn_mux_pair();

    // Open a stream from the client and accept it on the server.
    let (_client_reader, mut client_writer) = client_opener.open().await.unwrap();
    let (mut server_reader, _server_writer) = server_accepter.accept().await.unwrap();

    // Perform the manual shutdown on the writer.
    client_writer.shutdown().unwrap();

    // The peer must observe EOF on its reader: a read of any size returns 0.
    let mut buf = [0u8; 8];
    let n = server_reader.read(&mut buf).await.unwrap();
    assert_eq!(n, 0, "peer reader should observe EOF after shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_writer_does_not_break_peer() {
    let (client_opener, mut server_accepter, _spawner) = spawn_mux_pair();

    // Open a stream from the client and accept it on the server.
    let (_client_reader, client_writer) = client_opener.open().await.unwrap();
    let (mut server_reader, mut server_writer) = server_accepter.accept().await.unwrap();

    // Drop the client-side writer without an explicit shutdown.
    drop(client_writer);
    // Give the background tasks a moment to process the close message that
    // the StreamCloseTx Drop emits.
    tokio::task::yield_now().await;

    // Dropping the client writer should not break the mux connection: the
    // peer must still be able to open new streams and exchange data.
    let (mut client_reader2, _client_writer2) = client_opener.open().await.unwrap();
    let (_server_reader2, mut server_writer2) = server_accepter.accept().await.unwrap();

    let payload = b"still alive";
    server_writer2.write_all(payload).await.unwrap();
    server_writer2.shutdown().unwrap();

    let mut got = [0u8; 11];
    client_reader2.read_exact(&mut got).await.unwrap();
    assert_eq!(&got, payload);

    // The peer's reader on the first stream should observe EOF (the dropped
    // writer triggers a close-write control message, which surfaces as a
    // graceful EOF rather than a broken pipe).
    let mut buf = [0u8; 8];
    let n = server_reader.read(&mut buf).await.unwrap();
    assert_eq!(n, 0, "dropping writer should surface as graceful EOF");

    // The peer's writer on the first stream should still be usable enough to
    // be shut down cleanly without panicking.
    server_writer.shutdown().unwrap();
}
