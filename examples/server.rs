use std::time::Duration;

use clap::Parser;
use file_transfer::FileTransferCommand;
use mux::{Initiation, MuxConfig, spawn_mux_no_reconnection};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    task::JoinSet,
};

#[derive(Debug, Parser)]
pub struct Cli {
    /// The listen address
    pub listen: String,
    #[command(subcommand)]
    pub file_transfer: FileTransferCommand,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    let (protocol, internet_addresses) = args.listen.split_once("://").unwrap();
    let internet_addresses = internet_addresses.split(',').collect::<Vec<_>>();
    let mut mux_spawner = JoinSet::new();
    let (read, write): (
        Box<dyn AsyncRead + Unpin + Sync + Send + 'static>,
        Box<dyn AsyncWrite + Unpin + Sync + Send + 'static>,
    ) = match protocol {
        "tcp" => {
            let listener = TcpListener::bind(internet_addresses[0]).await.unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            let (read, write) = stream.into_split();
            (Box::new(read), Box::new(write))
        }
        "mux" => {
            let listener = TcpListener::bind(internet_addresses[0]).await.unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            let (read, write) = stream.into_split();
            let config = MuxConfig {
                initiation: Initiation::Server,
                heartbeat_interval: Duration::from_secs(5),
                frame_reassembly: false,
            };
            let (_opener, mut accepter) =
                spawn_mux_no_reconnection(read, write, config, &mut mux_spawner);
            let (r, w) = accepter.accept().await.unwrap();
            (Box::new(r), Box::new(w))
        }
        _ => panic!("unknown protocol `{protocol}`"),
    };
    println!("accepted");

    // The whole operation — transfer, shutdown, EOF verification, and the
    // stats report — runs inside ONE future selected against the mux
    // supervisor, so a premature mux completion or supervision error panics
    // instead of letting the operation continue against a dead session.
    let operation = async {
        let mut res = args.file_transfer.perform(read, write).await.unwrap();
        // Orderly shutdown: send our FIN, then wait for the peer's. The
        // strict EOF wait below is the explicit close acknowledgement —
        // the peer's EOF wait can only complete after our FIN was flushed
        // and ours only after the peer's, so neither side exits (tearing
        // down the transport) until the close has been acknowledged in both
        // directions.
        res.write.shutdown().await.unwrap();
        println!("shutdown");
        let mut buf = [0; 1];
        // Require a clean EOF: a reset/broken-pipe/session-death read here
        // is a failed close handshake, not a successful transfer.
        let n = res
            .read
            .read(&mut buf)
            .await
            .unwrap_or_else(|e| panic!("EOF verification read failed: {e}"));
        assert_eq!(n, 0, "expected a clean EOF from the peer");
        println!("{}", res.stats);
    };
    tokio::pin!(operation);
    // `biased` polls the operation first, so a teardown that finishes the
    // operation in the same poll cycle as the session end is resolved as a
    // completed operation; the supervisor branch then only fires when the
    // operation is still genuinely in flight — a premature mux completion.
    tokio::select! {
        biased;
        () = &mut operation => {}
        joined = mux_spawner.join_next(), if !mux_spawner.is_empty() => {
            // The mux session ended before the operation completed: a
            // panicked supervision task surfaces here (JoinError unwrapped
            // directly), and a MuxError session-end is also a failure.
            let err = joined.expect("mux supervision task exists").unwrap();
            panic!("mux session ended before the operation completed: {err:?}");
        }
    }

    // The mux session only ends when the underlying transport dies (at
    // process exit), so only ALREADY-completed results are reaped instead of
    // blocking forever; dropping the spawner at process exit aborts the
    // rest. Every completed result is a failure: the session must survive
    // the whole operation, so a supervisor that ended (with a MuxError or a
    // panic) is re-raised here instead of being discarded.
    if let Some(result) = mux_spawner.try_join_next() {
        let err = result.unwrap();
        panic!("mux session ended before the process exited: {err:?}");
    }
}
