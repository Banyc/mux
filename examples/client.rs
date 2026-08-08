use std::{path::PathBuf, time::Duration};

use clap::Parser;
use file_transfer::FileTransferCommand;
use mux::{Initiation, MuxConfig, spawn_mux_no_reconnection};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    task::JoinSet,
};

#[derive(Debug, Parser)]
pub struct Cli {
    /// The server address
    pub server: String,
    #[command(subcommand)]
    pub file_transfer: FileTransferCommand,
    #[clap(long)]
    pub log_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    let (protocol, internet_addresses) = args.server.split_once("://").unwrap();
    let internet_addresses = internet_addresses.split(',').collect::<Vec<_>>();
    let mut mux_spawner = JoinSet::new();
    let (read, write): (
        Box<dyn AsyncRead + Unpin + Sync + Send + 'static>,
        Box<dyn AsyncWrite + Unpin + Sync + Send + 'static>,
    ) = match protocol {
        "tcp" => {
            let stream = TcpStream::connect(internet_addresses[0]).await.unwrap();
            let (read, write) = stream.into_split();
            (Box::new(read), Box::new(write))
        }
        "mux" => {
            let stream = TcpStream::connect(internet_addresses[0]).await.unwrap();
            let (read, write) = stream.into_split();
            let config = MuxConfig {
                initiation: Initiation::Client,
                heartbeat_interval: Duration::from_secs(5),
                frame_reassembly: false,
            };
            let (opener, _accepter) =
                spawn_mux_no_reconnection(read, write, config, &mut mux_spawner);
            let (r, w) = opener.open().await.unwrap();
            (Box::new(r), Box::new(w))
        }
        _ => panic!("unknown protocol `{protocol}`"),
    };
    println!("connected");

    // The whole operation — transfer, shutdown, EOF verification, and the
    // stats report — runs inside ONE future selected against the mux
    // supervisor, so a premature mux completion or supervision error panics
    // instead of letting the operation continue against a dead session.
    let operation = async {
        let mut res = args.file_transfer.perform(read, write).await.unwrap();
        res.write.shutdown().await.unwrap();
        println!("shutdown");
        let mut buf = [0; 1];
        let n = match res.read.read(&mut buf).await {
            Ok(n) => n,
            // The peer exits right after its own EOF wait (the transport
            // dies at process exit), so the stream FIN may be cut short by
            // the connection teardown. The transfer itself already
            // completed, so treat the session-death read errors as the peer
            // having finished.
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::ConnectionReset
                ) =>
            {
                0
            }
            Err(e) => panic!("EOF verification read failed: {e}"),
        };
        assert_eq!(n, 0);
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
    // process exit), so drain only what has ALREADY completed (surfacing any
    // panic) instead of blocking forever; dropping the spawner at process
    // exit aborts the rest.
    while let Some(result) = mux_spawner.try_join_next() {
        result.unwrap();
    }
}
