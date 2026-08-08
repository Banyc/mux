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

    let transfer_fut = args.file_transfer.perform(read, write);
    tokio::pin!(transfer_fut);
    let res = tokio::select! {
        res = &mut transfer_fut => res,
        joined = mux_spawner.join_next(), if !mux_spawner.is_empty() => {
            // The mux session ended before the transfer completed: a
            // panicked supervision task surfaces here (JoinError unwrapped
            // directly), and a MuxError session-end is also a failure.
            let err = joined.expect("mux supervision task exists").unwrap();
            panic!("mux session ended before the transfer completed: {err:?}");
        }
    };
    let mut res = res.unwrap();
    res.write.shutdown().await.unwrap();
    println!("shutdown");
    let mut buf = [0; 1];
    let n = res.read.read(&mut buf).await.unwrap();
    assert_eq!(n, 0);

    // The mux session only ends when the underlying transport dies (at
    // process exit), so drain only what has ALREADY completed (surfacing any
    // panic) instead of blocking forever; dropping the spawner at process
    // exit aborts the rest.
    while let Some(result) = mux_spawner.try_join_next() {
        result.unwrap();
    }

    println!("{}", res.stats);
}
