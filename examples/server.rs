use std::time::Duration;

use async_async_io::{read::PollRead, write::PollWrite};
use clap::Parser;
use file_transfer::FileTransferCommand;
use mux::{spawn_mux_no_reconnection, Initiation, MuxConfig};
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
            };
            let (opener, mut accepter) =
                spawn_mux_no_reconnection(read, write, config, &mut mux_spawner);
            tokio::spawn(async move {
                let _opener = opener;
                tokio::time::sleep(Duration::MAX).await;
            });
            let (r, w) = accepter.accept().await.unwrap();
            tokio::spawn(async move {
                loop {
                    let _ = accepter.accept().await;
                }
            });
            (Box::new(PollRead::new(r)), Box::new(PollWrite::new(w)))
        }
        _ => panic!("unknown protocol `{protocol}`"),
    };
    println!("accepted");

    let mut res = args.file_transfer.perform(read, write).await.unwrap();
    res.write.shutdown().await.unwrap();
    println!("shutdown");
    let mut buf = [0; 1];
    let n = res.read.read(&mut buf).await.unwrap();
    assert_eq!(n, 0);

    println!("{}", res.stats);
}
