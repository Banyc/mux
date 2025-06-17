use std::{path::PathBuf, time::Duration};

use clap::Parser;
use file_transfer::FileTransferCommand;
use mux::{spawn_mux_no_reconnection, Initiation, MuxConfig};
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
            };
            let (opener, _accepter) =
                spawn_mux_no_reconnection(read, write, config, &mut mux_spawner);
            let (r, w) = opener.open().await.unwrap();
            (Box::new(r), Box::new(w))
        }
        _ => panic!("unknown protocol `{protocol}`"),
    };
    println!("connected");

    let mut res = args.file_transfer.perform(read, write).await.unwrap();
    res.write.shutdown().await.unwrap();
    println!("shutdown");
    let mut buf = [0; 1];
    let n = res.read.read(&mut buf).await.unwrap();
    assert_eq!(n, 0);

    println!("{}", res.stats);
}
