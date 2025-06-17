//! <https://github.com/black-binary/async-smux/blob/main/benches/bench.rs>

#[cfg(test)]
mod benches {
    use std::{pin::Pin, sync::LazyLock, time::Duration};

    use test::Bencher;
    use tokio::{
        io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        runtime::Runtime,
        task::JoinSet,
    };

    use crate::{
        spawn_mux_no_reconnection, Initiation, MuxConfig, MuxError, StreamReader, StreamWriter,
    };

    static RT: LazyLock<Runtime> = LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    });

    #[derive(Debug)]
    pub struct DuplexStream<R, W> {
        r: R,
        w: W,
    }
    impl<R, W> DuplexStream<R, W> {
        pub fn new(r: R, w: W) -> Self {
            Self { r, w }
        }
    }
    impl<R: AsyncRead + Unpin, W: Unpin> AsyncRead for DuplexStream<R, W> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            Pin::new(&mut self.r).poll_read(cx, buf)
        }
    }
    impl<R: Unpin, W: AsyncWrite + Unpin> AsyncWrite for DuplexStream<R, W> {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<Result<usize, std::io::Error>> {
            Pin::new(&mut self.w).poll_write(cx, buf)
        }
        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            Pin::new(&mut self.w).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            Pin::new(&mut self.w).poll_shutdown(cx)
        }
    }

    async fn get_tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let h = tokio::spawn(async move {
            let (a, _) = listener.accept().await.unwrap();
            a
        });

        let b = TcpStream::connect(addr).await.unwrap();
        let a = h.await.unwrap();
        // a.set_nodelay(true).unwrap();
        // b.set_nodelay(true).unwrap();
        (a, b)
    }
    fn get_mux_pair(
        spawner: &mut JoinSet<MuxError>,
    ) -> (
        DuplexStream<StreamReader, StreamWriter>,
        DuplexStream<StreamReader, StreamWriter>,
    ) {
        RT.block_on(async {
            let (a, b) = get_tcp_pair().await;
            let (a_r, a_w) = a.into_split();
            let config = MuxConfig {
                initiation: Initiation::Server,
                heartbeat_interval: Duration::from_secs(5),
            };
            let (opener, _) = spawn_mux_no_reconnection(a_r, a_w, config, spawner);
            let (b_r, b_w) = b.into_split();
            let config = MuxConfig {
                initiation: Initiation::Client,
                heartbeat_interval: Duration::from_secs(5),
            };
            let (_, mut accepter) = spawn_mux_no_reconnection(b_r, b_w, config, spawner);
            let a = opener.open().await.unwrap();
            let b = accepter.accept().await.unwrap();
            let a = DuplexStream::new(a.0, a.1);
            let b = DuplexStream::new(b.0, b.1);
            (a, b)
        })
    }
    fn get_smux_pair() -> (
        async_smux::MuxStream<TcpStream>,
        async_smux::MuxStream<TcpStream>,
    ) {
        RT.block_on(async {
            let (a, b) = get_tcp_pair().await;
            let (connector, _, worker) =
                async_smux::MuxBuilder::client().with_connection(a).build();
            RT.spawn(worker);
            let (_, mut acceptor, worker) =
                async_smux::MuxBuilder::server().with_connection(b).build();
            RT.spawn(worker);
            let a = connector.connect().unwrap();
            let b = acceptor.accept().await.unwrap();
            (a, b)
        })
    }

    #[inline]
    async fn send<T: AsyncWrite + Unpin>(data: &[u8], a: &mut T) {
        a.write_all(data).await.unwrap();
        a.flush().await.unwrap();
    }

    #[inline]
    async fn recv<T: AsyncRead + Unpin>(buf: &mut [u8], a: &mut T) -> std::io::Result<()> {
        a.read_exact(buf).await?;
        Ok(())
    }

    const DATA_SIZE: usize = 0x20000;

    fn bench_send<T: AsyncRead + AsyncWrite + Send + Unpin + 'static>(
        bencher: &mut Bencher,
        mut a: T,
        mut b: T,
    ) {
        let data = vec![0; DATA_SIZE];
        let mut buf = vec![0; DATA_SIZE];
        RT.spawn(async move {
            loop {
                if recv(&mut buf, &mut b).await.is_err() {
                    break;
                }
            }
        });
        bencher.bytes = DATA_SIZE as u64;

        // Warm up
        for _ in 0..10 {
            RT.block_on(async {
                send(&data, &mut a).await;
            });
        }

        for _ in 0..10 {
            bencher.iter(|| {
                RT.block_on(async {
                    send(&data, &mut a).await;
                });
            });
        }
    }

    #[bench]
    fn bench_tcp_send(bencher: &mut Bencher) {
        let (a, b) = RT.block_on(async { get_tcp_pair().await });
        bench_send(bencher, a, b);
    }
    #[bench]
    fn bench_mux_send(bencher: &mut Bencher) {
        let mut spawner = JoinSet::new();
        let (a, b) = get_mux_pair(&mut spawner);
        bench_send(bencher, a, b);
    }
    #[bench]
    fn bench_smux_send(bencher: &mut Bencher) {
        let (a, b) = get_smux_pair();
        bench_send(bencher, a, b);
    }
}
