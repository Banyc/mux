use std::{future::Future, io, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::oneshot,
    task::JoinSet,
};

use crate::{
    StreamAccepter,
    central_io::{
        encoder::{CentralIoEncoder, RunCentralIoWriterError, run_central_io_writer},
        reader::{
            CentralIoReader, RunCentralIoReaderError, central_io_read_channel,
            run_central_io_reader,
        },
        scheduler::{write_control_channel, write_data_channel},
    },
    control::{Initiation, MuxControl, RunControlArgs, RunControlError, run_control},
    protocol::Side,
    stream::{
        StreamInitChannels,
        accepter::stream_accept_channel,
        opener::{StreamOpener, stream_open_channel},
    },
};

#[derive(Debug, Clone)]
pub struct MuxConfig {
    pub initiation: Initiation,
    pub heartbeat_interval: Duration,
    /// Opt-in out-of-order frame reassembly. When enabled on both peers,
    /// Data frames carry a per-stream u32 byte offset and CloseWrite
    /// carries the stream's final offset, so the central reader can
    /// reassemble each stream independently from a transport that delivers
    /// complete frames out of order (e.g. the transport's frame-delivery
    /// mode).
    /// Default off = wire byte-identical to the stock protocol and zero
    /// extra cost. Both peers must enable it together; there is no
    /// in-band negotiation.
    pub frame_reassembly: bool,
}

impl MuxConfig {
    /// Stock defaults: server initiation, 5 s heartbeat, reassembly off.
    pub fn new(initiation: Initiation, heartbeat_interval: Duration) -> Self {
        Self {
            initiation,
            heartbeat_interval,
            frame_reassembly: false,
        }
    }
}

#[derive(Debug)]
pub enum MuxError {
    IoReader(io::Error),
    IoWriter(io::Error),
    DualLane {
        lane: crate::traffic_class::LaneClass,
        peer_lane_aborted: bool,
        source: Box<MuxError>,
    },
    TaskStopped {
        task: &'static str,
    },
    ControlChannelClosed {
        task: &'static str,
    },
}

pub fn spawn_mux_no_reconnection<R, W>(
    io_reader: R,
    io_writer: W,
    config: MuxConfig,
    tasks: &mut JoinSet<MuxError>,
) -> (StreamOpener, StreamAccepter)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    async fn never_reconnect<R, W>() -> Option<(R, W)> {
        unreachable!()
    }
    #[expect(unused_assignments)]
    let mut reconnect = Some(never_reconnect);
    reconnect = None;
    spawn_mux(io_reader, io_writer, config, reconnect, tasks, None, None)
}

/// Like [`spawn_mux_no_reconnection`] but enforces a shorter
/// `first_receive_deadline` until the first frame arrives, then
/// switches to the steady receive deadline derived from the
/// heartbeat interval. Use [`crate::write_liveness_heartbeat`] on the
/// paired transport to prove lane liveness before any app data flows.
pub fn spawn_mux_no_reconnection_with_first_receive_deadline<R, W>(
    io_reader: R,
    io_writer: W,
    config: MuxConfig,
    first_receive_deadline: Duration,
    tasks: &mut JoinSet<MuxError>,
) -> (StreamOpener, StreamAccepter)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    async fn never_reconnect<R, W>() -> Option<(R, W)> {
        unreachable!()
    }
    #[expect(unused_assignments)]
    let mut reconnect = Some(never_reconnect);
    reconnect = None;
    spawn_mux(
        io_reader,
        io_writer,
        config,
        reconnect,
        tasks,
        Some(first_receive_deadline),
        None,
    )
}

/// Like [`spawn_mux_no_reconnection_with_first_receive_deadline`] but also
/// returns a [`oneshot::Receiver`] that resolves when the first complete
/// mux frame (including heartbeat) is received, proving lane liveness.
pub fn spawn_mux_no_reconnection_with_first_receive_deadline_and_ready<R, W>(
    io_reader: R,
    io_writer: W,
    config: MuxConfig,
    deadline: Duration,
    tasks: &mut JoinSet<MuxError>,
) -> (StreamOpener, StreamAccepter, oneshot::Receiver<()>)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    async fn never_reconnect<R, W>() -> Option<(R, W)> {
        unreachable!()
    }
    #[expect(unused_assignments)]
    let mut reconnect = Some(never_reconnect);
    reconnect = None;
    let (ready_tx, ready_rx) = oneshot::channel();
    let (opener, accepter) = spawn_mux(
        io_reader,
        io_writer,
        config,
        reconnect,
        tasks,
        Some(deadline),
        Some(ready_tx),
    );
    (opener, accepter, ready_rx)
}
pub fn spawn_mux_with_reconnection<R, W, ReconnectFut>(
    io_reader: R,
    io_writer: W,
    config: MuxConfig,
    reconnect: impl FnMut() -> ReconnectFut + Send + 'static,
    tasks: &mut JoinSet<MuxError>,
) -> (StreamOpener, StreamAccepter)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
    ReconnectFut: Future<Output = Option<(R, W)>> + Send,
{
    spawn_mux(
        io_reader,
        io_writer,
        config,
        Some(reconnect),
        tasks,
        None,
        None,
    )
}
fn spawn_mux<R, W, ReconnectFut>(
    io_reader: R,
    io_writer: W,
    config: MuxConfig,
    reconnect: Option<impl FnMut() -> ReconnectFut + Send + 'static>,
    tasks: &mut JoinSet<MuxError>,
    first_receive_deadline: Option<Duration>,
    ready_tx: Option<oneshot::Sender<()>>,
) -> (StreamOpener, StreamAccepter)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
    ReconnectFut: Future<Output = Option<(R, W)>> + Send,
{
    let (stream_open_tx, stream_open_rx) = stream_open_channel();
    let (stream_accept_tx, stream_accept_rx) = stream_accept_channel();
    let stream_opener = StreamOpener::new(stream_open_tx);
    let stream_accepter = StreamAccepter::new(stream_accept_rx);
    let stream_init_handle = StreamInitChannels {
        stream_open_rx,
        stream_accept_tx,
    };
    tasks.spawn(async move {
        let (stream_init_handle, err) = run_session_tasks(
            io_reader,
            io_writer,
            &config,
            stream_init_handle,
            first_receive_deadline,
            ready_tx,
        )
        .await;
        let Some(mut reconnect) = reconnect else {
            return err;
        };
        let Some(mut curr_stream_init_handle) = stream_init_handle else {
            return err;
        };
        let mut last_err = err;
        loop {
            let Some((io_reader, io_writer)) = reconnect().await else {
                return last_err;
            };
            let (stream_init_handle, err) = run_session_tasks(
                io_reader,
                io_writer,
                &config,
                curr_stream_init_handle,
                first_receive_deadline,
                None,
            )
            .await;
            last_err = err;
            let Some(stream_init_handle) = stream_init_handle else {
                return last_err;
            };
            curr_stream_init_handle = stream_init_handle;
        }
    });
    (stream_opener, stream_accepter)
}

async fn run_session_tasks<R, W>(
    io_reader: R,
    io_writer: W,
    config: &MuxConfig,
    stream_init_handle: StreamInitChannels,
    first_receive_deadline: Option<Duration>,
    ready_tx: Option<oneshot::Sender<()>>,
) -> (Option<StreamInitChannels>, MuxError)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (write_control_tx, write_control_rx) = write_control_channel();
    let (write_data_tx, write_data_rx) = write_data_channel();
    let (central_io_read_tx, central_io_read_rx) = central_io_read_channel();

    let initiation = config.initiation;
    let frame_reassembly = config.frame_reassembly;
    let mut control_spawner = JoinSet::new();
    control_spawner.spawn(async move {
        let control = MuxControl::new(initiation, write_data_tx, frame_reassembly);
        let args = RunControlArgs {
            control,
            central_io_read_rx,
            write_control_tx,
            stream_init_handle,
        };
        run_control(args).await
    });

    let heartbeat_interval = config.heartbeat_interval;
    let frame_reassembly = config.frame_reassembly;
    let mut central_io_reader_spawner = JoinSet::new();
    central_io_reader_spawner.spawn(async move {
        let central_io_reader = CentralIoReader::new(io_reader, frame_reassembly);
        run_central_io_reader(
            central_io_reader,
            central_io_read_tx,
            heartbeat_interval,
            first_receive_deadline,
            ready_tx,
        )
        .await
    });
    let mut central_io_writer_spawner = JoinSet::new();
    central_io_writer_spawner.spawn(async move {
        let central_io_writer = CentralIoEncoder::new(io_writer, frame_reassembly);
        run_central_io_writer(
            central_io_writer,
            heartbeat_interval,
            write_control_rx,
            write_data_rx,
        )
        .await
    });

    // The control task is the supervisor: it returns Err(DeadCentralIo(...))
    // when the central reader/writer dies, which tells us which side failed.
    let control_err = match join_control(&mut control_spawner).await {
        ControlJoin::Err(e) => e,
        ControlJoin::Stopped => {
            crate::task_scope::abort_and_reap(&mut central_io_reader_spawner).await;
            crate::task_scope::abort_and_reap(&mut central_io_writer_spawner).await;
            return (None, MuxError::TaskStopped { task: "control" });
        }
    };

    let (err, stream_init_handle) = match control_err {
        RunControlError::DeadCentralIo(dead_central_io, stream_init_handle) => {
            let err = match dead_central_io.side {
                Side::Read => match join_central_io_reader(&mut central_io_reader_spawner).await {
                    ReaderJoin::Io(e) => MuxError::IoReader(e),
                    ReaderJoin::Stopped => MuxError::TaskStopped {
                        task: "central_io_reader",
                    },
                    ReaderJoin::ControlChannelClosed => MuxError::ControlChannelClosed {
                        task: "central_io_reader",
                    },
                },
                Side::Write => match join_central_io_writer(&mut central_io_writer_spawner).await {
                    WriterJoin::Io(e) => MuxError::IoWriter(e),
                    WriterJoin::Stopped => MuxError::TaskStopped {
                        task: "central_io_writer",
                    },
                    WriterJoin::ControlChannelClosed => MuxError::ControlChannelClosed {
                        task: "central_io_writer",
                    },
                },
            };
            (err, Some(stream_init_handle))
        }
    };
    // Abort and reap any remaining tasks on the other side so they don't
    // linger — and so a sibling panic that beat the abort still crosses
    // the session boundary instead of being hidden by a bare `abort_all`.
    crate::task_scope::abort_and_reap(&mut central_io_reader_spawner).await;
    crate::task_scope::abort_and_reap(&mut central_io_writer_spawner).await;
    (stream_init_handle, err)
}

#[derive(Debug)]
enum ControlJoin {
    Err(RunControlError),
    Stopped,
}

/// Join the control task. A `JoinError` (panic or cancellation) is
/// unwrapped here so the panic propagates with its original backtrace
/// instead of being silently downgraded to an ordinary error. The
/// supervised tasks only ever complete via `abort_all` (cancellation) —
/// cancellation is surfaced as a panic via `unwrap` rather than
/// downgraded to an ordinary `MuxError`.
async fn join_control(set: &mut JoinSet<Result<(), RunControlError>>) -> ControlJoin {
    match set.join_next().await {
        None => ControlJoin::Stopped,
        Some(Ok(Err(e))) => ControlJoin::Err(e),
        Some(Ok(Ok(()))) => ControlJoin::Stopped,
        Some(result) => {
            let _ = result.unwrap();
            ControlJoin::Stopped
        }
    }
}

#[derive(Debug)]
enum ReaderJoin {
    Io(io::Error),
    Stopped,
    ControlChannelClosed,
}

async fn join_central_io_reader(
    set: &mut JoinSet<Result<(), RunCentralIoReaderError>>,
) -> ReaderJoin {
    match set.join_next().await {
        None => ReaderJoin::Stopped,
        Some(Ok(Err(RunCentralIoReaderError::IoReader(e)))) => ReaderJoin::Io(e),
        Some(Ok(Err(RunCentralIoReaderError::Control(_)))) => ReaderJoin::ControlChannelClosed,
        Some(Ok(Ok(()))) => ReaderJoin::ControlChannelClosed,
        Some(result) => {
            let _ = result.unwrap();
            ReaderJoin::Stopped
        }
    }
}

#[derive(Debug)]
enum WriterJoin {
    Io(io::Error),
    Stopped,
    ControlChannelClosed,
}

async fn join_central_io_writer(
    set: &mut JoinSet<Result<(), RunCentralIoWriterError>>,
) -> WriterJoin {
    match set.join_next().await {
        None => WriterJoin::Stopped,
        Some(Ok(Err(RunCentralIoWriterError::IoWriter(e)))) => WriterJoin::Io(e),
        Some(Ok(Err(RunCentralIoWriterError::Control(_)))) => WriterJoin::ControlChannelClosed,
        Some(Ok(Ok(()))) => WriterJoin::ControlChannelClosed,
        Some(result) => {
            let _ = result.unwrap();
            WriterJoin::Stopped
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Cancellation of the control task is surfaced as a panic via
    // `unwrap` (a cancelled supervisor is not a recoverable event),
    // rather than being downgraded to an ordinary `MuxError`. The panic
    // is caught at the thread boundary with `catch_unwind` to assert it
    // is a `JoinError::Cancelled`.
    #[tokio::test(flavor = "multi_thread")]
    async fn join_control_cancellation_panics() {
        let mut set: JoinSet<Result<(), RunControlError>> = JoinSet::new();
        set.spawn(async move {
            // Forever-pending: never completes on its own.
            std::future::pending::<()>().await;
            Ok(())
        });
        set.abort_all();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(join_control(&mut set))
            })
        }));
        assert!(
            result.is_err(),
            "expected join_control to panic on cancellation"
        );
    }

    // Same scenario for the central IO reader/writer join helpers.
    #[tokio::test(flavor = "multi_thread")]
    async fn join_central_io_reader_cancellation_panics() {
        let mut set: JoinSet<Result<(), RunCentralIoReaderError>> = JoinSet::new();
        set.spawn(async move {
            std::future::pending::<()>().await;
            Ok(())
        });
        set.abort_all();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(join_central_io_reader(&mut set))
            })
        }));
        assert!(
            result.is_err(),
            "expected join_central_io_reader to panic on cancellation"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn join_central_io_writer_cancellation_panics() {
        let mut set: JoinSet<Result<(), RunCentralIoWriterError>> = JoinSet::new();
        set.spawn(async move {
            std::future::pending::<()>().await;
            Ok(())
        });
        set.abort_all();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(join_central_io_writer(&mut set))
            })
        }));
        assert!(
            result.is_err(),
            "expected join_central_io_writer to panic on cancellation"
        );
    }

    // `TaskStopped` when the JoinSet is empty (task never produced a result).
    #[tokio::test(flavor = "multi_thread")]
    async fn join_control_empty_is_task_stopped() {
        let mut set: JoinSet<Result<(), RunControlError>> = JoinSet::new();
        match join_control(&mut set).await {
            ControlJoin::Stopped => (),
            other => panic!("expected ControlJoin::Stopped, got {other:?}"),
        }
    }

    // A panicking control task must surface its panic through `join_control`
    // via `unwrap`. The panic is caught at the thread boundary with
    // `catch_unwind` to assert the payload.
    #[tokio::test(flavor = "multi_thread")]
    async fn join_control_panic_is_resumed() {
        let mut set: JoinSet<Result<(), RunControlError>> = JoinSet::new();
        set.spawn(async move {
            panic!("control boom");
        });
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(join_control(&mut set))
            })
        }));
        match result {
            Err(payload) => {
                let msg = payload
                    .downcast_ref::<&'static str>()
                    .copied()
                    .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                    .unwrap_or("");
                assert!(msg.contains("control boom"), "panic payload: {msg:?}");
            }
            Ok(join) => panic!("expected a resumed panic, got {join:?}"),
        }
    }

    // A panicking central-io reader task must surface its panic through
    // `join_central_io_reader` via `unwrap`.
    #[tokio::test(flavor = "multi_thread")]
    async fn join_central_io_reader_panic_is_resumed() {
        let mut set: JoinSet<Result<(), RunCentralIoReaderError>> = JoinSet::new();
        set.spawn(async move {
            panic!("reader boom");
        });
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(join_central_io_reader(&mut set))
            })
        }));
        match result {
            Err(payload) => {
                let msg = payload
                    .downcast_ref::<&'static str>()
                    .copied()
                    .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                    .unwrap_or("");
                assert!(msg.contains("reader boom"), "panic payload: {msg:?}");
            }
            Ok(join) => panic!("expected a resumed panic, got {join:?}"),
        }
    }

    // A panicking central-io writer task must surface its panic through
    // `join_central_io_writer` via `unwrap`.
    #[tokio::test(flavor = "multi_thread")]
    async fn join_central_io_writer_panic_is_resumed() {
        let mut set: JoinSet<Result<(), RunCentralIoWriterError>> = JoinSet::new();
        set.spawn(async move {
            panic!("writer boom");
        });
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(join_central_io_writer(&mut set))
            })
        }));
        match result {
            Err(payload) => {
                let msg = payload
                    .downcast_ref::<&'static str>()
                    .copied()
                    .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                    .unwrap_or("");
                assert!(msg.contains("writer boom"), "panic payload: {msg:?}");
            }
            Ok(join) => panic!("expected a resumed panic, got {join:?}"),
        }
    }

    // The steady receive deadline must still fire when the peer genuinely
    // goes silent: a lane whose reader sees no frame for
    // `heartbeat_interval * RECEIVE_DEADLINE_INTERVALS` ends with the
    // `receive deadline - session timed out` `IoReader` error.  This pins the
    // deadline's real purpose so a liveness fix elsewhere cannot silently
    // neuter it; `first_receive_deadline_widens_after_birth_heartbeat` is the
    // paired negative control (a live peer keeps the session open).
    #[tokio::test(flavor = "multi_thread")]
    async fn dead_peer_trips_the_receive_deadline() {
        let heartbeat_interval = Duration::from_millis(100);
        // A large duplex buffer holds every heartbeat the local writer emits
        // over the short test window, so the writer never blocks; the peer
        // half is held open but never written to.
        let (server_side, _peer_side) = tokio::io::duplex(1 << 20);
        let (server_r, server_w) = tokio::io::split(server_side);

        let mut spawner: JoinSet<MuxError> = JoinSet::new();
        let (_opener, _accepter) = spawn_mux_no_reconnection(
            server_r,
            server_w,
            MuxConfig::new(Initiation::Server, heartbeat_interval),
            &mut spawner,
        );

        // Steady deadline = heartbeat_interval * RECEIVE_DEADLINE_INTERVALS
        // (4) = 400 ms; the session must end once the peer has been silent
        // for that long.  The generous outer bound keeps the assertion about
        // the error, not the timing.
        let err = tokio::time::timeout(Duration::from_secs(5), spawner.join_next())
            .await
            .expect("the session did not end when the receive deadline elapsed")
            .expect("the session task disappeared")
            .expect("the session task panicked");
        match err {
            MuxError::IoReader(e) => {
                assert_eq!(e.kind(), io::ErrorKind::TimedOut, "wrong error kind: {e:?}");
                assert!(
                    e.to_string().contains("receive deadline"),
                    "wrong error: {e}"
                );
            }
            other => panic!("expected IoReader(TimedOut), got {other:?}"),
        }
        spawner.abort_all();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reconnection_reports_the_latest_failure() {
        use std::pin::Pin;
        use std::task::{Context, Poll};
        use tokio::io::ReadBuf;

        struct FailingReader(Option<io::Error>);

        impl AsyncRead for FailingReader {
            fn poll_read(
                mut self: Pin<&mut Self>,
                _: &mut Context<'_>,
                _: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                Poll::Ready(match self.0.take() {
                    Some(e) => Err(e),
                    None => Ok(()),
                })
            }
        }

        let first = FailingReader(Some(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "first",
        )));
        let mut attempts = 0usize;
        let reconnect = move || {
            attempts += 1;
            let first_try = attempts == 1;
            async move {
                if first_try {
                    Some((
                        FailingReader(Some(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "second",
                        ))),
                        tokio::io::sink(),
                    ))
                } else {
                    None
                }
            }
        };
        let mut tasks: JoinSet<MuxError> = JoinSet::new();
        let (_opener, _accepter) = spawn_mux_with_reconnection(
            first,
            tokio::io::sink(),
            MuxConfig::new(Initiation::Client, Duration::from_secs(5)),
            reconnect,
            &mut tasks,
        );
        let err = tokio::time::timeout(Duration::from_secs(10), tasks.join_next())
            .await
            .expect("the mux task never finished")
            .expect("the mux task disappeared")
            .expect("the mux task panicked");
        match err {
            MuxError::IoReader(e) => assert_eq!(
                e.kind(),
                io::ErrorKind::PermissionDenied,
                "the reconnecting mux reported the first connection's failure, not the last"
            ),
            other => panic!("expected IoReader, got {other:?}"),
        }
    }

    // A failure on the writer half is reported as `IoWriter`, not
    // `IoReader`: the session must name the side that actually failed so a
    // caller (or reconnect decision) can tell a dead peer from a dead sink.
    // The reader is parked forever so only the writer can fail.
    #[tokio::test(flavor = "multi_thread")]
    async fn writer_half_failure_is_reported_as_io_writer() {
        use std::pin::Pin;
        use std::task::{Context, Poll};
        use tokio::io::ReadBuf;

        struct PendingReader;

        impl AsyncRead for PendingReader {
            fn poll_read(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
                _: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                Poll::Pending
            }
        }

        struct FailingWriter(Option<io::Error>);

        impl AsyncWrite for FailingWriter {
            fn poll_write(
                mut self: Pin<&mut Self>,
                _: &mut Context<'_>,
                _: &[u8],
            ) -> Poll<io::Result<usize>> {
                Poll::Ready(Err(self.0.take().unwrap_or_else(|| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "writer already failed")
                })))
            }
            fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }

        let mut tasks: JoinSet<MuxError> = JoinSet::new();
        let (_opener, _accepter) = spawn_mux_no_reconnection(
            PendingReader,
            FailingWriter(Some(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "writer boom",
            ))),
            MuxConfig::new(Initiation::Server, Duration::from_millis(10)),
            &mut tasks,
        );
        let err = tokio::time::timeout(Duration::from_secs(5), tasks.join_next())
            .await
            .expect("the session did not end after the writer failed")
            .expect("the session task disappeared")
            .expect("the session task panicked");
        match err {
            MuxError::IoWriter(e) => assert_eq!(
                e.kind(),
                io::ErrorKind::ConnectionReset,
                "wrong error: {e:?}"
            ),
            other => panic!("a writer-half failure was classified as {other:?}"),
        }
        tasks.abort_all();
    }
}
/// Gate: the memory footprint of an idle mux stream is amortized-constant
/// in the number of open streams and bounded per stream.
///
/// Per the constitution's tier rule this is a **default-tier** gate (a
/// plain `cargo test -p mux` runs it): the measured quantity is a
/// deterministic allocation count over a fixed open sequence on a
/// single-threaded runtime, not wall-clock. The instrument is the lib's
/// own `test_alloc` live-byte counter; a `current_thread` runtime puts
/// every allocation both endpoints of a stream make on the driving test
/// thread, so the reading is exact and isolated from the other tests
/// running in parallel on other threads.
///
/// Bound derivation (measured on the stock tree, debug lib test, three
/// repeated runs): the live bytes held while `S` idle streams are open
/// on a fresh warm session, minus the same session with zero streams
/// open, divided by `S`, reads **10.7-11.6 KiB** per stream at S = 8,
/// 32 and 128 with <= 1.5 % run-to-run variance. The per-stream state is
/// the endpoint pair (client open + server accept) of one idle stream:
/// table entries, the stream channels and the reader/writer state
/// machines on both sides.
///
/// - **floor** `>= 1 KiB/stream`: an open path that holds no per-stream
///   state (e.g. lazy buffer deferral) would read ~0 and every ratio
///   below would pass vacuously; an idle stream must hold real state.
/// - **budget** `<= 24 KiB/stream` (2x the measured band): a change that
///   adds >= ~13 KiB of held per-stream state (say a per-stream 16 KiB
///   buffer) fails, while allocator/ordering variance (~100-200 bytes)
///   cannot trip it.
/// - **amortized-constant** `per_stream(128) <= 3.0 x per_stream(8)`:
///   the measured ratio is ~0.99; a per-stream cost that grows with the
///   number of already-open streams (a linear scan in the open path, an
///   O(S) held structure) reads >= 8 and fails at the 3.0 ceiling
///   (3x headroom over ~1.0).
#[tokio::test(flavor = "current_thread")]
async fn idle_stream_memory_is_amortized_constant_and_budget_bounded() {
    /// Fresh session pair; returns the live bytes held on this thread
    /// while `streams` idle streams are open (pre-allocated handle vecs
    /// so the window only sees stream state, not Vec growth).
    async fn footprint(joins: &mut JoinSet<MuxError>, streams: usize) -> usize {
        let buf = 1 << 20;
        let (c2s, s2c) = tokio::io::duplex(buf);
        let (cli_r, cli_w) = tokio::io::split(c2s);
        let (srv_r, srv_w) = tokio::io::split(s2c);
        let (opener, _) = spawn_mux_no_reconnection(
            srv_r,
            srv_w,
            MuxConfig::new(Initiation::Server, Duration::from_secs(60)),
            joins,
        );
        let (_, mut accepter) = spawn_mux_no_reconnection(
            cli_r,
            cli_w,
            MuxConfig::new(Initiation::Client, Duration::from_secs(60)),
            joins,
        );
        // Warm one stream so the session-level structures settle before
        // the baseline snapshot; the warm stream's own footprint is
        // present on both sides of the window and cancels out.
        let (_warm_r, _warm_w) = opener.open().await.unwrap();
        let (_warm_sr, _warm_sw) = accepter.accept().await.unwrap();
        let mut writers = Vec::with_capacity(streams);
        let mut readers = Vec::with_capacity(streams);
        tokio::task::yield_now().await;

        let before = crate::test_alloc::thread_live_bytes();
        for _ in 0..streams {
            let (_r, w) = opener.open().await.unwrap();
            writers.push(w);
        }
        for _ in 0..streams {
            let (r, _w) = accepter.accept().await.unwrap();
            readers.push(r);
        }
        tokio::task::yield_now().await;
        let after = crate::test_alloc::thread_live_bytes();
        after.saturating_sub(before)
    }

    const FLOOR_BYTES_PER_STREAM: usize = 1024;
    const BUDGET_BYTES_PER_STREAM: usize = 24 * 1024;
    const RATIO_CEILING: f64 = 3.0;

    let mut joins = JoinSet::new();
    let f0 = footprint(&mut joins, 0).await;
    let f8 = footprint(&mut joins, 8).await;
    let f128 = footprint(&mut joins, 128).await;
    joins.abort_all();

    let per_stream_8 = (f8 - f0) / 8;
    let per_stream_128 = (f128 - f0) / 128;

    assert!(
        per_stream_8 >= FLOOR_BYTES_PER_STREAM,
        "idle-stream memory floor: opening a stream holds {per_stream_8} bytes/stream at \
             8 streams, below the {FLOOR_BYTES_PER_STREAM} byte floor; an idle mux stream \
             must hold real state (table entry + endpoint channels), otherwise the scaling \
             ratios below pass vacuously"
    );
    assert!(
        per_stream_128 <= BUDGET_BYTES_PER_STREAM,
        "idle-stream memory budget: {per_stream_128} bytes/stream at 128 streams exceeds \
             the {BUDGET_BYTES_PER_STREAM} byte budget (measured band 10.7-11.6 KiB/stream, \
             the budget is 2x the measured band); a per-stream memory addition of \
             >= ~13 KiB fails"
    );
    let ratio = per_stream_128 as f64 / per_stream_8 as f64;
    assert!(
        ratio <= RATIO_CEILING,
        "idle-stream memory is not amortized-constant: per-stream cost at 128 streams \
             is {ratio:.2}x the 8-stream cost, above the {RATIO_CEILING:.1}x ceiling \
             (measured ~0.99); per-stream memory must not grow with the number of open \
             streams"
    );
}
