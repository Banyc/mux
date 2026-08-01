use std::{future::Future, io, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::oneshot,
    task::{JoinError, JoinSet},
};

use crate::{
    StreamAccepter,
    central_io::{
        reader::{
            CentralIoReader, RunCentralIoReaderError, central_io_read_channel,
            run_central_io_reader,
        },
        writer::{
            CentralIoWriter, RunCentralIoWriterError, run_central_io_writer, write_control_channel,
            write_data_channel,
        },
    },
    common::Side,
    control::{Initiation, MuxControl, RunControlArgs, RunControlError, run_control},
    stream::{
        StreamInitHandle,
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
    /// complete frames out of order (e.g. `rtp`'s frame-delivery mode).
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
        lane: crate::dual_lane::LaneClass,
        peer_lane_aborted: bool,
        source: Box<MuxError>,
    },
    TaskJoin {
        task: &'static str,
        source: JoinError,
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
    spawner: &mut JoinSet<MuxError>,
) -> (StreamOpener, StreamAccepter)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    async fn nah<R, W>() -> Option<(R, W)> {
        unreachable!()
    }
    #[allow(unused_assignments)]
    let mut reconnect = Some(nah);
    reconnect = None;
    spawn_mux(io_reader, io_writer, config, reconnect, spawner, None, None)
}

/// Like [`spawn_mux_no_reconnection`] but enforces a shorter
/// `first_receive_deadline` until the first frame arrives, then
/// switches to the steady receive deadline derived from the
/// heartbeat interval. Use [`crate::write_birth_heartbeat`] on the
/// paired transport to prove lane liveness before any app data flows.
pub fn spawn_mux_no_reconnection_with_first_receive_deadline<R, W>(
    io_reader: R,
    io_writer: W,
    config: MuxConfig,
    first_receive_deadline: Duration,
    spawner: &mut JoinSet<MuxError>,
) -> (StreamOpener, StreamAccepter)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    async fn nah<R, W>() -> Option<(R, W)> {
        unreachable!()
    }
    #[allow(unused_assignments)]
    let mut reconnect = Some(nah);
    reconnect = None;
    spawn_mux(
        io_reader,
        io_writer,
        config,
        reconnect,
        spawner,
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
    spawner: &mut JoinSet<MuxError>,
) -> (StreamOpener, StreamAccepter, oneshot::Receiver<()>)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    async fn nah<R, W>() -> Option<(R, W)> {
        unreachable!()
    }
    #[allow(unused_assignments)]
    let mut reconnect = Some(nah);
    reconnect = None;
    let (ready_tx, ready_rx) = oneshot::channel();
    let (opener, accepter) = spawn_mux(
        io_reader,
        io_writer,
        config,
        reconnect,
        spawner,
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
    spawner: &mut JoinSet<MuxError>,
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
        spawner,
        None,
        None,
    )
}
fn spawn_mux<R, W, ReconnectFut>(
    io_reader: R,
    io_writer: W,
    config: MuxConfig,
    reconnect: Option<impl FnMut() -> ReconnectFut + Send + 'static>,
    spawner: &mut JoinSet<MuxError>,
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
    let stream_init_handle = StreamInitHandle {
        stream_open_rx,
        stream_accept_tx,
    };
    spawner.spawn(async move {
        let (stream_init_handle, err) = run_services(
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
            let (stream_init_handle, err) = run_services(
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

async fn run_services<R, W>(
    io_reader: R,
    io_writer: W,
    config: &MuxConfig,
    stream_init_handle: StreamInitHandle,
    first_receive_deadline: Option<Duration>,
    ready_tx: Option<oneshot::Sender<()>>,
) -> (Option<StreamInitHandle>, MuxError)
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
        let central_io_writer = CentralIoWriter::new(io_writer, frame_reassembly);
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
        ControlJoin::Cancelled(e) => {
            // The control task was cancelled (e.g. abort_all during reset).
            // Tear down the remaining tasks and report the cancellation
            // instead of panicking.
            central_io_reader_spawner.abort_all();
            central_io_writer_spawner.abort_all();
            return (
                None,
                MuxError::TaskJoin {
                    task: "control",
                    source: e,
                },
            );
        }
        ControlJoin::Stopped => {
            central_io_reader_spawner.abort_all();
            central_io_writer_spawner.abort_all();
            return (None, MuxError::TaskStopped { task: "control" });
        }
    };

    let (err, stream_init_handle) = match control_err {
        RunControlError::DeadCentralIo(dead_central_io, stream_init_handle) => {
            let err = match dead_central_io.side {
                Side::Read => match join_central_io_reader(&mut central_io_reader_spawner).await {
                    ReaderJoin::Io(e) => MuxError::IoReader(e),
                    ReaderJoin::Cancelled(e) => MuxError::TaskJoin {
                        task: "central_io_reader",
                        source: e,
                    },
                    ReaderJoin::Stopped => MuxError::TaskStopped {
                        task: "central_io_reader",
                    },
                    ReaderJoin::ControlChannelClosed => MuxError::ControlChannelClosed {
                        task: "central_io_reader",
                    },
                },
                Side::Write => match join_central_io_writer(&mut central_io_writer_spawner).await {
                    WriterJoin::Io(e) => MuxError::IoWriter(e),
                    WriterJoin::Cancelled(e) => MuxError::TaskJoin {
                        task: "central_io_writer",
                        source: e,
                    },
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
    // Abort any remaining tasks on the other side so they don't linger.
    central_io_reader_spawner.abort_all();
    central_io_writer_spawner.abort_all();
    (stream_init_handle, err)
}

#[derive(Debug)]
enum ControlJoin {
    Err(RunControlError),
    Cancelled(JoinError),
    Stopped,
}

/// Join the control task. Maps `JoinError::Cancelled` to
/// [`ControlJoin::Cancelled`] instead of unwrapping/panicking.
async fn join_control(set: &mut JoinSet<Result<(), RunControlError>>) -> ControlJoin {
    match set.join_next().await {
        None => ControlJoin::Stopped,
        Some(Ok(Err(e))) => ControlJoin::Err(e),
        Some(Ok(Ok(()))) => ControlJoin::Stopped,
        Some(Err(e)) => ControlJoin::Cancelled(e),
    }
}

#[derive(Debug)]
enum ReaderJoin {
    Io(io::Error),
    Cancelled(JoinError),
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
        Some(Err(e)) => ReaderJoin::Cancelled(e),
    }
}

#[derive(Debug)]
enum WriterJoin {
    Io(io::Error),
    Cancelled(JoinError),
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
        Some(Err(e)) => WriterJoin::Cancelled(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Regression test for the supervision panic on cancellation.
    //
    // Before the fix, `run_services` (and `build_opener` in the proxy)
    // unwrapped the `JoinSet::join_next` result, which is an `Err` of kind
    // `JoinError::Cancelled` when the task is aborted via `abort_all`.
    // A normal shutdown/reset cancels child tasks, so the old supervisor
    // code panicked on a routine lifecycle event.
    //
    // This test spawns a forever-pending task, aborts it, then joins through
    // the control-join helper and asserts that cancellation surfaces as
    // `MuxError::TaskJoin` with `source.is_cancelled()`, with no panic.
    #[tokio::test(flavor = "multi_thread")]
    async fn join_control_cancellation_is_not_fatal() {
        let mut set: JoinSet<Result<(), RunControlError>> = JoinSet::new();
        set.spawn(async move {
            // Forever-pending: never completes on its own.
            std::future::pending::<()>().await;
            Ok(())
        });
        set.abort_all();
        match join_control(&mut set).await {
            ControlJoin::Cancelled(e) => assert!(e.is_cancelled()),
            other => panic!("expected ControlJoin::Cancelled, got {other:?}"),
        }
    }

    // Same scenario for the central IO reader/writer join helpers.
    #[tokio::test(flavor = "multi_thread")]
    async fn join_central_io_reader_cancellation_is_not_fatal() {
        let mut set: JoinSet<Result<(), RunCentralIoReaderError>> = JoinSet::new();
        set.spawn(async move {
            std::future::pending::<()>().await;
            Ok(())
        });
        set.abort_all();
        match join_central_io_reader(&mut set).await {
            ReaderJoin::Cancelled(e) => assert!(e.is_cancelled()),
            other => panic!("expected ReaderJoin::Cancelled, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn join_central_io_writer_cancellation_is_not_fatal() {
        let mut set: JoinSet<Result<(), RunCentralIoWriterError>> = JoinSet::new();
        set.spawn(async move {
            std::future::pending::<()>().await;
            Ok(())
        });
        set.abort_all();
        match join_central_io_writer(&mut set).await {
            WriterJoin::Cancelled(e) => assert!(e.is_cancelled()),
            other => panic!("expected WriterJoin::Cancelled, got {other:?}"),
        }
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
}
