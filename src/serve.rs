use std::{future::Future, io, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncWrite},
    task::JoinSet,
};

use crate::{
    central_io::{
        reader::{
            central_io_read_channel, run_central_io_reader, CentralIoReader,
            RunCentralIoReaderError,
        },
        writer::{
            run_central_io_writer, write_control_channel, write_data_channel, CentralIoWriter,
            RunCentralIoWriterError,
        },
    },
    common::Side,
    control::{run_control, Initiation, MuxControl, RunControlArgs, RunControlError},
    stream::{
        accepter::stream_accept_channel,
        opener::{stream_open_channel, StreamOpener},
        StreamInitHandle,
    },
    StreamAccepter,
};

#[derive(Debug, Clone)]
pub struct MuxConfig {
    pub initiation: Initiation,
    pub heartbeat_interval: Duration,
}

#[derive(Debug)]
pub enum MuxError {
    IoReader(io::Error),
    IoWriter(io::Error),
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
    spawn_mux(io_reader, io_writer, config, reconnect, spawner)
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
    spawn_mux(io_reader, io_writer, config, Some(reconnect), spawner)
}
fn spawn_mux<R, W, ReconnectFut>(
    io_reader: R,
    io_writer: W,
    config: MuxConfig,
    reconnect: Option<impl FnMut() -> ReconnectFut + Send + 'static>,
    spawner: &mut JoinSet<MuxError>,
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
        let (stream_init_handle, err) =
            run_services(io_reader, io_writer, &config, stream_init_handle).await;
        let Some(mut reconnect) = reconnect else {
            return err;
        };
        let Some(mut curr_stream_init_handle) = stream_init_handle else {
            return err;
        };
        loop {
            let Some((io_reader, io_writer)) = reconnect().await else {
                return err;
            };
            let (stream_init_handle, err) =
                run_services(io_reader, io_writer, &config, curr_stream_init_handle).await;
            let Some(stream_init_handle) = stream_init_handle else {
                return err;
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
) -> (Option<StreamInitHandle>, MuxError)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (write_control_tx, write_control_rx) = write_control_channel();
    let (write_data_tx, write_data_rx) = write_data_channel();
    let (central_io_read_tx, central_io_read_rx) = central_io_read_channel();

    let initiation = config.initiation;
    let mut control_spawner = JoinSet::new();
    control_spawner.spawn(async move {
        let control = MuxControl::new(initiation, write_data_tx);
        let args = RunControlArgs {
            control,
            central_io_read_rx,
            write_control_tx,
            stream_init_handle,
        };
        run_control(args).await
    });

    let mut central_io_reader_spawner = JoinSet::new();
    central_io_reader_spawner.spawn(async move {
        let central_io_reader = CentralIoReader::new(io_reader);
        run_central_io_reader(central_io_reader, central_io_read_tx).await
    });

    let heartbeat_interval = config.heartbeat_interval;
    let mut central_io_writer_spawner = JoinSet::new();
    central_io_writer_spawner.spawn(async move {
        let central_io_writer = CentralIoWriter::new(io_writer);
        run_central_io_writer(
            central_io_writer,
            heartbeat_interval,
            write_control_rx,
            write_data_rx,
        )
        .await
    });

    let control_res = control_spawner.join_next().await.unwrap().unwrap();
    let control_err = control_res.unwrap_err();

    let err = match &control_err {
        RunControlError::DeadCentralIo(dead_central_io, _) => match dead_central_io.side {
            Side::Read => {
                let err = central_io_reader_spawner
                    .join_next()
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap_err();
                let RunCentralIoReaderError::IoReader(e) = err else {
                    panic!();
                };
                MuxError::IoReader(e)
            }
            Side::Write => {
                let err = central_io_writer_spawner
                    .join_next()
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap_err();
                let RunCentralIoWriterError::IoWriter(e) = err else {
                    panic!();
                };
                MuxError::IoWriter(e)
            }
        },
    };
    let stream_init_handle = match control_err {
        RunControlError::DeadCentralIo(_, stream_init_handle) => Some(stream_init_handle),
    };
    (stream_init_handle, err)
}
