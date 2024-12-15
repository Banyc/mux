use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use primitive::ops::ring::RingSpace;

use crate::{
    central_io::{
        reader::{CentralIoReadMsg, CentralIoReadRx},
        writer::WriteDataMsg,
        DataBuf, DeadCentralIo,
    },
    common::Side,
    protocol::StreamId,
    stream::{
        accepter::StreamAcceptMsg,
        opener::StreamOpenMsg,
        reader::{stream_read_data_channel, StreamReadDataMsg, StreamReadDataTx},
        stream_close_channel,
        writer::{WriteControlMsg, WriteControlTx},
        DeadStream, DeadStreamInit, StreamCloseTxPrototype, StreamInitHandle,
    },
    StreamReader, StreamWriter,
};

const CHANNEL_SIZE: usize = 1024;

#[derive(Debug)]
pub struct RunControlArgs {
    pub control: MuxControl,
    pub central_io_read_rx: CentralIoReadRx,
    pub write_control_tx: WriteControlTx,
    pub stream_init_handle: StreamInitHandle,
}
pub async fn run_control(args: RunControlArgs) -> Result<(), RunControlError> {
    let RunControlArgs {
        mut control,
        mut central_io_read_rx,
        write_control_tx,
        mut stream_init_handle,
    } = args;
    let (stream_close_tx, mut stream_close_rx) = stream_close_channel();
    let e: DeadCentralIo = loop {
        tokio::select! {
            () = write_control_tx.closed() => {
                break DeadCentralIo { side: Side::Write };
            }
            res = stream_close_rx.recv() => {
                let msg = res.unwrap();
                control.local_close(msg.stream_id, msg.side);
                let control_msg = WriteControlMsg::Close(msg.stream_id, msg.side);
                if let Err(e) = write_control_tx.send(control_msg).await {
                    break e;
                };
            }
            Ok(msg) = stream_init_handle.stream_open_rx.recv() => {
                let stream_id = match handle_local_open(&mut control, &stream_close_tx, msg) {
                    Ok(stream_id) => stream_id,
                    Err(DeadStreamInit {}) => continue,
                };
                if let Some(stream_id) = stream_id {
                    let control_msg = WriteControlMsg::Open(stream_id);
                    if let Err(e) = write_control_tx.send(control_msg).await {
                        break e;
                    };
                }
            }
            res = central_io_read_rx.recv() => {
                let msg = match res {
                    Ok(x) => x,
                    Err(e) => break e,
                };
                if let Err(DeadStreamInit {}) = handle_central_read(
                    &mut control,
                    &stream_close_tx,
                    &mut stream_init_handle,
                    msg
                ).await {
                    continue;
                }
            }
        }
    };
    Err(RunControlError::DeadCentralIo(e, stream_init_handle))
}
#[derive(Debug)]
pub enum RunControlError {
    DeadCentralIo(DeadCentralIo, StreamInitHandle),
}

fn handle_local_open(
    control: &mut MuxControl,
    stream_close_tx: &StreamCloseTxPrototype,
    msg: StreamOpenMsg,
) -> Result<Option<StreamId>, DeadStreamInit> {
    let res = open_stream(control, stream_close_tx, None);
    let (resp, stream_id) = match res {
        Ok((stream_id, msg)) => (Ok(msg), Some(stream_id)),
        Err(e) => (Err(e), None),
    };
    msg.stream.send(resp).map_err(|_| DeadStreamInit {})?;
    Ok(stream_id)
}
async fn handle_central_read(
    control: &mut MuxControl,
    stream_close_tx: &StreamCloseTxPrototype,
    stream_init_handle: &mut StreamInitHandle,
    msg: CentralIoReadMsg,
) -> Result<(), DeadStreamInit> {
    match msg {
        CentralIoReadMsg::Open(stream_id) => {
            let (_, stream) = open_stream(control, stream_close_tx, Some(stream_id)).unwrap();
            stream_init_handle.stream_accept_tx.send(stream).await?;
        }
        CentralIoReadMsg::Close(stream_id, side) => {
            control.peer_close(stream_id, side).await;
        }
        CentralIoReadMsg::Data(stream_id, data_buf) => {
            let Some(dispatcher) = control.dispatcher(stream_id) else {
                return Ok(());
            };
            let msg = StreamReadDataMsg::Data(data_buf);
            let _ = dispatcher.send(msg).await;
        }
    }
    Ok(())
}
fn open_stream(
    control: &mut MuxControl,
    stream_close_tx: &StreamCloseTxPrototype,
    stream_id: Option<StreamId>,
) -> Result<(StreamId, StreamAcceptMsg), TooManyOpenStreams> {
    let write_broken_pipe = WriteBrokenPipe::new();
    let (stream_read_data_tx, stream_read_data_rx) = stream_read_data_channel();
    let (stream_id, stream_write_data_tx) =
        control.open(stream_read_data_tx, write_broken_pipe.clone(), stream_id)?;
    let stream_reader = StreamReader::new(
        stream_read_data_rx,
        stream_close_tx.derive(Side::Read, stream_id),
    );
    let stream_writer = StreamWriter::new(
        stream_write_data_tx,
        write_broken_pipe,
        stream_close_tx.derive(Side::Write, stream_id),
    );
    let msg = StreamAcceptMsg {
        reader: stream_reader,
        writer: stream_writer,
    };
    Ok((stream_id, msg))
}

#[derive(Debug)]
pub struct MuxControl {
    stream_table: HashMap<StreamId, StreamState>,
    local_opened_streams: usize,
    initiation: Initiation,
    next_possible_local_stream_id: StreamId,
    write_data_tx: WriteDataTxPrototype,
}
impl MuxControl {
    pub fn new(initiation: Initiation, write_data_tx: WriteDataTxPrototype) -> Self {
        Self {
            stream_table: HashMap::new(),
            local_opened_streams: 0,
            initiation,
            next_possible_local_stream_id: 0,
            write_data_tx,
        }
    }
    fn is_local_opened_stream(&self, stream_id: StreamId) -> bool {
        let is_first_bit_set = stream_id >> (StreamId::BITS - 1) == 1;
        match self.initiation {
            Initiation::Server => is_first_bit_set,
            Initiation::Client => !is_first_bit_set,
        }
    }
    fn should_stream_id_set_first_bit(&self) -> bool {
        match self.initiation {
            Initiation::Server => true,
            Initiation::Client => false,
        }
    }
    fn next_stream_id(&mut self) -> Result<StreamId, TooManyOpenStreams> {
        let max_local_stream_id = StreamId::MAX >> 1;
        if usize::try_from(max_local_stream_id).unwrap() <= self.local_opened_streams {
            return Err(TooManyOpenStreams {});
        }
        let mut next_local_stream_id = self.next_possible_local_stream_id;
        let local_stream_id = loop {
            if !self.stream_table.contains_key(&next_local_stream_id) {
                break next_local_stream_id;
            }
            next_local_stream_id = next_local_stream_id.ring_add(1, max_local_stream_id);
        };
        self.next_possible_local_stream_id = local_stream_id.ring_add(1, max_local_stream_id);
        let stream_id = if self.should_stream_id_set_first_bit() {
            local_stream_id | (1 << (StreamId::BITS - 1))
        } else {
            local_stream_id
        };
        Ok(stream_id)
    }
    pub fn local_close(&mut self, stream_id: StreamId, side: Side) {
        let Some(stream) = self.stream_table.get_mut(&stream_id) else {
            return;
        };
        stream.local_close(side);
        if stream.is_closed() {
            self.clean_closed_stream(stream_id);
        }
    }
    pub async fn peer_close(&mut self, stream_id: StreamId, side: Side) {
        let Some(stream) = self.stream_table.get_mut(&stream_id) else {
            return;
        };
        let _ = stream.peer_close(side).await;
        if stream.is_closed() {
            self.clean_closed_stream(stream_id);
        }
    }
    fn clean_closed_stream(&mut self, stream_id: StreamId) {
        if self.is_local_opened_stream(stream_id) {
            self.local_opened_streams -= 1;
        }
        self.stream_table.remove(&stream_id);
    }
    pub fn dispatcher(&self, stream_id: StreamId) -> Option<&StreamReadDataTx> {
        self.stream_table.get(&stream_id)?.dispatcher()
    }
    pub fn open(
        &mut self,
        dispatcher: StreamReadDataTx,
        broken_pipe: WriteBrokenPipe,
        stream_id: Option<StreamId>,
    ) -> Result<(StreamId, StreamWriteDataTx), TooManyOpenStreams> {
        let stream_id = match stream_id {
            Some(stream_id) => stream_id,
            None => self.next_stream_id()?,
        };
        if self.is_local_opened_stream(stream_id) {
            self.local_opened_streams += 1;
        }
        let stream = StreamState::new(dispatcher, broken_pipe);
        self.stream_table.insert(stream_id, stream);
        Ok((stream_id, self.write_data_tx.derive(stream_id)))
    }
}

#[derive(Debug)]
struct StreamState {
    is_write_closed: bool,
    is_read_closed: bool,
    read_dispatcher: StreamReadDataTx,
    is_peer_write_closed: bool,
    write_broken_pipe: WriteBrokenPipe,
}
impl StreamState {
    pub fn new(read_dispatcher: StreamReadDataTx, write_broken_pipe: WriteBrokenPipe) -> Self {
        Self {
            is_write_closed: false,
            is_read_closed: false,
            read_dispatcher,
            is_peer_write_closed: false,
            write_broken_pipe,
        }
    }
    pub fn local_close(&mut self, side: Side) {
        match side {
            Side::Read => self.is_read_closed = true,
            Side::Write => self.is_write_closed = true,
        }
    }
    pub async fn peer_close(&mut self, side: Side) -> Result<(), DeadStream> {
        match side {
            Side::Read => {
                self.write_broken_pipe.close();
            }
            Side::Write => {
                if self.is_peer_write_closed {
                    return Ok(());
                }
                self.is_peer_write_closed = true;
                self.read_dispatcher.send(StreamReadDataMsg::Fin).await?;
            }
        }
        Ok(())
    }
    pub fn dispatcher(&self) -> Option<&StreamReadDataTx> {
        if self.is_peer_write_closed {
            return None;
        }
        Some(&self.read_dispatcher)
    }
    pub fn is_closed(&self) -> bool {
        self.is_write_closed
            && self.is_read_closed
            && self.is_peer_write_closed
            && self.write_broken_pipe.is_closed()
    }
}
impl Drop for StreamState {
    fn drop(&mut self) {
        self.write_broken_pipe.close();
    }
}

pub fn write_data_channel() -> (WriteDataTxPrototype, WriteDataRx) {
    let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_SIZE);
    let tx = WriteDataTxPrototype { tx };
    let rx = WriteDataRx { rx };
    (tx, rx)
}
#[derive(Debug)]
pub struct WriteDataRx {
    rx: tokio::sync::mpsc::Receiver<WriteDataMsg>,
}
impl WriteDataRx {
    pub async fn recv(&mut self) -> Result<WriteDataMsg, DeadControl> {
        self.rx.recv().await.ok_or(DeadControl {})
    }
}
#[derive(Debug, Clone)]
pub struct WriteDataTxPrototype {
    tx: tokio::sync::mpsc::Sender<WriteDataMsg>,
}
impl WriteDataTxPrototype {
    pub fn derive(&self, stream: StreamId) -> StreamWriteDataTx {
        StreamWriteDataTx {
            tx: self.tx.clone(),
            stream_id: stream,
        }
    }
}
#[derive(Debug, Clone)]
pub struct StreamWriteDataTx {
    stream_id: StreamId,
    tx: tokio::sync::mpsc::Sender<WriteDataMsg>,
}
impl StreamWriteDataTx {
    pub async fn send(&self, data: DataBuf) -> Result<(), DeadCentralIo> {
        let msg = WriteDataMsg {
            stream_id: self.stream_id,
            data,
        };
        self.tx
            .send(msg)
            .await
            .map_err(|_| DeadCentralIo { side: Side::Write })
    }
}

#[derive(Debug, Clone)]
pub struct WriteBrokenPipe {
    should_write_close: Arc<AtomicBool>,
}
impl WriteBrokenPipe {
    pub fn new() -> Self {
        Self {
            should_write_close: Arc::new(AtomicBool::new(false)),
        }
    }
    pub fn is_closed(&self) -> bool {
        self.should_write_close.load(Ordering::Relaxed)
    }
    pub fn close(&self) {
        self.should_write_close.store(true, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
pub struct DeadControl {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Initiation {
    Server,
    Client,
}

#[derive(Debug)]
pub struct TooManyOpenStreams {}
