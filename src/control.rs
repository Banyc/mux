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
        reader::CentralIoReadMsg,
        writer::{WriteControlMsg, WriteDataMsg},
        DataBuf,
    },
    protocol::StreamId,
};

const CHANNEL_SIZE: usize = 1024;

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
    pub fn streams(&self) -> usize {
        self.stream_table.len()
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
    fn next_stream_id(&mut self) -> Option<StreamId> {
        let max_local_stream_id = StreamId::MAX >> 1;
        if usize::try_from(max_local_stream_id).unwrap() <= self.local_opened_streams {
            return None;
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
        Some(stream_id)
    }
    pub async fn close(&mut self, stream_id: StreamId, end: End, side: Side) {
        let Some(stream) = self.stream_table.get_mut(&stream_id) else {
            return;
        };
        stream.close(end, side).await;
        if stream.is_closed() {
            if self.is_local_opened_stream(stream_id) {
                self.local_opened_streams -= 1;
            }
            self.stream_table.remove(&stream_id);
        }
    }
    pub fn dispatcher(&self, stream_id: StreamId) -> Option<&StreamReadDataTx> {
        self.stream_table.get(&stream_id)?.dispatcher()
    }
    pub fn open(
        &mut self,
        dispatcher: StreamReadDataTx,
        broken_pipe: WriteBrokenPipe,
        stream_id: Option<StreamId>,
    ) -> Option<StreamWriteDataTx> {
        let stream_id = match stream_id {
            Some(stream_id) => stream_id,
            None => self.next_stream_id()?,
        };
        if self.is_local_opened_stream(stream_id) {
            self.local_opened_streams += 1;
        }
        let stream = StreamState::new(dispatcher, broken_pipe);
        self.stream_table.insert(stream_id, stream);
        Some(self.write_data_tx.derive(stream_id))
    }
}

#[derive(Debug)]
struct StreamState {
    is_write_closed: bool,
    is_read_closed: bool,
    read_dispatcher: StreamReadDataTx,
    is_peer_read_closed: bool,
    write_broken_pipe: WriteBrokenPipe,
}
impl StreamState {
    pub fn new(read_dispatcher: StreamReadDataTx, write_broken_pipe: WriteBrokenPipe) -> Self {
        Self {
            is_write_closed: false,
            is_read_closed: false,
            read_dispatcher,
            is_peer_read_closed: false,
            write_broken_pipe,
        }
    }
    pub async fn close(&mut self, end: End, side: Side) {
        match (end, side) {
            (End::Local, Side::Read) => self.is_read_closed = true,
            (End::Local, Side::Write) => self.is_write_closed = true,
            (End::Peer, Side::Read) => {
                if self.is_peer_read_closed {
                    return;
                }
                self.is_peer_read_closed = true;
                let _ = self.read_dispatcher.send(StreamReadDataMsg::Fin).await;
            }
            (End::Peer, Side::Write) => {
                self.write_broken_pipe.close();
            }
        }
    }
    pub fn is_write_closed(&self) -> bool {
        self.is_write_closed
    }
    pub fn dispatcher(&self) -> Option<&StreamReadDataTx> {
        if self.is_peer_read_closed {
            return None;
        }
        Some(&self.read_dispatcher)
    }
    pub fn is_closed(&self) -> bool {
        self.is_write_closed
            && self.is_read_closed
            && self.is_peer_read_closed
            && self.write_broken_pipe.is_closed()
    }
}
impl Drop for StreamState {
    fn drop(&mut self) {
        self.write_broken_pipe.close();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Read,
    Write,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum End {
    Local,
    Peer,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Initiation {
    Server,
    Client,
}

#[derive(Debug, Clone)]
pub struct CentralReadTx {
    tx: tokio::sync::mpsc::Sender<CentralIoReadMsg>,
}
impl CentralReadTx {
    pub async fn send(&self, msg: CentralIoReadMsg) -> Result<(), DeadControl> {
        self.tx.send(msg).await.map_err(|_| DeadControl {})
    }
}
#[derive(Debug)]
pub struct CentralReadRx {
    rx: tokio::sync::mpsc::Receiver<CentralIoReadMsg>,
}
impl CentralReadRx {
    pub async fn recv(&mut self) -> Result<CentralIoReadMsg, DeadCentralIo> {
        self.rx.recv().await.ok_or(DeadCentralIo {})
    }
}

#[derive(Debug)]
pub enum StreamReadDataMsg {
    Fin,
    Data(DataBuf),
}
pub fn stream_read_data_channel() -> (StreamReadDataTx, StreamReadDataRx) {
    let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_SIZE);
    let tx = StreamReadDataTx { tx };
    let rx = StreamReadDataRx { rx };
    (tx, rx)
}
#[derive(Debug, Clone)]
pub struct StreamReadDataTx {
    tx: tokio::sync::mpsc::Sender<StreamReadDataMsg>,
}
impl StreamReadDataTx {
    pub async fn send(&self, msg: StreamReadDataMsg) -> Result<(), DeadControl> {
        self.tx.send(msg).await.map_err(|_| DeadControl {})
    }
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}
#[derive(Debug)]
pub struct StreamReadDataRx {
    rx: tokio::sync::mpsc::Receiver<StreamReadDataMsg>,
}
impl StreamReadDataRx {
    pub async fn recv(&mut self) -> Result<StreamReadDataMsg, DeadCentralIo> {
        self.rx.recv().await.ok_or(DeadCentralIo {})
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
        self.tx.send(msg).await.map_err(|_| DeadCentralIo {})
    }
    pub fn stream_id(&self) -> StreamId {
        self.stream_id
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
pub struct WriteControlTx {
    tx: tokio::sync::mpsc::Sender<WriteControlMsg>,
}
impl WriteControlTx {
    pub async fn send(&self, msg: WriteControlMsg) -> Result<(), DeadCentralIo> {
        self.tx.send(msg).await.map_err(|_| DeadCentralIo {})
    }
}
#[derive(Debug)]
pub struct WriteControlRx {
    rx: tokio::sync::mpsc::Receiver<WriteControlMsg>,
}
impl WriteControlRx {
    pub async fn recv(&mut self) -> Result<WriteControlMsg, DeadControl> {
        self.rx.recv().await.ok_or(DeadControl {})
    }
}

#[derive(Debug)]
pub struct StreamCloseMsg {
    pub side: Side,
    pub stream_id: StreamId,
}
pub fn stream_close_channel() -> (StreamCloseTxPrototype, StreamCloseRx) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let tx = StreamCloseTxPrototype { tx };
    let rx = StreamCloseRx { rx };
    (tx, rx)
}
#[derive(Debug, Clone)]
pub struct StreamCloseTxPrototype {
    tx: tokio::sync::mpsc::UnboundedSender<StreamCloseMsg>,
}
impl StreamCloseTxPrototype {
    pub fn derive(&self, side: Side, stream_id: StreamId) -> StreamCloseTx {
        StreamCloseTx {
            stream_id,
            side,
            tx: self.tx.clone(),
        }
    }
}
#[derive(Debug, Clone)]
pub struct StreamCloseTx {
    stream_id: StreamId,
    side: Side,
    tx: tokio::sync::mpsc::UnboundedSender<StreamCloseMsg>,
}
impl Drop for StreamCloseTx {
    fn drop(&mut self) {
        let msg = StreamCloseMsg {
            stream_id: self.stream_id,
            side: self.side,
        };
        let _ = self.tx.send(msg);
    }
}
#[derive(Debug)]
pub struct StreamCloseRx {
    rx: tokio::sync::mpsc::UnboundedReceiver<StreamCloseMsg>,
}
impl StreamCloseRx {
    pub async fn recv(&mut self) -> Result<StreamCloseMsg, DeadStream> {
        self.rx.recv().await.ok_or(DeadStream {})
    }
}

#[derive(Debug, Clone)]
pub struct DeadCentralIo {}

#[derive(Debug, Clone)]
pub struct DeadControl {}

#[derive(Debug, Clone)]
pub struct DeadStream {}
