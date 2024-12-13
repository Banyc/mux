use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use crate::{
    central_io::{reader::CentralIoReadMsg, DataBuf},
    protocol::StreamId,
};

const CHANNEL_SIZE: usize = 1024;

#[derive(Debug)]
pub struct MuxControl {
    stream_table: HashMap<StreamId, StreamState>,
    up_tx: WriteDataTxPrototype,
    next_possible_stream_id: StreamId,
}
impl MuxControl {
    pub fn new(up_tx: WriteDataTxPrototype) -> Self {
        Self {
            stream_table: HashMap::new(),
            up_tx,
            next_possible_stream_id: 0,
        }
    }
    pub fn streams(&self) -> usize {
        self.stream_table.len()
    }
    pub async fn close(&mut self, stream_id: StreamId, end: End, side: Side) {
        let Some(stream) = self.stream_table.get_mut(&stream_id) else {
            return;
        };
        stream.close(end, side).await;
        if stream.is_closed() {
            self.stream_table.remove(&stream_id);
        }
    }
    pub fn dispatcher(&self, stream: StreamId) -> Option<&StreamReadDataTx> {
        self.stream_table.get(&stream)?.dispatcher()
    }
    fn next_stream_id(&mut self) -> Option<StreamId> {
        if usize::try_from(StreamId::MAX).unwrap() <= self.stream_table.len() {
            return None;
        }
        let mut next_stream_id = self.next_possible_stream_id;
        let stream_id = loop {
            if !self.stream_table.contains_key(&next_stream_id) {
                break next_stream_id;
            }
            next_stream_id = next_stream_id.wrapping_add(1);
        };
        self.next_possible_stream_id = stream_id.wrapping_add(1);
        Some(stream_id)
    }
    pub fn open(
        &mut self,
        dispatcher: StreamReadDataTx,
        broken_pipe: WriteBrokenPipe,
    ) -> Option<StreamWriteDataTx> {
        let stream_id = self.next_stream_id()?;
        let stream = StreamState::new(dispatcher, broken_pipe);
        self.stream_table.insert(stream_id, stream);
        Some(self.up_tx.derive(stream_id))
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

#[derive(Debug)]
pub struct WriteDataMsg {
    pub stream_id: StreamId,
    pub data: DataBuf,
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
    pub async fn recv(&mut self) -> Result<WriteDataMsg, DeadStream> {
        self.rx.recv().await.ok_or(DeadStream {})
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
    pub async fn send(&self, pkt: DataBuf) -> Result<(), DeadCentralIo> {
        let msg = WriteDataMsg {
            stream_id: self.stream_id,
            data: pkt,
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
pub enum WriteControlMsg {
    Open(StreamId),
    CloseRead(StreamId),
    CloseWrite(StreamId),
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
