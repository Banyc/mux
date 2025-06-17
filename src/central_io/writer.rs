use std::{collections::HashMap, io, time::Duration};

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{
    common::Side,
    control::DeadControl,
    fair_queue,
    protocol::{BodyLen, DataHeader, Header, StreamId, StreamIdMsg},
};

use super::{DataBuf, DeadCentralIo};

const CONTROL_CHANNEL_SIZE: usize = 1024;

pub async fn run_central_io_writer<W>(
    mut io_writer: CentralIoWriter<W>,
    heartbeat_interval: Duration,
    mut control: WriteControlRx,
    mut data: WriteDataRx,
) -> Result<(), RunCentralIoWriterError>
where
    W: AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            () = tokio::time::sleep(heartbeat_interval) => {
                io_writer.send_heartbeat().await.map_err(RunCentralIoWriterError::IoWriter)?;
            }
            res = control.recv() => {
                let msg = res.map_err(RunCentralIoWriterError::Control)?;
                io_writer.send_control(msg).await.map_err(RunCentralIoWriterError::IoWriter)?;
            }
            res = data.recv() => {
                let msg = res.map_err(RunCentralIoWriterError::Control)?;
                io_writer.send_data(msg).await.map_err(RunCentralIoWriterError::IoWriter)?;
            }
        }
    }
}
#[derive(Debug)]
pub enum RunCentralIoWriterError {
    IoWriter(io::Error),
    Control(DeadControl),
}

#[derive(Debug)]
pub struct CentralIoWriter<W> {
    io_writer: W,
}
impl<W> CentralIoWriter<W> {
    pub fn new(io_writer: W) -> Self {
        Self { io_writer }
    }
}
impl<W> CentralIoWriter<W>
where
    W: AsyncWrite + Unpin,
{
    pub async fn send_heartbeat(&mut self) -> io::Result<()> {
        let hdr = Header::Heartbeat;
        let hdr = hdr.encode();
        self.io_writer.write_all(&hdr).await?;
        Ok(())
    }
    pub async fn send_control(&mut self, msg: WriteControlMsg) -> io::Result<()> {
        let hdr = match &msg {
            WriteControlMsg::Open(_) => Header::Open,
            WriteControlMsg::Close(_, side) => match side {
                Side::Read => Header::CloseRead,
                Side::Write => Header::CloseWrite,
            },
        };
        let stream_id = match msg {
            WriteControlMsg::Open(stream_id) | WriteControlMsg::Close(stream_id, _) => stream_id,
        };
        self.send_control_(hdr, stream_id).await
    }
    async fn send_control_(&mut self, hdr: Header, stream_id: u32) -> io::Result<()> {
        let stream_id_msg = StreamIdMsg { stream_id };
        let hdr = hdr.encode();
        let stream_id_msg = stream_id_msg.encode();
        let mut concat = hdr.into_iter().chain(stream_id_msg);
        let buf: [u8; Header::SIZE + StreamIdMsg::SIZE] =
            core::array::from_fn(|_| concat.next().unwrap());
        self.io_writer.write_all(&buf).await?;
        Ok(())
    }
    pub async fn send_data(&mut self, msg: WriteDataMsg) -> io::Result<()> {
        let data_buf = match msg.data {
            StreamWriteData::Open => return Ok(()),
            StreamWriteData::Fin => {
                let hdr = Header::CloseWrite;
                return self.send_control_(hdr, msg.stream_id).await;
            }
            StreamWriteData::Data(data_buf) => data_buf,
        };
        let hdr = Header::Data;
        let mut remaining_body_len = data_buf.len();
        while remaining_body_len != 0 {
            let body_len = remaining_body_len.min(usize::from(BodyLen::MAX));
            remaining_body_len -= body_len;
            let body_len = BodyLen::try_from(body_len).unwrap();
            let data_hdr = DataHeader {
                stream_id: msg.stream_id,
                body_len,
            };
            let hdr = hdr.encode();
            let data_hdr = data_hdr.encode();
            let mut concat = hdr.into_iter().chain(data_hdr);
            let fixed_buf: [u8; Header::SIZE + DataHeader::SIZE] =
                core::array::from_fn(|_| concat.next().unwrap());
            self.io_writer.write_all(&fixed_buf).await?;
            self.io_writer.write_all(&data_buf).await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct WriteDataMsg {
    pub stream_id: StreamId,
    pub data: StreamWriteData,
}
#[derive(Debug)]
pub enum StreamWriteData {
    Open,
    Fin,
    Data(DataBuf),
}
pub fn write_data_channel() -> (WriteDataTxPrototype, WriteDataRx) {
    let (tx, rx) = fair_queue::channel();
    let tx = WriteDataTxPrototype { opener: tx };
    let rx = WriteDataRx {
        rx,
        token_to_stream: HashMap::new(),
    };
    (tx, rx)
}
#[derive(Debug)]
pub struct WriteDataRx {
    rx: fair_queue::Receiver<WriteDataMsg>,
    token_to_stream: HashMap<fair_queue::Token, StreamId>,
}
impl WriteDataRx {
    pub async fn recv(&mut self) -> Result<WriteDataMsg, DeadControl> {
        loop {
            let (token, msg) = self.rx.recv().await.ok_or(DeadControl {})?;
            match msg {
                fair_queue::ReceiverRecv::Open(value) => {
                    self.token_to_stream.insert(token, value.stream_id);
                    return Ok(value);
                }
                fair_queue::ReceiverRecv::Value(value) => return Ok(value),
                fair_queue::ReceiverRecv::Close => {
                    let Some(stream_id) = self.token_to_stream.remove(&token) else {
                        continue;
                    };
                    return Ok(WriteDataMsg {
                        stream_id,
                        data: StreamWriteData::Fin,
                    });
                }
            }
        }
    }
}
#[derive(Debug, Clone)]
pub struct WriteDataTxPrototype {
    opener: fair_queue::Opener<WriteDataMsg>,
}
impl WriteDataTxPrototype {
    pub async fn derive(&self, stream: StreamId) -> Result<StreamWriteDataTx, DeadCentralIo> {
        Ok(StreamWriteDataTx {
            tx: self
                .opener
                .open(WriteDataMsg {
                    stream_id: stream,
                    data: StreamWriteData::Open,
                })
                .await
                .ok_or(DeadCentralIo { side: Side::Write })?,
            stream_id: stream,
        })
    }
}
#[derive(Debug)]
pub struct StreamWriteDataTx {
    stream_id: StreamId,
    tx: fair_queue::Sender<WriteDataMsg>,
}
impl StreamWriteDataTx {
    pub async fn send(&mut self, data: StreamWriteData) -> Result<(), DeadCentralIo> {
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
pub enum WriteControlMsg {
    Open(StreamId),
    Close(StreamId, Side),
}
pub fn write_control_channel() -> (WriteControlTx, WriteControlRx) {
    let (tx, rx) = tokio::sync::mpsc::channel(CONTROL_CHANNEL_SIZE);
    let tx = WriteControlTx { tx };
    let rx = WriteControlRx { rx };
    (tx, rx)
}
#[derive(Debug, Clone)]
pub struct WriteControlTx {
    tx: tokio::sync::mpsc::Sender<WriteControlMsg>,
}
impl WriteControlTx {
    pub async fn send(&self, msg: WriteControlMsg) -> Result<(), DeadCentralIo> {
        self.tx
            .send(msg)
            .await
            .map_err(|_| DeadCentralIo { side: Side::Write })
    }
    pub async fn closed(&self) {
        self.tx.closed().await
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
