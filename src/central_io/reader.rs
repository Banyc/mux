use std::{io, num::NonZeroUsize};

use primitive::arena::obj_pool::ArcObjPool;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{
    common::Side,
    control::DeadControl,
    protocol::{DataHeader, Header, StreamId, StreamIdMsg},
};

use super::{DataBuf, DeadCentralIo};

const OBJ_POOL_SHARDS: NonZeroUsize = NonZeroUsize::new(4).unwrap();
const CHANNEL_SIZE: usize = 1024;

pub async fn run_central_io_reader<R>(
    mut io_reader: CentralIoReader<R>,
    tx: CentralIoReadTx,
) -> Result<(), RunCentralIoReaderError>
where
    R: AsyncRead + Unpin,
{
    loop {
        let msg = io_reader
            .recv()
            .await
            .map_err(RunCentralIoReaderError::IoReader)?;
        tx.send(msg)
            .await
            .map_err(RunCentralIoReaderError::Control)?;
    }
}
#[derive(Debug)]
pub enum RunCentralIoReaderError {
    IoReader(io::Error),
    Control(DeadControl),
}

#[derive(Debug)]
pub struct CentralIoReader<R> {
    io_reader: R,
    buf_pool: ArcObjPool<Vec<u8>>,
}
impl<R> CentralIoReader<R> {
    pub fn new(io_reader: R) -> Self {
        Self {
            io_reader,
            buf_pool: ArcObjPool::new(None, OBJ_POOL_SHARDS, Vec::new, |v| v.clear()),
        }
    }
}
impl<R> CentralIoReader<R>
where
    R: AsyncRead + Unpin,
{
    pub async fn recv(&mut self) -> io::Result<CentralIoReadMsg> {
        loop {
            let res = self.recv_pkt().await?;
            if let Some(res) = res {
                return Ok(res);
            }
        }
    }
    async fn recv_pkt(&mut self) -> io::Result<Option<CentralIoReadMsg>> {
        let mut hdr = [0; Header::SIZE];
        self.io_reader.read_exact(&mut hdr).await?;
        let hdr = Header::decode(hdr).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown header: {hdr:?}"),
            )
        })?;
        Ok(match hdr {
            Header::Heartbeat => None,
            Header::Open => Some(CentralIoReadMsg::Open(self.recv_stream_id().await?)),
            Header::Data => {
                let (stream, pkt) = self.recv_data().await?;
                Some(CentralIoReadMsg::Data(stream, pkt))
            }
            Header::CloseRead => Some(CentralIoReadMsg::Close(
                self.recv_stream_id().await?,
                Side::Read,
            )),
            Header::CloseWrite => Some(CentralIoReadMsg::Close(
                self.recv_stream_id().await?,
                Side::Write,
            )),
        })
    }
    async fn recv_data(&mut self) -> io::Result<(StreamId, DataBuf)> {
        let mut hdr = [0; DataHeader::SIZE];
        self.io_reader.read_exact(&mut hdr).await?;
        let hdr = DataHeader::decode(hdr);
        let mut buf = self.buf_pool.take_scoped();
        buf.extend(std::iter::repeat_n(0, usize::from(hdr.body_len)));
        self.io_reader.read_exact(&mut buf).await?;
        Ok((hdr.stream_id, buf))
    }
    async fn recv_stream_id(&mut self) -> io::Result<StreamId> {
        let mut hdr = [0; StreamIdMsg::SIZE];
        self.io_reader.read_exact(&mut hdr).await?;
        let hdr = StreamIdMsg::decode(hdr);
        Ok(hdr.stream_id)
    }
}

#[derive(Debug)]
pub enum CentralIoReadMsg {
    Open(StreamId),
    Data(StreamId, DataBuf),
    Close(StreamId, Side),
}
pub fn central_io_read_channel() -> (CentralIoReadTx, CentralIoReadRx) {
    let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_SIZE);
    let tx = CentralIoReadTx { tx };
    let rx = CentralIoReadRx { rx };
    (tx, rx)
}
#[derive(Debug, Clone)]
pub struct CentralIoReadTx {
    tx: tokio::sync::mpsc::Sender<CentralIoReadMsg>,
}
impl CentralIoReadTx {
    pub async fn send(&self, msg: CentralIoReadMsg) -> Result<(), DeadControl> {
        self.tx.send(msg).await.map_err(|_| DeadControl {})
    }
}
#[derive(Debug)]
pub struct CentralIoReadRx {
    rx: tokio::sync::mpsc::Receiver<CentralIoReadMsg>,
}
impl CentralIoReadRx {
    pub async fn recv(&mut self) -> Result<CentralIoReadMsg, DeadCentralIo> {
        self.rx
            .recv()
            .await
            .ok_or(DeadCentralIo { side: Side::Read })
    }
}
