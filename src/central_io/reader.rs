use std::{io, num::NonZeroUsize};

use primitive::arena::obj_pool::ArcObjPool;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{
    control::{CentralReadTx, DeadControl},
    protocol::{DataHeader, Header, StreamId, StreamIdMsg},
};

use super::DataBuf;

const OBJ_POOL_SHARDS: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(4) };

pub async fn run_central_io_reader<R>(
    mut io_reader: CentralIoReader<R>,
    tx: CentralReadTx,
) -> Result<(), RunCentralIoReaderError>
where
    R: AsyncRead + Unpin,
{
    loop {
        let msg = io_reader
            .recv()
            .await
            .map_err(RunCentralIoReaderError::Reader)?;
        tx.send(msg)
            .await
            .map_err(RunCentralIoReaderError::Control)?;
    }
}
#[derive(Debug)]
pub enum RunCentralIoReaderError {
    Reader(io::Error),
    Control(DeadControl),
}

#[derive(Debug)]
pub struct CentralIoReader<R> {
    recver: R,
    pkt_pool: ArcObjPool<Vec<u8>>,
}
impl<R> CentralIoReader<R> {
    pub fn new(recver: R) -> Self {
        Self {
            recver,
            pkt_pool: ArcObjPool::new(None, OBJ_POOL_SHARDS, Vec::new, |v| v.clear()),
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
        self.recver.read_exact(&mut hdr).await?;
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
            Header::CloseRead => Some(CentralIoReadMsg::CloseRead(self.recv_stream_id().await?)),
            Header::CloseWrite => Some(CentralIoReadMsg::CloseWrite(self.recv_stream_id().await?)),
        })
    }
    async fn recv_data(&mut self) -> io::Result<(StreamId, DataBuf)> {
        let mut hdr = [0; DataHeader::SIZE];
        self.recver.read_exact(&mut hdr).await.unwrap();
        let hdr = DataHeader::decode(hdr);
        let mut buf = self.pkt_pool.take_scoped();
        buf.extend(core::iter::repeat(0).take(usize::try_from(hdr.body_len).unwrap()));
        self.recver.read_exact(&mut buf).await?;
        Ok((hdr.stream, buf))
    }
    async fn recv_stream_id(&mut self) -> io::Result<StreamId> {
        let mut hdr = [0; StreamIdMsg::SIZE];
        self.recver.read_exact(&mut hdr).await.unwrap();
        let hdr = StreamIdMsg::decode(hdr);
        Ok(hdr.stream)
    }
}

#[derive(Debug)]
pub enum CentralIoReadMsg {
    Open(StreamId),
    Data(StreamId, DataBuf),
    CloseRead(StreamId),
    CloseWrite(StreamId),
}
