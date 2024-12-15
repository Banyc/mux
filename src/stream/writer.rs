use std::{io, num::NonZeroUsize};

use async_async_io::write::AsyncAsyncWrite;
use primitive::arena::obj_pool::ArcObjPool;

const CHANNEL_SIZE: usize = 1024;

use crate::{
    central_io::DeadCentralIo,
    common::Side,
    control::{DeadControl, StreamWriteDataTx, WriteBrokenPipe},
    protocol::StreamId,
};

use super::StreamCloseTx;

const BUF_POOL_SHARDS: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(1) };
const MAX_DATA_SENT_ONCE: usize = 1 << 12;

#[derive(Debug)]
pub struct StreamWriter {
    data: StreamWriteDataTx,
    broken_pipe: WriteBrokenPipe,
    close: Option<StreamCloseTx>,
    buf_pool: ArcObjPool<Vec<u8>>,
}
impl StreamWriter {
    pub(crate) fn new(
        data: StreamWriteDataTx,
        broken_pipe: WriteBrokenPipe,
        close: StreamCloseTx,
    ) -> Self {
        Self {
            data,
            broken_pipe,
            close: Some(close),
            buf_pool: ArcObjPool::new(None, BUF_POOL_SHARDS, Vec::new, |v| v.clear()),
        }
    }
    pub fn close(&mut self) {
        self.close = None;
    }
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, SendError> {
        if self.close.is_none() {
            return Err(SendError::LocalClosedStream);
        }
        if self.broken_pipe.is_closed() {
            return Err(SendError::PeerClosedStream);
        }
        if buf.is_empty() {
            return Ok(0);
        }
        let data_len = buf.len().min(MAX_DATA_SENT_ONCE);
        let mut data_buf = self.buf_pool.take_scoped();
        data_buf.extend(&buf[..data_len]);
        self.data
            .send(data_buf)
            .await
            .map_err(SendError::DeadCentralIo)?;
        Ok(data_len)
    }
    pub async fn send(&mut self, buf: &[u8]) -> Result<(), SendError> {
        if self.close.is_none() {
            return Err(SendError::LocalClosedStream);
        }
        if self.broken_pipe.is_closed() {
            return Err(SendError::PeerClosedStream);
        }
        let remaining_buf = &mut &*buf;
        while !remaining_buf.is_empty() {
            let data_len = remaining_buf.len().min(MAX_DATA_SENT_ONCE);
            let mut data_buf = self.buf_pool.take_scoped();
            data_buf.extend(&remaining_buf[..data_len]);
            *remaining_buf = &remaining_buf[data_len..];
            self.data
                .send(data_buf)
                .await
                .map_err(SendError::DeadCentralIo)?;
        }
        Ok(())
    }
}
#[derive(Debug)]
pub enum SendError {
    LocalClosedStream,
    PeerClosedStream,
    DeadCentralIo(DeadCentralIo),
}

impl AsyncAsyncWrite for StreamWriter {
    async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.write(buf).await {
            Ok(n) => Ok(n),
            Err(e) => Err(match e {
                SendError::LocalClosedStream => io::ErrorKind::NotConnected.into(),
                SendError::PeerClosedStream => io::ErrorKind::BrokenPipe.into(),
                SendError::DeadCentralIo(_) => io::ErrorKind::BrokenPipe.into(),
            }),
        }
    }

    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        self.close();
        Ok(())
    }
}

pub fn write_control_channel() -> (WriteControlTx, WriteControlRx) {
    let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_SIZE);
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

#[derive(Debug, Clone)]
pub enum WriteControlMsg {
    Open(StreamId),
    Close(StreamId, Side),
}
