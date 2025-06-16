use std::{io, num::NonZeroUsize};

use async_async_io::write::AsyncAsyncWrite;
use primitive::arena::obj_pool::ArcObjPool;

use crate::{
    central_io::{
        writer::{StreamWriteData, StreamWriteDataTx},
        DeadCentralIo,
    },
    control::WriteBrokenPipe,
    protocol::BodyLen,
};

use super::StreamCloseTx;

const BUF_POOL_SHARDS: NonZeroUsize = NonZeroUsize::new(1).unwrap();

#[derive(Debug)]
pub struct StreamWriter {
    data: StreamWriteDataTx,
    broken_pipe: WriteBrokenPipe,
    close: Option<StreamCloseTx>,
    buf_pool: ArcObjPool<Vec<u8>>,
}
impl Drop for StreamWriter {
    fn drop(&mut self) {
        let Some(mut close) = self.close.take() else {
            return;
        };
        close.no_send_to_peer()
    }
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
        let data_len = buf.len().min(usize::from(BodyLen::MAX));
        let mut data_buf = self.buf_pool.take_scoped();
        data_buf.extend(&buf[..data_len]);
        self.data
            .send(StreamWriteData::Data(data_buf))
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
            let data_len = remaining_buf.len().min(usize::from(BodyLen::MAX));
            let mut data_buf = self.buf_pool.take_scoped();
            data_buf.extend(&remaining_buf[..data_len]);
            *remaining_buf = &remaining_buf[data_len..];
            self.data
                .send(StreamWriteData::Data(data_buf))
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
        Ok(())
    }
}
