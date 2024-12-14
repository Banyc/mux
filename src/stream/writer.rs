use std::num::NonZeroUsize;

use primitive::arena::obj_pool::ArcObjPool;

use crate::{
    central_io::DeadCentralIo,
    control::{StreamWriteDataTx, WriteBrokenPipe},
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
