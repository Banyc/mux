use std::{
    future::Future,
    io,
    num::NonZeroUsize,
    ops::DerefMut,
    pin::Pin,
    task::{ready, Context, Poll, Waker},
};

use primitive::arena::obj_pool::ArcObjPool;
use tokio::io::AsyncWrite;

use crate::{
    central_io::{
        writer::{PollStreamWriteDataTx, StreamWriteData, StreamWriteDataTx},
        DeadCentralIo,
    },
    control::WriteBrokenPipe,
    protocol::BodyLen,
};

use super::StreamCloseTx;

const BUF_POOL_SHARDS: NonZeroUsize = NonZeroUsize::new(1).unwrap();

#[derive(Debug)]
struct StreamWriterState {
    broken_pipe: WriteBrokenPipe,
    close: Option<StreamCloseTx>,
    buf_pool: ArcObjPool<Vec<u8>>,
}
impl StreamWriterState {
    pub fn new(broken_pipe: WriteBrokenPipe, close: StreamCloseTx) -> Self {
        Self {
            broken_pipe,
            close: Some(close),
            buf_pool: ArcObjPool::new(None, BUF_POOL_SHARDS, Vec::new, |v| v.clear()),
        }
    }
    pub fn poll_write(
        &mut self,
        data: &mut PollStreamWriteDataTx,
        buf: &[u8],
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize, SendError>> {
        if self.close.is_none() {
            return Err(SendError::LocalClosedStream).into();
        }
        if self.broken_pipe.is_closed() {
            return Err(SendError::PeerClosedStream).into();
        }
        if buf.is_empty() {
            return Ok(0).into();
        }
        ready!(data.poll_preserve(cx)).map_err(SendError::DeadCentralIo)?;
        let data_len = buf.len().min(usize::from(BodyLen::MAX));
        let mut data_buf = self.buf_pool.take_scoped();
        data_buf.extend(&buf[..data_len]);
        data.send_item(StreamWriteData::Data(data_buf))
            .map_err(SendError::DeadCentralIo)?;
        Ok(data_len).into()
    }
    pub fn shutdown(&mut self, data: &mut PollStreamWriteDataTx) -> Result<(), SendError> {
        if self.close.is_none() {
            return Ok(());
        }
        if self.broken_pipe.is_closed() {
            return Err(SendError::PeerClosedStream);
        }
        let mut cx = Context::from_waker(Waker::noop());
        let _ = data
            .poll_preserve(&mut cx)
            .map_err(SendError::DeadCentralIo)?;
        let mut close = self.close.take().unwrap();
        close.mark_close_sent_to_peer();
        Ok(())
    }
}
#[derive(Debug)]
pub enum SendError {
    LocalClosedStream,
    PeerClosedStream,
    DeadCentralIo(DeadCentralIo),
}

#[derive(Debug)]
pub(crate) struct LiveStreamWriter {
    data: PollStreamWriteDataTx,
    state: StreamWriterState,
}
impl LiveStreamWriter {
    pub(crate) fn new(
        data: StreamWriteDataTx,
        broken_pipe: WriteBrokenPipe,
        close: StreamCloseTx,
    ) -> Self {
        let state = StreamWriterState::new(broken_pipe, close);
        Self {
            data: data.into(),
            state,
        }
    }
    pub(crate) fn shutdown(&mut self) -> Result<(), SendError> {
        self.state.shutdown(&mut self.data)
    }
    pub(crate) fn poll_write(
        &mut self,
        buf: &[u8],
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize, SendError>> {
        self.state.poll_write(&mut self.data, buf, cx)
    }
}

#[derive(Debug)]
pub struct StreamWriter {
    live: Option<LiveStreamWriter>,
}
impl Drop for StreamWriter {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
impl StreamWriter {
    pub(crate) fn new(live: LiveStreamWriter) -> Self {
        Self { live: Some(live) }
    }
    pub fn shutdown(&mut self) -> Result<(), SendError> {
        let Some(mut live) = self.live.take() else {
            return Ok(());
        };
        live.shutdown()
    }
    pub fn poll_write(
        &mut self,
        buf: &[u8],
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize, SendError>> {
        let live = self.live.as_mut().ok_or(SendError::LocalClosedStream)?;
        live.poll_write(buf, cx)
    }
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, SendError> {
        struct StreamWriterWrite<'a> {
            wtr: &'a mut StreamWriter,
            buf: &'a [u8],
        }
        impl Future for StreamWriterWrite<'_> {
            type Output = Result<usize, SendError>;
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let this = self.deref_mut();
                this.wtr.poll_write(this.buf, cx)
            }
        }
        StreamWriterWrite { wtr: self, buf }.await
    }
}
impl AsyncWrite for StreamWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.deref_mut();
        this.poll_write(buf, cx).map_err(map_send_error_to_io_error)
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Ok(()).into()
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let this = self.deref_mut();
        this.shutdown().map_err(map_send_error_to_io_error).into()
    }
}

fn map_send_error_to_io_error(e: SendError) -> io::Error {
    match e {
        SendError::LocalClosedStream => io::ErrorKind::NotConnected.into(),
        SendError::PeerClosedStream => io::ErrorKind::BrokenPipe.into(),
        SendError::DeadCentralIo(_) => io::ErrorKind::BrokenPipe.into(),
    }
}
