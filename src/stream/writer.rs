use std::{
    future::Future,
    io::{self, IoSlice},
    num::NonZeroUsize,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker, ready},
};

use primitive::arena::obj_pool::ArcObjPool;
use tokio::io::AsyncWrite;

use crate::{
    central_io::{
        DeadCentralIo,
        scheduler::{DATA_BULK_CAP, PollStreamWriteDataTx, StreamWriteData, StreamWriteDataTx},
    },
    control::WriteBrokenPipe,
};

use super::StreamCloseTx;

const BUF_POOL_SHARDS: NonZeroUsize = NonZeroUsize::new(1).unwrap();
/// Per-poll_write staging ceiling. Bounds the pooled buffer for this stream
/// writer (an uncapped stage pins one Vec as large as the largest write_all
/// for the writer's lifetime) while keeping the fair-queue message count low.
/// At exactly [`DATA_BULK_CAP`] the extra reserve/send/wake cycle per message
/// costs ~8-11% echo throughput on loopback, so stage a few bulk dispatches
/// per message instead.
const DATA_STAGING_CAP: usize = 4 * DATA_BULK_CAP;

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
        self.poll_write_vectored(data, &[IoSlice::new(buf)], cx)
    }
    pub fn poll_write_vectored(
        &mut self,
        data: &mut PollStreamWriteDataTx,
        bufs: &[IoSlice<'_>],
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize, SendError>> {
        if self.close.is_none() {
            return Err(SendError::LocalClosedStream).into();
        }
        if self.broken_pipe.is_closed() {
            return Err(SendError::PeerClosedStream).into();
        }
        let data_len = bufs
            .iter()
            .fold(0usize, |len, buf| len.saturating_add(buf.len()))
            .min(DATA_STAGING_CAP);
        if data_len == 0 {
            return Ok(0).into();
        }
        ready!(data.poll_reserve(cx)).map_err(SendError::DeadCentralIo)?;
        let mut data_buf = self.buf_pool.take_scoped();
        for buf in bufs {
            let remaining = data_len - data_buf.len();
            if remaining == 0 {
                break;
            }
            let take = remaining.min(buf.len());
            data_buf.extend_from_slice(&buf[..take]);
        }
        debug_assert_eq!(data_buf.len(), data_len);
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
        // poll_reserve probes the fair queue for capacity: it preserves error
        // reporting (PeerClosedStream surfaces as Err on shutdown) while dropping
        // self.close independently closes the fair queue and emits FIN after
        // accepted data has drained.
        let mut cx = Context::from_waker(Waker::noop());
        let _ = data
            .poll_reserve(&mut cx)
            .map_err(SendError::DeadCentralIo)?;
        drop(self.close.take());
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
    pub(crate) fn poll_write_vectored(
        &mut self,
        bufs: &[IoSlice<'_>],
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize, SendError>> {
        self.state.poll_write_vectored(&mut self.data, bufs, cx)
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
    /// Stage at most `DATA_STAGING_CAP` (`4 * DATA_BULK_CAP`) bytes of `buf`,
    /// returning how many were accepted. Writes are PARTIAL: a slice larger than
    /// the cap is never accepted in one call, so callers must loop (or use
    /// `AsyncWriteExt::write_all`) for full delivery. A future that is dropped
    /// after `Ready(n)` has irrevocably committed those `n` bytes to the stream —
    /// a cancelled `write_all` may therefore have written a prefix, and retrying
    /// it from the start corrupts the byte stream.
    pub fn poll_write(
        &mut self,
        buf: &[u8],
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize, SendError>> {
        let live = self.live.as_mut().ok_or(SendError::LocalClosedStream)?;
        live.poll_write(buf, cx)
    }

    /// Stage at most `DATA_STAGING_CAP` (`4 * DATA_BULK_CAP`) bytes across
    /// `bufs` as one queue item, preserving slice order. Returns the total
    /// number of bytes accepted.
    pub fn poll_write_vectored(
        &mut self,
        bufs: &[IoSlice<'_>],
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize, SendError>> {
        let live = self.live.as_mut().ok_or(SendError::LocalClosedStream)?;
        live.poll_write_vectored(bufs, cx)
    }

    /// Stage a prefix of `buf` and return its length; see [`Self::poll_write`]
    /// for the partial-write contract (at most `DATA_STAGING_CAP` (`4 *
    /// DATA_BULK_CAP`) bytes are accepted per call).
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
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.deref_mut();
        this.poll_write_vectored(bufs, cx)
            .map_err(map_send_error_to_io_error)
    }
    fn is_write_vectored(&self) -> bool {
        true
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Side,
        central_io::scheduler::{StreamWriteData, write_data_channel},
        control::WriteBrokenPipe,
    };
    use std::io::IoSlice;
    use std::task::{Context, Waker};

    async fn stream_writer_state(
        stream_id: u32,
    ) -> (
        StreamWriterState,
        PollStreamWriteDataTx,
        crate::central_io::scheduler::WriteDataRx,
    ) {
        let (prototype, mut rx) = write_data_channel();
        let derive_fut = prototype.derive(stream_id, false);
        let drive_open = async {
            loop {
                let msg = rx.recv().await.unwrap();
                if msg.stream_id == stream_id && matches!(msg.data, StreamWriteData::Open { .. }) {
                    break;
                }
            }
        };
        let (tx, ()) = tokio::join!(derive_fut, drive_open);
        let broken_pipe = WriteBrokenPipe::new();
        let (close_tx, _close_rx) = crate::stream::stream_close_channel();
        let close = close_tx.derive(Side::Write, stream_id);
        (
            StreamWriterState::new(broken_pipe, close),
            tx.unwrap().into(),
            rx,
        )
    }

    /// `poll_write` stages at most DATA_STAGING_CAP bytes per call, so a larger
    /// caller buffer is split and the returned length never exceeds the cap.
    /// This keeps the per-stream object-pool buffer bounded.
    #[tokio::test]
    async fn poll_write_stages_at_most_staging_cap() {
        let (mut writer, mut data_tx, mut rx) = stream_writer_state(1).await;
        let big = vec![0u8; DATA_STAGING_CAP * 2];
        let mut cx = Context::from_waker(Waker::noop());
        let n = match writer.poll_write(&mut data_tx, &big, &mut cx) {
            Poll::Ready(Ok(n)) => n,
            other => panic!("poll_write should return Ready(Ok(...)): {other:?}"),
        };
        assert_eq!(
            n, DATA_STAGING_CAP,
            "first poll_write must return DATA_STAGING_CAP"
        );
        let mut seen = 0usize;
        while seen < DATA_STAGING_CAP {
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg.stream_id, 1);
            if let StreamWriteData::Data(buf) = msg.data {
                seen += buf.len();
            } else {
                panic!("expected Data, got {other:?}", other = msg.data);
            }
        }
        assert_eq!(
            seen, DATA_STAGING_CAP,
            "total drained bytes must equal staged chunk"
        );
    }

    #[tokio::test]
    async fn poll_write_vectored_stages_slices_as_one_message() {
        let (mut writer, mut data_tx, mut rx) = stream_writer_state(7).await;
        let bufs = [
            IoSlice::new(b"header"),
            IoSlice::new(b""),
            IoSlice::new(b"body"),
        ];
        let mut cx = Context::from_waker(Waker::noop());
        let n = match writer.poll_write_vectored(&mut data_tx, &bufs, &mut cx) {
            Poll::Ready(Ok(n)) => n,
            other => panic!("poll_write_vectored should return Ready(Ok(...)): {other:?}"),
        };
        assert_eq!(n, 10);
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.stream_id, 7);
        let buf = match msg.data {
            StreamWriteData::Data(buf) => buf,
            other => panic!("expected Data, got {other:?}"),
        };
        assert_eq!(&buf[..], b"headerbody");
    }

    #[tokio::test]
    async fn poll_write_vectored_caps_across_slice_boundary() {
        let (mut writer, mut data_tx, mut rx) = stream_writer_state(9).await;
        let first = vec![0xAA; DATA_STAGING_CAP - 2];
        let second = [0xBB; 4];
        let bufs = [IoSlice::new(&first), IoSlice::new(&second)];
        let mut cx = Context::from_waker(Waker::noop());
        let n = match writer.poll_write_vectored(&mut data_tx, &bufs, &mut cx) {
            Poll::Ready(Ok(n)) => n,
            other => panic!("poll_write_vectored should return Ready(Ok(...)): {other:?}"),
        };
        assert_eq!(n, DATA_STAGING_CAP);
        let mut staged = Vec::with_capacity(DATA_STAGING_CAP);
        while staged.len() < DATA_STAGING_CAP {
            let msg = rx.recv().await.unwrap();
            let buf = match msg.data {
                StreamWriteData::Data(buf) => buf,
                other => panic!("expected Data, got {other:?}"),
            };
            staged.extend_from_slice(&buf);
        }
        assert_eq!(&staged[..DATA_STAGING_CAP - 2], &first);
        assert_eq!(&staged[DATA_STAGING_CAP - 2..], &[0xBB; 2]);
    }
}
