use std::{
    io,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, ready},
};

use tokio::io::{AsyncRead, ReadBuf};

use crate::{central_io::DataBuf, control::DeadControl};

use super::StreamCloseTx;

pub(crate) const CHANNEL_SIZE: usize = 1024;

#[derive(Debug)]
struct StreamReaderState {
    leftover: Option<(DataBuf, usize)>,
    prepend: Vec<u8>,
    prepend_pos: usize,
    is_eof: bool,
    read_error: Option<io::ErrorKind>,
    _close: StreamCloseTx,
}
impl StreamReaderState {
    pub fn new(close: StreamCloseTx) -> Self {
        Self {
            leftover: None,
            prepend: Vec::new(),
            prepend_pos: 0,
            is_eof: false,
            read_error: None,
            _close: close,
        }
    }
    pub fn prepend(&mut self, bytes: &[u8]) {
        if self.prepend_pos < self.prepend.len() {
            let mut combined = bytes.to_vec();
            combined.extend_from_slice(&self.prepend[self.prepend_pos..]);
            self.prepend = combined;
        } else {
            self.prepend = bytes.to_vec();
        }
        self.prepend_pos = 0;
    }
    pub fn poll_recv(
        &mut self,
        data: &mut StreamReadDataRx,
        buf: &mut [u8],
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<usize>> {
        if self.prepend_pos < self.prepend.len() {
            let src = &self.prepend[self.prepend_pos..];
            let n = buf.len().min(src.len());
            buf[..n].copy_from_slice(&src[..n]);
            self.prepend_pos += n;
            if self.prepend_pos >= self.prepend.len() {
                self.prepend.clear();
                self.prepend_pos = 0;
            }
            return Poll::Ready(Ok(n));
        }
        if let Some(kind) = self.read_error {
            return Poll::Ready(Err(io::Error::from(kind)));
        }
        if self.is_eof {
            return Poll::Ready(Ok(0));
        }
        let (data_buf, pos) = match self.leftover.take() {
            Some(x) => x,
            None => {
                let msg = match ready!(data.poll_recv(cx)) {
                    Ok(m) => m,
                    Err(DeadControl {}) => {
                        return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
                    }
                };
                let data_buf = match msg {
                    StreamReadDataMsg::Fin => {
                        self.is_eof = true;
                        return Poll::Ready(Ok(0));
                    }
                    StreamReadDataMsg::Data(data_buf) => data_buf,
                    StreamReadDataMsg::Error(e) => {
                        let kind = e.kind();
                        self.read_error = Some(kind);
                        self.is_eof = true;
                        return Poll::Ready(Err(e));
                    }
                };
                (data_buf, 0)
            }
        };
        let data = &data_buf[pos..];
        let data_len = buf.len().min(data.len());
        buf[..data_len].copy_from_slice(&data[..data_len]);
        let pos = pos + data_len;
        if pos < data_buf.len() {
            self.leftover = Some((data_buf, pos));
        }
        Poll::Ready(Ok(data_len))
    }
}

#[derive(Debug)]
pub struct StreamReader {
    data: StreamReadDataRx,
    state: StreamReaderState,
}
impl StreamReader {
    pub(crate) fn new(data: StreamReadDataRx, close: StreamCloseTx) -> Self {
        let state = StreamReaderState::new(close);
        Self { data, state }
    }
    /// Push `bytes` back to the front of the reader so they are returned
    /// before any subsequent channel data.  Used by peek-style probes
    /// (e.g. resume-header detection) that consume bytes and need to
    /// restore them for plain streams.
    pub fn prepend(&mut self, bytes: &[u8]) {
        self.state.prepend(bytes);
    }
}
impl AsyncRead for StreamReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.deref_mut();
        let n = ready!(
            this.state
                .poll_recv(&mut this.data, buf.initialize_unfilled(), cx)
        )?;
        buf.advance(n);
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
pub enum StreamReadDataMsg {
    Fin,
    Data(DataBuf),
    Error(io::Error),
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
    pub fn capacity(&self) -> usize {
        self.tx.capacity()
    }
    pub fn try_send(
        &self,
        msg: StreamReadDataMsg,
    ) -> Result<(), tokio::sync::mpsc::error::TrySendError<StreamReadDataMsg>> {
        self.tx.try_send(msg)
    }
}
#[derive(Debug)]
pub struct StreamReadDataRx {
    rx: tokio::sync::mpsc::Receiver<StreamReadDataMsg>,
}
impl StreamReadDataRx {
    pub fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StreamReadDataMsg, DeadControl>> {
        ready!(self.rx.poll_recv(cx)).ok_or(DeadControl {}).into()
    }
    /// Non-blocking receive: returns `Ok(msg)` if one is ready, `Err` if
    /// the channel is empty or closed. Used by reassembly tests.
    #[cfg(test)]
    pub fn try_recv(&mut self) -> Result<StreamReadDataMsg, ()> {
        self.rx.try_recv().map_err(|_| ())
    }
    // pub async fn recv(&mut self) -> Result<StreamReadDataMsg, DeadControl> {
    //     self.rx.recv().await.ok_or(DeadControl {})
    // }
}
