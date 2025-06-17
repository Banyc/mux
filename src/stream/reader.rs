use std::{
    io,
    ops::DerefMut,
    pin::Pin,
    task::{ready, Context, Poll},
};

use tokio::io::{AsyncRead, ReadBuf};

use crate::{central_io::DataBuf, control::DeadControl};

use super::{DeadStream, StreamCloseTx};

const CHANNEL_SIZE: usize = 1024;

#[derive(Debug)]
struct StreamReaderState {
    leftover: Option<(DataBuf, usize)>,
    is_eof: bool,
    _close: StreamCloseTx,
}
impl StreamReaderState {
    pub fn new(close: StreamCloseTx) -> Self {
        Self {
            leftover: None,
            is_eof: false,
            _close: close,
        }
    }
    pub fn poll_recv(
        &mut self,
        data: &mut StreamReadDataRx,
        buf: &mut [u8],
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize, DeadControl>> {
        if self.is_eof {
            return Ok(0).into();
        }
        let (data_buf, pos) = match self.leftover.take() {
            Some(x) => x,
            None => {
                let msg = ready!(data.poll_recv(cx))?;
                let data_buf = match msg {
                    StreamReadDataMsg::Fin => {
                        self.is_eof = true;
                        return Ok(0).into();
                    }
                    StreamReadDataMsg::Data(data_buf) => data_buf,
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
        Ok(data_len).into()
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
}
impl AsyncRead for StreamReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.deref_mut();
        let n = match ready!(this
            .state
            .poll_recv(&mut this.data, buf.initialize_unfilled(), cx))
        {
            Ok(n) => n,
            Err(DeadControl {}) => {
                return Err(io::ErrorKind::BrokenPipe.into()).into();
            }
        };
        buf.advance(n);
        Ok(()).into()
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
    pub async fn send(&self, msg: StreamReadDataMsg) -> Result<(), DeadStream> {
        self.tx.send(msg).await.map_err(|_| DeadStream {})
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
    // pub async fn recv(&mut self) -> Result<StreamReadDataMsg, DeadControl> {
    //     self.rx.recv().await.ok_or(DeadControl {})
    // }
}
