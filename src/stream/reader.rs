use std::io;

use async_async_io::read::AsyncAsyncRead;

use crate::{central_io::DataBuf, control::DeadControl};

use super::{DeadStream, StreamCloseTx};

const CHANNEL_SIZE: usize = 1024;

#[derive(Debug)]
pub struct StreamReader {
    data: StreamReadDataRx,
    leftover: Option<(DataBuf, usize)>,
    is_eof: bool,
    _close: StreamCloseTx,
}
impl StreamReader {
    pub(crate) fn new(data: StreamReadDataRx, close: StreamCloseTx) -> Self {
        Self {
            data,
            leftover: None,
            is_eof: false,
            _close: close,
        }
    }
    pub async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, DeadControl> {
        if self.is_eof {
            return Ok(0);
        }
        let (data_buf, pos) = match self.leftover.take() {
            Some(x) => x,
            None => {
                let msg = self.data.recv().await?;
                let data_buf = match msg {
                    StreamReadDataMsg::Fin => {
                        self.is_eof = true;
                        return Ok(0);
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
        Ok(data_len)
    }
}

impl AsyncAsyncRead for StreamReader {
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.recv(buf).await {
            Ok(n) => Ok(n),
            Err(DeadControl {}) => Err(io::ErrorKind::BrokenPipe.into()),
        }
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
    pub async fn recv(&mut self) -> Result<StreamReadDataMsg, DeadControl> {
        self.rx.recv().await.ok_or(DeadControl {})
    }
}
