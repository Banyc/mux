use crate::{control::DeadControl, StreamReader, StreamWriter};

use super::DeadStreamInit;

const CHANNEL_SIZE: usize = 1024;

#[derive(Debug)]
pub struct StreamAccepter {
    rx: StreamAcceptRx,
}
impl StreamAccepter {
    pub(crate) fn new(rx: StreamAcceptRx) -> Self {
        Self { rx }
    }
    pub async fn accept(&mut self) -> Result<(StreamReader, StreamWriter), DeadControl> {
        let msg = self.rx.recv().await?;
        Ok((msg.reader, msg.writer))
    }
}

#[derive(Debug)]
pub struct StreamAcceptMsg {
    pub reader: StreamReader,
    pub writer: StreamWriter,
}
pub fn stream_accept_channel() -> (StreamAcceptTx, StreamAcceptRx) {
    let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_SIZE);
    let tx = StreamAcceptTx { tx };
    let rx = StreamAcceptRx { rx };
    (tx, rx)
}
#[derive(Debug, Clone)]
pub struct StreamAcceptTx {
    tx: tokio::sync::mpsc::Sender<StreamAcceptMsg>,
}
impl StreamAcceptTx {
    pub async fn send(&self, msg: StreamAcceptMsg) -> Result<(), DeadStreamInit> {
        self.tx.send(msg).await.map_err(|_| DeadStreamInit {})
    }
}
#[derive(Debug)]
pub struct StreamAcceptRx {
    rx: tokio::sync::mpsc::Receiver<StreamAcceptMsg>,
}
impl StreamAcceptRx {
    pub async fn recv(&mut self) -> Result<StreamAcceptMsg, DeadControl> {
        self.rx.recv().await.ok_or(DeadControl {})
    }
}
