use crate::{
    control::{DeadControl, TooManyOpenStreams},
    StreamReader, StreamWriter,
};

use super::{accepter::StreamAcceptMsg, DeadStream};

const CHANNEL_SIZE: usize = 1024;

#[derive(Debug, Clone)]
pub struct StreamOpener {
    tx: StreamOpenTx,
}
impl StreamOpener {
    pub(crate) fn new(tx: StreamOpenTx) -> Self {
        Self { tx }
    }
    pub async fn open(&self) -> Result<(StreamReader, StreamWriter), StreamOpenError> {
        let msg = self.tx.send().await?;
        Ok((msg.reader, msg.writer))
    }
}

#[derive(Debug)]
pub struct StreamOpenMsg {
    pub stream: tokio::sync::oneshot::Sender<Result<StreamAcceptMsg, TooManyOpenStreams>>,
}
pub fn stream_open_channel() -> (StreamOpenTx, StreamOpenRx) {
    let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_SIZE);
    let tx = StreamOpenTx { tx };
    let rx = StreamOpenRx { rx };
    (tx, rx)
}
#[derive(Debug, Clone)]
pub struct StreamOpenTx {
    tx: tokio::sync::mpsc::Sender<StreamOpenMsg>,
}
impl StreamOpenTx {
    pub async fn send(&self) -> Result<StreamAcceptMsg, StreamOpenError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(StreamOpenMsg { stream: tx })
            .await
            .map_err(|_| StreamOpenError::DeadControl(DeadControl {}))?;
        let stream = rx
            .await
            .map_err(|_| StreamOpenError::DeadControl(DeadControl {}))?
            .map_err(StreamOpenError::TooManyOpenStreams)?;
        Ok(stream)
    }
}
#[derive(Debug)]
pub struct StreamOpenRx {
    rx: tokio::sync::mpsc::Receiver<StreamOpenMsg>,
}
impl StreamOpenRx {
    pub async fn recv(&mut self) -> Result<StreamOpenMsg, DeadStream> {
        self.rx.recv().await.ok_or(DeadStream {})
    }
}

#[derive(Debug)]
pub enum StreamOpenError {
    DeadControl(DeadControl),
    TooManyOpenStreams(TooManyOpenStreams),
}
