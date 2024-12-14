use crate::{common::Side, protocol::StreamId};

pub mod accepter;
pub mod reader;
pub mod writer;

#[derive(Debug)]
pub struct StreamCloseMsg {
    pub side: Side,
    pub stream_id: StreamId,
}
pub fn stream_close_channel() -> (StreamCloseTxPrototype, StreamCloseRx) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let tx = StreamCloseTxPrototype { tx };
    let rx = StreamCloseRx { rx };
    (tx, rx)
}
#[derive(Debug, Clone)]
pub struct StreamCloseTxPrototype {
    tx: tokio::sync::mpsc::UnboundedSender<StreamCloseMsg>,
}
impl StreamCloseTxPrototype {
    pub fn derive(&self, side: Side, stream_id: StreamId) -> StreamCloseTx {
        StreamCloseTx {
            stream_id,
            side,
            tx: self.tx.clone(),
        }
    }
}
#[derive(Debug, Clone)]
pub struct StreamCloseTx {
    stream_id: StreamId,
    side: Side,
    tx: tokio::sync::mpsc::UnboundedSender<StreamCloseMsg>,
}
impl Drop for StreamCloseTx {
    fn drop(&mut self) {
        let msg = StreamCloseMsg {
            stream_id: self.stream_id,
            side: self.side,
        };
        let _ = self.tx.send(msg);
    }
}
#[derive(Debug)]
pub struct StreamCloseRx {
    rx: tokio::sync::mpsc::UnboundedReceiver<StreamCloseMsg>,
}
impl StreamCloseRx {
    pub async fn recv(&mut self) -> Result<StreamCloseMsg, DeadStream> {
        self.rx.recv().await.ok_or(DeadStream {})
    }
}

#[derive(Debug, Clone)]
pub struct DeadStream {}
