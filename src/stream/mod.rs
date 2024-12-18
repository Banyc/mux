use accepter::StreamAcceptTx;
use opener::StreamOpenRx;

use crate::{common::Side, control::DeadControl, protocol::StreamId};

pub mod accepter;
pub mod opener;
pub mod reader;
pub mod writer;

#[derive(Debug)]
pub struct StreamInitHandle {
    pub stream_open_rx: StreamOpenRx,
    pub stream_accept_tx: StreamAcceptTx,
}

#[derive(Debug)]
pub struct StreamCloseMsg {
    pub side: Side,
    pub stream_id: StreamId,
    pub no_send_to_peer: bool,
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
            no_send_to_peer: false,
        }
    }
}
#[derive(Debug, Clone)]
pub struct StreamCloseTx {
    stream_id: StreamId,
    side: Side,
    tx: tokio::sync::mpsc::UnboundedSender<StreamCloseMsg>,
    no_send_to_peer: bool,
}
impl StreamCloseTx {
    pub fn no_send_to_peer(&mut self) {
        self.no_send_to_peer = true;
    }
}
impl Drop for StreamCloseTx {
    fn drop(&mut self) {
        let msg = StreamCloseMsg {
            stream_id: self.stream_id,
            side: self.side,
            no_send_to_peer: self.no_send_to_peer,
        };
        let _ = self.tx.send(msg);
    }
}
#[derive(Debug)]
pub struct StreamCloseRx {
    rx: tokio::sync::mpsc::UnboundedReceiver<StreamCloseMsg>,
}
impl StreamCloseRx {
    pub async fn recv(&mut self) -> Result<StreamCloseMsg, DeadControl> {
        self.rx.recv().await.ok_or(DeadControl {})
    }
}

#[derive(Debug, Clone)]
pub struct DeadStreamInit {}
#[derive(Debug, Clone)]
pub struct DeadStream {}
