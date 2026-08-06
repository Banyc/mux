use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use accepter::StreamAcceptTx;
use opener::StreamOpenRx;
use tokio::sync::Notify;

use crate::{
    control::DeadControl,
    protocol::{Side, StreamId},
};

pub mod accepter;
pub mod opener;
pub mod reader;
pub mod writer;

#[derive(Debug)]
pub struct StreamInitChannels {
    pub stream_open_rx: StreamOpenRx,
    pub stream_accept_tx: StreamAcceptTx,
}

#[derive(Debug, PartialEq, Eq)]
pub struct StreamCloseMsg {
    pub side: Side,
    pub stream_id: StreamId,
}

/// Per-(stream, side) close flags plus a [`Notify`]. `Drop` records a close
/// idempotently (coalescing duplicates) and pings the consumer; the consumer
/// drains every pending close on each wake, so close notifications can never
/// be lost to a full queue.
#[derive(Debug, Clone, Default)]
struct StreamCloseState {
    notify: Arc<Notify>,
    pending: Arc<Mutex<HashMap<StreamId, CloseSides>>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct CloseSides(u8);
impl CloseSides {
    const READ: u8 = 1 << 0;
    const WRITE: u8 = 1 << 1;
    fn set(&mut self, side: Side) {
        let bit = match side {
            Side::Read => Self::READ,
            Side::Write => Self::WRITE,
        };
        self.0 |= bit;
    }
    fn is_set(self, side: Side) -> bool {
        let bit = match side {
            Side::Read => Self::READ,
            Side::Write => Self::WRITE,
        };
        self.0 & bit != 0
    }
}

impl StreamCloseState {
    fn record(&self, stream_id: StreamId, side: Side) {
        {
            let mut pending = self.pending.lock().unwrap();
            pending.entry(stream_id).or_default().set(side);
        }
        self.notify.notify_one();
    }

    fn drain(&self) -> Vec<StreamCloseMsg> {
        let mut pending = self.pending.lock().unwrap();
        let pending = std::mem::take(&mut *pending);
        let mut out = Vec::with_capacity(pending.len());
        for (stream_id, sides) in pending {
            if sides.is_set(Side::Read) {
                out.push(StreamCloseMsg {
                    stream_id,
                    side: Side::Read,
                });
            }
            if sides.is_set(Side::Write) {
                out.push(StreamCloseMsg {
                    stream_id,
                    side: Side::Write,
                });
            }
        }
        out
    }
}

pub fn stream_close_channel() -> (StreamCloseTxPrototype, StreamCloseRx) {
    let state = StreamCloseState::default();
    let tx = StreamCloseTxPrototype {
        state: state.clone(),
    };
    let rx = StreamCloseRx {
        state,
        queue: VecDeque::new(),
    };
    (tx, rx)
}
#[derive(Debug, Clone)]
pub struct StreamCloseTxPrototype {
    state: StreamCloseState,
}
impl StreamCloseTxPrototype {
    pub fn derive(&self, side: Side, stream_id: StreamId) -> StreamCloseTx {
        StreamCloseTx {
            stream_id,
            side,
            state: self.state.clone(),
        }
    }
}
#[derive(Debug, Clone)]
pub struct StreamCloseTx {
    stream_id: StreamId,
    side: Side,
    state: StreamCloseState,
}
impl Drop for StreamCloseTx {
    fn drop(&mut self) {
        self.state.record(self.stream_id, self.side);
    }
}
#[derive(Debug)]
pub struct StreamCloseRx {
    state: StreamCloseState,
    queue: VecDeque<StreamCloseMsg>,
}
impl StreamCloseRx {
    pub async fn recv(&mut self) -> Result<StreamCloseMsg, DeadControl> {
        loop {
            if let Some(msg) = self.queue.pop_front() {
                return Ok(msg);
            }
            self.queue.extend(self.state.drain());
            if let Some(msg) = self.queue.pop_front() {
                return Ok(msg);
            }
            self.state.notify.notified().await;
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeadStreamInit {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn close_notifications_survive_a_large_burst() {
        let (close_tx, mut close_rx) = stream_close_channel();
        let mut expected = Vec::new();
        for stream_id in 0..10_000u32 {
            for side in [Side::Read, Side::Write] {
                expected.push(StreamCloseMsg { stream_id, side });
                drop(close_tx.derive(side, stream_id));
            }
        }
        let mut got = Vec::new();
        for _ in 0..expected.len() {
            got.push(close_rx.recv().await.unwrap());
        }
        assert_eq!(
            got.len(),
            expected.len(),
            "a close notification was lost to the queue"
        );
        for msg in expected {
            assert!(got.contains(&msg), "missing close notification {msg:?}");
        }
    }

    #[tokio::test]
    async fn duplicate_closes_coalesce_into_one_notification() {
        let (close_tx, mut close_rx) = stream_close_channel();
        drop(close_tx.derive(Side::Read, 7));
        drop(close_tx.derive(Side::Read, 7));
        drop(close_tx.derive(Side::Write, 7));
        assert_eq!(
            close_rx.recv().await.unwrap(),
            StreamCloseMsg {
                stream_id: 7,
                side: Side::Read,
            }
        );
        assert_eq!(
            close_rx.recv().await.unwrap(),
            StreamCloseMsg {
                stream_id: 7,
                side: Side::Write,
            }
        );
        let extra =
            tokio::time::timeout(std::time::Duration::from_millis(50), close_rx.recv()).await;
        assert!(
            extra.is_err(),
            "duplicate closes emitted a second notification for the same side"
        );
    }
}
