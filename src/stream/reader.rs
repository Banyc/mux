use std::{
    cell::Cell,
    io,
    ops::DerefMut,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll, ready},
    time::Duration,
};

use tokio::io::{AsyncRead, ReadBuf};

use crate::{central_io::DataBuf, control::DeadControl};

use super::StreamCloseTx;

pub(crate) const STREAM_READ_DRAIN_GRACE: Duration = Duration::from_millis(10);
pub(crate) const STREAM_READ_SOFT_DATA_LIMIT: usize = 1024 - 1;
pub(crate) const STREAM_READ_HARD_DATA_LIMIT: usize = 8 * 1024 - 1;
pub(crate) const CHANNEL_SIZE: usize = STREAM_READ_HARD_DATA_LIMIT + 1;

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
pub(crate) enum StreamReadDataMsg {
    Fin,
    Data(DataBuf),
    Error(io::Error),
}
#[derive(Debug)]
pub(crate) struct StreamReadQueueFull;
#[derive(Debug, Clone)]
struct StreamReadDataTx {
    tx: tokio::sync::mpsc::Sender<StreamReadDataMsg>,
}
impl StreamReadDataTx {
    fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
    fn capacity(&self) -> usize {
        self.tx.capacity()
    }
    fn try_send(
        &self,
        msg: StreamReadDataMsg,
    ) -> Result<(), tokio::sync::mpsc::error::TrySendError<StreamReadDataMsg>> {
        self.tx.try_send(msg)
    }
}
#[derive(Debug)]
pub(crate) struct StreamReadDataRx {
    rx: tokio::sync::mpsc::Receiver<StreamReadDataMsg>,
}
impl StreamReadDataRx {
    pub(crate) fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StreamReadDataMsg, DeadControl>> {
        ready!(self.rx.poll_recv(cx)).ok_or(DeadControl {}).into()
    }
    /// Non-blocking receive: returns `Ok(msg)` if one is ready, `Err` if
    /// the channel is empty or closed. Used by reassembly tests.
    #[cfg(test)]
    pub(crate) fn try_recv(&mut self) -> Result<StreamReadDataMsg, ()> {
        self.rx.try_recv().map_err(|_| ())
    }
}

/// A stream read dispatcher that owns the FIN-headroom reservation
/// structurally. `send_data` refuses once only one slot remains, so a
/// terminal `send_terminal` always fits — `Full` there would mean the
/// reservation was broken, which panics as a backstop rather than as the
/// normal failure path. Sustained occupancy at or above
/// [`STREAM_READ_SOFT_DATA_LIMIT`] is refused once
/// [`STREAM_READ_DRAIN_GRACE`] elapses; physical fullness
/// ([`STREAM_READ_HARD_DATA_LIMIT`]) is refused immediately.
#[derive(Debug)]
pub struct StreamDispatcher {
    tx: StreamReadDataTx,
    overloaded_since: Cell<Option<tokio::time::Instant>>,
    terminal_sent: AtomicBool,
}
impl StreamDispatcher {
    fn new(tx: StreamReadDataTx) -> Self {
        Self {
            tx,
            overloaded_since: Cell::new(None),
            terminal_sent: AtomicBool::new(false),
        }
    }
    pub(crate) fn send_data(&self, data: DataBuf) -> Result<(), StreamReadQueueFull> {
        if self.tx.is_closed() {
            return Ok(());
        }
        let mut capacity = self.tx.capacity();
        if capacity <= 1 {
            return Err(StreamReadQueueFull);
        }
        let mut queued_data = CHANNEL_SIZE - capacity;
        let now = tokio::time::Instant::now();
        if queued_data < STREAM_READ_SOFT_DATA_LIMIT {
            self.overloaded_since.set(None);
        } else if let Some(overloaded_since) = self.overloaded_since.get() {
            if now.duration_since(overloaded_since) >= STREAM_READ_DRAIN_GRACE {
                if self.tx.is_closed() {
                    return Ok(());
                }
                capacity = self.tx.capacity();
                if capacity <= 1 {
                    return Err(StreamReadQueueFull);
                }
                queued_data = CHANNEL_SIZE - capacity;
                if queued_data >= STREAM_READ_SOFT_DATA_LIMIT {
                    return Err(StreamReadQueueFull);
                }
                self.overloaded_since.set(None);
            }
        } else {
            self.overloaded_since.set(Some(now));
        }
        match self.tx.try_send(StreamReadDataMsg::Data(data)) {
            Ok(()) => {
                if queued_data + 1 >= STREAM_READ_SOFT_DATA_LIMIT
                    && self.overloaded_since.get().is_none()
                {
                    self.overloaded_since.set(Some(now));
                }
                Ok(())
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Err(StreamReadQueueFull),
        }
    }
    pub(crate) fn finish(&self) {
        self.send_terminal(StreamReadDataMsg::Fin);
    }
    pub(crate) fn fail(&self, error: io::Error) {
        self.send_terminal(StreamReadDataMsg::Error(error));
    }
    fn send_terminal(&self, terminal: StreamReadDataMsg) {
        if self.terminal_sent.swap(true, Ordering::Relaxed) {
            return;
        }
        match self.tx.try_send(terminal) {
            Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => (),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => panic!(
                "terminal Fin/Error must fit: data admission reserves one headroom slot for it"
            ),
        }
    }
}

pub(crate) fn stream_read_channel() -> (StreamDispatcher, StreamReadDataRx) {
    let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_SIZE);
    let tx = StreamReadDataTx { tx };
    let rx = StreamReadDataRx { rx };
    (StreamDispatcher::new(tx), rx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use primitive::arena::obj_pool::arc_buf_pool;
    use std::num::NonZeroUsize;
    use tokio::time;

    fn buf(bytes: &[u8]) -> DataBuf {
        let pool = arc_buf_pool::<u8>(None, NonZeroUsize::new(1).unwrap());
        let mut s = pool.take_scoped();
        s.clear();
        s.extend_from_slice(bytes);
        s
    }

    /// A fresh overload is admitted immediately: the burst spills past the
    /// 1023-item soft watermark into the 8191-item hard headroom without
    /// waiting for the 10 ms grace period, and the paused clock never
    /// advances during admission.
    #[tokio::test(start_paused = true)]
    async fn soft_limit_spills_into_headroom_without_waiting() {
        let (dispatcher, mut rx) = stream_read_channel();
        for _ in 0..STREAM_READ_SOFT_DATA_LIMIT {
            dispatcher.send_data(buf(&[0xAA])).unwrap();
        }
        // Crossing the soft limit is not a refusal: the grace period was
        // armed only just now, so the next dispatch proceeds immediately.
        let before = time::Instant::now();
        dispatcher.send_data(buf(&[0xBB])).unwrap();
        assert_eq!(
            time::Instant::now(),
            before,
            "admission advanced the paused clock"
        );
        // The rest of the hard headroom is equally nonblocking.
        for _ in 0..(STREAM_READ_HARD_DATA_LIMIT - STREAM_READ_SOFT_DATA_LIMIT - 1) {
            dispatcher.send_data(buf(&[0xCC])).unwrap();
        }
        // Only the reserved terminal slot remains.
        assert!(matches!(
            dispatcher.send_data(buf(&[0xDD])),
            Err(StreamReadQueueFull)
        ));
        // The reserved slot still carries a terminal.
        dispatcher.finish();
        for _ in 0..STREAM_READ_HARD_DATA_LIMIT {
            assert!(matches!(rx.try_recv().unwrap(), StreamReadDataMsg::Data(_)));
        }
        assert!(matches!(rx.try_recv().unwrap(), StreamReadDataMsg::Fin));
        assert!(rx.try_recv().is_err());
    }

    /// A sustained overload is tolerated for the grace period, refused on
    /// the first dispatch after it elapses, and accepted again once the
    /// queue drains below the soft limit.
    #[tokio::test(start_paused = true)]
    async fn sustained_soft_overload_resets_on_the_next_dispatch_after_grace() {
        let (dispatcher, mut rx) = stream_read_channel();
        for _ in 0..STREAM_READ_SOFT_DATA_LIMIT {
            dispatcher.send_data(buf(&[0xAA])).unwrap();
        }
        // Inside the grace period the sustained burst is still admitted.
        time::advance(Duration::from_millis(5)).await;
        dispatcher.send_data(buf(&[0xBB])).unwrap();
        // Once the grace period elapses, the next dispatch is refused.
        time::advance(Duration::from_millis(5)).await;
        assert!(matches!(
            dispatcher.send_data(buf(&[0xCC])),
            Err(StreamReadQueueFull)
        ));
        // Draining below the soft limit resets the overload state, so the
        // next dispatch succeeds again.
        for _ in 0..STREAM_READ_SOFT_DATA_LIMIT {
            assert!(matches!(rx.try_recv().unwrap(), StreamReadDataMsg::Data(_)));
        }
        dispatcher.send_data(buf(&[0xDD])).unwrap();
    }

    /// Once a drain brings the queue back under the soft limit, the grace
    /// period starts over: a fresh burst gets a full 10 ms before the next
    /// refusal.
    #[tokio::test(start_paused = true)]
    async fn draining_below_soft_limit_renews_the_grace_period() {
        let (dispatcher, mut rx) = stream_read_channel();
        for _ in 0..STREAM_READ_SOFT_DATA_LIMIT {
            dispatcher.send_data(buf(&[0xAA])).unwrap();
        }
        time::advance(Duration::from_millis(10)).await;
        assert!(matches!(
            dispatcher.send_data(buf(&[0xBB])),
            Err(StreamReadQueueFull)
        ));
        // Drain below the soft limit: the grace period starts afresh.
        for _ in 0..STREAM_READ_SOFT_DATA_LIMIT {
            assert!(matches!(rx.try_recv().unwrap(), StreamReadDataMsg::Data(_)));
        }
        for _ in 0..STREAM_READ_SOFT_DATA_LIMIT {
            dispatcher.send_data(buf(&[0xCC])).unwrap();
        }
        // The renewed grace period still admits...
        time::advance(Duration::from_millis(9)).await;
        dispatcher.send_data(buf(&[0xDD])).unwrap();
        // ...but once it elapses with the queue still over the soft limit,
        // admission is refused again.
        time::advance(Duration::from_millis(1)).await;
        assert!(matches!(
            dispatcher.send_data(buf(&[0xEE])),
            Err(StreamReadQueueFull)
        ));
    }

    /// Physical fullness refuses immediately, even entirely inside the
    /// grace period: the paused clock never advanced during the fill.
    #[tokio::test(start_paused = true)]
    async fn hard_limit_resets_immediately_even_inside_the_grace_period() {
        let (dispatcher, _rx) = stream_read_channel();
        let start = time::Instant::now();
        for _ in 0..STREAM_READ_HARD_DATA_LIMIT {
            dispatcher.send_data(buf(&[0xAA])).unwrap();
        }
        assert_eq!(
            time::Instant::now(),
            start,
            "the fill ran inside the grace period, yet the hard limit refused immediately"
        );
        assert!(matches!(
            dispatcher.send_data(buf(&[0xBB])),
            Err(StreamReadQueueFull)
        ));
    }

    /// With the queue saturated, data can never consume the reserved slot,
    /// the first terminal owns it, and a later terminal cannot overwrite it.
    #[tokio::test(start_paused = true)]
    async fn first_terminal_owns_the_reserved_slot_even_when_the_queue_is_saturated() {
        let (dispatcher, mut rx) = stream_read_channel();
        for _ in 0..STREAM_READ_HARD_DATA_LIMIT {
            dispatcher.send_data(buf(&[0xAA])).unwrap();
        }
        // Data can never consume the reserved slot.
        assert!(matches!(
            dispatcher.send_data(buf(&[0xBB])),
            Err(StreamReadQueueFull)
        ));
        // The first terminal fits into the reserved slot.
        dispatcher.finish();
        // A later terminal must not overwrite the first.
        dispatcher.fail(io::Error::new(io::ErrorKind::BrokenPipe, "late failure"));
        let mut data_items = 0;
        loop {
            match rx.try_recv() {
                Ok(StreamReadDataMsg::Data(_)) => data_items += 1,
                Ok(StreamReadDataMsg::Fin) => break,
                Ok(StreamReadDataMsg::Error(e)) => {
                    panic!("the first terminal was overwritten by a later one: {e}");
                }
                Err(()) => panic!("the reserved terminal slot was consumed by data"),
            }
        }
        assert_eq!(data_items, STREAM_READ_HARD_DATA_LIMIT);
        assert!(
            rx.try_recv().is_err(),
            "a second terminal leaked past the first"
        );
    }
}
