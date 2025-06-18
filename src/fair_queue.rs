use std::{
    collections::BTreeMap,
    fmt::Debug,
    future::Future,
    ops::DerefMut,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{ready, Context, Poll},
};

use tokio::sync::{mpsc, oneshot};

const MAX_QUEUE_COUNT: usize = 1 << 10;
const OPENER_QUEUE_SIZE: usize = 1 << 10;
const DATA_QUEUE_SIZE: usize = 1 << 10;

pub fn channel<T>() -> (Opener<T>, Receiver<T>) {
    let (opener_tx, opener_rx) = mpsc::channel(OPENER_QUEUE_SIZE);
    let tx = Opener::new(opener_tx);
    let rx = Receiver::new(opener_rx);
    (tx, rx)
}
#[derive(Debug)]
pub struct Opener<T> {
    opener: mpsc::Sender<OpenRequest<T>>,
}
impl<T> Clone for Opener<T> {
    fn clone(&self) -> Self {
        Self {
            opener: self.opener.clone(),
        }
    }
}
impl<T> Opener<T> {
    fn new(opener: mpsc::Sender<OpenRequest<T>>) -> Self {
        Self { opener }
    }
    pub fn is_closed(&self) -> bool {
        self.opener.is_closed()
    }
    pub async fn open(&self, opening_value: T) -> Option<Sender<T>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = OpenRequest {
            resp: resp_tx,
            opening_value,
        };
        match self.opener.send(req).await {
            Ok(_) => (),
            Err(_) => return None,
        };
        let resp = match resp_rx.await {
            Ok(resp) => resp,
            Err(_) => return None,
        };
        Some(Sender::new(resp))
    }
    pub fn lazy_open(&self, opening_value: T) -> LazySender<T> {
        LazySender::new(self.clone(), opening_value)
    }
}
#[derive(Debug)]
pub struct LazySender<T> {
    opener: Opener<T>,
    opening_value: Option<T>,
    sender: Option<NotClone<Sender<T>>>,
}
impl<T> LazySender<T> {
    fn new(opener: Opener<T>, opening_value: T) -> Self {
        Self {
            opener,
            opening_value: Some(opening_value),
            sender: None,
        }
    }
    async fn ensure_sender(&mut self) -> Option<&Sender<T>> {
        if self.sender.is_none() {
            let opening_value = self.opening_value.take()?;
            let sender = self.opener.open(opening_value).await?;
            self.sender.get_or_insert(NotClone(sender));
        }
        Some(&self.sender.as_ref().unwrap().0)
    }
    pub fn try_send(&mut self, value: T) -> Result<(), (LazySenderError, T)> {
        let Some(NotClone(sender)) = &self.sender else {
            return Err((LazySenderError::NotOpened, value));
        };
        match sender.try_send(value) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(value)) => Err((LazySenderError::Full, value)),
            Err(mpsc::error::TrySendError::Closed(value)) => Err((LazySenderError::Closed, value)),
        }
    }
    pub async fn send(&mut self, value: T) -> Result<(), (LazySenderError, T)> {
        let Some(sender) = self.ensure_sender().await else {
            return Err((LazySenderError::Closed, value));
        };
        match sender.send(value).await {
            Ok(()) => Ok(()),
            Err(mpsc::error::SendError(value)) => Err((LazySenderError::Closed, value)),
        }
    }
}
#[derive(Debug, Clone)]
pub enum LazySenderError {
    Closed,
    Full,
    NotOpened,
}

#[derive(Debug, Clone)]
struct SenderWithState<Sender> {
    pub state: SenderState,
    // drop ordering barrier
    pub sender: Sender,
}

#[derive(Debug)]
pub struct PollSender<T> {
    queue: SenderWithState<tokio_util::sync::PollSender<T>>,
}
impl<T: Send> From<Sender<T>> for PollSender<T> {
    fn from(value: Sender<T>) -> Self {
        let sender = tokio_util::sync::PollSender::new(value.queue.sender);
        Self {
            queue: SenderWithState {
                state: value.queue.state,
                sender,
            },
        }
    }
}
impl<T: Send> PollSender<T> {
    pub fn poll_reserve(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
        self.queue.state.poll_reserve(&mut self.queue.sender, cx)
    }
    pub fn send_item(&mut self, value: T) -> Result<(), T> {
        self.queue.state.send_item(&mut self.queue.sender, value)
    }
    pub fn poll_send(&mut self, value: T, cx: &mut Context<'_>) -> Poll<Result<(), T>> {
        match ready!(self.poll_reserve(cx)) {
            Ok(()) => (),
            Err(()) => return Err(value).into(),
        }
        self.send_item(value).into()
    }
}
#[derive(Debug)]
pub struct Sender<T> {
    queue: SenderWithState<mpsc::Sender<T>>,
}
impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let queue = SenderWithState {
            state: self.queue.state.clone(),
            sender: self.queue.sender.clone(),
        };
        Self { queue }
    }
}
impl<T> Sender<T> {
    fn new(resp: OpenResponse<T>) -> Self {
        let state = SenderState::new(resp.ready, resp.token);
        let queue = SenderWithState {
            state,
            sender: resp.dedicated_chan,
        };
        Self { queue }
    }
    pub fn is_closed(&self) -> bool {
        self.queue.sender.is_closed()
    }
    pub fn try_send(&self, value: T) -> Result<(), mpsc::error::TrySendError<T>> {
        self.queue.state.try_send(&self.queue.sender, value)
    }
    pub async fn send(&self, value: T) -> Result<(), mpsc::error::SendError<T>> {
        self.queue.state.send(&self.queue.sender, value).await
    }
}
#[derive(Debug, Clone)]
struct SenderState {
    ready: Arc<Mutex<ReadyTree>>,
    token: Token,
}
impl Drop for SenderState {
    fn drop(&mut self) {
        self.ready.lock().unwrap().add(self.token);
    }
}
impl SenderState {
    pub fn new(ready: Arc<Mutex<ReadyTree>>, token: Token) -> Self {
        Self { ready, token }
    }
    pub fn try_send<T>(
        &self,
        queue: &mpsc::Sender<T>,
        value: T,
    ) -> Result<(), mpsc::error::TrySendError<T>> {
        let mut undo = ready_incr(&self.ready, self.token);
        queue.try_send(value)?;
        undo.cancel();
        Ok(())
    }
    /// # Cancel safety
    ///
    /// safe
    pub async fn send<T>(
        &self,
        queue: &mpsc::Sender<T>,
        value: T,
    ) -> Result<(), mpsc::error::SendError<T>> {
        let mut undo = ready_incr(&self.ready, self.token);
        queue.send(value).await?;
        undo.cancel();
        Ok(())
    }
    pub fn poll_reserve<T: Send>(
        &self,
        queue: &mut tokio_util::sync::PollSender<T>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ()>> {
        let res = ready!(queue.poll_reserve(cx));
        match res {
            Ok(()) => Ok(()).into(),
            Err(e) => {
                assert!(e.into_inner().is_none());
                Err(()).into()
            }
        }
    }
    /// call [`Self::poll_reserve`] first
    pub fn send_item<T: Send>(
        &self,
        queue: &mut tokio_util::sync::PollSender<T>,
        value: T,
    ) -> Result<(), T> {
        let mut undo = ready_incr(&self.ready, self.token);
        if let Err(e) = queue.send_item(value) {
            return Err(e.into_inner().unwrap());
        }
        undo.cancel();
        Ok(())
    }
}

#[derive(Debug)]
pub struct Receiver<T> {
    opener: mpsc::Receiver<OpenRequest<T>>,
    next_new_token: Token,
    ready: Arc<Mutex<ReadyTree>>,
    queues: BTreeMap<Token, mpsc::Receiver<T>>,
    recv_queue_start: Token,
}
impl<T> Receiver<T> {
    fn new(opener: mpsc::Receiver<OpenRequest<T>>) -> Self {
        Self {
            opener,
            next_new_token: Token(0),
            ready: Arc::new(Mutex::new(ReadyTree::new())),
            queues: BTreeMap::new(),
            recv_queue_start: Token(0),
        }
    }
    pub async fn recv(&mut self) -> Option<(Token, ReceiverRecv<T>)> {
        struct FairReceiverRecv<'a, T>(&'a mut Receiver<T>);
        impl<T> Future for FairReceiverRecv<'_, T> {
            type Output = Option<(Token, ReceiverRecv<T>)>;
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let this = self.deref_mut();
                this.0.poll_recv(cx)
            }
        }
        FairReceiverRecv(self).await
    }
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<(Token, ReceiverRecv<T>)>> {
        if self.queues.len() != MAX_QUEUE_COUNT {
            match self.opener.poll_recv(cx) {
                Poll::Ready(None) => (),
                Poll::Ready(Some(open_req)) => {
                    let (tx, mut rx) = mpsc::channel(DATA_QUEUE_SIZE);
                    assert!(rx.poll_recv(cx).is_pending(), "register waker");
                    let new_token = loop {
                        let token = self.next_new_token;
                        self.next_new_token = Token(self.next_new_token.0.wrapping_add(1));
                        if !self.queues.contains_key(&token) {
                            break token;
                        }
                    };
                    let resp = OpenResponse {
                        dedicated_chan: tx,
                        token: new_token,
                        ready: self.ready.clone(),
                    };
                    if open_req.resp.send(resp).is_ok() {
                        self.queues.insert(new_token, rx);
                    }
                    return Some((new_token, ReceiverRecv::Open(open_req.opening_value))).into();
                }
                Poll::Pending => (),
            }
        }
        loop {
            let token = {
                let ready = self.ready.lock().unwrap();
                if ready.is_empty() {
                    break;
                }
                let Some(token) = ready.next(self.recv_queue_start) else {
                    self.recv_queue_start = Token(0);
                    continue;
                };
                token
            };
            let queue = self.queues.get_mut(&token).unwrap();
            self.recv_queue_start = Token(token.0.wrapping_add(1));
            match queue.poll_recv(cx) {
                Poll::Ready(Some(value)) => {
                    self.ready.lock().unwrap().sub(token);
                    return Some((token, ReceiverRecv::Value(value))).into();
                }
                Poll::Ready(None) => {
                    {
                        let mut ready = self.ready.lock().unwrap();
                        ready.sub(token);
                        assert!(!ready.spurious_unready(token).is_spurious);
                    }
                    self.queues.remove(&token);
                    return Some((token, ReceiverRecv::Close)).into();
                }
                Poll::Pending => {
                    if self
                        .ready
                        .lock()
                        .unwrap()
                        .spurious_unready(token)
                        .is_spurious
                    {
                        return Poll::Pending;
                    };
                }
            }
        }
        let nothing_else_to_poll = self.opener.is_closed() && self.queues.is_empty();
        if nothing_else_to_poll {
            None.into()
        } else {
            Poll::Pending
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub enum ReceiverRecv<T> {
    Open(T),
    Value(T),
    Close,
}

#[derive(Debug)]
struct OpenRequest<T> {
    pub opening_value: T,
    pub resp: oneshot::Sender<OpenResponse<T>>,
}
#[derive(Debug)]
struct OpenResponse<T> {
    pub dedicated_chan: mpsc::Sender<T>,
    pub token: Token,
    pub ready: Arc<Mutex<ReadyTree>>,
}

fn ready_incr<'a>(tree: &'a Mutex<ReadyTree>, token: Token) -> UndoGuard<impl FnMut() + 'a> {
    tree.lock().unwrap().add(token);
    UndoGuard::new(move || {
        tree.lock().unwrap().sub(token);
    })
}

#[derive(Debug, Clone)]
struct ReadyTree {
    ready_count: BTreeMap<Token, usize>,
}
impl ReadyTree {
    pub fn new() -> Self {
        Self {
            ready_count: BTreeMap::new(),
        }
    }
    pub fn add(&mut self, token: Token) {
        let count = self.ready_count.entry(token).or_insert(0);
        *count += 1;
    }
    pub fn sub(&mut self, token: Token) {
        let Some(count) = self.ready_count.get_mut(&token) else {
            return;
        };
        match *count {
            0 => {
                panic!();
                // self.ready_count.remove(&token);
            }
            _ => *count -= 1,
        }
    }
    pub fn spurious_unready(&mut self, token: Token) -> UnreadyResult {
        let Some(count) = self.ready_count.get(&token) else {
            return UnreadyResult { is_spurious: true };
        };
        let is_spurious = *count != 0;
        if is_spurious {
            return UnreadyResult { is_spurious: true };
        }
        self.ready_count.remove(&token);
        UnreadyResult { is_spurious: false }
    }
    pub fn is_empty(&self) -> bool {
        self.ready_count.is_empty()
    }
    pub fn next(&self, start: Token) -> Option<Token> {
        let (token, _) = self.ready_count.range(start..).next()?;
        Some(*token)
    }
}

#[derive(Debug, Clone)]
pub struct UnreadyResult {
    pub is_spurious: bool,
}

#[derive(Debug)]
pub struct UndoGuard<F: FnMut()> {
    no_undo: bool,
    undo: F,
}
impl<F: FnMut()> Drop for UndoGuard<F> {
    fn drop(&mut self) {
        if self.no_undo {
            return;
        }
        (self.undo)();
    }
}
impl<F: FnMut()> UndoGuard<F> {
    pub fn new(undo: F) -> Self {
        Self {
            no_undo: false,
            undo,
        }
    }
    pub fn cancel(&mut self) {
        self.no_undo = true;
    }
}

#[derive(Debug)]
struct NotClone<T>(pub T);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Token(pub usize);

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn open_send_close() {
        let (opener, mut receiver) = channel();
        for _ in 0..24 {
            let opener = opener.clone();
            tokio::spawn(async move {
                let sender = opener.open(0).await.unwrap();
                sender.send(1).await.unwrap();
            });
            tokio::time::sleep(Duration::from_millis(100)).await;
            let (token_1, res) = receiver.recv().await.unwrap();
            match res {
                ReceiverRecv::Open(value) => assert_eq!(value, 0),
                ReceiverRecv::Value(_) => panic!(),
                ReceiverRecv::Close => panic!(),
            }
            let (token_2, res) = receiver.recv().await.unwrap();
            match res {
                ReceiverRecv::Open(_) => panic!(),
                ReceiverRecv::Value(value) => assert_eq!(value, 1),
                ReceiverRecv::Close => panic!(),
            }
            let (token_3, res) = receiver.recv().await.unwrap();
            match res {
                ReceiverRecv::Open(_) => panic!(),
                ReceiverRecv::Value(_) => panic!(),
                ReceiverRecv::Close => (),
            }
            assert_eq!(token_1, token_2);
            assert_eq!(token_1, token_3);
        }
    }
}
