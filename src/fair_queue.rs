use std::{
    future::Future,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
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
    pub async fn open(&self) -> Option<Sender<T>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = OpenRequest {
            dedicated_chan: resp_tx,
        };
        match self.opener.send(req).await {
            Ok(_) => (),
            Err(_) => return None,
        };
        let queue = match resp_rx.await {
            Ok(queue) => queue,
            Err(_) => return None,
        };
        Some(Sender::new(queue))
    }
    pub fn lazy_open(&self) -> LazySender<T> {
        LazySender::new(self.clone())
    }
}
#[derive(Debug)]
pub struct Sender<T> {
    queue: mpsc::Sender<T>,
}
impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
        }
    }
}
impl<T> Sender<T> {
    fn new(queue: mpsc::Sender<T>) -> Self {
        Self { queue }
    }
    pub fn try_send(&self, value: T) -> Result<(), mpsc::error::TrySendError<T>> {
        self.queue.try_send(value)
    }
    pub async fn send(&self, value: T) -> Result<(), mpsc::error::SendError<T>> {
        self.queue.send(value).await
    }
}
#[derive(Debug)]
pub struct LazySender<T> {
    opener: Opener<T>,
    sender: Option<NotClone<Sender<T>>>,
}
impl<T> LazySender<T> {
    fn new(opener: Opener<T>) -> Self {
        Self {
            opener,
            sender: None,
        }
    }
    async fn ensure_sender(&mut self) -> Option<&Sender<T>> {
        if self.sender.is_none() {
            let sender = self.opener.open().await?;
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
#[derive(Debug)]
pub struct Receiver<T> {
    opener: mpsc::Receiver<OpenRequest<T>>,
    queues: Vec<mpsc::Receiver<T>>,
    next_queue: usize,
}
impl<T> Receiver<T> {
    fn new(opener: mpsc::Receiver<OpenRequest<T>>) -> Self {
        Self {
            opener,
            queues: vec![],
            next_queue: 0,
        }
    }
    pub async fn recv(&mut self) -> Option<T> {
        struct FairReceiverRecv<'a, T>(&'a mut Receiver<T>);
        impl<T> Future for FairReceiverRecv<'_, T> {
            type Output = Option<T>;
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let this = self.deref_mut();
                this.0.poll_recv(cx)
            }
        }
        FairReceiverRecv(self).await
    }
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        if self.queues.len() != MAX_QUEUE_COUNT {
            match self.opener.poll_recv(cx) {
                Poll::Ready(None) => return None.into(),
                Poll::Ready(Some(open_req)) => {
                    let (tx, rx) = mpsc::channel(DATA_QUEUE_SIZE);
                    if open_req.dedicated_chan.send(tx).is_ok() {
                        self.queues.push(rx);
                    }
                }
                Poll::Pending => (),
            };
        }
        for _ in 0..self.queues.len() {
            let queue = &mut self.queues[self.next_queue];
            match queue.poll_recv(cx) {
                Poll::Ready(Some(value)) => {
                    self.bump_round_robin();
                    return Some(value).into();
                }
                Poll::Ready(None) => {
                    self.queues.swap_remove(self.next_queue);
                    if self.queues.len() == self.next_queue {
                        self.next_queue = 0;
                    }
                }
                Poll::Pending => {
                    self.bump_round_robin();
                }
            }
        }
        Poll::Pending
    }
    fn bump_round_robin(&mut self) {
        self.next_queue = (self.next_queue + 1) % self.queues.len();
    }
}

#[derive(Debug)]
struct OpenRequest<T> {
    dedicated_chan: oneshot::Sender<mpsc::Sender<T>>,
}

#[derive(Debug)]
struct NotClone<T>(pub T);
