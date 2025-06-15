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

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let (opener_tx, opener_rx) = mpsc::channel(OPENER_QUEUE_SIZE);
    let tx = Sender::new(opener_tx);
    let rx = Receiver::new(opener_rx);
    (tx, rx)
}
#[derive(Debug)]
pub struct Sender<T> {
    opener: mpsc::Sender<OpenRequest<T>>,
    queue: Option<NotClone<mpsc::Sender<T>>>,
}
impl<T> Sender<T> {
    fn new(opener: mpsc::Sender<OpenRequest<T>>) -> Self {
        Self {
            opener,
            queue: None,
        }
    }
    pub fn try_send(&mut self, value: T) -> Result<(), mpsc::error::TrySendError<T>> {
        let queue = match &self.queue {
            Some(NotClone(queue)) => queue,
            None => {
                let (resp_tx, mut resp_rx) = oneshot::channel();
                let req = OpenRequest {
                    dedicated_chan: resp_tx,
                };
                match self.opener.try_send(req) {
                    Ok(_) => (),
                    Err(e) => match e {
                        mpsc::error::TrySendError::Full(_) => {
                            return Err(mpsc::error::TrySendError::Full(value))
                        }
                        mpsc::error::TrySendError::Closed(_) => {
                            return Err(mpsc::error::TrySendError::Closed(value))
                        }
                    },
                };
                let queue = match resp_rx.try_recv() {
                    Ok(queue) => queue,
                    Err(_) => return Err(mpsc::error::TrySendError::Closed(value)),
                };
                let NotClone(queue) = self.queue.get_or_insert(NotClone(queue));
                queue
            }
        };
        queue.try_send(value)
    }
    pub async fn send(&mut self, value: T) -> Result<(), mpsc::error::SendError<T>> {
        let queue = match &self.queue {
            Some(NotClone(queue)) => queue,
            None => {
                let (resp_tx, resp_rx) = oneshot::channel();
                let req = OpenRequest {
                    dedicated_chan: resp_tx,
                };
                match self.opener.send(req).await {
                    Ok(_) => (),
                    Err(_) => return Err(mpsc::error::SendError(value)),
                };
                let queue = match resp_rx.await {
                    Ok(queue) => queue,
                    Err(_) => return Err(mpsc::error::SendError(value)),
                };
                let NotClone(queue) = self.queue.get_or_insert(NotClone(queue));
                queue
            }
        };
        queue.send(value).await
    }
}
impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            opener: self.opener.clone(),
            queue: None,
        }
    }
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
        struct FairReceiverFut<'a, T>(&'a mut Receiver<T>);
        impl<T> Future for FairReceiverFut<'_, T> {
            type Output = Option<T>;
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let this = self.deref_mut();
                this.0.poll_recv(cx)
            }
        }
        FairReceiverFut(self).await
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
