use std::future::Future;
use std::pin::Pin;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use crate::migration_wire::{
    GenerationReader, MigrationError, ResumeHeader, SpliceRegistry, SplicedReader,
    spawn_splice_driver,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpliceRouterState {
    Running,
    Stopped,
    DriverFailed,
    ChildPanicked,
    ChildJoinFailed,
}

#[derive(Debug, Clone)]
pub struct SpliceRouterHandle {
    cont_tx: mpsc::Sender<(ResumeHeader, GenerationReader)>,
    register_tx: mpsc::Sender<(u64, tokio::sync::oneshot::Sender<SplicedReader>)>,
    state: watch::Receiver<SpliceRouterState>,
}

impl SpliceRouterHandle {
    pub(crate) async fn send_continuation(
        &self,
        header: ResumeHeader,
        reader: GenerationReader,
    ) -> Result<(), ()> {
        self.cont_tx.send((header, reader)).await.map_err(|_| ())
    }

    pub(crate) async fn await_gene(
        &self,
        logical_id: u64,
    ) -> Result<tokio::sync::oneshot::Receiver<SplicedReader>, ()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.register_tx
            .send((logical_id, tx))
            .await
            .map_err(|_| ())?;
        Ok(rx)
    }

    pub fn state(&self) -> SpliceRouterState {
        *self.state.borrow()
    }
}

/// The typed exit of one supervised splice task, produced when the
/// [`SpliceRouter`]'s supervisor reaps its inner [`JoinSet`]. A panic in
/// either the matcher or the driver surfaces as [`SpliceTaskExit::Panicked`]
/// (with the panic message) instead of being silently swallowed; a task
/// aborted during shutdown surfaces as [`SpliceTaskExit::Cancelled`].
#[derive(Debug)]
pub enum SpliceTaskExit {
    /// The matcher task exited normally (its feed channels closed).
    MatcherDone,
    /// The splice driver exited with its final result.
    DriverDone(Result<(), MigrationError>),
    /// A supervised splice task panicked; carries the panic message.
    Panicked(String),
    /// A supervised splice task was cancelled (aborted during shutdown).
    Cancelled,
}

#[derive(Debug)]
pub struct SpliceRouter {
    handle: SpliceRouterHandle,
    _supervision: JoinSet<()>,
}

pub(crate) const MAX_UNCLAIMED_GEN0: usize = 64;
pub(crate) const SPLICE_CONT_CAPACITY: usize = 1024;
pub(crate) const SPLICE_GEN0_CAPACITY: usize = 64;
pub(crate) const SPLICE_REGISTER_CAPACITY: usize = 64;

fn panic_message(err: tokio::task::JoinError) -> String {
    let payload = err.into_panic();
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        format!("non-string panic payload: {payload:p}")
    }
}

fn join_error_to_exit(err: tokio::task::JoinError) -> SpliceTaskExit {
    if err.is_panic() {
        SpliceTaskExit::Panicked(panic_message(err))
    } else {
        SpliceTaskExit::Cancelled
    }
}

fn observe_exit(exit: &SpliceTaskExit) {
    match exit {
        SpliceTaskExit::Panicked(msg) => {
            tracing::error!(panic = %msg, "a supervised splice task panicked");
        }
        SpliceTaskExit::DriverDone(Err(error)) => {
            tracing::warn!(?error, "splice driver exited with an error");
        }
        SpliceTaskExit::DriverDone(Ok(())) | SpliceTaskExit::MatcherDone => {
            tracing::debug!("a supervised splice task exited cleanly");
        }
        SpliceTaskExit::Cancelled => {
            tracing::debug!("a supervised splice task was cancelled");
        }
    }
}

impl SpliceRouter {
    pub fn handle(&self) -> SpliceRouterHandle {
        self.handle.clone()
    }
}

fn spawn_splice_supervisor(
    children: Vec<Pin<Box<dyn Future<Output = SpliceTaskExit> + Send>>>,
    state_tx: watch::Sender<SpliceRouterState>,
) -> JoinSet<()> {
    let mut outer = JoinSet::new();
    outer.spawn(async move {
        let mut inner: JoinSet<SpliceTaskExit> = JoinSet::new();
        for child in children {
            inner.spawn(child);
        }
        let mut saw_driver_done = false;
        let mut saw_matcher_done = false;
        loop {
            let Some(joined) = inner.join_next().await else {
                break;
            };
            match joined {
                Ok(exit) => {
                    observe_exit(&exit);
                    match exit {
                        SpliceTaskExit::DriverDone(Err(_)) => {
                            let _ = state_tx.send(SpliceRouterState::DriverFailed);
                            inner.abort_all();
                            while inner.join_next().await.is_some() {}
                            return;
                        }
                        SpliceTaskExit::DriverDone(Ok(())) => saw_driver_done = true,
                        SpliceTaskExit::MatcherDone => saw_matcher_done = true,
                        SpliceTaskExit::Panicked(_) => {
                            let _ = state_tx.send(SpliceRouterState::ChildPanicked);
                            inner.abort_all();
                            while inner.join_next().await.is_some() {}
                            return;
                        }
                        SpliceTaskExit::Cancelled => {
                            let _ = state_tx.send(SpliceRouterState::ChildJoinFailed);
                            inner.abort_all();
                            while inner.join_next().await.is_some() {}
                            return;
                        }
                    }
                }
                Err(err) => {
                    let exit = join_error_to_exit(err);
                    observe_exit(&exit);
                    match exit {
                        SpliceTaskExit::Panicked(_) => {
                            let _ = state_tx.send(SpliceRouterState::ChildPanicked);
                        }
                        _ => {
                            let _ = state_tx.send(SpliceRouterState::ChildJoinFailed);
                        }
                    }
                    inner.abort_all();
                    while inner.join_next().await.is_some() {}
                    return;
                }
            }
            if saw_driver_done && saw_matcher_done {
                let _ = state_tx.send(SpliceRouterState::Stopped);
                return;
            }
        }
    });
    outer
}

pub fn spawn_splice_router() -> SpliceRouter {
    let (cont_tx, cont_rx) = mpsc::channel(SPLICE_CONT_CAPACITY);
    let (gen0_tx, mut gen0_rx) =
        mpsc::channel::<(u64, Option<SplicedReader>)>(SPLICE_GEN0_CAPACITY);
    let (register_tx, mut register_rx) = mpsc::channel::<(
        u64,
        tokio::sync::oneshot::Sender<SplicedReader>,
    )>(SPLICE_REGISTER_CAPACITY);

    let (state_tx, state_rx) = watch::channel(SpliceRouterState::Running);

    let children: Vec<Pin<Box<dyn Future<Output = SpliceTaskExit> + Send>>> = vec![
        Box::pin(async move {
            let driver = spawn_splice_driver(SpliceRegistry::new(), cont_rx, gen0_tx);
            SpliceTaskExit::DriverDone(driver.await)
        }),
        Box::pin(async move {
            let mut waiters: std::collections::HashMap<
                u64,
                tokio::sync::oneshot::Sender<SplicedReader>,
            > = std::collections::HashMap::new();
            let mut ready: std::collections::VecDeque<(u64, Option<SplicedReader>)> =
                std::collections::VecDeque::new();
            loop {
                tokio::select! {
                    reg = register_rx.recv() => match reg {
                        Some((id, tx)) => match ready.iter().position(|(parked, _)| *parked == id)
                            .and_then(|at| ready.remove(at)).map(|(_, spliced)| spliced)
                        {
                            Some(None) => drop(tx),
                            Some(Some(spliced)) => { let _ = tx.send(spliced); }
                            None => match waiters.entry(id) {
                                std::collections::hash_map::Entry::Occupied(_) => drop(tx),
                                std::collections::hash_map::Entry::Vacant(slot) => { slot.insert(tx); }
                            },
                        },
                        None => break,
                    },
                    gen0 = gen0_rx.recv() => match gen0 {
                        Some((id, spliced)) => match waiters.remove(&id) {
                            Some(tx) => {
                                if let Some(spliced) = spliced {
                                    let _ = tx.send(spliced);
                                }
                            }
                            None => {
                                if ready.len() >= MAX_UNCLAIMED_GEN0
                                    && let Some((evicted_id, _)) = ready.pop_front() {
                                        tracing::debug!(
                                            evicted_id,
                                            "splice feed evicted an unclaimed gen-0 reader (ready queue full)"
                                        );
                                    }
                                ready.push_back((id, spliced));
                            }
                        },
                        None => break,
                    },
                }
            }
            SpliceTaskExit::MatcherDone
        }),
    ];

    let _supervision = spawn_splice_supervisor(children, state_tx);

    SpliceRouter {
        handle: SpliceRouterHandle {
            cont_tx,
            register_tx,
            state: state_rx,
        },
        _supervision,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct DropCounter(Arc<AtomicUsize>);
    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    // Dropping the router aborts every supervised task (the JoinSet drop
    // is the abort backstop), not just leaks them.
    #[tokio::test]
    async fn dropping_the_router_aborts_its_children() {
        let (state_tx, _state_rx) = watch::channel(SpliceRouterState::Running);
        let dropped = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&dropped);
        let started = Arc::new(tokio::sync::Notify::new());
        let notify = Arc::clone(&started);
        let child: Pin<Box<dyn Future<Output = SpliceTaskExit> + Send>> = Box::pin(async move {
            let _guard = DropCounter(counter);
            notify.notify_waiters();
            std::future::pending::<()>().await;
            #[allow(unreachable_code)]
            SpliceTaskExit::MatcherDone
        });
        let supervision = spawn_splice_supervisor(vec![child], state_tx);
        // Let the child run so its guard actually exists before aborting;
        // an aborted-but-never-polled task never constructed it.
        started.notified().await;
        drop(supervision);
        // Abort is asynchronous: the runtime must poll the aborted task to
        // drop its future (and the guard).
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while dropped.load(Ordering::SeqCst) == 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "dropping the router did not abort its children"
        );
    }

    // A panic in a supervised child is surfaced as ChildPanicked on the
    // shared state channel without any explicit reap call.
    #[tokio::test]
    async fn splice_child_panic_updates_health_without_reap() {
        let (state_tx, state_rx) = watch::channel(SpliceRouterState::Running);
        let child: Pin<Box<dyn Future<Output = SpliceTaskExit> + Send>> = Box::pin(async move {
            panic!("intentional splice supervisor child panic");
            #[allow(unreachable_code)]
            SpliceTaskExit::MatcherDone
        });
        let _supervision = spawn_splice_supervisor(vec![child], state_tx);
        let mut state_rx = state_rx;
        tokio::time::timeout(std::time::Duration::from_secs(1), state_rx.changed())
            .await
            .expect("the supervisor never reported its child's panic")
            .expect("the state sender closed before reporting the panic");
        assert_eq!(*state_rx.borrow(), SpliceRouterState::ChildPanicked);
    }

    // A closed registration channel surfaces as an Err from await_gene so
    // the caller maps it to FeedDead instead of awaiting a receiver that
    // can never resolve.
    #[tokio::test]
    async fn closed_register_channel_is_returned_to_await_gene() {
        let mut router = spawn_splice_router();
        let (register_tx, register_rx) = mpsc::channel::<(
            u64,
            tokio::sync::oneshot::Sender<SplicedReader>,
        )>(SPLICE_REGISTER_CAPACITY);
        drop(register_rx);
        router.handle.register_tx = register_tx;
        assert!(
            router.handle.await_gene(7).await.is_err(),
            "a closed registration channel was silently ignored"
        );
    }
}
