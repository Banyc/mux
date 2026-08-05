use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::migration_wire::{
    GenerationReader, MigrationError, ResumeHeader, SpliceRegistry, SplicedReader,
    spawn_splice_driver,
};

#[derive(Debug, Clone)]
pub struct SpliceRouterHandle {
    cont_tx: mpsc::UnboundedSender<(ResumeHeader, GenerationReader)>,
    register_tx: mpsc::UnboundedSender<(u64, tokio::sync::oneshot::Sender<SplicedReader>)>,
}

impl SpliceRouterHandle {
    pub(crate) fn send_continuation(
        &self,
        header: ResumeHeader,
        reader: GenerationReader,
    ) -> Result<(), ()> {
        self.cont_tx.send((header, reader)).map_err(|_| ())
    }

    pub(crate) fn await_gene(
        &self,
        logical_id: u64,
    ) -> tokio::sync::oneshot::Receiver<SplicedReader> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.register_tx.send((logical_id, tx));
        rx
    }
}

/// The typed exit of one supervised splice task, produced when the
/// [`SpliceRouter`] reaps its [`JoinSet`]. A panic in either the matcher
/// or the driver surfaces as [`SpliceTaskExit::Panicked`] (with the panic
/// message) instead of being silently swallowed; a task aborted during
/// shutdown surfaces as [`SpliceTaskExit::Cancelled`].
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
    tasks: JoinSet<SpliceTaskExit>,
}

pub(crate) const MAX_UNCLAIMED_GEN0: usize = 64;

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

    /// Reap a single completed splice task and return its typed exit.
    /// A panicked child is surfaced as [`SpliceTaskExit::Panicked`] (and
    /// logged), never silently swallowed.
    pub(crate) async fn reap_next(&mut self) -> Option<SpliceTaskExit> {
        let joined = self.tasks.join_next().await?;
        let exit = match joined {
            Ok(exit) => exit,
            Err(err) => join_error_to_exit(err),
        };
        observe_exit(&exit);
        Some(exit)
    }

    /// Reap every already-completed task without blocking, logging each
    /// exit.
    fn reap(&mut self) {
        while let Some(joined) = self.tasks.try_join_next() {
            let exit = match joined {
                Ok(exit) => exit,
                Err(err) => join_error_to_exit(err),
            };
            observe_exit(&exit);
        }
    }

    pub(crate) fn abort(&mut self) {
        self.reap();
        self.tasks.abort_all();
    }
}

pub fn spawn_splice_router() -> SpliceRouter {
    let (cont_tx, cont_rx) = mpsc::unbounded_channel();
    let (gen0_tx, mut gen0_rx) = mpsc::unbounded_channel::<(u64, Option<SplicedReader>)>();
    let (register_tx, mut register_rx) =
        mpsc::unbounded_channel::<(u64, tokio::sync::oneshot::Sender<SplicedReader>)>();
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        let driver = spawn_splice_driver(SpliceRegistry::new(), cont_rx, gen0_tx);
        SpliceTaskExit::DriverDone(driver.await)
    });
    tasks.spawn(async move {
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
    });
    SpliceRouter {
        handle: SpliceRouterHandle {
            cont_tx,
            register_tx,
        },
        tasks,
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
        let mut router = spawn_splice_router();
        let dropped = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&dropped);
        let started = Arc::new(tokio::sync::Notify::new());
        router.tasks.spawn({
            let started = started.clone();
            async move {
                let _guard = DropCounter(counter);
                started.notify_waiters();
                std::future::pending::<()>().await;
                #[allow(unreachable_code)]
                SpliceTaskExit::MatcherDone
            }
        });
        // Let the child run so its guard actually exists before aborting;
        // an aborted-but-never-polled task never constructed it.
        started.notified().await;
        drop(router);
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

    // When the feed channels close, the driver and matcher exit normally
    // and reaping drains both as typed exits (no abort involved).
    #[tokio::test]
    async fn normal_shutdown_drains_the_supervised_tasks() {
        let mut router = spawn_splice_router();
        // Sever the router's own senders so the driver and matcher see
        // their feed channels close and exit normally.
        router.handle.cont_tx = tokio::sync::mpsc::unbounded_channel().0;
        router.handle.register_tx = tokio::sync::mpsc::unbounded_channel().0;
        let mut exits = Vec::new();
        while let Some(exit) = router.reap_next().await {
            exits.push(exit);
        }
        assert_eq!(exits.len(), 2, "expected the driver and matcher to exit");
        assert!(
            exits
                .iter()
                .any(|e| matches!(e, SpliceTaskExit::DriverDone(Ok(())))),
            "the driver did not exit cleanly: {exits:?}"
        );
        assert!(
            exits.iter().any(|e| matches!(e, SpliceTaskExit::MatcherDone)),
            "the matcher did not exit cleanly: {exits:?}"
        );
    }

    // A panic in a supervised task is surfaced as a typed
    // SpliceTaskExit::Panicked when reaped, not silently swallowed.
    #[tokio::test]
    async fn a_supervised_task_panic_is_observed_when_reaped() {
        let mut router = spawn_splice_router();
        router.tasks.spawn(async {
            panic!("intentional splice task panic");
            #[allow(unreachable_code)]
            SpliceTaskExit::MatcherDone
        });
        let exit = router
            .reap_next()
            .await
            .expect("the panicking task should be reaped");
        match exit {
            SpliceTaskExit::Panicked(msg) => {
                assert!(
                    msg.contains("intentional splice task panic"),
                    "unexpected panic message: {msg}"
                );
            }
            other => panic!("expected a panic exit, got {other:?}"),
        }
    }
}
