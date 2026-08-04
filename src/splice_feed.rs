use tokio::sync::mpsc;

use crate::stream_migration::{
    GenerationReader, MigrationError, ResumeHeader, SpliceRegistry, SplicedReader,
    spawn_splice_driver, splice_driver_panics,
};

#[derive(Debug, Clone)]
pub struct SpliceFeedHandle {
    cont_tx: mpsc::UnboundedSender<(ResumeHeader, GenerationReader)>,
    register_tx: mpsc::UnboundedSender<(u64, tokio::sync::oneshot::Sender<SplicedReader>)>,
}

impl SpliceFeedHandle {
    pub(crate) fn send_continuation(
        &self,
        header: ResumeHeader,
        reader: GenerationReader,
    ) -> Result<(), ()> {
        self.cont_tx.send((header, reader)).map_err(|_| ())
    }

    pub(crate) fn expect_gen0(
        &self,
        logical_id: u64,
    ) -> tokio::sync::oneshot::Receiver<SplicedReader> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.register_tx.send((logical_id, tx));
        rx
    }
}

#[derive(Debug)]
pub struct SpliceFeed {
    handle: SpliceFeedHandle,
    matcher: tokio::task::JoinHandle<()>,
    driver: tokio::task::JoinHandle<Result<(), MigrationError>>,
}

pub(crate) const MAX_UNCLAIMED_GEN0: usize = 64;

impl SpliceFeed {
    pub fn handle(&self) -> SpliceFeedHandle {
        self.handle.clone()
    }

    pub(crate) fn abort(&self) {
        let panics = splice_driver_panics();
        if panics != 0 {
            tracing::warn!(panics, "splice driver has panicked since the feed started");
        }
        self.driver.abort();
        self.matcher.abort();
    }
}

pub fn spawn_splice_feed() -> SpliceFeed {
    let (cont_tx, cont_rx) = mpsc::unbounded_channel();
    let (gen0_tx, mut gen0_rx) = mpsc::unbounded_channel::<(u64, Option<SplicedReader>)>();
    let (register_tx, mut register_rx) =
        mpsc::unbounded_channel::<(u64, tokio::sync::oneshot::Sender<SplicedReader>)>();
    let driver = spawn_splice_driver(SpliceRegistry::new(), cont_rx, gen0_tx);
    let matcher = tokio::spawn(async move {
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
                            if ready.len() >= MAX_UNCLAIMED_GEN0 {
                                if let Some((evicted_id, _)) = ready.pop_front() {
                                    tracing::debug!(
                                        evicted_id,
                                        "splice feed evicted an unclaimed gen-0 reader (ready queue full)"
                                    );
                                }
                            }
                            ready.push_back((id, spliced));
                        }
                    },
                    None => break,
                },
            }
        }
    });
    SpliceFeed {
        handle: SpliceFeedHandle {
            cont_tx,
            register_tx,
        },
        matcher,
        driver,
    }
}
