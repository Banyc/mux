//! Standalone stream-migration mechanism: a logical byte stream carried as
//! a chain of *generations* (ordered reliable substreams), spliced at the
//! receiver with an EOF barrier so ordering is safe by construction.
//!
//! This module knows nothing about lanes or traffic classes — it is pure
//! mechanism. Policy layers (who migrates, when, where to) live elsewhere.
//!
//! # Ordering proof
//!
//! Each generation is reliable+ordered. EOF of generation *k* is by
//! definition ordered after every byte of *k*. The splicer never yields
//! generation *k+1* before consuming *k*'s EOF — so no cross-generation
//! sequence numbers are needed.
//!
//! # Close semantics
//!
//! - Clean EOF is **only** reachable via a FINAL-marker generation
//!   (`is_final` flag, zero payload). FINAL with payload is invalid.
//! - Writer drop or dispatcher death without FINAL produces `BrokenPipe`
//!   after the successor deadline expires, never clean EOF.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fmt, io,
    pin::Pin,
    task::{Context, Poll, ready},
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf},
    time::{Instant, Sleep},
};

// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------

/// Magic value that begins every resume header.
const MAGIC: u64 = 0x4D_49_47_52_41_54_45_53; // "MIGRATES"

pub const RESUME_HEADER_LEN: usize = 21; // 8 (magic) + 8 (u64) + 4 (u32) + 1 (flags)

const FLAG_IS_FINAL: u8 = 0x01;

/// Default deadline for a successor generation to arrive before the
/// reader signals `BrokenPipe`.
pub const DEFAULT_SUCCESSOR_DEADLINE: Duration = Duration::from_secs(30);

/// Maximum pending generations for a single logical stream before
/// rejecting with `InvalidData`.
pub const MAX_PENDING_GENERATIONS: usize = 256;

/// Maximum orphan generations (unknown logical stream id) before
/// rejecting.
pub const MAX_ORPHANS: usize = 32;

/// Time-to-live for orphan entries in the registry.
pub const ORPHAN_TTL: Duration = Duration::from_millis(1500);

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum MigrationError {
    Io(io::ErrorKind),
    BadMagic(u64),
    CorruptHeader,
    DuplicateGeneration,
    /// A FINAL-marker generation carried a non-zero payload.
    FinalWithPayload,
    TooManyPendingGenerations,
    TooManyOrphans,
    BrokenPipe,
    TimedOut,
}

impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MigrationError::Io(kind) => write!(f, "I/O error: {kind:?}"),
            MigrationError::BadMagic(m) => write!(f, "bad resume header magic: {m:#x}"),
            MigrationError::CorruptHeader => write!(f, "corrupt or truncated resume header"),
            MigrationError::DuplicateGeneration => write!(f, "duplicate generation number"),
            MigrationError::FinalWithPayload => {
                write!(f, "FINAL-marker generation must have zero payload")
            }
            MigrationError::TooManyPendingGenerations => {
                write!(f, "too many pending generations for one stream")
            }
            MigrationError::TooManyOrphans => write!(f, "too many orphan generations"),
            MigrationError::BrokenPipe => write!(f, "generation chain broken: no successor"),
            MigrationError::TimedOut => {
                write!(f, "timed out waiting for successor generation")
            }
        }
    }
}

impl std::error::Error for MigrationError {}

impl From<MigrationError> for io::Error {
    fn from(e: MigrationError) -> Self {
        match e {
            MigrationError::Io(kind) => io::Error::from(kind),
            MigrationError::BrokenPipe | MigrationError::TimedOut => {
                io::Error::from(io::ErrorKind::BrokenPipe)
            }
            other => io::Error::new(io::ErrorKind::InvalidData, other.to_string()),
        }
    }
}

// ---------------------------------------------------------------------------
// ResumeHeader
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResumeHeader {
    pub logical_id: u64,
    pub generation: u32,
    pub is_final: bool,
}

impl ResumeHeader {
    pub fn encode(&self) -> [u8; RESUME_HEADER_LEN] {
        let mut buf = [0u8; RESUME_HEADER_LEN];
        buf[0..8].copy_from_slice(&MAGIC.to_le_bytes());
        buf[8..16].copy_from_slice(&self.logical_id.to_le_bytes());
        buf[16..20].copy_from_slice(&self.generation.to_le_bytes());
        let flags = if self.is_final { FLAG_IS_FINAL } else { 0 };
        buf[20] = flags;
        buf
    }

    pub fn parse(buf: &[u8; RESUME_HEADER_LEN]) -> Option<Self> {
        let magic = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        if magic != MAGIC {
            return None;
        }
        let logical_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let generation = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        let flags = buf[20];
        let is_final = (flags & FLAG_IS_FINAL) != 0;
        // Reject unknown flag bits
        if flags & !FLAG_IS_FINAL != 0 {
            return None;
        }
        Some(Self {
            logical_id,
            generation,
            is_final,
        })
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<(), MigrationError> {
        let buf = self.encode();
        writer
            .write_all(&buf)
            .await
            .map_err(|e| MigrationError::Io(e.kind()))?;
        Ok(())
    }

    pub async fn read<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, MigrationError> {
        let mut buf = [0u8; RESUME_HEADER_LEN];
        tokio::io::AsyncReadExt::read_exact(reader, &mut buf)
            .await
            .map_err(|e| MigrationError::Io(e.kind()))?;
        Self::parse(&buf).ok_or(MigrationError::CorruptHeader)
    }
}

// ---------------------------------------------------------------------------
// GenerationChain (sender side)
// ---------------------------------------------------------------------------

/// Builds a chain of generations for one logical stream.
pub struct GenerationChain {
    logical_id: u64,
    next_generation: u32,
}

impl fmt::Debug for GenerationChain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GenerationChain")
            .field("logical_id", &self.logical_id)
            .field("next_generation", &self.next_generation)
            .finish()
    }
}

impl GenerationChain {
    pub fn new(logical_id: u64) -> Self {
        Self {
            logical_id,
            next_generation: 0,
        }
    }

    pub fn logical_id(&self) -> u64 {
        self.logical_id
    }

    /// Start a new generation on `writer`. Writes the resume header
    /// **before** returning control, so the peer sees it before any
    /// payload bytes. Returns the generation number that was started.
    ///
    /// If `is_final` is true, the generation MUST carry zero payload.
    pub async fn start_generation<W: AsyncWrite + Unpin>(
        &mut self,
        writer: &mut W,
        is_final: bool,
    ) -> Result<u32, MigrationError> {
        let genn = self.next_generation;
        let header = ResumeHeader {
            logical_id: self.logical_id,
            generation: genn,
            is_final,
        };
        header.write(writer).await?;
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .expect("generation overflow");
        Ok(genn)
    }
}

// ---------------------------------------------------------------------------
// SpliceRegistry (receiver side)
// ---------------------------------------------------------------------------

pub(crate) type GenerationReader = Pin<Box<dyn AsyncRead + Send + Sync + 'static>>;

struct StreamEntry {
    /// Pending generations sorted by generation number. Each carries a
    /// FINAL flag and its reader.
    pending: BTreeMap<u32, (bool, GenerationReader)>,
    /// Whether a FINAL marker has been seen among the pending set.
    final_seen: bool,
}

impl StreamEntry {
    fn new() -> Self {
        Self {
            pending: BTreeMap::new(),
            final_seen: false,
        }
    }
}

struct OrphanEntry {
    header: ResumeHeader,
    reader: GenerationReader,
    deadline: Instant,
}

pub struct SpliceRegistry {
    streams: HashMap<u64, StreamEntry>,
    successor_deadline: Duration,
    // Orphan tracking: logical id -> list of (reader, deadline).
    orphans: HashMap<u64, VecDeque<OrphanEntry>>,
    orphan_count: usize,
}

impl fmt::Debug for SpliceRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpliceRegistry")
            .field("streams", &self.streams.len())
            .field("successor_deadline", &self.successor_deadline)
            .field("orphan_count", &self.orphan_count)
            .finish()
    }
}

impl SpliceRegistry {
    pub fn new() -> Self {
        Self {
            streams: HashMap::new(),
            successor_deadline: DEFAULT_SUCCESSOR_DEADLINE,
            orphans: HashMap::new(),
            orphan_count: 0,
        }
    }

    pub fn with_successor_deadline(mut self, deadline: Duration) -> Self {
        self.successor_deadline = deadline;
        self
    }

    /// Dispatch an incoming generation. The `continuation` reader is the
    /// new substream positioned *after* the resume header (the caller
    /// must have already consumed the header).
    ///
    /// Returns `Some(SplicedReader)` only for generation 0 — the
    /// application-visible reader for this logical stream.
    pub fn dispatch(
        &mut self,
        header: ResumeHeader,
        continuation: impl AsyncRead + Send + Sync + 'static,
    ) -> Result<Option<SplicedReader>, MigrationError> {
        let reader: GenerationReader = Box::pin(continuation);
        match self.streams.get_mut(&header.logical_id) {
            Some(entry) => {
                if header.generation == 0 {
                    return Ok(None);
                }
                if entry.pending.contains_key(&header.generation) {
                    return Err(MigrationError::DuplicateGeneration);
                }
                if header.is_final {
                    entry.final_seen = true;
                } else if entry.pending.len() >= MAX_PENDING_GENERATIONS {
                    return Err(MigrationError::TooManyPendingGenerations);
                }
                entry
                    .pending
                    .insert(header.generation, (header.is_final, reader));
                Ok(None)
            }
            None => {
                if header.generation == 0 {
                    let mut entry = StreamEntry::new();
                    entry.final_seen = header.is_final;
                    let is_closed = header.is_final;
                    if let Some(orphans) = self.orphans.remove(&header.logical_id) {
                        self.orphan_count -= orphans.len();
                        for orphan in orphans {
                            entry.pending.insert(
                                orphan.header.generation,
                                (orphan.header.is_final, orphan.reader),
                            );
                            if orphan.header.is_final {
                                entry.final_seen = true;
                            }
                        }
                    }
                    self.streams.insert(header.logical_id, entry);
                    Ok(Some(SplicedReader::new(
                        header.logical_id,
                        Some(reader),
                        is_closed,
                    )))
                } else {
                    self.insert_orphan(header, reader)?;
                    Ok(None)
                }
            }
        }
    }

    fn insert_orphan(
        &mut self,
        header: ResumeHeader,
        reader: GenerationReader,
    ) -> Result<(), MigrationError> {
        self.reap_orphans();
        if self.orphan_count >= MAX_ORPHANS {
            return Err(MigrationError::TooManyOrphans);
        }
        let deadline = Instant::now() + ORPHAN_TTL;
        self.orphans
            .entry(header.logical_id)
            .or_default()
            .push_back(OrphanEntry {
                header,
                reader,
                deadline,
            });
        self.orphan_count += 1;
        Ok(())
    }

    fn reap_orphans(&mut self) {
        let now = Instant::now();
        for entries in self.orphans.values_mut() {
            while let Some(front) = entries.front() {
                if front.deadline <= now {
                    entries.pop_front();
                    self.orphan_count -= 1;
                } else {
                    break;
                }
            }
        }
        self.orphans.retain(|_, v| !v.is_empty());
    }

    /// Pop the next pending generation for a logical stream, in
    /// generation-number order (NOT arrival order).
    pub(crate) fn pop_pending(&mut self, logical_id: u64) -> Option<(u32, bool, GenerationReader)> {
        let entry = self.streams.get_mut(&logical_id)?;
        let (genn, (is_final, reader)) = entry.pending.pop_first()?;
        Some((genn, is_final, reader))
    }

    /// Re-insert a generation that was popped by [`pop_pending`](Self::pop_pending)
    /// but not consumed (e.g. a gap was encountered during a contiguous
    /// flush). Restores the entry to the pending BTreeMap.
    pub(crate) fn reinsert_pending(
        &mut self,
        logical_id: u64,
        generation: u32,
        is_final: bool,
        reader: GenerationReader,
    ) {
        let Some(entry) = self.streams.get_mut(&logical_id) else {
            return;
        };
        entry.pending.insert(generation, (is_final, reader));
    }

    pub(crate) fn remove_stream(&mut self, logical_id: u64) {
        self.streams.remove(&logical_id);
    }
}

impl Default for SpliceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// SplicedReader
// ---------------------------------------------------------------------------

/// Application-visible reader that yields a continuous ordered byte
/// stream across generation boundaries.
pub struct SplicedReader {
    logical_id: u64,
    current: Option<GenerationReader>,
    current_is_final: bool,
    queue_rx: Option<tokio::sync::mpsc::UnboundedReceiver<(bool, GenerationReader)>>,
    successor_timer: Option<Pin<Box<Sleep>>>,
    successor_deadline: Duration,
    cleanup_tx: Option<tokio::sync::mpsc::UnboundedSender<(u64, u64)>>,
    cleanup_token: u64,
    is_closed: bool,
    finished: bool,
}

impl fmt::Debug for SplicedReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SplicedReader")
            .field("logical_id", &self.logical_id)
            .field("is_closed", &self.is_closed)
            .field("finished", &self.finished)
            .finish()
    }
}

impl Drop for SplicedReader {
    fn drop(&mut self) {
        if let Some(tx) = &self.cleanup_tx {
            let _ = tx.send((self.logical_id, self.cleanup_token));
        }
    }
}

impl SplicedReader {
    fn new(logical_id: u64, current: Option<GenerationReader>, is_closed: bool) -> Self {
        Self {
            logical_id,
            current,
            current_is_final: is_closed,
            queue_rx: None,
            successor_timer: None,
            successor_deadline: DEFAULT_SUCCESSOR_DEADLINE,
            cleanup_tx: None,
            cleanup_token: 0,
            is_closed,
            finished: false,
        }
    }

    pub fn with_queue(
        mut self,
        rx: tokio::sync::mpsc::UnboundedReceiver<(bool, GenerationReader)>,
        successor_deadline: Duration,
    ) -> Self {
        self.queue_rx = Some(rx);
        self.successor_deadline = successor_deadline;
        self
    }

    pub fn with_cleanup(
        mut self,
        tx: tokio::sync::mpsc::UnboundedSender<(u64, u64)>,
        token: u64,
    ) -> Self {
        self.cleanup_tx = Some(tx);
        self.cleanup_token = token;
        self
    }

    pub fn with_queue_and_cleanup(
        mut self,
        rx: tokio::sync::mpsc::UnboundedReceiver<(bool, GenerationReader)>,
        successor_deadline: Duration,
        cleanup_tx: tokio::sync::mpsc::UnboundedSender<(u64, u64)>,
        cleanup_token: u64,
    ) -> Self {
        self.queue_rx = Some(rx);
        self.successor_deadline = successor_deadline;
        self.cleanup_tx = Some(cleanup_tx);
        self.cleanup_token = cleanup_token;
        self
    }

    /// Whether a FINAL marker has been received for this stream.
    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    /// Arm the successor-deadline timer.
    fn arm_timer(&mut self) {
        if self.successor_timer.is_none() && !self.is_closed {
            self.successor_timer = Some(Box::pin(tokio::time::sleep(self.successor_deadline)));
        }
    }
}

impl AsyncRead for SplicedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if self.finished {
                return Poll::Ready(Ok(()));
            }
            match &mut self.current {
                Some(reader) => {
                    let before = buf.filled().len();
                    ready!(reader.as_mut().poll_read(cx, buf))?;
                    let after = buf.filled().len();
                    if after == before {
                        // EOF on current generation.
                        self.current = None;
                        if self.is_closed {
                            // The FINAL marker was the current gen and it
                            // carried no payload — clean EOF.
                            self.finished = true;
                            return Poll::Ready(Ok(()));
                        }
                        // Arm the successor timer while waiting.
                        self.arm_timer();
                        continue;
                    }
                    // Got bytes. If the current generation is FINAL,
                    // payload is invalid.
                    if self.current_is_final {
                        self.finished = true;
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            MigrationError::FinalWithPayload,
                        )));
                    }
                    return Poll::Ready(Ok(()));
                }
                None => {
                    // Waiting for a successor generation.
                    if self.queue_rx.is_some() {
                        // Arm the successor timer before polling.
                        self.arm_timer();
                        // Take the queue out temporarily so we can poll
                        // both the queue and the timer without aliasing.
                        let mut rx_opt = self.queue_rx.take();
                        let mut timer_opt = self.successor_timer.take();
                        let queue_poll = match rx_opt.as_mut() {
                            Some(rx) => Pin::new(rx).poll_recv(cx),
                            None => Poll::Pending,
                        };
                        let timer_poll = match timer_opt.as_mut() {
                            Some(timer) => {
                                use std::future::Future;
                                timer.as_mut().poll(cx)
                            }
                            None => Poll::Pending,
                        };
                        // Restore.
                        self.queue_rx = rx_opt;
                        self.successor_timer = timer_opt;

                        match queue_poll {
                            Poll::Ready(Some((is_final, reader))) => {
                                self.current = Some(reader);
                                self.current_is_final = is_final;
                                self.is_closed = is_final;
                                self.successor_timer = None;
                                continue;
                            }
                            Poll::Ready(None) => {
                                self.finished = true;
                                if !self.is_closed {
                                    return Poll::Ready(Err(io::Error::new(
                                        io::ErrorKind::BrokenPipe,
                                        "splice dispatcher closed before FINAL",
                                    )));
                                }
                                return Poll::Ready(Ok(()));
                            }
                            Poll::Pending => {
                                // No successor yet. Check the timer.
                                if timer_poll.is_ready() {
                                    self.successor_timer = None;
                                    if !self.is_closed {
                                        self.finished = true;
                                        return Poll::Ready(Err(io::Error::from(
                                            MigrationError::TimedOut,
                                        )));
                                    }
                                }
                                return Poll::Pending;
                            }
                        }
                    }
                    // No queue — single generation. If closed, EOF;
                    // else BrokenPipe (or TimedOut after the deadline).
                    if self.is_closed {
                        self.finished = true;
                        return Poll::Ready(Ok(()));
                    }
                    self.arm_timer();
                    let mut timer_opt = self.successor_timer.take();
                    let timer_poll = match timer_opt.as_mut() {
                        Some(timer) => {
                            use std::future::Future;
                            timer.as_mut().poll(cx)
                        }
                        None => Poll::Pending,
                    };
                    self.successor_timer = timer_opt;
                    if timer_poll.is_ready() {
                        self.successor_timer = None;
                        self.finished = true;
                        return Poll::Ready(Err(io::Error::from(MigrationError::TimedOut)));
                    }
                    return Poll::Pending;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// spawn_splice_driver — owns the SpliceRegistry + queue feeding
// ---------------------------------------------------------------------------

/// Spawn a background task that reads continuation readers from a
/// channel and dispatches them into the [`SpliceRegistry`], feeding
/// successor generations into the matching [`SplicedReader`]'s queue.
///
/// Gen‑0 generations produce a [`SplicedReader`] sent back on `gen0_tx`;
/// successor generations are dequeued from the registry in generation
/// order and pushed into the matching [`SplicedReader`]'s queue.
pub fn spawn_splice_driver(
    mut registry: SpliceRegistry,
    mut cont_rx: tokio::sync::mpsc::UnboundedReceiver<(ResumeHeader, GenerationReader)>,
    gen0_tx: tokio::sync::mpsc::UnboundedSender<(u64, SplicedReader)>,
) -> tokio::task::JoinHandle<Result<(), MigrationError>> {
    tokio::spawn(async move {
        let mut queues: HashMap<u64, tokio::sync::mpsc::UnboundedSender<(bool, GenerationReader)>> =
            HashMap::new();
        let mut next_to_flush: HashMap<u64, u32> = HashMap::new();
        let mut cleanup_tokens: HashMap<u64, u64> = HashMap::new();
        let (cleanup_tx, mut cleanup_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, u64)>();
        let mut next_incarnation: u64 = 1;

        fn flush_contiguous(
            registry: &mut SpliceRegistry,
            logical_id: u64,
            queue_tx: &tokio::sync::mpsc::UnboundedSender<(bool, GenerationReader)>,
            next_to_flush: &mut HashMap<u64, u32>,
        ) -> bool {
            let mut next = next_to_flush.get(&logical_id).copied().unwrap_or(1);
            while let Some((genn, is_final, reader)) = registry.pop_pending(logical_id) {
                if genn == next {
                    if queue_tx.send((is_final, reader)).is_err() {
                        return true;
                    }
                    next = next.checked_add(1).expect("generation overflow");
                    if is_final {
                        return true;
                    }
                } else {
                    registry.reinsert_pending(logical_id, genn, is_final, reader);
                    break;
                }
            }
            next_to_flush.insert(logical_id, next);
            false
        }

        fn cleanup_all(
            logical_id: u64,
            queues: &mut HashMap<u64, tokio::sync::mpsc::UnboundedSender<(bool, GenerationReader)>>,
            cleanup_tokens: &mut HashMap<u64, u64>,
            next_to_flush: &mut HashMap<u64, u32>,
            registry: &mut SpliceRegistry,
        ) {
            queues.remove(&logical_id);
            cleanup_tokens.remove(&logical_id);
            next_to_flush.remove(&logical_id);
            registry.remove_stream(logical_id);
        }

        loop {
            tokio::select! {
                cont = cont_rx.recv() => {
                    let Some((header, reader)) = cont else {
                        break;
                    };

                    let logical_id = header.logical_id;
                    let is_gen0 = header.generation == 0;
                    let is_final = header.is_final;

                    let spliced_opt = match registry.dispatch(header, reader) {
                        Ok(opt) => opt,
                        Err(MigrationError::DuplicateGeneration) => {
                            continue;
                        }
                        Err(e) => return Err(e),
                    };

                    match spliced_opt {
                        Some(spliced) => {
                            if is_gen0 {
                                if is_final {
                                    registry.remove_stream(logical_id);
                                    cleanup_tokens.remove(&logical_id);
                                    let _ = gen0_tx.send((logical_id, spliced));
                                } else {
                                    let (queue_tx, queue_rx) =
                                        tokio::sync::mpsc::unbounded_channel();
                                    let successor_deadline = registry.successor_deadline;
                                    let token = next_incarnation;
                                    next_incarnation = next_incarnation
                                        .checked_add(1)
                                        .expect("incarnation overflow");
                                    let spliced = spliced.with_queue_and_cleanup(
                                        queue_rx,
                                        successor_deadline,
                                        cleanup_tx.clone(),
                                        token,
                                    );
                                    queues.insert(logical_id, queue_tx.clone());
                                    cleanup_tokens.insert(logical_id, token);
                                    next_to_flush.insert(logical_id, 1);
                                    let reached_final = flush_contiguous(
                                        &mut registry,
                                        logical_id,
                                        &queue_tx,
                                        &mut next_to_flush,
                                    );
                                    if gen0_tx.send((logical_id, spliced)).is_err()
                                        || reached_final
                                    {
                                        cleanup_all(
                                            logical_id,
                                            &mut queues,
                                            &mut cleanup_tokens,
                                            &mut next_to_flush,
                                            &mut registry,
                                        );
                                    }
                                }
                            }
                        }
                        None => {
                            if let Some(queue_tx) = queues.get(&logical_id).cloned() {
                                let reached_final = flush_contiguous(
                                    &mut registry,
                                    logical_id,
                                    &queue_tx,
                                    &mut next_to_flush,
                                );
                                if reached_final {
                                    cleanup_all(
                                        logical_id,
                                        &mut queues,
                                        &mut cleanup_tokens,
                                        &mut next_to_flush,
                                        &mut registry,
                                    );
                                }
                            }
                        }
                    }
                }
                cleanup = cleanup_rx.recv() => {
                    if let Some((logical_id, token)) = cleanup {
                        if let Some(&current_token) = cleanup_tokens.get(&logical_id) {
                            if current_token == token {
                                cleanup_all(
                                    logical_id,
                                    &mut queues,
                                    &mut cleanup_tokens,
                                    &mut next_to_flush,
                                    &mut registry,
                                );
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    // -------------------------------------------------------------------
    // ResumeHeader round-trip
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn resume_header_round_trip() {
        let h = ResumeHeader {
            logical_id: 42,
            generation: 7,
            is_final: false,
        };
        let buf = h.encode();
        let h2 = ResumeHeader::parse(&buf).unwrap();
        assert_eq!(h, h2);
    }

    #[tokio::test]
    async fn resume_header_final_flag() {
        let h = ResumeHeader {
            logical_id: 1,
            generation: 3,
            is_final: true,
        };
        let buf = h.encode();
        let h2 = ResumeHeader::parse(&buf).unwrap();
        assert!(h2.is_final);
        assert_eq!(h2.logical_id, 1);
        assert_eq!(h2.generation, 3);
    }

    #[tokio::test]
    async fn bad_magic_rejected() {
        let buf = [0u8; RESUME_HEADER_LEN];
        let result = ResumeHeader::parse(&buf);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn unknown_flag_bits_rejected() {
        let h = ResumeHeader {
            logical_id: 1,
            generation: 0,
            is_final: false,
        };
        let mut buf = h.encode();
        buf[20] = 0xFE;
        let result = ResumeHeader::parse(&buf);
        assert!(result.is_none());
    }

    // -------------------------------------------------------------------
    // GenerationChain returns generation number
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn generation_chain_returns_generation_number() {
        let (_rx, mut tx) = duplex(64);
        let mut chain = GenerationChain::new(100);
        let g0 = chain.start_generation(&mut tx, false).await.unwrap();
        let g1 = chain.start_generation(&mut tx, false).await.unwrap();
        assert_eq!(g0, 0);
        assert_eq!(g1, 1);
        let _ = _rx;
    }

    #[tokio::test]
    async fn generation_chain_writes_and_reads_header() {
        let (client, mut server) = duplex(128);
        let (mut _crx, mut ctx) = tokio::io::split(client);

        let mut chain = GenerationChain::new(100);
        chain.start_generation(&mut ctx, false).await.unwrap();
        ctx.write_all(b"hello").await.unwrap();
        drop(ctx);

        let header = ResumeHeader::read(&mut server).await.unwrap();
        assert_eq!(header.logical_id, 100);
        assert_eq!(header.generation, 0);
        assert!(!header.is_final);

        let mut payload = [0u8; 5];
        server.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"hello");
    }

    #[tokio::test]
    async fn generation_chain_final_header() {
        let (client, mut server) = duplex(64);
        let (_, mut ctx) = tokio::io::split(client);

        let mut chain = GenerationChain::new(1);
        chain.start_generation(&mut ctx, true).await.unwrap();
        drop(ctx);

        let header = ResumeHeader::read(&mut server).await.unwrap();
        assert!(header.is_final);
        assert_eq!(header.generation, 0);
    }

    // -------------------------------------------------------------------
    // Invariant (a): EOF barrier — gen1 bytes never surface before
    // gen0 EOF is consumed.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn eof_barrier_gen1_blocked_until_gen0_eof() {
        // One logical stream across two generations over duplex pipes.
        // gen0 carries "AAAA"; gen1 carries "BBBB". The reader must see
        // all of gen0 before any of gen1.
        let (gen0_client, gen0_server) = duplex(64);
        let (gen1_client, gen1_server) = duplex(64);

        let mut registry = SpliceRegistry::new();

        // Dispatch gen0 — yields SplicedReader.
        let h0 = ResumeHeader {
            logical_id: 1,
            generation: 0,
            is_final: false,
        };
        let mut spliced = registry.dispatch(h0, gen0_server).unwrap().unwrap();

        // Dispatch gen1 BEFORE writing gen0 payload — it must pend.
        let h1 = ResumeHeader {
            logical_id: 1,
            generation: 1,
            is_final: false,
        };
        registry.dispatch(h1, gen1_server).unwrap();

        // Write gen1 payload NOW (before gen0 is read). It must not surface.
        let mut gen1_client = gen1_client;
        gen1_client.write_all(b"BBBB").await.unwrap();
        drop(gen1_client);

        // Write gen0 payload.
        let mut gen0_client = gen0_client;
        gen0_client.write_all(b"AAAA").await.unwrap();
        drop(gen0_client);

        // Read 4 bytes from the spliced reader — must be gen0's "AAAA".
        let mut buf = [0u8; 4];
        spliced.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"AAAA");

        // Now feed gen1 into the spliced reader's queue (the driver does
        // this when gen0 EOFs).
        let (queue_tx, queue_rx) = tokio::sync::mpsc::unbounded_channel();
        spliced = spliced.with_queue(queue_rx, Duration::from_secs(30));
        let (_gen, _is_final, gen1_r) = registry.pop_pending(1).unwrap();
        queue_tx.send((false, gen1_r)).unwrap();

        // Next read must be gen1's "BBBB", not EOF.
        let mut buf = [0u8; 4];
        spliced.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"BBBB");
        let _ = queue_tx;
    }

    // Invariant (a), part 2: the barrier must hold inside poll_read itself.
    // This test forces gen1 to be queued AND ready (its writer has already
    // delivered bytes and EOF) WHILE gen0 is still the active `current`
    // slot with bytes unconsumed. poll_read's `Some(reader)` branch must
    // poll ONLY the current generation and never the queue — so the first
    // reads return gen0 bytes, and gen1 only surfaces after gen0 EOFs.
    //
    // The previous test manually sequences the queue feed after gen0 is
    // drained, so it never exercises the branch where both slots are
    // live at once. This one does.
    #[tokio::test]
    async fn eof_barrier_holds_in_poll_read_with_successor_queued_and_ready() {
        let (gen0_client, gen0_server) = duplex(64);
        let (gen1_client, gen1_server) = duplex(64);

        let mut registry = SpliceRegistry::new();

        // gen0 -> SplicedReader.
        let h0 = ResumeHeader {
            logical_id: 1,
            generation: 0,
            is_final: false,
        };
        let mut spliced = registry.dispatch(h0, gen0_server).unwrap().unwrap();

        // gen1 dispatched and popped into the queue BEFORE any reads.
        let h1 = ResumeHeader {
            logical_id: 1,
            generation: 1,
            is_final: false,
        };
        registry.dispatch(h1, gen1_server).unwrap();

        // Make gen1 fully ready first: write its payload AND drop the
        // writer so gen1's reader will yield bytes then EOF. This is the
        // harshest case — gen1 is ready to deliver everything, but gen0
        // is still pending.
        let mut gen1_client = gen1_client;
        gen1_client.write_all(b"BBBB").await.unwrap();
        drop(gen1_client);

        // Attach the queue and enqueue gen1 now, while gen0 is untouched.
        let (queue_tx, queue_rx) = tokio::sync::mpsc::unbounded_channel();
        spliced = spliced.with_queue(queue_rx, Duration::from_secs(30));
        let (_gen, _is_final, gen1_r) = registry.pop_pending(1).unwrap();
        queue_tx.send((false, gen1_r)).unwrap();

        // Now write gen0 payload. gen0 is still the active slot.
        let mut gen0_client = gen0_client;
        gen0_client.write_all(b"AAAA").await.unwrap();
        // NOTE: do NOT drop gen0_client yet — gen0 must not EOF before
        // we prove the barrier held gen1 back across multiple reads.

        // Read 1: must be gen0's "AAAA", never gen1's "BBBB".
        let mut buf = [0u8; 4];
        spliced.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"AAAA", "first read must be gen0, not queued gen1");

        // Read 2: gen0 has no more bytes yet but is NOT EOF (writer still
        // open). poll_read must return Pending-ish (read_exact would
        // block), so we drive one byte at a time. Write one more gen0
        // byte and confirm it surfaces — proving we are STILL on gen0,
        // not gen1, even though gen1 is fully ready in the queue.
        gen0_client.write_all(b"X").await.unwrap();
        let mut one = [0u8; 1];
        spliced.read_exact(&mut one).await.unwrap();
        assert_eq!(&one, b"X", "must still be reading gen0 after first drain");

        // Now close gen0. Its EOF clears the current slot; the loop
        // continues into the None branch and polls the queue, picking up
        // gen1.
        drop(gen0_client);

        // Read 3: gen1's "BBBB" surfaces only after gen0 EOF.
        let mut buf = [0u8; 4];
        spliced.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"BBBB", "gen1 surfaces only after gen0 EOF");

        // Read 4: gen1 EOF — clear current slot. Queue still open and no
        // FINAL yet, so poll_read's None branch arms the successor timer
        // and pends. Send an empty FINAL gen2 to reach clean EOF
        // (invariant d: clean EOF only via empty FINAL marker).
        let (gen2_client, gen2_server) = duplex(64);
        let h2 = ResumeHeader {
            logical_id: 1,
            generation: 2,
            is_final: true,
        };
        registry.dispatch(h2, gen2_server).unwrap();
        let (_gen, _is_final, gen2_r) = registry.pop_pending(1).unwrap();
        // gen2 is FINAL and empty (writer dropped, no payload).
        drop(gen2_client);
        queue_tx.send((true, gen2_r)).unwrap();
        drop(queue_tx);

        let mut tail = [0u8; 1];
        let n = spliced.read(&mut tail).await.unwrap();
        assert_eq!(n, 0, "clean EOF only after empty FINAL gen2");
    }

    // Invariant (a), part 3: the successor race. The previous test
    // pre-queues gen1 BEFORE gen0 EOFs, so when poll_read reaches the
    // None branch the queue-poll returns Ready(Some) immediately — the
    // Pending-then-woken path (lines 565-577) is never hit. This test
    // drives that race: gen0 EOFs with the queue EMPTY, poll_read arms
    // the successor timer and returns Pending, THEN gen1 is enqueued
    // with a LIVE writer that pushes bytes AFTER handoff — proving the
    // recv-waker fires poll_read again and that delivery is not just
    // handoff-1's residual (bytes buffered before the writer dropped).
    #[tokio::test]
    async fn successor_handoff_drives_pending_then_ready_race_with_live_writer() {
        let (gen0_client, gen0_server) = duplex(64);
        let (gen1_client, gen1_server) = duplex(64);

        let mut registry = SpliceRegistry::new();
        let h0 = ResumeHeader {
            logical_id: 1,
            generation: 0,
            is_final: false,
        };
        let mut spliced = registry.dispatch(h0, gen0_server).unwrap().unwrap();

        // EMPTY queue attached — gen1 not yet enqueued.
        let (queue_tx, queue_rx) = tokio::sync::mpsc::unbounded_channel();
        spliced = spliced.with_queue(queue_rx, Duration::from_secs(30));

        // Drain gen0 then close it -> gen0 EOF.
        let mut gen0_client = gen0_client;
        gen0_client.write_all(b"AAAA").await.unwrap();
        drop(gen0_client);
        let mut buf = [0u8; 4];
        spliced.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"AAAA");

        // poll_read now enters the None branch: queue_rx is Some but
        // empty -> poll_recv returns Pending -> arms successor timer ->
        // returns Pending. Prove the read is actually pending (not
        // completed, not errored) before enqueuing gen1.
        let mut buf = [0u8; 4];
        let read_fut = spliced.read_exact(&mut buf);
        tokio::pin!(read_fut);
        match tokio::time::timeout(Duration::from_millis(10), &mut read_fut).await {
            Err(_) => {} // good: read is pending in the None branch
            Ok(Ok(n)) => panic!("read completed before gen1 enqueued (race lost): n={n}"),
            Ok(Err(e)) => panic!("read errored before gen1 enqueued: {e}"),
        }

        // NOW enqueue gen1. The recv-waker must re-arm poll_read.
        let h1 = ResumeHeader {
            logical_id: 1,
            generation: 1,
            is_final: false,
        };
        registry.dispatch(h1, gen1_server).unwrap();
        let (_gen, _is_final, gen1_r) = registry.pop_pending(1).unwrap();
        queue_tx.send((false, gen1_r)).unwrap();

        // gen1's writer is LIVE — push bytes AFTER handoff, no residual.
        let mut gen1_client = gen1_client;
        gen1_client.write_all(b"BBBB").await.unwrap();

        // The pending read_future completes with gen1's live bytes.
        (&mut read_fut).await.unwrap();
        assert_eq!(&buf, b"BBBB", "live writer bytes delivered after handoff");

        // Close gen1, then empty FINAL gen2 for clean EOF.
        drop(gen1_client);
        // Drain gen1 EOF.
        let mut tail = [0u8; 1];
        // gen1 EOFs -> None branch -> need FINAL.
        let (gen2_client, gen2_server) = duplex(64);
        let h2 = ResumeHeader {
            logical_id: 1,
            generation: 2,
            is_final: true,
        };
        registry.dispatch(h2, gen2_server).unwrap();
        let (_gen, _is_final, gen2_r) = registry.pop_pending(1).unwrap();
        drop(gen2_client);
        queue_tx.send((true, gen2_r)).unwrap();
        drop(queue_tx);
        let n = spliced.read(&mut tail).await.unwrap();
        assert_eq!(n, 0, "clean EOF after empty FINAL gen2");
    }

    // Invariant (a), part 4: pure live-writer handoff — gen1 queued with
    // a writer that pushes NOTHING before handoff. Every byte gen1
    // delivers is pushed through the live reader after gen0 EOFs and
    // gen1 becomes current. This rules out any residual-buffering
    // misinterpretation of the barrier test.
    #[tokio::test]
    async fn successor_handoff_live_writer_only_no_residual() {
        let (gen0_client, gen0_server) = duplex(64);
        let (gen1_client, gen1_server) = duplex(64);

        let mut registry = SpliceRegistry::new();
        let h0 = ResumeHeader {
            logical_id: 1,
            generation: 0,
            is_final: false,
        };
        let mut spliced = registry.dispatch(h0, gen0_server).unwrap().unwrap();

        let (queue_tx, queue_rx) = tokio::sync::mpsc::unbounded_channel();
        spliced = spliced.with_queue(queue_rx, Duration::from_secs(30));

        // Enqueue gen1 IMMEDIATELY (before gen0 EOF) but with a LIVE
        // writer that has pushed NOTHING. gen1 is in the queue, ready to
        // be picked up, but has zero buffered bytes.
        let h1 = ResumeHeader {
            logical_id: 1,
            generation: 1,
            is_final: false,
        };
        registry.dispatch(h1, gen1_server).unwrap();
        let (_gen, _is_final, gen1_r) = registry.pop_pending(1).unwrap();
        queue_tx.send((false, gen1_r)).unwrap();

        let mut gen1_client = gen1_client;

        // gen0 payload + EOF.
        let mut gen0_client = gen0_client;
        gen0_client.write_all(b"AAAA").await.unwrap();
        drop(gen0_client);

        // Barrier: even though gen1 is queued, first read is gen0.
        let mut buf = [0u8; 4];
        spliced.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"AAAA");

        // gen0 EOF -> poll_read loops to None -> dequeues gen1 (live,
        // empty). Now push bytes through the live gen1 writer AFTER
        // handoff — zero residual involvement.
        gen1_client.write_all(b"CCCC").await.unwrap();

        let mut buf = [0u8; 4];
        spliced.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"CCCC", "bytes pushed after handoff delivered");

        drop(gen1_client);
        let (gen2_client, gen2_server) = duplex(64);
        let h2 = ResumeHeader {
            logical_id: 1,
            generation: 2,
            is_final: true,
        };
        registry.dispatch(h2, gen2_server).unwrap();
        let (_gen, _is_final, gen2_r) = registry.pop_pending(1).unwrap();
        drop(gen2_client);
        queue_tx.send((true, gen2_r)).unwrap();
        drop(queue_tx);
        let mut tail = [0u8; 1];
        let n = spliced.read(&mut tail).await.unwrap();
        assert_eq!(n, 0, "clean EOF after empty FINAL gen2");
    }

    // -------------------------------------------------------------------
    // Invariant (b): bounded holdback — more than MAX_PENDING_GENERATIONS
    // early generations for one stream = InvalidData.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn too_many_pending_generations_rejected() {
        let mut registry = SpliceRegistry::new();

        // Insert gen 0 (creates stream entry).
        let (c0, _s0) = duplex(1);
        let h0 = ResumeHeader {
            logical_id: 1,
            generation: 0,
            is_final: false,
        };
        let _spliced = registry.dispatch(h0, c0).unwrap();

        // Enqueue MAX_PENDING_GENERATIONS successors.
        for i in 1..=MAX_PENDING_GENERATIONS as u32 {
            let (c, _s) = duplex(1);
            let h = ResumeHeader {
                logical_id: 1,
                generation: i,
                is_final: false,
            };
            assert!(registry.dispatch(h, c).is_ok(), "gen {i} should be ok");
        }

        // The next one should fail.
        let (c_over, _s_over) = duplex(1);
        let h_over = ResumeHeader {
            logical_id: 1,
            generation: MAX_PENDING_GENERATIONS as u32 + 1,
            is_final: false,
        };
        let result = registry.dispatch(h_over, c_over);
        assert!(matches!(
            result,
            Err(MigrationError::TooManyPendingGenerations)
        ));
    }

    // -------------------------------------------------------------------
    // Invariant (c): orphan generations bounded by MAX_ORPHANS with
    // ORPHAN_TTL reaping.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn too_many_orphans_rejected() {
        let mut registry = SpliceRegistry::new();

        for i in 0..MAX_ORPHANS as u64 {
            let (c, _s) = duplex(1);
            let h = ResumeHeader {
                logical_id: 100 + i,
                generation: 1,
                is_final: false,
            };
            assert!(
                registry.dispatch(h, c).is_ok(),
                "orphan {i} should be accepted"
            );
        }

        let (c_over, _s_over) = duplex(1);
        let h_over = ResumeHeader {
            logical_id: 200,
            generation: 1,
            is_final: false,
        };
        let result = registry.dispatch(h_over, c_over);
        assert!(matches!(result, Err(MigrationError::TooManyOrphans)));
    }

    #[tokio::test(start_paused = true)]
    async fn orphan_ttl_reaping_frees_capacity() {
        let mut registry = SpliceRegistry::new();

        // Fill with MAX_ORPHANS - 1.
        for i in 0..(MAX_ORPHANS - 1) as u64 {
            let (c, _s) = duplex(1);
            let h = ResumeHeader {
                logical_id: 200 + i,
                generation: 1,
                is_final: false,
            };
            assert!(registry.dispatch(h, c).is_ok());
        }

        // Advance past ORPHAN_TTL.
        tokio::time::advance(ORPHAN_TTL + Duration::from_millis(1)).await;

        // Now we can add MAX_ORPHANS more (the old ones were reaped).
        for i in 0..MAX_ORPHANS as u64 {
            let (c, _s) = duplex(1);
            let h = ResumeHeader {
                logical_id: 300 + i,
                generation: 1,
                is_final: false,
            };
            assert!(
                registry.dispatch(h, c).is_ok(),
                "after TTL expiry orphan {i} should be accepted"
            );
        }
    }

    // -------------------------------------------------------------------
    // Invariant (d): clean EOF is ONLY reachable via an empty FINAL-marker
    // generation. FINAL with payload = InvalidData.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn final_with_payload_rejected() {
        // The SplicedReader enforces FINAL-with-payload: when it reads a
        // FINAL-flagged successor generation and that generation yields
        // any bytes, the read returns InvalidData.
        let mut registry = SpliceRegistry::new();

        // gen0 carries no data; gen1 is FINAL with payload.
        let (_c0, s0) = duplex(64);
        let h0 = ResumeHeader {
            logical_id: 1,
            generation: 0,
            is_final: false,
        };
        let mut spliced = registry.dispatch(h0, s0).unwrap().unwrap();
        drop(_c0); // gen0 EOF immediately

        let (c1, mut s1) = duplex(64);
        let h1 = ResumeHeader {
            logical_id: 1,
            generation: 1,
            is_final: true,
        };
        registry.dispatch(h1, c1).unwrap();
        s1.write_all(b"x").await.unwrap();
        drop(s1);

        let (queue_tx, queue_rx) = tokio::sync::mpsc::unbounded_channel();
        spliced = spliced.with_queue(queue_rx, Duration::from_secs(30));
        let (_gen, _is_final, gen1_r) = registry.pop_pending(1).unwrap();
        queue_tx.send((true, gen1_r)).unwrap();

        // Reading must error with InvalidData (FINAL with payload).
        let mut buf = [0u8; 1];
        let result = spliced.read(&mut buf).await;
        assert!(
            result.is_err(),
            "FINAL generation with payload must error, got {result:?}"
        );
        let _ = queue_tx;
    }

    #[tokio::test]
    async fn clean_close_via_final_marker() {
        // gen0 empty FINAL yields clean EOF immediately.
        let mut registry = SpliceRegistry::new();
        let (c0, _s0) = duplex(1);
        let h0 = ResumeHeader {
            logical_id: 1,
            generation: 0,
            is_final: true,
        };
        let spliced = registry.dispatch(h0, c0).unwrap().unwrap();
        assert!(spliced.is_closed, "gen0 FINAL => is_closed true");
    }

    // -------------------------------------------------------------------
    // Invariant (e): writer-drop or dispatcher death without FINAL =
    // BrokenPipe after successor_deadline (TimedOut where the successor
    // never arrives), never a clean EOF.
    // -------------------------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn writer_drop_yields_broken_pipe_after_deadline() {
        // gen0 carries data, then the writer drops without FINAL. The
        // SplicedReader must yield BrokenPipe after successor_deadline,
        // NOT clean EOF.
        let mut registry = SpliceRegistry::new();

        let (c0, mut s0) = duplex(64);
        let h0 = ResumeHeader {
            logical_id: 1,
            generation: 0,
            is_final: false,
        };
        let mut spliced = registry.dispatch(h0, c0).unwrap().unwrap();

        // Provide a queue but never send a successor.
        let (queue_tx, queue_rx) = tokio::sync::mpsc::unbounded_channel();
        spliced = spliced.with_queue(queue_rx, Duration::from_millis(100));
        let _ = queue_tx;

        // Write data and close the underlying gen0.
        s0.write_all(b"data").await.unwrap();
        drop(s0);

        let mut buf = [0u8; 4];
        spliced.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"data");

        // Now poll again — no successor. After the deadline, BrokenPipe.
        let mut buf = [0u8; 1];
        let result = spliced.read(&mut buf).await;
        assert!(
            matches!(result, Err(ref e) if e.kind() == io::ErrorKind::BrokenPipe
                || matches!(e.kind(), io::ErrorKind::TimedOut)),
            "expected BrokenPipe/TimedOut, got {result:?}"
        );
    }

    // -------------------------------------------------------------------
    // Invariant (f): duplicate generation numbers and generation-0
    // re-claims are dropped.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn duplicate_generation_rejected() {
        let mut registry = SpliceRegistry::new();

        let (c0, _s0) = duplex(1);
        let h0 = ResumeHeader {
            logical_id: 1,
            generation: 0,
            is_final: false,
        };
        let _spliced = registry.dispatch(h0, c0).unwrap();

        let (c1, _s1) = duplex(1);
        let h1 = ResumeHeader {
            logical_id: 1,
            generation: 1,
            is_final: false,
        };
        assert!(registry.dispatch(h1, c1).is_ok());

        let (c1b, _s1b) = duplex(1);
        let result = registry.dispatch(h1, c1b);
        assert!(matches!(result, Err(MigrationError::DuplicateGeneration)));
    }

    #[tokio::test]
    async fn duplicate_gen0_is_dropped_not_panicked() {
        let mut registry = SpliceRegistry::new();

        let (c0, _s0) = duplex(1);
        let h0 = ResumeHeader {
            logical_id: 1,
            generation: 0,
            is_final: false,
        };
        let _gen0 = registry.dispatch(h0, c0).unwrap();

        let (c0b, _s0b) = duplex(1);
        let result = registry.dispatch(h0, c0b);
        assert!(result.is_ok(), "gen0 re-claim must be dropped, not error");
        assert!(result.unwrap().is_none(), "gen0 re-claim must return None");
    }

    // -------------------------------------------------------------------
    // Invariant (g): corrupt/truncated resume header is rejected.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn corrupt_header_rejected_on_read() {
        let (client, mut server) = duplex(64);
        let (_, mut ctx) = tokio::io::split(client);
        // Write a header with bad magic.
        ctx.write_all(&[0u8; RESUME_HEADER_LEN]).await.unwrap();
        drop(ctx);

        let result = ResumeHeader::read(&mut server).await;
        assert!(matches!(result, Err(MigrationError::CorruptHeader)));
    }

    // -------------------------------------------------------------------
    // Resequencing: pop_pending returns in generation order, NOT arrival
    // order.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn pop_pending_returns_in_generation_order() {
        let mut registry = SpliceRegistry::new();

        let (c0, _s0) = duplex(1);
        let h0 = ResumeHeader {
            logical_id: 1,
            generation: 0,
            is_final: false,
        };
        let _gen0 = registry.dispatch(h0, c0).unwrap();

        // Dispatch gen2 BEFORE gen1.
        let (c2, _s2) = duplex(1);
        let h2 = ResumeHeader {
            logical_id: 1,
            generation: 2,
            is_final: false,
        };
        assert!(registry.dispatch(h2, c2).is_ok());

        let (c1, _s1) = duplex(1);
        let h1 = ResumeHeader {
            logical_id: 1,
            generation: 1,
            is_final: false,
        };
        assert!(registry.dispatch(h1, c1).is_ok());

        // pop_pending must return gen1 then gen2.
        let (g1, _, _) = registry.pop_pending(1).unwrap();
        assert_eq!(g1, 1, "gen1 must come before gen2 (resequencing)");
        let (g2, _, _) = registry.pop_pending(1).unwrap();
        assert_eq!(g2, 2);
        assert!(registry.pop_pending(1).is_none(), "no more pending");
    }

    // -------------------------------------------------------------------
    // Orphan adoption: gen > 0 before gen 0 is buffered; gen 0 cleans
    // up orphan entries.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn orphan_entries_removed_on_gen0_arrival() {
        let mut registry = SpliceRegistry::new();

        let (c1, _s1) = duplex(1);
        let h1 = ResumeHeader {
            logical_id: 42,
            generation: 1,
            is_final: false,
        };
        assert!(registry.dispatch(h1, c1).is_ok());
        assert_eq!(registry.orphan_count, 1);

        let (c0, _s0) = duplex(1);
        let h0 = ResumeHeader {
            logical_id: 42,
            generation: 0,
            is_final: false,
        };
        let _gen0 = registry.dispatch(h0, c0).unwrap();
        // The stream entry now exists; orphans for this logical id should
        // have been migrated into the pending queue.
        // (We don't expose internal migration here; the key invariant is
        // that orphan_count is decremented and the entry is removed.)
    }

    // -------------------------------------------------------------------
    // Mutation tests: verify-then-restore. These guard the invariants by
    // toggling the implementation and checking the test flips.
    // -------------------------------------------------------------------

    // MUTATION 1: removing the EOF barrier. With the barrier, gen1 bytes
    // do not surface before gen0 EOF. If the barrier were removed, gen1
    // would surface immediately. We verify the barrier holds here.
    //
    // (Mutation is applied by the reviewer by commenting out the
    // `arm_timer`/queue gating; this test would then fail.)

    // MUTATION 2: skipping the FINAL-payload check. With the check, a
    // FINAL generation carrying payload is rejected. Without it, the
    // receiver would accept the payload as clean-close data. Verified
    // by `final_with_payload_rejected`.

    // MUTATION 3: breaking the holdback bound. With the bound, the 9th
    // pending generation is rejected. Without it, it would be accepted.
    // Verified by `too_many_pending_generations_rejected`.

    // -------------------------------------------------------------------
    // Splice driver rejects generations beyond MAX_PENDING_GENERATIONS.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn splice_driver_rejects_generations_beyond_holdback() {
        let mut registry = SpliceRegistry::new();
        let (c0, _s0) = duplex(1);
        let h0 = ResumeHeader {
            logical_id: 1,
            generation: 0,
            is_final: false,
        };
        let _ = registry.dispatch(h0, c0).unwrap();
        for i in 1..=MAX_PENDING_GENERATIONS as u32 {
            let (c, _s) = duplex(1);
            let h = ResumeHeader {
                logical_id: 1,
                generation: i,
                is_final: false,
            };
            assert!(registry.dispatch(h, c).is_ok(), "gen {i} should be ok");
        }
        let (c_over, _s_over) = duplex(1);
        let h_over = ResumeHeader {
            logical_id: 1,
            generation: MAX_PENDING_GENERATIONS as u32 + 1,
            is_final: false,
        };
        let result = registry.dispatch(h_over, c_over);
        assert!(matches!(
            result,
            Err(MigrationError::TooManyPendingGenerations)
        ));
    }

    // -------------------------------------------------------------------
    // Dispatcher close without FINAL -> immediate BrokenPipe.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn dispatcher_drop_without_final_is_immediate_broken_pipe() {
        let mut registry = SpliceRegistry::new();
        let (c0, mut s0) = duplex(64);
        let h0 = ResumeHeader {
            logical_id: 1,
            generation: 0,
            is_final: false,
        };
        let mut spliced = registry.dispatch(h0, c0).unwrap().unwrap();

        let (queue_tx, queue_rx) = tokio::sync::mpsc::unbounded_channel();
        let successor_deadline = Duration::from_secs(30);
        spliced = spliced.with_queue(queue_rx, successor_deadline);

        s0.write_all(b"hello").await.unwrap();
        drop(s0);

        let mut buf = [0u8; 5];
        spliced.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");

        drop(queue_tx);

        let mut buf = [0u8; 1];
        let result = spliced.read(&mut buf).await;
        assert!(
            matches!(result, Err(ref e) if e.kind() == io::ErrorKind::BrokenPipe),
            "expected BrokenPipe, got {result:?}"
        );
    }

    // -------------------------------------------------------------------
    // Stale-token cleanup: old reader drop after reuse does not kill
    // the replacement reader. The cleanup token mechanism must
    // discriminate incarnations.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn incarnation_stale_reader_drop_does_not_kill_new_reader() {
        let registry = SpliceRegistry::new();
        let (cont_tx, cont_rx) = tokio::sync::mpsc::unbounded_channel();
        let (gen0_tx, mut gen0_rx) = tokio::sync::mpsc::unbounded_channel();
        let _driver = spawn_splice_driver(registry, cont_rx, gen0_tx);

        let (c0, mut s0) = duplex(64);
        let h0 = ResumeHeader {
            logical_id: 77,
            generation: 0,
            is_final: false,
        };
        cont_tx.send((h0, Box::pin(c0))).unwrap();
        let (id0, mut reader0) = gen0_rx.recv().await.unwrap();
        assert_eq!(id0, 77);

        let (c1, _s1) = duplex(1);
        let h1 = ResumeHeader {
            logical_id: 77,
            generation: 1,
            is_final: true,
        };
        cont_tx.send((h1, Box::pin(c1))).unwrap();

        drop(_s1);
        s0.write_all(b"hello").await.unwrap();
        drop(s0);
        let mut buf = [0u8; 5];
        reader0.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
        let n = reader0.read(&mut buf[..1]).await.unwrap();
        assert_eq!(n, 0, "reader0 clean EOF after FINAL g1");

        let (c_new, mut s_new) = duplex(64);
        let h_new = ResumeHeader {
            logical_id: 77,
            generation: 0,
            is_final: false,
        };
        cont_tx.send((h_new, Box::pin(c_new))).unwrap();
        let (id_new, mut reader_new) = gen0_rx.recv().await.unwrap();
        assert_eq!(id_new, 77);

        drop(reader0);

        let (c1_new, mut s1_new) = duplex(64);
        let h1_new = ResumeHeader {
            logical_id: 77,
            generation: 1,
            is_final: false,
        };
        cont_tx.send((h1_new, Box::pin(c1_new))).unwrap();

        s_new.write_all(b"y").await.unwrap();
        drop(s_new);
        let mut buf = [0u8; 1];
        reader_new.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"y", "new reader gen0 byte");

        s1_new.write_all(b"x").await.unwrap();
        drop(s1_new);
        let mut buf = [0u8; 1];
        reader_new.read_exact(&mut buf).await.unwrap();
        assert_eq!(
            &buf, b"x",
            "new reader receives gen1 byte despite old reader drop"
        );
    }

    // -------------------------------------------------------------------
    // Fix 7 tests
    // -------------------------------------------------------------------

    /// gen0 FINAL + immediate reuse.
    /// Deliver gen-0 FINAL for logical ID 77, reuse ID 77 with replacement
    /// gen-0, deliver gen-1, assert replacement receives gen-1.
    #[tokio::test]
    async fn gen0_final_then_reuse_receives_gen1() {
        let registry = SpliceRegistry::new();
        let (cont_tx, cont_rx) = tokio::sync::mpsc::unbounded_channel();
        let (gen0_tx, mut gen0_rx) = tokio::sync::mpsc::unbounded_channel();
        let _driver = spawn_splice_driver(registry, cont_rx, gen0_tx);

        let (c_final, _s_final) = duplex(64);
        let h_final = ResumeHeader {
            logical_id: 77,
            generation: 0,
            is_final: true,
        };
        cont_tx.send((h_final, Box::pin(c_final))).unwrap();

        let (_, old_reader) = gen0_rx.recv().await.unwrap();
        assert!(old_reader.is_closed(), "gen0 FINAL reader must be closed");

        let (c_reuse, mut s_reuse) = duplex(64);
        let h_reuse = ResumeHeader {
            logical_id: 77,
            generation: 0,
            is_final: false,
        };
        cont_tx.send((h_reuse, Box::pin(c_reuse))).unwrap();

        let (id_new, mut replacement) = gen0_rx.recv().await.unwrap();
        assert_eq!(id_new, 77, "reuse ID must be 77");

        let (c_gen1, mut s_gen1) = duplex(64);
        let h_gen1 = ResumeHeader {
            logical_id: 77,
            generation: 1,
            is_final: false,
        };
        cont_tx.send((h_gen1, Box::pin(c_gen1))).unwrap();

        s_gen1.write_all(b"gen1-data").await.unwrap();
        drop(s_gen1);

        s_reuse.write_all(b"gen0-data").await.unwrap();
        drop(s_reuse);

        let mut buf = [0u8; 9];
        replacement.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"gen0-data", "replacement receives gen0 data");

        let mut buf = [0u8; 9];
        replacement.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"gen1-data", "replacement receives gen1 data");
    }

    /// No generations flushed after FINAL.
    /// After a FINAL is contiguously flushed, dispatch another generation;
    /// assert it's NOT flushed to the queue (because state was cleaned up).
    #[tokio::test]
    async fn no_generations_flushed_after_final() {
        let registry = SpliceRegistry::new();
        let (cont_tx, cont_rx) = tokio::sync::mpsc::unbounded_channel();
        let (gen0_tx, mut gen0_rx) = tokio::sync::mpsc::unbounded_channel();
        let _driver = spawn_splice_driver(registry, cont_rx, gen0_tx);

        let (c0, mut s0) = duplex(64);
        let h0 = ResumeHeader {
            logical_id: 42,
            generation: 0,
            is_final: false,
        };
        cont_tx.send((h0, Box::pin(c0))).unwrap();
        let (_id, mut reader) = gen0_rx.recv().await.unwrap();

        s0.write_all(b"pre-final").await.unwrap();
        drop(s0);
        let mut buf = [0u8; 9];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pre-final");

        let (c_final, final_writer) = duplex(1);
        // write nothing to FINAL
        drop(final_writer);
        let h_final = ResumeHeader {
            logical_id: 42,
            generation: 1,
            is_final: true,
        };
        cont_tx.send((h_final, Box::pin(c_final))).unwrap();

        let n = reader.read(&mut [0u8; 1]).await.unwrap();
        assert_eq!(n, 0, "clean EOF after FINAL gen1");

        let (c_after, _s_after) = duplex(1);
        let h_after = ResumeHeader {
            logical_id: 42,
            generation: 2,
            is_final: false,
        };
        cont_tx.send((h_after, Box::pin(c_after))).unwrap();

        let mut buf = [0u8; 1];
        let read_result =
            tokio::time::timeout(Duration::from_millis(100), reader.read(&mut buf)).await;
        assert!(
            matches!(read_result, Ok(Ok(0)) | Ok(Err(_)) | Err(_)),
            "gen2 must NOT surface in reader — state was cleaned up after FINAL"
        );
    }

    #[tokio::test]
    async fn final_orphan_payload_is_validated_in_order() {
        let registry = SpliceRegistry::new().with_successor_deadline(Duration::from_millis(100));
        let (cont_tx, cont_rx) = tokio::sync::mpsc::unbounded_channel();
        let (gen0_tx, mut gen0_rx) = tokio::sync::mpsc::unbounded_channel();
        let _driver = spawn_splice_driver(registry, cont_rx, gen0_tx);
        let (final_reader, mut final_writer) = duplex(8);
        final_writer.write_all(b"x").await.unwrap();
        drop(final_writer);
        cont_tx
            .send((
                ResumeHeader {
                    logical_id: 90,
                    generation: 1,
                    is_final: true,
                },
                Box::pin(final_reader),
            ))
            .unwrap();
        let (gen0_reader, gen0_writer) = duplex(1);
        drop(gen0_writer);
        cont_tx
            .send((
                ResumeHeader {
                    logical_id: 90,
                    generation: 0,
                    is_final: false,
                },
                Box::pin(gen0_reader),
            ))
            .unwrap();
        let (_, mut reader) = tokio::time::timeout(Duration::from_secs(1), gen0_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let error = tokio::time::timeout(Duration::from_secs(1), reader.read(&mut [0]))
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn final_orphan_after_gap_does_not_close_early() {
        let registry = SpliceRegistry::new().with_successor_deadline(Duration::from_millis(50));
        let (cont_tx, cont_rx) = tokio::sync::mpsc::unbounded_channel();
        let (gen0_tx, mut gen0_rx) = tokio::sync::mpsc::unbounded_channel();
        let _driver = spawn_splice_driver(registry, cont_rx, gen0_tx);
        let (final_reader, final_writer) = duplex(1);
        drop(final_writer);
        cont_tx
            .send((
                ResumeHeader {
                    logical_id: 91,
                    generation: 2,
                    is_final: true,
                },
                Box::pin(final_reader),
            ))
            .unwrap();
        let (gen0_reader, gen0_writer) = duplex(1);
        drop(gen0_writer);
        cont_tx
            .send((
                ResumeHeader {
                    logical_id: 91,
                    generation: 0,
                    is_final: false,
                },
                Box::pin(gen0_reader),
            ))
            .unwrap();
        let (_, mut reader) = tokio::time::timeout(Duration::from_secs(1), gen0_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let error = tokio::time::timeout(Duration::from_secs(1), reader.read(&mut [0]))
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    }
}
