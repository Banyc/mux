use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
    time::Duration,
};

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::task::JoinSet;

use crate::{
    StreamReader,
    dual_lane::{DualStreamAccepter, DualStreamOpener},
    migration_wire::{
        GenerationChain, GenerationReader, MigrationError, ResumeHeader, SplicedReader,
    },
    splice_feed::{SpliceFeedError, SpliceRouterHandle, spawn_splice_router},
    stream::writer::StreamWriter,
    traffic_class::LaneClass,
};

// ---------------------------------------------------------------------------
// Constants (mirror central_io::scheduler's LatencyControl)
// ---------------------------------------------------------------------------

/// Cross-reference: `DATA_MEDIUM_CAP` in `central_io::scheduler`.
pub const AUTO_BULK_THRESHOLD: usize = crate::traffic_class::BULK_THRESHOLD;

#[cfg(not(test))]
const RESUME_HEADER_DEADLINE: Duration = Duration::from_secs(30);
#[cfg(test)]
const RESUME_HEADER_DEADLINE: Duration = Duration::from_millis(100);

// ---------------------------------------------------------------------------
// Mirrored classifier
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct StreamName(Arc<OnceLock<Box<str>>>);
impl StreamName {
    pub fn set(&self, name: &str) {
        let _ = self.0.set(name.into());
    }
    fn get(&self) -> &str {
        self.0.get().map(|s| &**s).unwrap_or("")
    }
}

// ---------------------------------------------------------------------------
// Migration error
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum MigratingStreamError {
    Migration(MigrationError),
    OpenFailed,
    OpenUnderlying(String),
    WriteFailed,
    LaneDead,
}

impl From<MigrationError> for MigratingStreamError {
    fn from(e: MigrationError) -> Self {
        MigratingStreamError::Migration(e)
    }
}

// ---------------------------------------------------------------------------
// MigratingStreamWriter
// ---------------------------------------------------------------------------

enum WriterState {
    /// Writing on the current lane.
    Active {
        writer: StreamWriter,
        lane: LaneClass,
    },
    /// No open writer — need to open first.
    PendingOpen {
        lane: LaneClass,
    },
    /// Migration in progress — waiting for old writer to drain then
    /// opening new generation on target lane.
    Migrating {
        target_lane: LaneClass,
    },
    Closed,
}

/// A migrating stream writer. Dropping an opened writer is an abort: the
/// peer eventually gets [`BrokenPipe`](io::ErrorKind::BrokenPipe). Callers
/// that require clean EOF must call [`Self::finalize`] before dropping.
pub struct MigratingStreamWriter {
    opener: DualStreamOpener,
    chain: GenerationChain,
    state: WriterState,
    policy: crate::traffic_class::LanePolicy,
    name: StreamName,
    auto_migrate: bool,
    gen0_reader_tx: Option<tokio::sync::oneshot::Sender<StreamReader>>,
    /// When set, the most recent successor generation reader is held
    /// alive here so its sub-stream doesn't close on the peer; gen 0 is
    /// delivered via [`gen0_reader_tx`]. `None` while no successor is
    /// open.
    latest_held_reader: Option<StreamReader>,
}

impl std::fmt::Debug for MigratingStreamWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MigratingStreamWriter")
            .field("auto_migrate", &self.auto_migrate)
            .finish_non_exhaustive()
    }
}

impl MigratingStreamWriter {
    pub(crate) fn new(
        opener: DualStreamOpener,
        logical_id: u64,
        initial_lane: LaneClass,
        auto_migrate: bool,
    ) -> Self {
        Self {
            opener,
            chain: GenerationChain::new(logical_id),
            state: WriterState::PendingOpen { lane: initial_lane },
            policy: crate::traffic_class::LanePolicy::new(),
            name: StreamName::default(),
            auto_migrate,
            gen0_reader_tx: None,
            latest_held_reader: None,
        }
    }

    fn new_with_reader_tx(
        opener: DualStreamOpener,
        logical_id: u64,
        initial_lane: LaneClass,
        auto_migrate: bool,
        gen0_reader_tx: tokio::sync::oneshot::Sender<StreamReader>,
    ) -> Self {
        Self {
            opener,
            chain: GenerationChain::new(logical_id),
            state: WriterState::PendingOpen { lane: initial_lane },
            policy: crate::traffic_class::LanePolicy::new(),
            name: StreamName::default(),
            auto_migrate,
            gen0_reader_tx: Some(gen0_reader_tx),
            latest_held_reader: None,
        }
    }

    pub async fn force_migrate(&mut self, target: LaneClass) -> Result<(), MigratingStreamError> {
        let decision =
            self.policy
                .decision(target, crate::traffic_class::LaneMigrationReason::Forced, 0);
        self.migrate_to(decision).await
    }

    pub fn name(&self) -> StreamName {
        self.name.clone()
    }

    pub(crate) fn new_response_seeded(
        opener: DualStreamOpener,
        logical_id: u64,
        gen0_writer: StreamWriter,
        gen0_lane: LaneClass,
    ) -> Self {
        Self {
            opener,
            chain: GenerationChain::new_response(logical_id),
            state: WriterState::Active {
                writer: gen0_writer,
                lane: gen0_lane,
            },
            policy: crate::traffic_class::LanePolicy::new(),
            name: StreamName::default(),
            auto_migrate: true,
            gen0_reader_tx: None,
            latest_held_reader: None,
        }
    }

    async fn ensure_open(&mut self) -> Result<(), MigratingStreamError> {
        let lane = match &self.state {
            WriterState::Active { .. } => return Ok(()),
            WriterState::PendingOpen { lane } => *lane,
            WriterState::Migrating { target_lane } => *target_lane,
            WriterState::Closed => return Err(MigratingStreamError::LaneDead),
        };
        let (reader, mut writer) = match self.opener.open(lane).await {
            Ok(x) => x,
            Err(e) => return Err(MigratingStreamError::OpenUnderlying(format!("{e:?}"))),
        };
        let genn = self
            .chain
            .start_generation(&mut as_async_write(&mut writer), false)
            .await?;
        self.route_opened_reader(genn, reader);
        self.state = WriterState::Active { writer, lane };
        Ok(())
    }

    async fn migrate_to(
        &mut self,
        decision: crate::traffic_class::LaneMigration,
    ) -> Result<(), MigratingStreamError> {
        let target = decision.target;
        let from = match &self.state {
            WriterState::Active { lane, .. } | WriterState::PendingOpen { lane } => Some(*lane),
            WriterState::Migrating { target_lane } => Some(*target_lane),
            WriterState::Closed => None,
        };
        tracing::info!(
            name = self.name.get(),
            ?from, to = ?target,
            reason = decision.reason.as_str(),
            write_size = decision.write_size,
            small_writes = decision.small_writes,
            bulk_writes = decision.bulk_writes,
            small_streak = decision.small_streak,
            "stream lane migration"
        );
        if let WriterState::Active { writer, .. } = &mut self.state {
            let _ = writer.shutdown();
        }
        self.state = WriterState::Migrating {
            target_lane: target,
        };
        self.policy.note_migration(tokio::time::Instant::now());
        Ok(())
    }

    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), MigratingStreamError> {
        if self.auto_migrate {
            self.classify_and_maybe_migrate(buf.len()).await?;
        }
        self.ensure_open().await?;

        // Safe to access because ensure_open left us in Active state
        let writer = match &mut self.state {
            WriterState::Active { writer, .. } => writer,
            _ => return Err(MigratingStreamError::LaneDead),
        };

        use tokio::io::AsyncWriteExt;
        writer
            .write_all(buf)
            .await
            .map_err(|_| MigratingStreamError::WriteFailed)?;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), MigratingStreamError> {
        use tokio::io::AsyncWriteExt;
        match &mut self.state {
            WriterState::Active { writer, .. } => writer
                .flush()
                .await
                .map_err(|_| MigratingStreamError::WriteFailed),
            WriterState::Migrating { .. } => {
                self.ensure_open().await?;
                Ok(())
            }
            WriterState::PendingOpen { .. } => Ok(()),
            WriterState::Closed => Err(MigratingStreamError::LaneDead),
        }
    }

    fn route_opened_reader(&mut self, generation: u32, reader: StreamReader) {
        let mut reader = reader;
        if generation == 0
            && let Some(tx) = self.gen0_reader_tx.take()
        {
            match tx.send(reader) {
                Ok(()) => return,
                Err(r) => reader = r,
            }
        }
        self.latest_held_reader = Some(reader);
    }

    async fn classify_and_maybe_migrate(
        &mut self,
        size: usize,
    ) -> Result<(), MigratingStreamError> {
        let current = match &self.state {
            WriterState::Active { lane, .. } | WriterState::PendingOpen { lane } => Some(*lane),
            _ => None,
        };
        let decision = self
            .policy
            .on_write(size, current, tokio::time::Instant::now());
        if let Some(decision) = decision {
            return self.migrate_to(decision).await;
        }
        Ok(())
    }

    /// Confirmed close: opens a new substream for a FINAL-marker generation,
    /// writes the FINAL resume header, and closes. The peer receives a
    /// clean EOF (the [`SplicedReader`] sees `has_final_marker`). If the
    /// stream was never announced to the peer (still unopened), this is
    /// a no-op — no FINAL is emitted for a stream the peer never saw.
    /// Dropping an opened writer instead of calling this is an abort and
    /// the peer eventually gets [`BrokenPipe`](io::ErrorKind::BrokenPipe);
    /// callers that require clean EOF must call this method before drop.
    pub async fn finalize(&mut self) -> Result<(), MigratingStreamError> {
        if self.nothing_to_close() {
            self.state = WriterState::Closed;
            return Ok(());
        }
        if let WriterState::Active { writer, .. } = &mut self.state {
            let _ = writer.shutdown();
        }
        self.state = WriterState::Closed;
        let (_, mut final_writer) = self
            .opener
            .open(LaneClass::Interactive)
            .await
            .map_err(|_| MigratingStreamError::OpenFailed)?;
        self.chain
            .start_generation(&mut as_async_write(&mut final_writer), true)
            .await?;
        let _ = final_writer.shutdown();
        Ok(())
    }

    /// A stream with nothing left to close is either already closed or
    /// was never announced to the peer (generation 0 never opened).
    fn nothing_to_close(&self) -> bool {
        matches!(self.state, WriterState::Closed) || self.chain.never_opened()
    }

    pub async fn rebind(&mut self, opener: DualStreamOpener) -> Result<(), MigratingStreamError> {
        self.opener = opener;
        match &self.state {
            WriterState::Active { lane, .. } => {
                let decision = self.policy.decision(
                    *lane,
                    crate::traffic_class::LaneMigrationReason::Rebind,
                    0,
                );
                self.migrate_to(decision).await
            }
            WriterState::PendingOpen { .. }
            | WriterState::Migrating { .. }
            | WriterState::Closed => Ok(()),
        }
    }
}

// ---------------------------------------------------------------------------
// Adapter to use StreamWriter with GenerationChain::start_generation
// ---------------------------------------------------------------------------

struct StreamWriterRef<'a> {
    inner: &'a mut StreamWriter,
}

impl<'a> AsyncWrite for StreamWriterRef<'a> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_shutdown(cx)
    }
}

fn as_async_write(w: &mut StreamWriter) -> StreamWriterRef<'_> {
    StreamWriterRef { inner: w }
}

// ---------------------------------------------------------------------------
// DualStreamOpener extensions
// ---------------------------------------------------------------------------

impl DualStreamOpener {
    /// Open a stream on `initial_lane` that classifies every write and
    /// migrates between lanes when the traffic pattern changes.
    ///
    /// Auto-policy: a single write larger than [`AUTO_BULK_THRESHOLD`]
    /// migrates to the bulk lane **before** that write is sent. Demotion
    /// back to interactive requires [`DEMOTE_STREAK`] (4) consecutive
    /// small writes and a cool-down of [`MIGRATION_COOLDOWN`] (150 ms) since
    /// the last migration.
    pub fn open_migrating(
        &self,
        logical_id: u64,
        initial_lane: LaneClass,
    ) -> MigratingStreamWriter {
        MigratingStreamWriter::new(self.clone(), logical_id, initial_lane, true)
    }

    /// Like [`open_migrating`] but also returns a
    /// [`tokio::sync::oneshot::Receiver`] that resolves to the gen-0
    /// [`StreamReader`] once the first write opens the underlying stream.
    /// The receiver yields the reader that the server side can use to
    /// send responses back to the opener — needed for bidirectional
    /// protocols (e.g. proxy tunnels) where the opener is also a reader.
    pub fn open_migrating_with_reader(
        &self,
        logical_id: u64,
        initial_lane: LaneClass,
    ) -> (
        MigratingStreamWriter,
        tokio::sync::oneshot::Receiver<StreamReader>,
    ) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let writer = MigratingStreamWriter::new_with_reader_tx(
            self.clone(),
            logical_id,
            initial_lane,
            true,
            tx,
        );
        (writer, rx)
    }

    /// Same as [`open_migrating`] but with auto-classification disabled.
    /// The caller drives lane changes via [`MigratingStreamWriter::force_migrate`].
    pub fn open_migrating_manual(
        &self,
        logical_id: u64,
        initial_lane: LaneClass,
    ) -> MigratingStreamWriter {
        MigratingStreamWriter::new(self.clone(), logical_id, initial_lane, false)
    }

    /// Open a bidirectional migrating stream on `initial_lane`. The
    /// returned [`MigratingStreamWriter`] handles the write side; the
    /// returned [`PendingResponseReader`] handles the read side.
    ///
    /// RESPONSE-direction traffic stays pinned to the lane where
    /// generation 0 opened; only the REQUEST direction migrates. True
    /// bidirectional lane migration needs the accepter API to expose
    /// successor writers — future work, out of scope.
    pub fn open_migrating_duplex(
        &self,
        logical_id: u64,
        initial_lane: LaneClass,
    ) -> (PendingResponseReader, MigratingStreamWriter) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let writer = MigratingStreamWriter::new_with_reader_tx(
            self.clone(),
            logical_id,
            initial_lane,
            true,
            tx,
        );
        let reader = PendingResponseReader {
            logical_id,
            inner: None,
            gen0_rx: Some(rx),
        };
        (reader, writer)
    }
}

// ---------------------------------------------------------------------------
// MigratingCapableAccepter
// ---------------------------------------------------------------------------

/// Wraps a [`DualStreamAccepter`] and transparently handles migrating
/// streams while passing non-migrating streams through untouched.
///
/// When the accept loop receives a gen-0 [`SplicedReader`] whose
/// `logical_id` does not match the incoming stream, the reader is
/// stashed and re-tried on the next accept — this handles interleaved
/// logical streams arriving out of order.
pub struct MigratingCapableAccepter {
    inner: DualStreamAccepter,
    pass_plain_streams: bool,
    response_opener: Option<DualStreamOpener>,
    feed: SpliceRouterHandle,
    own_feed_driver: Option<JoinSet<()>>,
    peeks: JoinSet<PeekedStream>,
}

const MAX_CONCURRENT_PEEKS: usize = 256;

enum PeekOutcome {
    Gen0 {
        spliced: SplicedReader,
        logical_id: u64,
    },
    Plain {
        reader: StreamReader,
    },
    Consumed,
    FeedDead,
}

struct PeekedStream {
    outcome: Result<PeekOutcome, MigratingStreamError>,
    writer: StreamWriter,
    lane: LaneClass,
}

enum AcceptStep {
    Accepted(Result<(StreamReader, StreamWriter, LaneClass), crate::dual_lane::DualAcceptError>),
    Peeked(Option<Result<PeekedStream, tokio::task::JoinError>>),
    FeedDone(Option<Result<(), tokio::task::JoinError>>),
}

/// Reap the accepter's own splice-feed supervision [`JoinSet`]. Each completed
/// supervisor task is unwrapped so a panicked supervisor surfaces on reap;
/// once the set is empty the driver is dropped so the accepter stops polling
/// it.
async fn drain_feed_driver(
    driver: &mut Option<JoinSet<()>>,
) -> Option<Result<(), tokio::task::JoinError>> {
    match driver {
        Some(set) => match set.join_next().await {
            Some(result) => Some(result),
            None => {
                *driver = None;
                None
            }
        },
        None => None,
    }
}

impl MigratingCapableAccepter {
    pub fn new(inner: DualStreamAccepter) -> Self {
        Self::new_with_plain_streams(inner, true)
    }

    fn new_with_plain_streams(inner: DualStreamAccepter, pass_plain_streams: bool) -> Self {
        let (feed, own_feed_driver) = spawn_splice_router();
        Self {
            inner,
            pass_plain_streams,
            response_opener: None,
            feed,
            own_feed_driver: Some(own_feed_driver),
            peeks: JoinSet::new(),
        }
    }

    fn new_shared(inner: DualStreamAccepter, feed: SpliceRouterHandle) -> Self {
        Self {
            inner,
            pass_plain_streams: false,
            response_opener: None,
            feed,
            own_feed_driver: None,
            peeks: JoinSet::new(),
        }
    }

    fn accepted_migrating(
        &self,
        reader: SplicedReader,
        writer: StreamWriter,
        source_lane: LaneClass,
        logical_id: u64,
    ) -> AcceptedStream {
        match &self.response_opener {
            Some(opener) => AcceptedStream::MigratingDuplex {
                reader,
                writer: MigratingStreamWriter::new_response_seeded(
                    opener.clone(),
                    logical_id,
                    writer,
                    source_lane,
                ),
                source_lane,
            },
            None => AcceptedStream::Migrating {
                reader,
                writer,
                source_lane,
            },
        }
    }

    pub async fn accept(&mut self) -> Result<AcceptedStream, MigratingStreamError> {
        loop {
            let can_accept = self.peeks.len() < MAX_CONCURRENT_PEEKS;
            let has_peeks = !self.peeks.is_empty();
            let has_own_feed = self.own_feed_driver.is_some();
            let step = tokio::select! {
                accepted = self.inner.accept(), if can_accept => AcceptStep::Accepted(accepted),
                joined = self.peeks.join_next(), if has_peeks => AcceptStep::Peeked(joined),
                drained = drain_feed_driver(&mut self.own_feed_driver), if has_own_feed => {
                    AcceptStep::FeedDone(drained)
                }
            };
            let peek = match step {
                AcceptStep::Accepted(accepted) => {
                    let (reader, writer, lane) =
                        accepted.map_err(|_| MigratingStreamError::LaneDead)?;
                    let feed = self.feed.clone();
                    self.peeks.spawn(async move {
                        PeekedStream {
                            outcome: Self::peek_and_dispatch(reader, feed).await,
                            writer,
                            lane,
                        }
                    });
                    continue;
                }
                AcceptStep::Peeked(None) => unreachable!("peek JoinSet was nonempty"),
                AcceptStep::Peeked(Some(result)) => result.unwrap(),
                AcceptStep::FeedDone(Some(result)) => {
                    result.unwrap();
                    continue;
                }
                AcceptStep::FeedDone(None) => continue,
            };
            let PeekedStream {
                outcome,
                writer,
                lane,
            } = peek;
            match outcome? {
                PeekOutcome::Gen0 {
                    spliced,
                    logical_id,
                } => {
                    return Ok(self.accepted_migrating(spliced, writer, lane, logical_id));
                }
                PeekOutcome::Plain { reader } => {
                    if !self.pass_plain_streams {
                        continue;
                    }
                    return Ok(AcceptedStream::Plain {
                        reader,
                        writer,
                        source_lane: lane,
                    });
                }
                PeekOutcome::Consumed => continue,
                PeekOutcome::FeedDead => return Err(MigratingStreamError::LaneDead),
            }
        }
    }

    async fn peek_and_dispatch(
        reader: StreamReader,
        feed: SpliceRouterHandle,
    ) -> Result<PeekOutcome, MigratingStreamError> {
        let Some((is_migrating, header_opt, reader)) = Self::peek_resume_header(reader).await?
        else {
            return Ok(PeekOutcome::Consumed);
        };
        let (Some(header), true) = (header_opt, is_migrating) else {
            return Ok(PeekOutcome::Plain { reader });
        };
        if header.is_response {
            return Ok(PeekOutcome::Consumed);
        }
        let logical_id = header.logical_id;
        let is_gen0 = header.generation == 0;
        let gen_reader: GenerationReader = Box::pin(reader);
        if !is_gen0 {
            return match feed.send_continuation(header, gen_reader).await {
                Ok(()) => Ok(PeekOutcome::Consumed),
                Err(SpliceFeedError::Closed) => Ok(PeekOutcome::FeedDead),
            };
        }
        let spliced_rx = match feed.await_gene(logical_id).await {
            Ok(rx) => rx,
            Err(SpliceFeedError::Closed) => return Ok(PeekOutcome::FeedDead),
        };
        if feed.send_continuation(header, gen_reader).await.is_err() {
            return Ok(PeekOutcome::FeedDead);
        }
        match spliced_rx.await {
            Ok(spliced) => Ok(PeekOutcome::Gen0 {
                spliced,
                logical_id,
            }),
            Err(_) => Ok(PeekOutcome::Consumed),
        }
    }

    pub(crate) async fn peek_resume_header(
        mut reader: StreamReader,
    ) -> Result<Option<(bool, Option<ResumeHeader>, StreamReader)>, MigratingStreamError> {
        use crate::migration_wire::{RESUME_HEADER_LEN, ResumeHeader};
        use tokio::io::AsyncReadExt;
        let mut buf = [0u8; RESUME_HEADER_LEN];
        let mut filled = 0;
        let deadline = tokio::time::Instant::now() + RESUME_HEADER_DEADLINE;
        while filled < buf.len() {
            let read = tokio::time::timeout_at(deadline, reader.read(&mut buf[filled..])).await;
            match read {
                Err(_) => return Ok(None),
                Ok(result) => match result {
                    Ok(0) | Err(_) => {
                        if filled > 0 {
                            reader.prepend(&buf[..filled]);
                        }
                        return Ok(Some((false, None, reader)));
                    }
                    Ok(n) => filled += n,
                },
            }
        }
        if let Some(header) = ResumeHeader::parse(&buf) {
            Ok(Some((true, Some(header), reader)))
        } else {
            reader.prepend(&buf);
            Ok(Some((false, None, reader)))
        }
    }
}

// ---------------------------------------------------------------------------
// AcceptedStream
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum AcceptedStream {
    /// A migrating stream whose reader yields a continuous byte stream
    /// across generation boundaries.
    Migrating {
        reader: SplicedReader,
        writer: StreamWriter,
        source_lane: LaneClass,
    },
    MigratingDuplex {
        reader: SplicedReader,
        writer: MigratingStreamWriter,
        source_lane: LaneClass,
    },
    /// A plain (non-migrating) stream.
    Plain {
        reader: StreamReader,
        writer: StreamWriter,
        source_lane: LaneClass,
    },
}

// ---------------------------------------------------------------------------
// PendingResponseReader — opener-side reader for duplex migrating
// ---------------------------------------------------------------------------

/// Client-side reader for a duplex migrating stream. The gen-0
/// [`StreamReader`] is delivered via a oneshot from the paired
/// [`MigratingStreamWriter`] when the first write opens the underlying
/// stream. Once installed, every [`AsyncRead::poll_read`] delegates to
/// that single reader — response traffic stays pinned to the lane where
/// generation 0 opened; only the request direction migrates.
///
/// If the writer is dropped before any write, the oneshot sender drops
/// → the reader gets [`BrokenPipe`](io::ErrorKind::BrokenPipe) (the
/// stream never materialised).
pub struct PendingResponseReader {
    logical_id: u64,
    inner: Option<StreamReader>,
    gen0_rx: Option<tokio::sync::oneshot::Receiver<StreamReader>>,
}

impl std::fmt::Debug for PendingResponseReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingResponseReader")
            .field("logical_id", &self.logical_id)
            .field("has_inner", &self.inner.is_some())
            .finish()
    }
}

impl AsyncRead for PendingResponseReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.inner.is_none() {
            let mut rx_opt = self.gen0_rx.take();
            match rx_opt.as_mut() {
                Some(rx) => match Pin::new(rx).poll(cx) {
                    Poll::Ready(Ok(r)) => self.inner = Some(r),
                    Poll::Ready(Err(_)) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "duplex writer dropped before gen0 arrived",
                        )));
                    }
                    Poll::Pending => {
                        self.gen0_rx = rx_opt;
                        return Poll::Pending;
                    }
                },
                None => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "duplex writer dropped before gen0 arrived",
                    )));
                }
            }
        }
        match &mut self.inner {
            Some(r) => Pin::new(r).poll_read(cx, buf),
            None => unreachable!(),
        }
    }
}

// ---------------------------------------------------------------------------
// DualStreamAccepter extension
// ---------------------------------------------------------------------------

impl DualStreamAccepter {
    /// Wrap this accepter in a migrating-capable variant that
    /// transparently handles migration streams.
    pub fn into_migrating_capable(self) -> MigratingCapableAccepter {
        MigratingCapableAccepter::new(self)
    }

    pub fn into_migrating_only(self) -> MigratingCapableAccepter {
        MigratingCapableAccepter::new_with_plain_streams(self, false)
    }

    pub fn into_migrating_duplex(self, opener: DualStreamOpener) -> MigratingCapableAccepter {
        let mut mac = MigratingCapableAccepter::new_with_plain_streams(self, false);
        mac.response_opener = Some(opener);
        mac
    }

    pub fn into_migrating_only_with_feed(
        self,
        feed: SpliceRouterHandle,
    ) -> MigratingCapableAccepter {
        MigratingCapableAccepter::new_shared(self, feed)
    }

    pub fn into_migrating_duplex_with_feed(
        self,
        opener: DualStreamOpener,
        feed: SpliceRouterHandle,
    ) -> MigratingCapableAccepter {
        let mut mac = MigratingCapableAccepter::new_shared(self, feed);
        mac.response_opener = Some(opener);
        mac
    }
}

/// A response router is split into a cheap, cloneable [`ResponseRouterHandle`]
/// (handed out to sessions/streams) and a driver that owns the supervised
/// accepter-task [`JoinSet`]. The driver is reaped by a long-lived owner —
/// `rtp_mux::run_connector` — so completed-task and peek-task [`JoinError`]s
/// are observed instead of being silently discarded (a panic in an accepter
/// loop surfaces on reap rather than vanishing into a `try_join_next` drain).
///
/// This mirrors the [`SpliceRouterHandle`] split: the handle only carries mpsc
/// senders, the driver owns the supervision. The splice-feed supervision
/// [`JoinSet`] created by [`spawn_splice_router`] is folded into the driver on
/// the first [`ResponseRouter::add_accepter`] call, so it is actively reaped
/// alongside the accepter tasks instead of sitting un-polled in the router.
#[derive(Debug)]
pub struct ResponseRouter {
    feed: SpliceRouterHandle,
    splice_driver: Option<JoinSet<()>>,
}
impl Default for ResponseRouter {
    fn default() -> Self {
        let (feed, splice_driver) = spawn_splice_router();
        Self {
            feed,
            splice_driver: Some(splice_driver),
        }
    }
}
impl ResponseRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn handle(&self) -> ResponseRouterHandle {
        ResponseRouterHandle {
            feed: self.feed.clone(),
        }
    }

    pub fn add_accepter(
        &mut self,
        mut accepter: DualStreamAccepter,
        driver: &mut ResponseRouterDriver,
    ) {
        if let Some(supervision) = self.splice_driver.take() {
            driver.fold_supervision(supervision);
        }
        let feed = self.feed.clone();
        driver.spawn(async move {
            let mut inner: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
            inner.spawn(async move {
                let mut peeks: JoinSet<Option<(ResumeHeader, StreamReader)>> = JoinSet::new();
                let mut accepting = true;
                loop {
                    let can_accept = accepting && peeks.len() < MAX_CONCURRENT_PEEKS;
                    let has_peeks = !peeks.is_empty();
                    if !can_accept && !has_peeks {
                        break;
                    }
                    tokio::select! {
                        accepted = accepter.accept(), if can_accept => match accepted {
                            Ok((reader, _writer, _lane)) => {
                                peeks.spawn(async move {
                                    match MigratingCapableAccepter::peek_resume_header(reader).await {
                                        Ok(Some((true, Some(header), reader))) if header.is_response => {
                                            Some((header, reader))
                                        }
                                        _ => None,
                                    }
                                });
                            }
                            Err(_) => accepting = false,
                        },
                        joined = peeks.join_next(), if has_peeks => {
                            match joined {
                                Some(Ok(Some((header, reader)))) => {
                                    if feed
                                        .send_continuation(header, Box::pin(reader) as GenerationReader)
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                                Some(Ok(None)) => {}
                                Some(result) => {
                                    result.unwrap();
                                }
                                None => {}
                            }
                        }
                    }
                }
            });
            match inner.join_next().await {
                Some(Ok(())) => tracing::debug!("ResponseRouter accepter task stopped"),
                Some(result) => result.unwrap(),
                None => unreachable!("one accepter task was inserted"),
            }
        });
    }
}

#[derive(Debug, Default)]
pub struct ResponseRouterDriver {
    tasks: tokio::task::JoinSet<()>,
}

impl ResponseRouterDriver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn spawn(&mut self, task: impl std::future::Future<Output = ()> + Send + 'static) {
        self.tasks.spawn(task);
    }

    /// Fold a supervisor [`JoinSet`] (e.g. the splice-feed supervision) into
    /// this driver so its tasks are reaped alongside the accepter tasks. Each
    /// completed task is unwrapped, so a panicked supervisor propagates
    /// through this driver's reap.
    pub fn fold_supervision(&mut self, mut supervision: JoinSet<()>) {
        self.tasks.spawn(async move {
            while let Some(result) = supervision.join_next().await {
                result.unwrap();
            }
        });
    }

    pub async fn join_next(&mut self) -> Option<Result<(), tokio::task::JoinError>> {
        self.tasks.join_next().await
    }

    pub fn try_join_next(&mut self) -> Option<Result<(), tokio::task::JoinError>> {
        self.tasks.try_join_next()
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub fn abort_all(&mut self) {
        self.tasks.abort_all();
    }
}

#[derive(Debug, Clone)]
pub struct ResponseRouterHandle {
    feed: SpliceRouterHandle,
}
impl ResponseRouterHandle {
    pub async fn inject_response_gene(
        &self,
        logical_id: u64,
        gen0_reader: StreamReader,
    ) -> Result<tokio::sync::oneshot::Receiver<SplicedReader>, SpliceFeedError> {
        let rx = self.feed.await_gene(logical_id).await?;
        let header = ResumeHeader {
            logical_id,
            generation: 0,
            is_final: false,
            is_response: true,
        };
        self.feed
            .send_continuation(header, Box::pin(gen0_reader) as GenerationReader)
            .await?;
        Ok(rx)
    }
}

pub fn spawn_response_router(
    accepter: DualStreamAccepter,
) -> (ResponseRouter, ResponseRouterDriver) {
    let mut router = ResponseRouter::new();
    let mut driver = ResponseRouterDriver::new();
    router.add_accepter(accepter, &mut driver);
    (router, driver)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DualStreamAccepter, DualStreamOpener,
        control::Initiation,
        dual_lane::Liveness,
        session::{MuxConfig, spawn_mux_no_reconnection},
        splice_feed::MAX_UNCLAIMED_GEN0,
    };
    use std::time::Duration;
    use tokio::io::{AsyncWriteExt, duplex};

    async fn make_dual_session() -> (
        DualStreamOpener,
        DualStreamAccepter,
        tokio::task::JoinSet<crate::session::MuxError>,
        tokio::task::JoinSet<crate::session::MuxError>,
        tokio::task::JoinSet<crate::session::MuxError>,
        tokio::task::JoinSet<crate::session::MuxError>,
    ) {
        // Large duplex buffers so writes don't block when the receiver
        // hasn't started accepting yet.
        let buf_size = 4 * 1024 * 1024; // 4 MiB
        let (int_c2s, int_s2c) = duplex(buf_size);
        let (bulk_c2s, bulk_s2c) = duplex(buf_size);

        let (int_srv_r, int_srv_w) = tokio::io::split(int_c2s);
        let (int_cli_r, int_cli_w) = tokio::io::split(int_s2c);
        let (bulk_srv_r, bulk_srv_w) = tokio::io::split(bulk_c2s);
        let (bulk_cli_r, bulk_cli_w) = tokio::io::split(bulk_s2c);

        let srv_cfg = MuxConfig {
            initiation: Initiation::Server,
            heartbeat_interval: Duration::from_secs(1),
            frame_reassembly: false,
        };
        let cli_cfg = MuxConfig {
            initiation: Initiation::Client,
            heartbeat_interval: Duration::from_secs(1),
            frame_reassembly: false,
        };

        let mut srv_int = tokio::task::JoinSet::new();
        let (int_srv_op, _) =
            spawn_mux_no_reconnection(int_srv_r, int_srv_w, srv_cfg.clone(), &mut srv_int);
        let mut srv_bulk = tokio::task::JoinSet::new();
        let (bulk_srv_op, _) =
            spawn_mux_no_reconnection(bulk_srv_r, bulk_srv_w, srv_cfg, &mut srv_bulk);

        let mut cli_int = tokio::task::JoinSet::new();
        let (_, int_cli_acc) =
            spawn_mux_no_reconnection(int_cli_r, int_cli_w, cli_cfg.clone(), &mut cli_int);
        let mut cli_bulk = tokio::task::JoinSet::new();
        let (_, bulk_cli_acc) =
            spawn_mux_no_reconnection(bulk_cli_r, bulk_cli_w, cli_cfg, &mut cli_bulk);

        let opener = DualStreamOpener::new(int_srv_op, bulk_srv_op, Liveness::new());
        let accepter = DualStreamAccepter::new(int_cli_acc, bulk_cli_acc, Liveness::new());

        (opener, accepter, srv_int, srv_bulk, cli_int, cli_bulk)
    }

    async fn make_duplex_session() -> (
        DualStreamOpener,
        DualStreamAccepter,
        DualStreamOpener,
        DualStreamAccepter,
        Vec<tokio::task::JoinSet<crate::session::MuxError>>,
    ) {
        let buf_size = 4 * 1024 * 1024;
        let (int_c2s, int_s2c) = duplex(buf_size);
        let (bulk_c2s, bulk_s2c) = duplex(buf_size);
        let (int_x_r, int_x_w) = tokio::io::split(int_c2s);
        let (int_y_r, int_y_w) = tokio::io::split(int_s2c);
        let (bulk_x_r, bulk_x_w) = tokio::io::split(bulk_c2s);
        let (bulk_y_r, bulk_y_w) = tokio::io::split(bulk_s2c);
        let x_cfg = MuxConfig {
            initiation: Initiation::Server,
            heartbeat_interval: Duration::from_secs(1),
            frame_reassembly: false,
        };
        let y_cfg = MuxConfig {
            initiation: Initiation::Client,
            heartbeat_interval: Duration::from_secs(1),
            frame_reassembly: false,
        };
        let mut tasks = Vec::new();
        let mut js = tokio::task::JoinSet::new();
        let (int_x_op, int_x_acc) =
            spawn_mux_no_reconnection(int_x_r, int_x_w, x_cfg.clone(), &mut js);
        tasks.push(js);
        let mut js = tokio::task::JoinSet::new();
        let (bulk_x_op, bulk_x_acc) = spawn_mux_no_reconnection(bulk_x_r, bulk_x_w, x_cfg, &mut js);
        tasks.push(js);
        let mut js = tokio::task::JoinSet::new();
        let (int_y_op, int_y_acc) =
            spawn_mux_no_reconnection(int_y_r, int_y_w, y_cfg.clone(), &mut js);
        tasks.push(js);
        let mut js = tokio::task::JoinSet::new();
        let (bulk_y_op, bulk_y_acc) = spawn_mux_no_reconnection(bulk_y_r, bulk_y_w, y_cfg, &mut js);
        tasks.push(js);
        let x_opener = DualStreamOpener::new(int_x_op, bulk_x_op, Liveness::new());
        let x_accepter = DualStreamAccepter::new(int_x_acc, bulk_x_acc, Liveness::new());
        let y_opener = DualStreamOpener::new(int_y_op, bulk_y_op, Liveness::new());
        let y_accepter = DualStreamAccepter::new(int_y_acc, bulk_y_acc, Liveness::new());
        (x_opener, x_accepter, y_opener, y_accepter, tasks)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn duplex_response_migrates_independently_with_clean_eof() {
        let (x_op, x_acc, y_op, y_acc, _tasks) = make_duplex_session().await;
        let (router, _driver) = spawn_response_router(x_acc);
        let mut mac = y_acc.into_migrating_duplex(y_op);
        let (mut req_writer, gen0_rx) = x_op.open_migrating_with_reader(42, LaneClass::Interactive);
        req_writer.write_all(b"request").await.unwrap();
        let accepted = mac.accept().await.unwrap();
        let (mut req_reader, mut resp_writer) = migrating_duplex(accepted);
        let mut req = [0u8; 7];
        tokio::io::AsyncReadExt::read_exact(&mut req_reader, &mut req)
            .await
            .unwrap();
        assert_eq!(&req, b"request");
        let gen0_reader = gen0_rx.await.unwrap();
        let spliced_rx = router
            .handle()
            .inject_response_gene(42, gen0_reader)
            .await
            .expect("splice feed alive");
        let mut resp_reader = spliced_rx.await.unwrap();
        resp_writer.write_all(b"head-").await.unwrap();
        resp_writer.write_all(&vec![0xEE; 40_000]).await.unwrap();
        resp_writer.write_all(b"-tail").await.unwrap();
        resp_writer.finalize().await.unwrap();
        let mut resp = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut resp_reader, &mut resp)
            .await
            .unwrap();
        assert_eq!(resp.len(), 5 + 40_000 + 5, "response length mismatch");
        assert_eq!(&resp[..5], b"head-");
        assert!(resp[5..5 + 40_000].iter().all(|b| *b == 0xEE));
        assert_eq!(&resp[5 + 40_000..], b"-tail");
        req_writer.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn duplex_response_without_migration_needs_final_for_clean_eof() {
        let (x_op, x_acc, y_op, y_acc, _tasks) = make_duplex_session().await;
        let (router, _driver) = spawn_response_router(x_acc);
        let mut mac = y_acc.into_migrating_duplex(y_op);
        let (mut req_writer, gen0_rx) = x_op.open_migrating_with_reader(7, LaneClass::Interactive);
        req_writer.write_all(b"ping").await.unwrap();
        let (_, mut resp_writer) = migrating_duplex(mac.accept().await.unwrap());
        let gen0_reader = gen0_rx.await.unwrap();
        let mut resp_reader = router
            .handle()
            .inject_response_gene(7, gen0_reader)
            .await
            .expect("splice feed alive")
            .await
            .unwrap();
        resp_writer.write_all(b"pong").await.unwrap();
        resp_writer.finalize().await.unwrap();
        let mut resp = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut resp_reader, &mut resp)
            .await
            .unwrap();
        assert_eq!(resp, "pong");
        req_writer.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn duplex_bidirectional_migration_integrity() {
        let (x_op, x_acc, y_op, y_acc, _tasks) = make_duplex_session().await;
        let (router, _driver) = spawn_response_router(x_acc);
        let mut mac = y_acc.into_migrating_duplex(y_op);
        let upload = 512 * 1024;
        let download = 512 * 1024;
        let (mut req_writer, gen0_rx) = x_op.open_migrating_with_reader(9, LaneClass::Interactive);
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let chunk = vec![0xABu8; 64 * 1024];
            let mut sent = 0;
            while sent < upload {
                req_writer.write_all(&chunk).await.unwrap();
                sent += chunk.len();
            }
            req_writer.finalize().await.unwrap();
        });
        let accepted = mac.accept().await.unwrap();
        let (mut req_reader, mut resp_writer) = migrating_duplex(accepted);
        let mut drains = JoinSet::new();
        spawn_drain(&mut drains, mac);
        tasks.spawn(async move {
            let chunk = vec![0xCDu8; 64 * 1024];
            let mut sent = 0;
            while sent < download {
                resp_writer.write_all(&chunk).await.unwrap();
                sent += chunk.len();
            }
            resp_writer.finalize().await.unwrap();
        });
        let gen0_reader = gen0_rx.await.unwrap();
        let mut resp_reader = router
            .handle()
            .inject_response_gene(9, gen0_reader)
            .await
            .expect("splice feed alive")
            .await
            .unwrap();
        let (up, down) = tokio::join!(
            async {
                let mut buf = Vec::new();
                tokio::io::AsyncReadExt::read_to_end(&mut req_reader, &mut buf)
                    .await
                    .unwrap();
                buf
            },
            async {
                let mut buf = Vec::new();
                tokio::io::AsyncReadExt::read_to_end(&mut resp_reader, &mut buf)
                    .await
                    .unwrap();
                buf
            },
        );
        assert_eq!(up.len(), upload, "upload byte count mismatch");
        assert!(up.iter().all(|b| *b == 0xAB), "upload corrupted");
        assert_eq!(down.len(), download, "download byte count mismatch");
        assert!(down.iter().all(|b| *b == 0xCD), "download corrupted");
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    }

    // -------------------------------------------------------------------
    // Promote-before-write
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn promote_before_write() {
        let (opener, _accepter, _s, _sb, _c, _cb) = make_dual_session().await;

        let mut writer = opener.open_migrating(1, LaneClass::Interactive);

        // A single large write should promote to bulk BEFORE writing
        writer.write_all(&[0u8; 3000]).await.unwrap();

        // The write was sent on the bulk lane. Just verify it didn't error.
        writer.finalize().await.unwrap();
    }

    // -------------------------------------------------------------------
    // Demote streak
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn demote_streak() {
        let (opener, _accepter, _s, _sb, _c, _cb) = make_dual_session().await;

        let mut writer = opener.open_migrating(1, LaneClass::Bulk);

        // Write 4 consecutive small writes — should trigger demotion
        for _ in 0..4 {
            writer.write_all(&[0u8; 100]).await.unwrap();
        }

        // After demotion, the writer should be on interactive lane.
        writer.finalize().await.unwrap();
    }

    // -------------------------------------------------------------------
    // Manual force_migrate
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn force_migrate_manual() {
        let (opener, _accepter, _s, _sb, _c, _cb) = make_dual_session().await;

        let mut writer = opener.open_migrating_manual(1, LaneClass::Interactive);

        writer.write_all(&[0u8; 100]).await.unwrap();

        // Force migration to bulk
        writer.force_migrate(LaneClass::Bulk).await.unwrap();

        writer.write_all(&[0u8; 5000]).await.unwrap();
        writer.finalize().await.unwrap();
    }

    // -------------------------------------------------------------------
    // Close during migration
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn close_during_migration() {
        let (opener, _accepter, _s, _sb, _c, _cb) = make_dual_session().await;

        let mut writer = opener.open_migrating(1, LaneClass::Interactive);

        writer.write_all(&[0u8; 3000]).await.unwrap(); // triggers promote to bulk
        writer.finalize().await.unwrap();
    }

    // -------------------------------------------------------------------
    // Policy: promote is eager
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn promote_is_eager_not_cooled() {
        let (opener, _accepter, _s, _sb, _c, _cb) = make_dual_session().await;

        let mut writer = opener.open_migrating(1, LaneClass::Interactive);

        // Two large writes back-to-back — both should promote immediately
        writer.write_all(&[0u8; 3000]).await.unwrap();
        writer.write_all(&[0u8; 4000]).await.unwrap();
        writer.finalize().await.unwrap();
    }

    // -------------------------------------------------------------------
    // Mirrored classifier records history
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn mirrored_classifier_tracks_bulk_ratio() {
        let mut c = crate::traffic_class::SizeMix::new();

        // Small writes → interactive
        for _ in 0..10 {
            c.record(100);
        }
        assert!(!c.is_bulk(), "all small writes => not bulk");

        // Large writes → bulk
        for _ in 0..10 {
            c.record(3000);
        }
        assert!(c.is_bulk(), "many large writes => bulk");

        // Halving on overflow
        for _ in 0..20 {
            c.record(100);
        }
        assert!(!c.is_bulk(), "halving should let small wins dominate");
    }

    // -------------------------------------------------------------------
    // DualStreamAccepter::into_migrating_capable
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn migrating_capable_accepter_passes_plain_streams() {
        let (opener, accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut mac = accepter.into_migrating_capable();

        let (_reader, mut writer) = opener.open(LaneClass::Interactive).await.unwrap();
        let data = vec![0x00u8; 30];
        writer.write_all(&data).await.unwrap();
        writer.shutdown().unwrap();

        let accepted = mac.accept().await.unwrap();
        match accepted {
            AcceptedStream::Plain { mut reader, .. } => {
                let mut received = Vec::new();
                tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut received)
                    .await
                    .unwrap();
                assert_eq!(received, data);
            }
            _ => panic!("expected Plain stream"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn migrating_capable_accepter_preserves_short_plain_stream() {
        let (opener, accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut mac = accepter.into_migrating_capable();

        let (_reader, mut writer) = opener.open(LaneClass::Interactive).await.unwrap();
        let data = b"short";
        writer.write_all(data).await.unwrap();
        writer.shutdown().unwrap();

        let accepted = mac.accept().await.unwrap();
        match accepted {
            AcceptedStream::Plain { mut reader, .. } => {
                let mut received = Vec::new();
                tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut received)
                    .await
                    .unwrap();
                assert_eq!(received, data);
            }
            _ => panic!("expected Plain stream"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn migrating_only_accepter_discards_empty_and_truncated_streams() {
        let (opener, accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut mac = accepter.into_migrating_only();
        let (_reader, mut empty_writer) = opener.open(LaneClass::Interactive).await.unwrap();
        empty_writer.shutdown().unwrap();
        let (_reader, mut short_writer) = opener.open(LaneClass::Interactive).await.unwrap();
        short_writer.write_all(b"short").await.unwrap();
        short_writer.shutdown().unwrap();
        let mut accept = JoinSet::new();
        accept.spawn(async move { mac.accept().await });
        tokio::task::yield_now().await;
        assert!(
            accept.try_join_next().is_none(),
            "empty or truncated stream escaped as an application stream"
        );
        let mut writer = opener.open_migrating(42, LaneClass::Interactive);
        writer.write_all(b"hello").await.unwrap();
        let accepted = tokio::time::timeout(Duration::from_secs(1), accept.join_next())
            .await
            .expect("strict accepter did not reach the valid migrating stream")
            .unwrap()
            .unwrap()
            .unwrap();
        match accepted {
            AcceptedStream::Migrating { mut reader, .. } => {
                let mut received = [0; 5];
                tokio::io::AsyncReadExt::read_exact(&mut reader, &mut received)
                    .await
                    .unwrap();
                assert_eq!(&received, b"hello");
            }
            other => panic!("expected Migrating, got: {other:?}"),
        }
    }

    // -------------------------------------------------------------------
    // Migrating stream: concurrent send + accept + read (no FINAL)
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn migrating_stream_concurrent_basic() {
        let (opener, accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut mac = accepter.into_migrating_capable();

        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut writer = opener.open_migrating(42, LaneClass::Interactive);
            writer.write_all(b"hello-world").await.unwrap();
            writer.finalize().await.unwrap();
        });

        // Accept gen0
        let accepted = mac.accept().await.unwrap();
        let (reader, _) = migrating(accepted);

        // Drain successor generations so the FINAL gen reaches the
        // SplicedReader's queue before we read.
        let mut drains = JoinSet::new();
        spawn_drain(&mut drains, mac);

        // Read from SplicedReader — gets clean EOF after payload + FINAL
        let mut data = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut Box::pin(reader), &mut data)
            .await
            .unwrap();
        assert_eq!(data, b"hello-world");

        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    }

    // -------------------------------------------------------------------
    // Multi-MB integrity across many forced migrations
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn multi_mb_integrity_across_many_forced_migrations() {
        let (opener, accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut mac = accepter.into_migrating_capable();

        let chunk_size = 10 * 1024;
        let migrations: usize = 100;

        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut writer = opener.open_migrating_manual(77, LaneClass::Interactive);
            for i in 0..migrations {
                let mut chunk = vec![0u8; chunk_size];
                for (j, b) in chunk.iter_mut().enumerate() {
                    *b = (i.wrapping_mul(chunk_size).wrapping_add(j)) as u8;
                }
                writer.write_all(&chunk).await.unwrap();
                let target = if i % 2 == 0 {
                    LaneClass::Bulk
                } else {
                    LaneClass::Interactive
                };
                writer.force_migrate(target).await.unwrap();
            }
            writer.finalize().await.unwrap();
        });

        let accepted = mac.accept().await.unwrap();
        let (mut reader, _) = migrating(accepted);

        let mut drains = JoinSet::new();
        spawn_drain(&mut drains, mac);

        let mut data = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut data)
            .await
            .unwrap();
        assert_eq!(data.len(), chunk_size * migrations, "byte count mismatch");

        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    }

    // -------------------------------------------------------------------
    // Migration during peer-stall
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn migration_during_peer_stall() {
        let (opener, accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut mac = accepter.into_migrating_capable();

        let first = b"AAAA BBBB CCCC DDDD EEEE FFFF GGGG HHHH";
        let second = b"1111 2222 3333 4444 5555 6666 7777 8888";
        let third = b"xxxx yyyy zzzz wwww vvvv uuuu tttt ssss";

        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut writer = opener.open_migrating_manual(1, LaneClass::Interactive);
            writer.write_all(first).await.unwrap();
            writer.force_migrate(LaneClass::Bulk).await.unwrap();
            writer.write_all(second).await.unwrap();
            writer.force_migrate(LaneClass::Interactive).await.unwrap();
            writer.write_all(third).await.unwrap();
            writer.finalize().await.unwrap();
        });

        let accepted = mac.accept().await.unwrap();
        let (mut reader, _) = migrating(accepted);

        let mut drains = JoinSet::new();
        spawn_drain(&mut drains, mac);

        let mut data = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut data)
            .await
            .unwrap();
        let expected: Vec<u8> = first.iter().chain(second).chain(third).copied().collect();
        assert_eq!(data, expected, "data mismatch");

        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    }

    // -------------------------------------------------------------------
    // Close with finalize (FINAL-marker clean EOF)
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn close_with_final_marker_gives_clean_eof() {
        let (opener, accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut mac = accepter.into_migrating_capable();

        // Sender writes gen0 data then finalize (gen1 FINAL)
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut writer = opener.open_migrating(42, LaneClass::Interactive);
            writer.write_all(b"Hello, world!").await.unwrap();
            writer.finalize().await.unwrap();
        });

        // Accept gen0 → SplicedReader with payload
        let accepted = mac.accept().await.unwrap();
        let (mut reader, _) = migrating(accepted);

        // Continuously drain successor generations so the FINAL gen
        // reaches the SplicedReader's queue.
        let mut drains = JoinSet::new();
        drains.spawn(async move { while mac.accept().await.is_ok() {} });

        // SplicedReader should read payload, then see FINAL and get clean EOF
        let mut data = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut data)
            .await
            .unwrap();
        assert_eq!(data, b"Hello, world!");

        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    }

    // -------------------------------------------------------------------
    // Close with finalize after migration
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn close_with_finalize_after_migration() {
        let (opener, accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut mac = accepter.into_migrating_capable();

        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut writer = opener.open_migrating(1, LaneClass::Interactive);
            writer.write_all(b"Prologue ").await.unwrap();
            writer.write_all(&[0u8; 3000]).await.unwrap();
            writer.write_all(b" Epilogue.").await.unwrap();
            writer.finalize().await.unwrap();
        });

        let accepted = mac.accept().await.unwrap();
        let (mut reader, _) = migrating(accepted);

        let mut drains = JoinSet::new();
        spawn_drain(&mut drains, mac);

        let mut buf = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut buf)
            .await
            .unwrap();
        assert!(buf.starts_with("Prologue "), "got: {buf:?}");
        assert!(buf.contains("Epilogue."), "got: {buf:?}");

        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    }

    // -------------------------------------------------------------------
    // Both-directions simultaneous migration
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn simultaneous_bidirectional_migration() {
        let (opener, accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut mac = accepter.into_migrating_capable();

        let opener2 = opener.clone();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut w = opener.open_migrating_manual(10, LaneClass::Interactive);
            w.write_all(b"stream-A-chunk-1").await.unwrap();
            w.force_migrate(LaneClass::Bulk).await.unwrap();
            w.write_all(b"stream-A-chunk-2").await.unwrap();
            w.finalize().await.unwrap();
        });

        let opener3 = opener2.clone();
        tasks.spawn(async move {
            let mut w = opener3.open_migrating_manual(20, LaneClass::Bulk);
            w.write_all(b"stream-B-chunk-1").await.unwrap();
            w.force_migrate(LaneClass::Interactive).await.unwrap();
            w.write_all(b"stream-B-chunk-2").await.unwrap();
            w.finalize().await.unwrap();
        });

        // Accept first gen0
        let a = mac.accept().await.unwrap();
        let (reader_a, _) = migrating(a);

        // Accept second gen0
        let b = mac.accept().await.unwrap();
        let (reader_b, _) = migrating(b);

        // Drain successors while readers consume data
        let mut drains = JoinSet::new();
        spawn_drain(&mut drains, mac);

        let (ra, rb) = tokio::join!(
            async {
                let mut reader = reader_a;
                let mut s = String::new();
                tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut s)
                    .await
                    .unwrap();
                s
            },
            async {
                let mut reader = reader_b;
                let mut s = String::new();
                tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut s)
                    .await
                    .unwrap();
                s
            }
        );

        let mut results = vec![ra, rb];
        results.sort();
        assert_eq!(
            results,
            vec![
                "stream-A-chunk-1stream-A-chunk-2".to_string(),
                "stream-B-chunk-1stream-B-chunk-2".to_string(),
            ]
        );

        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    }

    // -------------------------------------------------------------------
    // Gaming pattern: big sync phase then small deltas
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn gaming_pattern_big_sync_then_small_deltas() {
        let (opener, accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut mac = accepter.into_migrating_capable();

        let sync_size = 3 * 1024 * 1024;
        let deltas = 10;

        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut writer = opener.open_migrating(1, LaneClass::Interactive);
            writer.write_all(&vec![0xABu8; sync_size]).await.unwrap();
            for i in 0..deltas {
                writer
                    .write_all(format!("delta-{:02}-", i).as_bytes())
                    .await
                    .unwrap();
            }
            writer.finalize().await.unwrap();
        });

        let accepted = mac.accept().await.unwrap();
        let (mut reader, _) = migrating(accepted);

        let mut drains = JoinSet::new();
        spawn_drain(&mut drains, mac);

        let mut data = Vec::with_capacity(sync_size + 200);
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut data)
            .await
            .unwrap();

        assert_eq!(data.len(), sync_size + 9 * deltas, "total size mismatch");
        for b in &data[..sync_size] {
            assert_eq!(*b, 0xAB, "sync data corrupted");
        }
        let deltas_str = std::str::from_utf8(&data[sync_size..]).unwrap();
        for i in 0..deltas {
            assert!(
                deltas_str.contains(&format!("delta-{:02}-", i)),
                "missing delta {i}"
            );
        }

        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    }

    // -------------------------------------------------------------------
    // Policy: demotion respects cooldown
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn demotion_respects_cooldown() {
        let (opener, _accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut writer = opener.open_migrating(1, LaneClass::Bulk);
        for _ in 0..20 {
            writer.write_all(&[0u8; 100]).await.unwrap();
        }
        writer.write_all(&[0u8; 3000]).await.unwrap();
        for _ in 0..20 {
            writer.write_all(&[0u8; 100]).await.unwrap();
        }
        for _ in 0..4 {
            writer.write_all(&[0u8; 100]).await.unwrap();
        }
        writer.finalize().await.unwrap();
    }

    // -------------------------------------------------------------------
    // Policy: demotion after cooldown expires
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn demotion_after_cooldown_expires() {
        let (opener, _accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut writer = opener.open_migrating(1, LaneClass::Bulk);
        for _ in 0..20 {
            writer.write_all(&[0u8; 100]).await.unwrap();
        }
        writer.write_all(&[0u8; 3000]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        for _ in 0..20 {
            writer.write_all(&[0u8; 100]).await.unwrap();
        }
        writer.finalize().await.unwrap();
    }

    // -------------------------------------------------------------------
    // Finalize with no data written (PendingOpen) is a no-op
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn finalize_no_data_no_panic() {
        let (opener, _accepter, _s, _sb, _c, _cb) = make_dual_session().await;

        let mut writer = opener.open_migrating(1, LaneClass::Interactive);
        writer.finalize().await.unwrap();
    }

    // -------------------------------------------------------------------
    // Force migrate from PendingOpen (before any writes)
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn force_migrate_from_pending_open() {
        let (opener, accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut mac = accepter.into_migrating_capable();

        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut writer = opener.open_migrating_manual(1, LaneClass::Interactive);
            writer.force_migrate(LaneClass::Bulk).await.unwrap();
            writer.write_all(b"data-on-bulk").await.unwrap();
            writer.finalize().await.unwrap();
        });

        let accepted = mac.accept().await.unwrap();
        let (mut reader, _) = migrating(accepted);

        let mut drains = JoinSet::new();
        spawn_drain(&mut drains, mac);

        let mut data = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut data)
            .await
            .unwrap();
        assert_eq!(data, "data-on-bulk");

        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    }

    // -------------------------------------------------------------------
    // Duplex: client reads peer bytes on gen0
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn duplex_client_reads_peer_bytes_on_gen0() {
        let (opener, accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut mac = accepter.into_migrating_capable();

        let (mut client_reader, client_writer) =
            opener.open_migrating_duplex(7, LaneClass::Interactive);

        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut writer = client_writer;
            writer.write_all(b"hello-from-c2s  ").await.unwrap();
            writer.finalize().await.unwrap();
        });

        let accepted = mac.accept().await.unwrap();
        let (_accepted_reader, mut accepted_writer) = migrating(accepted);
        tokio::io::AsyncWriteExt::write_all(&mut accepted_writer, b"hello-from-s2c  ")
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::shutdown(&mut accepted_writer)
            .await
            .unwrap();

        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }

        let mut resp = [0u8; 16];
        tokio::io::AsyncReadExt::read_exact(&mut client_reader, &mut resp)
            .await
            .unwrap();
        assert_eq!(&resp, b"hello-from-s2c  ");

        let mut buf = [0u8; 1];
        let n = tokio::io::AsyncReadExt::read(&mut client_reader, &mut buf)
            .await
            .unwrap();
        assert_eq!(n, 0, "expected clean EOF after server writer shutdown");
    }

    // -------------------------------------------------------------------
    // Duplex: client reads across forced migration
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn duplex_client_reads_across_forced_migration() {
        let (opener, accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut mac = accepter.into_migrating_capable();

        let (mut client_reader, client_writer) =
            opener.open_migrating_duplex(8, LaneClass::Interactive);

        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut writer = client_writer;
            writer.write_all(b"c2s-1").await.unwrap();
            writer.force_migrate(LaneClass::Bulk).await.unwrap();
            writer.write_all(b"c2s-2").await.unwrap();
            writer.finalize().await.unwrap();
        });

        let accepted = mac.accept().await.unwrap();
        let (_accepted_reader, mut accepted_writer) = migrating(accepted);
        tokio::io::AsyncWriteExt::write_all(&mut accepted_writer, b"first-response ")
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut accepted_writer, b"second-response")
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::shutdown(&mut accepted_writer)
            .await
            .unwrap();

        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }

        let mut resp = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut client_reader, &mut resp)
            .await
            .unwrap();
        assert_eq!(resp, "first-response second-response");
    }

    // -------------------------------------------------------------------
    // Duplex: multi-MB integrity with bidirectional echo
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn duplex_multi_mb_integrity_bidirectional() {
        let (opener, accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut mac = accepter.into_migrating_capable();

        let (mut client_reader, client_writer) =
            opener.open_migrating_duplex(9, LaneClass::Interactive);

        const MIGRATIONS: usize = 100;

        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut writer = client_writer;
            for i in 0..MIGRATIONS {
                let chunk = format!("chunk-{:03}-{:04X}", i, i);
                writer.write_all(chunk.as_bytes()).await.unwrap();
                if i % 2 == 0 {
                    writer.force_migrate(LaneClass::Bulk).await.unwrap();
                } else {
                    writer.force_migrate(LaneClass::Interactive).await.unwrap();
                }
            }
            writer.finalize().await.unwrap();
        });

        // Accept gen0
        let accepted = mac.accept().await.unwrap();
        let (mut accepted_reader, mut accepted_writer) = match accepted {
            AcceptedStream::Migrating { reader, writer, .. } => (reader, writer),
            _ => panic!("expected migrating"),
        };

        // Drain successors while echoing
        let mut drains = JoinSet::new();
        spawn_drain(&mut drains, mac);

        tasks.spawn(async move {
            let mut buf = vec![0u8; 256];
            loop {
                let n = tokio::io::AsyncReadExt::read(&mut accepted_reader, &mut buf)
                    .await
                    .unwrap();
                if n == 0 {
                    break;
                }
                tokio::io::AsyncWriteExt::write_all(&mut accepted_writer, &buf[..n])
                    .await
                    .unwrap();
            }
            tokio::io::AsyncWriteExt::shutdown(&mut accepted_writer)
                .await
                .unwrap();
        });

        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }

        let mut echoed = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut client_reader, &mut echoed)
            .await
            .unwrap();
        let mut expected = Vec::new();
        for i in 0..MIGRATIONS {
            expected.extend_from_slice(format!("chunk-{:03}-{:04X}", i, i).as_bytes());
        }
        assert_eq!(
            echoed.len(),
            expected.len(),
            "total echoed byte count mismatch"
        );
        assert_eq!(echoed, expected, "echoed content mismatch");
    }

    // -------------------------------------------------------------------
    // Write-only open_migrating drops reader, wire behavior unchanged
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn write_only_open_migrating_drops_reader_unchanged() {
        let (opener, accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut mac = accepter.into_migrating_capable();

        let client_writer = opener.open_migrating(10, LaneClass::Interactive);

        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut writer = client_writer;
            writer.write_all(b"write-only-data").await.unwrap();
            writer.finalize().await.unwrap();
        });

        let accepted = mac.accept().await.unwrap();
        let (mut accepted_reader, _) = migrating(accepted);

        // Drain successors (FINAL generation from shutdown) so the
        // SplicedReader can chain through to clean EOF.
        let mut drains = JoinSet::new();
        spawn_drain(&mut drains, mac);

        let mut data = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut accepted_reader, &mut data)
            .await
            .unwrap();
        assert_eq!(data, "write-only-data");

        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shared_feed_second_accepter_splices_byte_exact() {
        let (op1, acc1, _a, _b, _c, _d) = make_dual_session().await;
        let (op2, acc2, _e, _f, _g, _h) = make_dual_session().await;
        let (feed, mut driver) = spawn_splice_router();
        let mut mac1 = acc1.into_migrating_only_with_feed(feed.clone());
        let mac2 = acc2.into_migrating_only_with_feed(feed);
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut w = op1.open_migrating_manual(42, LaneClass::Interactive);
            w.write_all(b"born-on-session-one|").await.unwrap();
            w.rebind(op2).await.unwrap();
            w.write_all(b"continued-on-session-two").await.unwrap();
            w.finalize().await.unwrap();
        });
        let accepted = mac1.accept().await.unwrap();
        let (mut reader, _) = migrating(accepted);
        let mut drains = JoinSet::new();
        spawn_drain(&mut drains, mac1);
        spawn_drain(&mut drains, mac2);
        let mut data = String::new();
        tokio::time::timeout(
            Duration::from_secs(10),
            tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut data),
        )
        .await
        .expect("cross-session splice stalled")
        .unwrap();
        assert_eq!(data, "born-on-session-one|continued-on-session-two");
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        driver.abort_all();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rebind_mid_stream_is_lossless() {
        let (op1, acc1, _a, _b, _c, _d) = make_dual_session().await;
        let (op2, acc2, _e, _f, _g, _h) = make_dual_session().await;
        let (feed, mut driver) = spawn_splice_router();
        let mut mac1 = acc1.into_migrating_only_with_feed(feed.clone());
        let mac2 = acc2.into_migrating_only_with_feed(feed);
        let half = 200 * 1024;
        let pattern: Vec<u8> = (0..2 * half).map(|i| (i % 251) as u8).collect();
        let expected = pattern.clone();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut w = op1.open_migrating(9, LaneClass::Interactive);
            w.write_all(&pattern[..half]).await.unwrap();
            w.rebind(op2).await.unwrap();
            w.write_all(&pattern[half..]).await.unwrap();
            w.finalize().await.unwrap();
        });
        let accepted = mac1.accept().await.unwrap();
        let (mut reader, _) = migrating(accepted);
        let mut drains = JoinSet::new();
        spawn_drain(&mut drains, mac1);
        spawn_drain(&mut drains, mac2);
        let mut data = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(10),
            tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut data),
        )
        .await
        .expect("rebound stream stalled")
        .unwrap();
        assert_eq!(data.len(), expected.len(), "byte count mismatch");
        assert_eq!(data, expected, "bytes lost or reordered across rebind");
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        driver.abort_all();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn response_final_crossing_shared_feed_closes_cleanly() {
        let (x1_op, x1_acc, y1_op, y1_acc, _t1) = make_duplex_session().await;
        let (_x2_op, x2_acc, y2_op, _y2_acc, _t2) = make_duplex_session().await;
        let (mut router, mut driver) = spawn_response_router(x1_acc);
        router.add_accepter(x2_acc, &mut driver);
        let mut mac1 = y1_acc.into_migrating_duplex(y1_op);
        let (mut req_writer, gen0_rx) = x1_op.open_migrating_with_reader(7, LaneClass::Interactive);
        req_writer.write_all(b"ping").await.unwrap();
        let accepted = mac1.accept().await.unwrap();
        let mut resp_writer = match accepted {
            AcceptedStream::MigratingDuplex { writer, .. } => writer,
            other => panic!("expected MigratingDuplex, got {other:?}"),
        };
        let gen0_reader = gen0_rx.await.unwrap();
        let mut resp_reader = router
            .handle()
            .inject_response_gene(7, gen0_reader)
            .await
            .expect("splice feed alive")
            .await
            .unwrap();
        resp_writer.write_all(b"pong-").await.unwrap();
        resp_writer.rebind(y2_op).await.unwrap();
        resp_writer.write_all(b"across").await.unwrap();
        resp_writer.finalize().await.unwrap();
        let mut resp = String::new();
        tokio::time::timeout(
            Duration::from_secs(10),
            tokio::io::AsyncReadExt::read_to_string(&mut resp_reader, &mut resp),
        )
        .await
        .expect("RESPONSE|FINAL did not cross the shared feed")
        .unwrap();
        assert_eq!(resp, "pong-across");
        req_writer.finalize().await.unwrap();
    }

    fn migrating(accepted: AcceptedStream) -> (SplicedReader, StreamWriter) {
        match accepted {
            AcceptedStream::Migrating { reader, writer, .. } => (reader, writer),
            other => panic!("expected Migrating, got {other:?}"),
        }
    }

    fn migrating_duplex(accepted: AcceptedStream) -> (SplicedReader, MigratingStreamWriter) {
        match accepted {
            AcceptedStream::MigratingDuplex { reader, writer, .. } => (reader, writer),
            other => panic!("expected MigratingDuplex, got {other:?}"),
        }
    }

    fn spawn_drain(tasks: &mut JoinSet<()>, mut mac: MigratingCapableAccepter) {
        tasks.spawn(async move {
            loop {
                let _ = mac.accept().await;
            }
        });
    }

    fn counting_dead_opener(
        tasks: &mut JoinSet<()>,
    ) -> (DualStreamOpener, Arc<std::sync::atomic::AtomicUsize>) {
        use crate::stream::opener::{StreamOpener, stream_open_channel};
        let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (int_tx, mut int_rx) = stream_open_channel();
        let (bulk_tx, _bulk_rx) = stream_open_channel();
        let opens_task = Arc::clone(&opens);
        tasks.spawn(async move {
            while int_rx.recv().await.is_ok() {
                opens_task.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        });
        let opener = DualStreamOpener::new(
            StreamOpener::new(int_tx),
            StreamOpener::new(bulk_tx),
            Liveness::new(),
        );
        (opener, opens)
    }

    #[test]
    fn dropping_a_writer_outside_the_runtime_does_not_panic() {
        let runtime = tokio::runtime::Runtime::new().expect("a runtime for setup");
        let writer = runtime.block_on(async {
            let (opener, _accepter, _srv_int, _srv_bulk, _cli_int, _cli_bulk) =
                make_dual_session().await;
            let mut writer = opener.open_migrating_manual(1, LaneClass::Interactive);
            writer
                .write_all(b"announce")
                .await
                .expect("the session is live");
            writer
                .force_migrate(LaneClass::Interactive)
                .await
                .expect("migrating is a local state change");
            writer
        });
        drop(writer);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dropping_a_writer_does_not_open_a_final_generation() {
        let (opener, _accepter, _srv_int, _srv_bulk, _cli_int, _cli_bulk) =
            make_dual_session().await;
        let mut writer = opener.open_migrating_manual(1, LaneClass::Interactive);
        writer
            .write_all(b"announce")
            .await
            .expect("the session is live");
        let mut dead_tasks = JoinSet::new();
        let (dead, opens) = counting_dead_opener(&mut dead_tasks);
        writer
            .rebind(dead)
            .await
            .expect("rebinding is a local state change");
        drop(writer);
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            opens.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "dropping an opened writer opened a FINAL generation on the peer, so a dropped stream is treated as cleanly closed instead of an abort"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_finalize_is_not_retried_behind_the_caller() {
        let (opener, _accepter, _srv_int, _srv_bulk, _cli_int, _cli_bulk) =
            make_dual_session().await;
        let mut writer = opener.open_migrating_manual(1, LaneClass::Interactive);
        writer
            .write_all(b"announce")
            .await
            .expect("the session is live");
        let mut dead_tasks = JoinSet::new();
        let (dead, opens) = counting_dead_opener(&mut dead_tasks);
        writer
            .rebind(dead)
            .await
            .expect("rebinding is a local state change");
        writer
            .finalize()
            .await
            .expect_err("the opener never fulfils a request");
        drop(writer);
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            opens.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a finalize the caller was told had failed was retried from Drop, so the peer can still receive a FINAL for a stream whose close reported an error"
        );
    }

    #[tokio::test]
    async fn a_repeated_gen0_releases_its_waiter_instead_of_parking_the_accepter() {
        let (feed, mut driver) = spawn_splice_router();
        let handle = feed;
        let gen0 = |logical_id| ResumeHeader {
            logical_id,
            generation: 0,
            is_final: false,
            is_response: false,
        };
        let first = handle.await_gene(7).await.expect("splice feed alive");
        handle
            .send_continuation(gen0(7), Box::pin(tokio::io::empty()) as GenerationReader)
            .await
            .expect("the feed is alive");
        let _live = first.await.expect("the first gen-0 is spliced");
        let second = handle.await_gene(7).await.expect("splice feed alive");
        handle
            .send_continuation(gen0(7), Box::pin(tokio::io::empty()) as GenerationReader)
            .await
            .expect("the feed is alive");
        let settled = tokio::time::timeout(Duration::from_millis(200), second).await;
        let Ok(result) = settled else {
            panic!(
                "the repeated gen-0 left its waiter parked, so 'accept' blocks forever and the session takes no further stream"
            );
        };
        assert!(
            result.is_err(),
            "the repeated gen-0 handed out a second reader for a logical stream that already has one"
        );
        driver.abort_all();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn closing_a_never_announced_stream_tells_the_peer_nothing() {
        let (x_op, x_acc, y_op, y_acc, _tasks) = make_duplex_session().await;
        let (_router, _driver) = spawn_response_router(x_acc);
        let mut mac = y_acc.into_migrating_duplex(y_op);
        let mut writer = x_op.open_migrating_manual(77, LaneClass::Interactive);
        writer
            .force_migrate(LaneClass::Bulk)
            .await
            .expect("migrating is a local state change");
        writer.finalize().await.expect("closing an unopened stream");
        drop(writer);
        let accepted = tokio::time::timeout(Duration::from_millis(500), mac.accept()).await;
        assert!(
            accepted.is_err(),
            "the peer accepted a stream that was never opened: the close wrote a FINAL on a chain still at generation 0, which is the marker that introduces a stream"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn silent_substreams_do_not_block_accepting_others() {
        const SILENT: usize = 60;
        let (x_op, x_acc, y_op, y_acc, _tasks) = make_duplex_session().await;
        let (_router, _driver) = spawn_response_router(x_acc);
        let mut mac = y_acc.into_migrating_duplex(y_op);
        let mut silent = Vec::new();
        for _ in 0..SILENT {
            silent.push(x_op.open(LaneClass::Interactive).await.unwrap());
        }
        let (mut req_writer, _gen0_rx) =
            x_op.open_migrating_with_reader(11, LaneClass::Interactive);
        req_writer.write_all(b"payload").await.unwrap();
        let budget = RESUME_HEADER_DEADLINE * 4;
        let accepted = tokio::time::timeout(budget, mac.accept())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "the {SILENT} silent sub-streams head-of-line-blocked the accepter for {SILENT} x RESUME_HEADER_DEADLINE ({:?}), stalling every other sub-stream on the session - including the successor generations live migrating streams wait on",
                    RESUME_HEADER_DEADLINE * SILENT as u32
                )
            })
            .unwrap();
        let (mut req_reader, _) = migrating_duplex(accepted);
        let mut got = [0u8; 7];
        tokio::io::AsyncReadExt::read_exact(&mut req_reader, &mut got)
            .await
            .unwrap();
        assert_eq!(&got, b"payload");
        req_writer.finalize().await.unwrap();
        drop(silent);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn silent_substreams_do_not_block_the_response_router() {
        const SILENT: usize = 60;
        let (x_op, x_acc, y_op, y_acc, _tasks) = make_duplex_session().await;
        let (router, _driver) = spawn_response_router(x_acc);
        let y_op2 = y_op.clone();
        let mut mac = y_acc.into_migrating_duplex(y_op);
        let (mut req_writer, gen0_rx) = x_op.open_migrating_with_reader(21, LaneClass::Interactive);
        req_writer.write_all(b"ping").await.unwrap();
        let (_, mut resp_writer) = migrating_duplex(mac.accept().await.unwrap());
        let gen0_reader = gen0_rx.await.unwrap();
        let mut resp_reader = router
            .handle()
            .inject_response_gene(21, gen0_reader)
            .await
            .expect("splice feed alive")
            .await
            .unwrap();
        resp_writer.write_all(b"pong-").await.unwrap();
        let mut silent = Vec::new();
        for _ in 0..SILENT {
            silent.push(y_op2.open(LaneClass::Interactive).await.unwrap());
        }
        resp_writer.force_migrate(LaneClass::Bulk).await.unwrap();
        resp_writer.write_all(b"across").await.unwrap();
        resp_writer.finalize().await.unwrap();
        let budget = RESUME_HEADER_DEADLINE * 4;
        let mut resp = String::new();
        tokio::time::timeout(
            budget,
            tokio::io::AsyncReadExt::read_to_string(&mut resp_reader, &mut resp),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "the {SILENT} silent sub-streams head-of-line-blocked the response router for {SILENT} x RESUME_HEADER_DEADLINE ({:?}), stalling every response successor generation on the session",
                RESUME_HEADER_DEADLINE * SILENT as u32
            )
        })
        .unwrap();
        assert_eq!(resp, "pong-across");
        req_writer.finalize().await.unwrap();
        drop(silent);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn superseded_generations_release_their_readers() {
        const MIGRATIONS: usize = 24;
        let (x_op, x_acc, y_op, y_acc, _tasks) = make_duplex_session().await;
        let (_router, _driver) = spawn_response_router(x_acc);
        let mut mac = y_acc.into_migrating_duplex(y_op);
        let (mut req_writer, gen0_rx) = x_op.open_migrating_with_reader(5, LaneClass::Interactive);
        req_writer.write_all(b"gen0").await.unwrap();
        let (mut req_reader, _resp_writer) = migrating_duplex(mac.accept().await.unwrap());
        let _gen0_reader = gen0_rx.await.unwrap();
        let mut drains = JoinSet::new();
        spawn_drain(&mut drains, mac);
        for i in 0..MIGRATIONS {
            let target = match i % 2 {
                0 => LaneClass::Bulk,
                _ => LaneClass::Interactive,
            };
            req_writer.force_migrate(target).await.unwrap();
            req_writer.write_all(b"genn").await.unwrap();
        }
        let held = usize::from(req_writer.latest_held_reader.is_some());
        assert!(
            held <= 1,
            "held {held} readers after {MIGRATIONS} migrations; a superseded generation is write-shut on both sides, so holding its reader pins a stream-table entry on both peers - and a 1024-slot read channel - per migration, for the life of the stream"
        );
        req_writer.finalize().await.unwrap();
        let mut got = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut req_reader, &mut got)
            .await
            .unwrap();
        assert_eq!(
            got.len(),
            4 * (MIGRATIONS + 1),
            "bytes lost across generations"
        );
        assert_eq!(&got[..4], b"gen0");
        assert!(got[4..].as_chunks::<4>().0.iter().all(|c| c == b"genn"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rebind_revives_writer_after_open_failure() {
        let (op1, _acc1, s1a, s1b, s1c, s1d) = make_dual_session().await;
        let (op2, acc2, _e, _f, _g, _h) = make_dual_session().await;
        drop((s1a, s1b, s1c, s1d));
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut mac2 = acc2.into_migrating_only();
        let mut w = op1.open_migrating(11, LaneClass::Interactive);
        assert!(
            w.write_all(b"dead").await.is_err(),
            "write on a dead session must fail"
        );
        w.rebind(op2).await.unwrap();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            w.write_all(b"revived").await.unwrap();
            w.finalize().await.unwrap();
        });
        let accepted = tokio::time::timeout(Duration::from_secs(10), mac2.accept())
            .await
            .expect("rebound stream never reached the fresh session")
            .unwrap();
        let (mut reader, _) = migrating(accepted);
        let mut drains = JoinSet::new();
        spawn_drain(&mut drains, mac2);
        let mut data = String::new();
        tokio::time::timeout(
            Duration::from_secs(10),
            tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut data),
        )
        .await
        .expect("rebound stream stalled")
        .unwrap();
        assert_eq!(data, "revived");
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn add_accepter_reaps_finished_accepters() {
        let (_x_op, live, _y_op, _y_acc, live_tasks) = make_duplex_session().await;
        let (mut router, mut driver) = spawn_response_router(live);
        for _ in 0..8 {
            let (_x_op, dead, _y_op, _y_acc, tasks) = make_duplex_session().await;
            drop(tasks);
            router.add_accepter(dead, &mut driver);
            tokio::time::sleep(Duration::from_millis(20)).await;
            while let Some(joined) = driver.try_join_next() {
                joined.unwrap();
            }
        }
        assert!(
            driver.len() <= 3,
            "finished accepter tasks accumulate: {}",
            driver.len()
        );
        drop(live_tasks);
    }

    struct CountingWrite<W> {
        inner: W,
        count: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl<W: AsyncWrite + Unpin> AsyncWrite for CountingWrite<W> {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let n = std::task::ready!(Pin::new(&mut self.inner).poll_write(cx, buf))?;
            self.count
                .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
            Poll::Ready(Ok(n))
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    async fn make_counted_dual_session() -> (
        DualStreamOpener,
        DualStreamAccepter,
        Arc<std::sync::atomic::AtomicUsize>,
        Vec<tokio::task::JoinSet<crate::session::MuxError>>,
    ) {
        let buf_size = 4 * 1024 * 1024;
        let (int_c2s, int_s2c) = duplex(buf_size);
        let (bulk_c2s, bulk_s2c) = duplex(buf_size);
        let (int_srv_r, int_srv_w) = tokio::io::split(int_c2s);
        let (int_cli_r, int_cli_w) = tokio::io::split(int_s2c);
        let (bulk_srv_r, bulk_srv_w) = tokio::io::split(bulk_c2s);
        let (bulk_cli_r, bulk_cli_w) = tokio::io::split(bulk_s2c);
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let int_srv_w = CountingWrite {
            inner: int_srv_w,
            count: Arc::clone(&count),
        };
        let bulk_srv_w = CountingWrite {
            inner: bulk_srv_w,
            count: Arc::clone(&count),
        };
        let srv_cfg = MuxConfig {
            initiation: Initiation::Server,
            heartbeat_interval: Duration::from_secs(1),
            frame_reassembly: false,
        };
        let cli_cfg = MuxConfig {
            initiation: Initiation::Client,
            heartbeat_interval: Duration::from_secs(1),
            frame_reassembly: false,
        };
        let mut tasks = Vec::new();
        let mut js = tokio::task::JoinSet::new();
        let (int_srv_op, _) =
            spawn_mux_no_reconnection(int_srv_r, int_srv_w, srv_cfg.clone(), &mut js);
        tasks.push(js);
        let mut js = tokio::task::JoinSet::new();
        let (bulk_srv_op, _) = spawn_mux_no_reconnection(bulk_srv_r, bulk_srv_w, srv_cfg, &mut js);
        tasks.push(js);
        let mut js = tokio::task::JoinSet::new();
        let (_, int_cli_acc) =
            spawn_mux_no_reconnection(int_cli_r, int_cli_w, cli_cfg.clone(), &mut js);
        tasks.push(js);
        let mut js = tokio::task::JoinSet::new();
        let (_, bulk_cli_acc) = spawn_mux_no_reconnection(bulk_cli_r, bulk_cli_w, cli_cfg, &mut js);
        tasks.push(js);
        let opener = DualStreamOpener::new(int_srv_op, bulk_srv_op, Liveness::new());
        let accepter = DualStreamAccepter::new(int_cli_acc, bulk_cli_acc, Liveness::new());
        (opener, accepter, count, tasks)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn many_streams_migrating_and_rebinding_at_once_keep_their_own_bytes() {
        use std::sync::atomic::Ordering::Relaxed;
        const STREAMS: u64 = 16;
        const CHUNKS: usize = 8;
        const CHUNK: usize = 4 * 1024;
        fn payload_byte(id: u64, i: usize) -> u8 {
            (id.wrapping_mul(97).wrapping_add(i as u64 * 31) % 251) as u8
        }
        let (op1, acc1, _c1, _t1) = make_counted_dual_session().await;
        let (op2, acc2, session2_bytes, _t2) = make_counted_dual_session().await;
        let (feed, mut driver) = spawn_splice_router();
        let mut mac1 = acc1.into_migrating_only_with_feed(feed.clone());
        let mac2 = acc2.into_migrating_only_with_feed(feed);
        let mut writers = JoinSet::new();
        for id in 0..STREAMS {
            let op1 = op1.clone();
            let op2 = op2.clone();
            writers.spawn(async move {
                let mut w = op1.open_migrating_manual(id, LaneClass::Interactive);
                w.write_all(&id.to_be_bytes()).await.unwrap();
                let rebind_at = (id as usize * 3) % CHUNKS;
                for c in 0..CHUNKS {
                    let chunk: Vec<u8> = (0..CHUNK)
                        .map(|j| payload_byte(id, 8 + c * CHUNK + j))
                        .collect();
                    w.write_all(&chunk).await.unwrap();
                    if c == rebind_at {
                        w.rebind(op2.clone()).await.unwrap();
                    } else {
                        let target = if c.is_multiple_of(2) {
                            LaneClass::Bulk
                        } else {
                            LaneClass::Interactive
                        };
                        w.force_migrate(target).await.unwrap();
                    }
                }
                w.finalize().await.unwrap();
            });
        }
        let mut drains = JoinSet::new();
        spawn_drain(&mut drains, mac2);
        let mut readers: JoinSet<u64> = JoinSet::new();
        for _ in 0..STREAMS {
            let accepted = tokio::time::timeout(Duration::from_secs(30), mac1.accept())
                .await
                .expect("a gen-0 never arrived")
                .unwrap();
            let (mut reader, _) = migrating(accepted);
            readers.spawn(async move {
                let mut data = Vec::new();
                tokio::time::timeout(
                    Duration::from_secs(30),
                    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut data),
                )
                .await
                .expect("a stream stalled")
                .unwrap();
                assert_eq!(
                    data.len(),
                    8 + CHUNKS * CHUNK,
                    "short stream: {} bytes",
                    data.len()
                );
                let id = u64::from_be_bytes(data[..8].try_into().unwrap());
                assert!(id < STREAMS, "stream announced a nonexistent id {id}");
                for (i, b) in data.iter().enumerate().skip(8) {
                    assert_eq!(
                        *b,
                        payload_byte(id, i),
                        "stream {id} byte {i}: another stream's bytes, or its own reordered"
                    );
                }
                id
            });
        }
        spawn_drain(&mut drains, mac1);
        let mut seen = std::collections::HashSet::new();
        while let Some(id) = readers.join_next().await {
            assert!(seen.insert(id.unwrap()), "two readers claimed the same id");
        }
        assert_eq!(seen.len(), STREAMS as usize, "a stream never arrived");
        while let Some(w) = writers.join_next().await {
            w.unwrap();
        }
        let carried = session2_bytes.load(Relaxed);
        assert!(
            carried > STREAMS as usize * CHUNK,
            "the rebind target carried only {carried} bytes, so nothing rebound onto it"
        );
        driver.abort_all();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_duplex_streams_do_not_cross_their_responses() {
        const STREAMS: u64 = 12;
        const CHUNKS: usize = 6;
        const CHUNK: usize = 4 * 1024;
        fn request_byte(id: u64, i: usize) -> u8 {
            (id.wrapping_mul(97).wrapping_add(i as u64 * 31) % 251) as u8
        }
        fn response_byte(id: u64, i: usize) -> u8 {
            (id.wrapping_mul(131).wrapping_add(i as u64 * 17) % 241) as u8
        }
        fn body(id: u64, byte: fn(u64, usize) -> u8) -> Vec<u8> {
            let mut out = id.to_be_bytes().to_vec();
            out.extend((8..8 + CHUNKS * CHUNK).map(|i| byte(id, i)));
            out
        }
        let (x_op, x_acc, y_op, y_acc, _tasks) = make_duplex_session().await;
        let (router, _driver) = spawn_response_router(x_acc);
        let mut mac = y_acc.into_migrating_duplex(y_op);
        let mut clients: JoinSet<u64> = JoinSet::new();
        for id in 0..STREAMS {
            let (mut req_writer, gen0_rx) = x_op.open_migrating_with_reader(id, LaneClass::Bulk);
            let handle = router.handle();
            clients.spawn(async move {
                let request = body(id, request_byte);
                let mut send_tasks = JoinSet::new();
                send_tasks.spawn(async move {
                    req_writer.write_all(&request[..8]).await.unwrap();
                    for c in 0..CHUNKS {
                        let at = 8 + c * CHUNK;
                        req_writer
                            .write_all(&request[at..at + CHUNK])
                            .await
                            .unwrap();
                        let target = if c.is_multiple_of(2) {
                            LaneClass::Interactive
                        } else {
                            LaneClass::Bulk
                        };
                        req_writer.force_migrate(target).await.unwrap();
                    }
                    req_writer.finalize().await.unwrap();
                    assert!(
                        req_writer.chain.generations_started() > CHUNKS as u32,
                        "request {id} never migrated"
                    );
                });
                let gen0_reader = gen0_rx.await.expect("the request never opened");
                let mut resp_reader = handle
                    .inject_response_gene(id, gen0_reader)
                    .await
                    .expect("splice feed alive")
                    .await
                    .expect("no response reader");
                let mut got = Vec::new();
                tokio::time::timeout(
                    Duration::from_secs(30),
                    tokio::io::AsyncReadExt::read_to_end(&mut resp_reader, &mut got),
                )
                .await
                .expect("a response stalled")
                .unwrap();
                while let Some(result) = send_tasks.join_next().await {
                    result.unwrap();
                }
                assert_eq!(
                    got,
                    body(id, response_byte),
                    "stream {id} got the wrong response"
                );
                id
            });
        }
        let mut servers = JoinSet::new();
        for _ in 0..STREAMS {
            let accepted = tokio::time::timeout(Duration::from_secs(30), mac.accept())
                .await
                .expect("a request never arrived")
                .unwrap();
            let (mut req_reader, mut resp_writer) = migrating_duplex(accepted);
            servers.spawn(async move {
                let mut got = Vec::new();
                tokio::time::timeout(
                    Duration::from_secs(30),
                    tokio::io::AsyncReadExt::read_to_end(&mut req_reader, &mut got),
                )
                .await
                .expect("a request stalled")
                .unwrap();
                let id = u64::from_be_bytes(got[..8].try_into().unwrap());
                assert_eq!(got, body(id, request_byte), "request {id} arrived wrong");
                let response = body(id, response_byte);
                resp_writer.write_all(&response[..8]).await.unwrap();
                for c in 0..CHUNKS {
                    let at = 8 + c * CHUNK;
                    resp_writer
                        .write_all(&response[at..at + CHUNK])
                        .await
                        .unwrap();
                    let target = if c.is_multiple_of(2) {
                        LaneClass::Bulk
                    } else {
                        LaneClass::Interactive
                    };
                    resp_writer.force_migrate(target).await.unwrap();
                }
                resp_writer.finalize().await.unwrap();
                assert!(
                    resp_writer.chain.generations_started() > CHUNKS as u32,
                    "response {id} never migrated"
                );
            });
        }
        let mut drains = JoinSet::new();
        spawn_drain(&mut drains, mac);
        let mut seen = std::collections::HashSet::new();
        while let Some(id) = clients.join_next().await {
            assert!(seen.insert(id.unwrap()), "two clients claimed the same id");
        }
        assert_eq!(seen.len(), STREAMS as usize, "a response never arrived");
        while let Some(s) = servers.join_next().await {
            s.unwrap();
        }
    }

    #[tokio::test]
    async fn the_first_registration_for_a_gen0_owns_it() {
        let (feed, mut driver) = spawn_splice_router();
        let handle = feed;
        let first = handle.await_gene(3).await.expect("splice feed alive");
        let second = handle.await_gene(3).await.expect("splice feed alive");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (theirs, _ours) = tokio::io::duplex(64);
        handle
            .send_continuation(
                ResumeHeader {
                    logical_id: 3,
                    generation: 0,
                    is_final: false,
                    is_response: false,
                },
                Box::pin(theirs) as GenerationReader,
            )
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), first)
                .await
                .expect("the first registration was never answered")
                .is_ok(),
            "the stream went to a later registration than the one that opened it"
        );
        assert!(
            !matches!(
                tokio::time::timeout(Duration::from_millis(200), second).await,
                Ok(Ok(_))
            ),
            "two registrations were both told they own the same stream"
        );
        driver.abort_all();
    }

    #[tokio::test]
    async fn an_unclaimed_gen0_is_eventually_let_go() {
        let (feed, mut driver) = spawn_splice_router();
        let handle = feed;
        let mut peer_halves = Vec::new();
        for logical_id in 0..(MAX_UNCLAIMED_GEN0 as u64 + 8) {
            let (theirs, ours) = tokio::io::duplex(64);
            let header = ResumeHeader {
                logical_id,
                generation: 0,
                is_final: false,
                is_response: true,
            };
            handle
                .send_continuation(header, Box::pin(theirs) as GenerationReader)
                .await
                .unwrap();
            peer_halves.push(ours);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            peer_halves[0].write_all(b"x").await.is_err(),
            "the oldest unclaimed gen-0 is still pinned, so a peer can pin one per header it sends"
        );
        assert!(
            peer_halves
                .last_mut()
                .unwrap()
                .write_all(b"x")
                .await
                .is_ok(),
            "a gen-0 whose registration is still on its way was thrown away"
        );
        driver.abort_all();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_dead_old_session_after_a_rebind_does_not_abort_the_stream() {
        const STREAMS: u64 = 8;
        const CHUNKS: usize = 6;
        const CHUNK: usize = 4 * 1024;
        const BEFORE_KILL: usize = 8 + 2 * CHUNK;
        fn payload_byte(id: u64, i: usize) -> u8 {
            (id.wrapping_mul(97).wrapping_add(i as u64 * 31) % 251) as u8
        }
        fn body(id: u64) -> Vec<u8> {
            let mut out = id.to_be_bytes().to_vec();
            out.extend((8..8 + CHUNKS * CHUNK).map(|i| payload_byte(id, i)));
            out
        }
        let (op1, acc1, s1a, s1b, s1c, s1d) = make_dual_session().await;
        let (op2, acc2, _s2a, _s2b, _s2c, _s2d) = make_dual_session().await;
        let (feed, mut driver) = spawn_splice_router();
        let mut mac1 = acc1.into_migrating_only_with_feed(feed.clone());
        let mac2 = acc2.into_migrating_only_with_feed(feed);
        let mut drains = JoinSet::new();
        spawn_drain(&mut drains, mac2);
        let (killed_tx, killed_rx) = tokio::sync::watch::channel(false);
        let (past_rebind_tx, mut past_rebind_rx) = tokio::sync::mpsc::channel(STREAMS as usize);
        let mut writers = JoinSet::new();
        for id in 0..STREAMS {
            let op1 = op1.clone();
            let op2 = op2.clone();
            let mut killed_rx = killed_rx.clone();
            writers.spawn(async move {
                let data = body(id);
                let mut w = op1.open_migrating_manual(id, LaneClass::Interactive);
                w.write_all(&data[..8 + CHUNK]).await.unwrap();
                w.rebind(op2).await.unwrap();
                w.write_all(&data[8 + CHUNK..BEFORE_KILL]).await.unwrap();
                while !*killed_rx.borrow() {
                    killed_rx.changed().await.unwrap();
                }
                w.write_all(&data[BEFORE_KILL..]).await.unwrap();
                w.finalize().await.unwrap();
                assert!(
                    w.chain.generations_started() > 1,
                    "stream {id} did not rebind onto the second session"
                );
            });
        }
        let mut readers: JoinSet<u64> = JoinSet::new();
        for _ in 0..STREAMS {
            let accepted = tokio::time::timeout(Duration::from_secs(30), mac1.accept())
                .await
                .expect("a gen-0 never arrived")
                .unwrap();
            let (mut reader, _) = migrating(accepted);
            let past_rebind_tx = past_rebind_tx.clone();
            readers.spawn(async move {
                let mut head = vec![0u8; BEFORE_KILL];
                tokio::time::timeout(
                    Duration::from_secs(30),
                    tokio::io::AsyncReadExt::read_exact(&mut reader, &mut head),
                )
                .await
                .expect("a stream stalled before the rebind")
                .unwrap();
                past_rebind_tx.send(()).await.unwrap();
                let mut tail = Vec::new();
                tokio::time::timeout(
                    Duration::from_secs(30),
                    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut tail),
                )
                .await
                .expect("the old session's death aborted a live stream")
                .unwrap();
                head.extend(tail);
                let id = u64::from_be_bytes(head[..8].try_into().unwrap());
                assert_eq!(head, body(id), "stream {id} lost or crossed bytes");
                id
            });
        }
        drop(past_rebind_tx);
        for _ in 0..STREAMS {
            past_rebind_rx.recv().await.expect("a reader gave up");
        }
        let probe = op1.clone();
        drop((op1, mac1, s1a, s1b, s1c, s1d));
        killed_tx.send(true).unwrap();
        let mut seen = std::collections::HashSet::new();
        while let Some(id) = readers.join_next().await {
            assert!(seen.insert(id.unwrap()), "two readers claimed the same id");
        }
        assert_eq!(seen.len(), STREAMS as usize, "a stream never arrived");
        while let Some(w) = writers.join_next().await {
            w.unwrap();
        }
        let mut dead = probe.open_migrating_manual(STREAMS + 1, LaneClass::Interactive);
        assert!(
            dead.write_all(b"x").await.is_err(),
            "the old session still opens streams, so it was never killed"
        );
        driver.abort_all();
    }
}
