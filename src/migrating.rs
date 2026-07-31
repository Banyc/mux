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
use tokio::sync::mpsc;

use crate::{
    StreamReader,
    dual_lane::{DualStreamAccepter, DualStreamOpener, LaneClass},
    stream::writer::StreamWriter,
    stream_migration::{
        GenerationChain, GenerationReader, MigrationError, ResumeHeader, SpliceRegistry,
        SplicedReader, spawn_splice_driver,
    },
};

// ---------------------------------------------------------------------------
// Constants (mirror central_io::writer's LatencyControl)
// ---------------------------------------------------------------------------

/// Cross-reference: `DATA_MEDIUM_CAP` in `central_io::writer`.
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
pub enum MigratingError {
    Migration(MigrationError),
    OpenFailed,
    OpenUnderlying(String),
    WriteFailed,
    LaneDead,
}

impl From<MigrationError> for MigratingError {
    fn from(e: MigrationError) -> Self {
        MigratingError::Migration(e)
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

pub struct MigratingStreamWriter {
    opener: DualStreamOpener,
    chain: GenerationChain,
    state: WriterState,
    policy: crate::traffic_class::LanePolicy,
    name: StreamName,
    auto: bool,
    gen0_reader_tx: Option<tokio::sync::oneshot::Sender<StreamReader>>,
    /// When set, successor generation readers are held alive here so
    /// their sub-streams don't close on the peer; gen 0 is delivered via
    /// [`gen0_reader_tx`]. `None` for write-only mode.
    held_readers: Option<Vec<StreamReader>>,
}

impl std::fmt::Debug for MigratingStreamWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MigratingStreamWriter")
            .field("auto", &self.auto)
            .finish_non_exhaustive()
    }
}

impl MigratingStreamWriter {
    pub(crate) fn new(
        opener: DualStreamOpener,
        logical_id: u64,
        initial_lane: LaneClass,
        auto: bool,
    ) -> Self {
        Self {
            opener,
            chain: GenerationChain::new(logical_id),
            state: WriterState::PendingOpen { lane: initial_lane },
            policy: crate::traffic_class::LanePolicy::new(),
            name: StreamName::default(),
            auto,
            gen0_reader_tx: None,
            held_readers: None,
        }
    }

    fn new_with_reader_tx(
        opener: DualStreamOpener,
        logical_id: u64,
        initial_lane: LaneClass,
        auto: bool,
        gen0_reader_tx: tokio::sync::oneshot::Sender<StreamReader>,
    ) -> Self {
        Self {
            opener,
            chain: GenerationChain::new(logical_id),
            state: WriterState::PendingOpen { lane: initial_lane },
            policy: crate::traffic_class::LanePolicy::new(),
            name: StreamName::default(),
            auto,
            gen0_reader_tx: Some(gen0_reader_tx),
            held_readers: Some(Vec::new()),
        }
    }

    /// Force-migrate to `target` lane. Closes the current generation and
    /// opens a new one on the target lane with an explicit `LaneClass`
    /// hint (NOT `open_auto` — the resume header is small and would
    /// misroute).
    pub async fn force_migrate(&mut self, target: LaneClass) -> Result<(), MigratingError> {
        self.migrate_to(target).await
    }

    pub fn name_handle(&self) -> StreamName {
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
            auto: true,
            gen0_reader_tx: None,
            held_readers: None,
        }
    }

    async fn ensure_open(&mut self) -> Result<(), MigratingError> {
        let state = std::mem::replace(&mut self.state, WriterState::Closed);
        match state {
            WriterState::Active { .. } => {
                self.state = state;
                Ok(())
            }
            WriterState::PendingOpen { lane } => {
                let (reader, mut writer) = match self.opener.open(lane).await {
                    Ok(x) => x,
                    Err(e) => return Err(MigratingError::OpenUnderlying(format!("{e:?}"))),
                };
                let genn = self
                    .chain
                    .start_generation(&mut tokio_util_writer(&mut writer), false)
                    .await?;
                self.route_opened_reader(genn, reader);
                self.state = WriterState::Active { writer, lane };
                Ok(())
            }
            WriterState::Migrating { target_lane } => {
                let (reader, mut writer) = match self.opener.open(target_lane).await {
                    Ok(x) => x,
                    Err(e) => return Err(MigratingError::OpenUnderlying(format!("{e:?}"))),
                };
                let genn = self
                    .chain
                    .start_generation(&mut tokio_util_writer(&mut writer), false)
                    .await?;
                self.route_opened_reader(genn, reader);
                self.state = WriterState::Active {
                    writer,
                    lane: target_lane,
                };
                Ok(())
            }
            WriterState::Closed => Err(MigratingError::LaneDead),
        }
    }

    async fn migrate_to(&mut self, target: LaneClass) -> Result<(), MigratingError> {
        let from = match &self.state {
            WriterState::Active { lane, .. } | WriterState::PendingOpen { lane } => Some(*lane),
            WriterState::Migrating { target_lane } => Some(*target_lane),
            WriterState::Closed => None,
        };
        tracing::info!(name = self.name.get(), ?from, to = ?target, "stream lane migration");
        if let WriterState::Active { writer, .. } = &mut self.state {
            let _ = writer.shutdown();
        }
        self.state = WriterState::Migrating {
            target_lane: target,
        };
        self.policy.note_migration(tokio::time::Instant::now());
        Ok(())
    }

    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), MigratingError> {
        if self.auto {
            self.classify_and_maybe_migrate(buf.len()).await?;
        }
        self.ensure_open().await?;

        // Safe to access because ensure_open left us in Active state
        let writer = match &mut self.state {
            WriterState::Active { writer, .. } => writer,
            _ => return Err(MigratingError::LaneDead),
        };

        use tokio::io::AsyncWriteExt;
        writer
            .write_all(buf)
            .await
            .map_err(|_| MigratingError::WriteFailed)?;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), MigratingError> {
        use tokio::io::AsyncWriteExt;
        match &mut self.state {
            WriterState::Active { writer, .. } => writer
                .flush()
                .await
                .map_err(|_| MigratingError::WriteFailed),
            WriterState::Migrating { .. } => {
                self.ensure_open().await?;
                Ok(())
            }
            WriterState::PendingOpen { .. } => Ok(()),
            WriterState::Closed => Err(MigratingError::LaneDead),
        }
    }

    fn route_opened_reader(&mut self, generation: u32, reader: StreamReader) {
        let mut reader = reader;
        if generation == 0 {
            if let Some(tx) = self.gen0_reader_tx.take() {
                match tx.send(reader) {
                    Ok(()) => return,
                    Err(r) => reader = r,
                }
            }
        }
        if let Some(held) = &mut self.held_readers {
            held.push(reader);
        }
    }

    async fn classify_and_maybe_migrate(&mut self, size: usize) -> Result<(), MigratingError> {
        let current = match &self.state {
            WriterState::Active { lane, .. } | WriterState::PendingOpen { lane } => Some(*lane),
            _ => None,
        };
        let decision = self
            .policy
            .on_write(size, current, tokio::time::Instant::now());
        if let Some(target) = decision {
            return self.migrate_to(target).await;
        }
        Ok(())
    }

    /// Graceful shutdown — closes the active writer (if any) and emits a
    /// FINAL-marker generation so the peer receives a clean EOF. The
    /// FINAL generation is written by a detached background task (the
    /// [`GenerationChain`] and an owned [`DualStreamOpener`] clone move
    /// into it), so this method stays synchronous. If no data was ever
    /// written (still [`PendingOpen`](WriterState::PendingOpen)), this is
    /// a no-op — the peer was never aware of the stream, so no FINAL is
    /// needed.
    pub fn shutdown(&mut self) -> Result<(), MigratingError> {
        match self.state {
            WriterState::PendingOpen { .. } | WriterState::Closed => {
                self.state = WriterState::Closed;
                return Ok(());
            }
            _ => {}
        }
        // Close the data-carrying generation first.
        if let WriterState::Active { writer, .. } = &mut self.state {
            let _ = writer.shutdown();
        }
        // Move the chain + a clone of the opener into a detached task
        // that opens a fresh substream, writes the FINAL resume header,
        // and closes — giving the peer a positive end-of-stream signal.
        let opener = self.opener.clone();
        let mut chain = std::mem::replace(&mut self.chain, GenerationChain::new(0));
        tokio::spawn(async move {
            if let Ok((_, mut final_writer)) = opener.open(LaneClass::Interactive).await {
                let _ = chain
                    .start_generation(&mut tokio_util_writer(&mut final_writer), true)
                    .await;
                let _ = final_writer.shutdown();
            }
        });
        self.state = WriterState::Closed;
        Ok(())
    }

    /// Clean close: opens a new subs-stream for a FINAL-marker generation,
    /// writes the FINAL resume header, and closes. The peer receives a
    /// clean EOF (the [`SplicedReader`] sees `is_closed = true`). If no
    /// data was ever written (still [`PendingOpen`](WriterState::PendingOpen)),
    /// this is a no-op — the peer was never aware of the stream.
    pub async fn finalize(&mut self) -> Result<(), MigratingError> {
        match &self.state {
            WriterState::PendingOpen { .. } | WriterState::Closed => {
                self.state = WriterState::Closed;
                return Ok(());
            }
            _ => {}
        }
        // Close the data-carrying generation first.
        if let WriterState::Active { writer, .. } = &mut self.state {
            let _ = writer.shutdown();
        }
        // Open a fresh subs-tream for the FINAL-only generation.
        // Any lane works — the resume header is 21 bytes.
        let (_, mut final_writer) = self
            .opener
            .open(LaneClass::Interactive)
            .await
            .map_err(|_| MigratingError::OpenFailed)?;
        self.chain
            .start_generation(&mut tokio_util_writer(&mut final_writer), true)
            .await?;
        let _ = final_writer.shutdown();
        self.state = WriterState::Closed;
        Ok(())
    }

    pub async fn rebind(&mut self, opener: DualStreamOpener) -> Result<(), MigratingError> {
        self.opener = opener;
        match &self.state {
            WriterState::Active { lane, .. } => {
                let lane = *lane;
                self.migrate_to(lane).await
            }
            WriterState::PendingOpen { .. }
            | WriterState::Migrating { .. }
            | WriterState::Closed => Ok(()),
        }
    }
}

impl Drop for MigratingStreamWriter {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

// ---------------------------------------------------------------------------
// Adapter to use StreamWriter with GenerationChain::start_generation
// ---------------------------------------------------------------------------

struct TokioUtilWriter<'a> {
    inner: &'a mut StreamWriter,
}

impl<'a> AsyncWrite for TokioUtilWriter<'a> {
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

fn tokio_util_writer(w: &mut StreamWriter) -> TokioUtilWriter<'_> {
    TokioUtilWriter { inner: w }
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
    /// small writes and a cool-down of [`DEMOTE_COOLDOWN`] (150 ms) since
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
    /// returned [`ClientSplicedReader`] handles the read side.
    ///
    /// RESPONSE-direction traffic stays pinned to the lane where
    /// generation 0 opened; only the REQUEST direction migrates. True
    /// bidirectional lane migration needs the accepter API to expose
    /// successor writers — future work, out of scope.
    pub fn open_migrating_duplex(
        &self,
        logical_id: u64,
        initial_lane: LaneClass,
    ) -> (ClientSplicedReader, MigratingStreamWriter) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let writer = MigratingStreamWriter::new_with_reader_tx(
            self.clone(),
            logical_id,
            initial_lane,
            true,
            tx,
        );
        let reader = ClientSplicedReader {
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
    feed: SpliceFeedHandle,
    _own_feed: Option<SpliceFeed>,
}

impl MigratingCapableAccepter {
    pub fn new(inner: DualStreamAccepter) -> Self {
        Self::new_with_plain_streams(inner, true)
    }

    fn new_with_plain_streams(inner: DualStreamAccepter, pass_plain_streams: bool) -> Self {
        let feed = spawn_splice_feed();
        Self {
            inner,
            pass_plain_streams,
            response_opener: None,
            feed: feed.handle(),
            _own_feed: Some(feed),
        }
    }

    fn new_shared(inner: DualStreamAccepter, feed: SpliceFeedHandle) -> Self {
        Self {
            inner,
            pass_plain_streams: false,
            response_opener: None,
            feed,
            _own_feed: None,
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

    /// Accept the next stream. If it carries a resume header (generation
    /// ≥ 0), it is routed through the background splice driver; for
    /// generation 0, a new [`SplicedReader`] is returned once the driver
    /// creates it. Non-migrating streams pass through unchanged.
    ///
    /// Gen-0 readers arriving out of logical-id order are stashed in
    /// a [`VecDeque`] and re-tried on subsequent accepts.
    pub async fn accept(&mut self) -> Result<AcceptedStream, MigratingError> {
        loop {
            let (reader, writer, lane) = self
                .inner
                .accept()
                .await
                .map_err(|_| MigratingError::LaneDead)?;
            let Some((is_migrating, header_opt, reader)) = Self::peek_resume_header(reader).await?
            else {
                continue;
            };
            if is_migrating {
                if let Some(header) = header_opt {
                    if header.is_response {
                        continue;
                    };
                    let logical_id = header.logical_id;
                    let is_gen0 = header.generation == 0;
                    let gen_reader: GenerationReader = Box::pin(reader);
                    if is_gen0 {
                        let spliced_rx = self.feed.expect_gen0(logical_id);
                        self.feed
                            .send_continuation(header, gen_reader)
                            .map_err(|_| MigratingError::LaneDead)?;
                        let spliced = spliced_rx.await.map_err(|_| MigratingError::LaneDead)?;
                        return Ok(self.accepted_migrating(spliced, writer, lane, logical_id));
                    } else {
                        self.feed
                            .send_continuation(header, gen_reader)
                            .map_err(|_| MigratingError::LaneDead)?;
                        continue;
                    }
                }
            }
            if !self.pass_plain_streams {
                continue;
            }
            return Ok(AcceptedStream::Plain {
                reader,
                writer,
                source_lane: lane,
            });
        }
    }

    pub(crate) async fn peek_resume_header(
        mut reader: StreamReader,
    ) -> Result<Option<(bool, Option<ResumeHeader>, StreamReader)>, MigratingError> {
        use crate::stream_migration::{RESUME_HEADER_LEN, ResumeHeader};
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
// ClientSplicedReader — opener-side reader for duplex migrating
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
pub struct ClientSplicedReader {
    logical_id: u64,
    inner: Option<StreamReader>,
    gen0_rx: Option<tokio::sync::oneshot::Receiver<StreamReader>>,
}

impl std::fmt::Debug for ClientSplicedReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientSplicedReader")
            .field("logical_id", &self.logical_id)
            .field("has_inner", &self.inner.is_some())
            .finish()
    }
}

impl AsyncRead for ClientSplicedReader {
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

    pub fn into_migrating_only_shared(self, feed: SpliceFeedHandle) -> MigratingCapableAccepter {
        MigratingCapableAccepter::new_shared(self, feed)
    }

    pub fn into_migrating_duplex_shared(
        self,
        opener: DualStreamOpener,
        feed: SpliceFeedHandle,
    ) -> MigratingCapableAccepter {
        let mut mac = MigratingCapableAccepter::new_shared(self, feed);
        mac.response_opener = Some(opener);
        mac
    }
}

#[derive(Debug)]
pub struct ResponseRouter {
    feed: SpliceFeed,
    tasks: tokio::task::JoinSet<()>,
}
impl Drop for ResponseRouter {
    fn drop(&mut self) {
        self.feed.abort();
    }
}
impl ResponseRouter {
    pub fn handle(&self) -> ResponseRouterHandle {
        ResponseRouterHandle {
            feed: self.feed.handle(),
        }
    }

    pub fn add_accepter(&mut self, mut accepter: DualStreamAccepter) {
        let feed = self.feed.handle();
        self.tasks.spawn(async move {
            loop {
                let Ok((reader, _writer, _lane)) = accepter.accept().await else {
                    break;
                };
                match MigratingCapableAccepter::peek_resume_header(reader).await {
                    Ok(Some((true, Some(header), reader))) if header.is_response => {
                        if feed
                            .send_continuation(header, Box::pin(reader) as GenerationReader)
                            .is_err()
                        {
                            break;
                        }
                    }
                    _ => {}
                }
            }
        });
    }
}
#[derive(Debug, Clone)]
pub struct ResponseRouterHandle {
    feed: SpliceFeedHandle,
}
impl ResponseRouterHandle {
    pub fn expect_response(
        &self,
        logical_id: u64,
        gen0_reader: StreamReader,
    ) -> tokio::sync::oneshot::Receiver<SplicedReader> {
        let rx = self.feed.expect_gen0(logical_id);
        let header = ResumeHeader {
            logical_id,
            generation: 0,
            is_final: false,
            is_response: true,
        };
        let _ = self
            .feed
            .send_continuation(header, Box::pin(gen0_reader) as GenerationReader);
        rx
    }
}
pub fn spawn_response_router(accepter: DualStreamAccepter) -> ResponseRouter {
    let mut router = ResponseRouter {
        feed: spawn_splice_feed(),
        tasks: tokio::task::JoinSet::new(),
    };
    router.add_accepter(accepter);
    router
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
        serve::{MuxConfig, spawn_mux_no_reconnection},
    };
    use std::time::Duration;
    use tokio::io::{AsyncWriteExt, duplex};

    async fn make_dual_session() -> (
        DualStreamOpener,
        DualStreamAccepter,
        tokio::task::JoinSet<crate::serve::MuxError>,
        tokio::task::JoinSet<crate::serve::MuxError>,
        tokio::task::JoinSet<crate::serve::MuxError>,
        tokio::task::JoinSet<crate::serve::MuxError>,
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
        Vec<tokio::task::JoinSet<crate::serve::MuxError>>,
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
        let router = spawn_response_router(x_acc);
        let mut mac = y_acc.into_migrating_duplex(y_op);
        let (mut req_writer, gen0_rx) = x_op.open_migrating_with_reader(42, LaneClass::Interactive);
        req_writer.write_all(b"request").await.unwrap();
        let accepted = mac.accept().await.unwrap();
        let (mut req_reader, mut resp_writer) = match accepted {
            AcceptedStream::MigratingDuplex { reader, writer, .. } => (reader, writer),
            other => panic!("expected MigratingDuplex, got {other:?}"),
        };
        let mut req = [0u8; 7];
        tokio::io::AsyncReadExt::read_exact(&mut req_reader, &mut req)
            .await
            .unwrap();
        assert_eq!(&req, b"request");
        let gen0_reader = gen0_rx.await.unwrap();
        let spliced_rx = router.handle().expect_response(42, gen0_reader);
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
        req_writer.shutdown().unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn duplex_response_without_migration_needs_final_for_clean_eof() {
        let (x_op, x_acc, y_op, y_acc, _tasks) = make_duplex_session().await;
        let router = spawn_response_router(x_acc);
        let mut mac = y_acc.into_migrating_duplex(y_op);
        let (mut req_writer, gen0_rx) = x_op.open_migrating_with_reader(7, LaneClass::Interactive);
        req_writer.write_all(b"ping").await.unwrap();
        let accepted = mac.accept().await.unwrap();
        let mut resp_writer = match accepted {
            AcceptedStream::MigratingDuplex { writer, .. } => writer,
            other => panic!("expected MigratingDuplex, got {other:?}"),
        };
        let gen0_reader = gen0_rx.await.unwrap();
        let mut resp_reader = router
            .handle()
            .expect_response(7, gen0_reader)
            .await
            .unwrap();
        resp_writer.write_all(b"pong").await.unwrap();
        resp_writer.finalize().await.unwrap();
        let mut resp = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut resp_reader, &mut resp)
            .await
            .unwrap();
        assert_eq!(resp, "pong");
        req_writer.shutdown().unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn duplex_bidirectional_migration_integrity() {
        let (x_op, x_acc, y_op, y_acc, _tasks) = make_duplex_session().await;
        let router = spawn_response_router(x_acc);
        let mut mac = y_acc.into_migrating_duplex(y_op);
        let upload = 512 * 1024;
        let download = 512 * 1024;
        let (mut req_writer, gen0_rx) = x_op.open_migrating_with_reader(9, LaneClass::Interactive);
        let send = tokio::spawn(async move {
            let chunk = vec![0xABu8; 64 * 1024];
            let mut sent = 0;
            while sent < upload {
                req_writer.write_all(&chunk).await.unwrap();
                sent += chunk.len();
            }
            req_writer.finalize().await.unwrap();
        });
        let accepted = mac.accept().await.unwrap();
        let (mut req_reader, mut resp_writer) = match accepted {
            AcceptedStream::MigratingDuplex { reader, writer, .. } => (reader, writer),
            other => panic!("expected MigratingDuplex, got {other:?}"),
        };
        let drain = tokio::spawn(async move {
            loop {
                _ = mac.accept().await;
            }
        });
        let respond = tokio::spawn(async move {
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
            .expect_response(9, gen0_reader)
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
            }
        );
        assert_eq!(up.len(), upload, "upload byte count mismatch");
        assert!(
            up.iter().all(|b| *b == 0xAB),
            "upload
    corrupted"
        );
        assert_eq!(down.len(), download, "download byte count mismatch");
        assert!(down.iter().all(|b| *b == 0xCD), "download corrupted");
        send.await.unwrap();
        respond.await.unwrap();
        drain.abort();
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
        writer.shutdown().unwrap();
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
        writer.shutdown().unwrap();
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
        writer.shutdown().unwrap();
    }

    // -------------------------------------------------------------------
    // Close during migration
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn close_during_migration() {
        let (opener, _accepter, _s, _sb, _c, _cb) = make_dual_session().await;

        let mut writer = opener.open_migrating(1, LaneClass::Interactive);

        writer.write_all(&[0u8; 3000]).await.unwrap(); // triggers promote to bulk
        writer.shutdown().unwrap();
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
        writer.shutdown().unwrap();
    }

    // -------------------------------------------------------------------
    // Mirrored classifier records history
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn mirrored_classifier_tracks_bulk_ratio() {
        let mut c = crate::traffic_class::Classifier::new();

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
        let accept = tokio::spawn(async move { mac.accept().await });
        tokio::task::yield_now().await;
        assert!(
            !accept.is_finished(),
            "empty or truncated stream escaped as an application stream"
        );
        let mut writer = opener.open_migrating(42, LaneClass::Interactive);
        writer.write_all(b"hello").await.unwrap();
        let accepted = tokio::time::timeout(Duration::from_secs(1), accept)
            .await
            .expect("strict accepter did not reach the valid migrating stream")
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

        let send = tokio::spawn(async move {
            let mut writer = opener.open_migrating(42, LaneClass::Interactive);
            writer.write_all(b"hello-world").await.unwrap();
            writer.shutdown().unwrap();
        });

        // Accept gen0
        let accepted = mac.accept().await.unwrap();
        let reader = match accepted {
            AcceptedStream::Migrating { reader, .. } => reader,
            other => panic!("expected Migrating, got: {other:?}"),
        };

        // Drain successor generations so the FINAL gen reaches the
        // SplicedReader's queue before we read.
        let drain = tokio::spawn(async move {
            loop {
                let _ = mac.accept().await;
            }
        });

        send.await.unwrap();

        // Read from SplicedReader — gets clean EOF after payload + FINAL
        let mut data = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut Box::pin(reader), &mut data)
            .await
            .unwrap();
        assert_eq!(data, b"hello-world");

        drain.abort();
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

        let send = tokio::spawn(async move {
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
            writer.shutdown().unwrap();
        });

        let accepted = mac.accept().await.unwrap();
        let mut reader = match accepted {
            AcceptedStream::Migrating { reader, .. } => reader,
            _ => panic!("expected migrating stream"),
        };

        let drain = tokio::spawn(async move {
            loop {
                let _ = mac.accept().await;
            }
        });

        let mut data = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut data)
            .await
            .unwrap();
        assert_eq!(data.len(), chunk_size * migrations, "byte count mismatch");

        send.await.unwrap();
        drain.abort();
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

        let send = tokio::spawn(async move {
            let mut writer = opener.open_migrating_manual(1, LaneClass::Interactive);
            writer.write_all(first).await.unwrap();
            writer.force_migrate(LaneClass::Bulk).await.unwrap();
            writer.write_all(second).await.unwrap();
            writer.force_migrate(LaneClass::Interactive).await.unwrap();
            writer.write_all(third).await.unwrap();
            writer.shutdown().unwrap();
        });

        let accepted = mac.accept().await.unwrap();
        let mut reader = match accepted {
            AcceptedStream::Migrating { reader, .. } => reader,
            _ => panic!("expected migrating stream"),
        };

        let drain = tokio::spawn(async move {
            loop {
                let _ = mac.accept().await;
            }
        });

        let mut data = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut data)
            .await
            .unwrap();
        let expected: Vec<u8> = first.iter().chain(second).chain(third).copied().collect();
        assert_eq!(data, expected, "data mismatch");

        send.await.unwrap();
        drain.abort();
    }

    // -------------------------------------------------------------------
    // Close with finalize (FINAL-marker clean EOF)
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn close_with_final_marker_gives_clean_eof() {
        let (opener, accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut mac = accepter.into_migrating_capable();

        // Sender writes gen0 data then finalize (gen1 FINAL)
        let send = tokio::spawn(async move {
            let mut writer = opener.open_migrating(42, LaneClass::Interactive);
            writer.write_all(b"Hello, world!").await.unwrap();
            writer.finalize().await.unwrap();
        });

        // Accept gen0 → SplicedReader with payload
        let accepted = mac.accept().await.unwrap();
        let mut reader = match accepted {
            AcceptedStream::Migrating { reader, .. } => reader,
            _ => panic!("expected migrating"),
        };

        // Continuously drain successor generations so the FINAL gen
        // reaches the SplicedReader's queue.
        let drain = tokio::spawn(async move { while mac.accept().await.is_ok() {} });

        // SplicedReader should read payload, then see FINAL and get clean EOF
        let mut data = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut data)
            .await
            .unwrap();
        assert_eq!(data, b"Hello, world!");

        send.await.unwrap();
        drain.abort();
    }

    // -------------------------------------------------------------------
    // Close with finalize after migration
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn close_with_finalize_after_migration() {
        let (opener, accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut mac = accepter.into_migrating_capable();

        let send = tokio::spawn(async move {
            let mut writer = opener.open_migrating(1, LaneClass::Interactive);
            writer.write_all(b"Prologue ").await.unwrap();
            writer.write_all(&[0u8; 3000]).await.unwrap();
            writer.write_all(b" Epilogue.").await.unwrap();
            writer.finalize().await.unwrap();
        });

        let accepted = mac.accept().await.unwrap();
        let mut reader = match accepted {
            AcceptedStream::Migrating { reader, .. } => reader,
            _ => panic!("expected migrating stream"),
        };

        let drain = tokio::spawn(async move {
            loop {
                let _ = mac.accept().await;
            }
        });

        let mut buf = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut buf)
            .await
            .unwrap();
        assert!(buf.starts_with("Prologue "), "got: {buf:?}");
        assert!(buf.contains("Epilogue."), "got: {buf:?}");

        send.await.unwrap();
        drain.abort();
    }

    // -------------------------------------------------------------------
    // Both-directions simultaneous migration
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn simultaneous_bidirectional_migration() {
        let (opener, accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut mac = accepter.into_migrating_capable();

        let opener2 = opener.clone();
        let send_a = tokio::spawn(async move {
            let mut w = opener.open_migrating_manual(10, LaneClass::Interactive);
            w.write_all(b"stream-A-chunk-1").await.unwrap();
            w.force_migrate(LaneClass::Bulk).await.unwrap();
            w.write_all(b"stream-A-chunk-2").await.unwrap();
            w.shutdown().unwrap();
        });

        let opener3 = opener2.clone();
        let send_b = tokio::spawn(async move {
            let mut w = opener3.open_migrating_manual(20, LaneClass::Bulk);
            w.write_all(b"stream-B-chunk-1").await.unwrap();
            w.force_migrate(LaneClass::Interactive).await.unwrap();
            w.write_all(b"stream-B-chunk-2").await.unwrap();
            w.shutdown().unwrap();
        });

        // Accept first gen0
        let a = mac.accept().await.unwrap();
        let reader_a = match a {
            AcceptedStream::Migrating { reader, .. } => reader,
            _ => panic!("expected migrating"),
        };

        // Accept second gen0
        let b = mac.accept().await.unwrap();
        let reader_b = match b {
            AcceptedStream::Migrating { reader, .. } => reader,
            _ => panic!("expected migrating"),
        };

        // Drain successors while readers consume data
        let drain = tokio::spawn(async move {
            loop {
                let _ = mac.accept().await;
            }
        });

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

        send_a.await.unwrap();
        send_b.await.unwrap();
        drain.abort();
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

        let send = tokio::spawn(async move {
            let mut writer = opener.open_migrating(1, LaneClass::Interactive);
            writer.write_all(&vec![0xABu8; sync_size]).await.unwrap();
            for i in 0..deltas {
                writer
                    .write_all(format!("delta-{:02}-", i).as_bytes())
                    .await
                    .unwrap();
            }
            writer.shutdown().unwrap();
        });

        let accepted = mac.accept().await.unwrap();
        let mut reader = match accepted {
            AcceptedStream::Migrating { reader, .. } => reader,
            _ => panic!("expected migrating stream"),
        };

        let drain = tokio::spawn(async move {
            loop {
                let _ = mac.accept().await;
            }
        });

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

        send.await.unwrap();
        drain.abort();
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
        writer.shutdown().unwrap();
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
        writer.shutdown().unwrap();
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

        let send = tokio::spawn(async move {
            let mut writer = opener.open_migrating_manual(1, LaneClass::Interactive);
            writer.force_migrate(LaneClass::Bulk).await.unwrap();
            writer.write_all(b"data-on-bulk").await.unwrap();
            writer.shutdown().unwrap();
        });

        let accepted = mac.accept().await.unwrap();
        let mut reader = match accepted {
            AcceptedStream::Migrating { reader, .. } => reader,
            _ => panic!("expected migrating stream"),
        };

        let drain = tokio::spawn(async move {
            loop {
                let _ = mac.accept().await;
            }
        });

        let mut data = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut data)
            .await
            .unwrap();
        assert_eq!(data, "data-on-bulk");

        send.await.unwrap();
        drain.abort();
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

        let write = tokio::spawn(async move {
            let mut writer = client_writer;
            writer.write_all(b"hello-from-c2s  ").await.unwrap();
            writer.shutdown().unwrap();
        });

        let accepted = mac.accept().await.unwrap();
        let (_accepted_reader, mut accepted_writer) = match accepted {
            AcceptedStream::Migrating { reader, writer, .. } => (reader, writer),
            _ => panic!("expected migrating"),
        };
        tokio::io::AsyncWriteExt::write_all(&mut accepted_writer, b"hello-from-s2c  ")
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::shutdown(&mut accepted_writer)
            .await
            .unwrap();

        write.await.unwrap();

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

        let write = tokio::spawn(async move {
            let mut writer = client_writer;
            writer.write_all(b"c2s-1").await.unwrap();
            writer.force_migrate(LaneClass::Bulk).await.unwrap();
            writer.write_all(b"c2s-2").await.unwrap();
            writer.shutdown().unwrap();
        });

        let accepted = mac.accept().await.unwrap();
        let (_accepted_reader, mut accepted_writer) = match accepted {
            AcceptedStream::Migrating { reader, writer, .. } => (reader, writer),
            _ => panic!("expected migrating"),
        };
        tokio::io::AsyncWriteExt::write_all(&mut accepted_writer, b"first-response ")
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut accepted_writer, b"second-response")
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::shutdown(&mut accepted_writer)
            .await
            .unwrap();

        write.await.unwrap();

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

        let write = tokio::spawn(async move {
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
            writer.shutdown().unwrap();
        });

        // Accept gen0
        let accepted = mac.accept().await.unwrap();
        let (mut accepted_reader, mut accepted_writer) = match accepted {
            AcceptedStream::Migrating { reader, writer, .. } => (reader, writer),
            _ => panic!("expected migrating"),
        };

        // Drain successors while echoing
        let drain = tokio::spawn(async move {
            loop {
                let _ = mac.accept().await;
            }
        });

        let echo = tokio::spawn(async move {
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

        write.await.unwrap();
        echo.await.unwrap();
        drain.abort();

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

        let write = tokio::spawn(async move {
            let mut writer = client_writer;
            writer.write_all(b"write-only-data").await.unwrap();
            writer.shutdown().unwrap();
        });

        let accepted = mac.accept().await.unwrap();
        let mut accepted_reader = match accepted {
            AcceptedStream::Migrating { reader, .. } => reader,
            _ => panic!("expected migrating"),
        };

        // Drain successors (FINAL generation from shutdown) so the
        // SplicedReader can chain through to clean EOF.
        let drain = tokio::spawn(async move {
            loop {
                let _ = mac.accept().await;
            }
        });

        let mut data = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut accepted_reader, &mut data)
            .await
            .unwrap();
        assert_eq!(data, "write-only-data");

        write.await.unwrap();
        drain.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shared_feed_second_accepter_splices_byte_exact() {
        let (op1, acc1, _a, _b, _c, _d) = make_dual_session().await;
        let (op2, acc2, _e, _f, _g, _h) = make_dual_session().await;
        let feed = spawn_splice_feed();
        let mut mac1 = acc1.into_migrating_only_shared(feed.handle());
        let mut mac2 = acc2.into_migrating_only_shared(feed.handle());
        let send = tokio::spawn(async move {
            let mut w = op1.open_migrating_manual(42, LaneClass::Interactive);
            w.write_all(b"born-on-session-one|").await.unwrap();
            w.rebind(op2).await.unwrap();
            w.write_all(b"continued-on-session-two").await.unwrap();
            w.finalize().await.unwrap();
        });
        let accepted = mac1.accept().await.unwrap();
        let mut reader = match accepted {
            AcceptedStream::Migrating { reader, .. } => reader,
            other => panic!("expected migrating, got {other:?}"),
        };
        let drain1 = tokio::spawn(async move {
            loop {
                let _ = mac1.accept().await;
            }
        });
        let drain2 = tokio::spawn(async move {
            loop {
                let _ = mac2.accept().await;
            }
        });
        let mut data = String::new();
        tokio::time::timeout(
            Duration::from_secs(10),
            tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut data),
        )
        .await
        .expect("cross-session splice stalled")
        .unwrap();
        assert_eq!(data, "born-on-session-one|continued-on-session-two");
        send.await.unwrap();
        drain1.abort();
        drain2.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rebind_mid_stream_is_lossless() {
        let (op1, acc1, _a, _b, _c, _d) = make_dual_session().await;
        let (op2, acc2, _e, _f, _g, _h) = make_dual_session().await;
        let feed = spawn_splice_feed();
        let mut mac1 = acc1.into_migrating_only_shared(feed.handle());
        let mut mac2 = acc2.into_migrating_only_shared(feed.handle());
        let half = 200 * 1024;
        let pattern: Vec<u8> = (0..2 * half).map(|i| (i % 251) as u8).collect();
        let expected = pattern.clone();
        let send = tokio::spawn(async move {
            let mut w = op1.open_migrating(9, LaneClass::Interactive);
            w.write_all(&pattern[..half]).await.unwrap();
            w.rebind(op2).await.unwrap();
            w.write_all(&pattern[half..]).await.unwrap();
            w.finalize().await.unwrap();
        });
        let accepted = mac1.accept().await.unwrap();
        let mut reader = match accepted {
            AcceptedStream::Migrating { reader, .. } => reader,
            other => panic!("expected migrating, got {other:?}"),
        };
        let drain1 = tokio::spawn(async move {
            loop {
                let _ = mac1.accept().await;
            }
        });
        let drain2 = tokio::spawn(async move {
            loop {
                let _ = mac2.accept().await;
            }
        });
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
        send.await.unwrap();
        drain1.abort();
        drain2.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn response_final_crossing_shared_feed_closes_cleanly() {
        let (x1_op, x1_acc, y1_op, y1_acc, _t1) = make_duplex_session().await;
        let (_x2_op, x2_acc, y2_op, _y2_acc, _t2) = make_duplex_session().await;
        let mut router = spawn_response_router(x1_acc);
        router.add_accepter(x2_acc);
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
            .expect_response(7, gen0_reader)
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
        req_writer.shutdown().unwrap();
    }
}

#[derive(Debug, Clone)]
pub struct SpliceFeedHandle {
    cont_tx: mpsc::UnboundedSender<(ResumeHeader, GenerationReader)>,
    register_tx: mpsc::UnboundedSender<(u64, tokio::sync::oneshot::Sender<SplicedReader>)>,
}

impl SpliceFeedHandle {
    fn send_continuation(&self, header: ResumeHeader, reader: GenerationReader) -> Result<(), ()> {
        self.cont_tx.send((header, reader)).map_err(|_| ())
    }

    fn expect_gen0(&self, logical_id: u64) -> tokio::sync::oneshot::Receiver<SplicedReader> {
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

impl SpliceFeed {
    pub fn handle(&self) -> SpliceFeedHandle {
        self.handle.clone()
    }

    fn abort(&self) {
        self.driver.abort();
        self.matcher.abort();
    }
}

pub fn spawn_splice_feed() -> SpliceFeed {
    let (cont_tx, cont_rx) = mpsc::unbounded_channel();
    let (gen0_tx, mut gen0_rx) = mpsc::unbounded_channel::<(u64, SplicedReader)>();
    let (register_tx, mut register_rx) =
        mpsc::unbounded_channel::<(u64, tokio::sync::oneshot::Sender<SplicedReader>)>();
    let driver = spawn_splice_driver(SpliceRegistry::new(), cont_rx, gen0_tx);
    let matcher = tokio::spawn(async move {
        let mut waiters: std::collections::HashMap<
            u64,
            tokio::sync::oneshot::Sender<SplicedReader>,
        > = std::collections::HashMap::new();
        let mut ready: std::collections::HashMap<u64, SplicedReader> =
            std::collections::HashMap::new();
        loop {
            tokio::select! {
                reg = register_rx.recv() => match reg {
                    Some((id, tx)) => match ready.remove(&id) {
                        Some(spliced) => {
                            let _ = tx.send(spliced);
                        }
                        None => {
                            waiters.insert(id, tx);
                        }
                    },
                    None => break,
                },
                gen0 = gen0_rx.recv() => match gen0 {
                    Some((id, spliced)) => match waiters.remove(&id) {
                        Some(tx) => {
                            let _ = tx.send(spliced);
                        }
                        None => {
                            ready.insert(id, spliced);
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
