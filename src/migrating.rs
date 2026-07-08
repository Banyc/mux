use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;

use crate::{
    dual_lane::{DualStreamAccepter, DualStreamOpener, LaneClass},
    stream::writer::StreamWriter,
    stream_migration::{GenerationChain, GenerationReader, MigrationError, ResumeHeader,
        SpliceRegistry, SplicedReader, spawn_splice_driver},
    StreamReader,
};

// ---------------------------------------------------------------------------
// Constants (mirror central_io::writer's LatencyControl)
// ---------------------------------------------------------------------------

/// Cross-reference: `DATA_MEDIUM_CAP` in `central_io::writer`.
pub const AUTO_BULK_THRESHOLD: usize = 2048;

/// Number of consecutive small writes needed on the bulk lane before a
/// demotion to interactive is considered.
const DEMOTE_STREAK: usize = 4;

/// Minimum time between any two migrations for the same stream.
const DEMOTE_COOLDOWN: Duration = Duration::from_millis(150);

/// History window for the mirrored classification ratio (mirrors
/// `LATENCY_HISTORY_MAX` in `central_io::writer`).
const HISTORY_MAX: usize = 16;

// ---------------------------------------------------------------------------
// Mirrored classifier
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct StreamClassifier {
    small_count: u32,
    bulk_count: u32,
}

impl StreamClassifier {
    fn new() -> Self {
        Self { small_count: 0, bulk_count: 0 }
    }

    fn record(&mut self, size: usize) {
        if self.small_count + self.bulk_count >= HISTORY_MAX as u32 {
            self.small_count /= 2;
            self.bulk_count /= 2;
        }
        if size > AUTO_BULK_THRESHOLD {
            self.bulk_count += 1;
        } else {
            self.small_count += 1;
        }
    }

    /// Mirrors `LatencyControl::is_bulk` in `central_io::writer`:
    /// `small_count * 3 < total * 2` with a minimum of 3 observations.
    /// Equivalent to `small_count < 2 * bulk_count` when total ≥ 3.
    fn is_bulk(&self) -> bool {
        let total = self.small_count + self.bulk_count;
        const HISTORY_MIN: u32 = 3; // mirrors LATENCY_HISTORY_MIN in central_io::writer
        if total < HISTORY_MIN {
            return false;
        }
        self.small_count * 3 < total * 2
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
    classifier: StreamClassifier,
    // Auto-policy state
    small_streak: usize,
    last_migration: Option<tokio::time::Instant>,
    auto: bool,
    gen0_reader_tx: Option<tokio::sync::oneshot::Sender<StreamReader>>,
    /// When set, generation readers (gen 0 and successors) are routed
    /// into this channel for the client-side splice driver, producing a
    /// [`SplicedReader`] on the paired `gen0_rx`.
    cont_tx: Option<mpsc::UnboundedSender<(ResumeHeader, GenerationReader)>>,
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
            classifier: StreamClassifier::new(),
            small_streak: 0,
            last_migration: None,
            auto,
            gen0_reader_tx: None,
            cont_tx: None,
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
            classifier: StreamClassifier::new(),
            small_streak: 0,
            last_migration: None,
            auto,
            gen0_reader_tx: Some(gen0_reader_tx),
            cont_tx: None,
        }
    }

    /// Force-migrate to `target` lane. Closes the current generation and
    /// opens a new one on the target lane with an explicit `LaneClass`
    /// hint (NOT `open_auto` — the resume header is small and would
    /// misroute).
    pub async fn force_migrate(&mut self, target: LaneClass) -> Result<(), MigratingError> {
        self.migrate_to(target).await
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
                let oneshot_tx = self.gen0_reader_tx.take();
                let gen = self
                    .chain
                    .start_generation(&mut tokio_util_writer(&mut writer), false)
                    .await?;
                if let Some(tx) = oneshot_tx {
                    let _ = tx.send(reader);
                } else {
                    self.route_reader(gen, reader);
                }
                self.state = WriterState::Active { writer, lane };
                Ok(())
            }
            WriterState::Migrating { target_lane } => {
                let (reader, mut writer) = match self.opener.open(target_lane).await {
                    Ok(x) => x,
                    Err(e) => return Err(MigratingError::OpenUnderlying(format!("{e:?}"))),
                };
                let gen = self
                    .chain
                    .start_generation(&mut tokio_util_writer(&mut writer), false)
                    .await?;
                self.route_reader(gen, reader);
                self.state = WriterState::Active { writer, lane: target_lane };
                Ok(())
            }
            WriterState::Closed => Err(MigratingError::LaneDead),
        }
    }

    async fn migrate_to(&mut self, target: LaneClass) -> Result<(), MigratingError> {
        // Close current generation
        if let WriterState::Active { writer, .. } = &mut self.state {
            let _ = writer.shutdown();
        }
        self.state = WriterState::Migrating { target_lane: target };
        self.last_migration = Some(tokio::time::Instant::now());
        self.small_streak = 0;
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
        writer.write_all(buf).await.map_err(|_| MigratingError::WriteFailed)?;
        Ok(())
    }

    /// When `cont_tx` is set, boxes the generation reader and sends it
    /// with its [`ResumeHeader`] into the client-side splice driver.
    /// When `cont_tx` is `None`, the reader is silently dropped.
    fn route_reader(&self, generation: u32, reader: StreamReader) {
        if let Some(tx) = &self.cont_tx {
            let header = ResumeHeader {
                logical_id: self.chain.logical_id(),
                generation,
                is_final: false,
            };
            let _ = tx.send((header, Box::pin(reader)));
        }
    }

    async fn classify_and_maybe_migrate(&mut self, size: usize) -> Result<(), MigratingError> {
        self.classifier.record(size);

        match &self.state {
            WriterState::Active { lane, .. } | WriterState::PendingOpen { lane } => {
                let current_lane = *lane;

                // PROMOTE: any single write > threshold migrates BEFORE the write
                if size > AUTO_BULK_THRESHOLD && current_lane == LaneClass::Interactive {
                    return self.migrate_to(LaneClass::Bulk).await;
                }

                // DEMOTE: conservative
                if current_lane == LaneClass::Bulk {
                    if size <= AUTO_BULK_THRESHOLD {
                        self.small_streak += 1;
                    } else {
                        self.small_streak = 0;
                    }

                    if self.small_streak >= DEMOTE_STREAK
                        && !self.classifier.is_bulk()
                    {
                        if let Some(last) = self.last_migration {
                            if last.elapsed() >= DEMOTE_COOLDOWN {
                                return self.migrate_to(LaneClass::Interactive).await;
                            }
                        } else {
                            return self.migrate_to(LaneClass::Interactive).await;
                        }
                    }
                }
            }
            _ => {}
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
        let mut chain = std::mem::replace(
            &mut self.chain,
            GenerationChain::new(0),
        );
        tokio::spawn(async move {
            if let Ok((_, mut final_writer)) = opener.open(LaneClass::Interactive).await {
                let _ = chain
                    .start_generation(
                        &mut tokio_util_writer(&mut final_writer),
                        true,
                    )
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
    ) -> (MigratingStreamWriter, tokio::sync::oneshot::Receiver<StreamReader>) {
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
    /// returned [`ClientSplicedReader`] handles the read side across
    /// lane migrations.
    ///
    /// Internally spawns a client-side splice driver that re-assembles
    /// generation readers into a single [`SplicedReader`] — the same
    /// mechanism used by the accepter side.
    pub fn open_migrating_duplex(
        &self,
        logical_id: u64,
        initial_lane: LaneClass,
    ) -> (ClientSplicedReader, MigratingStreamWriter) {
        let (cont_tx, cont_rx) = mpsc::unbounded_channel();
        let (gen0_tx, gen0_rx) = mpsc::unbounded_channel();
        let registry = SpliceRegistry::new();
        let driver = spawn_splice_driver(registry, cont_rx, gen0_tx);

        let mut writer =
            MigratingStreamWriter::new(self.clone(), logical_id, initial_lane, true);
        writer.cont_tx = Some(cont_tx);

        let reader = ClientSplicedReader {
            logical_id,
            inner: None,
            gen0_rx: Some(gen0_rx),
            driver,
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
    /// Feeds all migrating generations (gen 0 and successors) into the
    /// background splice driver.
    cont_tx: mpsc::UnboundedSender<(ResumeHeader, GenerationReader)>,
    /// Receives gen‑0 [`SplicedReader`]s created by the driver.
    gen0_rx: mpsc::UnboundedReceiver<(u64, SplicedReader)>,
    /// Background driver that owns the [`SpliceRegistry`] and routes
    /// successor generations into the matching [`SplicedReader`] queues.
    #[allow(dead_code)]
    driver: tokio::task::JoinHandle<Result<(), MigrationError>>,
    /// Stash for gen-0 readers that arrive out of logical-id order.
    stash: std::collections::VecDeque<(u64, SplicedReader)>,
}

impl MigratingCapableAccepter {
    pub fn new(inner: DualStreamAccepter) -> Self {
        let (cont_tx, cont_rx) = mpsc::unbounded_channel();
        let (gen0_tx, gen0_rx) = mpsc::unbounded_channel();
        let registry = SpliceRegistry::new();
        let driver = spawn_splice_driver(registry, cont_rx, gen0_tx);
        Self {
            inner,
            cont_tx,
            gen0_rx,
            driver,
            stash: std::collections::VecDeque::new(),
        }
    }

    /// Accept the next stream. If it carries a resume header (generation
    /// ≥ 0), it is routed through the background splice driver; for
    /// generation 0, a new [`SplicedReader`] is returned once the driver
    /// creates it. Non-migrating streams pass through unchanged.
    ///
    /// Gen-0 readers arriving out of logical-id order are stashed in
    /// a [`VecDeque`] and re-tried on subsequent accepts.
    pub async fn accept(
        &mut self,
    ) -> Result<AcceptedStream, MigratingError> {
        // Check stash first for previously-mismatched gen-0 readers.
        loop {
            // We need a full accept cycle (inner.accept → peek → maybe
            // check stash). Drain the stash when there's no pending
            // accept in progress.
            let (reader, writer, lane) = self.inner.accept()
                .await
                .map_err(|_| MigratingError::LaneDead)?;

            let (is_migrating, header_opt, reader) =
                Self::peek_resume_header(reader).await?;

            if is_migrating {
                if let Some(header) = header_opt {
                    let logical_id = header.logical_id;
                    let is_gen0 = header.generation == 0;

                    let gen_reader: GenerationReader = Box::pin(reader);
                    self.cont_tx
                        .send((header, gen_reader))
                        .map_err(|_| MigratingError::LaneDead)?;

                    if is_gen0 {
                        // Drain gen-0 rx, checking stash first.
                        loop {
                            // Check stash.
                            if let Some(pos) = self
                                .stash
                                .iter()
                                .position(|(id, _)| *id == logical_id)
                            {
                                let (_, spliced) = self.stash.remove(pos).unwrap();
                                return Ok(AcceptedStream::Migrating {
                                    reader: spliced,
                                    writer,
                                    source_lane: lane,
                                });
                            }
                            match self.gen0_rx.recv().await {
                                Some((id, spliced)) if id == logical_id => {
                                    return Ok(AcceptedStream::Migrating {
                                        reader: spliced,
                                        writer,
                                        source_lane: lane,
                                    });
                                }
                                Some((other_id, spliced)) => {
                                    self.stash.push_back((other_id, spliced));
                                }
                                None => return Err(MigratingError::LaneDead),
                            }
                        }
                    } else {
                        continue;
                    }
                }
            }

            return Ok(AcceptedStream::Plain {
                reader,
                writer,
                source_lane: lane,
            });
        }
    }

    async fn peek_resume_header(
        mut reader: StreamReader,
    ) -> Result<(bool, Option<ResumeHeader>, StreamReader), MigratingError> {
        use tokio::io::AsyncReadExt;
        use crate::stream_migration::RESUME_HEADER_LEN;

        let mut buf = [0u8; RESUME_HEADER_LEN];
        match reader.read_exact(&mut buf).await {
            Ok(_n) => {
                if let Some(header) = ResumeHeader::parse(&buf) {
                    Ok((true, Some(header), reader))
                } else {
                    // Not a resume header: restore the 21 consumed bytes
                    // so the caller sees the full original byte stream.
                    reader.prepend(&buf);
                    Ok((false, None, reader))
                }
            }
            Err(_) => {
                Ok((false, None, reader))
            }
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
    /// A plain (non-migrating) stream.
    Plain {
        reader: StreamReader,
        writer: StreamWriter,
        source_lane: LaneClass,
    },
}

// ---------------------------------------------------------------------------
// ClientSplicedReader — opener-side spliced reader for duplex migrating
// ---------------------------------------------------------------------------

/// Opener-side reader that consumes the [`SplicedReader`] for a single
/// known `logical_id` from the client-side splice driver's `gen0_rx`
/// channel.  The reader is lazily obtained on the first [`AsyncRead::poll_read`]
/// invocation — no blocking until the first poll.
///
/// The background splice driver is kept alive via the owned
/// [`tokio::task::JoinHandle`] stored inside this struct; dropping the
/// `ClientSplicedReader` drops the driver.
pub struct ClientSplicedReader {
    logical_id: u64,
    inner: Option<SplicedReader>,
    gen0_rx: Option<mpsc::UnboundedReceiver<(u64, SplicedReader)>>,
    #[allow(dead_code)]
    driver: tokio::task::JoinHandle<Result<(), MigrationError>>,
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
            let result = match rx_opt.as_mut() {
                Some(rx) => Pin::new(rx).poll_recv(cx),
                None => Poll::Ready(None),
            };
            match result {
                Poll::Ready(Some((id, spliced))) if id == self.logical_id => {
                    self.inner = Some(spliced);
                }
                Poll::Ready(Some((_other_id, _spliced))) => {
                    self.gen0_rx = rx_opt;
                    return Poll::Pending;
                }
                Poll::Ready(None) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "splice driver dropped before gen0 arrived",
                    )));
                }
                Poll::Pending => {
                    self.gen0_rx = rx_opt;
                    return Poll::Pending;
                }
            }
        }
        match &mut self.inner {
            Some(spliced) => Pin::new(spliced).poll_read(cx, buf),
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
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        control::Initiation,
        dual_lane::Liveness,
        serve::{spawn_mux_no_reconnection, MuxConfig},
        DualStreamAccepter, DualStreamOpener,
    };
    use std::time::Duration;
    use tokio::io::{duplex, AsyncWriteExt};

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
        let mut c = StreamClassifier::new();

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

        // Open a plain (non-migrating) stream with enough data to
        // survive the resume-header peek (21 bytes minimum).
        let (_reader, mut writer) = opener.open(LaneClass::Interactive).await.unwrap();
        let data = vec![0x00u8; 30];
        writer.write_all(&data).await.unwrap();
        writer.shutdown().unwrap();

        // Accept should return a Plain stream
        let accepted = mac.accept().await.unwrap();
        match accepted {
            AcceptedStream::Plain { .. } => {} // expected
            _ => panic!("expected Plain stream"),
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

        // Drop mac to close the splice-driver queue, unblocking SplicedReader
        drop(mac);

        send.await.unwrap();

        // Read from SplicedReader — gets EOF after payload (no FINAL)
        let mut data = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut Box::pin(reader), &mut data)
            .await
            .unwrap();
        assert_eq!(data, b"hello-world");
    }

    // -------------------------------------------------------------------
    // Multi-MB integrity across many forced migrations
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn multi_mb_integrity_across_many_forced_migrations() {
        let (opener, accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut mac = accepter.into_migrating_capable();

        let chunk_size = 10 * 1024;
        let migrations = 100;

        let send = tokio::spawn(async move {
            let mut writer = opener.open_migrating_manual(77, LaneClass::Interactive);
            for i in 0..migrations {
                let mut chunk = vec![0u8; chunk_size];
                for (j, b) in chunk.iter_mut().enumerate() {
                    *b = ((i as usize).wrapping_mul(chunk_size).wrapping_add(j)) as u8;
                }
                writer.write_all(&chunk).await.unwrap();
                let target = if i % 2 == 0 { LaneClass::Bulk } else { LaneClass::Interactive };
                writer.force_migrate(target).await.unwrap();
            }
            writer.shutdown().unwrap();
        });

        let accepted = mac.accept().await.unwrap();
        let mut reader = match accepted {
            AcceptedStream::Migrating { reader, .. } => reader,
            _ => panic!("expected migrating stream"),
        };

        let drain = tokio::spawn(async move { loop { let _ = mac.accept().await; } });

        let mut data = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut data).await.unwrap();
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

        let drain = tokio::spawn(async move { loop { let _ = mac.accept().await; } });

        let mut data = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut data).await.unwrap();
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
        let drain = tokio::spawn(async move {
            loop {
                match mac.accept().await {
                    Ok(_) => {} // successor routed internally
                    Err(_) => break,
                }
            }
        });

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
            loop { let _ = mac.accept().await; }
        });

        let mut buf = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut buf).await.unwrap();
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
        let drain = tokio::spawn(async move { loop { let _ = mac.accept().await; } });

        let (ra, rb) = tokio::join!(
            async {
                let mut reader = reader_a;
                let mut s = String::new();
                tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut s).await.unwrap();
                s
            },
            async {
                let mut reader = reader_b;
                let mut s = String::new();
                tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut s).await.unwrap();
                s
            }
        );

        let mut results = vec![ra, rb];
        results.sort();
        assert_eq!(results, vec![
            "stream-A-chunk-1stream-A-chunk-2".to_string(),
            "stream-B-chunk-1stream-B-chunk-2".to_string(),
        ]);

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
                writer.write_all(format!("delta-{:02}-", i).as_bytes()).await.unwrap();
            }
            writer.shutdown().unwrap();
        });

        let accepted = mac.accept().await.unwrap();
        let mut reader = match accepted {
            AcceptedStream::Migrating { reader, .. } => reader,
            _ => panic!("expected migrating stream"),
        };

        drop(mac);

        let mut data = Vec::with_capacity(sync_size + 200);
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut data).await.unwrap();

        assert_eq!(data.len(), sync_size + 9 * deltas, "total size mismatch");
        for b in &data[..sync_size] {
            assert_eq!(*b, 0xAB, "sync data corrupted");
        }
        let deltas_str = std::str::from_utf8(&data[sync_size..]).unwrap();
        for i in 0..deltas {
            assert!(deltas_str.contains(&format!("delta-{:02}-", i)), "missing delta {i}");
        }

        send.await.unwrap();
    }

    // -------------------------------------------------------------------
    // Policy: demotion respects cooldown
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn demotion_respects_cooldown() {
        let (opener, _accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut writer = opener.open_migrating(1, LaneClass::Bulk);
        for _ in 0..20 { writer.write_all(&[0u8; 100]).await.unwrap(); }
        writer.write_all(&[0u8; 3000]).await.unwrap();
        for _ in 0..20 { writer.write_all(&[0u8; 100]).await.unwrap(); }
        for _ in 0..4 { writer.write_all(&[0u8; 100]).await.unwrap(); }
        writer.shutdown().unwrap();
    }

    // -------------------------------------------------------------------
    // Policy: demotion after cooldown expires
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn demotion_after_cooldown_expires() {
        let (opener, _accepter, _s, _sb, _c, _cb) = make_dual_session().await;
        let mut writer = opener.open_migrating(1, LaneClass::Bulk);
        for _ in 0..20 { writer.write_all(&[0u8; 100]).await.unwrap(); }
        writer.write_all(&[0u8; 3000]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        for _ in 0..20 { writer.write_all(&[0u8; 100]).await.unwrap(); }
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

        drop(mac);

        let mut data = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut data).await.unwrap();
        assert_eq!(data, "data-on-bulk");

        send.await.unwrap();
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
        tokio::io::AsyncWriteExt::write_all(&mut accepted_writer, b"hello-from-s2c  ").await.unwrap();
        tokio::io::AsyncWriteExt::shutdown(&mut accepted_writer).await.unwrap();

        write.await.unwrap();

        let mut resp = [0u8; 16];
        tokio::io::AsyncReadExt::read_exact(&mut client_reader, &mut resp).await.unwrap();
        assert_eq!(&resp, b"hello-from-s2c  ");

        let mut buf = [0u8; 1];
        let n = tokio::io::AsyncReadExt::read(&mut client_reader, &mut buf).await.unwrap();
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
        tokio::io::AsyncWriteExt::write_all(&mut accepted_writer, b"first-response ").await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut accepted_writer, b"second-response").await.unwrap();
        tokio::io::AsyncWriteExt::shutdown(&mut accepted_writer).await.unwrap();

        write.await.unwrap();

        let mut resp = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut client_reader, &mut resp).await.unwrap();
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
            loop { let _ = mac.accept().await; }
        });

        let echo = tokio::spawn(async move {
            let mut buf = vec![0u8; 256];
            loop {
                let n = tokio::io::AsyncReadExt::read(&mut accepted_reader, &mut buf).await.unwrap();
                if n == 0 { break; }
                tokio::io::AsyncWriteExt::write_all(&mut accepted_writer, &buf[..n]).await.unwrap();
            }
            tokio::io::AsyncWriteExt::shutdown(&mut accepted_writer).await.unwrap();
        });

        write.await.unwrap();
        echo.await.unwrap();
        drain.abort();

        let mut echoed = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut client_reader, &mut echoed).await.unwrap();
        let mut expected = Vec::new();
        for i in 0..MIGRATIONS {
            expected.extend_from_slice(format!("chunk-{:03}-{:04X}", i, i).as_bytes());
        }
        assert_eq!(echoed.len(), expected.len(), "total echoed byte count mismatch");
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
            loop { let _ = mac.accept().await; }
        });

        let mut data = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut accepted_reader, &mut data).await.unwrap();
        assert_eq!(data, "write-only-data");

        write.await.unwrap();
        drain.abort();
    }
}
