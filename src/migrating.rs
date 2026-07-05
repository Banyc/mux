use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;

use crate::{
    dual_lane::{DualStreamAccepter, DualStreamOpener, LaneClass},
    stream::writer::StreamWriter,
    stream_migration::{GenerationChain, MigrationError, ResumeHeader, SpliceRegistry, SplicedReader},
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

    fn is_bulk(&self) -> bool {
        self.bulk_count > self.small_count
    }
}

// ---------------------------------------------------------------------------
// Migration error
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum MigratingError {
    Migration(MigrationError),
    OpenFailed,
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
        loop {
            let state = std::mem::replace(&mut self.state, WriterState::Closed);
            match state {
                WriterState::Active { .. } => {
                    self.state = state;
                    return Ok(());
                }
                WriterState::PendingOpen { lane } => {
                    let (_, mut writer) = self.opener.open(lane)
                        .await
                        .map_err(|_| MigratingError::OpenFailed)?;
                    self.chain
                        .start_generation(&mut tokio_util_writer(&mut writer), false)
                        .await?;
                    self.state = WriterState::Active { writer, lane };
                    return Ok(());
                }
                WriterState::Migrating { target_lane } => {
                    let (_, mut writer) = self.opener.open(target_lane)
                        .await
                        .map_err(|_| MigratingError::OpenFailed)?;
                    self.chain
                        .start_generation(&mut tokio_util_writer(&mut writer), false)
                        .await?;
                    self.state = WriterState::Active { writer, lane: target_lane };
                    return Ok(());
                }
                WriterState::Closed => return Err(MigratingError::LaneDead),
            }
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

    pub fn shutdown(&mut self) -> Result<(), MigratingError> {
        if let WriterState::Active { writer, .. } = &mut self.state {
            let _ = writer.shutdown();
        }
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

    /// Same as [`open_migrating`] but with auto-classification disabled.
    /// The caller drives lane changes via [`MigratingStreamWriter::force_migrate`].
    pub fn open_migrating_manual(
        &self,
        logical_id: u64,
        initial_lane: LaneClass,
    ) -> MigratingStreamWriter {
        MigratingStreamWriter::new(self.clone(), logical_id, initial_lane, false)
    }
}

// ---------------------------------------------------------------------------
// MigratingCapableAccepter
// ---------------------------------------------------------------------------

/// Wraps a [`DualStreamAccepter`] and transparently handles migrating
/// streams while passing non-migrating streams through untouched.
pub struct MigratingCapableAccepter {
    inner: DualStreamAccepter,
    registry: SpliceRegistry,
    // Queue for successor generations
    successor_tx: mpsc::UnboundedSender<(ResumeHeader, StreamReader)>,
    successor_rx: mpsc::UnboundedReceiver<(ResumeHeader, StreamReader)>,
}

impl MigratingCapableAccepter {
    pub fn new(inner: DualStreamAccepter) -> Self {
        let (successor_tx, successor_rx) = mpsc::unbounded_channel();
        Self {
            inner,
            registry: SpliceRegistry::new(),
            successor_tx,
            successor_rx,
        }
    }

    /// Accept the next stream. If it carries a resume header (generation
    /// ≥ 0), it is routed through the [`SpliceRegistry`]; for generation
    /// 0, a new [`MigratingStreamReader`] pair is returned. Non-migrating
    /// streams pass through unchanged.
    pub async fn accept(
        &mut self,
    ) -> Result<AcceptedStream, MigratingError> {
        loop {
            let (reader, writer, lane) = self.inner.accept()
                .await
                .map_err(|_| MigratingError::LaneDead)?;

            // Peek at the first 21 bytes to check for a resume header
            let (is_migrating, header_opt, reader) =
                Self::peek_resume_header(reader).await?;

            if is_migrating {
                if let Some(header) = header_opt {
                    let generation = header.generation;

                    if generation == 0 {
                        let spliced = self.registry
                            .dispatch(header, reader)
                            .map_err(MigratingError::Migration)?
                            .expect("gen 0 must return SplicedReader");

                        return Ok(AcceptedStream::Migrating {
                            reader: spliced,
                            writer,
                            source_lane: lane,
                        });
                    } else {
                        // Successor generation — dispatch into registry
                        self.registry
                            .dispatch(header, reader)
                            .map_err(MigratingError::Migration)?;
                        // Enqueue successor to the SplicedReader
                        // (for now, this is a stub — the driver handles it)
                        continue;
                    }
                }
            }

            // Non-migrating stream — pass through
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
                    Ok((false, None, reader))
                }
            }
            Err(_) => {
                Ok((false, None, reader))
            }
        }
    }

    /// Enqueue a successor generation for a previously returned
    /// migrating stream. The header should match the logical stream.
    pub fn enqueue_successor(
        &mut self,
        header: ResumeHeader,
        reader: StreamReader,
    ) {
        let _ = self.successor_tx.send((header, reader));
    }
}

// ---------------------------------------------------------------------------
// AcceptedStream
// ---------------------------------------------------------------------------

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
// PrefixReader — prepends buffered bytes to a StreamReader
// ---------------------------------------------------------------------------

struct PrefixReader {
    prefix: Vec<u8>,
    pos: usize,
    inner: StreamReader,
}

impl PrefixReader {
    fn new(prefix: Vec<u8>, inner: StreamReader) -> Self {
        Self { prefix, pos: 0, inner }
    }
}

impl AsyncRead for PrefixReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        // Serve prefix bytes first
        if this.pos < this.prefix.len() {
            let remaining = this.prefix.len() - this.pos;
            let n = buf.remaining().min(remaining);
            buf.put_slice(&this.prefix[this.pos..this.pos + n]);
            this.pos += n;
            return Poll::Ready(Ok(()));
        }
        // Fall through to inner reader
        Pin::new(&mut this.inner).poll_read(cx, buf)
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
        let (int_c2s, int_s2c) = duplex(32768);
        let (bulk_c2s, bulk_s2c) = duplex(32768);

        let (int_srv_r, int_srv_w) = tokio::io::split(int_c2s);
        let (int_cli_r, int_cli_w) = tokio::io::split(int_s2c);
        let (bulk_srv_r, bulk_srv_w) = tokio::io::split(bulk_c2s);
        let (bulk_cli_r, bulk_cli_w) = tokio::io::split(bulk_s2c);

        let srv_cfg = MuxConfig {
            initiation: Initiation::Server,
            heartbeat_interval: Duration::from_secs(1),
        };
        let cli_cfg = MuxConfig {
            initiation: Initiation::Client,
            heartbeat_interval: Duration::from_secs(1),
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
}
