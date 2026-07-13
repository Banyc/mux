use std::{
    collections::hash_map::RandomState,
    future::Future,
    hash::{BuildHasher, Hasher},
    io,
    ops::DerefMut,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{ready, Context, Poll},
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf},
    sync::oneshot,
    task::JoinSet,
};

use crate::{
    protocol::Header,
    serve::{spawn_mux_no_reconnection, MuxConfig, MuxError},
    stream::{
        opener::{StreamOpenError, StreamOpener},
        writer::StreamWriter,
    },
    StreamAccepter, StreamReader,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const LANE_HELLO_INTERACTIVE: u8 = 0xD1;
const LANE_HELLO_BULK: u8 = 0xD2;
const PAIRING_NONCE_LEN: usize = 16;
const HELLO_LEN: usize = 1 + PAIRING_NONCE_LEN;

/// Threshold for `open_auto` classification. Writes strictly larger than
/// this go to the bulk lane; equal-or-smaller go to interactive. Mirrors
/// `DATA_MEDIUM_CAP` in `central_io::writer`.
pub const AUTO_BULK_THRESHOLD: usize = 2 * 1024; // 2048

// ---------------------------------------------------------------------------
// LaneClass
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneClass {
    Interactive,
    Bulk,
}

impl LaneClass {
    fn hello_byte(self) -> u8 {
        match self {
            LaneClass::Interactive => LANE_HELLO_INTERACTIVE,
            LaneClass::Bulk => LANE_HELLO_BULK,
        }
    }
    fn from_hello_byte(b: u8) -> Option<Self> {
        match b {
            LANE_HELLO_INTERACTIVE => Some(LaneClass::Interactive),
            LANE_HELLO_BULK => Some(LaneClass::Bulk),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// PairingNonce
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PairingNonce([u8; PAIRING_NONCE_LEN]);

impl PairingNonce {
    pub fn generate() -> Self {
        let rs = RandomState::new();
        let mut buf = [0u8; PAIRING_NONCE_LEN];
        // Fill each u64 word from a separate OS-seeded hasher for full entropy.
        let mut h0 = rs.build_hasher();
        h0.write_u64(0);
        buf[0..8].copy_from_slice(&h0.finish().to_le_bytes());
        let mut h1 = rs.build_hasher();
        h1.write_u64(1);
        buf[8..16].copy_from_slice(&h1.finish().to_le_bytes());
        Self(buf)
    }
}

impl AsRef<[u8]> for PairingNonce {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

// ---------------------------------------------------------------------------
// Lane hello I/O
// ---------------------------------------------------------------------------

/// Write a single heartbeat frame on `writer` to prove the paired lane is
/// alive before any application data flows. Use with
/// [`spawn_mux_no_reconnection_with_first_receive_deadline`] so the
/// receiver switches off its shorter first-receive deadline.
pub async fn write_birth_heartbeat<W: AsyncWrite + Unpin>(writer: &mut W) -> io::Result<()> {
    writer.write_all(&Header::Heartbeat.encode()).await
}

#[derive(Debug, Clone)]
pub enum LaneHelloError {
    Io(io::ErrorKind),
    BadLaneClass(u8),
    ShortRead { expected: usize, got: usize },
}

impl From<LaneHelloError> for io::Error {
    fn from(e: LaneHelloError) -> Self {
        match e {
            LaneHelloError::Io(kind) => io::Error::from(kind),
            LaneHelloError::BadLaneClass(_) => {
                io::Error::new(io::ErrorKind::InvalidData, "bad lane hello class byte")
            }
            LaneHelloError::ShortRead { .. } => {
                io::Error::new(io::ErrorKind::UnexpectedEof, "short lane hello read")
            }
        }
    }
}

pub async fn write_lane_hello<W: AsyncWrite + Unpin>(
    writer: &mut W,
    class: LaneClass,
    nonce: PairingNonce,
) -> Result<(), LaneHelloError> {
    use tokio::io::AsyncWriteExt;
    let mut buf = [0u8; HELLO_LEN];
    buf[0] = class.hello_byte();
    buf[1..].copy_from_slice(nonce.as_ref());
    writer
        .write_all(&buf)
        .await
        .map_err(|e| LaneHelloError::Io(e.kind()))?;
    Ok(())
}

pub async fn read_lane_hello<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<(LaneClass, PairingNonce), LaneHelloError> {
    use tokio::io::AsyncReadExt;
    let mut buf = [0u8; HELLO_LEN];
    reader
        .read_exact(&mut buf)
        .await
        .map_err(|e| LaneHelloError::Io(e.kind()))?;
    let class = LaneClass::from_hello_byte(buf[0]).ok_or(LaneHelloError::BadLaneClass(buf[0]))?;
    let mut nonce_bytes = [0u8; PAIRING_NONCE_LEN];
    nonce_bytes.copy_from_slice(&buf[1..]);
    Ok((class, PairingNonce(nonce_bytes)))
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum DualMuxError {
    Mux(MuxError),
    LaneHello(LaneHelloError),
    NonceMismatch,
    HelloDeadline,
}

#[derive(Debug)]
pub enum DualStreamOpenError {
    LaneDead,
    StreamOpen(StreamOpenError),
    /// Writer was dropped before its first write — peer should see clean EOF.
    CleanClose,
}

impl From<StreamOpenError> for DualStreamOpenError {
    fn from(e: StreamOpenError) -> Self {
        DualStreamOpenError::StreamOpen(e)
    }
}

#[derive(Debug)]
pub enum AutoWriteError {
    LaneDead,
    OpenFailed(DualStreamOpenError),
    SendFailed(crate::stream::writer::SendError),
}

#[derive(Debug)]
pub enum DualAcceptError {
    LaneDead,
}

// ---------------------------------------------------------------------------
// Liveness guard
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Liveness {
    alive: Arc<AtomicBool>,
}

impl Liveness {
    pub fn new() -> Self {
        Self {
            alive: Arc::new(AtomicBool::new(true)),
        }
    }
    #[allow(dead_code)]
    fn kill(&self) {
        self.alive.store(false, Ordering::SeqCst);
    }
    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }
}

impl Default for Liveness {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// DualStreamOpener
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct DualStreamOpener {
    interactive: StreamOpener,
    bulk: StreamOpener,
    liveness: Liveness,
}

impl DualStreamOpener {
    pub(crate) fn new(interactive: StreamOpener, bulk: StreamOpener, liveness: Liveness) -> Self {
        Self {
            interactive,
            bulk,
            liveness,
        }
    }

    pub async fn open(
        &self,
        class: LaneClass,
    ) -> Result<(StreamReader, StreamWriter), DualStreamOpenError> {
        if !self.liveness.is_alive() {
            return Err(DualStreamOpenError::LaneDead);
        }
        let opener = match class {
            LaneClass::Interactive => &self.interactive,
            LaneClass::Bulk => &self.bulk,
        };
        opener.open().await.map_err(Into::into)
    }

    /// Returns a lazy writer/reader pair. The actual stream is opened on the
    /// first write to [`AutoWriter`]. The first write's total length
    /// determines the lane: strictly larger than [`AUTO_BULK_THRESHOLD`]
    /// (2048) → bulk lane, else interactive. The decision is **sticky** —
    /// the stream never migrates lanes.
    ///
    /// The [`AutoReader`] blocks until the first write classifies and opens
    /// the real stream.
    ///
    /// # Pitfalls
    ///
    /// - **Header-first protocols**: a separate small length-prefix write
    ///   misclassifies. Use [`Self::open`] with an explicit
    ///   [`LaneClass::Bulk`] hint.
    /// - **Server-speaks-first protocols**: the stream does not exist on the
    ///   wire until the first *local* write. If only the peer writes, the
    ///   local side never opens the stream.
    /// - **Buffered combinators** (`write_all_buf`): the first poll_write
    ///   may carry multiple buffered chunks concatenated; classification on
    ///   that full length is correct. Vectored writes sum all slices.
    pub fn open_auto(&self) -> (AutoReader, AutoWriter) {
        let (reader_tx, reader_rx) = oneshot::channel();
        let writer = AutoWriter::new(
            self.interactive.clone(),
            self.bulk.clone(),
            reader_tx,
            self.liveness.clone(),
        );
        let reader = AutoReader::new(reader_rx);
        (reader, writer)
    }
}

// ---------------------------------------------------------------------------
// AutoWriter
// ---------------------------------------------------------------------------

pub struct AutoWriter {
    state: AutoWriterState,
    liveness: Liveness,
}

type OpenFuture =
    Pin<Box<dyn Future<Output = Result<(StreamReader, StreamWriter), StreamOpenError>> + Send>>;

enum AutoWriterState {
    Pending {
        interactive: StreamOpener,
        bulk: StreamOpener,
        reader_tx: Option<oneshot::Sender<Result<StreamReader, DualStreamOpenError>>>,
    },
    Opening {
        open_fut: OpenFuture,
        reader_tx: Option<oneshot::Sender<Result<StreamReader, DualStreamOpenError>>>,
    },
    Active {
        writer: StreamWriter,
    },
    Failed,
}

impl std::fmt::Debug for AutoWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AutoWriter").finish_non_exhaustive()
    }
}

impl AutoWriter {
    fn new(
        interactive: StreamOpener,
        bulk: StreamOpener,
        reader_tx: oneshot::Sender<Result<StreamReader, DualStreamOpenError>>,
        liveness: Liveness,
    ) -> Self {
        Self {
            state: AutoWriterState::Pending {
                interactive,
                bulk,
                reader_tx: Some(reader_tx),
            },
            liveness,
        }
    }

    fn classify_len(total_len: usize) -> LaneClass {
        if total_len > AUTO_BULK_THRESHOLD {
            LaneClass::Bulk
        } else {
            LaneClass::Interactive
        }
    }

    /// Synchronously attempt to open the stream. Called from `poll_write`
    /// (inside an async runtime context).
    fn try_open(&mut self, total_len: usize) {
        let (class, interactive, bulk, reader_tx) =
            match std::mem::replace(&mut self.state, AutoWriterState::Failed) {
                AutoWriterState::Pending {
                    interactive,
                    bulk,
                    reader_tx,
                } => {
                    let class = Self::classify_len(total_len);
                    let (interactive, bulk) = match class {
                        LaneClass::Interactive => (Some(interactive), None),
                        LaneClass::Bulk => (None, Some(bulk)),
                    };
                    (class, interactive, bulk, reader_tx)
                }
                other => {
                    self.state = other;
                    return;
                }
            };
        let opener = match class {
            LaneClass::Interactive => interactive.unwrap(),
            LaneClass::Bulk => bulk.unwrap(),
        };
        let open_fut = {
            let opener = opener.clone();
            Box::pin(async move { opener.open().await })
        };
        self.state = AutoWriterState::Opening {
            open_fut,
            reader_tx,
        };
    }

    fn active_writer(&mut self) -> Result<&mut StreamWriter, AutoWriteError> {
        if !self.liveness.is_alive() {
            return Err(AutoWriteError::LaneDead);
        }
        match &mut self.state {
            AutoWriterState::Active { writer } => Ok(writer),
            _ => Err(AutoWriteError::LaneDead),
        }
    }

    fn poll_open(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), AutoWriteError>> {
        let state = std::mem::replace(&mut self.state, AutoWriterState::Failed);
        let (mut open_fut, reader_tx) = match state {
            AutoWriterState::Opening {
                open_fut,
                reader_tx,
            } => (open_fut, reader_tx),
            other => {
                self.state = other;
                return Poll::Ready(Ok(()));
            }
        };
        match open_fut.as_mut().poll(cx) {
            Poll::Ready(Ok((reader, writer))) => {
                if let Some(tx) = reader_tx {
                    let _ = tx.send(Ok(reader));
                }
                self.state = AutoWriterState::Active { writer };
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                if let Some(tx) = reader_tx {
                    let _ = tx.send(Err(DualStreamOpenError::StreamOpen(e)));
                }
                self.state = AutoWriterState::Failed;
                Poll::Ready(Err(AutoWriteError::OpenFailed(
                    DualStreamOpenError::LaneDead,
                )))
            }
            Poll::Pending => {
                self.state = AutoWriterState::Opening {
                    open_fut,
                    reader_tx,
                };
                Poll::Pending
            }
        }
    }

    pub fn poll_write(
        &mut self,
        buf: &[u8],
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize, AutoWriteError>> {
        if !self.liveness.is_alive() {
            return Poll::Ready(Err(AutoWriteError::LaneDead));
        }
        if matches!(self.state, AutoWriterState::Pending { .. }) {
            self.try_open(buf.len());
        }
        if matches!(self.state, AutoWriterState::Opening { .. }) {
            ready!(self.poll_open(cx))?;
        }
        let writer = match self.active_writer() {
            Ok(w) => w,
            Err(e) => return Poll::Ready(Err(e)),
        };
        writer
            .poll_write(buf, cx)
            .map_err(AutoWriteError::SendFailed)
    }

    pub fn poll_write_vectored(
        &mut self,
        bufs: &[io::IoSlice<'_>],
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize, AutoWriteError>> {
        if !self.liveness.is_alive() {
            return Poll::Ready(Err(AutoWriteError::LaneDead));
        }
        if matches!(self.state, AutoWriterState::Pending { .. }) {
            let total_len: usize = bufs.iter().map(|s| s.len()).sum();
            self.try_open(total_len);
        }
        if matches!(self.state, AutoWriterState::Opening { .. }) {
            ready!(self.poll_open(cx))?;
        }
        let writer = match self.active_writer() {
            Ok(w) => w,
            Err(e) => return Poll::Ready(Err(e)),
        };
        let mut total = 0usize;
        for slice in bufs {
            match writer.poll_write(slice, cx) {
                Poll::Ready(Ok(n)) => {
                    total += n;
                    if n < slice.len() {
                        return Poll::Ready(Ok(total));
                    }
                }
                Poll::Ready(Err(e)) => {
                    if total > 0 {
                        return Poll::Ready(Ok(total));
                    }
                    return Poll::Ready(Err(AutoWriteError::SendFailed(e)));
                }
                Poll::Pending => {
                    if total > 0 {
                        return Poll::Ready(Ok(total));
                    }
                    return Poll::Pending;
                }
            }
        }
        Poll::Ready(Ok(total))
    }

    pub fn shutdown(&mut self) -> Result<(), AutoWriteError> {
        match &mut self.state {
            AutoWriterState::Active { writer } => {
                writer.shutdown().map_err(AutoWriteError::SendFailed)
            }
            AutoWriterState::Pending { reader_tx, .. } => {
                if let Some(tx) = reader_tx.take() {
                    let _ = tx.send(Err(DualStreamOpenError::CleanClose));
                }
                self.state = AutoWriterState::Failed;
                Ok(())
            }
            AutoWriterState::Opening { reader_tx, .. } => {
                if let Some(tx) = reader_tx.take() {
                    let _ = tx.send(Err(DualStreamOpenError::CleanClose));
                }
                self.state = AutoWriterState::Failed;
                Ok(())
            }
            AutoWriterState::Failed => Ok(()),
        }
    }
}

impl AsyncWrite for AutoWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.deref_mut();
        this.poll_write(buf, cx).map_err(auto_write_to_io)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.deref_mut();
        this.poll_write_vectored(bufs, cx).map_err(auto_write_to_io)
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let this = self.deref_mut();
        this.shutdown().map_err(auto_write_to_io).into()
    }
}

impl Drop for AutoWriter {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn auto_write_to_io(e: AutoWriteError) -> io::Error {
    match e {
        AutoWriteError::LaneDead => io::ErrorKind::BrokenPipe.into(),
        AutoWriteError::OpenFailed(_) => io::ErrorKind::BrokenPipe.into(),
        AutoWriteError::SendFailed(crate::stream::writer::SendError::LocalClosedStream) => {
            io::ErrorKind::NotConnected.into()
        }
        AutoWriteError::SendFailed(crate::stream::writer::SendError::PeerClosedStream) => {
            io::ErrorKind::BrokenPipe.into()
        }
        AutoWriteError::SendFailed(crate::stream::writer::SendError::DeadCentralIo(_)) => {
            io::ErrorKind::BrokenPipe.into()
        }
    }
}

// ---------------------------------------------------------------------------
// AutoReader
// ---------------------------------------------------------------------------

pub struct AutoReader {
    state: AutoReaderState,
}

enum AutoReaderState {
    Pending {
        rx: oneshot::Receiver<Result<StreamReader, DualStreamOpenError>>,
    },
    Ready {
        reader: StreamReader,
    },
    Failed,
}

impl std::fmt::Debug for AutoReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AutoReader").finish_non_exhaustive()
    }
}

impl AutoReader {
    fn new(rx: oneshot::Receiver<Result<StreamReader, DualStreamOpenError>>) -> Self {
        Self {
            state: AutoReaderState::Pending { rx },
        }
    }
}

impl AsyncRead for AutoReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            let this = self.deref_mut();
            match &mut this.state {
                AutoReaderState::Pending { rx } => match ready!(Pin::new(rx).poll(cx)) {
                    Ok(Ok(reader)) => {
                        this.state = AutoReaderState::Ready { reader };
                        continue;
                    }
                    Ok(Err(DualStreamOpenError::CleanClose)) => {
                        this.state = AutoReaderState::Failed;
                        return Poll::Ready(Ok(()));
                    }
                    Ok(Err(_)) | Err(_) => {
                        this.state = AutoReaderState::Failed;
                        return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)));
                    }
                },
                AutoReaderState::Ready { reader } => {
                    return Pin::new(reader).poll_read(cx, buf);
                }
                AutoReaderState::Failed => {
                    return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// DualStreamAccepter
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct DualStreamAccepter {
    interactive: StreamAccepter,
    bulk: StreamAccepter,
    liveness: Liveness,
}

impl DualStreamAccepter {
    pub(crate) fn new(
        interactive: StreamAccepter,
        bulk: StreamAccepter,
        liveness: Liveness,
    ) -> Self {
        Self {
            interactive,
            bulk,
            liveness,
        }
    }

    /// Accept a stream from either lane. Cancel-safe: a dropped future loses
    /// no stream — the stream stays in the underlying lane's accept queue.
    pub async fn accept(
        &mut self,
    ) -> Result<(StreamReader, StreamWriter, LaneClass), DualAcceptError> {
        if !self.liveness.is_alive() {
            return Err(DualAcceptError::LaneDead);
        }
        tokio::select! {
            res = self.interactive.accept() => {
                res.map(|(r, w)| (r, w, LaneClass::Interactive))
                    .map_err(|_| DualAcceptError::LaneDead)
            }
            res = self.bulk.accept() => {
                res.map(|(r, w)| (r, w, LaneClass::Bulk))
                    .map_err(|_| DualAcceptError::LaneDead)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Spawn helpers
// ---------------------------------------------------------------------------

/// Pair two already-established mux sessions' openers/accepters into a
/// dual-lane facade. The caller is responsible for ensuring the two sessions
/// belong to the same logical peer (e.g. via the lane-hello protocol) AND
/// for wiring joint-liveness supervision — if either lane dies, the
/// caller must kill the other lane and drop the [`Liveness`] guard so
/// extant stream handles error promptly. For a supervised variant that
/// wires this automatically, see [`spawn_dual_mux_paired_supervised`].
pub fn spawn_dual_mux_paired(
    interactive_opener: StreamOpener,
    interactive_accepter: StreamAccepter,
    bulk_opener: StreamOpener,
    bulk_accepter: StreamAccepter,
) -> (DualStreamOpener, DualStreamAccepter) {
    let liveness = Liveness::new();
    let opener = DualStreamOpener::new(interactive_opener, bulk_opener, liveness.clone());
    let accepter = DualStreamAccepter::new(interactive_accepter, bulk_accepter, liveness);
    (opener, accepter)
}

/// Like [`spawn_dual_mux_paired`] but also wires a joint-liveness
/// supervisor: the two lane spawners are folded into `supervisor`. When
/// either lane's mux session finishes (or errors), the supervisor
/// aborts the other lane and kills the shared [`Liveness`] guard so
/// every extant stream handle on the surviving lane errors (rather
/// than hanging) — the two lanes are one session.
///
/// The lane sessions already enforce a receive deadline derived from
/// the heartbeat interval (`RECEIVE_DEADLINE_INTERVALS` in
/// `central_io::reader`), so a dead peer is detected on quiet lanes
/// too — the supervisor propagates that detection across the pair.
fn aggregate_dual_lane_result(
    lane: LaneClass,
    result: Option<Result<MuxError, tokio::task::JoinError>>,
) -> MuxError {
    let task = match lane {
        LaneClass::Interactive => "interactive_lane",
        LaneClass::Bulk => "bulk_lane",
    };
    let source = match result {
        Some(Ok(error)) => error,
        Some(Err(source)) => MuxError::TaskJoin { task, source },
        None => MuxError::TaskStopped { task },
    };
    MuxError::DualLane {
        lane,
        peer_lane_aborted: true,
        source: Box::new(source),
    }
}

pub fn spawn_dual_mux_paired_supervised(
    interactive_opener: StreamOpener,
    interactive_accepter: StreamAccepter,
    interactive_spawner: JoinSet<MuxError>,
    bulk_opener: StreamOpener,
    bulk_accepter: StreamAccepter,
    bulk_spawner: JoinSet<MuxError>,
    supervisor: &mut JoinSet<MuxError>,
) -> (DualStreamOpener, DualStreamAccepter) {
    let liveness = Liveness::new();
    let alive = liveness.alive.clone();

    let mut int_s = interactive_spawner;
    let mut bulk_s = bulk_spawner;

    supervisor.spawn(async move {
        tokio::select! {
            res = int_s.join_next() => {
                bulk_s.abort_all();
                alive.store(false, Ordering::SeqCst);
                aggregate_dual_lane_result(LaneClass::Interactive, res)
            }
            res = bulk_s.join_next() => {
                int_s.abort_all();
                alive.store(false, Ordering::SeqCst);
                aggregate_dual_lane_result(LaneClass::Bulk, res)
            }
        }
    });

    let opener = DualStreamOpener::new(interactive_opener, bulk_opener, liveness.clone());
    let accepter = DualStreamAccepter::new(interactive_accepter, bulk_accepter, liveness);
    (opener, accepter)
}

/// Build a dual-lane session by connecting two transports, writing lane
/// hellos on both, and spawning mux sessions over each.
pub async fn spawn_dual_mux_connector<F, Fut, R, W>(
    mut connect_interactive: F,
    mut connect_bulk: impl FnMut() -> Fut,
    config: MuxConfig,
    spawner: &mut JoinSet<MuxError>,
) -> Result<(DualStreamOpener, DualStreamAccepter), DualMuxError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<(R, W)>> + Send,
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let nonce = PairingNonce::generate();

    let Some((int_reader, mut int_writer)) = connect_interactive().await else {
        return Err(DualMuxError::LaneHello(LaneHelloError::Io(
            io::ErrorKind::ConnectionRefused,
        )));
    };
    write_lane_hello(&mut int_writer, LaneClass::Interactive, nonce)
        .await
        .map_err(DualMuxError::LaneHello)?;

    let Some((bulk_reader, mut bulk_writer)) = connect_bulk().await else {
        return Err(DualMuxError::LaneHello(LaneHelloError::Io(
            io::ErrorKind::ConnectionRefused,
        )));
    };
    write_lane_hello(&mut bulk_writer, LaneClass::Bulk, nonce)
        .await
        .map_err(DualMuxError::LaneHello)?;

    let mut int_spawner = JoinSet::new();
    let (int_opener, int_accepter) =
        spawn_mux_no_reconnection(int_reader, int_writer, config.clone(), &mut int_spawner);
    let mut bulk_spawner = JoinSet::new();
    let (bulk_opener, bulk_accepter) =
        spawn_mux_no_reconnection(bulk_reader, bulk_writer, config.clone(), &mut bulk_spawner);

    let liveness = Liveness::new();
    let alive = liveness.alive.clone();

    spawner.spawn(async move {
        tokio::select! {
            res = int_spawner.join_next() => {
                bulk_spawner.abort_all();
                alive.store(false, Ordering::SeqCst);
                aggregate_dual_lane_result(LaneClass::Interactive, res)
            }
            res = bulk_spawner.join_next() => {
                int_spawner.abort_all();
                alive.store(false, Ordering::SeqCst);
                aggregate_dual_lane_result(LaneClass::Bulk, res)
            }
        }
    });

    let opener = DualStreamOpener::new(int_opener, bulk_opener, liveness.clone());
    let accepter = DualStreamAccepter::new(int_accepter, bulk_accepter, liveness);
    Ok((opener, accepter))
}

/// Read one lane hello from a freshly-connected transport with a deadline.
/// Returns the lane class, nonce, and a [`PendingAcceptor`] that can be
/// paired with its partner later via [`complete_pairing`].
pub async fn spawn_dual_mux_acceptor<R, W>(
    mut reader: R,
    writer: W,
    config: MuxConfig,
    hello_deadline: Duration,
) -> Result<(LaneClass, PairingNonce, PendingAcceptor), DualMuxError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (class, nonce) =
        match tokio::time::timeout(hello_deadline, read_lane_hello(&mut reader)).await {
            Ok(Ok(x)) => x,
            Ok(Err(e)) => return Err(DualMuxError::LaneHello(e)),
            Err(_) => return Err(DualMuxError::HelloDeadline),
        };

    let mut lane_spawner = JoinSet::new();
    let (opener, accepter) = spawn_mux_no_reconnection(reader, writer, config, &mut lane_spawner);

    let pending = PendingAcceptor {
        class,
        nonce,
        opener,
        accepter,
        spawner: lane_spawner,
    };
    Ok((class, nonce, pending))
}

/// A half-accepted lane connection waiting for its nonce-matching partner.
///
/// Fields are public so callers that read the lane hello externally can
/// construct a pending acceptor by spawning the mux session themselves
/// (e.g. to send a kill packet on the raw transport before the mux is
/// started when the hello is rejected).
pub struct PendingAcceptor {
    pub class: LaneClass,
    pub nonce: PairingNonce,
    pub opener: StreamOpener,
    pub accepter: StreamAccepter,
    pub spawner: JoinSet<MuxError>,
}

impl std::fmt::Debug for PendingAcceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingAcceptor")
            .field("class", &self.class)
            .field("nonce", &self.nonce)
            .finish_non_exhaustive()
    }
}

impl PendingAcceptor {
    pub fn new(
        class: LaneClass,
        nonce: PairingNonce,
        opener: StreamOpener,
        accepter: StreamAccepter,
        spawner: JoinSet<MuxError>,
    ) -> Self {
        Self {
            class,
            nonce,
            opener,
            accepter,
            spawner,
        }
    }
}

/// Combine two pending acceptors with matching nonces into a dual-lane
/// facade. Both lane spawners are folded into `spawner`.
pub fn complete_pairing(
    pending1: PendingAcceptor,
    pending2: PendingAcceptor,
    spawner: &mut JoinSet<MuxError>,
) -> Result<(DualStreamOpener, DualStreamAccepter), DualMuxError> {
    if pending1.nonce != pending2.nonce {
        return Err(DualMuxError::NonceMismatch);
    }
    if pending1.class == pending2.class {
        return Err(DualMuxError::NonceMismatch);
    }

    let (int_pending, bulk_pending) = match (pending1.class, pending2.class) {
        (LaneClass::Interactive, LaneClass::Bulk) => (pending1, pending2),
        (LaneClass::Bulk, LaneClass::Interactive) => (pending2, pending1),
        _ => return Err(DualMuxError::NonceMismatch),
    };

    let liveness = Liveness::new();
    let alive = liveness.alive.clone();

    // One supervisor races both lane spawners: when either lane's session
    // finishes (or errors), it aborts the OTHER lane and kills the liveness
    // guard so every extant stream handle on the surviving lane errors promptly.
    spawner.spawn(async move {
        let mut int_s = int_pending.spawner;
        let mut bulk_s = bulk_pending.spawner;
        tokio::select! {
            res = int_s.join_next() => {
                bulk_s.abort_all();
                alive.store(false, Ordering::SeqCst);
                aggregate_dual_lane_result(LaneClass::Interactive, res)
            }
            res = bulk_s.join_next() => {
                int_s.abort_all();
                alive.store(false, Ordering::SeqCst);
                aggregate_dual_lane_result(LaneClass::Bulk, res)
            }
        }
    });

    let opener = DualStreamOpener::new(int_pending.opener, bulk_pending.opener, liveness.clone());
    let accepter = DualStreamAccepter::new(int_pending.accepter, bulk_pending.accepter, liveness);
    Ok((opener, accepter))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::Initiation;
    use tokio::io::{duplex, AsyncWriteExt};

    fn srv_config() -> MuxConfig {
        MuxConfig {
            initiation: Initiation::Server,
            heartbeat_interval: Duration::from_secs(1),
            frame_reassembly: false,
        }
    }

    // -------------------------------------------------------------------
    // Hello round-trip
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn hello_round_trip() {
        let nonce = PairingNonce::generate();
        let (c2s, s2c) = duplex(64);
        let (mut crx, mut ctx) = tokio::io::split(c2s);
        let (mut srx, mut stx) = tokio::io::split(s2c);

        write_lane_hello(&mut ctx, LaneClass::Interactive, nonce)
            .await
            .unwrap();
        let (class, read_nonce) = read_lane_hello(&mut srx).await.unwrap();
        assert_eq!(class, LaneClass::Interactive);
        assert_eq!(read_nonce, nonce);

        write_lane_hello(&mut stx, LaneClass::Bulk, nonce)
            .await
            .unwrap();
        let (class, read_nonce) = read_lane_hello(&mut crx).await.unwrap();
        assert_eq!(class, LaneClass::Bulk);
        assert_eq!(read_nonce, nonce);
    }

    #[tokio::test]
    async fn hello_bad_class_rejected() {
        let nonce = PairingNonce::generate();
        let (c2s, s2c) = duplex(64);
        let (_, mut ctx) = tokio::io::split(c2s);
        let (mut srx, _) = tokio::io::split(s2c);

        let mut buf = [0u8; HELLO_LEN];
        buf[0] = 0xFF;
        buf[1..].copy_from_slice(nonce.as_ref());
        ctx.write_all(&buf).await.unwrap();

        let result = read_lane_hello(&mut srx).await;
        assert!(matches!(result, Err(LaneHelloError::BadLaneClass(0xFF))));
    }

    #[tokio::test]
    async fn hello_short_read() {
        let nonce = PairingNonce::generate();
        let (c2s, s2c) = duplex(64);
        let (_, mut ctx) = tokio::io::split(c2s);
        let (mut srx, _) = tokio::io::split(s2c);

        ctx.write_all(&nonce.as_ref()[..10]).await.unwrap();
        drop(ctx);

        let result = read_lane_hello(&mut srx).await;
        assert!(matches!(result, Err(LaneHelloError::Io(_))));
    }

    // -------------------------------------------------------------------
    // Helper: build two paired mux sessions (server + client) for each lane
    // -------------------------------------------------------------------

    struct SessionEnds {
        opener: StreamOpener,
        accepter: StreamAccepter,
        _spawner: JoinSet<MuxError>,
    }

    async fn make_session_pair(
        srv_init: Initiation,
        cli_init: Initiation,
    ) -> (SessionEnds, SessionEnds) {
        let (c2s, s2c) = duplex(32768);
        let (srv_r, srv_w) = tokio::io::split(c2s);
        let (cli_r, cli_w) = tokio::io::split(s2c);

        let mut srv_spawner = JoinSet::new();
        let (srv_opener, srv_accepter) = spawn_mux_no_reconnection(
            srv_r,
            srv_w,
            MuxConfig {
                initiation: srv_init,
                heartbeat_interval: Duration::from_secs(1),
                frame_reassembly: false,
            },
            &mut srv_spawner,
        );

        let mut cli_spawner = JoinSet::new();
        let (cli_opener, cli_accepter) = spawn_mux_no_reconnection(
            cli_r,
            cli_w,
            MuxConfig {
                initiation: cli_init,
                heartbeat_interval: Duration::from_secs(1),
                frame_reassembly: false,
            },
            &mut cli_spawner,
        );

        (
            SessionEnds {
                opener: srv_opener,
                accepter: srv_accepter,
                _spawner: srv_spawner,
            },
            SessionEnds {
                opener: cli_opener,
                accepter: cli_accepter,
                _spawner: cli_spawner,
            },
        )
    }

    // -------------------------------------------------------------------
    // open_auto classification
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn open_auto_small_goes_interactive() {
        let (srv_int, mut cli_int) =
            make_session_pair(Initiation::Server, Initiation::Client).await;
        let (srv_bulk, mut cli_bulk) =
            make_session_pair(Initiation::Server, Initiation::Client).await;

        let liveness = Liveness::new();
        let opener = DualStreamOpener::new(srv_int.opener, srv_bulk.opener, liveness);
        let (_reader, mut writer) = opener.open_auto();

        // Small write → interactive lane
        writer.write_all(&[0xAAu8; 100]).await.unwrap();

        // Should arrive on interactive lane within 500ms
        let int_res =
            tokio::time::timeout(Duration::from_millis(500), cli_int.accepter.accept()).await;
        assert!(
            int_res.is_ok(),
            "small write should arrive on interactive lane"
        );

        // Bulk lane should still be empty
        let bulk_res =
            tokio::time::timeout(Duration::from_millis(200), cli_bulk.accepter.accept()).await;
        assert!(
            bulk_res.is_err(),
            "bulk lane should not have received the small write"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn open_auto_large_goes_bulk() {
        let (srv_int, mut cli_int) =
            make_session_pair(Initiation::Server, Initiation::Client).await;
        let (srv_bulk, mut cli_bulk) =
            make_session_pair(Initiation::Server, Initiation::Client).await;

        let liveness = Liveness::new();
        let opener = DualStreamOpener::new(srv_int.opener, srv_bulk.opener, liveness);
        let (_reader, mut writer) = opener.open_auto();

        // Large write → bulk lane
        writer.write_all(&[0xBBu8; 3000]).await.unwrap();

        // Should arrive on bulk lane
        let bulk_res =
            tokio::time::timeout(Duration::from_millis(500), cli_bulk.accepter.accept()).await;
        assert!(bulk_res.is_ok(), "large write should arrive on bulk lane");

        // Interactive lane should be empty
        let int_res =
            tokio::time::timeout(Duration::from_millis(200), cli_int.accepter.accept()).await;
        assert!(
            int_res.is_err(),
            "interactive lane should not have received the large write"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn open_auto_at_threshold_is_interactive() {
        let (srv_int, mut cli_int) =
            make_session_pair(Initiation::Server, Initiation::Client).await;
        let (srv_bulk, _cli_bulk) = make_session_pair(Initiation::Server, Initiation::Client).await;

        let liveness = Liveness::new();
        let opener = DualStreamOpener::new(srv_int.opener, srv_bulk.opener, liveness);
        let (_reader, mut writer) = opener.open_auto();

        // Exactly the threshold → not strictly larger → interactive
        writer
            .write_all(&[0xCCu8; AUTO_BULK_THRESHOLD])
            .await
            .unwrap();

        let int_res =
            tokio::time::timeout(Duration::from_millis(500), cli_int.accepter.accept()).await;
        assert!(
            int_res.is_ok(),
            "exactly-threshold write should go to interactive lane"
        );
    }

    // -------------------------------------------------------------------
    // Stickiness
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn open_auto_is_sticky() {
        let (srv_int, mut cli_int) =
            make_session_pair(Initiation::Server, Initiation::Client).await;
        let (srv_bulk, mut cli_bulk) =
            make_session_pair(Initiation::Server, Initiation::Client).await;

        let liveness = Liveness::new();
        let opener = DualStreamOpener::new(srv_int.opener, srv_bulk.opener, liveness);
        let (_reader, mut writer) = opener.open_auto();

        // First write small → interactive
        writer.write_all(&[0x01u8; 200]).await.unwrap();
        // Second write large → still interactive (sticky)
        writer.write_all(&[0x02u8; 5000]).await.unwrap();

        // Both writes appear on the interactive lane
        let int_res =
            tokio::time::timeout(Duration::from_millis(500), cli_int.accepter.accept()).await;
        assert!(int_res.is_ok(), "sticky stream must stay on interactive");

        // Bulk lane should have nothing
        let bulk_res =
            tokio::time::timeout(Duration::from_millis(200), cli_bulk.accepter.accept()).await;
        assert!(
            bulk_res.is_err(),
            "bulk lane must be empty for sticky stream"
        );
    }

    // -------------------------------------------------------------------
    // Joint liveness
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn killed_liveness_propagates_to_open() {
        let (srv_int, _cli_int) = make_session_pair(Initiation::Server, Initiation::Client).await;
        let (srv_bulk, _cli_bulk) = make_session_pair(Initiation::Server, Initiation::Client).await;

        let liveness = Liveness::new();
        let opener = DualStreamOpener::new(srv_int.opener, srv_bulk.opener, liveness.clone());
        liveness.kill();

        let result = opener.open(LaneClass::Interactive).await;
        assert!(matches!(result, Err(DualStreamOpenError::LaneDead)));
    }

    // -------------------------------------------------------------------
    // Cancel-safe accept
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn dual_accept_is_cancel_safe() {
        let (srv_int, cli_int) = make_session_pair(Initiation::Server, Initiation::Client).await;
        let (srv_bulk, cli_bulk) = make_session_pair(Initiation::Server, Initiation::Client).await;

        let liveness = Liveness::new();
        let mut accepter = DualStreamAccepter::new(cli_int.accepter, cli_bulk.accepter, liveness);

        // Open one stream on each lane (server-side)
        srv_int.opener.open().await.unwrap();
        srv_bulk.opener.open().await.unwrap();

        // Accept one — the other must still be available afterward
        let (_, _, _) = accepter.accept().await.unwrap();
        let (_, _, _) = accepter.accept().await.unwrap();
    }

    // -------------------------------------------------------------------
    // open_auto vectored write (AsyncWrite::write_vectored classification)
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn open_auto_vectored_small_total_interactive() {
        let (srv_int, mut cli_int) =
            make_session_pair(Initiation::Server, Initiation::Client).await;
        let (srv_bulk, _cli_bulk) = make_session_pair(Initiation::Server, Initiation::Client).await;

        let liveness = Liveness::new();
        let opener = DualStreamOpener::new(srv_int.opener, srv_bulk.opener, liveness);
        let (_reader, mut writer) = opener.open_auto();

        // Vectored write: 1000 + 1000 = 2000 (≤ threshold) → interactive
        let bufs = [
            io::IoSlice::new(&[0x01u8; 1000]),
            io::IoSlice::new(&[0x02u8; 1000]),
        ];
        let n = writer.write_vectored(&bufs).await.unwrap();
        assert_eq!(n, 2000);

        let int_res =
            tokio::time::timeout(Duration::from_millis(500), cli_int.accepter.accept()).await;
        assert!(
            int_res.is_ok(),
            "vectored write ≤ threshold should go to interactive"
        );
    }

    // -------------------------------------------------------------------
    // DualMuxAccepter: hello_deadline
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn acceptor_hello_deadline_expires() {
        let (c2s, _s2c) = duplex(64);
        let (srv_r, srv_w) = tokio::io::split(c2s);

        let result =
            spawn_dual_mux_acceptor(srv_r, srv_w, srv_config(), Duration::from_millis(10)).await;
        assert!(matches!(result, Err(DualMuxError::HelloDeadline)));
    }

    // -------------------------------------------------------------------
    // Nonce mismatch rejection
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn nonce_mismatch_rejected() {
        let nonce_a = PairingNonce([0x01u8; PAIRING_NONCE_LEN]);
        let nonce_b = PairingNonce([0x02u8; PAIRING_NONCE_LEN]);

        let (c2s, s2c) = duplex(64);
        let (int_r, mut int_w) = tokio::io::split(c2s);
        let (bulk_r, mut bulk_w) = tokio::io::split(s2c);

        write_lane_hello(&mut int_w, LaneClass::Interactive, nonce_a)
            .await
            .unwrap();
        write_lane_hello(&mut bulk_w, LaneClass::Bulk, nonce_b)
            .await
            .unwrap();

        let mut set = JoinSet::new();
        let (_, _, pending_int) =
            spawn_dual_mux_acceptor(int_r, duplex(1).1, srv_config(), Duration::from_secs(1))
                .await
                .unwrap();
        let (_, _, pending_bulk) =
            spawn_dual_mux_acceptor(bulk_r, duplex(1).1, srv_config(), Duration::from_secs(1))
                .await
                .unwrap();

        let result = complete_pairing(pending_int, pending_bulk, &mut set);
        assert!(matches!(result, Err(DualMuxError::NonceMismatch)));
    }

    // -------------------------------------------------------------------
    // failed-open on dead lane
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn failed_open_on_dead_lane() {
        let (srv_int, _cli_int) = make_session_pair(Initiation::Server, Initiation::Client).await;
        let (srv_bulk, _cli_bulk) = make_session_pair(Initiation::Server, Initiation::Client).await;

        let liveness = Liveness::new();
        liveness.kill();

        let opener = DualStreamOpener::new(srv_int.opener, srv_bulk.opener, liveness);
        let result = opener.open(LaneClass::Interactive).await;
        assert!(matches!(result, Err(DualStreamOpenError::LaneDead)));
    }

    // -------------------------------------------------------------------
    // Joint liveness via spawn_dual_mux_paired_supervised: killing one
    // lane's spawner kills the shared Liveness, so opens on the
    // surviving lane fail (rather than hanging).
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn paired_supervised_kills_pair_on_lane_death() {
        // Build two lanes with their spawners accessible so we can kill
        // one lane and observe the supervisor propagate the death.
        let (int_c2s, int_s2c) = duplex(32768);
        let (bulk_c2s, bulk_s2c) = duplex(32768);
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
        let mut int_spawner = JoinSet::new();
        let (int_op, _int_srv_acc) =
            spawn_mux_no_reconnection(int_srv_r, int_srv_w, srv_cfg.clone(), &mut int_spawner);
        let mut bulk_spawner = JoinSet::new();
        let (bulk_op, _bulk_srv_acc) =
            spawn_mux_no_reconnection(bulk_srv_r, bulk_srv_w, srv_cfg, &mut bulk_spawner);

        let mut cli_int = JoinSet::new();
        let (_, int_cli_acc) =
            spawn_mux_no_reconnection(int_cli_r, int_cli_w, cli_cfg.clone(), &mut cli_int);
        let mut cli_bulk = JoinSet::new();
        let (_, bulk_cli_acc) =
            spawn_mux_no_reconnection(bulk_cli_r, bulk_cli_w, cli_cfg, &mut cli_bulk);

        let mut supervisor = JoinSet::new();
        let (opener, _accepter) = spawn_dual_mux_paired_supervised(
            int_op,
            int_cli_acc,
            int_spawner,
            bulk_op,
            bulk_cli_acc,
            bulk_spawner,
            &mut supervisor,
        );

        // Drop the client-side lanes so the server-side detects a dead
        // peer via the receive deadline (heartbeat=1s, ~4x deadline).
        drop(cli_int);
        drop(cli_bulk);

        // Wait long enough for the receive deadline to fire on both lanes.
        tokio::time::sleep(Duration::from_secs(6)).await;

        // Opens on either lane must now fail (LaneDead), not hang.
        let result =
            tokio::time::timeout(Duration::from_secs(2), opener.open(LaneClass::Bulk)).await;
        assert!(
            matches!(result, Ok(Err(DualStreamOpenError::LaneDead))),
            "surviving lane must report LaneDead after pair death, got {result:?}"
        );
        let _ = supervisor;
    }

    // -------------------------------------------------------------------
    // Birth heartbeat widens the first-receive deadline to steady.
    //
    // Regression: the old reader only dropped `first_receive_deadline`
    // after `recv()` RETURNED a message, but `recv()` swallows heartbeat
    // frames (recv_pkt returns Ok(None) for Header::Heartbeat and loops
    // internally), so a heartbeat-only birth never returned from recv(),
    // the short deadline was never cleared, and the peer's next periodic
    // heartbeat (heartbeat_interval away) missed the short window. The
    // fix moves the "switch to steady" to per-recv_pkt granularity: any
    // packet (heartbeat or data) widens the deadline to steady
    // immediately.
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn first_receive_deadline_widens_after_birth_heartbeat() {
        use crate::serve::spawn_mux_no_reconnection_with_first_receive_deadline;

        // Server side: short first-receive deadline (100 ms) but a
        // heartbeat_interval (400 ms) longer than it. The steady
        // deadline is heartbeat_interval * RECEIVE_DEADLINE_INTERVALS
        // (4) = 1600 ms.
        let first_receive_deadline = Duration::from_millis(100);
        let heartbeat_interval = Duration::from_millis(400);

        // Build a transport where we keep the write half feeding the
        // server's reader so we can inject raw heartbeat frames.
        // `duplex` returns (a, b): writing to b is read from a.
        let (server_side, injector_side) = tokio::io::duplex(8192);
        // The server owns `server_side` (both read + write). We keep
        // `injector_side` and split it: write half injects frames to
        // the server reader; read half drains server output so its
        // writer doesn't break the pipe (must stay alive).
        let (server_r, server_w) = tokio::io::split(server_side);
        let (injector_r, injector_w) = tokio::io::split(injector_side);
        // Keep the injector read half alive so server writes don't get
        // a broken pipe. We don't need to actually read from it; the
        // 8192-byte buffer is plenty for the test duration.
        let _injector_r = injector_r;
        let mut injector_w = injector_w;

        // Spawn the server mux with the short first-receive deadline.
        let mut srv_spawner: JoinSet<MuxError> = JoinSet::new();
        let (_srv_opener, _srv_accepter) = spawn_mux_no_reconnection_with_first_receive_deadline(
            server_r,
            server_w,
            MuxConfig {
                initiation: Initiation::Server,
                heartbeat_interval,
                frame_reassembly: false,
            },
            first_receive_deadline,
            &mut srv_spawner,
        );

        // Write the birth heartbeat — the first frame the server sees.
        // After this, the server must widen to the steady deadline.
        write_birth_heartbeat(&mut injector_w).await.unwrap();

        // Now send nothing for 250 ms — longer than the 100 ms
        // first-receive deadline, but less than the 1600 ms steady
        // deadline. With the bug, the server's reader would still be on
        // the 100 ms deadline at this point (the heartbeat was
        // swallowed, recv() never returned) and would time out here.
        tokio::time::sleep(Duration::from_millis(250)).await;

        // At this point the buggy reader would already have produced a
        // "receive deadline" error (100 ms after the birth heartbeat,
        // i.e. ~150 ms ago). Drain any completed results without
        // blocking and assert none of them is a receive-deadline
        // timeout. A healthy session has no completed tasks yet.
        //
        // try_join_next is non-blocking: returns None if nothing has
        // finished. We use a tight loop with try_join_next so we don't
        // sleep past the point where the bug would be visible.
        let mut saw_timeout = false;
        while let Some(res) = srv_spawner.try_join_next() {
            if let Ok(MuxError::IoReader(ref e)) = res {
                if e.to_string().contains("receive deadline") {
                    saw_timeout = true;
                }
            }
        }
        assert!(
            !saw_timeout,
            "server session timed out with 'receive deadline' during the \
             250 ms gap — the birth heartbeat did not widen the deadline \
             (bug present)"
        );

        // Send a normal-interval heartbeat to prove the session is still
        // alive on the widened (steady) deadline. This frame is only
        // delivered if the reader is still running (i.e. it did not time
        // out during the gap).
        write_birth_heartbeat(&mut injector_w).await.unwrap();

        // Give the reader a moment to process the second heartbeat, then
        // assert the session is still alive (no completed tasks). A
        // receive-deadline timeout here would mean the reader died
        // before the second heartbeat arrived.
        tokio::time::sleep(Duration::from_millis(50)).await;
        while let Some(res) = srv_spawner.try_join_next() {
            if let Ok(MuxError::IoReader(ref e)) = res {
                if e.to_string().contains("receive deadline") {
                    saw_timeout = true;
                }
            }
        }
        assert!(
            !saw_timeout,
            "server session timed out with 'receive deadline' — the \
             birth heartbeat did not widen the deadline (bug present)"
        );

        // Clean up.
        srv_spawner.abort_all();
    }

    // -------------------------------------------------------------------
    // spawn_dual_mux_acceptor: hello_deadline returns despite a writer
    // whose poll_shutdown never readies.
    //
    // Regression: the old rejection arms ran
    // `let _ = writer.shutdown().await;` before returning. If the
    // writer's `AsyncWrite::poll_shutdown` stays `Poll::Pending`
    // indefinitely (a backpressured or never-ready peer), that await
    // hangs forever, defeating the hello_deadline and leaking the
    // accept. The fix drops the writer on rejection instead of awaiting
    // a graceful shutdown.
    // -------------------------------------------------------------------

    /// A writer whose `poll_shutdown` always returns `Poll::Pending`,
    /// simulating a backpressured / never-ready peer. Writes are
    /// discarded so `write_all` doesn't interfere with the test.
    struct PendingShutdownWriter;

    impl AsyncWrite for PendingShutdownWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            // Never readies — this is the condition that used to hang
            // the rejection path.
            Poll::Pending
        }
    }

    /// A reader that never yields any bytes, so `read_lane_hello`
    /// blocks until the hello_deadline elapses.
    struct NeverReader;

    impl AsyncRead for NeverReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn hello_deadline_returns_despite_pending_shutdown() {
        let hello_deadline = Duration::from_millis(50);

        let start = std::time::Instant::now();
        let result = spawn_dual_mux_acceptor(
            NeverReader,
            PendingShutdownWriter,
            srv_config(),
            hello_deadline,
        )
        .await;
        let elapsed = start.elapsed();

        assert!(
            matches!(result, Err(DualMuxError::HelloDeadline)),
            "expected Err(HelloDeadline), got {result:?}"
        );
        // Must NOT hang: it returns within a small multiple of
        // hello_deadline. With the bug, `writer.shutdown().await` on
        // `PendingShutdownWriter` would never complete, so this test
        // would hang past the timeout and fail (or never return).
        assert!(
            elapsed < hello_deadline * 3,
            "returned in {elapsed:?}, expected < 3× hello_deadline \
             ({:?}); a hang would blow past this bound",
            hello_deadline * 3
        );
    }

    #[test]
    fn aggregate_dual_lane_result_preserves_trigger_context() {
        let result = aggregate_dual_lane_result(
            LaneClass::Bulk,
            Some(Ok(MuxError::TaskStopped {
                task: "central_io_reader",
            })),
        );
        match result {
            MuxError::DualLane {
                lane,
                peer_lane_aborted,
                source,
            } => {
                assert_eq!(lane, LaneClass::Bulk);
                assert!(peer_lane_aborted);
                assert!(matches!(
                    *source,
                    MuxError::TaskStopped {
                        task: "central_io_reader"
                    }
                ));
            }
            other => panic!("expected aggregate dual-lane error, got {other:?}"),
        }
    }
}
