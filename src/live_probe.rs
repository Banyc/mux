//! Stall-localisation probe for the interactive-path liveness soak.
//!
//! A liveness stall is a task parked on a wake that never arrives, and from
//! outside the session the parked stage is invisible: the same symptom (a
//! cycle that never completes) looks identical whether the egress queue lost a
//! ready mark, the transport write never returned, or the receiving side
//! stopped draining. The egress pipeline therefore publishes a handful of
//! relaxed counters at the points where a wake is expected, and the soak
//! prints them at a stall verdict.
//!
//! The decisive reading is [`EgressTotals::parks_with_queued_work`]. The mux
//! egress queue parks only while it holds no cached head, and it drains every
//! ready token before parking, so a park that leaves a message sitting in a
//! per-token queue is proof it parked on a message it can never be woken for.
//! [`EgressTotals::last_census`] then says which way: `unmarked_queues > 0` is
//! a lost ready mark (the send that promises the wake never published the
//! signal the scan looks for), while `unmarked_queues == 0` with
//! `queued_messages > 0` is a scan that stopped short of a marked token.
//!
//! A park with `queued_messages == 0` says the opposite: every per-stream
//! queue is empty, so whatever is stuck is downstream of the fair queue —
//! read [`EgressTotals::transport_write_in_flight`], which is set exactly
//! while the writer task is inside a transport write, to separate a writer
//! blocked on a full transport from a peer that stopped draining.
//!
//! The probe is a report instrument: it asserts nothing, gates nothing, and
//! exists only so that a stall names its own stage instead of being guessed
//! at. Costs are relaxed atomic increments at egress-frame, egress-park and
//! transport-write granularity, never per payload byte.

use std::{
    collections::HashMap,
    sync::{
        LazyLock, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::protocol::StreamId;

pub use crate::fair_queue::ReceiverCensus;

/// Nothing known yet: no egress task has entered the loop.
const STAGE_UNKNOWN: u64 = 0;
/// Parked in the writer task's `select!` with no frame to send.
const STAGE_SELECT: u64 = 1;
/// Inside `io_writer.send_*`, i.e. a transport write that has not returned.
const STAGE_TRANSPORT_WRITE: u64 = 2;

static STAGE: AtomicU64 = AtomicU64::new(STAGE_UNKNOWN);
static PARKS: AtomicU64 = AtomicU64::new(0);
static PARKS_WITH_QUEUED_WORK: AtomicU64 = AtomicU64::new(0);
static PARKS_WITH_UNMARKED_QUEUES: AtomicU64 = AtomicU64::new(0);
static DISPATCHED: AtomicU64 = AtomicU64::new(0);
static FRAMES_DATA: AtomicU64 = AtomicU64::new(0);
static FRAMES_CONTROL: AtomicU64 = AtomicU64::new(0);
static FRAMES_CLOSE_WRITE: AtomicU64 = AtomicU64::new(0);
static LAST_MARKED_TOKENS: AtomicU64 = AtomicU64::new(0);
static LAST_STALE_TOKENS: AtomicU64 = AtomicU64::new(0);
static LAST_QUEUED_MESSAGES: AtomicU64 = AtomicU64::new(0);
static LAST_UNMARKED_QUEUES: AtomicU64 = AtomicU64::new(0);

// ─── receive-side pipeline ─────────────────────────────────────────────────
//
// The egress counters above say whether a frame left the sender. These say how
// far it then travelled: decoded by the peer's central reader, handed to the
// peer's control loop, applied to the stream table, and finally pushed into
// the receiving stream's read queue. Comparing the two sets at a stall names
// the stage that swallowed the frame instead of leaving it to be inferred
// from which end looked stuck.

static READ_FRAMES_DATA: AtomicU64 = AtomicU64::new(0);
static READ_FRAMES_CONTROL: AtomicU64 = AtomicU64::new(0);
static READ_FRAMES_CLOSE_WRITE: AtomicU64 = AtomicU64::new(0);
static CONTROL_HANDLED_DATA: AtomicU64 = AtomicU64::new(0);
static CONTROL_HANDLED_OPEN: AtomicU64 = AtomicU64::new(0);
static CONTROL_HANDLED_CLOSE_READ: AtomicU64 = AtomicU64::new(0);
static CONTROL_HANDLED_CLOSE_WRITE: AtomicU64 = AtomicU64::new(0);
static PEER_WRITE_CLOSE_APPLIED: AtomicU64 = AtomicU64::new(0);
static PEER_WRITE_CLOSE_IGNORED: AtomicU64 = AtomicU64::new(0);
static DISPATCHED_TO_READER: AtomicU64 = AtomicU64::new(0);
static READ_QUEUE_FULL: AtomicU64 = AtomicU64::new(0);
static READER_FINISHED: AtomicU64 = AtomicU64::new(0);
static STREAM_READ_PUSHED: AtomicU64 = AtomicU64::new(0);
static STREAM_READ_ABSORBED: AtomicU64 = AtomicU64::new(0);
static STREAM_READ_POPPED: AtomicU64 = AtomicU64::new(0);
static STREAM_TERMINAL_PUSHED: AtomicU64 = AtomicU64::new(0);
static STREAM_TERMINAL_POPPED: AtomicU64 = AtomicU64::new(0);
static READER_EOF_OBSERVED: AtomicU64 = AtomicU64::new(0);

/// The central reader decoded a frame of the named kind.
pub(crate) fn note_frame_read(kind: EgressFrameKind) {
    match kind {
        EgressFrameKind::Data => READ_FRAMES_DATA.fetch_add(1, Ordering::Relaxed),
        EgressFrameKind::Control => READ_FRAMES_CONTROL.fetch_add(1, Ordering::Relaxed),
        EgressFrameKind::CloseWrite => READ_FRAMES_CLOSE_WRITE.fetch_add(1, Ordering::Relaxed),
    };
}

/// The control loop took a frame off the reader's channel. Split by kind
/// because the two halves of a teardown travel different paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandledFrame {
    Data,
    Open,
    CloseRead,
    CloseWrite,
}

pub(crate) fn note_frame_handled(frame: HandledFrame) {
    match frame {
        HandledFrame::Data => CONTROL_HANDLED_DATA.fetch_add(1, Ordering::Relaxed),
        HandledFrame::Open => CONTROL_HANDLED_OPEN.fetch_add(1, Ordering::Relaxed),
        HandledFrame::CloseRead => CONTROL_HANDLED_CLOSE_READ.fetch_add(1, Ordering::Relaxed),
        HandledFrame::CloseWrite => CONTROL_HANDLED_CLOSE_WRITE.fetch_add(1, Ordering::Relaxed),
    };
}

/// The peer's `CloseWrite` reached a stream and closed its read side.
/// `applied == false` means it was dropped, either for a stream this side
/// never materialised or for one that was already closed.
pub(crate) fn note_peer_write_close(applied: bool) {
    if applied {
        PEER_WRITE_CLOSE_APPLIED.fetch_add(1, Ordering::Relaxed);
    } else {
        PEER_WRITE_CLOSE_IGNORED.fetch_add(1, Ordering::Relaxed);
    }
}

/// A data frame reached the receiving stream's dispatcher.
pub(crate) fn note_dispatched_to_reader() {
    DISPATCHED_TO_READER.fetch_add(1, Ordering::Relaxed);
}

/// The receiving stream's dispatcher refused a data frame (read queue full).
pub(crate) fn note_read_queue_full() {
    READ_QUEUE_FULL.fetch_add(1, Ordering::Relaxed);
}

/// A stream dispatcher delivered its terminal (Fin or Error) to the reader.
pub(crate) fn note_reader_finished() {
    READER_FINISHED.fetch_add(1, Ordering::Relaxed);
}

// ─── per-stream byte trace (opt-in) ───────────────────────────────────────
//
// The counters above are process-wide, so they can say a stage lost a frame
// but never which stream lost it, and both sessions of one mux pair run in
// this process. The trace below is keyed by `(stream id, end)`, where `end`
// distinguishes the side that *owns* the wire id from the side that accepted
// it, so one entry covers exactly one read path. It is enabled by the soak
// and costs one relaxed load everywhere else.

static STREAM_TRACE_ENABLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
static STREAM_TRACE: LazyLock<Mutex<HashMap<(StreamId, bool), StreamTrace>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// One stream end's byte ledger: what the peer's session handed to this end's
/// reader queue, and what this end's reader handed to its application. The
/// gap between them is what is parked in the read path, and it is attributed
/// to a stream id and an end.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamTrace {
    /// Bytes this end's session emitted towards the peer for the stream.
    pub egress: u64,
    /// Bytes this end's session pushed into the reader queue for the stream.
    pub dispatched: u64,
    /// Bytes this end's reader handed to its application.
    pub delivered: u64,
    /// Times this end's reader observed EOF.
    pub eof_observed: u64,
    /// This end's reader is gone: bytes left in its queue are read by nobody.
    pub reader_dropped: bool,
}

pub fn enable_stream_trace() {
    STREAM_TRACE_ENABLED.store(true, Ordering::Relaxed);
}

pub fn reset_stream_trace() {
    if STREAM_TRACE_ENABLED.load(Ordering::Relaxed) {
        STREAM_TRACE.lock().unwrap().clear();
    }
}

fn with_trace(stream_id: StreamId, is_owner: bool, update: impl FnOnce(&mut StreamTrace)) {
    if !STREAM_TRACE_ENABLED.load(Ordering::Relaxed) {
        return;
    }
    let mut trace = STREAM_TRACE.lock().unwrap();
    update(trace.entry((stream_id, is_owner)).or_default());
}

pub(crate) fn note_stream_egress(stream_id: StreamId, is_owner: bool, bytes: usize) {
    with_trace(stream_id, is_owner, |entry| {
        entry.egress += bytes as u64;
    });
}

pub(crate) fn note_stream_dispatched(stream_id: StreamId, is_owner: bool, bytes: usize) {
    with_trace(stream_id, is_owner, |entry| {
        entry.dispatched += bytes as u64;
    });
}

pub(crate) fn note_stream_delivered(stream_id: StreamId, is_owner: bool, bytes: usize) {
    with_trace(stream_id, is_owner, |entry| {
        entry.delivered += bytes as u64;
    });
}

pub(crate) fn note_stream_eof(stream_id: StreamId, is_owner: bool) {
    with_trace(stream_id, is_owner, |entry| {
        entry.eof_observed += 1;
    });
}

pub(crate) fn note_stream_reader_dropped(stream_id: StreamId, is_owner: bool) {
    with_trace(stream_id, is_owner, |entry| {
        entry.reader_dropped = true;
    });
}

/// Every traced end with its byte ledger. With the trace reset per cycle the
/// map holds one cycle's streams, so the whole ledger is small enough to
/// print: a stall then shows, per end, whether its bytes were dispatched but
/// not delivered (parked in that end's read path) or never dispatched at all
/// (parked upstream of it).
#[derive(Debug, Clone, Default)]
pub struct StreamTraceReport {
    pub ends: Vec<(StreamId, bool, StreamTrace)>,
    pub egress_total: u64,
    pub dispatched_total: u64,
    pub delivered_total: u64,
    pub unbalanced: usize,
}

pub fn stream_trace_report() -> StreamTraceReport {
    let trace = STREAM_TRACE.lock().unwrap();
    let mut ends: Vec<(StreamId, bool, StreamTrace)> = trace
        .iter()
        .map(|((id, is_owner), entry)| (*id, *is_owner, *entry))
        .collect();
    ends.sort_by_key(|(id, is_owner, _)| (*id, *is_owner));
    let dispatched_total = ends.iter().map(|(_, _, e)| e.dispatched).sum();
    let delivered_total = ends.iter().map(|(_, _, e)| e.delivered).sum();
    let egress_total: u64 = ends.iter().map(|(_, _, e)| e.egress).sum();
    let unbalanced = ends
        .iter()
        .filter(|(_, _, e)| e.dispatched != e.delivered)
        .count();
    StreamTraceReport {
        ends,
        egress_total,
        dispatched_total,
        delivered_total,
        unbalanced,
    }
}

impl std::fmt::Display for StreamTraceReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "stream_trace(ends={} egress_total={} dispatched_total={} delivered_total={} unbalanced={}):",
            self.ends.len(),
            self.egress_total,
            self.dispatched_total,
            self.delivered_total,
            self.unbalanced,
        )?;
        for (id, is_owner, entry) in &self.ends {
            let end = if *is_owner { "owner" } else { "peer" };
            write!(
                f,
                " [{id}/{end} egress={} dispatched={} delivered={} eof={} reader_dropped={}]",
                entry.egress,
                entry.dispatched,
                entry.delivered,
                entry.eof_observed,
                entry.reader_dropped,
            )?;
        }
        Ok(())
    }
}

/// A data message entered the receiving stream's read queue.
pub(crate) fn note_stream_read_pushed() {
    STREAM_READ_PUSHED.fetch_add(1, Ordering::Relaxed);
}

/// A data message was absorbed because the receiving stream's reader is gone.
pub(crate) fn note_stream_read_absorbed() {
    STREAM_READ_ABSORBED.fetch_add(1, Ordering::Relaxed);
}

/// The receiving stream's reader dequeued a message.
pub(crate) fn note_stream_read_popped(terminal: bool) {
    if terminal {
        STREAM_TERMINAL_POPPED.fetch_add(1, Ordering::Relaxed);
    } else {
        STREAM_READ_POPPED.fetch_add(1, Ordering::Relaxed);
    }
}

/// A terminal (Fin or Error) entered the receiving stream's read queue.
pub(crate) fn note_stream_terminal_pushed() {
    STREAM_TERMINAL_PUSHED.fetch_add(1, Ordering::Relaxed);
}

/// A `StreamReader` turned a terminal into the `Ok(0)` its caller reads as
/// EOF. A send that is recorded as pushed but never as observed is a reader
/// parked on a message already in its queue.
pub(crate) fn note_reader_eof_observed() {
    READER_EOF_OBSERVED.fetch_add(1, Ordering::Relaxed);
}

/// The writer task is between frames; it reports this before it parks in its
/// `select!`, so a stall that reads this stage is a writer parked with no
/// frame to send rather than one blocked in a transport write.
pub(crate) fn note_egress_select() {
    STAGE.store(STAGE_SELECT, Ordering::Relaxed);
}

/// The writer task has taken a frame from one of its channels and is about to
/// hand it to the transport. The stage stays here until the write returns, so
/// a stall that reads it is a transport write that never completed.
pub(crate) fn note_egress_frame(kind: EgressFrameKind) {
    STAGE.store(STAGE_TRANSPORT_WRITE, Ordering::Relaxed);
    match kind {
        EgressFrameKind::Data => FRAMES_DATA.fetch_add(1, Ordering::Relaxed),
        EgressFrameKind::Control => FRAMES_CONTROL.fetch_add(1, Ordering::Relaxed),
        EgressFrameKind::CloseWrite => FRAMES_CLOSE_WRITE.fetch_add(1, Ordering::Relaxed),
    };
}

/// The transport write returned.
pub(crate) fn note_egress_frame_done() {
    STAGE.store(STAGE_SELECT, Ordering::Relaxed);
}

/// The egress queue delivered a message to the writer task.
pub(crate) fn note_egress_dispatched() {
    DISPATCHED.fetch_add(1, Ordering::Relaxed);
}

/// The egress queue is about to park. `census` is the fair queue's ready set
/// and per-token queue lengths at that instant; it is published so a stall can
/// be read without re-deriving the state the parked task holds.
pub(crate) fn note_egress_park(census: ReceiverCensus) {
    PARKS.fetch_add(1, Ordering::Relaxed);
    if census.queued_messages != 0 {
        PARKS_WITH_QUEUED_WORK.fetch_add(1, Ordering::Relaxed);
    }
    if census.unmarked_queues != 0 {
        PARKS_WITH_UNMARKED_QUEUES.fetch_add(1, Ordering::Relaxed);
    }
    LAST_MARKED_TOKENS.store(census.marked_tokens as u64, Ordering::Relaxed);
    LAST_STALE_TOKENS.store(census.stale_tokens as u64, Ordering::Relaxed);
    LAST_QUEUED_MESSAGES.store(census.queued_messages as u64, Ordering::Relaxed);
    LAST_UNMARKED_QUEUES.store(census.unmarked_queues as u64, Ordering::Relaxed);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EgressFrameKind {
    Data,
    Control,
    CloseWrite,
}

/// Cumulative egress counters plus the census published by the most recent
/// park. Deltas across one soak cycle are what a stall verdict reports: a
/// park with queued work that first appears inside the stalled cycle is that
/// cycle's defect, not noise from an earlier one.
#[derive(Debug, Clone, Copy)]
pub struct EgressTotals {
    pub parks: u64,
    pub parks_with_queued_work: u64,
    pub parks_with_unmarked_queues: u64,
    pub dispatched: u64,
    pub data_frames: u64,
    pub control_frames: u64,
    pub close_write_frames: u64,
    pub last_census: ReceiverCensus,
    /// True while a transport write has not returned: the writer task is
    /// blocked on the transport rather than parked on a lost wake.
    pub transport_write_in_flight: bool,
}

fn egress_totals() -> EgressTotals {
    EgressTotals {
        parks: PARKS.load(Ordering::Relaxed),
        parks_with_queued_work: PARKS_WITH_QUEUED_WORK.load(Ordering::Relaxed),
        parks_with_unmarked_queues: PARKS_WITH_UNMARKED_QUEUES.load(Ordering::Relaxed),
        dispatched: DISPATCHED.load(Ordering::Relaxed),
        data_frames: FRAMES_DATA.load(Ordering::Relaxed),
        control_frames: FRAMES_CONTROL.load(Ordering::Relaxed),
        close_write_frames: FRAMES_CLOSE_WRITE.load(Ordering::Relaxed),
        last_census: ReceiverCensus {
            marked_tokens: LAST_MARKED_TOKENS.load(Ordering::Relaxed) as usize,
            stale_tokens: LAST_STALE_TOKENS.load(Ordering::Relaxed) as usize,
            queued_messages: LAST_QUEUED_MESSAGES.load(Ordering::Relaxed) as usize,
            unmarked_queues: LAST_UNMARKED_QUEUES.load(Ordering::Relaxed) as usize,
        },
        transport_write_in_flight: STAGE.load(Ordering::Relaxed) == STAGE_TRANSPORT_WRITE,
    }
}

impl EgressTotals {
    /// The delta between an earlier snapshot and this one, keeping this
    /// snapshot's last-park census and stage.
    pub fn since(&self, earlier: &EgressTotals) -> EgressTotals {
        EgressTotals {
            parks: self.parks - earlier.parks,
            parks_with_queued_work: self.parks_with_queued_work - earlier.parks_with_queued_work,
            parks_with_unmarked_queues: self.parks_with_unmarked_queues
                - earlier.parks_with_unmarked_queues,
            dispatched: self.dispatched - earlier.dispatched,
            data_frames: self.data_frames - earlier.data_frames,
            control_frames: self.control_frames - earlier.control_frames,
            close_write_frames: self.close_write_frames - earlier.close_write_frames,
            last_census: self.last_census,
            transport_write_in_flight: self.transport_write_in_flight,
        }
    }
}

impl std::fmt::Display for EgressTotals {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "egress: parks={} parks_with_queued_work={} parks_with_unmarked_queues={} \
             dispatched={} frames(data={} control={} close_write={}) \
             last_park(ready_marked={} ready_stale={} queued_msgs={} unmarked_queues={}) \
             transport_write_in_flight={}",
            self.parks,
            self.parks_with_queued_work,
            self.parks_with_unmarked_queues,
            self.dispatched,
            self.data_frames,
            self.control_frames,
            self.close_write_frames,
            self.last_census.marked_tokens,
            self.last_census.stale_tokens,
            self.last_census.queued_messages,
            self.last_census.unmarked_queues,
            self.transport_write_in_flight,
        )
    }
}

/// Cumulative receive-side counters. A frame the egress counters say was
/// emitted, but that never appears in the stage it was addressed to, is the
/// frame the stall is parked on.
#[derive(Debug, Clone, Copy, Default)]
pub struct PipelineTotals {
    pub read_data: u64,
    pub read_control: u64,
    pub read_close_write: u64,
    pub handled_data: u64,
    pub handled_open: u64,
    pub handled_close_read: u64,
    pub handled_close_write: u64,
    pub peer_write_close_applied: u64,
    pub peer_write_close_ignored: u64,
    pub dispatched_to_reader: u64,
    pub read_queue_full: u64,
    pub reader_finished: u64,
    /// Data messages entered a receiving stream's read queue.
    pub stream_read_pushed: u64,
    /// Data messages absorbed because the reader was already gone.
    pub stream_read_absorbed: u64,
    /// Messages a receiving stream's reader dequeued. `pushed + terminal -
    /// popped - absorbed` is how many data messages are sitting in a live
    /// read queue; a soak stall whose byte gap equals that residue has parked
    /// on a reader that was never woken for a message already in its queue.
    pub stream_read_popped: u64,
    /// Terminals entered a receiving stream's read queue.
    pub stream_terminal_pushed: u64,
    /// Terminals a reader dequeued.
    pub stream_terminal_popped: u64,
    /// Readers that turned a terminal into `Ok(0)` for their caller.
    pub reader_eof_observed: u64,
}

impl PipelineTotals {
    pub fn since(&self, earlier: &PipelineTotals) -> PipelineTotals {
        PipelineTotals {
            read_data: self.read_data - earlier.read_data,
            read_control: self.read_control - earlier.read_control,
            read_close_write: self.read_close_write - earlier.read_close_write,
            handled_data: self.handled_data - earlier.handled_data,
            handled_open: self.handled_open - earlier.handled_open,
            handled_close_read: self.handled_close_read - earlier.handled_close_read,
            handled_close_write: self.handled_close_write - earlier.handled_close_write,
            peer_write_close_applied: self.peer_write_close_applied
                - earlier.peer_write_close_applied,
            peer_write_close_ignored: self.peer_write_close_ignored
                - earlier.peer_write_close_ignored,
            dispatched_to_reader: self.dispatched_to_reader - earlier.dispatched_to_reader,
            read_queue_full: self.read_queue_full - earlier.read_queue_full,
            reader_finished: self.reader_finished - earlier.reader_finished,
            stream_read_pushed: self.stream_read_pushed - earlier.stream_read_pushed,
            stream_read_absorbed: self.stream_read_absorbed - earlier.stream_read_absorbed,
            stream_read_popped: self.stream_read_popped - earlier.stream_read_popped,
            stream_terminal_pushed: self.stream_terminal_pushed - earlier.stream_terminal_pushed,
            stream_terminal_popped: self.stream_terminal_popped - earlier.stream_terminal_popped,
            reader_eof_observed: self.reader_eof_observed - earlier.reader_eof_observed,
        }
    }
}

impl PipelineTotals {
    /// Data messages a live reader still holds: pushed into a queue that was
    /// not closed and not dequeued. Non-zero is a stall on the read path.
    pub fn undelivered_data(&self) -> u64 {
        self.stream_read_pushed
            .saturating_sub(self.stream_read_popped + self.stream_read_absorbed)
    }
    /// Terminals a live reader still holds.
    pub fn undelivered_terminal(&self) -> u64 {
        self.stream_terminal_pushed
            .saturating_sub(self.stream_terminal_popped + self.stream_read_absorbed)
    }
}

impl std::fmt::Display for PipelineTotals {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ingress: read(data={} control={} close_write={}) \
             handled(data={} open={} close_read={} close_write={}) \
             peer_write_close(applied={} ignored={}) dispatched_to_reader={} \
             read_queue_full={} reader_finished={} \
             stream_read(pushed={} absorbed={} popped={} undelivered={}) \
             stream_terminal(pushed={} popped={} undelivered={}) reader_eof_observed={}",
            self.read_data,
            self.read_control,
            self.read_close_write,
            self.handled_data,
            self.handled_open,
            self.handled_close_read,
            self.handled_close_write,
            self.peer_write_close_applied,
            self.peer_write_close_ignored,
            self.dispatched_to_reader,
            self.read_queue_full,
            self.reader_finished,
            self.stream_read_pushed,
            self.stream_read_absorbed,
            self.stream_read_popped,
            self.undelivered_data(),
            self.stream_terminal_pushed,
            self.stream_terminal_popped,
            self.undelivered_terminal(),
            self.reader_eof_observed,
        )
    }
}

// ─── per-session stream-table ledger ───────────────────────────────────────
//
// The stream table is the admission resource: `MuxControl::open` refuses a new
// stream once the table holds `max_concurrent_streams` entries, and under
// `frame_reassembly` the peer materialises one slot per stream it opens, so a
// session that never releases a table entry stops admitting streams for the
// rest of its life while every counter above still looks healthy (the session
// is parked on nothing; it is simply full).
//
// Both sessions of a mux pair, and every pair an instrument opens, live in one
// process, so the ledger is published per session *role* rather than as one
// set of gauges: a reader can then tell which session saturated. Two sessions
// with the same role sharing a process share a slot and last-writer wins,
// which is all a stall verdict needs to read the session it names.

/// Which half of a mux pair a session is. The only attribution the ledger
/// needs: the two roles hold disjoint stream-id spaces, so a saturated peer
/// table and a saturated local table are different defects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionRole {
    Client,
    Server,
}

/// The two states a retained stream-table entry can be in, sampled when
/// admission refuses a stream (the one moment the whole table is inspected).
#[derive(Debug, Clone, Copy, Default)]
pub struct AdmissionCensus {
    /// Entries `StreamState::is_closed` already reports finished: every side
    /// has closed, so no further transition can release them. Non-zero here is
    /// a release defect, not a load effect.
    pub closed_but_retained: u64,
    /// Entries whose only outstanding side is the peer's read close: these are
    /// still live and are released when that frame arrives, so a table made of
    /// them is pressure, not a leak.
    pub awaiting_peer_read_close: u64,
}

/// One session role's stream-table ledger.
#[derive(Debug, Clone, Copy, Default)]
pub struct AdmissionLedger {
    /// Entries in the table now.
    pub stream_table_len: u64,
    /// Of those, the ones this session opened itself. The rest were
    /// materialised from the peer's frames (the invariant `local_opened ==
    /// occupied local ids` is maintained by `MuxControl::retire_stream`).
    pub local_opened_streams: u64,
    /// High-water mark of `stream_table_len`: reaching the cap here is what
    /// makes every later admission refuse.
    pub max_stream_table_len: u64,
    /// Streams inserted into the table, and streams released from it. Their
    /// gap is the table length, so the pair is what shows whether a saturated
    /// table is steady state or monotonically growing.
    pub inserted: u64,
    pub retired: u64,
    /// Admissions refused because the table was full, split by which id space
    /// asked. A peer refusal is answered by dropping the peer's frame and
    /// nothing else; a local refusal is returned to the local `open` caller.
    pub refused_peer: u64,
    pub refused_local: u64,
    /// Census at the most recent refusal.
    pub census: AdmissionCensus,
}

#[derive(Debug)]
struct LedgerSlot {
    stream_table_len: AtomicU64,
    local_opened_streams: AtomicU64,
    max_stream_table_len: AtomicU64,
    inserted: AtomicU64,
    retired: AtomicU64,
    refused_peer: AtomicU64,
    refused_local: AtomicU64,
    census_closed_but_retained: AtomicU64,
    census_awaiting_peer_read_close: AtomicU64,
}
impl LedgerSlot {
    const fn new() -> Self {
        Self {
            stream_table_len: AtomicU64::new(0),
            local_opened_streams: AtomicU64::new(0),
            max_stream_table_len: AtomicU64::new(0),
            inserted: AtomicU64::new(0),
            retired: AtomicU64::new(0),
            refused_peer: AtomicU64::new(0),
            refused_local: AtomicU64::new(0),
            census_closed_but_retained: AtomicU64::new(0),
            census_awaiting_peer_read_close: AtomicU64::new(0),
        }
    }
    fn get(&self) -> AdmissionLedger {
        AdmissionLedger {
            stream_table_len: self.stream_table_len.load(Ordering::Relaxed),
            local_opened_streams: self.local_opened_streams.load(Ordering::Relaxed),
            max_stream_table_len: self.max_stream_table_len.load(Ordering::Relaxed),
            inserted: self.inserted.load(Ordering::Relaxed),
            retired: self.retired.load(Ordering::Relaxed),
            refused_peer: self.refused_peer.load(Ordering::Relaxed),
            refused_local: self.refused_local.load(Ordering::Relaxed),
            census: AdmissionCensus {
                closed_but_retained: self.census_closed_but_retained.load(Ordering::Relaxed),
                awaiting_peer_read_close: self
                    .census_awaiting_peer_read_close
                    .load(Ordering::Relaxed),
            },
        }
    }
}

static CLIENT_LEDGER: LedgerSlot = LedgerSlot::new();
static SERVER_LEDGER: LedgerSlot = LedgerSlot::new();

impl SessionRole {
    fn slot(self) -> &'static LedgerSlot {
        match self {
            SessionRole::Client => &CLIENT_LEDGER,
            SessionRole::Server => &SERVER_LEDGER,
        }
    }
}

impl AdmissionLedger {
    /// Entries the peer's frames materialised, derived rather than tracked: it
    /// is the table minus this session's own streams.
    pub fn peer_materialised_streams(&self) -> u64 {
        self.stream_table_len
            .saturating_sub(self.local_opened_streams)
    }
}

impl std::fmt::Display for AdmissionLedger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "table={} local_opened={} peer_materialised={} max_table={} \
             inserted={} retired={} refused(peer={} local={}) \
             refusal_census(closed_but_retained={} awaiting_peer_read_close={})",
            self.stream_table_len,
            self.local_opened_streams,
            self.peer_materialised_streams(),
            self.max_stream_table_len,
            self.inserted,
            self.retired,
            self.refused_peer,
            self.refused_local,
            self.census.closed_but_retained,
            self.census.awaiting_peer_read_close,
        )
    }
}

/// Both session roles' ledgers. Printed whole rather than differenced: a gauge
/// is a state, and the state at a stall is the reading that names the defect.
#[derive(Debug, Clone, Copy, Default)]
pub struct AdmissionLedgers {
    pub client: AdmissionLedger,
    pub server: AdmissionLedger,
}

impl std::fmt::Display for AdmissionLedgers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "admission: client[{}] server[{}]",
            self.client, self.server
        )
    }
}

pub fn admission_ledgers() -> AdmissionLedgers {
    AdmissionLedgers {
        client: CLIENT_LEDGER.get(),
        server: SERVER_LEDGER.get(),
    }
}

/// A stream entered the table (`len`/`local_opened` are the post-insert
/// values).
pub(crate) fn note_stream_table(role: SessionRole, len: usize, local_opened: usize) {
    let slot = role.slot();
    slot.stream_table_len.store(len as u64, Ordering::Relaxed);
    slot.local_opened_streams
        .store(local_opened as u64, Ordering::Relaxed);
    slot.max_stream_table_len
        .fetch_max(len as u64, Ordering::Relaxed);
    slot.inserted.fetch_add(1, Ordering::Relaxed);
}

/// A stream left the table: the release side of the ledger.
pub(crate) fn note_stream_retired(role: SessionRole, len: usize, local_opened: usize) {
    let slot = role.slot();
    slot.stream_table_len.store(len as u64, Ordering::Relaxed);
    slot.local_opened_streams
        .store(local_opened as u64, Ordering::Relaxed);
    slot.retired.fetch_add(1, Ordering::Relaxed);
}

/// Admission refused a stream because the table was full, with the census of
/// what was retained at that instant. Counted where the refusal is decided, so
/// a refusal that is then swallowed still appears here.
pub(crate) fn note_admission_refused(
    role: SessionRole,
    peer_stream: bool,
    census: AdmissionCensus,
) {
    let slot = role.slot();
    if peer_stream {
        slot.refused_peer.fetch_add(1, Ordering::Relaxed);
    } else {
        slot.refused_local.fetch_add(1, Ordering::Relaxed);
    }
    slot.census_closed_but_retained
        .store(census.closed_but_retained, Ordering::Relaxed);
    slot.census_awaiting_peer_read_close
        .store(census.awaiting_peer_read_close, Ordering::Relaxed);
}

/// Both halves of the probe at one instant: what the senders emitted and what
/// the receivers recorded. A stall reports the delta of a pair across the
/// stalled cycle.
#[derive(Debug, Clone, Copy)]
pub struct Totals {
    pub egress: EgressTotals,
    pub pipeline: PipelineTotals,
    /// State, not a delta: carried through `since` so a stall verdict always
    /// prints the admission ledger of the sessions that ran the cycle.
    pub ledgers: AdmissionLedgers,
}

impl Totals {
    pub fn since(&self, earlier: &Totals) -> Totals {
        Totals {
            egress: self.egress.since(&earlier.egress),
            pipeline: self.pipeline.since(&earlier.pipeline),
            ledgers: self.ledgers,
        }
    }
}

impl std::fmt::Display for Totals {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}; {}; {}", self.egress, self.pipeline, self.ledgers)
    }
}

pub fn totals() -> Totals {
    Totals {
        egress: egress_totals(),
        pipeline: PipelineTotals {
            read_data: READ_FRAMES_DATA.load(Ordering::Relaxed),
            read_control: READ_FRAMES_CONTROL.load(Ordering::Relaxed),
            read_close_write: READ_FRAMES_CLOSE_WRITE.load(Ordering::Relaxed),
            handled_data: CONTROL_HANDLED_DATA.load(Ordering::Relaxed),
            handled_open: CONTROL_HANDLED_OPEN.load(Ordering::Relaxed),
            handled_close_read: CONTROL_HANDLED_CLOSE_READ.load(Ordering::Relaxed),
            handled_close_write: CONTROL_HANDLED_CLOSE_WRITE.load(Ordering::Relaxed),
            peer_write_close_applied: PEER_WRITE_CLOSE_APPLIED.load(Ordering::Relaxed),
            peer_write_close_ignored: PEER_WRITE_CLOSE_IGNORED.load(Ordering::Relaxed),
            dispatched_to_reader: DISPATCHED_TO_READER.load(Ordering::Relaxed),
            read_queue_full: READ_QUEUE_FULL.load(Ordering::Relaxed),
            reader_finished: READER_FINISHED.load(Ordering::Relaxed),
            stream_read_pushed: STREAM_READ_PUSHED.load(Ordering::Relaxed),
            stream_read_absorbed: STREAM_READ_ABSORBED.load(Ordering::Relaxed),
            stream_read_popped: STREAM_READ_POPPED.load(Ordering::Relaxed),
            stream_terminal_pushed: STREAM_TERMINAL_PUSHED.load(Ordering::Relaxed),
            stream_terminal_popped: STREAM_TERMINAL_POPPED.load(Ordering::Relaxed),
            reader_eof_observed: READER_EOF_OBSERVED.load(Ordering::Relaxed),
        },
        ledgers: admission_ledgers(),
    }
}
