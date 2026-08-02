use std::{
    collections::{BTreeMap, HashMap},
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use primitive::ops::ring::RingSpace;

use crate::{
    StreamReader, StreamWriter,
    central_io::{
        DeadCentralIo,
        reader::{CentralIoReadMsg, CentralIoReadRx},
        writer::{StreamWriteDataTx, WriteControlMsg, WriteControlTx, WriteDataTxPrototype},
    },
    common::Side,
    protocol::{Offset, StreamId},
    stream::{
        DeadStreamInit, StreamCloseMsg, StreamCloseTxPrototype, StreamInitHandle,
        accepter::StreamAcceptMsg,
        opener::StreamOpenMsg,
        reader::{StreamReadDataMsg, StreamReadDataTx, stream_read_data_channel},
        stream_close_channel,
        writer::LiveStreamWriter,
    },
};

#[derive(Debug)]
pub struct RunControlArgs {
    pub control: MuxControl,
    pub central_io_read_rx: CentralIoReadRx,
    pub write_control_tx: WriteControlTx,
    pub stream_init_handle: StreamInitHandle,
}
pub async fn run_control(args: RunControlArgs) -> Result<(), RunControlError> {
    let RunControlArgs {
        mut control,
        mut central_io_read_rx,
        write_control_tx,
        mut stream_init_handle,
    } = args;
    let (stream_close_tx, mut stream_close_rx) = stream_close_channel();
    let e: DeadCentralIo = loop {
        tokio::select! {
            () = write_control_tx.closed() => {
                break DeadCentralIo { side: Side::Write };
            }
            res = stream_close_rx.recv() => {
                let msg = res.unwrap();
                if let Err(e) = handle_stream_close(&mut control, &write_control_tx, msg).await {
                    break e;
                }
            }
            Ok(msg) = stream_init_handle.stream_open_rx.recv() => {
                handle_local_open(&mut control, &stream_close_tx, msg).await;
            }
            res = central_io_read_rx.recv() => {
                let msg = match res {
                    Ok(x) => x,
                    Err(e) => break e,
                };
                match handle_central_read(
                    &mut control,
                    &stream_close_tx,
                    &mut stream_init_handle,
                    &write_control_tx,
                    msg
                ).await {
                    Ok(()) => (),
                    Err(HandleCentralReadError::DeadStreamInit(_)) => continue,
                    Err(HandleCentralReadError::DeadCentralIo(e)) => break e,
                }
            }
        }
    };
    Err(RunControlError::DeadCentralIo(e, stream_init_handle))
}
async fn handle_stream_close(
    control: &mut MuxControl,
    write_control_tx: &WriteControlTx,
    msg: StreamCloseMsg,
) -> Result<(), DeadCentralIo> {
    control.local_close(msg.stream_id, msg.side);
    match msg.side {
        Side::Read => {
            write_control_tx
                .send(WriteControlMsg::CloseRead(msg.stream_id))
                .await?;
        }
        Side::Write => {
            // Dropping the per-stream data sender is the sole graceful FIN producer. Its fair queue drains accepted data before closing.
        }
    }
    Ok(())
}
#[derive(Debug)]
pub enum RunControlError {
    DeadCentralIo(DeadCentralIo, StreamInitHandle),
}

async fn handle_local_open(
    control: &mut MuxControl,
    stream_close_tx: &StreamCloseTxPrototype,
    msg: StreamOpenMsg,
) {
    let res = open_stream(control, stream_close_tx, None).await;
    let resp = match res {
        Ok((_, msg)) => Ok(msg),
        Err(e) => Err(e),
    };
    let _ = msg.stream.send(resp);
}

async fn handle_central_read(
    control: &mut MuxControl,
    stream_close_tx: &StreamCloseTxPrototype,
    stream_init_handle: &mut StreamInitHandle,
    write_control_tx: &WriteControlTx,
    msg: CentralIoReadMsg,
) -> Result<(), HandleCentralReadError> {
    match msg {
        CentralIoReadMsg::Open(stream_id) => {
            if control.is_local_opened_stream(stream_id) {
                return Ok(());
            }
            if control.stream_table.contains_key(&stream_id) {
                return Ok(());
            }
            let (_, stream) = match open_stream(control, stream_close_tx, Some(stream_id)).await {
                Ok(x) => x,
                Err(e) => match e {
                    ControlOpenError::TooManyOpenStreams(_) => panic!(),
                    ControlOpenError::DeadCentralIo(dead_central_io) => {
                        return Err(HandleCentralReadError::DeadCentralIo(dead_central_io));
                    }
                },
            };
            if let Err(e) = stream_init_handle.stream_accept_tx.try_send(stream) {
                control.clean_closed_stream(stream_id);
                return Err(HandleCentralReadError::DeadStreamInit(e));
            }
        }
        CentralIoReadMsg::Close(stream_id, side, final_offset) => {
            if control.frame_reassembly && side == Side::Write {
                if !control.stream_table.contains_key(&stream_id) {
                    if control.is_local_opened_stream(stream_id) {
                        return Ok(());
                    }
                    let res = open_stream(control, stream_close_tx, Some(stream_id)).await;
                    match res {
                        Ok((_, stream)) => {
                            if stream_init_handle
                                .stream_accept_tx
                                .try_send(stream)
                                .is_err()
                            {
                                control.clean_closed_stream(stream_id);
                                return Ok(());
                            }
                        }
                        Err(ControlOpenError::TooManyOpenStreams(_)) => {
                            return Ok(());
                        }
                        Err(ControlOpenError::DeadCentralIo(e)) => {
                            return Err(HandleCentralReadError::DeadCentralIo(e));
                        }
                    }
                }
                if control
                    .peer_close_write_with_offset(stream_id, final_offset)
                    .await
                    .is_err()
                    && control.reassembly_error_teardown(stream_id).await
                {
                    let _ = write_control_tx
                        .send(WriteControlMsg::CloseRead(stream_id))
                        .await;
                }
            } else {
                control.peer_close(stream_id, side);
            }
        }
        CentralIoReadMsg::Data(stream_id, offset, data_buf) => {
            if control.frame_reassembly {
                if !control.stream_table.contains_key(&stream_id) {
                    if control.is_local_opened_stream(stream_id) {
                        return Ok(());
                    }
                    let res = open_stream(control, stream_close_tx, Some(stream_id)).await;
                    match res {
                        Ok((_, stream)) => {
                            if stream_init_handle
                                .stream_accept_tx
                                .try_send(stream)
                                .is_err()
                            {
                                control.clean_closed_stream(stream_id);
                                return Ok(());
                            }
                        }
                        Err(ControlOpenError::TooManyOpenStreams(_)) => {
                            return Ok(());
                        }
                        Err(ControlOpenError::DeadCentralIo(e)) => {
                            return Err(HandleCentralReadError::DeadCentralIo(e));
                        }
                    }
                }
                if control
                    .ingest_reassembly(stream_id, offset, data_buf)
                    .await
                    .is_err()
                    && control.reassembly_error_teardown(stream_id).await
                {
                    let _ = write_control_tx
                        .send(WriteControlMsg::CloseRead(stream_id))
                        .await;
                }
            } else if control.try_dispatch_data(stream_id, data_buf).is_err() {
                let _ = write_control_tx
                    .send(WriteControlMsg::CloseRead(stream_id))
                    .await;
                let _ = write_control_tx
                    .send(WriteControlMsg::ForceCloseWrite(stream_id))
                    .await;
            }
        }
    }
    Ok(())
}
#[derive(Debug)]
enum HandleCentralReadError {
    DeadCentralIo(DeadCentralIo),
    DeadStreamInit(DeadStreamInit),
}
#[derive(Debug)]
struct StreamReadQueueFull;
fn try_send_data(
    dispatcher: &StreamReadDataTx,
    data: crate::central_io::DataBuf,
) -> Result<(), StreamReadQueueFull> {
    if dispatcher.capacity() <= 1 {
        return Err(StreamReadQueueFull);
    }
    match dispatcher.try_send(StreamReadDataMsg::Data(data)) {
        Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => Ok(()),
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Err(StreamReadQueueFull),
    }
}
async fn open_stream(
    control: &mut MuxControl,
    stream_close_tx: &StreamCloseTxPrototype,
    stream_id: Option<StreamId>,
) -> Result<(StreamId, StreamAcceptMsg), ControlOpenError> {
    let write_broken_pipe = WriteBrokenPipe::new();
    let (stream_read_data_tx, stream_read_data_rx) = stream_read_data_channel();
    let (stream_id, stream_write_data_tx) = control
        .open(stream_read_data_tx, write_broken_pipe.clone(), stream_id)
        .await?;
    let stream_reader = StreamReader::new(
        stream_read_data_rx,
        stream_close_tx.derive(Side::Read, stream_id),
    );
    let live_stream_writer = LiveStreamWriter::new(
        stream_write_data_tx,
        write_broken_pipe,
        stream_close_tx.derive(Side::Write, stream_id),
    );
    let stream_writer = StreamWriter::new(live_stream_writer);
    let msg = StreamAcceptMsg {
        reader: stream_reader,
        writer: stream_writer,
    };
    Ok((stream_id, msg))
}

#[derive(Debug)]
pub struct MuxControl {
    stream_table: HashMap<StreamId, StreamState>,
    local_opened_streams: usize,
    initiation: Initiation,
    next_possible_local_stream_id: StreamId,
    write_data_tx: WriteDataTxPrototype,
    frame_reassembly: bool,
}
impl MuxControl {
    pub fn new(
        initiation: Initiation,
        write_data_tx: WriteDataTxPrototype,
        frame_reassembly: bool,
    ) -> Self {
        Self {
            stream_table: HashMap::new(),
            local_opened_streams: 0,
            initiation,
            next_possible_local_stream_id: 0,
            write_data_tx,
            frame_reassembly,
        }
    }
    fn is_local_opened_stream(&self, stream_id: StreamId) -> bool {
        let is_first_bit_set = stream_id >> (StreamId::BITS - 1) == 1;
        match self.initiation {
            Initiation::Server => is_first_bit_set,
            Initiation::Client => !is_first_bit_set,
        }
    }
    fn should_stream_id_set_first_bit(&self) -> bool {
        match self.initiation {
            Initiation::Server => true,
            Initiation::Client => false,
        }
    }
    fn wire_stream_id(&self, local_stream_id: StreamId) -> StreamId {
        match self.should_stream_id_set_first_bit() {
            true => local_stream_id | (1 << (StreamId::BITS - 1)),
            false => local_stream_id,
        }
    }
    fn next_stream_id(&mut self) -> Result<StreamId, TooManyOpenStreams> {
        let max_local_stream_id = StreamId::MAX >> 1;
        if usize::try_from(max_local_stream_id).unwrap() <= self.local_opened_streams {
            return Err(TooManyOpenStreams {});
        }
        let mut next_local_stream_id = self.next_possible_local_stream_id;
        let local_stream_id = loop {
            if !self
                .stream_table
                .contains_key(&self.wire_stream_id(next_local_stream_id))
            {
                break next_local_stream_id;
            }
            next_local_stream_id = next_local_stream_id.ring_add(1, max_local_stream_id);
        };
        self.next_possible_local_stream_id = local_stream_id.ring_add(1, max_local_stream_id);
        Ok(self.wire_stream_id(local_stream_id))
    }
    pub fn local_close(&mut self, stream_id: StreamId, side: Side) {
        let Some(stream) = self.stream_table.get_mut(&stream_id) else {
            return;
        };
        stream.local_close(side);
        if stream.is_closed() {
            self.clean_closed_stream(stream_id);
        }
    }
    pub fn peer_close(&mut self, stream_id: StreamId, side: Side) {
        let Some(stream) = self.stream_table.get_mut(&stream_id) else {
            return;
        };
        stream.peer_close(side);
        if stream.is_closed() {
            self.clean_closed_stream(stream_id);
        }
    }
    fn clean_closed_stream(&mut self, stream_id: StreamId) {
        if self.is_local_opened_stream(stream_id) {
            self.local_opened_streams -= 1;
        }
        self.stream_table.remove(&stream_id);
    }
    pub fn dispatcher(&self, stream_id: StreamId) -> Option<&StreamReadDataTx> {
        self.stream_table.get(&stream_id)?.dispatcher()
    }
    fn try_dispatch_data(
        &mut self,
        stream_id: StreamId,
        data: crate::central_io::DataBuf,
    ) -> Result<(), StreamReadQueueFull> {
        if data.is_empty() {
            return Ok(());
        }
        let Some(dispatcher) = self.dispatcher(stream_id) else {
            return Ok(());
        };
        match try_send_data(dispatcher, data) {
            Ok(()) => Ok(()),
            Err(StreamReadQueueFull) => {
                self.clean_closed_stream(stream_id);
                Err(StreamReadQueueFull)
            }
        }
    }
    pub async fn open(
        &mut self,
        dispatcher: StreamReadDataTx,
        broken_pipe: WriteBrokenPipe,
        stream_id: Option<StreamId>,
    ) -> Result<(StreamId, StreamWriteDataTx), ControlOpenError> {
        let wire_open = stream_id.is_none();
        let stream_id = match stream_id {
            Some(stream_id) => stream_id,
            None => self
                .next_stream_id()
                .map_err(ControlOpenError::TooManyOpenStreams)?,
        };
        if self.is_local_opened_stream(stream_id) {
            self.local_opened_streams += 1;
        }
        let stream = StreamState::new(dispatcher, broken_pipe, self.frame_reassembly);
        self.stream_table.insert(stream_id, stream);
        Ok((
            stream_id,
            self.write_data_tx
                .derive(stream_id, wire_open)
                .await
                .map_err(ControlOpenError::DeadCentralIo)?,
        ))
    }

    async fn ingest_reassembly(
        &mut self,
        stream_id: StreamId,
        offset: Offset,
        data: crate::central_io::DataBuf,
    ) -> Result<(), ()> {
        let Some(stream) = self.stream_table.get_mut(&stream_id) else {
            return Err(());
        };
        let Some(reassembly) = stream.reassembly.as_mut() else {
            return Err(());
        };
        reassembly.ingest(offset, data).map_err(|e| {
            tracing_reassembly_error(stream_id, offset, &e);
        })?;
        let to_release = reassembly.drain_contiguous();
        let dispatcher = &stream.read_dispatcher;
        for chunk in to_release {
            try_send_data(dispatcher, chunk).map_err(|_| ())?;
        }
        if reassembly.is_complete() && !stream.is_peer_write_closed {
            stream.is_peer_write_closed = true;
            let _ = stream.read_dispatcher.try_send(StreamReadDataMsg::Fin);
        }
        Ok(())
    }

    async fn peer_close_write_with_offset(
        &mut self,
        stream_id: StreamId,
        final_offset: Offset,
    ) -> Result<(), ()> {
        let Some(stream) = self.stream_table.get_mut(&stream_id) else {
            return Ok(());
        };
        if stream.is_peer_write_closed {
            return Ok(());
        }
        let Some(reassembly) = stream.reassembly.as_mut() else {
            return Err(());
        };
        reassembly.set_final_offset(final_offset).map_err(|e| {
            tracing_reassembly_error(stream_id, final_offset, &e);
        })?;
        let to_release = reassembly.drain_contiguous();
        let dispatcher = &stream.read_dispatcher;
        for chunk in to_release {
            try_send_data(dispatcher, chunk).map_err(|_| ())?;
        }
        if reassembly.is_complete() {
            stream.is_peer_write_closed = true;
            let _ = stream.read_dispatcher.try_send(StreamReadDataMsg::Fin);
        }
        Ok(())
    }

    async fn reassembly_error_teardown(&mut self, stream_id: StreamId) -> bool {
        let Some(stream) = self.stream_table.get_mut(&stream_id) else {
            return false;
        };
        if stream.is_read_closed {
            return false;
        }
        let _ = stream.read_dispatcher.try_send(StreamReadDataMsg::Error(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "mux stream read side closed - reassembly protocol error, or the reader stopped draining its queue",
        )));
        stream.reassembly = None;
        stream.is_read_closed = true;
        stream.is_peer_write_closed = true;
        true
    }
}
#[derive(Debug, Clone)]
pub enum ControlOpenError {
    TooManyOpenStreams(TooManyOpenStreams),
    DeadCentralIo(DeadCentralIo),
}

#[derive(Debug)]
struct StreamState {
    is_write_closed: bool,
    is_read_closed: bool,
    read_dispatcher: StreamReadDataTx,
    is_peer_write_closed: bool,
    write_broken_pipe: WriteBrokenPipe,
    /// Per-stream reorder buffer, present only when `frame_reassembly` is on.
    reassembly: Option<ReorderBuffer>,
}
impl StreamState {
    pub fn new(
        read_dispatcher: StreamReadDataTx,
        write_broken_pipe: WriteBrokenPipe,
        frame_reassembly: bool,
    ) -> Self {
        Self {
            is_write_closed: false,
            is_read_closed: false,
            read_dispatcher,
            is_peer_write_closed: false,
            write_broken_pipe,
            reassembly: if frame_reassembly {
                Some(ReorderBuffer::new())
            } else {
                None
            },
        }
    }
    pub fn local_close(&mut self, side: Side) {
        match side {
            Side::Read => self.is_read_closed = true,
            Side::Write => self.is_write_closed = true,
        }
    }
    pub fn peer_close(&mut self, side: Side) {
        match side {
            Side::Read => {
                self.write_broken_pipe.close();
            }
            Side::Write => {
                if self.is_peer_write_closed {
                    return;
                }
                self.is_peer_write_closed = true;
                let _ = self.read_dispatcher.try_send(StreamReadDataMsg::Fin);
            }
        }
    }
    pub fn dispatcher(&self) -> Option<&StreamReadDataTx> {
        if self.is_peer_write_closed {
            return None;
        }
        Some(&self.read_dispatcher)
    }
    pub fn is_closed(&self) -> bool {
        self.is_write_closed
            && self.is_read_closed
            && self.is_peer_write_closed
            && self.write_broken_pipe.is_closed()
    }
}
impl Drop for StreamState {
    fn drop(&mut self) {
        self.write_broken_pipe.close();
    }
}

/// Maximum bytes a single stream's reorder buffer may hold across all
/// pending (not-yet-contiguous) frames. Must cover the transport's max
/// out-of-order delivery — one lost packet holds everything past it
/// until repair; a 1 MiB bound is below one RTT of bulk data and kills
/// healthy streams on the first loss. 16 MiB absorbs ~0.8 s at 20 MiB/s,
/// comfortably above typical rtp repair times under burst loss.
pub const REASSEMBLY_MAX_BUFFERED_BYTES: usize = 16 * 1024 * 1024;

/// Maximum byte range (highest buffered offset − next-expected offset)
/// a single stream's reorder buffer may span. Same rationale as the
/// byte bound: a too-tight window spuriously kills healthy bulk streams
/// the moment a single packet is lost.
pub const REASSEMBLY_MAX_RANGE_BYTES: usize = 16 * 1024 * 1024;

/// Per-stream reorder buffer for frame-reassembly mode. Holds
/// out-of-order Data frames keyed by their absolute byte offset (u64)
/// and releases contiguous bytes to the reader strictly in offset order.
///
/// A monotonic absolute u64 cursor avoids numeric-u32 wrap bugs: wire
/// offsets are mapped to absolute positions by signed serial distance
/// from the cursor, so a frame that wraps past u32::MAX while a near-0
/// frame is buffered still orders correctly. Pending frames are stored
/// in a `BTreeMap<u64, DataBuf>` so the lowest-offset pending frame is
/// always at the front. `ingest` rejects duplicates, overlaps, out-of-
/// window offsets, and overflow of the buffered-bytes / range bounds.
/// `drain_contiguous` pops every frame whose absolute offset equals
/// `cursor` and advances.
#[derive(Debug)]
struct ReorderBuffer {
    /// Next byte to deliver (the reader's contiguous cursor) in absolute
    /// u64 space. Starts at 0 and grows monotonically; never wraps.
    cursor: u64,
    /// Pending frames keyed by their absolute start offset. Always contains
    /// only frames strictly ahead of `cursor` (drain removes contiguous
    /// ones immediately).
    pending: BTreeMap<u64, crate::central_io::DataBuf>,
    /// Total bytes currently buffered across all pending frames.
    buffered_bytes: usize,
    /// Highest absolute byte offset the stream will ever receive, set by
    /// CloseWrite. The stream is complete once `cursor` reaches this
    /// value. `None` until CloseWrite arrives.
    final_offset_abs: Option<u64>,
}

#[derive(Debug)]
enum ReassemblyError {
    Overlap,
    OutOfWindow,
    BufferOverflow,
    RangeOverflow,
    FinalOffsetBeforeCursor,
    AmbiguousOffset,
}

impl ReorderBuffer {
    fn new() -> Self {
        Self {
            cursor: 0,
            pending: BTreeMap::new(),
            buffered_bytes: 0,
            final_offset_abs: None,
        }
    }

    /// Map a wire `Offset` to its absolute u64 position relative to the
    /// current `cursor`. Returns `None` when the distance is the exact
    /// 2³¹ ambiguity (both "2³¹ ahead" and "2³¹ behind" are equally
    /// valid interpretations — reject the frame as ambiguous).
    fn wire_to_abs(&self, wire: Offset) -> Option<u64> {
        let cursor_wire = self.cursor as u32;
        let dist = wire.wrapping_sub(cursor_wire) as i32;
        if dist == i32::MIN {
            return None;
        }
        u64::try_from(self.cursor as i64 + dist as i64).ok()
    }

    /// Ingest one complete frame at `offset`. Duplicates (offset+len ≤
    /// cursor) are silently dropped (idempotent). Overlaps and
    /// out-of-window offsets are errors.
    fn ingest(
        &mut self,
        offset: Offset,
        mut data: crate::central_io::DataBuf,
    ) -> Result<(), ReassemblyError> {
        if data.is_empty() {
            return Ok(());
        }
        let Some(mut abs) = self.wire_to_abs(offset) else {
            return Err(ReassemblyError::AmbiguousOffset);
        };
        let len = data.len() as u64;
        let mut end_abs = abs + len;
        // Already fully delivered: idempotent drop.
        if end_abs <= self.cursor {
            return Ok(());
        }
        // If final_offset_abs is set, the frame must lie entirely within
        // [.. final_offset_abs).
        if let Some(fin) = self.final_offset_abs {
            if abs >= fin {
                return Err(ReassemblyError::FinalOffsetBeforeCursor);
            }
            if end_abs > fin {
                return Err(ReassemblyError::FinalOffsetBeforeCursor);
            }
        }
        // Out-of-window: offset too far ahead of cursor.
        if abs > self.cursor && abs - self.cursor > REASSEMBLY_MAX_RANGE_BYTES as u64 {
            return Err(ReassemblyError::OutOfWindow);
        }
        // Trim already-delivered prefix.
        if abs < self.cursor {
            let trim = (self.cursor - abs) as usize;
            if trim >= data.len() {
                return Ok(());
            }
            data.drain(..trim);
            abs = self.cursor;
            end_abs = abs + data.len() as u64;
        }
        let len = data.len() as u64;
        // Exact duplicate of a buffered frame (same offset, same len).
        if let Some(existing) = self.pending.get(&abs) {
            if existing.len() as u64 == len {
                return Ok(());
            }
            return Err(ReassemblyError::Overlap);
        }
        // Predecessor overlap: does the immediately-preceding buffered
        // frame extend into our range?
        if let Some((&prev_abs, prev_data)) = self.pending.range(..abs).next_back() {
            let prev_end = prev_abs + prev_data.len() as u64;
            if abs < prev_end {
                if prev_end == end_abs && prev_data.len() as u64 == len {
                    return Ok(());
                }
                return Err(ReassemblyError::Overlap);
            }
        }
        // Successor overlap: does our frame extend into the immediately-
        // following buffered frame? (Missing in the old u32-keyed BTreeMap
        // — a predecessor-only check falsely rejects a valid frame that
        // wraps past u32::MAX while a near-0 frame is buffered.)
        if let Some((&succ_abs, _succ_data)) = self.pending.range(abs + 1..).next() {
            if end_abs > succ_abs {
                return Err(ReassemblyError::Overlap);
            }
        }
        // Bounds: buffered bytes and range.
        let new_buffered = self
            .buffered_bytes
            .checked_add(data.len())
            .ok_or(ReassemblyError::BufferOverflow)?;
        if new_buffered > REASSEMBLY_MAX_BUFFERED_BYTES {
            return Err(ReassemblyError::BufferOverflow);
        }
        if abs > self.cursor
            && (abs - self.cursor) as usize + data.len() > REASSEMBLY_MAX_RANGE_BYTES
        {
            return Err(ReassemblyError::RangeOverflow);
        }
        self.buffered_bytes = new_buffered;
        self.pending.insert(abs, data);
        Ok(())
    }

    /// Pop every contiguous frame starting at `cursor` and advance the
    /// cursor. Returns the released frames in offset order.
    fn drain_contiguous(&mut self) -> Vec<crate::central_io::DataBuf> {
        let mut out = Vec::new();
        loop {
            let Some(entry) = self.pending.remove_entry(&self.cursor) else {
                break;
            };
            let len = entry.1.len() as u64;
            self.buffered_bytes = self.buffered_bytes.saturating_sub(entry.1.len());
            self.cursor += len;
            out.push(entry.1);
        }
        out
    }

    /// Record the stream's final byte offset (from CloseWrite). `final_offset`
    /// is a wire offset; it is mapped to absolute space and must be at or
    /// ahead of the cursor. Returns `Err` if `final_offset` is behind the
    /// cursor or a pending frame extends past it. A duplicate CloseWrite
    /// with the same final offset is idempotent; a conflicting final offset
    /// is an error. The final offset consumes no reassembly buffer, so it
    /// is not bounded by `REASSEMBLY_MAX_RANGE_BYTES`.
    fn set_final_offset(&mut self, final_offset: Offset) -> Result<(), ReassemblyError> {
        let Some(fin_abs) = self.wire_to_abs(final_offset) else {
            return Err(ReassemblyError::AmbiguousOffset);
        };
        if let Some(existing) = self.final_offset_abs {
            return if fin_abs == existing {
                Ok(())
            } else {
                Err(ReassemblyError::FinalOffsetBeforeCursor)
            };
        }
        if fin_abs < self.cursor {
            return Err(ReassemblyError::FinalOffsetBeforeCursor);
        }
        for (&off, data) in &self.pending {
            if off + data.len() as u64 > fin_abs {
                return Err(ReassemblyError::FinalOffsetBeforeCursor);
            }
        }
        self.final_offset_abs = Some(fin_abs);
        Ok(())
    }

    /// True once the delivered cursor has reached the final offset.
    fn is_complete(&self) -> bool {
        self.final_offset_abs == Some(self.cursor)
    }
}

fn tracing_reassembly_error(stream_id: StreamId, offset: Offset, e: &ReassemblyError) {
    tracing::warn!(stream_id, offset, error = ?e, "mux reassembly protocol error");
}

#[derive(Debug, Clone)]
pub struct WriteBrokenPipe {
    should_write_close: Arc<AtomicBool>,
}
impl WriteBrokenPipe {
    pub fn new() -> Self {
        Self {
            should_write_close: Arc::new(AtomicBool::new(false)),
        }
    }
    pub fn is_closed(&self) -> bool {
        self.should_write_close.load(Ordering::Relaxed)
    }
    pub fn close(&self) {
        self.should_write_close.store(true, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
pub struct DeadControl {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Initiation {
    Server,
    Client,
}

#[derive(Debug, Clone)]
pub struct TooManyOpenStreams {}

#[cfg(test)]
mod reassembly_tests {
    use super::*;
    use crate::central_io::{
        DataBuf,
        writer::{write_control_channel, write_data_channel},
    };
    use primitive::arena::obj_pool::arc_buf_pool;
    use std::time::Duration;
    use tokio::time::timeout;

    fn buf(bytes: &[u8]) -> DataBuf {
        let pool = arc_buf_pool::<u8>(None, std::num::NonZeroUsize::new(1).unwrap());
        let mut s = pool.take_scoped();
        s.clear();
        s.extend_from_slice(bytes);
        s
    }

    fn collect(out: Vec<DataBuf>) -> Vec<u8> {
        let mut v = Vec::new();
        for b in out {
            v.extend_from_slice(&b);
        }
        v
    }

    #[tokio::test]
    async fn local_write_close_never_uses_mux_control_lane() {
        for frame_reassembly in [false, true] {
            let (write_data_tx, _write_data_rx) = write_data_channel();
            let mut control = MuxControl::new(Initiation::Server, write_data_tx, frame_reassembly);
            let (write_control_tx, mut write_control_rx) = write_control_channel();
            handle_stream_close(
                &mut control,
                &write_control_tx,
                StreamCloseMsg {
                    stream_id: 7,
                    side: Side::Write,
                },
            )
            .await
            .unwrap();
            assert!(
                timeout(Duration::from_millis(10), write_control_rx.recv())
                    .await
                    .is_err(),
                "write close must be emitted only by the per-stream data queue"
            );
        }
    }

    #[tokio::test]
    async fn local_read_close_uses_mux_control_lane_once() {
        for frame_reassembly in [false, true] {
            let (write_data_tx, _write_data_rx) = write_data_channel();
            let mut control = MuxControl::new(Initiation::Server, write_data_tx, frame_reassembly);
            let (write_control_tx, mut write_control_rx) = write_control_channel();
            handle_stream_close(
                &mut control,
                &write_control_tx,
                StreamCloseMsg {
                    stream_id: 11,
                    side: Side::Read,
                },
            )
            .await
            .unwrap();
            let msg = write_control_rx.recv().await.unwrap();
            assert!(matches!(msg, WriteControlMsg::CloseRead(11)));
            assert!(
                timeout(Duration::from_millis(10), write_control_rx.recv())
                    .await
                    .is_err(),
                "read close must emit exactly one control message"
            );
        }
    }

    #[tokio::test]
    async fn duplicate_mode_off_close_write_is_idempotent() {
        let (mut control, _close_tx, _drain) = make_control(false);
        let mut rx = open_test_stream(&mut control, 19).await;
        control.peer_close(19, Side::Write);
        control.peer_close(19, Side::Write);
        assert!(matches!(rx.try_recv(), Ok(StreamReadDataMsg::Fin)));
        assert!(
            rx.try_recv().is_err(),
            "duplicate mode-off CloseWrite must not emit a second local FIN"
        );
    }

    /// A frame for a later offset is held in the reorder buffer until the
    /// gap fills, then released.
    #[tokio::test]
    async fn frame_for_later_offset_held_until_gap_fills() {
        let mut rb = ReorderBuffer::new();
        // Frame at offset 4 arrives before offset 0.
        rb.ingest(4, buf(&[0xDD; 4])).unwrap();
        assert!(rb.drain_contiguous().is_empty(), "no contiguous bytes yet");
        // Fill the gap.
        rb.ingest(0, buf(&[0xAA; 4])).unwrap();
        let out = collect(rb.drain_contiguous());
        assert_eq!(out, [0xAA, 0xAA, 0xAA, 0xAA, 0xDD, 0xDD, 0xDD, 0xDD]);
        assert_eq!(rb.cursor, 8);
    }

    /// Duplicate and overlapping ranges are dropped idempotently (exact
    /// dups) or rejected (partial overlaps).
    #[tokio::test]
    async fn duplicate_and_overlapping_ranges_dropped_idempotently() {
        let mut rb = ReorderBuffer::new();
        rb.ingest(0, buf(&[1, 2, 3, 4])).unwrap();
        let out = collect(rb.drain_contiguous());
        assert_eq!(out, [1, 2, 3, 4]);
        assert_eq!(rb.cursor, 4);

        // Exact duplicate (already delivered): idempotent.
        rb.ingest(0, buf(&[1, 2, 3, 4])).unwrap();
        assert!(rb.drain_contiguous().is_empty());

        // Frame at offset 8 buffered.
        rb.ingest(8, buf(&[5, 6, 7, 8])).unwrap();
        assert!(rb.drain_contiguous().is_empty());

        // Exact duplicate of buffered frame: idempotent.
        rb.ingest(8, buf(&[5, 6, 7, 8])).unwrap();
        assert!(rb.drain_contiguous().is_empty());

        // Partial overlap with buffered frame at 8: error.
        let err = rb.ingest(10, buf(&[9, 10])).unwrap_err();
        assert!(matches!(err, ReassemblyError::Overlap));

        // A frame at offset 6 overlaps with the buffered frame at 8
        // (6..10 vs 8..12). This is now detected by the successor check.
        let err = rb.ingest(6, buf(&[11, 12, 13, 14])).unwrap_err();
        assert!(matches!(err, ReassemblyError::Overlap));

        // To fill the gap properly, use offset 4 with length 4.
        rb.ingest(4, buf(&[21, 22, 23, 24])).unwrap();
        let out = collect(rb.drain_contiguous());
        assert_eq!(out, [21, 22, 23, 24, 5, 6, 7, 8]);
        assert_eq!(rb.cursor, 12);

        // Overlap with cursor (partially delivered): trimmed.
        // Let's test actual overlap with cursor:
        let mut rb2 = ReorderBuffer::new();
        rb2.ingest(0, buf(&[1, 2, 3, 4])).unwrap();
        let _ = collect(rb2.drain_contiguous());
        // cursor is now 4. A frame at offset 2 with 6 bytes overlaps
        // the delivered prefix [2,4); the suffix [4,8) should be kept.
        rb2.ingest(2, buf(&[10, 20, 30, 40, 50, 60])).unwrap();
        let out = collect(rb2.drain_contiguous());
        assert_eq!(out, [30, 40, 50, 60]);
        assert_eq!(rb2.cursor, 8);
    }

    /// Overflow of REASSEMBLY_MAX_BUFFERED_BYTES kills the stream (returns
    /// Err), not the session. The buffer itself survives.
    #[tokio::test]
    async fn reorder_buffer_bound_kills_stream_not_session() {
        let mut rb = ReorderBuffer::new();
        // A frame far ahead creates a large gap. Fill it with a big frame
        // at offset 0 (so next_offset advances), then test the buffered-
        // bytes bound by exceeding it.
        // First, advance cursor to 4.
        rb.ingest(0, buf(&[0; 4])).unwrap();
        let _ = collect(rb.drain_contiguous());

        // Now buffer frames that exceed REASSEMBLY_MAX_BUFFERED_BYTES.
        let big = vec![0u8; REASSEMBLY_MAX_BUFFERED_BYTES + 1];
        let err = rb.ingest(4, buf(&big)).unwrap_err();
        assert!(
            matches!(err, ReassemblyError::BufferOverflow),
            "expected BufferOverflow, got {err:?}"
        );

        // A valid frame after the error still works (session survives).
        rb.ingest(4, buf(&[1, 2, 3, 4])).unwrap();
        let out = collect(rb.drain_contiguous());
        assert_eq!(out, [1, 2, 3, 4]);

        // Range overflow.
        let far_offset = 4 + 4 + (REASSEMBLY_MAX_RANGE_BYTES as u32) + 1;
        let err = rb.ingest(far_offset, buf(&[0; 4])).unwrap_err();
        assert!(
            matches!(
                err,
                ReassemblyError::RangeOverflow | ReassemblyError::OutOfWindow
            ),
            "expected range/out-of-window error, got {err:?}"
        );
    }

    /// Wraparound offset comparison: offsets near the u32 boundary compare
    /// correctly using TCP-style serial-number arithmetic.
    #[tokio::test]
    async fn wraparound_offset_comparison() {
        let mut rb = ReorderBuffer::new();
        // Deliver a frame ending exactly at u32::MAX - 3.
        rb.ingest(0, buf(&[0xAB; 4])).unwrap();
        let _ = collect(rb.drain_contiguous());
        assert_eq!(rb.cursor, 4);

        // Advance to near the wrap point in absolute space.
        rb.cursor = 0xFFFF_FFFC;
        rb.ingest(0xFFFF_FFFC, buf(&[1, 2, 3, 4])).unwrap();
        let out = collect(rb.drain_contiguous());
        assert_eq!(out, [1, 2, 3, 4]);
        assert_eq!(rb.cursor, 0x1_0000_0000, "cursor advanced past u32::MAX");

        // A frame at offset 0 maps to absolute 0x1_0000_0000 (via signed serial
        // distance from cursor 0x1_0000_0000 → 0), which is the next expected.
        rb.ingest(0, buf(&[5, 6, 7, 8])).unwrap();
        let out = collect(rb.drain_contiguous());
        assert_eq!(out, [5, 6, 7, 8]);

        // An "old" frame at wire offset 0xFFFF_FFF8 maps far behind the
        // absolute cursor and is idempotently dropped.
        rb.ingest(0xFFFF_FFF8, buf(&[0; 4])).unwrap();
        assert!(rb.drain_contiguous().is_empty());
    }

    #[tokio::test]
    async fn reorder_buffer_reassembles_any_arrival_order() {
        fn truth(abs: u64) -> u8 {
            (abs.wrapping_mul(31).wrapping_add(7) & 0xFF) as u8
        }
        struct Lcg(u64);
        impl Lcg {
            fn next(&mut self) -> u64 {
                self.0 = self
                    .0
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                self.0 >> 33
            }
            fn below(&mut self, n: u64) -> u64 {
                self.next() % n
            }
        }
        for start in [0u64, 0xFFFF_FF00, 0x1_0000_0000 - 7] {
            for trial in 0..200u64 {
                let mut rng = Lcg(trial.wrapping_mul(0x9E37_79B9) ^ start);
                let total = 1 + rng.below(300);
                let mut frames: Vec<(u64, usize)> = Vec::new();
                let mut at = 0u64;
                while at < total {
                    let len = (1 + rng.below(20)).min(total - at);
                    frames.push((at, len as usize));
                    at += len;
                }
                for i in (1..frames.len()).rev() {
                    frames.swap(i, rng.below(i as u64 + 1) as usize);
                }
                let originals = frames.clone();
                for _ in 0..rng.below(originals.len() as u64 + 1) {
                    let pick = originals[rng.below(originals.len() as u64) as usize];
                    let at = rng.below(frames.len() as u64 + 1) as usize;
                    frames.insert(at, pick);
                }
                let mut rb = ReorderBuffer::new();
                rb.cursor = start;
                let mut delivered: Vec<u8> = Vec::new();
                for (rel, len) in frames {
                    let abs = start + rel;
                    let bytes: Vec<u8> = (0..len as u64).map(|i| truth(abs + i)).collect();
                    rb.ingest(abs as Offset, buf(&bytes)).unwrap_or_else(|e| {
                        panic!("start={start:#x} trial={trial} rel={rel} len={len}: {e:?}")
                    });
                    delivered.extend_from_slice(&collect(rb.drain_contiguous()));
                    let expected: Vec<u8> = (0..delivered.len() as u64)
                        .map(|i| truth(start + i))
                        .collect();
                    assert_eq!(
                        delivered, expected,
                        "start={start:#x} trial={trial}: delivered bytes diverged"
                    );
                    assert_eq!(
                        rb.cursor,
                        start + delivered.len() as u64,
                        "start={start:#x} trial={trial}: cursor disagrees with what was released"
                    );
                    let pending: usize = rb.pending.values().map(|d| d.len()).sum();
                    assert_eq!(
                        rb.buffered_bytes, pending,
                        "start={start:#x} trial={trial}: buffered_bytes drifted from pending"
                    );
                }
                assert_eq!(
                    delivered.len() as u64,
                    total,
                    "start={start:#x} trial={trial}: stream did not complete"
                );
                assert!(rb.pending.is_empty());
                rb.set_final_offset((start + total) as Offset).unwrap();
                assert!(rb.is_complete());
            }
        }
    }

    #[tokio::test]
    async fn reorder_buffer_survives_arbitrary_peer_frames() {
        struct Lcg(u64);
        impl Lcg {
            fn next(&mut self) -> u64 {
                self.0 = self
                    .0
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                self.0 >> 33
            }
            fn below(&mut self, n: u64) -> u64 {
                self.next() % n
            }
        }
        for start in [0u64, 1000, 0xFFFF_FF80, 0x1_0000_0000 + 5] {
            for trial in 0..300u64 {
                let mut rng = Lcg(trial.wrapping_mul(0x9E37_79B9) ^ start ^ 0xDEAD_BEEF);
                let mut rb = ReorderBuffer::new();
                rb.cursor = start;
                let mut last_cursor = rb.cursor;
                for step in 0..60 {
                    let where_ = |rng: &mut Lcg, rb: &ReorderBuffer| -> Offset {
                        match rng.below(4) {
                            0..=1 => {
                                (rb.cursor as i64 + (rng.below(41) as i64 - 20)) as u64 as Offset
                            }
                            2 => rb
                                .cursor
                                .wrapping_add(REASSEMBLY_MAX_RANGE_BYTES as u64)
                                .wrapping_sub(rng.below(3))
                                as Offset,
                            _ => rng.next() as Offset,
                        }
                    };
                    match rng.below(10) {
                        0 => {
                            let _ = rb.set_final_offset(where_(&mut rng, &rb));
                        }
                        1 => {
                            let _ = rb.drain_contiguous();
                        }
                        _ => {
                            let off = where_(&mut rng, &rb);
                            let len = rng.below(41) as usize;
                            let bytes: Vec<u8> = (0..len).map(|i| (i as u8) ^ 0x5A).collect();
                            let _ = rb.ingest(off, buf(&bytes));
                        }
                    }
                    let ctx = format!("start={start:#x} trial={trial} step={step}");
                    assert!(rb.cursor >= last_cursor, "{ctx}: cursor went backwards");
                    last_cursor = rb.cursor;
                    let pending: usize = rb.pending.values().map(|d| d.len()).sum();
                    assert_eq!(rb.buffered_bytes, pending, "{ctx}: buffered_bytes drifted");
                    assert!(
                        rb.buffered_bytes <= REASSEMBLY_MAX_BUFFERED_BYTES,
                        "{ctx}: buffered past the bound"
                    );
                    let mut prev_end = rb.cursor;
                    for (&off, data) in &rb.pending {
                        assert!(
                            off >= prev_end,
                            "{ctx}: pending {off} overlaps or is behind"
                        );
                        assert!(!data.is_empty(), "{ctx}: empty frame buffered");
                        prev_end = off + data.len() as u64;
                    }
                    if let Some(fin) = rb.final_offset_abs {
                        assert!(rb.cursor <= fin, "{ctx}: cursor ran past the final offset");
                        assert!(
                            prev_end <= fin,
                            "{ctx}: a pending frame ends past the final offset"
                        );
                    }
                }
            }
        }
    }

    // ---- End-to-end reassembly tests via MuxControl ----

    use crate::control::WriteBrokenPipe;
    use crate::stream::reader::{StreamReadDataMsg, StreamReadDataRx, stream_read_data_channel};
    use crate::stream::stream_close_channel;

    fn make_control(
        frame_reassembly: bool,
    ) -> (
        MuxControl,
        StreamCloseTxPrototype,
        tokio::task::JoinHandle<()>,
    ) {
        let (tx, mut rx) = crate::central_io::writer::write_data_channel();
        let control = MuxControl::new(Initiation::Server, tx, frame_reassembly);
        let (close_tx, _close_rx) = stream_close_channel();
        // Drain the write-data receiver so `derive` (which sends an Open
        // through the fair queue) never blocks.
        let drain = tokio::spawn(async move { while rx.recv().await.is_ok() {} });
        (control, close_tx, drain)
    }

    /// Open a stream with a fresh (dispatcher, receiver) pair we control.
    async fn open_test_stream(control: &mut MuxControl, stream_id: StreamId) -> StreamReadDataRx {
        let (tx, rx) = stream_read_data_channel();
        let bp = WriteBrokenPipe::new();
        control.open(tx, bp, Some(stream_id)).await.unwrap();
        rx
    }

    #[tokio::test]
    async fn full_stream_read_queue_resets_only_that_stream() {
        let mut rig = central_read_rig(false);
        let _blocked_rx = open_test_stream(&mut rig.control, 1).await;
        let mut sibling_rx = open_test_stream(&mut rig.control, 2).await;
        let mut queued = 0;
        loop {
            let result = rig
                .control
                .dispatcher(1)
                .unwrap()
                .try_send(StreamReadDataMsg::Data(buf(&[0xAA])));
            match result {
                Ok(()) => queued += 1,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => break,
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    panic!("stream reader unexpectedly closed")
                }
            }
        }
        assert!(queued > 0);
        rig.deliver(CentralIoReadMsg::Data(1, 0, buf(&[0xBB])))
            .await
            .unwrap();
        assert!(!rig.control.stream_table.contains_key(&1));
        assert!(matches!(
            rig.write_control_rx.recv().await.unwrap(),
            WriteControlMsg::CloseRead(1)
        ));
        assert!(matches!(
            rig.write_control_rx.recv().await.unwrap(),
            WriteControlMsg::ForceCloseWrite(1)
        ));
        assert!(
            timeout(Duration::from_millis(10), rig.write_control_rx.recv())
                .await
                .is_err(),
            "overflow abort must emit exactly CloseRead then ForceCloseWrite"
        );
        rig.control.try_dispatch_data(2, buf(&[0xCC])).unwrap();
        let msg = sibling_rx.try_recv().expect("sibling data must progress");
        match msg {
            StreamReadDataMsg::Data(data) => assert_eq!(&data[..], &[0xCC]),
            other => panic!("expected sibling Data, got {other:?}"),
        }
    }

    /// Stream A has a gap (frame at offset 0 missing); stream B's
    /// complete frames deliver immediately because each stream
    /// reassembles independently.
    #[tokio::test]
    async fn streams_pass_each_other() {
        let (mut control, _close_tx, _drain) = make_control(true);
        let mut rx_a = open_test_stream(&mut control, 100).await;
        let mut rx_b = open_test_stream(&mut control, 200).await;

        // Stream A: deliver offset 4 first (gap at 0).
        control
            .ingest_reassembly(100, 4, buf(&[0xAA; 4]))
            .await
            .unwrap();
        // Stream B: deliver offset 0 (complete).
        control
            .ingest_reassembly(200, 0, buf(&[0xBB; 4]))
            .await
            .unwrap();

        // Stream B's reader should have its bytes; stream A's should
        // not (gap at 0).
        let b_msg = rx_b.try_recv().expect("B's frame delivered");
        match b_msg {
            StreamReadDataMsg::Data(d) => assert_eq!(&d[..], &[0xBB; 4]),
            other => panic!("expected Data, got {other:?}"),
        }
        assert!(
            rx_a.try_recv().is_err(),
            "A should not deliver until gap fills"
        );

        // Fill A's gap.
        control
            .ingest_reassembly(100, 0, buf(&[0xAA; 4]))
            .await
            .unwrap();
        // Two frames now deliver: [0..4] and [4..8].
        let mut got = Vec::new();
        for _ in 0..2 {
            let msg = rx_a.try_recv().expect("A now delivers");
            match msg {
                StreamReadDataMsg::Data(d) => got.extend_from_slice(&d),
                other => panic!("expected Data, got {other:?}"),
            }
        }
        assert_eq!(got, [0xAA; 8]);
    }

    /// Data for an unknown stream_id is rejected by ingest_reassembly
    /// (the implicit-open responsibility moved to handle_central_read).
    /// Pre-open the stream to exercise ingestion.
    #[tokio::test]
    async fn data_before_open_implicitly_creates_stream() {
        let (mut control, _close_tx, _drain) = make_control(true);

        // Pre-open stream 42 (the control loop handles Data-before-Open
        // implicit creation and routes the accept message; we test the
        // ingestion path here).
        open_test_stream(&mut control, 42).await;

        // Data for stream 42: ingestion works because the stream exists.
        control
            .ingest_reassembly(42, 0, buf(&[1, 2, 3]))
            .await
            .unwrap();

        // Stream 42 exists.
        assert!(control.stream_table.contains_key(&42));

        // Ingest another frame; it delivers.
        control
            .ingest_reassembly(42, 3, buf(&[4, 5]))
            .await
            .unwrap();
    }

    /// CloseWrite's final offset completes the stream even if some Data
    /// frames arrive after the CloseWrite frame (reordering).
    #[tokio::test]
    async fn closewrite_final_offset_completes_stream_despite_reordering() {
        let (mut control, _close_tx, _drain) = make_control(true);
        let mut rx = open_test_stream(&mut control, 7).await;

        // Deliver offset 0 and offset 4 (total 8 bytes).
        control.ingest_reassembly(7, 0, buf(&[0; 4])).await.unwrap();
        control.ingest_reassembly(7, 4, buf(&[1; 4])).await.unwrap();

        // CloseWrite with final_offset=8. All bytes delivered, so the
        // stream completes and the reader sees Fin.
        control.peer_close_write_with_offset(7, 8).await.unwrap();
        let msg = rx.try_recv().expect("first frame");
        assert!(matches!(msg, StreamReadDataMsg::Data(_)));
        let msg = rx.try_recv().expect("second frame");
        assert!(matches!(msg, StreamReadDataMsg::Data(_)));
        let msg = rx.try_recv().expect("Fin");
        assert!(matches!(msg, StreamReadDataMsg::Fin));

        // Reordered case: CloseWrite arrives BEFORE the last Data frame.
        let (mut control2, _close_tx2, _drain2) = make_control(true);
        let mut rx2 = open_test_stream(&mut control2, 8).await;

        // Deliver offset 0, then CloseWrite(final=8) — gap at 4.
        control2
            .ingest_reassembly(8, 0, buf(&[0; 4]))
            .await
            .unwrap();
        control2.peer_close_write_with_offset(8, 8).await.unwrap();

        // Offset 0 frame delivered, but stream NOT complete (gap at 4).
        let msg = rx2.try_recv().expect("first frame delivered");
        assert!(matches!(msg, StreamReadDataMsg::Data(_)));
        assert!(rx2.try_recv().is_err(), "no Fin yet — gap at 4");

        // Now fill the gap (CloseWrite arrived before this Data frame).
        control2
            .ingest_reassembly(8, 4, buf(&[1; 4]))
            .await
            .unwrap();
        let msg = rx2.try_recv().expect("second frame delivered");
        assert!(matches!(msg, StreamReadDataMsg::Data(_)));
        let msg = rx2.try_recv().expect("Fin after gap fills");
        assert!(matches!(msg, StreamReadDataMsg::Fin));
    }

    /// A >65535-byte write splits into multiple Data frames and
    /// reassembles correctly by offset (guards the u16 body_len split).
    #[tokio::test]
    async fn multi_frame_write_reassembles_by_offset() {
        let (mut control, _close_tx, _drain) = make_control(true);
        let mut rx = open_test_stream(&mut control, 5).await;

        // Simulate the writer splitting a 200_000-byte write into
        // multiple Data frames at BodyLen::MAX (65535) boundaries.
        let total = 200_000u32;
        let payload: Vec<u8> = (0u8..=u8::MAX).cycle().take(total as usize).collect();
        let mut offset = 0u32;
        while offset < total {
            let len = (total - offset).min(u16::MAX as u32) as usize;
            control
                .ingest_reassembly(
                    5,
                    offset,
                    buf(&payload[offset as usize..offset as usize + len]),
                )
                .await
                .unwrap();
            offset += len as u32;
        }

        // Reassemble from the receiver.
        let mut got = Vec::new();
        while got.len() < total as usize {
            let msg = rx.try_recv().expect("frame available");
            match msg {
                StreamReadDataMsg::Data(d) => got.extend_from_slice(&d),
                other => panic!("expected Data, got {other:?}"),
            }
        }
        assert_eq!(got, payload);
    }

    // ---- Named acceptance tests ----

    /// Wraparound ordering: when the absolute cursor is near u32::MAX
    /// (0xFFFF_FFFC), frames at wire offsets that wrap correctly sort by
    /// absolute u64 key, not by numeric u32. A frame at wire offset 0
    /// (mapping to absolute 0x1_0000_0000) must sort AFTER a frame at
    /// 0xFFFF_FFFC (absolute 0xFFFF_FFFC) — the numeric-u32 BTreeMap
    /// would put 0 before 0xFFFF_FFFC, falsely ordering the wrap case.
    #[tokio::test]
    async fn wraparound_pending_order_uses_absolute_epoch() {
        let mut rb = ReorderBuffer::new();
        // Plant the cursor near the wrap boundary.
        rb.cursor = 0xFFFF_FFFC;

        // Frame at 0xFFFF_FFFC (abs 0xFFFF_FFFC): contiguous, delivers.
        rb.ingest(0xFFFF_FFFC, buf(&[1, 2, 3, 4])).unwrap();
        let out = collect(rb.drain_contiguous());
        assert_eq!(out, [1, 2, 3, 4]);
        assert_eq!(rb.cursor, 0x1_0000_0000);

        // Frame at wire offset 4 (abs 0x1_0000_0004): held in buffer.
        rb.ingest(4, buf(&[0xAA; 4])).unwrap();
        assert!(rb.drain_contiguous().is_empty());

        // Frame at wire offset 0 (abs = 0x1_0000_0000, BEFORE the held
        // frame at abs 0x1_0000_0004). With u64 keys this sorts correctly
        // as the predecessor; with a numeric-u32 BTreeMap (key 0 vs key 4)
        // it would be the successor and break contiguous delivery.
        rb.ingest(0, buf(&[5, 6, 7, 8])).unwrap();

        // Drain: the contiguous range [0..8] in absolute space delivers.
        let out = collect(rb.drain_contiguous());
        assert_eq!(out, [5, 6, 7, 8, 0xAA, 0xAA, 0xAA, 0xAA]);
        assert_eq!(rb.cursor, 0x1_0000_0008);
    }

    /// Successor overlap: a frame whose range extends into a later
    /// (higher absolute offset) buffered frame is rejected. The old
    /// u32-keyed BTreeMap only checked predecessor overlap, so a frame
    /// that overlapped from BELOW went undetected.
    #[tokio::test]
    async fn successor_overlap_is_rejected() {
        let mut rb = ReorderBuffer::new();
        // Buffer a frame at abs 8 (wire 8, len 4, for 8..12).
        rb.cursor = 4; // simulate already-delivered bytes 0..4
        rb.ingest(8, buf(&[0xCC; 4])).unwrap();

        // A frame at wire offset 6 (abs 6, len 4, for 6..10) overlaps
        // with the buffered frame (8..12). Successor check catches this.
        let err = rb.ingest(6, buf(&[0xDD; 4])).unwrap_err();
        assert!(
            matches!(err, ReassemblyError::Overlap),
            "successor overlap must be rejected: got {err:?}"
        );
    }

    /// A final offset far ahead of the cursor is accepted. The final
    /// offset is a marker that consumes no reassembly buffer, so capping
    /// it by `REASSEMBLY_MAX_RANGE_BYTES` is wrong: a valid far
    /// CloseWrite would tear the reader down (OutOfWindow -> teardown)
    /// and lose subsequently-arriving in-order data. The range cap
    /// applies only to buffered DATA frames, not to the CloseWrite
    /// marker.
    #[tokio::test]
    async fn far_final_offset_is_accepted_not_rejected() {
        let mut rb = ReorderBuffer::new();
        rb.cursor = 100;
        // Final offset far beyond REASSEMBLY_MAX_RANGE_BYTES from cursor.
        let far = 100u32.wrapping_add(REASSEMBLY_MAX_RANGE_BYTES as u32 + 1);
        // The far final offset is accepted (not OutOfWindow).
        rb.set_final_offset(far).unwrap();
        assert!(rb.final_offset_abs.is_some());
        assert!(!rb.is_complete(), "cursor 100 hasn't reached the far final");
    }

    /// Final offset that wraps past u32::MAX but stays within the forward
    /// reassembly window is accepted. This guards the common case where a
    /// stream carries >4 GiB and the final offset wraps.
    #[tokio::test]
    async fn final_offset_may_cross_wire_wrap_within_window() {
        let mut rb = ReorderBuffer::new();
        // Cursor is near u32::MAX; final offset wraps to a small value.
        rb.cursor = 0xFFFF_FFF0;
        let fin = 0x10; // wraps, but forward distance from cursor is 32 < window
        rb.set_final_offset(fin).unwrap();
        assert!(rb.final_offset_abs.is_some());
        // Not yet complete — cursor 0xFFFF_FFF0 hasn't reached the final.
        assert!(!rb.is_complete());
    }

    /// Data-before-Open routes the accept message to the accepter exactly
    /// once. When a Data frame arrives for a stream that hasn't been
    /// opened yet, the control loop implicitly creates the stream AND
    /// sends an `AcceptMsg` down the accept channel so the application
    /// gets a reader/writer pair. The subsequent Open is a no-op.
    #[tokio::test]
    async fn data_before_open_surfaces_accept_msg_once() {
        use crate::central_io::writer::write_control_channel;
        use crate::stream::accepter::stream_accept_channel;
        use crate::stream::opener::stream_open_channel;

        let (tx, mut rx) = crate::central_io::writer::write_data_channel();
        let drain = tokio::spawn(async move { while rx.recv().await.is_ok() {} });
        let mut control = MuxControl::new(Initiation::Server, tx, true);
        let (close_tx, _close_rx) = stream_close_channel();
        let (_open_tx, open_rx) = stream_open_channel();
        let (accept_tx, mut accept_rx) = stream_accept_channel();
        let mut stream_init_handle = StreamInitHandle {
            stream_open_rx: open_rx,
            stream_accept_tx: accept_tx,
        };
        let (write_control_tx, _write_control_rx) = write_control_channel();

        // Data arrives before Open for stream 42.
        let msg = CentralIoReadMsg::Data(42, 0, buf(&[1, 2, 3]));
        handle_central_read(
            &mut control,
            &close_tx,
            &mut stream_init_handle,
            &write_control_tx,
            msg,
        )
        .await
        .unwrap();

        // The stream exists in the table.
        assert!(control.stream_table.contains_key(&42));

        // The accept channel has exactly one message.
        let accepted = accept_rx.try_recv().expect("accept msg must be available");
        let _ = accepted; // reader + writer created

        // A second accept should NOT have a message (exactly once).
        assert!(accept_rx.try_recv().is_err());

        // A subsequent Open is a no-op.
        let open_msg = CentralIoReadMsg::Open(42);
        handle_central_read(
            &mut control,
            &close_tx,
            &mut stream_init_handle,
            &write_control_tx,
            open_msg,
        )
        .await
        .unwrap();

        // Still only one accept message was sent (Open was no-op).
        assert!(accept_rx.try_recv().is_err());

        drop(drain);
    }

    /// CloseWrite-before-Open routes accept msg and preserves EOF. When
    /// a CloseWrite frame arrives for an unknown stream (reassembly on),
    /// it implicitly creates the stream, sends AcceptMsg, and records the
    /// final offset. The reader then sees Fin once all bytes up to the
    /// final offset are delivered.
    #[tokio::test]
    async fn close_before_open_surfaces_accept_msg_and_preserves_eof() {
        use crate::central_io::writer::write_control_channel;
        use crate::stream::accepter::stream_accept_channel;
        use crate::stream::opener::stream_open_channel;

        let (tx, mut rx) = crate::central_io::writer::write_data_channel();
        let drain = tokio::spawn(async move { while rx.recv().await.is_ok() {} });
        let mut control = MuxControl::new(Initiation::Server, tx, true);
        let (close_tx, _close_rx) = stream_close_channel();
        let (_open_tx, open_rx) = stream_open_channel();
        let (accept_tx, mut accept_rx) = stream_accept_channel();
        let mut stream_init_handle = StreamInitHandle {
            stream_open_rx: open_rx,
            stream_accept_tx: accept_tx,
        };
        let (write_control_tx, _write_control_rx) = write_control_channel();

        // CloseWrite arrives before Open for stream 7 with final_offset=3.
        let msg = CentralIoReadMsg::Close(7, Side::Write, 3);
        handle_central_read(
            &mut control,
            &close_tx,
            &mut stream_init_handle,
            &write_control_tx,
            msg,
        )
        .await
        .unwrap();

        // Stream exists.
        assert!(control.stream_table.contains_key(&7));

        // Exactly one accept msg was sent.
        let accepted = accept_rx.try_recv().expect("accept msg must be available");
        let (reader, _writer) = (accepted.reader, accepted.writer);
        assert!(accept_rx.try_recv().is_err());

        // Now deliver data to the stream.
        control
            .ingest_reassembly(7, 0, buf(&[0xAA; 3]))
            .await
            .unwrap();

        // The reader should see the data bytes, then Fin (EOF).
        let mut got = Vec::new();
        use tokio::io::AsyncReadExt;
        let mut reader = std::pin::pin!(reader);
        reader.read_to_end(&mut got).await.unwrap();
        assert_eq!(
            got, [0xAA; 3],
            "reader gets data, then EOF after final_offset"
        );

        drop(drain);
    }

    /// Reassembly error teardown is idempotent: after the first error
    /// closes the stream's read side, subsequent calls must not enqueue
    /// additional Error messages. A burst of late/bad frames after the
    /// first error would fill the reader channel (1024 entries) and
    /// stall sibling streams.
    #[tokio::test]
    async fn reassembly_error_teardown_is_idempotent() {
        let (mut control, _close_tx, _drain) = make_control(true);
        let mut broken_rx = open_test_stream(&mut control, 1).await;
        let mut sibling_rx = open_test_stream(&mut control, 2).await;

        // First error: enqueues Error and marks is_read_closed.
        control.reassembly_error_teardown(1).await;

        // Verify Error arrived (exactly one).
        let msg = broken_rx
            .try_recv()
            .expect("first error should be readable");
        assert!(
            matches!(msg, StreamReadDataMsg::Error(_)),
            "expected Error, got {msg:?}"
        );

        // Second call on same stream: guard fires, no new message.
        control.reassembly_error_teardown(1).await;
        assert!(
            broken_rx.try_recv().is_err(),
            "no second Error after teardown is already closed"
        );

        // Sibling stream still makes progress.
        control
            .ingest_reassembly(2, 0, buf(&[0xAA; 4]))
            .await
            .unwrap();
        let msg = sibling_rx
            .try_recv()
            .expect("sibling should receive data after sibling teardown");
        assert!(
            matches!(msg, StreamReadDataMsg::Data(_)),
            "sibling stream must progress, got {msg:?}"
        );
    }

    // -----------------------------------------------------------------
    // Fix 1 acceptance: a far final offset (beyond
    // REASSEMBLY_MAX_RANGE_BYTES) is accepted; in-order data up to it
    // advances the cursor and yields Fin.
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn far_final_offset_accepted_and_completes() {
        use std::future::poll_fn;

        let (mut control, _close_tx, _drain) = make_control(true);
        let mut rx = open_test_stream(&mut control, 9).await;

        // Cursor is 0; no pending data. CloseWrite with final offset
        // = REASSEMBLY_MAX_RANGE_BYTES + 1 (far beyond the range cap).
        // This used to return OutOfWindow -> teardown and lose all data.
        control
            .peer_close_write_with_offset(9, REASSEMBLY_MAX_RANGE_BYTES as u32 + 1)
            .await
            .unwrap();

        // Deliver in-order data up to the final offset in 1 MiB chunks
        // (16 messages for ~16 MiB) and drain the receiver concurrently
        // so the bounded channel doesn't block ingestion.
        let fin = REASSEMBLY_MAX_RANGE_BYTES as u32 + 1;
        let chunk = vec![0xABu8; 1024 * 1024];
        let mut off = 0u32;
        let mut total = 0u64;
        let mut saw_fin = false;
        while off < fin || !saw_fin {
            // Ingest the next chunk if not all delivered yet.
            if off < fin {
                let len = ((fin - off) as usize).min(chunk.len());
                control
                    .ingest_reassembly(9, off, buf(&chunk[..len]))
                    .await
                    .unwrap();
                off += len as u32;
            }
            // Drain whatever is ready on the receiver without blocking.
            while !saw_fin {
                match rx.try_recv() {
                    Ok(StreamReadDataMsg::Data(d)) => total += d.len() as u64,
                    Ok(StreamReadDataMsg::Fin) => saw_fin = true,
                    Ok(StreamReadDataMsg::Error(e)) => {
                        panic!("expected Data/Fin, got Error: {e}");
                    }
                    Err(_) => break,
                }
            }
            // If all data is ingested but Fin hasn't arrived yet, poll
            // the receiver cooperatively until it does.
            if off == fin && !saw_fin {
                match poll_fn(|cx| rx.poll_recv(cx)).await {
                    Ok(StreamReadDataMsg::Data(d)) => total += d.len() as u64,
                    Ok(StreamReadDataMsg::Fin) => saw_fin = true,
                    Ok(StreamReadDataMsg::Error(e)) => {
                        panic!("expected Data/Fin, got Error: {e}");
                    }
                    Err(_) => break,
                }
            }
        }

        assert_eq!(total, fin as u64, "all bytes up to the far final delivered");
        assert!(saw_fin, "Fin must arrive");
    }

    // -----------------------------------------------------------------
    // Fix 2 acceptance: a duplicate CloseWrite with the SAME final
    // offset is idempotent. Delayed data after the duplicate is still
    // delivered, and Fin still arrives.
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn duplicate_close_write_is_idempotent() {
        let (mut control, _close_tx, _drain) = make_control(true);
        let mut rx = open_test_stream(&mut control, 11).await;

        // Data[0,4).
        control
            .ingest_reassembly(11, 0, buf(&[0xAA; 4]))
            .await
            .unwrap();
        // CloseWrite(final=8).
        control.peer_close_write_with_offset(11, 8).await.unwrap();
        // Duplicate CloseWrite(final=8) — must be a no-op, NOT a
        // destructive teardown. (Old code: FinalOffsetBeforeCursor ->
        // reassembly_error_teardown -> reader gets BrokenPipe and all
        // later data/FIN is lost.)
        control.peer_close_write_with_offset(11, 8).await.unwrap();
        // Delayed Data[4,8).
        control
            .ingest_reassembly(11, 4, buf(&[0xBB; 4]))
            .await
            .unwrap();

        // Drain: [0,4), [4,8), Fin — no Error.
        let mut got = Vec::new();
        let mut saw_fin = false;
        loop {
            match rx.try_recv() {
                Ok(StreamReadDataMsg::Data(d)) => got.extend_from_slice(&d),
                Ok(StreamReadDataMsg::Fin) => {
                    saw_fin = true;
                    break;
                }
                Ok(StreamReadDataMsg::Error(e)) => {
                    panic!("duplicate CloseWrite must not error: {e}");
                }
                Err(_) => break,
            }
        }
        assert_eq!(
            got,
            [0xAA; 4]
                .iter()
                .chain([0xBB; 4].iter())
                .copied()
                .collect::<Vec<_>>()
        );
        assert!(saw_fin, "Fin must arrive after the gap fills");
    }

    // -----------------------------------------------------------------
    // Fix 3 acceptance: after a reassembly error tears down a stream's
    // read side, closing the local write side and delivering the peer's
    // CloseRead + CloseWrite retires the stream-table entry (is_closed
    // becomes true). Before the fix, is_peer_write_closed was never set
    // after teardown, so is_closed() never became true and the entry
    // leaked forever.
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn stream_retired_after_reassembly_error() {
        let (mut control, _close_tx, _drain) = make_control(true);
        let _rx = open_test_stream(&mut control, 13).await;
        control
            .ingest_reassembly(13, 4, buf(&[0; 4]))
            .await
            .unwrap();
        let overlap_err = control.ingest_reassembly(13, 2, buf(&[1; 4])).await;
        assert!(overlap_err.is_err(), "overlapping frame must error");
        control.reassembly_error_teardown(13).await;
        assert!(control.stream_table.contains_key(&13));
        {
            let stream = control.stream_table.get(&13).unwrap();
            assert!(stream.is_read_closed, "teardown sets is_read_closed");
            assert!(
                stream.is_peer_write_closed,
                "teardown must set is_peer_write_closed (Fix 3) so the entry can be retired once the local write side closes"
            );
            assert!(!stream.is_write_closed, "local write side still open");
            assert!(
                !stream.is_closed(),
                "is_closed() must be false until the local write side and broken-pipe also close"
            );
        }
        control.local_close(13, Side::Write);
        if let Some(stream) = control.stream_table.get(&13) {
            assert!(!stream.is_closed(), "write_broken_pipe still open");
        }
        control.peer_close(13, Side::Read);
        assert!(
            !control.stream_table.contains_key(&13),
            "stream-table entry must be removed after is_closed() becomes true"
        );
    }

    struct CentralReadRig {
        control: MuxControl,
        close_tx: StreamCloseTxPrototype,
        stream_init_handle: StreamInitHandle,
        accept_rx: crate::stream::accepter::StreamAcceptRx,
        write_control_tx: WriteControlTx,
        write_control_rx: crate::central_io::writer::WriteControlRx,
        _open_tx: crate::stream::opener::StreamOpenTx,
        _drain: tokio::task::JoinHandle<()>,
    }
    impl CentralReadRig {
        fn drop_accepter(&mut self) {
            let (_dead_tx, dead_rx) = crate::stream::accepter::stream_accept_channel();
            self.accept_rx = dead_rx;
        }
        async fn deliver(&mut self, msg: CentralIoReadMsg) -> Result<(), HandleCentralReadError> {
            handle_central_read(
                &mut self.control,
                &self.close_tx,
                &mut self.stream_init_handle,
                &self.write_control_tx,
                msg,
            )
            .await
        }
    }

    #[test]
    fn a_reassembly_protocol_error_is_reported_through_tracing() {
        use std::sync::atomic::AtomicUsize;
        static EVENTS: AtomicUsize = AtomicUsize::new(0);
        struct Counting;
        impl tracing::Subscriber for Counting {
            fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
                true
            }
            fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::Id {
                tracing::Id::from_u64(1)
            }
            fn record(&self, _: &tracing::Id, _: &tracing::span::Record<'_>) {}
            fn record_follows_from(&self, _: &tracing::Id, _: &tracing::Id) {}
            fn event(&self, _: &tracing::Event<'_>) {
                EVENTS.fetch_add(1, Ordering::Relaxed);
            }
            fn enter(&self, _: &tracing::Id) {}
            fn exit(&self, _: &tracing::Id) {}
        }
        tracing::subscriber::with_default(Counting, || {
            tracing_reassembly_error(7, 0x40, &ReassemblyError::Overlap);
        });
        assert_eq!(
            EVENTS.load(Ordering::Relaxed),
            1,
            "the protocol error never reached the tracing subscriber"
        );
    }

    #[tokio::test]
    async fn offset_before_the_start_of_the_stream_is_rejected() {
        let mut rb = ReorderBuffer::new();
        let err = rb.ingest(0xFFFF_FFFF, buf(&[1, 2, 3, 4])).unwrap_err();
        assert!(
            matches!(err, ReassemblyError::AmbiguousOffset),
            "expected the pre-stream offset to be rejected, got {err:?}"
        );
        let mut rb = ReorderBuffer::new();
        let err = rb.set_final_offset(0xFFFF_FFFF).unwrap_err();
        assert!(
            matches!(err, ReassemblyError::AmbiguousOffset),
            "expected the pre-stream final offset to be rejected, got {err:?}"
        );
        assert!(
            rb.final_offset_abs.is_none(),
            "a pre-stream final offset must not be recorded - the stream could never reach it and would hang instead of closing"
        );
    }

    #[test]
    fn any_reordering_of_a_stream_reassembles_it_exactly() {
        struct Rng(u64);
        impl Rng {
            fn next(&mut self) -> u64 {
                let mut x = self.0;
                x ^= x >> 12;
                x ^= x << 25;
                x ^= x >> 27;
                self.0 = x;
                x.wrapping_mul(0x2545_F491_4F6C_DD1D)
            }
            fn below(&mut self, n: usize) -> usize {
                (self.next() % n as u64) as usize
            }
        }
        const TOTAL: usize = 4096;
        let stream: Vec<u8> = (0..TOTAL).map(|i| (i % 251) as u8).collect();
        for seed in 1..64u64 {
            for start in [0u64, 0xFFFF_FF00, 0x1_0000_0000, 0x7FFF_FFF0] {
                let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
                let mut rb = ReorderBuffer::new();
                rb.cursor = start;
                let mut frames: Vec<(u64, usize, usize)> = Vec::new();
                let mut off = 0usize;
                while off < TOTAL {
                    let len = (1 + rng.below(120)).min(TOTAL - off);
                    frames.push((start + off as u64, off, len));
                    off += len;
                }
                frames.extend_from_within(..);
                for i in (1..frames.len()).rev() {
                    frames.swap(i, rng.below(i + 1));
                }
                let final_at = rng.below(frames.len());
                let mut delivered: Vec<u8> = Vec::new();
                for (i, &(abs, off, len)) in frames.iter().enumerate() {
                    if i == final_at {
                        rb.set_final_offset((start + TOTAL as u64) as Offset)
                            .unwrap_or_else(|e| {
                                panic!("seed {seed} start {start:#x} early final offset: {e:?}")
                            });
                    }
                    rb.ingest(abs as Offset, buf(&stream[off..off + len]))
                        .unwrap_or_else(|e| {
                            panic!("seed {seed} start {start:#x} frame {abs:#x}+{len}: {e:?}")
                        });
                    for chunk in rb.drain_contiguous() {
                        delivered.extend_from_slice(&chunk);
                    }
                    let pending: usize = rb.pending.values().map(|d| d.len()).sum();
                    assert_eq!(
                        pending, rb.buffered_bytes,
                        "seed {seed} start {start:#x}: buffered_bytes drifted from the pending map"
                    );
                    assert!(
                        rb.pending.keys().all(|&k| k >= rb.cursor),
                        "seed {seed} start {start:#x}: a pending frame sits at or below the cursor"
                    );
                }
                assert_eq!(
                    delivered, stream,
                    "seed {seed} start {start:#x}: reassembled stream differs from what was sent"
                );
                assert_eq!(rb.cursor, start + TOTAL as u64);
                assert!(rb.pending.is_empty());
                rb.set_final_offset((start + TOTAL as u64) as Offset)
                    .unwrap();
                assert!(rb.is_complete());
            }
        }
    }

    #[test]
    fn a_peer_that_reframes_the_stream_never_corrupts_it() {
        struct Rng(u64);
        impl Rng {
            fn next(&mut self) -> u64 {
                let mut x = self.0;
                x ^= x >> 12;
                x ^= x << 25;
                x ^= x >> 27;
                self.0 = x;
                x.wrapping_mul(0x2545_F491_4F6C_DD1D)
            }
            fn below(&mut self, n: usize) -> usize {
                (self.next() % n as u64) as usize
            }
        }
        const TOTAL: usize = 4096;
        let stream: Vec<u8> = (0..TOTAL).map(|i| (i % 251) as u8).collect();
        for seed in 1..64u64 {
            for start in [0u64, 0xFFFF_FF00, 0x1_0000_0000, 0x7FFF_FFF0] {
                let mut rng = Rng(seed.wrapping_mul(0xD1B5_4A32_D192_ED03) | 1);
                let mut rb = ReorderBuffer::new();
                rb.cursor = start;
                let mut frames: Vec<(usize, usize)> = Vec::new();
                let mut off = 0usize;
                while off < TOTAL {
                    let len = (1 + rng.below(120)).min(TOTAL - off);
                    frames.push((off, len));
                    off += len;
                }
                let reframed: Vec<(usize, usize)> = (0..frames.len())
                    .map(|_| {
                        let lo = rng.below(TOTAL);
                        let len = (1 + rng.below(200)).min(TOTAL - lo);
                        (lo, len)
                    })
                    .collect();
                frames.extend_from_slice(&reframed);
                frames.extend_from_within(..frames.len() / 2);
                for i in (1..frames.len()).rev() {
                    frames.swap(i, rng.below(i + 1));
                }
                let mut delivered: Vec<u8> = Vec::new();
                for &(off, len) in &frames {
                    let abs = start + off as u64;
                    let _ = rb.ingest(abs as Offset, buf(&stream[off..off + len]));
                    for chunk in rb.drain_contiguous() {
                        delivered.extend_from_slice(&chunk);
                    }
                    assert_eq!(
                        delivered.len() as u64,
                        rb.cursor - start,
                        "seed {seed} start {start:#x}: delivered length and cursor disagree"
                    );
                    assert_eq!(
                        delivered,
                        stream[..delivered.len()],
                        "seed {seed} start {start:#x}: reframing corrupted the delivered stream"
                    );
                    let pending: usize = rb.pending.values().map(|d| d.len()).sum();
                    assert_eq!(
                        pending, rb.buffered_bytes,
                        "seed {seed} start {start:#x}: buffered_bytes drifted from the pending map"
                    );
                    assert!(
                        rb.pending.keys().all(|&k| k >= rb.cursor),
                        "seed {seed} start {start:#x}: a pending frame sits at or below the cursor"
                    );
                }
            }
        }
    }

    fn central_read_rig(frame_reassembly: bool) -> CentralReadRig {
        use crate::stream::accepter::stream_accept_channel;
        use crate::stream::opener::stream_open_channel;
        let (control, close_tx, _drain) = make_control(frame_reassembly);
        let (_open_tx, open_rx) = stream_open_channel();
        let (accept_tx, accept_rx) = stream_accept_channel();
        let (write_control_tx, write_control_rx) = write_control_channel();
        CentralReadRig {
            control,
            close_tx,
            stream_init_handle: StreamInitHandle {
                stream_open_rx: open_rx,
                stream_accept_tx: accept_tx,
            },
            accept_rx,
            write_control_tx,
            write_control_rx,
            _open_tx,
            _drain,
        }
    }

    #[tokio::test]
    async fn a_stalled_reader_does_not_freeze_the_other_streams() {
        let mut rig = central_read_rig(true);
        rig.deliver(CentralIoReadMsg::Open(1)).await.unwrap();
        let _stalled = rig.accept_rx.recv().await.unwrap();
        for i in 0..1100u32 {
            timeout(
                Duration::from_secs(3),
                rig.deliver(CentralIoReadMsg::Data(1, i, buf(&[7]))),
            )
            .await
            .expect("a stalled stream reader blocked the session control loop")
            .unwrap();
        }
        rig.deliver(CentralIoReadMsg::Open(2)).await.unwrap();
        let live = rig.accept_rx.recv().await.unwrap();
        timeout(
            Duration::from_secs(3),
            rig.deliver(CentralIoReadMsg::Data(2, 0, buf(b"hello"))),
        )
        .await
        .expect("a stalled stream reader blocked a sibling stream")
        .unwrap();
        let mut got = [0u8; 5];
        let n = timeout(
            Duration::from_secs(3),
            tokio::io::AsyncReadExt::read(&mut { live }.reader, &mut got),
        )
        .await
        .expect("the sibling stream's reader never woke")
        .unwrap();
        assert_eq!(&got[..n], b"hello");
    }

    async fn drive_until_close(frames: usize) -> (Vec<u8>, io::Result<usize>) {
        let mut rig = central_read_rig(false);
        rig.deliver(CentralIoReadMsg::Open(1)).await.unwrap();
        let mut reader = rig.accept_rx.recv().await.unwrap().reader;
        for _ in 0..frames {
            rig.deliver(CentralIoReadMsg::Data(1, 0, buf(&[7])))
                .await
                .unwrap();
        }
        timeout(
            Duration::from_secs(3),
            rig.deliver(CentralIoReadMsg::Close(1, Side::Write, 0)),
        )
        .await
        .expect("CloseWrite waited on a full read queue, freezing the session")
        .unwrap();
        let mut got = Vec::new();
        let res = timeout(
            Duration::from_secs(3),
            tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut got),
        )
        .await
        .expect("the reader never reached the end of its stream - it was told neither");
        (got, res)
    }

    #[tokio::test]
    async fn a_reader_a_queue_behind_still_gets_the_end_of_its_stream() {
        let frames = crate::stream::reader::CHANNEL_SIZE - 1;
        let (got, res) = drive_until_close(frames).await;
        assert!(
            res.is_ok(),
            "the end of the stream surfaced as an error instead of EOF"
        );
        assert_eq!(got, vec![7; frames]);
    }

    #[tokio::test]
    async fn a_reader_that_overruns_its_queue_still_reaches_an_end() {
        let (_got, res) = drive_until_close(crate::stream::reader::CHANNEL_SIZE).await;
        assert!(
            res.is_err(),
            "the stream outran its read queue, so its reader owes an error, not a clean EOF"
        );
    }

    #[tokio::test]
    async fn a_peer_stream_does_not_reserve_the_server_s_own_id() {
        let (mut control, _close_tx, _drain) = make_control(false);
        let _peer = open_test_stream(&mut control, 0).await;
        assert_eq!(
            control.next_stream_id().unwrap(),
            1 << (StreamId::BITS - 1),
            "a stream in the peer's half of the id space blocked a free local id"
        );
    }

    #[tokio::test]
    async fn a_wrapped_allocator_never_hands_out_a_live_stream_s_id() {
        let (mut control, _close_tx, _drain) = make_control(false);
        let live = control.next_stream_id().unwrap();
        let _live_rx = open_test_stream(&mut control, live).await;
        control.next_possible_local_stream_id = 0;
        let next = control.next_stream_id().unwrap();
        assert!(
            !control.stream_table.contains_key(&next),
            "next_stream_id handed out {next:#x}, which is still live"
        );
    }

    #[tokio::test]
    async fn empty_data_frame_is_not_forwarded_as_eof() {
        let (mut control, _close_tx, _drain) = make_control(false);
        let mut rx = open_test_stream(&mut control, 7).await;
        control.try_dispatch_data(7, buf(&[])).unwrap();
        match rx.try_recv() {
            Err(()) => {}
            Ok(msg) => panic!("an empty frame reached the reader as a forged EOF: {msg:?}"),
        }
        control.try_dispatch_data(7, buf(&[0xCC; 4])).unwrap();
        match rx
            .try_recv()
            .expect("the real frame after it must still arrive")
        {
            StreamReadDataMsg::Data(d) => assert_eq!(&d[..], &[0xCC; 4]),
            other => panic!("expected Data, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn stray_frame_for_a_local_id_fabricates_no_stream() {
        for reassembly in [true, false] {
            let mut rig = central_read_rig(reassembly);
            let local_id: StreamId = 1 << (StreamId::BITS - 1);
            assert!(rig.control.is_local_opened_stream(local_id));
            for msg in [
                CentralIoReadMsg::Open(local_id),
                CentralIoReadMsg::Data(local_id, 0, buf(&[0xAA])),
                CentralIoReadMsg::Close(local_id, Side::Write, 1),
            ] {
                let described = format!("{msg:?}");
                rig.deliver(msg).await.unwrap();
                assert!(
                    !rig.control.stream_table.contains_key(&local_id),
                    "{described} (reassembly={reassembly}) created a stream on an id only our own `open` allocates"
                );
                assert!(
                    rig.accept_rx.try_recv().is_err(),
                    "{described} (reassembly={reassembly}) handed the application a phantom accepted stream on an id it opens itself"
                );
            }
        }
    }

    #[tokio::test]
    async fn peer_open_cannot_replace_a_live_local_stream() {
        let mut rig = central_read_rig(false);
        let (tx, mut local_rx) = stream_read_data_channel();
        let (local_id, _write_tx) = rig
            .control
            .open(tx, WriteBrokenPipe::new(), None)
            .await
            .unwrap();
        assert!(rig.control.is_local_opened_stream(local_id));
        rig.deliver(CentralIoReadMsg::Open(local_id)).await.unwrap();
        assert!(
            rig.accept_rx.try_recv().is_err(),
            "a peer 'Open' for an id we allocated was accepted as a new inbound stream"
        );
        rig.control
            .try_dispatch_data(local_id, buf(&[0xCC]))
            .unwrap();
        let msg = local_rx.try_recv().expect(
            "the peer 'Open' replaced the live local stream's state, so its reader no longer receives data");
        match msg {
            StreamReadDataMsg::Data(data) => assert_eq!(&data[..], &[0xCC]),
            other => panic!("expected Data, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn duplicate_peer_open_cannot_replace_a_live_stream() {
        for reassembly in [false, true] {
            let mut rig = central_read_rig(reassembly);
            let peer_id: StreamId = 7;
            assert!(!rig.control.is_local_opened_stream(peer_id));
            let mut peer_rx = open_test_stream(&mut rig.control, peer_id).await;
            rig.deliver(CentralIoReadMsg::Open(peer_id)).await.unwrap();
            assert!(
                rig.accept_rx.try_recv().is_err(),
                "(reassembly={reassembly}) a duplicate peer 'Open' was accepted as a second inbound stream on an id that is already live"
            );
            rig.control
                .try_dispatch_data(peer_id, buf(&[0xCC]))
                .unwrap();
            let msg = peer_rx.try_recv().unwrap_or_else(|_| panic!(
                "(reassembly={reassembly}) the duplicate peer 'Open' replaced the live stream's state, so its reader no longer receives data"));
            match msg {
                StreamReadDataMsg::Data(data) => assert_eq!(&data[..], &[0xCC]),
                other => panic!("expected Data, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn a_stream_the_accepter_cannot_take_is_not_left_in_the_table() {
        let peer_id: StreamId = 7;
        for (reassembly, msg) in [
            (false, CentralIoReadMsg::Open(peer_id)),
            (true, CentralIoReadMsg::Open(peer_id)),
            (true, CentralIoReadMsg::Data(peer_id, 0, buf(&[0xAA; 4]))),
            (true, CentralIoReadMsg::Close(peer_id, Side::Write, 4)),
        ] {
            let described = format!("{msg:?}");
            let mut rig = central_read_rig(reassembly);
            rig.drop_accepter();
            let _ = rig.deliver(msg).await;
            assert!(
                !rig.control.stream_table.contains_key(&peer_id),
                "{described} (reassembly={reassembly}) left a stream in the table that no reader or writer can ever close - the peer leaks one entry per stream it opens"
            );
        }
    }

    #[tokio::test]
    async fn a_repeated_reassembly_error_emits_one_close_read() {
        let mut rig = central_read_rig(true);
        let _reader_rx = open_test_stream(&mut rig.control, 7).await;
        for _ in 0..3 {
            rig.deliver(CentralIoReadMsg::Data(7, 0xFFFF_FFFF, buf(&[0xAA; 4])))
                .await
                .unwrap();
        }
        assert!(matches!(
            rig.write_control_rx.recv().await.unwrap(),
            WriteControlMsg::CloseRead(7)
        ));
        assert!(
            timeout(Duration::from_millis(10), rig.write_control_rx.recv())
                .await
                .is_err(),
            "every stray frame after the teardown amplified into another CloseRead on the session-wide control lane"
        );
    }
}
