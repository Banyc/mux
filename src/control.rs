use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use primitive::ops::ring::RingSpace;

use crate::{
    central_io::{
        reader::{CentralIoReadMsg, CentralIoReadRx},
        writer::{StreamWriteDataTx, WriteControlMsg, WriteControlTx, WriteDataTxPrototype},
        DeadCentralIo,
    },
    common::Side,
    protocol::{offset_less, Offset, StreamId},
    stream::{
        accepter::StreamAcceptMsg,
        opener::StreamOpenMsg,
        reader::{stream_read_data_channel, StreamReadDataMsg, StreamReadDataTx},
        stream_close_channel,
        writer::LiveStreamWriter,
        DeadStream, DeadStreamInit, StreamCloseTxPrototype, StreamInitHandle,
    },
    StreamReader, StreamWriter,
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
                control.local_close(msg.stream_id, msg.side);
                if !msg.already_sent_to_peer {
                    let control_msg = WriteControlMsg::Close(msg.stream_id, msg.side);
                    if let Err(e) = write_control_tx.send(control_msg).await {
                        break e;
                    };
                }
            }
            Ok(msg) = stream_init_handle.stream_open_rx.recv() => {
                match handle_local_open(&mut control, &stream_close_tx, &write_control_tx, msg).await {
                    Ok(_) => (),
                    Err(HandleLocalOpenError::DeadCentralIo(e)) => break e,
                }
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
#[derive(Debug)]
pub enum RunControlError {
    DeadCentralIo(DeadCentralIo, StreamInitHandle),
}

async fn handle_local_open(
    control: &mut MuxControl,
    stream_close_tx: &StreamCloseTxPrototype,
    write_control_tx: &WriteControlTx,
    msg: StreamOpenMsg,
) -> Result<(), HandleLocalOpenError> {
    let res = open_stream(control, stream_close_tx, None).await;
    let (resp, stream_id) = match res {
        Ok((stream_id, msg)) => (Ok(msg), Some(stream_id)),
        Err(e) => (Err(e), None),
    };
    // Queue the Open control frame on the central writer BEFORE resolving
    // the opener. This guarantees the peer learns about the new stream
    // before any data the opener writes can reach the wire, preventing data
    // from overtaking Open on the central byte stream (which would cause the
    // receiver to drop data for an unknown stream).
    if let Some(stream_id) = stream_id {
        let control_msg = WriteControlMsg::Open(stream_id);
        write_control_tx
            .send(control_msg)
            .await
            .map_err(HandleLocalOpenError::DeadCentralIo)?;
    }
    // If the opener's response channel is closed, the opener gave up; skip
    // this stream but keep the control loop alive.
    let _ = msg.stream.send(resp);
    Ok(())
}

enum HandleLocalOpenError {
    DeadCentralIo(DeadCentralIo),
}
async fn handle_central_read(
    control: &mut MuxControl,
    stream_close_tx: &StreamCloseTxPrototype,
    stream_init_handle: &mut StreamInitHandle,
    msg: CentralIoReadMsg,
) -> Result<(), HandleCentralReadError> {
    match msg {
        CentralIoReadMsg::Open(stream_id) => {
            if control.frame_reassembly && control.stream_table.contains_key(&stream_id) {
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
            stream_init_handle
                .stream_accept_tx
                .send(stream)
                .await
                .map_err(HandleCentralReadError::DeadStreamInit)?;
        }
        CentralIoReadMsg::Close(stream_id, side, final_offset) => {
            if control.frame_reassembly && side == Side::Write {
                if let Err(()) = control.peer_close_write_with_offset(stream_id, final_offset).await
                {
                    control.local_close(stream_id, Side::Read);
                }
            } else {
                control.peer_close(stream_id, side).await;
            }
        }
        CentralIoReadMsg::Data(stream_id, offset, data_buf) => {
            if control.frame_reassembly {
                if control
                    .ingest_reassembly(stream_id, offset, data_buf, stream_close_tx)
                    .await
                    .is_err()
                {
                    control.local_close(stream_id, Side::Read);
                }
            } else {
                let Some(dispatcher) = control.dispatcher(stream_id) else {
                    return Ok(());
                };
                let msg = StreamReadDataMsg::Data(data_buf);
                let _ = dispatcher.send(msg).await;
            }
        }
    }
    Ok(())
}
enum HandleCentralReadError {
    DeadCentralIo(DeadCentralIo),
    DeadStreamInit(DeadStreamInit),
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
    fn next_stream_id(&mut self) -> Result<StreamId, TooManyOpenStreams> {
        let max_local_stream_id = StreamId::MAX >> 1;
        if usize::try_from(max_local_stream_id).unwrap() <= self.local_opened_streams {
            return Err(TooManyOpenStreams {});
        }
        let mut next_local_stream_id = self.next_possible_local_stream_id;
        let local_stream_id = loop {
            if !self.stream_table.contains_key(&next_local_stream_id) {
                break next_local_stream_id;
            }
            next_local_stream_id = next_local_stream_id.ring_add(1, max_local_stream_id);
        };
        self.next_possible_local_stream_id = local_stream_id.ring_add(1, max_local_stream_id);
        let stream_id = if self.should_stream_id_set_first_bit() {
            local_stream_id | (1 << (StreamId::BITS - 1))
        } else {
            local_stream_id
        };
        Ok(stream_id)
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
    pub async fn peer_close(&mut self, stream_id: StreamId, side: Side) {
        let Some(stream) = self.stream_table.get_mut(&stream_id) else {
            return;
        };
        let _ = stream.peer_close(side).await;
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
    pub async fn open(
        &mut self,
        dispatcher: StreamReadDataTx,
        broken_pipe: WriteBrokenPipe,
        stream_id: Option<StreamId>,
    ) -> Result<(StreamId, StreamWriteDataTx), ControlOpenError> {
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
                .derive(stream_id)
                .await
                .map_err(ControlOpenError::DeadCentralIo)?,
        ))
    }

    /// Ingest one out-of-order Data frame for `stream_id` into the stream's
    /// reorder buffer, releasing any newly-contiguous bytes to the reader.
    /// Returns `Err(())` if the frame is a protocol error on this stream
    /// (out-of-window offset, or a duplicate/overlapping range that the
    /// buffer cannot accept); the caller closes the stream's read side but
    /// leaves the session alive.
    ///
    /// If the stream is unknown (Data arrived before Open), it is created
    /// implicitly via the normal open path so the peer's Open (when it
    /// arrives) is a no-op.
    async fn ingest_reassembly(
        &mut self,
        stream_id: StreamId,
        offset: Offset,
        data: crate::central_io::DataBuf,
        stream_close_tx: &StreamCloseTxPrototype,
    ) -> Result<(), ()> {
        if !self.stream_table.contains_key(&stream_id) {
            let res = open_stream(self, stream_close_tx, Some(stream_id)).await;
            if res.is_err() {
                return Err(());
            }
        }
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
            let _ = dispatcher.send(StreamReadDataMsg::Data(chunk)).await;
        }
        // If CloseWrite already arrived and this frame filled the final
        // gap, complete the stream now.
        if reassembly.is_complete() && !stream.is_peer_write_closed {
            stream.is_peer_write_closed = true;
            let _ = stream.read_dispatcher.send(StreamReadDataMsg::Fin).await;
        }
        Ok(())
    }

    /// Peer CloseWrite carrying the stream's final byte offset. Pending
    /// frames below `final_offset` are still released in order; once the
    /// reorder buffer has delivered every byte up to `final_offset`, the
    /// reader receives Fin. If `final_offset` is behind the delivered
    /// cursor or any pending frame would exceed it, the stream is in
    /// error and the caller closes its read side.
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
            let _ = dispatcher.send(StreamReadDataMsg::Data(chunk)).await;
        }
        if reassembly.is_complete() {
            stream.is_peer_write_closed = true;
            let _ = stream
                .read_dispatcher
                .send(StreamReadDataMsg::Fin)
                .await;
        }
        Ok(())
    }
}
#[derive(Debug)]
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
    pub async fn peer_close(&mut self, side: Side) -> Result<(), DeadStream> {
        match side {
            Side::Read => {
                self.write_broken_pipe.close();
            }
            Side::Write => {
                if self.is_peer_write_closed {
                    return Ok(());
                }
                self.is_peer_write_closed = true;
                self.read_dispatcher.send(StreamReadDataMsg::Fin).await?;
            }
        }
        Ok(())
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
/// out-of-order Data frames keyed by their byte offset and releases
/// contiguous bytes to the reader strictly in offset order.
///
/// Frames are stored in a `BTreeMap<Offset, DataBuf>` so the
/// lowest-offset pending frame is always at the front. `ingest`
/// rejects duplicates, overlaps, out-of-window offsets, and
/// overflow of the buffered-bytes / range bounds. `drain_contiguous`
/// pops every frame whose offset equals `next_offset` and advances.
#[derive(Debug)]
struct ReorderBuffer {
    /// Next byte offset to deliver (the reader's contiguous cursor).
    next_offset: Offset,
    /// Pending frames keyed by their start offset. Always contains
    /// only frames strictly ahead of `next_offset` (drain removes
    /// contiguous ones immediately).
    pending: BTreeMap<Offset, crate::central_io::DataBuf>,
    /// Total bytes currently buffered across all pending frames.
    buffered_bytes: usize,
    /// Highest byte offset the stream will ever receive, set by
    /// CloseWrite. The stream is complete once `next_offset` reaches
    /// this value. `None` until CloseWrite arrives.
    final_offset: Option<Offset>,
}

#[derive(Debug)]
enum ReassemblyError {
    Overlap,
    OutOfWindow,
    BufferOverflow,
    RangeOverflow,
    FinalOffsetBeforeCursor,
}

impl ReorderBuffer {
    fn new() -> Self {
        Self {
            next_offset: 0,
            pending: BTreeMap::new(),
            buffered_bytes: 0,
            final_offset: None,
        }
    }

    /// Ingest one complete frame at `offset`. Duplicates (offset+len ≤
    /// next_offset) are silently dropped (idempotent). Overlaps and
    /// out-of-window offsets are errors.
    fn ingest(
        &mut self,
        offset: Offset,
        data: crate::central_io::DataBuf,
    ) -> Result<(), ReassemblyError> {
        // Zero-length frames are no-ops (a writer may emit them e.g. when
        // a write is split at exactly BodyLen::MAX and the final chunk is
        // empty). Drop silently.
        if data.is_empty() {
            return Ok(());
        }
        let len = data.len() as u32;
        let end = offset.wrapping_add(len);
        // If final_offset is set, the frame must lie entirely within
        // [.. final_offset). A frame starting at or past it, or ending
        // past it, is a protocol error.
        if let Some(fin) = self.final_offset {
            if !offset_less(offset, fin) {
                return Err(ReassemblyError::FinalOffsetBeforeCursor);
            }
            if offset_less(fin, end) && fin != end {
                return Err(ReassemblyError::FinalOffsetBeforeCursor);
            }
        }
        // Already fully delivered (end ≤ next_offset in wrap-aware
        // terms): idempotent drop. `end == next_offset` is also fully
        // delivered.
        if !offset_less(self.next_offset, end) {
            return Ok(());
        }
        // Out-of-window: offset too far ahead of next_offset (gap >
        // REASSEMBLY_MAX_RANGE_BYTES). Implies a gap larger than 2 GiB
        // or a corrupt/malicious sender.
        if offset_less(self.next_offset, offset) {
            let gap = offset.wrapping_sub(self.next_offset) as usize;
            if gap > REASSEMBLY_MAX_RANGE_BYTES {
                return Err(ReassemblyError::OutOfWindow);
            }
        }
        // Overlap with next_offset: trim the already-delivered prefix so
        // the frame starts exactly at next_offset. If the frame is
        // entirely behind next_offset it was caught by the early
        // already-delivered check.
        let (mut offset, mut data) = (offset, data);
        if offset_less(offset, self.next_offset) {
            let trim = self.next_offset.wrapping_sub(offset) as usize;
            if trim >= data.len() {
                return Ok(());
            }
            // Keep the suffix starting at `trim`; drop the already-
            // delivered prefix.
            data.drain(..trim);
            offset = self.next_offset;
        }
        let len = data.len() as u32;
        let end = offset.wrapping_add(len);
        // Exact duplicate of a buffered frame (same offset, same len):
        // idempotent drop.
        if let Some(existing) = self.pending.get(&offset) {
            if existing.len() as u32 == len {
                return Ok(());
            }
            return Err(ReassemblyError::Overlap);
        }
        // Overlap with a preceding buffered frame whose range extends
        // into ours: error (partial overlaps aren't trim-able without
        // splitting the existing frame, which we reject for simplicity).
        if let Some((&prev_off, prev_data)) = self.pending.range(..offset).next_back() {
            let prev_end = prev_off.wrapping_add(prev_data.len() as u32);
            if offset_less(offset, prev_end) {
                if prev_end == end && prev_data.len() as u32 == len {
                    return Ok(()); // exact dup of a buffered frame
                }
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
        if offset_less(self.next_offset, offset) {
            let gap = offset.wrapping_sub(self.next_offset) as usize;
            if gap + data.len() > REASSEMBLY_MAX_RANGE_BYTES {
                return Err(ReassemblyError::RangeOverflow);
            }
        }
        self.buffered_bytes = new_buffered;
        self.pending.insert(offset, data);
        Ok(())
    }

    /// Pop every contiguous frame starting at `next_offset` and advance
    /// the cursor. Returns the released frames in offset order.
    fn drain_contiguous(&mut self) -> Vec<crate::central_io::DataBuf> {
        let mut out = Vec::new();
        loop {
            let off = self.next_offset;
            let Some(entry) = self.pending.remove_entry(&off) else {
                break;
            };
            let len = entry.1.len() as u32;
            self.buffered_bytes = self.buffered_bytes.saturating_sub(entry.1.len());
            self.next_offset = off.wrapping_add(len);
            out.push(entry.1);
        }
        out
    }

    /// Record the stream's final byte offset (from CloseWrite). Returns
    /// `Err` if `final_offset` is behind the delivered cursor or a
    /// pending frame extends past it.
    fn set_final_offset(&mut self, final_offset: Offset) -> Result<(), ReassemblyError> {
        if self.final_offset.is_some() {
            return Err(ReassemblyError::FinalOffsetBeforeCursor);
        }
        // final_offset must be >= next_offset (in wrap-aware terms).
        if offset_less(final_offset, self.next_offset) {
            return Err(ReassemblyError::FinalOffsetBeforeCursor);
        }
        // No pending frame may extend past final_offset.
        for (&off, data) in &self.pending {
            let end = off.wrapping_add(data.len() as u32);
            if offset_less(final_offset, end) {
                return Err(ReassemblyError::FinalOffsetBeforeCursor);
            }
        }
        self.final_offset = Some(final_offset);
        Ok(())
    }

    /// True once the delivered cursor has reached the final offset.
    fn is_complete(&self) -> bool {
        self.final_offset == Some(self.next_offset)
    }
}

fn tracing_reassembly_error(stream_id: StreamId, offset: Offset, e: &ReassemblyError) {
    eprintln!(
        "mux: reassembly protocol error on stream {stream_id} at offset {offset:#x}: {e:?}"
    );
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

#[derive(Debug)]
pub struct TooManyOpenStreams {}

#[cfg(test)]
mod reassembly_tests {
    use super::*;
    use crate::central_io::DataBuf;
    use primitive::arena::obj_pool::arc_buf_pool;

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
        assert_eq!(rb.next_offset, 8);
    }

    /// Duplicate and overlapping ranges are dropped idempotently (exact
    /// dups) or rejected (partial overlaps).
    #[tokio::test]
    async fn duplicate_and_overlapping_ranges_dropped_idempotently() {
        let mut rb = ReorderBuffer::new();
        rb.ingest(0, buf(&[1, 2, 3, 4])).unwrap();
        let out = collect(rb.drain_contiguous());
        assert_eq!(out, [1, 2, 3, 4]);
        assert_eq!(rb.next_offset, 4);

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

        // Overlap with next_offset (partially delivered): trimmed.
        rb.ingest(6, buf(&[11, 12, 13, 14])).unwrap();
        // offset 6 < next_offset 4? No, 6 > 4. This is a normal frame at 6,
        // filling the gap. Let's test actual overlap with next_offset:
        let mut rb2 = ReorderBuffer::new();
        rb2.ingest(0, buf(&[1, 2, 3, 4])).unwrap();
        let _ = collect(rb2.drain_contiguous());
        // next_offset is now 4. A frame at offset 2 with 6 bytes overlaps
        // the delivered prefix [2,4); the suffix [4,8) should be kept.
        rb2.ingest(2, buf(&[10, 20, 30, 40, 50, 60])).unwrap();
        let out = collect(rb2.drain_contiguous());
        assert_eq!(out, [30, 40, 50, 60]);
        assert_eq!(rb2.next_offset, 8);
    }

    /// Overflow of REASSEMBLY_MAX_BUFFERED_BYTES kills the stream (returns
    /// Err), not the session. The buffer itself survives.
    #[tokio::test]
    async fn reorder_buffer_bound_kills_stream_not_session() {
        let mut rb = ReorderBuffer::new();
        // A frame far ahead creates a large gap. Fill it with a big frame
        // at offset 0 (so next_offset advances), then test the buffered-
        // bytes bound by exceeding it.
        // First, advance next_offset to 4.
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
        let err = rb
            .ingest(far_offset, buf(&[0; 4]))
            .unwrap_err();
        assert!(
            matches!(err, ReassemblyError::RangeOverflow | ReassemblyError::OutOfWindow),
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
        assert_eq!(rb.next_offset, 4);

        // Advance to near the wrap point.
        rb.next_offset = 0xFFFF_FFFC;
        rb.ingest(0xFFFF_FFFC, buf(&[1, 2, 3, 4])).unwrap();
        let out = collect(rb.drain_contiguous());
        assert_eq!(out, [1, 2, 3, 4]);
        assert_eq!(rb.next_offset, 0, "wrapped to 0");

        // A frame at offset 0 is now the next expected; a frame at
        // 0xFFFF_FFF8 is "before" next_offset (already delivered in
        // wrap-aware terms).
        rb.ingest(0, buf(&[5, 6, 7, 8])).unwrap();
        let out = collect(rb.drain_contiguous());
        assert_eq!(out, [5, 6, 7, 8]);
        assert_eq!(rb.next_offset, 4);

        // An "old" frame (offset behind next_offset in wrap space) is
        // idempotently dropped.
        rb.ingest(0xFFFF_FFF8, buf(&[0; 4])).unwrap();
        assert!(rb.drain_contiguous().is_empty());
    }

    // ---- End-to-end reassembly tests via MuxControl ----

    use crate::stream::stream_close_channel;
    use crate::stream::reader::{stream_read_data_channel, StreamReadDataRx, StreamReadDataMsg};
    use crate::control::WriteBrokenPipe;

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
        let drain = tokio::spawn(async move {
            while rx.recv().await.is_ok() {}
        });
        (control, close_tx, drain)
    }

    /// Open a stream with a fresh (dispatcher, receiver) pair we control.
    async fn open_test_stream(
        control: &mut MuxControl,
        stream_id: StreamId,
    ) -> StreamReadDataRx {
        let (tx, rx) = stream_read_data_channel();
        let bp = WriteBrokenPipe::new();
        control.open(tx, bp, Some(stream_id)).await.unwrap();
        rx
    }

    /// Stream A has a gap (frame at offset 0 missing); stream B's
    /// complete frames deliver immediately because each stream
    /// reassembles independently.
    #[tokio::test]
    async fn streams_pass_each_other() {
        let (mut control, close_tx, _drain) = make_control(true);
        let mut rx_a = open_test_stream(&mut control, 100).await;
        let mut rx_b = open_test_stream(&mut control, 200).await;

        // Stream A: deliver offset 4 first (gap at 0).
        control
            .ingest_reassembly(100, 4, buf(&[0xAA; 4]), &close_tx)
            .await
            .unwrap();
        // Stream B: deliver offset 0 (complete).
        control
            .ingest_reassembly(200, 0, buf(&[0xBB; 4]), &close_tx)
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
            .ingest_reassembly(100, 0, buf(&[0xAA; 4]), &close_tx)
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

    /// Data for an unknown stream_id implicitly creates the stream; a
    /// later Open is a no-op.
    #[tokio::test]
    async fn data_before_open_implicitly_creates_stream() {
        let (mut control, close_tx, _drain) = make_control(true);

        // Data for stream 42 arrives before Open.
        control
            .ingest_reassembly(42, 0, buf(&[1, 2, 3]), &close_tx)
            .await
            .unwrap();

        // Stream 42 now exists (implicitly created).
        assert!(control.stream_table.contains_key(&42));

        // Open for stream 42 is a no-op (stream already exists).
        // handle_central_read checks stream_table and short-circuits.
        assert!(control.stream_table.contains_key(&42));

        // Ingest another frame; it delivers.
        control
            .ingest_reassembly(42, 3, buf(&[4, 5]), &close_tx)
            .await
            .unwrap();
    }

    /// CloseWrite's final offset completes the stream even if some Data
    /// frames arrive after the CloseWrite frame (reordering).
    #[tokio::test]
    async fn closewrite_final_offset_completes_stream_despite_reordering() {
        let (mut control, close_tx, _drain) = make_control(true);
        let mut rx = open_test_stream(&mut control, 7).await;

        // Deliver offset 0 and offset 4 (total 8 bytes).
        control
            .ingest_reassembly(7, 0, buf(&[0; 4]), &close_tx)
            .await
            .unwrap();
        control
            .ingest_reassembly(7, 4, buf(&[1; 4]), &close_tx)
            .await
            .unwrap();

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
        let (mut control2, close_tx2, _drain2) = make_control(true);
        let mut rx2 = open_test_stream(&mut control2, 8).await;

        // Deliver offset 0, then CloseWrite(final=8) — gap at 4.
        control2
            .ingest_reassembly(8, 0, buf(&[0; 4]), &close_tx2)
            .await
            .unwrap();
        control2.peer_close_write_with_offset(8, 8).await.unwrap();

        // Offset 0 frame delivered, but stream NOT complete (gap at 4).
        let msg = rx2.try_recv().expect("first frame delivered");
        assert!(matches!(msg, StreamReadDataMsg::Data(_)));
        assert!(
            rx2.try_recv().is_err(),
            "no Fin yet — gap at 4"
        );

        // Now fill the gap (CloseWrite arrived before this Data frame).
        control2
            .ingest_reassembly(8, 4, buf(&[1; 4]), &close_tx2)
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
        let (mut control, close_tx, _drain) = make_control(true);
        let mut rx = open_test_stream(&mut control, 5).await;

        // Simulate the writer splitting a 200_000-byte write into
        // multiple Data frames at BodyLen::MAX (65535) boundaries.
        let total = 200_000u32;
        let payload: Vec<u8> = (0u8..).cycle().take(total as usize).collect();
        let mut offset = 0u32;
        while offset < total {
            let len = (total - offset).min(u16::MAX as u32) as usize;
            control
                .ingest_reassembly(
                    5,
                    offset,
                    buf(&payload[offset as usize..offset as usize + len]),
                    &close_tx,
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
}
