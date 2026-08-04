use std::collections::BTreeMap;

use crate::protocol::{Offset, StreamId};

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
pub(crate) struct ReorderBuffer {
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
pub(crate) enum ReassemblyError {
    Overlap,
    OutOfWindow,
    BufferOverflow,
    RangeOverflow,
    BeyondFinalOffset,
    FinalOffsetConflict,
    AmbiguousOffset,
}

impl ReorderBuffer {
    pub(crate) fn new() -> Self {
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
    pub(crate) fn ingest(
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
                return Err(ReassemblyError::BeyondFinalOffset);
            }
            if end_abs > fin {
                return Err(ReassemblyError::BeyondFinalOffset);
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
        if let Some((&succ_abs, _succ_data)) = self.pending.range(abs + 1..).next()
            && end_abs > succ_abs
        {
            return Err(ReassemblyError::Overlap);
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
    pub(crate) fn drain_contiguous(&mut self) -> Vec<crate::central_io::DataBuf> {
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
    pub(crate) fn set_final_offset(&mut self, final_offset: Offset) -> Result<(), ReassemblyError> {
        let Some(fin_abs) = self.wire_to_abs(final_offset) else {
            return Err(ReassemblyError::AmbiguousOffset);
        };
        if let Some(existing) = self.final_offset_abs {
            return if fin_abs == existing {
                Ok(())
            } else {
                Err(ReassemblyError::FinalOffsetConflict)
            };
        }
        if fin_abs < self.cursor {
            return Err(ReassemblyError::FinalOffsetConflict);
        }
        for (&off, data) in &self.pending {
            if off + data.len() as u64 > fin_abs {
                return Err(ReassemblyError::BeyondFinalOffset);
            }
        }
        self.final_offset_abs = Some(fin_abs);
        Ok(())
    }

    /// True once the delivered cursor has reached the final offset.
    pub(crate) fn is_complete(&self) -> bool {
        self.final_offset_abs == Some(self.cursor)
    }
}

pub(crate) fn tracing_reassembly_error(stream_id: StreamId, offset: Offset, e: &ReassemblyError) {
    tracing::warn!(stream_id, offset, error = ?e, "mux reassembly protocol error");
}

#[cfg(test)]
mod reassembly_tests {
    use super::*;
    use crate::central_io::DataBuf;
    use primitive::arena::obj_pool::arc_buf_pool;
    use std::sync::atomic::Ordering;

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
}
