use std::io::{self, Read, Write};

pub type StreamId = u32;

#[derive(Debug, Clone)]
pub enum Header {
    Heartbeat,
    Open,
    Data,
    CloseRead,
    CloseWrite,
}
impl Header {
    pub const SIZE: usize = 1;
    pub fn decode(buf: [u8; Self::SIZE]) -> Option<Self> {
        let mut rdr = io::Cursor::new(&buf[..]);
        let mut code = [0];
        rdr.read_exact(&mut code).unwrap();
        assert_eq!(rdr.read(&mut [0]).unwrap(), 0);
        let code = code[0];
        Some(match code {
            0 => Self::Heartbeat,
            1 => Self::Open,
            2 => Self::Data,
            3 => Self::CloseRead,
            4 => Self::CloseWrite,
            _ => return None,
        })
    }
    pub fn encode(&self) -> [u8; Self::SIZE] {
        let code = match self {
            Header::Heartbeat => 0,
            Header::Open => 1,
            Header::Data => 2,
            Header::CloseRead => 3,
            Header::CloseWrite => 4,
        };
        [code]
    }
}

#[derive(Debug, Clone)]
pub struct StreamIdMsg {
    pub stream_id: StreamId,
}
impl StreamIdMsg {
    pub const SIZE: usize = core::mem::size_of::<StreamId>();
    pub fn decode(buf: [u8; Self::SIZE]) -> Self {
        let mut rdr = io::Cursor::new(&buf[..]);
        let mut stream = [0; 4];
        rdr.read_exact(&mut stream).unwrap();
        let stream = StreamId::from_be_bytes(stream);
        assert_eq!(rdr.read(&mut [0]).unwrap(), 0);
        Self { stream_id: stream }
    }
    pub fn encode(&self) -> [u8; Self::SIZE] {
        let mut buf = [0; Self::SIZE];
        let mut wtr = io::Cursor::new(&mut buf[..]);
        wtr.write_all(&self.stream_id.to_be_bytes()).unwrap();
        assert_eq!(wtr.write(&[0]).unwrap(), 0);
        buf
    }
}

pub type BodyLen = u16;
#[derive(Debug, Clone)]
pub struct DataHeader {
    pub stream_id: StreamId,
    pub body_len: BodyLen,
}
impl DataHeader {
    pub const SIZE: usize = core::mem::size_of::<StreamId>() + core::mem::size_of::<BodyLen>();
    pub fn decode(buf: [u8; Self::SIZE]) -> Self {
        let mut rdr = io::Cursor::new(&buf[..]);
        let mut stream = [0; 4];
        rdr.read_exact(&mut stream).unwrap();
        let stream = StreamId::from_be_bytes(stream);
        let mut body_len = [0; 2];
        rdr.read_exact(&mut body_len).unwrap();
        let body_len = BodyLen::from_be_bytes(body_len);
        assert_eq!(rdr.read(&mut [0]).unwrap(), 0);
        Self {
            stream_id: stream,
            body_len,
        }
    }
    pub fn encode(&self) -> [u8; Self::SIZE] {
        let mut buf = [0; Self::SIZE];
        let mut wtr = io::Cursor::new(&mut buf[..]);
        wtr.write_all(&self.stream_id.to_be_bytes()).unwrap();
        wtr.write_all(&self.body_len.to_be_bytes()).unwrap();
        assert_eq!(wtr.write(&[0]).unwrap(), 0);
        buf
    }
}

/// Per-stream byte offset carried in a Data frame when frame-reassembly
/// mode is enabled. TCP-style serial-number: comparisons wrap, but an
/// offset outside the valid comparison window (≈2 GiB ahead or behind the
/// next-expected offset) is a protocol error on that stream. At 20 MiB/s
/// the u32 space wraps every ~3.5 minutes, which is longer than any
/// practical single-stream stall under reassembly.
pub type Offset = u32;

/// Extended Data header used when `frame_reassembly` is enabled. Lays
/// out as `stream_id (u32 BE) | body_len (u16 BE) | offset (u32 BE)`,
/// i.e. the stock `DataHeader` fields followed by a per-stream byte
/// offset. The stock `Header::Data` code byte precedes this sub-header,
/// so mode-on and mode-off frames are distinguishable only by the
/// agreed-upon config — there is no in-band negotiation.
#[derive(Debug, Clone)]
pub struct DataHeaderExt {
    pub stream_id: StreamId,
    pub body_len: BodyLen,
    pub offset: Offset,
}
impl DataHeaderExt {
    pub const SIZE: usize = core::mem::size_of::<StreamId>()
        + core::mem::size_of::<BodyLen>()
        + core::mem::size_of::<Offset>();
    pub fn decode(buf: [u8; Self::SIZE]) -> Self {
        let mut rdr = io::Cursor::new(&buf[..]);
        let mut stream = [0; 4];
        rdr.read_exact(&mut stream).unwrap();
        let stream = StreamId::from_be_bytes(stream);
        let mut body_len = [0; 2];
        rdr.read_exact(&mut body_len).unwrap();
        let body_len = BodyLen::from_be_bytes(body_len);
        let mut offset = [0; 4];
        rdr.read_exact(&mut offset).unwrap();
        let offset = Offset::from_be_bytes(offset);
        assert_eq!(rdr.read(&mut [0]).unwrap(), 0);
        Self {
            stream_id: stream,
            body_len,
            offset,
        }
    }
    pub fn encode(&self) -> [u8; Self::SIZE] {
        let mut buf = [0; Self::SIZE];
        let mut wtr = io::Cursor::new(&mut buf[..]);
        wtr.write_all(&self.stream_id.to_be_bytes()).unwrap();
        wtr.write_all(&self.body_len.to_be_bytes()).unwrap();
        wtr.write_all(&self.offset.to_be_bytes()).unwrap();
        assert_eq!(wtr.write(&[0]).unwrap(), 0);
        buf
    }
}

/// Extended CloseWrite payload used when `frame_reassembly` is enabled.
/// Carries the stream's final byte offset so the receiver knows the
/// stream is complete once all bytes up to that offset have been
/// delivered, even if the CloseWrite frame arrives before some delayed
/// Data frames. Lays out as `stream_id (u32 BE) | final_offset (u32 BE)`.
#[derive(Debug, Clone)]
pub struct CloseWriteExtMsg {
    pub stream_id: StreamId,
    pub final_offset: Offset,
}
impl CloseWriteExtMsg {
    pub const SIZE: usize = core::mem::size_of::<StreamId>() + core::mem::size_of::<Offset>();
    pub fn decode(buf: [u8; Self::SIZE]) -> Self {
        let mut rdr = io::Cursor::new(&buf[..]);
        let mut stream = [0; 4];
        rdr.read_exact(&mut stream).unwrap();
        let stream = StreamId::from_be_bytes(stream);
        let mut final_offset = [0; 4];
        rdr.read_exact(&mut final_offset).unwrap();
        let final_offset = Offset::from_be_bytes(final_offset);
        assert_eq!(rdr.read(&mut [0]).unwrap(), 0);
        Self {
            stream_id: stream,
            final_offset,
        }
    }
    pub fn encode(&self) -> [u8; Self::SIZE] {
        let mut buf = [0; Self::SIZE];
        let mut wtr = io::Cursor::new(&mut buf[..]);
        wtr.write_all(&self.stream_id.to_be_bytes()).unwrap();
        wtr.write_all(&self.final_offset.to_be_bytes()).unwrap();
        assert_eq!(wtr.write(&[0]).unwrap(), 0);
        buf
    }
}

/// Wraparound-aware less-than: returns true if `a` is "before" `b` in
/// TCP-style serial-number space. Two offsets are within the valid
/// comparison window as long as their distance is less than 2³¹. Used
/// to order out-of-order Data frames and to decide whether a Data
/// frame's offset is ahead of (buffer it) or at/behind (deliver/drop)
/// the next-expected offset.
#[cfg(test)]
fn offset_less(a: Offset, b: Offset) -> bool {
    // (b.wrapping_sub(a)) as i32 > 0  ⟺  a is strictly before b.
    (b.wrapping_sub(a) as i32) > 0
}

#[cfg(test)]
#[test]
fn test_header_decode_rejects_reserved_codes() {
    for b in 5u8..=255 {
        assert!(
            Header::decode([b]).is_none(),
            "reserved header byte {b} must not decode"
        );
    }
}

#[cfg(test)]
#[test]
fn test_data_header() {
    let h = DataHeader {
        stream_id: 1,
        body_len: 2,
    };
    let b = h.encode();
    let h2 = DataHeader::decode(b);
    assert_eq!(h.stream_id, h2.stream_id);
    assert_eq!(h.body_len, h2.body_len);
}

#[cfg(test)]
#[test]
fn test_data_header_ext_round_trip() {
    let h = DataHeaderExt {
        stream_id: 42,
        body_len: 1200,
        offset: 0xdead_beef,
    };
    let b = h.encode();
    let h2 = DataHeaderExt::decode(b);
    assert_eq!(h.stream_id, h2.stream_id);
    assert_eq!(h.body_len, h2.body_len);
    assert_eq!(h.offset, h2.offset);
}

#[cfg(test)]
#[test]
fn test_close_write_ext_round_trip() {
    let h = CloseWriteExtMsg {
        stream_id: 7,
        final_offset: 0x1234_5678,
    };
    let b = h.encode();
    let h2 = CloseWriteExtMsg::decode(b);
    assert_eq!(h.stream_id, h2.stream_id);
    assert_eq!(h.final_offset, h2.final_offset);
}

#[cfg(test)]
#[test]
fn test_offset_less_wraparound() {
    // Simple ordering.
    assert!(offset_less(0, 1));
    assert!(offset_less(1, 2));
    assert!(!offset_less(2, 1));
    assert!(!offset_less(1, 1));
    // Wrap: 0xFFFF_FFFF is before 0.
    assert!(offset_less(0xFFFF_FFFF, 0));
    assert!(offset_less(0xFFFF_FFFF, 1));
    assert!(!offset_less(0, 0xFFFF_FFFF));
    // Mid-range stays sane.
    assert!(offset_less(100, 200));
    assert!(!offset_less(200, 100));
}
