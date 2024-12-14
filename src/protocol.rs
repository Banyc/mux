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

#[derive(Debug, Clone)]
pub struct DataHeader {
    pub stream_id: StreamId,
    pub body_len: u32,
}
impl DataHeader {
    pub const SIZE: usize = core::mem::size_of::<StreamId>() + core::mem::size_of::<u32>();
    pub fn decode(buf: [u8; Self::SIZE]) -> Self {
        let mut rdr = io::Cursor::new(&buf[..]);
        let mut stream = [0; 4];
        rdr.read_exact(&mut stream).unwrap();
        let stream = StreamId::from_be_bytes(stream);
        let mut body_len = [0; 4];
        rdr.read_exact(&mut body_len).unwrap();
        let body_len = u32::from_be_bytes(body_len);
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
