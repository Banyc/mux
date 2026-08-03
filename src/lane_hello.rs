//! Lane pairing protocol: Latest-only protocol, stale peer = attacker, lockstep deploy.
use std::io;

use tokio::io::{AsyncRead, AsyncWrite};

const LANE_HELLO_INTERACTIVE: u8 = 0xD1;
const LANE_HELLO_BULK: u8 = 0xD2;
pub(crate) const PAIRING_NONCE_LEN: usize = 16;
pub(crate) const HELLO_LEN: usize = 1 + PAIRING_NONCE_LEN + GROUP_TOKEN_LEN;
pub(crate) const GROUP_TOKEN_LEN: usize = 16;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PairingNonce(pub(crate) [u8; PAIRING_NONCE_LEN]);

impl PairingNonce {
    pub fn generate() -> Self {
        let mut buf = [0u8; PAIRING_NONCE_LEN];
        getrandom::fill(&mut buf).expect("PairingNonce generation failed");
        Self(buf)
    }

    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl AsRef<[u8]> for PairingNonce {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
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
    group: GroupToken,
) -> Result<(), LaneHelloError> {
    use tokio::io::AsyncWriteExt;
    let mut buf = [0u8; HELLO_LEN];
    buf[0] = class.hello_byte();
    buf[1..1 + PAIRING_NONCE_LEN].copy_from_slice(nonce.as_ref());
    buf[1 + PAIRING_NONCE_LEN..].copy_from_slice(group.as_ref());
    writer
        .write_all(&buf)
        .await
        .map_err(|e| LaneHelloError::Io(e.kind()))?;
    Ok(())
}

pub async fn read_lane_hello<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<(LaneClass, PairingNonce, GroupToken), LaneHelloError> {
    use tokio::io::AsyncReadExt;
    let mut buf = [0u8; HELLO_LEN];
    reader
        .read_exact(&mut buf)
        .await
        .map_err(|e| LaneHelloError::Io(e.kind()))?;
    let class = LaneClass::from_hello_byte(buf[0]).ok_or(LaneHelloError::BadLaneClass(buf[0]))?;
    let mut nonce_bytes = [0u8; PAIRING_NONCE_LEN];
    nonce_bytes.copy_from_slice(&buf[1..1 + PAIRING_NONCE_LEN]);
    let mut group_bytes = [0u8; GROUP_TOKEN_LEN];
    group_bytes.copy_from_slice(&buf[1 + PAIRING_NONCE_LEN..]);
    Ok((class, PairingNonce(nonce_bytes), GroupToken(group_bytes)))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GroupToken(pub(crate) [u8; GROUP_TOKEN_LEN]);

impl GroupToken {
    pub fn generate() -> Self {
        let mut buf = [0u8; GROUP_TOKEN_LEN];
        getrandom::fill(&mut buf).expect("GroupToken generation failed");
        Self(buf)
    }
}

impl AsRef<[u8]> for GroupToken {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
