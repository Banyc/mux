//! Lane pairing protocol: Latest-only protocol, stale peer = attacker, lockstep deploy.
use std::io;

use tokio::io::{AsyncRead, AsyncWrite};

use crate::traffic_class::LaneClass;

pub(crate) const PAIRING_NONCE_LEN: usize = 16;
pub(crate) const HELLO_LEN: usize = 1 + PAIRING_NONCE_LEN + GROUP_TOKEN_LEN;
pub(crate) const GROUP_TOKEN_LEN: usize = 16;

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Cursor};

    /// A fully-valid Interactive hello frame plus the parsed values it must
    /// round-trip to.
    fn valid_hello() -> ([u8; HELLO_LEN], LaneClass, PairingNonce, GroupToken) {
        let class = LaneClass::Interactive;
        let nonce = PairingNonce([0xAB; PAIRING_NONCE_LEN]);
        let group = GroupToken([0xCD; GROUP_TOKEN_LEN]);
        let mut buf = [0u8; HELLO_LEN];
        buf[0] = class.hello_byte();
        buf[1..1 + PAIRING_NONCE_LEN].copy_from_slice(nonce.as_ref());
        buf[1 + PAIRING_NONCE_LEN..].copy_from_slice(group.as_ref());
        (buf, class, nonce, group)
    }

    #[tokio::test]
    async fn truncation_fails_gracefully_at_every_byte_boundary() {
        let (full, class, nonce, group) = valid_hello();
        // A full valid packet decodes fine.
        let (got_class, got_nonce, got_group) = read_lane_hello(&mut Cursor::new(&full[..]))
            .await
            .expect("a full valid hello must parse");
        assert_eq!(got_class, class);
        assert_eq!(got_nonce, nonce);
        assert_eq!(got_group, group);
        // Every truncation 0..33 bytes must fail gracefully, not panic.
        for len in 0..HELLO_LEN {
            let truncated = &full[..len];
            let res = read_lane_hello(&mut Cursor::new(truncated)).await;
            assert!(
                res.is_err(),
                "truncating a valid hello to {len} bytes must fail gracefully, got {res:?}"
            );
        }
    }

    #[tokio::test]
    async fn all_zero_group_token_round_trips() {
        let group = GroupToken([0u8; GROUP_TOKEN_LEN]);
        let nonce = PairingNonce([0x42; PAIRING_NONCE_LEN]);
        let mut buf = Vec::new();
        write_lane_hello(&mut buf, LaneClass::Bulk, nonce, group)
            .await
            .unwrap();
        let (class, got_nonce, got_group) = read_lane_hello(&mut Cursor::new(&buf)).await.unwrap();
        assert_eq!(class, LaneClass::Bulk);
        assert_eq!(got_nonce, nonce);
        assert_eq!(got_group, group);
    }

    #[test]
    fn lane_hello_error_maps_to_io_error() {
        let short: io::Error = LaneHelloError::ShortRead {
            expected: HELLO_LEN,
            got: 5,
        }
        .into();
        assert_eq!(short.kind(), io::ErrorKind::UnexpectedEof);

        let bad_class: io::Error = LaneHelloError::BadLaneClass(0x00).into();
        assert_eq!(bad_class.kind(), io::ErrorKind::InvalidData);

        let io_kind: io::Error = LaneHelloError::Io(io::ErrorKind::ConnectionReset).into();
        assert_eq!(io_kind.kind(), io::ErrorKind::ConnectionReset);
    }
}
