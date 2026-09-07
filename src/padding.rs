//! Random-length zero-filled padding for mux control frames, so control
//! frames do not fingerprint as fixed-size frames. The tail is
//! `[pad_len u16][padding pad_len]` appended after a control frame's
//! payload; the reader skips it.

use std::io;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

/// Maximum padding tail: sized to reach a full transport datagram (MSS),
/// so a padded control frame can be indistinguishable from a full data
/// packet.
pub(crate) const MAX_PAD: usize = 1500;
/// The pad_len field size (u16).
pub(crate) const PAD_LEN_LEN: usize = 2;

/// Random padding length in `0..=MAX_PAD`.
pub(crate) fn pad_len() -> usize {
    (random_u64() % (MAX_PAD as u64 + 1)) as usize
}

/// Append the padding tail to `buf`: `[pad_len u16][zero padding pad_len]`.
/// Buffer-only (no awaits) so the caller writes the whole padded control
/// frame atomically in one transport write;
pub(crate) fn append_tail(buf: &mut Vec<u8>) {
    let pad_len = pad_len();
    buf.extend_from_slice(&(pad_len as u16).to_be_bytes());
    buf.resize(buf.len() + pad_len, 0);
}

/// Read and skip the padding tail.
pub(crate) async fn skip_tail<R: AsyncBufRead + Unpin>(reader: &mut R) -> io::Result<()> {
    let mut pad_len = [0u8; PAD_LEN_LEN];
    reader.read_exact(&mut pad_len).await?;
    let mut remaining = u16::from_be_bytes(pad_len) as usize;
    while remaining > 0 {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let n = chunk.len().min(remaining);
        reader.consume(n);
        remaining -= n;
    }
    Ok(())
}

/// Random u64 from the OS.
pub(crate) fn random_u64() -> u64 {
    let mut buf = [0u8; core::mem::size_of::<u64>()];
    getrandom::fill(&mut buf).expect("operating-system randomness unavailable");
    u64::from_le_bytes(buf)
}
