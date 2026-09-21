//! Random-length zero-filled padding for mux control frames, so control
//! frames do not fingerprint as fixed-size frames. The tail is
//! `[pad_len u16][padding pad_len]` appended after a control frame's
//! payload; the reader skips it.

use std::{cell::Cell, io};
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

// Per-thread state for [`random_u64`], seeded from the OS once per thread.
thread_local! {
    static WIRE_RANDOM: Cell<u64> = Cell::new(seed_from_os());
}

/// Random u64 for wire-shaping values: a padding length or a heartbeat
/// jitter. Neither is a secret — both are observable on the wire — and their
/// only job is to keep a cadence from fingerprinting as fixed. They come from
/// a per-thread PRNG seeded once from the OS instead of one OS entropy
/// syscall per value, because the central-IO egress loop draws a heartbeat
/// jitter on every iteration and an entropy syscall there sits on the
/// per-frame path.
pub(crate) fn random_u64() -> u64 {
    WIRE_RANDOM.with(|state| {
        let counter = state.get().wrapping_add(0x9E37_79B9_7F4A_7C15);
        state.set(counter);
        avalanche(counter)
    })
}

fn seed_from_os() -> u64 {
    let mut buf = [0u8; core::mem::size_of::<u64>()];
    getrandom::fill(&mut buf).expect("operating-system randomness unavailable");
    u64::from_le_bytes(buf)
}

/// The splitmix64 finalizer: a bijective avalanche over the counter, so
/// consecutive draws are uncorrelated without carrying a generator.
fn avalanche(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}
