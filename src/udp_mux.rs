use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
pub const MAX_UDP_MUX_DATAGRAM_LEN: usize = u16::MAX as usize;
const LENGTH_PREFIX_LEN: usize = size_of::<u16>();
pub fn udp_mux<R, W>(reader: R, writer: W) -> (UdpMuxReader<R>, UdpMuxWriter<W>) {
    (UdpMuxReader::new(reader), UdpMuxWriter::new(writer))
}
#[derive(Debug)]
pub struct UdpMuxReader<R> {
    inner: R,
}
impl<R> UdpMuxReader<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
    pub fn into_inner(self) -> R {
        self.inner
    }
}
impl<R> UdpMuxReader<R>
where
    R: AsyncRead + Unpin,
{
    /// Read exactly `buf.len()` bytes, returning how many were read if EOF
    /// arrived first (`< buf.len()`). Non-EOF I/O errors propagate. The
    /// caller decides whether an early EOF is a clean close or truncated
    /// framing based on how much of a datagram was already committed.
    async fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut filled = 0;
        while filled < buf.len() {
            let n = self.inner.read(&mut buf[filled..]).await?;
            if n == 0 {
                return Ok(filled);
            }
            filled += n;
        }
        Ok(buf.len())
    }

    pub async fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut len = [0; LENGTH_PREFIX_LEN];
        let n = self.read_exact(&mut len).await?;
        if n < LENGTH_PREFIX_LEN {
            // The clean-close check happens only here, before any byte of
            // a new datagram is committed: EOF at the start of the length
            // prefix is a clean close at a datagram boundary, while EOF
            // after a partial prefix already truncated the datagram.
            return Err(if n == 0 {
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "UDP mux stream closed between datagrams",
                )
            } else {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "truncated UDP mux datagram length prefix: read {n} of {LENGTH_PREFIX_LEN} bytes before EOF"
                    ),
                )
            });
        }
        let datagram_len = u16::from_be_bytes(len) as usize;
        let copied = datagram_len.min(buf.len());
        // The datagram is committed: any EOF from here on, including
        // before the first payload byte or at a discard-chunk boundary,
        // is truncated framing, never a clean close.
        let n = self.read_exact(&mut buf[..copied]).await?;
        if n < copied {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "truncated UDP mux datagram: read {n} of {copied} payload bytes before EOF"
                ),
            ));
        }
        let mut remaining = datagram_len - copied;
        let mut discard = [0; 2048];
        while remaining > 0 {
            let chunk = remaining.min(discard.len());
            let n = self.read_exact(&mut discard[..chunk]).await?;
            if n < chunk {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "truncated UDP mux datagram: read {n} of {chunk} tail bytes before EOF"
                    ),
                ));
            }
            remaining -= chunk;
        }
        Ok(copied)
    }
}
#[derive(Debug)]
pub struct UdpMuxWriter<W> {
    inner: W,
}
impl<W> UdpMuxWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner }
    }
    pub fn into_inner(self) -> W {
        self.inner
    }
}
impl<W> UdpMuxWriter<W>
where
    W: AsyncWrite + Unpin,
{
    pub async fn send(&mut self, payload: &[u8]) -> io::Result<usize> {
        let len = u16::try_from(payload.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "UDP mux datagram is {} bytes; maximum is {MAX_UDP_MUX_DATAGRAM_LEN}",
                    payload.len()
                ),
            )
        })?;
        self.inner.write_all(&len.to_be_bytes()).await?;
        self.inner.write_all(payload).await?;
        Ok(payload.len())
    }
    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.inner.shutdown().await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;
    #[tokio::test]
    async fn preserves_empty_and_non_empty_datagram_boundaries() {
        let (left, right) = duplex(128);
        let (left_read, left_write) = tokio::io::split(left);
        let (right_read, right_write) = tokio::io::split(right);
        let (_left_rx, mut left_tx) = udp_mux(left_read, left_write);
        let (mut right_rx, _right_tx) = udp_mux(right_read, right_write);
        left_tx.send(b"").await.unwrap();
        left_tx.send(b"one").await.unwrap();
        left_tx.send(b"two-two").await.unwrap();
        let mut buf = [0; 32];
        assert_eq!(right_rx.recv(&mut buf).await.unwrap(), 0);
        let n = right_rx.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"one");
        let n = right_rx.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"two-two");
    }
    #[tokio::test]
    async fn truncation_discards_only_the_current_datagram_tail() {
        let (left, right) = duplex(128);
        let (_left_read, mut left_write) = tokio::io::split(left);
        let (mut right_read, _right_write) = tokio::io::split(right);
        let mut tx = UdpMuxWriter::new(&mut left_write);
        let mut rx = UdpMuxReader::new(&mut right_read);
        tx.send(b"oversized").await.unwrap();
        tx.send(b"next").await.unwrap();
        let mut short = [0; 4];
        assert_eq!(rx.recv(&mut short).await.unwrap(), short.len());
        assert_eq!(&short, b"over");
        let n = rx.recv(&mut short).await.unwrap();
        assert_eq!(&short[..n], b"next");
    }
    #[tokio::test]
    async fn eof_between_datagrams_is_a_clean_close() {
        let (left, right) = duplex(128);
        let (_left_read, mut left_write) = tokio::io::split(left);
        let (right_read, _right_write) = tokio::io::split(right);
        let mut rx = UdpMuxReader::new(right_read);
        // Writer closed with no datagram in flight: EOF at a boundary.
        left_write.shutdown().await.unwrap();
        drop(left_write);
        let error = rx.recv(&mut [0; 8]).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }
    #[tokio::test]
    async fn eof_after_a_complete_datagram_is_a_clean_close() {
        let (left, right) = duplex(128);
        let (_left_read, left_write) = tokio::io::split(left);
        let (right_read, _right_write) = tokio::io::split(right);
        let mut tx = UdpMuxWriter::new(left_write);
        let mut rx = UdpMuxReader::new(right_read);
        tx.send(b"x").await.unwrap();
        tx.shutdown().await.unwrap();
        drop(tx);
        let mut buf = [0; 8];
        let n = rx.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"x");
        let error = rx.recv(&mut buf).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }
    #[tokio::test]
    async fn eof_mid_length_prefix_is_truncation_not_a_clean_close() {
        let (left, right) = duplex(128);
        let (_left_read, mut left_write) = tokio::io::split(left);
        let (right_read, _right_write) = tokio::io::split(right);
        let mut rx = UdpMuxReader::new(right_read);
        // Half a length prefix, then the writer dies.
        left_write.write_all(&[0]).await.unwrap();
        left_write.shutdown().await.unwrap();
        drop(left_write);
        let error = rx.recv(&mut [0; 8]).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
    #[tokio::test]
    async fn eof_mid_payload_is_truncation_not_a_clean_close() {
        let (left, right) = duplex(128);
        let (_left_read, mut left_write) = tokio::io::split(left);
        let (right_read, _right_write) = tokio::io::split(right);
        let mut rx = UdpMuxReader::new(right_read);
        // Complete length prefix claiming 5 bytes, but only 1 arrives.
        left_write.write_all(&[0, 5]).await.unwrap();
        left_write.write_all(b"a").await.unwrap();
        left_write.shutdown().await.unwrap();
        drop(left_write);
        let error = rx.recv(&mut [0; 8]).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
    #[tokio::test]
    async fn eof_after_full_length_prefix_but_before_payload_is_truncation() {
        let (left, right) = duplex(128);
        let (_left_read, mut left_write) = tokio::io::split(left);
        let (right_read, _right_write) = tokio::io::split(right);
        let mut rx = UdpMuxReader::new(right_read);
        // Full length prefix claiming 5 bytes, then the writer dies before
        // any payload byte arrives: committed to a datagram, so this must
        // be truncation, not a clean close.
        left_write.write_all(&[0, 5]).await.unwrap();
        left_write.shutdown().await.unwrap();
        drop(left_write);
        let error = rx.recv(&mut [0; 8]).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
    #[tokio::test]
    async fn eof_between_payload_and_tail_bytes_is_truncation() {
        let (left, right) = duplex(128);
        let (_left_read, mut left_write) = tokio::io::split(left);
        let (right_read, _right_write) = tokio::io::split(right);
        let mut rx = UdpMuxReader::new(right_read);
        // Datagram of 5 bytes with a 4-byte caller buffer: 4 payload bytes
        // arrive, then the writer dies before the 5th (discarded tail)
        // byte. EOF lands exactly at a discard-chunk boundary and must
        // still be truncation.
        left_write.write_all(&[0, 5]).await.unwrap();
        left_write.write_all(b"abcd").await.unwrap();
        left_write.shutdown().await.unwrap();
        drop(left_write);
        let error = rx.recv(&mut [0; 4]).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
    #[tokio::test]
    async fn rejects_a_datagram_larger_than_udp_can_carry() {
        let (left, _right) = duplex(128);
        let (_read, write) = tokio::io::split(left);
        let mut tx = UdpMuxWriter::new(write);
        let error = tx
            .send(&vec![0; MAX_UDP_MUX_DATAGRAM_LEN + 1])
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
