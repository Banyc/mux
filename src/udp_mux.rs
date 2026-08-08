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
    /// Length-prefix bytes already consumed from `inner` but not yet
    /// completed. A `recv` cancelled mid-prefix resumes from here.
    prefix: [u8; LENGTH_PREFIX_LEN],
    prefix_filled: usize,
    /// Payload length of the datagram being assembled, if a frame is in
    /// progress.
    frame_len: Option<usize>,
    /// Payload bytes already consumed from `inner` but not yet delivered.
    /// A `recv` cancelled mid-payload resumes from here.
    frame: Vec<u8>,
}
impl<R> UdpMuxReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            prefix: [0; LENGTH_PREFIX_LEN],
            prefix_filled: 0,
            frame_len: None,
            frame: Vec::new(),
        }
    }
    pub fn into_inner(self) -> R {
        self.inner
    }
}
impl<R> UdpMuxReader<R>
where
    R: AsyncRead + Unpin,
{
    /// Finish reading the 2-byte length prefix of the next datagram.
    ///
    /// Cancellation-safe: partial prefix bytes stay in `self.prefix` and a
    /// later call resumes from `self.prefix_filled`. Returns `UnexpectedEof`
    /// only for a clean close at a datagram boundary (EOF before any byte
    /// of the prefix); EOF after a partial prefix is `InvalidData`.
    async fn read_prefix(&mut self) -> io::Result<()> {
        while self.prefix_filled < LENGTH_PREFIX_LEN {
            let n = self
                .inner
                .read(&mut self.prefix[self.prefix_filled..])
                .await?;
            if n == 0 {
                return Err(if self.prefix_filled == 0 {
                    io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "UDP mux stream closed between datagrams",
                    )
                } else {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "truncated UDP mux datagram length prefix: read {} of {LENGTH_PREFIX_LEN} bytes before EOF",
                            self.prefix_filled
                        ),
                    )
                });
            }
            self.prefix_filled += n;
        }
        self.frame_len = Some(u16::from_be_bytes(self.prefix) as usize);
        self.prefix_filled = 0;
        Ok(())
    }

    pub async fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.frame_len.is_none() {
            self.read_prefix().await?;
        }
        let frame_len = self
            .frame_len
            .expect("read_prefix sets frame_len before recv proceeds");
        // The datagram is committed: read its payload (or the remainder
        // after a cancelled call) into `self.frame`. Any EOF from here on
        // is truncated framing, never a clean close.
        if self.frame.is_empty() && self.frame.capacity() < frame_len {
            self.frame.reserve(frame_len);
        }
        while self.frame.len() < frame_len {
            let want = (frame_len - self.frame.len()).min(2048);
            let mut chunk = [0u8; 2048];
            let n = self.inner.read(&mut chunk[..want]).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "truncated UDP mux datagram: read {} of {frame_len} payload bytes before EOF",
                        self.frame.len()
                    ),
                ));
            }
            self.frame.extend_from_slice(&chunk[..n]);
        }
        let copied = frame_len.min(buf.len());
        buf[..copied].copy_from_slice(&self.frame[..copied]);
        self.frame.clear();
        self.frame_len = None;
        Ok(copied)
    }
}
#[derive(Debug)]
pub struct UdpMuxWriter<W> {
    inner: W,
    /// Encoded bytes of a frame whose write was cancelled partway. The
    /// next `send` (or `shutdown`) finishes this frame before writing the
    /// next one, so the peer never continues a damaged frame with a fresh
    /// length prefix.
    pending: Vec<u8>,
}
impl<W> UdpMuxWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            pending: Vec::new(),
        }
    }
    pub fn into_inner(self) -> W {
        self.inner
    }
}
impl<W> UdpMuxWriter<W>
where
    W: AsyncWrite + Unpin,
{
    /// Write out the bytes of a frame whose write was cancelled partway,
    /// resuming from where it stopped. No-op when nothing is pending.
    /// Written bytes are drained from `self.pending` as they go, so a
    /// cancelled call leaves only the unsent tail behind.
    async fn flush_pending(&mut self) -> io::Result<()> {
        while !self.pending.is_empty() {
            let n = self.inner.write(&self.pending).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "UDP mux stream rejected a frame write",
                ));
            }
            if n == self.pending.len() {
                self.pending.clear();
            } else {
                self.pending.drain(..n);
            }
        }
        Ok(())
    }

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
        // Finish any frame left half-written by a cancelled call, so the
        // peer receives complete datagrams in order.
        self.flush_pending().await?;
        // Encode the whole frame up front: there is no await point between
        // the length prefix and the payload, and cancellation only ever
        // discards progress recorded in `self.pending`.
        self.pending.clear();
        self.pending.extend_from_slice(&len.to_be_bytes());
        self.pending.extend_from_slice(payload);
        self.flush_pending().await?;
        Ok(payload.len())
    }
    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.flush_pending().await?;
        self.inner.shutdown().await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
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
    async fn cancel_mid_prefix_recovers_the_partial_datagram() {
        let (left, right) = duplex(16);
        let (_left_read, mut left_write) = tokio::io::split(left);
        let (right_read, _right_write) = tokio::io::split(right);
        let mut rx = UdpMuxReader::new(right_read);
        // Only half of the length prefix is available; cancel recv after it
        // has consumed that byte.
        left_write.write_all(&[0]).await.unwrap();
        let mut buf = [0; 16];
        {
            let fut = rx.recv(&mut buf);
            tokio::pin!(fut);
            tokio::select! {
                biased;
                _ = &mut fut => panic!("recv must not complete with a half length prefix"),
                _ = tokio::time::sleep(Duration::ZERO) => {}
            }
        }
        // The rest of the prefix and the payload arrive later; the next
        // recv must resume the prefix, not treat its tail as a new frame.
        left_write.write_all(&[5]).await.unwrap();
        left_write.write_all(b"hello").await.unwrap();
        let n = rx.recv(&mut buf).await.unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], b"hello");
    }
    #[tokio::test]
    async fn cancel_mid_payload_recovers_the_partial_datagram() {
        let (left, right) = duplex(16);
        let (_left_read, mut left_write) = tokio::io::split(left);
        let (right_read, _right_write) = tokio::io::split(right);
        let mut rx = UdpMuxReader::new(right_read);
        left_write.write_all(&[0, 5]).await.unwrap();
        left_write.write_all(b"he").await.unwrap();
        let mut buf = [0; 16];
        {
            let fut = rx.recv(&mut buf);
            tokio::pin!(fut);
            tokio::select! {
                biased;
                _ = &mut fut => panic!("recv must not complete with a partial payload"),
                _ = tokio::time::sleep(Duration::ZERO) => {}
            }
        }
        left_write.write_all(b"llo").await.unwrap();
        let n = rx.recv(&mut buf).await.unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], b"hello");
    }
    #[tokio::test]
    async fn cancel_mid_send_does_not_corrupt_the_next_datagram() {
        // Capacity 4 forces the 7-byte frame to write in two chunks, so the
        // first send can be cancelled with bytes still in flight.
        let (left, right) = duplex(4);
        let (_left_read, left_write) = tokio::io::split(left);
        let (right_read, _right_write) = tokio::io::split(right);
        let mut tx = UdpMuxWriter::new(left_write);
        let mut rx = UdpMuxReader::new(right_read);
        {
            let fut = tx.send(b"hello");
            tokio::pin!(fut);
            tokio::select! {
                biased;
                _ = &mut fut => panic!("send must not complete while the duplex is full"),
                _ = tokio::time::sleep(Duration::ZERO) => {}
            }
        }
        // The next send finishes the damaged frame first, then writes the
        // new one; the peer must see two intact datagrams.
        let send_task = tokio::spawn(async move { tx.send(b"x").await.unwrap() });
        let mut buf = [0; 16];
        let n = rx.recv(&mut buf).await.unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], b"hello");
        let n = rx.recv(&mut buf).await.unwrap();
        assert_eq!(n, 1);
        assert_eq!(&buf[..n], b"x");
        assert_eq!(send_task.await.unwrap(), 1);
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
