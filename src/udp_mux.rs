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
    pub async fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut len = [0; LENGTH_PREFIX_LEN];
        self.inner.read_exact(&mut len).await?;
        let datagram_len = u16::from_be_bytes(len) as usize;
        let copied = datagram_len.min(buf.len());
        self.inner.read_exact(&mut buf[..copied]).await?;
        let mut remaining = datagram_len - copied;
        let mut discard = [0; 2048];
        while remaining > 0 {
            let chunk = remaining.min(discard.len());
            self.inner.read_exact(&mut discard[..chunk]).await?;
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
