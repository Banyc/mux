use std::{io, num::NonZeroUsize, time::Duration};

use primitive::arena::obj_pool::ArcObjPool;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};

use crate::{
    control::DeadControl,
    protocol::{
        CloseWriteExtMsg, DataHeader, DataHeaderExt, Header, Offset, Side, StreamId, StreamIdMsg,
    },
};

use super::{DataBuf, DeadCentralIo};

const OBJ_POOL_SHARDS: NonZeroUsize = NonZeroUsize::new(4).unwrap();
const CHANNEL_SIZE: usize = 1024;
const READ_BUF_CAPACITY: usize = 64 * 1024;
const RECEIVE_DEADLINE_INTERVALS: u32 = 4;

pub async fn run_central_io_reader<R>(
    mut io_reader: CentralIoReader<R>,
    tx: CentralIoReadTx,
    heartbeat_interval: Duration,
    first_receive_deadline: Option<Duration>,
    mut first_receive_tx: Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<(), RunCentralIoReaderError>
where
    R: AsyncRead + Unpin,
{
    let steady_deadline = heartbeat_interval * RECEIVE_DEADLINE_INTERVALS;
    let mut deadline = first_receive_deadline.unwrap_or(steady_deadline);
    loop {
        let msg = io_reader
            .recv_with_steady_deadline(deadline, steady_deadline, &mut first_receive_tx)
            .await
            .map_err(RunCentralIoReaderError::IoReader)?;
        deadline = steady_deadline;
        tx.send(msg)
            .await
            .map_err(RunCentralIoReaderError::Control)?;
    }
}
#[derive(Debug)]
pub enum RunCentralIoReaderError {
    IoReader(io::Error),
    Control(DeadControl),
}

impl From<io::Error> for RunCentralIoReaderError {
    fn from(e: io::Error) -> Self {
        RunCentralIoReaderError::IoReader(e)
    }
}

#[derive(Debug)]
pub struct CentralIoReader<R> {
    io_reader: BufReader<R>,
    buf_pool: ArcObjPool<Vec<u8>>,
    frame_reassembly: bool,
}
impl<R> CentralIoReader<R>
where
    R: AsyncRead + Unpin,
{
    pub fn new(io_reader: R, frame_reassembly: bool) -> Self {
        Self {
            io_reader: BufReader::with_capacity(READ_BUF_CAPACITY, io_reader),
            buf_pool: ArcObjPool::new(None, OBJ_POOL_SHARDS, Vec::new, |v| v.clear()),
            frame_reassembly,
        }
    }
}
impl<R> CentralIoReader<R>
where
    R: AsyncRead + Unpin,
{
    pub async fn recv_with_steady_deadline(
        &mut self,
        mut deadline: Duration,
        steady_deadline: Duration,
        first_receive_tx: &mut Option<tokio::sync::oneshot::Sender<()>>,
    ) -> io::Result<CentralIoReadMsg> {
        loop {
            let res = tokio::time::timeout(deadline, self.recv_pkt())
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        "receive deadline - session timed out",
                    )
                })??;
            if let Some(tx) = first_receive_tx.take() {
                let _ = tx.send(());
            }
            deadline = steady_deadline;
            if let Some(res) = res {
                return Ok(res);
            }
        }
    }
    async fn recv_pkt(&mut self) -> io::Result<Option<CentralIoReadMsg>> {
        let mut hdr = [0; Header::SIZE];
        self.io_reader.read_exact(&mut hdr).await?;
        let hdr = Header::decode(hdr).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown header: {hdr:?}"),
            )
        })?;
        Ok(match hdr {
            Header::Heartbeat => None,
            Header::Open => Some(CentralIoReadMsg::Open(self.recv_stream_id().await?)),
            Header::Data => {
                let (stream, offset, pkt) = self.recv_data().await?;
                Some(CentralIoReadMsg::Data(stream, offset, pkt))
            }
            Header::CloseRead => Some(CentralIoReadMsg::Close(
                self.recv_stream_id().await?,
                Side::Read,
                0,
            )),
            Header::CloseWrite => {
                let (stream, final_offset) = self.recv_close_write().await?;
                Some(CentralIoReadMsg::Close(stream, Side::Write, final_offset))
            }
        })
    }
    async fn recv_data(&mut self) -> io::Result<(StreamId, Offset, DataBuf)> {
        if self.frame_reassembly {
            let mut hdr = [0; DataHeaderExt::SIZE];
            self.io_reader.read_exact(&mut hdr).await?;
            let hdr = DataHeaderExt::decode(hdr);
            let mut remaining = usize::from(hdr.body_len);
            let mut buf = self.buf_pool.take_scoped();
            buf.reserve(remaining);
            while remaining != 0 {
                let chunk = self.io_reader.fill_buf().await?;
                if chunk.is_empty() {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
                let n = chunk.len().min(remaining);
                buf.extend_from_slice(&chunk[..n]);
                self.io_reader.consume(n);
                remaining -= n;
            }
            Ok((hdr.stream_id, hdr.offset, buf))
        } else {
            let mut hdr = [0; DataHeader::SIZE];
            self.io_reader.read_exact(&mut hdr).await?;
            let hdr = DataHeader::decode(hdr);
            let mut remaining = usize::from(hdr.body_len);
            let mut buf = self.buf_pool.take_scoped();
            buf.reserve(remaining);
            while remaining != 0 {
                let chunk = self.io_reader.fill_buf().await?;
                if chunk.is_empty() {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
                let n = chunk.len().min(remaining);
                buf.extend_from_slice(&chunk[..n]);
                self.io_reader.consume(n);
                remaining -= n;
            }
            Ok((hdr.stream_id, 0, buf))
        }
    }
    async fn recv_close_write(&mut self) -> io::Result<(StreamId, Offset)> {
        if self.frame_reassembly {
            let mut buf = [0; CloseWriteExtMsg::SIZE];
            self.io_reader.read_exact(&mut buf).await?;
            let msg = CloseWriteExtMsg::decode(buf);
            Ok((msg.stream_id, msg.final_offset))
        } else {
            Ok((self.recv_stream_id().await?, 0))
        }
    }
    async fn recv_stream_id(&mut self) -> io::Result<StreamId> {
        let mut hdr = [0; StreamIdMsg::SIZE];
        self.io_reader.read_exact(&mut hdr).await?;
        let hdr = StreamIdMsg::decode(hdr);
        Ok(hdr.stream_id)
    }
}

#[derive(Debug)]
pub enum CentralIoReadMsg {
    Open(StreamId),
    /// `(stream_id, byte_offset, body)`. In mode-off the offset is always 0
    /// and is ignored by the control loop; in mode-on it is the per-stream
    /// byte offset of the first byte in `body`.
    Data(StreamId, Offset, DataBuf),
    /// `(stream_id, side, final_offset)`. `final_offset` is meaningful only
    /// for `Side::Write` in mode-on; in mode-off (or `Side::Read`) it is 0.
    Close(StreamId, Side, Offset),
}
pub fn central_io_read_channel() -> (CentralIoReadTx, CentralIoReadRx) {
    let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_SIZE);
    let tx = CentralIoReadTx { tx };
    let rx = CentralIoReadRx { rx };
    (tx, rx)
}
#[derive(Debug, Clone)]
pub struct CentralIoReadTx {
    tx: tokio::sync::mpsc::Sender<CentralIoReadMsg>,
}
impl CentralIoReadTx {
    pub async fn send(&self, msg: CentralIoReadMsg) -> Result<(), DeadControl> {
        self.tx.send(msg).await.map_err(|_| DeadControl {})
    }
}
#[derive(Debug)]
pub struct CentralIoReadRx {
    rx: tokio::sync::mpsc::Receiver<CentralIoReadMsg>,
}
impl CentralIoReadRx {
    pub async fn recv(&mut self) -> Result<CentralIoReadMsg, DeadCentralIo> {
        self.rx
            .recv()
            .await
            .ok_or(DeadCentralIo { side: Side::Read })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn first_receive_ready_waits_for_complete_open_frame() {
        let (mut client, server) = tokio::io::duplex(64);
        let mut reader = CentralIoReader::new(server, false);
        let (ready_tx, mut ready_rx) = tokio::sync::oneshot::channel();
        let mut first_receive_tx = Some(ready_tx);

        client.write_all(&[0x01]).await.unwrap();

        let _handle = tokio::spawn(async move {
            let _ = reader
                .recv_with_steady_deadline(
                    Duration::from_secs(5),
                    Duration::from_secs(5),
                    &mut first_receive_tx,
                )
                .await;
        });

        match tokio::time::timeout(Duration::from_millis(100), &mut ready_rx).await {
            Err(_elapsed) => {}
            Ok(Ok(())) => panic!("ready_tx fired before complete frame was consumed"),
            Ok(Err(_)) => {}
        }

        client.write_all(&0u32.to_be_bytes()).await.unwrap();

        match tokio::time::timeout(Duration::from_secs(1), &mut ready_rx).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => panic!("ready_tx sender dropped without sending"),
            Err(_elapsed) => panic!("ready_tx did not resolve after complete frame"),
        }
    }
}
