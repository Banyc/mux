use std::{
    collections::HashMap,
    io::{self, IoSlice},
    task::{Context, Poll},
    time::Duration,
};

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{
    common::Side,
    control::DeadControl,
    fair_queue,
    protocol::{BodyLen, DataHeader, Header, StreamId, StreamIdMsg},
};

use super::{DataBuf, DeadCentralIo};

const CONTROL_CHANNEL_SIZE: usize = 1024;

pub async fn run_central_io_writer<W>(
    mut io_writer: CentralIoWriter<W>,
    heartbeat_interval: Duration,
    mut control: WriteControlRx,
    mut data: WriteDataRx,
) -> Result<(), RunCentralIoWriterError>
where
    W: AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            biased;
            res = control.recv() => {
                let msg = res.map_err(RunCentralIoWriterError::Control)?;
                io_writer.send_control(msg).await.map_err(RunCentralIoWriterError::IoWriter)?;
            }
            res = data.recv() => {
                let msg = res.map_err(RunCentralIoWriterError::Control)?;
                io_writer.send_data(msg).await.map_err(RunCentralIoWriterError::IoWriter)?;
            }
            () = tokio::time::sleep(heartbeat_interval) => {
                io_writer.send_heartbeat().await.map_err(RunCentralIoWriterError::IoWriter)?;
            }
        }
    }
}
#[derive(Debug)]
pub enum RunCentralIoWriterError {
    IoWriter(io::Error),
    Control(DeadControl),
}

#[derive(Debug)]
pub struct CentralIoWriter<W> {
    io_writer: W,
}
impl<W> CentralIoWriter<W> {
    pub fn new(io_writer: W) -> Self {
        Self { io_writer }
    }
}
impl<W> CentralIoWriter<W>
where
    W: AsyncWrite + Unpin,
{
    pub async fn send_heartbeat(&mut self) -> io::Result<()> {
        let hdr = Header::Heartbeat;
        let hdr = hdr.encode();
        self.io_writer.write_all(&hdr).await?;
        Ok(())
    }
    pub async fn send_control(&mut self, msg: WriteControlMsg) -> io::Result<()> {
        let hdr = match &msg {
            WriteControlMsg::Open(_) => Header::Open,
            WriteControlMsg::Close(_, side) => match side {
                Side::Read => Header::CloseRead,
                Side::Write => Header::CloseWrite,
            },
        };
        let stream_id = match msg {
            WriteControlMsg::Open(stream_id) | WriteControlMsg::Close(stream_id, _) => stream_id,
        };
        self.send_control_(hdr, stream_id).await
    }
    async fn send_control_(&mut self, hdr: Header, stream_id: u32) -> io::Result<()> {
        let stream_id_msg = StreamIdMsg { stream_id };
        let hdr = hdr.encode();
        let stream_id_msg = stream_id_msg.encode();
        let mut concat = hdr.into_iter().chain(stream_id_msg);
        let buf: [u8; Header::SIZE + StreamIdMsg::SIZE] =
            core::array::from_fn(|_| concat.next().unwrap());
        self.io_writer.write_all(&buf).await?;
        Ok(())
    }
    pub async fn send_data(&mut self, msg: WriteDataMsg) -> io::Result<()> {
        let data_buf = match msg.data {
            StreamWriteData::Open => return Ok(()),
            StreamWriteData::Fin => {
                let hdr = Header::CloseWrite;
                return self.send_control_(hdr, msg.stream_id).await;
            }
            StreamWriteData::Data(data_buf) => data_buf,
        };
        let hdr = Header::Data;
        let mut body_offset = 0usize;
        while body_offset != data_buf.len() {
            let body_len = (data_buf.len() - body_offset).min(usize::from(BodyLen::MAX));
            let body_len_u16 = BodyLen::try_from(body_len).unwrap();
            let data_hdr = DataHeader {
                stream_id: msg.stream_id,
                body_len: body_len_u16,
            };
            let hdr = hdr.encode();
            let data_hdr = data_hdr.encode();
            let mut concat = hdr.into_iter().chain(data_hdr);
            let fixed_buf: [u8; Header::SIZE + DataHeader::SIZE] =
                core::array::from_fn(|_| concat.next().unwrap());
            let body = &data_buf[body_offset..body_offset + body_len];
            body_offset += body_len;
            self.write_all_frame(&fixed_buf, body).await?;
        }
        Ok(())
    }
    async fn write_all_frame(&mut self, fixed_header: &[u8], body: &[u8]) -> io::Result<()> {
        if self.io_writer.is_write_vectored() && !fixed_header.is_empty() && !body.is_empty() {
            let mut header_remaining = fixed_header;
            let mut body_remaining = body;
            loop {
                let n = if header_remaining.is_empty() {
                    self.io_writer.write(body_remaining).await?
                } else {
                    let bufs = &mut [
                        IoSlice::new(header_remaining),
                        IoSlice::new(body_remaining),
                    ];
                    self.io_writer.write_vectored(bufs).await?
                };
                if n == 0 {
                    return Err(io::ErrorKind::WriteZero.into());
                }
                if n < header_remaining.len() {
                    header_remaining = &header_remaining[n..];
                } else {
                    let consumed_body = n - header_remaining.len();
                    body_remaining = &body_remaining[consumed_body..];
                    header_remaining = &[];
                    if body_remaining.is_empty() {
                        return Ok(());
                    }
                }
            }
        } else {
            if !fixed_header.is_empty() {
                self.io_writer.write_all(fixed_header).await?;
            }
            if !body.is_empty() {
                self.io_writer.write_all(body).await?;
            }
            Ok(())
        }
    }
}

#[derive(Debug)]
pub struct WriteDataMsg {
    pub stream_id: StreamId,
    pub data: StreamWriteData,
}
#[derive(Debug)]
pub enum StreamWriteData {
    Open,
    Fin,
    Data(DataBuf),
}
pub fn write_data_channel() -> (WriteDataTxPrototype, WriteDataRx) {
    let (tx, rx) = fair_queue::channel();
    let tx = WriteDataTxPrototype { opener: tx };
    let rx = WriteDataRx {
        rx,
        token_to_stream: HashMap::new(),
    };
    (tx, rx)
}
#[derive(Debug)]
pub struct WriteDataRx {
    rx: fair_queue::Receiver<WriteDataMsg>,
    token_to_stream: HashMap<fair_queue::Token, StreamId>,
}
impl WriteDataRx {
    pub async fn recv(&mut self) -> Result<WriteDataMsg, DeadControl> {
        loop {
            let (token, msg) = self.rx.recv().await.ok_or(DeadControl {})?;
            match msg {
                fair_queue::ReceiverRecv::Open(value) => {
                    self.token_to_stream.insert(token, value.stream_id);
                    return Ok(value);
                }
                fair_queue::ReceiverRecv::Value(value) => return Ok(value),
                fair_queue::ReceiverRecv::Close => {
                    let Some(stream_id) = self.token_to_stream.remove(&token) else {
                        continue;
                    };
                    return Ok(WriteDataMsg {
                        stream_id,
                        data: StreamWriteData::Fin,
                    });
                }
            }
        }
    }
}
#[derive(Debug, Clone)]
pub struct WriteDataTxPrototype {
    opener: fair_queue::Opener<WriteDataMsg>,
}
impl WriteDataTxPrototype {
    pub async fn derive(&self, stream: StreamId) -> Result<StreamWriteDataTx, DeadCentralIo> {
        Ok(StreamWriteDataTx {
            tx: self
                .opener
                .open(WriteDataMsg {
                    stream_id: stream,
                    data: StreamWriteData::Open,
                })
                .await
                .ok_or(DeadCentralIo { side: Side::Write })?,
            stream_id: stream,
        })
    }
}
#[derive(Debug)]
pub struct StreamWriteDataTx {
    stream_id: StreamId,
    tx: fair_queue::Sender<WriteDataMsg>,
}
// impl StreamWriteDataTx {
//     pub async fn send(&self, data: StreamWriteData) -> Result<(), DeadCentralIo> {
//         let msg = WriteDataMsg {
//             stream_id: self.stream_id,
//             data,
//         };
//         self.tx
//             .send(msg)
//             .await
//             .map_err(|_| DeadCentralIo { side: Side::Write })
//     }
// }
#[derive(Debug)]
pub struct PollStreamWriteDataTx {
    stream_id: StreamId,
    tx: fair_queue::PollSender<WriteDataMsg>,
}
impl From<StreamWriteDataTx> for PollStreamWriteDataTx {
    fn from(value: StreamWriteDataTx) -> Self {
        Self {
            stream_id: value.stream_id,
            tx: value.tx.into(),
        }
    }
}
impl PollStreamWriteDataTx {
    pub fn poll_preserve(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), DeadCentralIo>> {
        self.tx
            .poll_reserve(cx)
            .map_err(|_| DeadCentralIo { side: Side::Write })
    }
    pub fn send_item(&mut self, data: StreamWriteData) -> Result<(), DeadCentralIo> {
        let msg = WriteDataMsg {
            stream_id: self.stream_id,
            data,
        };
        self.tx
            .send_item(msg)
            .map_err(|_| DeadCentralIo { side: Side::Write })
    }
}

#[derive(Debug, Clone)]
pub enum WriteControlMsg {
    Open(StreamId),
    Close(StreamId, Side),
}
pub fn write_control_channel() -> (WriteControlTx, WriteControlRx) {
    let (tx, rx) = tokio::sync::mpsc::channel(CONTROL_CHANNEL_SIZE);
    let tx = WriteControlTx { tx };
    let rx = WriteControlRx { rx };
    (tx, rx)
}
#[derive(Debug, Clone)]
pub struct WriteControlTx {
    tx: tokio::sync::mpsc::Sender<WriteControlMsg>,
}
impl WriteControlTx {
    pub async fn send(&self, msg: WriteControlMsg) -> Result<(), DeadCentralIo> {
        self.tx
            .send(msg)
            .await
            .map_err(|_| DeadCentralIo { side: Side::Write })
    }
    pub async fn closed(&self) {
        self.tx.closed().await
    }
}
#[derive(Debug)]
pub struct WriteControlRx {
    rx: tokio::sync::mpsc::Receiver<WriteControlMsg>,
}
impl WriteControlRx {
    pub async fn recv(&mut self) -> Result<WriteControlMsg, DeadControl> {
        self.rx.recv().await.ok_or(DeadControl {})
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, IoSlice},
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
    };

    use primitive::arena::obj_pool::arc_buf_pool;
    use tokio::io::AsyncWrite;

    use super::{CentralIoWriter, StreamWriteData, WriteDataMsg};
    use crate::protocol::{BodyLen, DataHeader, Header};

    /// A mock writer that records every byte and can simulate partial vectored
    /// writes. `max_per_write` caps the number of bytes any single `write` /
    /// `write_vectored` call may consume, exercising the partial-write loop.
    #[derive(Default)]
    struct MockWriter {
        out: Vec<u8>,
        max_per_write: Option<usize>,
        vectored: bool,
        write_vectored_calls: Arc<Mutex<usize>>,
    }
    impl AsyncWrite for MockWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let cap = self.max_per_write.unwrap_or(buf.len()).min(buf.len());
            let cap = cap.max(1);
            let n = cap.min(buf.len());
            let this = unsafe { self.get_unchecked_mut() };
            this.out.extend_from_slice(&buf[..n]);
            Poll::Ready(Ok(n))
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn is_write_vectored(&self) -> bool {
            self.vectored
        }
        fn poll_write_vectored(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let this = unsafe { self.get_unchecked_mut() };
            *this.write_vectored_calls.lock().unwrap() += 1;
            // Concatenate all bufs up to max_per_write bytes.
            let cap = this.max_per_write.unwrap_or(usize::MAX);
            let mut written = 0usize;
            for b in bufs {
                if written >= cap {
                    break;
                }
                let remaining = cap - written;
                let take = b.len().min(remaining);
                this.out.extend_from_slice(&b[..take]);
                written += take;
                if take < b.len() {
                    return Poll::Ready(Ok(written));
                }
            }
            Poll::Ready(Ok(written))
        }
    }

    fn make_data_buf(bytes: &[u8]) -> crate::central_io::DataBuf {
        let pool = arc_buf_pool::<u8>(None, std::num::NonZeroUsize::new(1).unwrap());
        let mut scoped = pool.take_scoped();
        scoped.clear();
        scoped.extend_from_slice(bytes);
        scoped
    }

    fn expected_frame(stream_id: u32, body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut offset = 0usize;
        while offset != body.len() {
            let len = (body.len() - offset).min(usize::from(BodyLen::MAX));
            let data_hdr = DataHeader {
                stream_id,
                body_len: BodyLen::try_from(len).unwrap(),
            };
            out.extend_from_slice(&Header::Data.encode());
            out.extend_from_slice(&data_hdr.encode());
            out.extend_from_slice(&body[offset..offset + len]);
            offset += len;
        }
        out
    }

    #[tokio::test]
    async fn vectored_partial_writes_output_exact_bytes() {
        let calls = Arc::new(Mutex::new(0usize));
        let writer = MockWriter { out: Vec::new(),
            max_per_write: Some(3),
            vectored: true,
            write_vectored_calls: Arc::clone(&calls),
        };
        let mut central = CentralIoWriter::new(writer);
        let body = (0u8..200u8).collect::<Vec<u8>>();
        central
            .send_data(WriteDataMsg {
                stream_id: 1,
                data: StreamWriteData::Data(make_data_buf(&body)),
            })
            .await
            .unwrap();
        let got = central.io_writer.out.clone();
        assert_eq!(got, expected_frame(1, &body));
        assert!(*calls.lock().unwrap() > 0, "vectored path was taken");
    }

    #[tokio::test]
    async fn non_vectored_writer_uses_fallback_and_outputs_exact_bytes() {
        let writer = MockWriter { out: Vec::new(),
            max_per_write: Some(5),
            vectored: false,
            write_vectored_calls: Arc::new(Mutex::new(0)),
        };
        let mut central = CentralIoWriter::new(writer);
        let body = (0u8..200u8).collect::<Vec<u8>>();
        central
            .send_data(WriteDataMsg {
                stream_id: 7,
                data: StreamWriteData::Data(make_data_buf(&body)),
            })
            .await
            .unwrap();
        assert_eq!(
            central.io_writer.write_vectored_calls.lock().unwrap().clone(),
            0,
            "fallback path should not invoke write_vectored"
        );
        let got = central.io_writer.out.clone();
        assert_eq!(got, expected_frame(7, &body));
    }

    #[tokio::test]
    async fn body_larger_than_max_creates_multiple_frames_no_first_chunk_repeat() {
        let writer = MockWriter { out: Vec::new(),
            max_per_write: None,
            vectored: true,
            write_vectored_calls: Arc::new(Mutex::new(0)),
        };
        let mut central = CentralIoWriter::new(writer);
        let big_len = usize::from(BodyLen::MAX) * 2 + 10;
        let body = (0u8..big_len as u8)
            .cycle()
            .take(big_len)
            .collect::<Vec<u8>>();
        central
            .send_data(WriteDataMsg {
                stream_id: 42,
                data: StreamWriteData::Data(make_data_buf(&body)),
            })
            .await
            .unwrap();
        let got = central.io_writer.out.clone();
        // Byte-exact comparison already verifies chunk boundaries and that the
        // first chunk isn't repeated.
        assert_eq!(got, expected_frame(42, &body));
        // Walk the emitted stream, decoding each Data frame, to prove there were
        // multiple frames with distinct body slices that reassemble to `body`.
        let mut frames = 0usize;
        let mut reassembled = Vec::new();
        let mut pos = 0usize;
        let data_code = Header::Data.encode()[0];
        while pos < got.len() {
            assert_eq!(got[pos], data_code, "expected Data header");
            let dh_off = pos + Header::SIZE;
            let dh = DataHeader::decode(got[dh_off..dh_off + DataHeader::SIZE].try_into().unwrap());
            let blen = usize::from(dh.body_len);
            assert_eq!(dh.stream_id, 42);
            let body_off = dh_off + DataHeader::SIZE;
            reassembled.extend_from_slice(&got[body_off..body_off + blen]);
            pos = body_off + blen;
            frames += 1;
        }
        assert_eq!(reassembled, body);
        let expected_count = (big_len + usize::from(BodyLen::MAX) - 1) / usize::from(BodyLen::MAX);
        assert_eq!(frames, expected_count);
    }
}
