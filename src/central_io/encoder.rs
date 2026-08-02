use std::{
    collections::HashMap,
    io::{self, IoSlice},
    time::Duration,
};

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{
    central_io::scheduler::{StreamWriteData, WriteControlMsg, WriteControlRx, WriteDataMsg, WriteDataRx},
    control::DeadControl,
    protocol::{
        BodyLen, CloseWriteExtMsg, DataHeader, DataHeaderExt, Header, Offset, StreamId, StreamIdMsg,
    },
};

/// Maximum body length in a single Data frame when `frame_reassembly` is on.
/// The total on-wire frame is `Header::SIZE (1) + DataHeaderExt::SIZE (10) +
/// body`, which must fit within a single 64 KiB transport frame.
const REASSEMBLY_MAX_BODY: usize = 64 * 1024 - Header::SIZE - DataHeaderExt::SIZE;

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
    /// Reused staging buffer for coalescing a non-vectored Data frame's fixed
    /// header and body into a single transport write. Grows to at most
    /// `Header::SIZE + DataHeaderExt::SIZE + usize::from(BodyLen::MAX)`.
    frame_buf: Vec<u8>,
    /// When true, Data frames carry a per-stream u32 byte offset and
    /// CloseWrite carries a final offset, emitted via `DataHeaderExt` /
    /// `CloseWriteExtMsg`. When false, the stock headers are used and the
    /// wire is byte-identical to the pre-reassembly protocol.
    frame_reassembly: bool,
    /// Next byte offset to emit for each stream, tracked as a u64 and
    /// emitted as `Offset` (u32) per frame — wrap-safe because the reader
    /// uses TCP-style serial-number comparison. Only used when
    /// `frame_reassembly` is true.
    next_offset: HashMap<StreamId, u64>,
}
impl<W> CentralIoWriter<W> {
    pub fn new(io_writer: W, frame_reassembly: bool) -> Self {
        Self {
            io_writer,
            frame_buf: Vec::new(),
            frame_reassembly,
            next_offset: HashMap::new(),
        }
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
        match msg {
            WriteControlMsg::CloseRead(stream_id) => {
                self.send_control_(Header::CloseRead, stream_id).await
            }
            WriteControlMsg::ForceCloseWrite(stream_id) => {
                debug_assert!(
                    !self.frame_reassembly,
                    "ForceCloseWrite is the legacy mode-off abort path"
                );
                self.send_control_(Header::CloseWrite, stream_id).await
            }
        }
    }
    async fn send_close_write_ext(
        &mut self,
        stream_id: StreamId,
        final_offset: Offset,
    ) -> io::Result<()> {
        let hdr = Header::CloseWrite;
        let payload = CloseWriteExtMsg {
            stream_id,
            final_offset,
        };
        let hdr = hdr.encode();
        let payload = payload.encode();
        let mut concat = hdr.into_iter().chain(payload);
        let buf: [u8; Header::SIZE + CloseWriteExtMsg::SIZE] =
            core::array::from_fn(|_| concat.next().unwrap());
        self.io_writer.write_all(&buf).await?;
        Ok(())
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
            StreamWriteData::Open { wire } => {
                self.next_offset.remove(&msg.stream_id);
                if !wire {
                    return Ok(());
                }
                return self.send_control_(Header::Open, msg.stream_id).await;
            }
            StreamWriteData::Fin => {
                if self.frame_reassembly {
                    let final_offset = self.next_offset.remove(&msg.stream_id).unwrap_or(0);
                    return self
                        .send_close_write_ext(msg.stream_id, final_offset as Offset)
                        .await;
                }
                let hdr = Header::CloseWrite;
                return self.send_control_(hdr, msg.stream_id).await;
            }
            StreamWriteData::Data(data_buf) => data_buf,
        };
        let hdr = Header::Data;
        let max_body = if self.frame_reassembly {
            REASSEMBLY_MAX_BODY
        } else {
            usize::from(BodyLen::MAX)
        };
        let mut body_offset = 0usize;
        while body_offset != data_buf.len() {
            let body_len = (data_buf.len() - body_offset).min(max_body);
            let body_len_u16 = BodyLen::try_from(body_len).unwrap();
            let fixed_buf: Vec<u8>;
            if self.frame_reassembly {
                let cur_offset_64 = *self.next_offset.get(&msg.stream_id).unwrap_or(&0);
                let data_hdr = DataHeaderExt {
                    stream_id: msg.stream_id,
                    body_len: body_len_u16,
                    offset: cur_offset_64 as Offset,
                };
                let hdr_bytes = hdr.encode();
                let data_hdr_bytes = data_hdr.encode();
                let mut concat = hdr_bytes.into_iter().chain(data_hdr_bytes);
                let buf: [u8; Header::SIZE + DataHeaderExt::SIZE] =
                    core::array::from_fn(|_| concat.next().unwrap());
                fixed_buf = buf.to_vec();
                self.next_offset
                    .insert(msg.stream_id, cur_offset_64 + body_len as u64);
            } else {
                let data_hdr = DataHeader {
                    stream_id: msg.stream_id,
                    body_len: body_len_u16,
                };
                let hdr_bytes = hdr.encode();
                let data_hdr_bytes = data_hdr.encode();
                let mut concat = hdr_bytes.into_iter().chain(data_hdr_bytes);
                let buf: [u8; Header::SIZE + DataHeader::SIZE] =
                    core::array::from_fn(|_| concat.next().unwrap());
                fixed_buf = buf.to_vec();
            }
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
                    let bufs = &mut [IoSlice::new(header_remaining), IoSlice::new(body_remaining)];
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
            if fixed_header.is_empty() || body.is_empty() {
                // Keep the existing single-write behaviour when either part is
                // empty; only coalesce when both are non-empty.
                if !fixed_header.is_empty() {
                    self.io_writer.write_all(fixed_header).await?;
                }
                if !body.is_empty() {
                    self.io_writer.write_all(body).await?;
                }
            } else {
                self.frame_buf.clear();
                self.frame_buf.extend_from_slice(fixed_header);
                self.frame_buf.extend_from_slice(body);
                self.io_writer.write_all(&self.frame_buf).await?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, IoSlice},
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
        time::Duration,
    };

    use primitive::arena::obj_pool::arc_buf_pool;
    use tokio::io::AsyncWrite;

    use super::{CentralIoWriter, REASSEMBLY_MAX_BODY};
    use crate::central_io::scheduler::{StreamWriteData, WriteControlMsg, WriteDataMsg};
    use crate::protocol::{BodyLen, DataHeader, DataHeaderExt, Header};

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
        let writer = MockWriter {
            out: Vec::new(),
            max_per_write: Some(3),
            vectored: true,
            write_vectored_calls: Arc::clone(&calls),
        };
        let mut central = CentralIoWriter::new(writer, false);
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
        let writer = MockWriter {
            out: Vec::new(),
            max_per_write: Some(5),
            vectored: false,
            write_vectored_calls: Arc::new(Mutex::new(0)),
        };
        let mut central = CentralIoWriter::new(writer, false);
        let body = (0u8..200u8).collect::<Vec<u8>>();
        central
            .send_data(WriteDataMsg {
                stream_id: 7,
                data: StreamWriteData::Data(make_data_buf(&body)),
            })
            .await
            .unwrap();
        assert_eq!(
            central
                .io_writer
                .write_vectored_calls
                .lock()
                .unwrap()
                .clone(),
            0,
            "fallback path should not invoke write_vectored"
        );
        let got = central.io_writer.out.clone();
        assert_eq!(got, expected_frame(7, &body));
    }

    /// On a non-vectored transport a Data frame must be emitted as exactly one
    /// write call whose bytes are the fixed header immediately followed by the
    /// body. The frame_buf is reused across frames, so after two frames it holds
    /// the second frame's contents and the byte output reassembles correctly.
    #[tokio::test]
    async fn non_vectored_data_frame_coalesces_into_single_write() {
        struct CountingWriter {
            out: Vec<u8>,
            write_calls: Arc<Mutex<usize>>,
        }
        impl AsyncWrite for CountingWriter {
            fn poll_write(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                let this = unsafe { self.get_unchecked_mut() };
                *this.write_calls.lock().unwrap() += 1;
                this.out.extend_from_slice(buf);
                Poll::Ready(Ok(buf.len()))
            }
            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn is_write_vectored(&self) -> bool {
                false
            }
        }

        let calls = Arc::new(Mutex::new(0usize));
        let writer = CountingWriter {
            out: Vec::new(),
            write_calls: Arc::clone(&calls),
        };
        let mut central = CentralIoWriter::new(writer, false);

        let body: Vec<u8> = (0u8..=255).cycle().take(1234).collect();
        central
            .send_data(WriteDataMsg {
                stream_id: 7,
                data: StreamWriteData::Data(make_data_buf(&body)),
            })
            .await
            .unwrap();

        let got = central.io_writer.out.clone();
        assert_eq!(got, expected_frame(7, &body));
        assert_eq!(
            *calls.lock().unwrap(),
            1,
            "non-vectored Data frame should reach transport as exactly one write"
        );
        assert_eq!(
            central.frame_buf,
            expected_frame(7, &body),
            "staging buffer should retain the last coalesced frame"
        );
    }

    #[tokio::test]
    async fn body_larger_than_max_creates_multiple_frames_no_first_chunk_repeat() {
        let writer = MockWriter {
            out: Vec::new(),
            max_per_write: None,
            vectored: true,
            write_vectored_calls: Arc::new(Mutex::new(0)),
        };
        let mut central = CentralIoWriter::new(writer, false);
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
        let expected_count = big_len.div_ceil(usize::from(BodyLen::MAX));
        assert_eq!(frames, expected_count);
    }

    /// `write_all` of a payload larger than `BodyLen::MAX` still reaches the
    /// peer intact end-to-end through the mux stream pair.
    #[tokio::test(flavor = "multi_thread")]
    async fn large_write_all_reaches_peer_intact() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        use crate::{Initiation, MuxConfig, spawn_mux_no_reconnection};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move { listener.accept().await.unwrap() });

        let b = tokio::net::TcpStream::connect(addr).await.unwrap();
        let a = accept.await.unwrap().0;

        let mut spawner = tokio::task::JoinSet::new();
        let (a_r, a_w) = a.into_split();
        let (opener, _) = spawn_mux_no_reconnection(
            a_r,
            a_w,
            MuxConfig {
                initiation: Initiation::Server,
                heartbeat_interval: Duration::from_secs(5),
                frame_reassembly: false,
            },
            &mut spawner,
        );
        let (b_r, b_w) = b.into_split();
        let (_, mut accepter) = spawn_mux_no_reconnection(
            b_r,
            b_w,
            MuxConfig {
                initiation: Initiation::Client,
                heartbeat_interval: Duration::from_secs(5),
                frame_reassembly: false,
            },
            &mut spawner,
        );

        let (a_stream, b_stream) = tokio::join!(opener.open(), accepter.accept());
        let mut a_stream = a_stream.unwrap().1;
        let mut b_stream = b_stream.unwrap().0;

        // Larger than BodyLen::MAX so it spans multiple wire frames and
        // exercises the split/reinsert path.
        let payload: Vec<u8> = (0u8..=255)
            .cycle()
            .take(usize::from(BodyLen::MAX) * 3 + 123)
            .collect();
        let expected = payload.clone();

        let writer = tokio::spawn(async move {
            a_stream.write_all(&payload).await.unwrap();
            a_stream.shutdown().unwrap();
            a_stream
        });
        let mut received = Vec::new();
        b_stream.read_to_end(&mut received).await.unwrap();
        writer.await.unwrap();
        assert_eq!(received, expected);
    }

    // ---- Frame-reassembly wire tests ----

    /// Mode off: the writer emits the exact stock header bytes (1-byte
    /// Header::Data + 6-byte DataHeader with no offset field). A fixed
    /// input must produce a byte-identical frame to the pre-reassembly
    /// protocol.
    #[tokio::test]
    async fn mode_off_wire_identical() {
        struct SinkWriter(Vec<u8>);
        impl AsyncWrite for SinkWriter {
            fn poll_write(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                let this = unsafe { self.get_unchecked_mut() };
                this.0.extend_from_slice(buf);
                Poll::Ready(Ok(buf.len()))
            }
            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn is_write_vectored(&self) -> bool {
                false
            }
        }
        let mut central = CentralIoWriter::new(SinkWriter(Vec::new()), false);
        let body = (0u8..100u8).collect::<Vec<u8>>();
        central
            .send_data(WriteDataMsg {
                stream_id: 42,
                data: StreamWriteData::Data(make_data_buf(&body)),
            })
            .await
            .unwrap();
        let mut expected = Vec::new();
        expected.push(0x02);
        expected.extend_from_slice(&42u32.to_be_bytes());
        expected.extend_from_slice(&100u16.to_be_bytes());
        expected.extend_from_slice(&body);
        assert_eq!(
            central.io_writer.0, expected,
            "mode-off wire must be byte-identical to stock"
        );
    }

    #[tokio::test]
    async fn mode_off_force_close_write_uses_stock_frame_without_consuming_offset() {
        struct SinkWriter(Vec<u8>);
        impl AsyncWrite for SinkWriter {
            fn poll_write(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                let this = unsafe { self.get_unchecked_mut() };
                this.0.extend_from_slice(buf);
                Poll::Ready(Ok(buf.len()))
            }
            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn is_write_vectored(&self) -> bool {
                false
            }
        }
        let mut central = CentralIoWriter::new(SinkWriter(Vec::new()), false);
        central.next_offset.insert(9, 123);
        central
            .send_control(WriteControlMsg::ForceCloseWrite(9))
            .await
            .unwrap();
        let mut expected = Vec::new();
        expected.push(Header::CloseWrite.encode()[0]);
        expected.extend_from_slice(&9u32.to_be_bytes());
        assert_eq!(central.io_writer.0, expected);
        assert_eq!(
            central.next_offset.get(&9),
            Some(&123),
            "Legacy abort must not consume FIN offset state"
        );
    }

    /// Mode on: Data header carries a u32 offset, CloseWrite carries a
    /// final offset. Verify the wire layout is the extended form.
    #[tokio::test]
    async fn mode_on_wire_has_offset() {
        struct SinkWriter(Vec<u8>);
        impl AsyncWrite for SinkWriter {
            fn poll_write(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                let this = unsafe { self.get_unchecked_mut() };
                this.0.extend_from_slice(buf);
                Poll::Ready(Ok(buf.len()))
            }
            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn is_write_vectored(&self) -> bool {
                false
            }
        }
        let mut central = CentralIoWriter::new(SinkWriter(Vec::new()), true);
        let body: Vec<u8> = (0u8..50u8).collect::<Vec<u8>>();
        central
            .send_data(WriteDataMsg {
                stream_id: 7,
                data: StreamWriteData::Data(make_data_buf(&body)),
            })
            .await
            .unwrap();
        let mut expected = Vec::new();
        expected.push(0x02);
        expected.extend_from_slice(&7u32.to_be_bytes());
        expected.extend_from_slice(&50u16.to_be_bytes());
        expected.extend_from_slice(&0u32.to_be_bytes());
        expected.extend_from_slice(&body);
        assert_eq!(central.io_writer.0, expected);
        central
            .send_data(WriteDataMsg {
                stream_id: 7,
                data: StreamWriteData::Data(make_data_buf(&body)),
            })
            .await
            .unwrap();
        let mut expected2 = expected.clone();
        expected2.push(0x02);
        expected2.extend_from_slice(&7u32.to_be_bytes());
        expected2.extend_from_slice(&50u16.to_be_bytes());
        expected2.extend_from_slice(&50u32.to_be_bytes());
        expected2.extend_from_slice(&body);
        assert_eq!(central.io_writer.0, expected2);
        central
            .send_data(WriteDataMsg {
                stream_id: 7,
                data: StreamWriteData::Fin,
            })
            .await
            .unwrap();
        let mut expected3 = expected2;
        expected3.push(Header::CloseWrite.encode()[0]);
        expected3.extend_from_slice(&7u32.to_be_bytes());
        expected3.extend_from_slice(&100u32.to_be_bytes());
        assert_eq!(
            central.io_writer.0, expected3,
            "mode-on must end in exactly one extended CloseWrite"
        );
    }

    #[tokio::test]
    async fn recycled_stream_id_restarts_at_offset_zero() {
        struct SinkWriter(Vec<u8>);
        impl AsyncWrite for SinkWriter {
            fn poll_write(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                let this = unsafe { self.get_unchecked_mut() };
                this.0.extend_from_slice(buf);
                Poll::Ready(Ok(buf.len()))
            }
            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn is_write_vectored(&self) -> bool {
                false
            }
        }
        let mut central = CentralIoWriter::new(SinkWriter(Vec::new()), true);
        let body: Vec<u8> = (0u8..50u8).collect::<Vec<u8>>();
        central
            .send_data(WriteDataMsg {
                stream_id: 7,
                data: StreamWriteData::Data(make_data_buf(&body)),
            })
            .await
            .unwrap();
        central.io_writer.0.clear();
        central
            .send_data(WriteDataMsg {
                stream_id: 7,
                data: StreamWriteData::Open { wire: true },
            })
            .await
            .unwrap();
        central
            .send_data(WriteDataMsg {
                stream_id: 7,
                data: StreamWriteData::Data(make_data_buf(&body)),
            })
            .await
            .unwrap();
        let mut expected = Vec::new();
        expected.push(Header::Open.encode()[0]);
        expected.extend_from_slice(&7u32.to_be_bytes());
        expected.push(0x02);
        expected.extend_from_slice(&7u32.to_be_bytes());
        expected.extend_from_slice(&50u16.to_be_bytes());
        expected.extend_from_slice(&0u32.to_be_bytes());
        expected.extend_from_slice(&body);
        assert_eq!(
            central.io_writer.0, expected,
            "a recycled id must not inherit its predecessor's wire offset"
        );
        central.io_writer.0.clear();
        central
            .send_data(WriteDataMsg {
                stream_id: 7,
                data: StreamWriteData::Open { wire: false },
            })
            .await
            .unwrap();
        assert!(
            central.io_writer.0.is_empty(),
            "a non-wire Open must not emit a frame"
        );
        central
            .send_data(WriteDataMsg {
                stream_id: 7,
                data: StreamWriteData::Fin,
            })
            .await
            .unwrap();
        let mut expected = Vec::new();
        expected.push(Header::CloseWrite.encode()[0]);
        expected.extend_from_slice(&7u32.to_be_bytes());
        expected.extend_from_slice(&0u32.to_be_bytes());
        assert_eq!(
            central.io_writer.0, expected,
            "the final offset must count only the recycled stream's own bytes"
        );
    }

    /// Reassembly frames must never exceed 64 KiB total on-wire size.
    /// Before the cap, `BodyLen::MAX (65535)` was used as the max body
    /// in reassembly mode too, yielding 65546-byte frames that overflow
    /// a single 64 KiB transport datagram.
    #[tokio::test]
    async fn reassembly_frame_total_le_64kib() {
        struct SinkWriter(Vec<u8>);
        impl AsyncWrite for SinkWriter {
            fn poll_write(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                let this = unsafe { self.get_unchecked_mut() };
                this.0.extend_from_slice(buf);
                Poll::Ready(Ok(buf.len()))
            }
            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn is_write_vectored(&self) -> bool {
                false
            }
        }

        let mut central = CentralIoWriter::new(SinkWriter(Vec::new()), true);

        // Two chunks: REASSEMBLY_MAX_BODY + 1 forces the writer to split.
        let payload_len = REASSEMBLY_MAX_BODY + 7890;
        let payload: Vec<u8> = (0u8..=u8::MAX).cycle().take(payload_len).collect();
        central
            .send_data(WriteDataMsg {
                stream_id: 1,
                data: StreamWriteData::Data(make_data_buf(&payload)),
            })
            .await
            .unwrap();

        let out = &central.io_writer.0;
        let mut pos = 0usize;
        let data_code = Header::Data.encode()[0];
        while pos < out.len() {
            assert_eq!(out[pos], data_code, "expected Data header");
            // frame = Header(1) + DataHeaderExt(10) + body
            let body_len = u16::from_be_bytes(out[pos + 5..pos + 7].try_into().unwrap()) as usize;
            let frame_total = Header::SIZE + DataHeaderExt::SIZE + body_len;
            assert!(
                frame_total <= 64 * 1024,
                "reassembly frame total {frame_total} exceeds 64 KiB"
            );
            pos += frame_total;
        }
        assert_eq!(pos, out.len());
    }
}
