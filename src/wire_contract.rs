//! Literal pins for the `mux` wire format.
//!
//! Every expectation below is a **hand-written duplicate** of bytes that a
//! peer built at another revision has to agree on. `mux` framing is a
//! cross-version contract: it is consumed by `rtp_mux` and `proxy` at
//! independently pinned revisions, so a frame code renumbering or an
//! endianness swap that is applied consistently to the encoder *and* the
//! decoder round-trips through this crate's own code perfectly while breaking
//! every peer built elsewhere.
//!
//! These tests therefore deliberately do **not** build their expected value by
//! calling the production `encode`, `decode`, `to_be_bytes` or `to_le_bytes`.
//! A pin that reads its expectation back out of the production encoder is
//! tautological — it passes for *any* encoding, which is precisely the hole
//! this module closes. Do not "deduplicate" these literals against the
//! production types or collapse them into round-trip assertions; the
//! duplication is the oracle.

use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use primitive::arena::obj_pool::arc_buf_pool;
use tokio::io::AsyncWrite;

use crate::{
    central_io::{
        DataBuf,
        encoder::CentralIoEncoder,
        reader::{CentralIoReadMsg, CentralIoReader},
        scheduler::{StreamWriteData, WriteControlMsg, WriteDataMsg},
    },
    lane_hello::{
        GROUP_TOKEN_LEN, GroupToken, HELLO_LEN, PAIRING_NONCE_LEN, PairingNonce, read_lane_hello,
        write_lane_hello,
    },
    lane_message::DEFAULT_MAX_MESSAGE_LEN,
    migration_wire::{RESUME_HEADER_LEN, ResumeHeader},
    padding::{MAX_PAD, PAD_LEN_LEN, append_tail, skip_tail},
    protocol::{CloseWriteExtMsg, DataHeader, DataHeaderExt, Header, Side, StreamIdMsg},
    traffic_class::LaneClass,
    udp_mux::{MAX_UDP_MUX_DATAGRAM_LEN, UdpMuxReader, UdpMuxWriter},
};

// ---------------------------------------------------------------------------
// Shared test writer
// ---------------------------------------------------------------------------

/// Records every byte a production writer emits. The recorder is shared so the
/// test can read the bytes without reaching into a private field of the type
/// under test.
#[derive(Clone, Default)]
struct SharedRecorder(Arc<Mutex<Vec<u8>>>);

impl SharedRecorder {
    fn bytes(&self) -> Vec<u8> {
        self.0.lock().unwrap().clone()
    }
}

impl AsyncWrite for SharedRecorder {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    /// Force the coalescing path so a frame is observed as one contiguous
    /// buffer regardless of how the encoder splits a vectored write.
    fn is_write_vectored(&self) -> bool {
        false
    }
}

fn data_buf(bytes: &[u8]) -> DataBuf {
    let pool = arc_buf_pool::<u8>(None, std::num::NonZeroUsize::new(1).unwrap());
    let mut buf = pool.take_scoped();
    buf.clear();
    buf.extend_from_slice(bytes);
    buf
}

/// Assert `bytes` is exactly `prefix` followed by a well-formed padding tail:
/// `[pad_len u16]` in **big-endian** order, then `pad_len` zero bytes.
///
/// The length is assembled with hand-written shifts rather than
/// `u16::from_be_bytes` so this helper is an independent statement of the
/// byte order, not a mirror of the production encoder.
fn assert_prefix_then_be_tail(bytes: &[u8], prefix: &[u8], what: &str) {
    assert!(
        bytes.len() >= prefix.len() + PAD_LEN_LEN,
        "{what}: frame {bytes:?} is shorter than its fixed prefix plus a pad-length field"
    );
    assert_eq!(&bytes[..prefix.len()], prefix, "{what}: fixed prefix");
    let pad_len = ((bytes[prefix.len()] as usize) << 8) | (bytes[prefix.len() + 1] as usize);
    assert_eq!(
        bytes.len(),
        prefix.len() + PAD_LEN_LEN + pad_len,
        "{what}: pad-length field is not the big-endian length of the tail: {bytes:?}"
    );
    assert!(
        bytes[prefix.len() + PAD_LEN_LEN..].iter().all(|b| *b == 0),
        "{what}: padding must be zero-filled"
    );
}

// ---------------------------------------------------------------------------
// protocol::Header — frame kind codes
// ---------------------------------------------------------------------------

#[test]
fn header_kind_codes_are_literal() {
    assert_eq!(Header::SIZE, 1);
    assert_eq!(Header::Heartbeat.encode(), [0x00]);
    assert_eq!(Header::Open.encode(), [0x01]);
    assert_eq!(Header::Data.encode(), [0x02]);
    assert_eq!(Header::CloseRead.encode(), [0x03]);
    assert_eq!(Header::CloseWrite.encode(), [0x04]);
}

#[test]
fn header_kind_codes_decode_from_their_literal_bytes() {
    assert!(matches!(Header::decode([0x00]), Some(Header::Heartbeat)));
    assert!(matches!(Header::decode([0x01]), Some(Header::Open)));
    assert!(matches!(Header::decode([0x02]), Some(Header::Data)));
    assert!(matches!(Header::decode([0x03]), Some(Header::CloseRead)));
    assert!(matches!(Header::decode([0x04]), Some(Header::CloseWrite)));
    // The five codes above are the whole space; 0x05 is already rejected by the
    // in-module `test_header_decode_rejects_reserved_codes`, which pins the
    // valid set as a subset of 0x00..=0x04. The per-kind equality assertions
    // above are what pin *which* kind owns which of those five bytes.
}

// ---------------------------------------------------------------------------
// protocol field layouts — big-endian, fixed width
// ---------------------------------------------------------------------------

#[test]
fn stream_id_msg_is_a_big_endian_u32() {
    assert_eq!(StreamIdMsg::SIZE, 4);
    let encoded = StreamIdMsg {
        stream_id: 0x0102_0304,
    }
    .encode();
    assert_eq!(encoded, [0x01, 0x02, 0x03, 0x04]);
    assert_eq!(StreamIdMsg::decode(encoded).stream_id, 0x0102_0304);
}

#[test]
fn data_header_is_stream_id_then_big_endian_body_len() {
    assert_eq!(DataHeader::SIZE, 6);
    let encoded = DataHeader {
        stream_id: 0x0102_0304,
        body_len: 0x0506,
    }
    .encode();
    assert_eq!(encoded, [0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
    let decoded = DataHeader::decode(encoded);
    assert_eq!(decoded.stream_id, 0x0102_0304);
    assert_eq!(decoded.body_len, 0x0506);
}

#[test]
fn data_header_ext_appends_a_big_endian_offset() {
    assert_eq!(DataHeaderExt::SIZE, 10);
    let encoded = DataHeaderExt {
        stream_id: 0x0102_0304,
        body_len: 0x0506,
        offset: 0x0708_090A,
    }
    .encode();
    assert_eq!(
        encoded,
        [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A]
    );
    let decoded = DataHeaderExt::decode(encoded);
    assert_eq!(decoded.stream_id, 0x0102_0304);
    assert_eq!(decoded.body_len, 0x0506);
    assert_eq!(decoded.offset, 0x0708_090A);
}

#[test]
fn close_write_ext_is_stream_id_then_big_endian_final_offset() {
    assert_eq!(CloseWriteExtMsg::SIZE, 8);
    let encoded = CloseWriteExtMsg {
        stream_id: 0x0102_0304,
        final_offset: 0x0506_0708,
    }
    .encode();
    assert_eq!(encoded, [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    let decoded = CloseWriteExtMsg::decode(encoded);
    assert_eq!(decoded.stream_id, 0x0102_0304);
    assert_eq!(decoded.final_offset, 0x0506_0708);
}

// ---------------------------------------------------------------------------
// central_io: the composite on-wire frame
// ---------------------------------------------------------------------------

/// Feed one literal frame through the production reader and return what it
/// decoded.
async fn read_one_frame(src: &[u8], frame_reassembly: bool) -> io::Result<CentralIoReadMsg> {
    let mut reader = CentralIoReader::new(src, frame_reassembly);
    let mut first_receive_tx: Option<tokio::sync::oneshot::Sender<()>> = None;
    reader
        .recv_with_steady_deadline(
            Duration::from_secs(5),
            Duration::from_secs(5),
            &mut first_receive_tx,
        )
        .await
}

/// Mode off, reader side: `[kind][stream_id u32 BE][body_len u16 BE][body]`
/// for Data, and `[kind][stream_id u32 BE][pad_len u16 BE][padding]` for the
/// control frames and heartbeat.
#[tokio::test]
async fn reader_decodes_literal_mode_off_frames() {
    let data = [0x02, 0x01, 0x02, 0x03, 0x04, 0x00, 0x03, 0xAA, 0xBB, 0xCC];
    match read_one_frame(&data, false).await.expect("Data frame") {
        CentralIoReadMsg::Data(stream, offset, body) => {
            assert_eq!(stream, 0x0102_0304);
            assert_eq!(offset, 0, "mode off carries no offset on the wire");
            assert_eq!(&body[..], &[0xAA, 0xBB, 0xCC]);
        }
        other => panic!("expected Data, got {other:?}"),
    }

    // Open: a zero-length padding tail completes the frame literally.
    let open = [0x01, 0x00, 0x00, 0x00, 0x2A, 0x00, 0x00];
    match read_one_frame(&open, false).await.expect("Open frame") {
        CentralIoReadMsg::Open(stream) => assert_eq!(stream, 42),
        other => panic!("expected Open, got {other:?}"),
    }

    let close_read = [0x03, 0x00, 0x00, 0x00, 0x2A, 0x00, 0x00];
    match read_one_frame(&close_read, false)
        .await
        .expect("CloseRead frame")
    {
        CentralIoReadMsg::Close(stream, side, offset) => {
            assert_eq!(stream, 42);
            assert_eq!(side, Side::Read);
            assert_eq!(offset, 0);
        }
        other => panic!("expected Close, got {other:?}"),
    }

    // Mode-off CloseWrite is the stock frame: no final offset follows.
    let close_write = [0x04, 0x00, 0x00, 0x00, 0x2A, 0x00, 0x00];
    match read_one_frame(&close_write, false)
        .await
        .expect("CloseWrite frame")
    {
        CentralIoReadMsg::Close(stream, side, offset) => {
            assert_eq!(stream, 42);
            assert_eq!(side, Side::Write);
            assert_eq!(offset, 0, "mode-off CloseWrite has no offset field");
        }
        other => panic!("expected Close, got {other:?}"),
    }

    // A heartbeat is skipped, not yielded: a literal heartbeat frame followed
    // by a literal Open frame must decode to the Open.
    let heartbeat_then_open = [
        0x00, 0x00, 0x00, // heartbeat kind + zero-length tail
        0x01, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00,
    ];
    match read_one_frame(&heartbeat_then_open, false)
        .await
        .expect("heartbeat then Open")
    {
        CentralIoReadMsg::Open(stream) => assert_eq!(stream, 7),
        other => panic!("expected Open after a heartbeat, got {other:?}"),
    }
}

/// Mode on, reader side: Data carries a trailing `offset u32 BE`, CloseWrite
/// carries a trailing `final_offset u32 BE`.
#[tokio::test]
async fn reader_decodes_literal_mode_on_frames() {
    let data = [
        0x02, 0x00, 0x00, 0x00, 0x07, // Data, stream 7
        0x00, 0x05, // body_len 5
        0x00, 0x00, 0x01, 0x00, // offset 256
        b'h', b'e', b'l', b'l', b'o',
    ];
    match read_one_frame(&data, true).await.expect("Data frame") {
        CentralIoReadMsg::Data(stream, offset, body) => {
            assert_eq!(stream, 7);
            assert_eq!(offset, 256);
            assert_eq!(&body[..], b"hello");
        }
        other => panic!("expected Data, got {other:?}"),
    }

    let close_write = [
        0x04, 0x00, 0x00, 0x00, 0x07, // CloseWrite, stream 7
        0x00, 0x00, 0x01, 0x00, // final_offset 256
        0x00, 0x00, // zero-length padding tail
    ];
    match read_one_frame(&close_write, true)
        .await
        .expect("CloseWrite frame")
    {
        CentralIoReadMsg::Close(stream, side, offset) => {
            assert_eq!(stream, 7);
            assert_eq!(side, Side::Write);
            assert_eq!(offset, 256);
        }
        other => panic!("expected Close, got {other:?}"),
    }
}

#[tokio::test]
async fn reader_rejects_a_reserved_frame_kind_byte() {
    let error = read_one_frame(&[0x05, 0x00, 0x00], false)
        .await
        .expect_err("0x05 is not a frame kind");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

/// Mode off, writer side: every frame the encoder emits, checked against its
/// literal bytes.
#[tokio::test]
async fn encoder_emits_literal_mode_off_frames() {
    let recorder = SharedRecorder::default();
    let mut encoder = CentralIoEncoder::new(recorder.clone(), false);

    encoder
        .send_data(WriteDataMsg {
            stream_id: 42,
            data: StreamWriteData::Open { wire: true },
        })
        .await
        .unwrap();
    assert_prefix_then_be_tail(&recorder.bytes(), &[0x01, 0x00, 0x00, 0x00, 0x2A], "Open");
    recorder.0.lock().unwrap().clear();

    encoder
        .send_control(WriteControlMsg::CloseRead(9))
        .await
        .unwrap();
    assert_prefix_then_be_tail(
        &recorder.bytes(),
        &[0x03, 0x00, 0x00, 0x00, 0x09],
        "CloseRead",
    );
    recorder.0.lock().unwrap().clear();

    encoder
        .send_data(WriteDataMsg {
            stream_id: 9,
            data: StreamWriteData::Fin,
        })
        .await
        .unwrap();
    assert_prefix_then_be_tail(
        &recorder.bytes(),
        &[0x04, 0x00, 0x00, 0x00, 0x09],
        "CloseWrite",
    );
    recorder.0.lock().unwrap().clear();

    encoder.send_heartbeat().await.unwrap();
    assert_prefix_then_be_tail(&recorder.bytes(), &[0x00], "Heartbeat");
    recorder.0.lock().unwrap().clear();

    encoder
        .send_data(WriteDataMsg {
            stream_id: 7,
            data: StreamWriteData::Data(data_buf(&[0xAA, 0xBB, 0xCC])),
        })
        .await
        .unwrap();
    assert_eq!(
        recorder.bytes(),
        [
            0x02, 0x00, 0x00, 0x00, 0x07, // Data, stream 7
            0x00, 0x03, // body_len 3
            0xAA, 0xBB, 0xCC, // body, no padding tail on a Data frame
        ],
        "mode-off Data frame bytes"
    );
}

/// Mode on, writer side: the same frames plus the trailing offset fields.
#[tokio::test]
async fn encoder_emits_literal_mode_on_frames() {
    let recorder = SharedRecorder::default();
    let mut encoder = CentralIoEncoder::new(recorder.clone(), true);

    encoder
        .send_data(WriteDataMsg {
            stream_id: 7,
            data: StreamWriteData::Data(data_buf(&[0xAA, 0xBB, 0xCC])),
        })
        .await
        .unwrap();
    assert_eq!(
        recorder.bytes(),
        [
            0x02, 0x00, 0x00, 0x00, 0x07, // Data, stream 7
            0x00, 0x03, // body_len 3
            0x00, 0x00, 0x00, 0x00, // offset 0
            0xAA, 0xBB, 0xCC,
        ],
        "first mode-on Data frame bytes"
    );
    recorder.0.lock().unwrap().clear();

    // The second frame's offset counts the bytes already emitted on this
    // stream: 3. A swapped or wrapped field would move these four bytes.
    encoder
        .send_data(WriteDataMsg {
            stream_id: 7,
            data: StreamWriteData::Data(data_buf(&[0xDD])),
        })
        .await
        .unwrap();
    assert_eq!(
        recorder.bytes(),
        [
            0x02, 0x00, 0x00, 0x00, 0x07, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0xDD
        ],
        "second mode-on Data frame bytes"
    );
    recorder.0.lock().unwrap().clear();

    // Mode-on CloseWrite carries the stream's final offset before the tail.
    encoder
        .send_data(WriteDataMsg {
            stream_id: 7,
            data: StreamWriteData::Fin,
        })
        .await
        .unwrap();
    assert_prefix_then_be_tail(
        &recorder.bytes(),
        &[0x04, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x04],
        "extended CloseWrite",
    );
}

// ---------------------------------------------------------------------------
// padding tail
// ---------------------------------------------------------------------------

#[test]
fn padding_constants_are_literal() {
    assert_eq!(PAD_LEN_LEN, 2);
    assert_eq!(MAX_PAD, 1500);
}

/// Reader side: a literal tail declares a **big-endian** length. The buffer
/// below is `[0x01, 0x00]` (256) followed by 256 zero bytes and the sentinel
/// `0xEE 0xEE`; if the field were little-endian the reader would consume 1
/// byte and leave the rest.
#[tokio::test]
async fn skip_tail_honours_a_literal_big_endian_length() {
    let mut input = vec![0x01u8, 0x00];
    input.extend(std::iter::repeat_n(0u8, 256));
    input.extend_from_slice(&[0xEE, 0xEE]);
    let mut reader: &[u8] = &input;
    skip_tail(&mut reader).await.unwrap();
    assert_eq!(reader, &[0xEE, 0xEE]);
}

/// Writer side: the length field is the big-endian encoding of the tail.
#[test]
fn append_tail_writes_a_big_endian_length_and_zero_fill() {
    let mut saw_non_empty_tail = false;
    for _ in 0..16 {
        let mut buf = vec![0xABu8];
        append_tail(&mut buf);
        assert!(buf.len() >= 3, "prefix byte plus the 2-byte length field");
        let pad_len = ((buf[1] as usize) << 8) | (buf[2] as usize);
        assert_eq!(
            buf.len(),
            1 + PAD_LEN_LEN + pad_len,
            "pad-length field must be the big-endian length of the tail: {buf:?}"
        );
        assert!(
            buf[3..].iter().all(|b| *b == 0),
            "padding must be zero-filled: {buf:?}"
        );
        saw_non_empty_tail |= pad_len != 0;
    }
    assert!(
        saw_non_empty_tail,
        "sixteen consecutive zero-length draws make this pin vacuous"
    );
}

// ---------------------------------------------------------------------------
// udp_mux datagram framing
// ---------------------------------------------------------------------------

#[test]
fn udp_mux_datagram_limit_is_literal() {
    assert_eq!(MAX_UDP_MUX_DATAGRAM_LEN, 65_535);
}

/// Writer side: the length prefix is a **big-endian** u16, checked at lengths
/// whose big- and little-endian encodings differ.
#[tokio::test]
async fn udp_mux_writer_prefixes_each_datagram_with_a_big_endian_u16() {
    let recorder = SharedRecorder::default();
    let mut writer = UdpMuxWriter::new(recorder.clone());

    writer.send(b"hello").await.unwrap();
    assert_eq!(recorder.bytes(), [0x00, 0x05, b'h', b'e', b'l', b'l', b'o']);
    recorder.0.lock().unwrap().clear();

    writer.send(&[0xAA; 256]).await.unwrap();
    let bytes = recorder.bytes();
    assert_eq!(&bytes[..2], [0x01, 0x00], "256 must be 0x0100 on the wire");
    assert_eq!(bytes.len(), 2 + 256);
    recorder.0.lock().unwrap().clear();

    // The largest representable prefix must stay exactly the u16 maximum.
    writer.send(&[0xBB; 65_535]).await.unwrap();
    let bytes = recorder.bytes();
    assert_eq!(&bytes[..2], [0xFF, 0xFF]);
    assert_eq!(bytes.len(), 2 + 65_535);
}

/// Reader side: the same framing, decoded from literal bytes.
#[tokio::test]
async fn udp_mux_reader_decodes_a_literal_prefix() {
    let mut reader = UdpMuxReader::new(&[0x00u8, 0x05, b'h', b'e', b'l', b'l', b'o'][..]);
    let mut buf = [0u8; 8];
    let n = reader.recv(&mut buf).await.unwrap();
    assert_eq!(n, 5);
    assert_eq!(&buf[..n], b"hello");

    // `[0x01, 0x00]` is 256 big-endian and 1 little-endian; only the
    // big-endian reading consumes the 256-byte payload that follows.
    let mut frame = vec![0x01u8, 0x00];
    frame.extend(std::iter::repeat_n(0xAAu8, 256));
    let mut reader = UdpMuxReader::new(&frame[..]);
    let mut buf = vec![0u8; 256];
    let n = reader.recv(&mut buf).await.unwrap();
    assert_eq!(n, 256, "0x0100 must be 256, not 1");
    assert_eq!(buf, vec![0xAAu8; 256]);
}

#[tokio::test]
async fn udp_mux_writer_accepts_the_limit_and_rejects_one_byte_more() {
    let recorder = SharedRecorder::default();
    let mut writer = UdpMuxWriter::new(recorder.clone());
    let payload = vec![0u8; MAX_UDP_MUX_DATAGRAM_LEN];
    assert_eq!(writer.send(&payload).await.unwrap(), payload.len());
    assert_eq!(recorder.bytes().len(), 2 + MAX_UDP_MUX_DATAGRAM_LEN);

    let mut writer = UdpMuxWriter::new(SharedRecorder::default());
    let error = writer
        .send(&vec![0u8; MAX_UDP_MUX_DATAGRAM_LEN + 1])
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}

// ---------------------------------------------------------------------------
// lane_hello
// ---------------------------------------------------------------------------

#[test]
fn lane_hello_constants_and_class_bytes_are_literal() {
    assert_eq!(HELLO_LEN, 33);
    assert_eq!(PAIRING_NONCE_LEN, 16);
    assert_eq!(GROUP_TOKEN_LEN, 16);
    assert_eq!(LaneClass::Interactive.hello_byte(), 0xD1);
    assert_eq!(LaneClass::Bulk.hello_byte(), 0xD2);
    assert!(matches!(
        LaneClass::from_hello_byte(0xD1),
        Some(LaneClass::Interactive)
    ));
    assert!(matches!(
        LaneClass::from_hello_byte(0xD2),
        Some(LaneClass::Bulk)
    ));
    for byte in 0x00u8..=0xFF {
        if byte != 0xD1 && byte != 0xD2 {
            assert!(
                LaneClass::from_hello_byte(byte).is_none(),
                "{byte:#04x} must not decode as a lane class"
            );
        }
    }
}

/// The hello frame is `[class byte][16-byte nonce][16-byte group]`.
#[tokio::test]
async fn lane_hello_frame_is_class_then_nonce_then_group() {
    let nonce = PairingNonce([0x11; PAIRING_NONCE_LEN]);
    let group = GroupToken([0x22; GROUP_TOKEN_LEN]);
    let mut written = Vec::new();
    write_lane_hello(&mut written, LaneClass::Interactive, nonce, group)
        .await
        .unwrap();

    let mut expected = Vec::with_capacity(HELLO_LEN);
    expected.push(0xD1);
    expected.extend_from_slice(&[0x11; 16]);
    expected.extend_from_slice(&[0x22; 16]);
    assert_eq!(written, expected);

    let (class, read_nonce, read_group) = read_lane_hello(&mut &expected[..]).await.unwrap();
    assert_eq!(class, LaneClass::Interactive);
    assert_eq!(read_nonce, nonce);
    assert_eq!(read_group, group);

    // Reordering the two 16-byte fields is self-consistent on both sides; the
    // literal bytes above are what pins their positions.
    let mut swapped = vec![0xD1];
    swapped.extend_from_slice(&[0x22; 16]);
    swapped.extend_from_slice(&[0x11; 16]);
    assert_ne!(swapped, expected);
}

// ---------------------------------------------------------------------------
// migration_wire::ResumeHeader
// ---------------------------------------------------------------------------

/// The resume header is `[magic u64 LE][logical_id u64 LE][generation u32
/// LE][flags u8]`.
///
/// The magic's *integer* value spells "MIGRATES"; written little-endian its
/// bytes read back as `"SETARGIM"`. That is intentional and matches the
/// reader, so the literal below is the wire spelling, not the mnemonic.
#[test]
fn resume_header_bytes_are_little_endian() {
    assert_eq!(RESUME_HEADER_LEN, 21);
    let header = ResumeHeader {
        logical_id: 0x0102_0304_0506_0708,
        generation: 0x090A_0B0C,
        is_final: true,
        is_response: false,
    };
    let encoded = header.encode();
    assert_eq!(encoded.len(), 21);
    assert_eq!(
        &encoded[0..8],
        [0x53, 0x45, 0x54, 0x41, 0x52, 0x47, 0x49, 0x4D]
    );
    assert_eq!(
        &encoded[8..16],
        [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]
    );
    assert_eq!(&encoded[16..20], [0x0C, 0x0B, 0x0A, 0x09]);
    assert_eq!(encoded[20], 0x01);

    let parsed = ResumeHeader::parse(&encoded).expect("literal header must parse");
    assert_eq!(parsed.logical_id, 0x0102_0304_0506_0708);
    assert_eq!(parsed.generation, 0x090A_0B0C);
    assert!(parsed.is_final);
    assert!(!parsed.is_response);
}

#[test]
fn resume_header_flag_bits_are_literal() {
    let base = ResumeHeader {
        logical_id: 1,
        generation: 0,
        is_final: false,
        is_response: false,
    };
    let flags_byte = |header: ResumeHeader| header.encode()[20];
    assert_eq!(flags_byte(base), 0x00);
    assert_eq!(
        flags_byte(ResumeHeader {
            is_final: true,
            ..base
        }),
        0x01
    );
    assert_eq!(
        flags_byte(ResumeHeader {
            is_response: true,
            ..base
        }),
        0x02
    );
    assert_eq!(
        flags_byte(ResumeHeader {
            is_final: true,
            is_response: true,
            ..base
        }),
        0x03
    );

    // Decode side: each literal flags byte maps back to the same two bits, and
    // any bit outside the pair is a protocol error.
    let literal = base.encode();
    for (flags, is_final, is_response) in [
        (0x00u8, false, false),
        (0x01, true, false),
        (0x02, false, true),
        (0x03, true, true),
    ] {
        let mut bytes = literal;
        bytes[20] = flags;
        let parsed = ResumeHeader::parse(&bytes).expect("known flag bits must parse");
        assert_eq!(parsed.is_final, is_final, "flags {flags:#04x}");
        assert_eq!(parsed.is_response, is_response, "flags {flags:#04x}");
    }
    for flags in [0x04u8, 0x08, 0x10, 0x80, 0xFE] {
        let mut bytes = literal;
        bytes[20] = flags;
        assert!(
            ResumeHeader::parse(&bytes).is_none(),
            "flags byte {flags:#04x} must be rejected"
        );
    }
}

// ---------------------------------------------------------------------------
// lane_message frame
// ---------------------------------------------------------------------------

#[test]
fn lane_message_limits_are_literal() {
    assert_eq!(DEFAULT_MAX_MESSAGE_LEN, 16 * 1024 * 1024);
}
