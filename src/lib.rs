#![cfg_attr(feature = "nightly", feature(test))]
#[cfg(feature = "nightly")]
extern crate test;

#[cfg(feature = "nightly")]
mod bench;
mod central_io;
mod common;
mod control;
mod dual_lane;
mod dual_message;
#[allow(unused)]
mod fair_queue;
mod migrating;
mod protocol;
mod serve;
mod stream;
mod stream_migration;
mod traffic_class;

pub use central_io::DeadCentralIo;
pub use common::Side;
pub use control::{ControlOpenError, DeadControl, Initiation, TooManyOpenStreams};
pub use control::{REASSEMBLY_MAX_BUFFERED_BYTES, REASSEMBLY_MAX_RANGE_BYTES};
pub use dual_lane::{
    AUTO_BULK_THRESHOLD, AutoReader, AutoWriteError, AutoWriter, DualAcceptError, DualMuxError,
    DualStreamAccepter, DualStreamOpenError, DualStreamOpener, GroupToken, LaneClass,
    LaneHelloError, PairingNonce, PendingAcceptor, complete_pairing, read_lane_hello,
    spawn_dual_mux_acceptor, spawn_dual_mux_connector, spawn_dual_mux_paired_supervised,
    write_birth_heartbeat, write_lane_hello,
};
pub use dual_message::{
    DEFAULT_MAX_INFLIGHT_MESSAGES, DEFAULT_MAX_MESSAGE_LEN, DEFAULT_REORDER_CAP, DeliveryMode,
    DualMessageReceiver, DualMessageSender, RecvError, SendError,
};
pub use migrating::{
    AUTO_BULK_THRESHOLD as MIGRATING_AUTO_BULK_THRESHOLD, AcceptedStream, ClientSplicedReader,
    MigratingCapableAccepter, MigratingError, MigratingStreamWriter, ResponseRouter,
    ResponseRouterHandle, SpliceFeed, SpliceFeedHandle, StreamName, spawn_response_router,
    spawn_splice_feed,
};
pub use protocol::Offset;
pub use serve::{
    MuxConfig, MuxError, spawn_mux_no_reconnection,
    spawn_mux_no_reconnection_with_first_receive_deadline,
    spawn_mux_no_reconnection_with_first_receive_deadline_and_ready, spawn_mux_with_reconnection,
};
pub use stream::{
    accepter::StreamAccepter,
    opener::{StreamOpenError, StreamOpener},
    reader::StreamReader,
    writer::StreamWriter,
};
pub use stream_migration::{
    DEFAULT_SUCCESSOR_DEADLINE, GenerationChain, MAX_PENDING_GENERATIONS, MigrationError,
    RESUME_HEADER_LEN, ResumeHeader, SpliceRegistry, SplicedReader,
};
