#![cfg_attr(feature = "nightly", feature(test))]
#![warn(clippy::disallowed_methods, clippy::disallowed_types)]
#[cfg(feature = "nightly")]
extern crate test;

#[cfg(feature = "nightly")]
#[allow(clippy::disallowed_methods)]
mod bench;
mod central_io;
mod control;
mod dual_lane;
#[expect(unused)]
mod fair_queue;
mod lane_hello;
mod lane_message;
mod migration_api;
mod migration_wire;
mod protocol;
mod reassembly;
mod session;
mod splice_feed;
mod stream;
mod traffic_class;

pub use central_io::DeadCentralIo;
pub use control::{ControlOpenError, DeadControl, Initiation, TooManyOpenStreams};
pub use dual_lane::{
    AUTO_BULK_THRESHOLD, AutoLaneReader, AutoLaneWriter, AutoWriteError, DualAcceptError,
    DualMuxError, DualStreamAccepter, DualStreamOpenError, DualStreamOpener, UnpairedLane,
    begin_lane_pairing, complete_pairing, spawn_dual_mux_connector,
    spawn_dual_mux_paired_supervised, write_liveness_heartbeat,
};
pub use lane_hello::{GroupToken, LaneHelloError, PairingNonce, read_lane_hello, write_lane_hello};
pub use lane_message::{
    DEFAULT_MAX_INFLIGHT_MESSAGES, DEFAULT_MAX_MESSAGE_LEN, DEFAULT_REORDER_CAP, DeliveryMode,
    DualMessageReceiver, DualMessageSender, MessageSendError, RecvError,
};
pub use migration_api::{
    AUTO_BULK_THRESHOLD as MIGRATING_AUTO_BULK_THRESHOLD, AcceptedStream, MigratingCapableAccepter,
    MigratingStreamError, MigratingStreamWriter, PendingResponseReader, ResponseRouter,
    ResponseRouterDriver, ResponseRouterHandle, StreamName, spawn_response_router,
};
pub use migration_wire::{
    DEFAULT_SUCCESSOR_DEADLINE, GenerationChain, MAX_PENDING_GENERATIONS, MigrationError,
    RESUME_HEADER_LEN, ResumeHeader, SpliceRegistry, SplicedReader,
};
pub use protocol::{Offset, Side};
pub use reassembly::{REASSEMBLY_MAX_BUFFERED_BYTES, REASSEMBLY_MAX_RANGE_BYTES};
pub use session::{
    MuxConfig, MuxError, spawn_mux_no_reconnection,
    spawn_mux_no_reconnection_with_first_receive_deadline,
    spawn_mux_no_reconnection_with_first_receive_deadline_and_ready, spawn_mux_with_reconnection,
};
pub use splice_feed::{
    SpliceFeedError, SpliceRouter, SpliceRouterHandle, SpliceTaskExit, spawn_splice_router,
};
pub use stream::{
    accepter::StreamAccepter,
    opener::{StreamOpenError, StreamOpener},
    reader::StreamReader,
    writer::StreamWriter,
};
pub use traffic_class::LaneClass;
