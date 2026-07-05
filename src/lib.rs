#![cfg_attr(feature = "nightly", feature(test))]
#[cfg(feature = "nightly")]
extern crate test;

#[cfg(feature = "nightly")]
mod bench;
mod central_io;
mod common;
mod control;
mod dual_lane;
#[allow(unused)]
mod fair_queue;
mod protocol;
mod serve;
mod stream;

pub use central_io::DeadCentralIo;
pub use common::Side;
pub use control::{ControlOpenError, DeadControl, Initiation, TooManyOpenStreams};
pub use dual_lane::{
    complete_pairing, read_lane_hello, spawn_dual_mux_acceptor, spawn_dual_mux_connector,
    spawn_dual_mux_paired, write_lane_hello, AutoReader, AutoWriteError, AutoWriter,
    DualAcceptError, DualMuxError, DualStreamAccepter, DualStreamOpenError, DualStreamOpener,
    LaneClass, LaneHelloError, PendingAcceptor, AUTO_BULK_THRESHOLD,
};
pub use serve::{spawn_mux_no_reconnection, spawn_mux_with_reconnection, MuxConfig, MuxError};
pub use stream::{
    accepter::StreamAccepter,
    opener::{StreamOpenError, StreamOpener},
    reader::StreamReader,
    writer::StreamWriter,
};
