#![cfg_attr(feature = "nightly", feature(test))]
#[cfg(feature = "nightly")]
extern crate test;

#[cfg(feature = "nightly")]
mod bench;
mod central_io;
mod common;
mod control;
mod fair_queue;
mod protocol;
mod serve;
mod stream;

pub use async_async_io;
pub use control::{DeadControl, Initiation, TooManyOpenStreams};
pub use serve::{spawn_mux_no_reconnection, spawn_mux_with_reconnection, MuxConfig, MuxError};
pub use stream::{
    accepter::StreamAccepter,
    opener::{StreamOpenError, StreamOpener},
    reader::StreamReader,
    writer::StreamWriter,
};
