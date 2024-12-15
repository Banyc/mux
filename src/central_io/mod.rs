use primitive::arena::obj_pool::ObjScoped;

use crate::common::Side;

pub mod reader;
pub mod writer;

pub type DataBuf = ObjScoped<Vec<u8>>;

#[derive(Debug, Clone)]
pub struct DeadCentralIo {
    pub side: Side,
}
