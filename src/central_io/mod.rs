use primitive::arena::obj_pool::ObjScoped;

pub mod reader;
pub mod writer;

pub type DataBuf = ObjScoped<Vec<u8>>;
