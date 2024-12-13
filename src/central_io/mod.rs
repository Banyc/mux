use primitive::arena::obj_pool::ObjScoped;

pub mod reader;

pub type DataBuf = ObjScoped<Vec<u8>>;
