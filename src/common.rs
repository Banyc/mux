#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Read,
    Write,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum End {
    Local,
    Peer,
}
