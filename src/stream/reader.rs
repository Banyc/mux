use crate::{
    central_io::DataBuf,
    control::{DeadControl, StreamCloseTx, StreamReadDataMsg, StreamReadDataRx},
};

#[derive(Debug)]
pub struct StreamReader {
    data: StreamReadDataRx,
    leftover: Option<(DataBuf, usize)>,
    is_eof: bool,
    _close: StreamCloseTx,
}
impl StreamReader {
    pub(crate) fn new(data: StreamReadDataRx, close: StreamCloseTx) -> Self {
        Self {
            data,
            leftover: None,
            is_eof: false,
            _close: close,
        }
    }
    pub async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, RecvError> {
        if self.is_eof {
            return Err(RecvError::EarlyEof);
        }
        let (data_buf, pos) = match self.leftover.take() {
            Some(x) => x,
            None => {
                let msg = self.data.recv().await.map_err(RecvError::DeadControl)?;
                let data_buf = match msg {
                    StreamReadDataMsg::Fin => {
                        self.is_eof = true;
                        return Err(RecvError::EarlyEof);
                    }
                    StreamReadDataMsg::Data(data_buf) => data_buf,
                };
                (data_buf, 0)
            }
        };
        let data = &data_buf[pos..];
        let data_len = buf.len().min(data.len());
        buf[..data_len].copy_from_slice(&data[..data_len]);
        let pos = pos + data_len;
        if pos < data_buf.len() {
            self.leftover = Some((data_buf, pos));
        }
        Ok(data_len)
    }
}
#[derive(Debug)]
pub enum RecvError {
    EarlyEof,
    DeadControl(DeadControl),
}
