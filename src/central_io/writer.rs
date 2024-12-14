use std::{io, time::Duration};

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{
    control::{DeadControl, WriteControlRx, WriteDataRx},
    protocol::{DataHeader, Header, StreamId, StreamIdMsg},
};

use super::DataBuf;

pub async fn run_central_io_writer<W>(
    mut io_writer: CentralIoWriter<W>,
    heartbeat_interval: Duration,
    mut control: WriteControlRx,
    mut data: WriteDataRx,
) -> Result<(), RunCentralIoWriterError>
where
    W: AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            () = tokio::time::sleep(heartbeat_interval) => {
                io_writer.send_heartbeat().await.map_err(RunCentralIoWriterError::IoWriter)?;
            }
            res = control.recv() => {
                let msg = res.map_err(RunCentralIoWriterError::Control)?;
                io_writer.send_control(msg).await.map_err(RunCentralIoWriterError::IoWriter)?;
            }
            res = data.recv() => {
                let msg = res.map_err(RunCentralIoWriterError::Control)?;
                io_writer.send_data(msg).await.map_err(RunCentralIoWriterError::IoWriter)?;
            }
        }
    }
}
#[derive(Debug)]
pub enum RunCentralIoWriterError {
    IoWriter(io::Error),
    Control(DeadControl),
}

#[derive(Debug)]
pub struct CentralIoWriter<W> {
    io_writer: W,
}
impl<W> CentralIoWriter<W> {
    pub fn new(io_writer: W) -> Self {
        Self { io_writer }
    }
}
impl<W> CentralIoWriter<W>
where
    W: AsyncWrite + Unpin,
{
    pub async fn send_heartbeat(&mut self) -> io::Result<()> {
        let hdr = Header::Heartbeat;
        let hdr = hdr.encode();
        self.io_writer.write_all(&hdr).await?;
        Ok(())
    }
    pub async fn send_control(&mut self, msg: WriteControlMsg) -> io::Result<()> {
        let hdr = match &msg {
            WriteControlMsg::Open(_) => Header::Open,
            WriteControlMsg::CloseRead(_) => Header::CloseRead,
            WriteControlMsg::CloseWrite(_) => Header::CloseWrite,
        };
        let stream_id = match msg {
            WriteControlMsg::Open(stream_id)
            | WriteControlMsg::CloseRead(stream_id)
            | WriteControlMsg::CloseWrite(stream_id) => stream_id,
        };
        let stream_id_msg = StreamIdMsg { stream_id };
        let hdr = hdr.encode();
        let stream_id_msg = stream_id_msg.encode();
        let mut concat = hdr.into_iter().chain(stream_id_msg);
        let buf: [u8; Header::SIZE + StreamIdMsg::SIZE] =
            core::array::from_fn(|_| concat.next().unwrap());
        self.io_writer.write_all(&buf).await?;
        Ok(())
    }
    pub async fn send_data(&mut self, msg: WriteDataMsg) -> io::Result<()> {
        let hdr = Header::Data;
        let mut remaining_body_len = msg.data.len();
        while remaining_body_len != 0 {
            let body_len = remaining_body_len.min(usize::try_from(u32::MAX).unwrap());
            remaining_body_len -= body_len;
            let body_len = u32::try_from(body_len).unwrap();
            let data_hdr = DataHeader {
                stream_id: msg.stream_id,
                body_len,
            };
            let hdr = hdr.encode();
            let data_hdr = data_hdr.encode();
            let mut concat = hdr.into_iter().chain(data_hdr);
            let fixed_buf: [u8; Header::SIZE + DataHeader::SIZE] =
                core::array::from_fn(|_| concat.next().unwrap());
            self.io_writer.write_all(&fixed_buf).await?;
            self.io_writer.write_all(&msg.data).await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum WriteControlMsg {
    Open(StreamId),
    CloseRead(StreamId),
    CloseWrite(StreamId),
}

#[derive(Debug)]
pub struct WriteDataMsg {
    pub stream_id: StreamId,
    pub data: DataBuf,
}
