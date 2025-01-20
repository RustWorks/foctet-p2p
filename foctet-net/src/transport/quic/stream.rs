use crate::transport::stream::{FoctetRecvStream, FoctetSendStream, FoctetStream};
use crate::transport::stream::{RecvStream, SendStream};
use crate::protocol::TransportProtocol;
use anyhow::anyhow;
use anyhow::Result;
use foctet_core::error::StreamError;
use foctet_core::frame::{HandshakeData, OperationId};
use foctet_core::frame::{Frame, FrameType, Payload, StreamId};
use foctet_core::node::{SessionId, NodeId};
use futures::sink::SinkExt;
use quinn::{RecvStream as QuinnRecvStream, SendStream as QuinnSendStream, VarInt};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tokio::io::{AsyncRead, AsyncWrite};
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct QuicSendStream {
    pub framed_writer: FramedWrite<QuinnSendStream, LengthDelimitedCodec>,
    pub node_id: NodeId,
    pub stream_id: StreamId,
    pub session_id: SessionId,
    pub send_buffer_size: usize,
    pub is_closed: bool,
    pub is_relay: bool,
    pub next_operation_id: OperationId,
    pub remote_address: SocketAddr,
}

impl AsyncWrite for QuicSendStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().framed_writer.get_mut()).poll_write(cx, buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().framed_writer.get_mut()).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().framed_writer.get_mut()).poll_shutdown(cx)
    }
}

impl FoctetSendStream for QuicSendStream {
    fn session_id(&self) -> SessionId {
        self.session_id.clone()
    }
    fn stream_id(&self) -> StreamId {
        self.stream_id
    }
    fn operation_id(&self) -> OperationId {
        self.next_operation_id
    }
    async fn send_bytes(&mut self, bytes: bytes::Bytes) -> Result<usize> {
        let len = bytes.len();
        self.framed_writer.send(bytes).await?;
        self.framed_writer.flush().await?;
        Ok(len)
    }
    async fn send_data(&mut self, data: &[u8]) -> Result<OperationId> {
        let mut offset = 0;
        while offset < data.len() {
            let end = std::cmp::min(offset + self.send_buffer_size, data.len());
            let chunk = Payload::DataChunk(data[offset..end].to_vec());
            // Check if this is the last chunk
            let is_last_frame = end == data.len();
            let frame: Frame = Frame::builder()
                .with_fin(is_last_frame)
                .with_frame_type(FrameType::DataTransfer)
                .with_operation_id(self.next_operation_id)
                .with_payload(chunk)
                .build();
            let serialized_message = frame.to_bytes()?;
            self.framed_writer.send(serialized_message).await?;

            offset = end;
        }
        self.framed_writer.flush().await?;
        //framed_writer.get_mut().finish()?;
        let operation_id = self.operation_id();
        self.next_operation_id.increment();
        Ok(operation_id)
    }
    async fn send_frame(&mut self, frame: Frame) -> Result<OperationId> {
        let serialized_message = frame.to_bytes()?;
        self.framed_writer.send(serialized_message).await?;

        self.framed_writer.flush().await?;
        //framed_writer.get_mut().finish()?;
        let operation_id = self.operation_id();
        self.next_operation_id.increment();
        Ok(operation_id)
    }
    async fn send_file(&mut self, file_path: &std::path::Path) -> Result<OperationId> {
        let mut file = tokio::fs::File::open(file_path).await?;
        let mut buffer = vec![0u8; self.send_buffer_size];

        loop {
            let n = file.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            let chunk = Payload::FileChunk(buffer[..n].to_vec());
            let frame: Frame = Frame::builder()
                .with_fin(false)
                .with_frame_type(FrameType::FileTransfer)
                .with_operation_id(self.next_operation_id)
                .with_payload(chunk)
                .build();
            let serialized_message = frame.to_bytes()?;
            self.framed_writer.send(serialized_message).await?;
        }

        // Send the last frame with the FIN flag and NO payload
        let frame: Frame = Frame::builder()
            .with_fin(true)
            .with_frame_type(FrameType::FileTransfer)
            .with_operation_id(self.next_operation_id)
            .build();
        let serialized_message = frame.to_bytes()?;
        self.framed_writer.send(serialized_message).await?;

        self.framed_writer.flush().await?;
        //framed_writer.get_mut().finish()?;
        let operation_id = self.operation_id();
        self.next_operation_id.increment();
        Ok(operation_id)
    }

    async fn send_file_raw_bytes(&mut self, file_path: &std::path::Path) -> Result<()> {
        let mut file = tokio::fs::File::open(file_path).await?;
        let send_stream = self.framed_writer.get_mut();
        let mut buffer = vec![0u8; self.send_buffer_size];
    
        loop {
            let n = file.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            send_stream.write(&buffer[..n]).await?;
        }
        send_stream.flush().await?;
        send_stream.finish()?;
        Ok(())
    }
    async fn send_file_framed_bytes(&mut self, file_path: &std::path::Path) -> Result<()> {
        let mut file = tokio::fs::File::open(file_path).await?;
        let mut buffer = vec![0u8; self.send_buffer_size];
    
        loop {
            let n = file.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            self.framed_writer.send(bytes::Bytes::copy_from_slice(&buffer[..n])).await?;
        }

        // Send an empty byte array to indicate the end of the file
        self.framed_writer.send(bytes::Bytes::new()).await?;
    
        self.framed_writer.flush().await?;
        Ok(())
    }

    /// Send a specific range of a file, divided into chunks, to the receiver.
    /// This method is designed for parallel file transfers.
    async fn send_file_range(
        &mut self,
        file_path: &std::path::Path,
        offset: u64,
        length: u64,
    ) -> Result<OperationId> {
        let mut file = tokio::fs::File::open(file_path).await?;
        let mut buffer = vec![0u8; self.send_buffer_size];

        // Seek to the specified offset
        file.seek(tokio::io::SeekFrom::Start(offset)).await?;

        let mut remaining = length;
        while remaining > 0 {
            // Determine the size of the next chunk
            let read_size = std::cmp::min(remaining, self.send_buffer_size as u64) as usize;

            // Read the next chunk from the file
            let n = file.read(&mut buffer[..read_size]).await?;
            if n == 0 {
                break; // EOF
            }

            // Send the chunk to the receiver
            let chunk = Payload::FileChunk(buffer[..n].to_vec());
            let frame: Frame = Frame::builder()
                .with_fin(false)
                .with_frame_type(FrameType::FileTransfer)
                .with_operation_id(self.next_operation_id)
                .with_payload(chunk)
                .build();
            let serialized_message = frame.to_bytes()?;
            self.framed_writer.send(serialized_message).await?;

            remaining -= n as u64;
        }

        // Send the last frame with the FIN flag and NO payload
        let frame: Frame = Frame::builder()
            .with_fin(true)
            .with_frame_type(FrameType::FileTransfer)
            .with_operation_id(self.next_operation_id)
            .build();
        let serialized_message = frame.to_bytes()?;
        self.framed_writer.send(serialized_message).await?;

        self.framed_writer.flush().await?;
        let operation_id = self.operation_id();
        self.next_operation_id.increment();
        Ok(operation_id)
    }

    async fn close(&mut self) -> Result<()> {
        self.framed_writer.get_mut().finish()?;
        self.framed_writer.close().await?;
        self.is_closed = true;
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.is_closed
    }

    fn is_relay(&self) -> bool {
        self.is_relay
    }

    fn remote_address(&self) -> SocketAddr {
        self.remote_address
    }

    fn write_buffer_size(&self) -> usize {
        self.send_buffer_size
    }

    fn set_write_buffer_size(&mut self, size: usize) {
        self.send_buffer_size = size;
    }
}

#[derive(Debug)]
pub struct QuicRecvStream {
    pub framed_reader: FramedRead<QuinnRecvStream, LengthDelimitedCodec>,
    pub node_id: NodeId,
    pub stream_id: StreamId,
    pub session_id: SessionId,
    pub receive_buffer_size: usize,
    pub is_closed: bool,
    pub is_relay: bool,
    pub remote_address: SocketAddr,
}

impl FoctetRecvStream for QuicRecvStream {
    fn session_id(&self) -> SessionId {
        self.session_id.clone()
    }
    fn stream_id(&self) -> StreamId {
        self.stream_id
    }
    async fn receive_bytes(&mut self) -> Result<bytes::BytesMut> {
        let bytes = self.framed_reader.next().await;
        match bytes {
            Some(Ok(bytes)) => {
                Ok(bytes)
            }
            Some(Err(e)) => {
                Err(e.into())
            }
            None => {
                Err(StreamError::Closed.into())
            }
        }
    }
    async fn receive_data(&mut self, buffer: &mut Vec<u8>) -> Result<usize> {
        let mut total_bytes_read: usize = 0;
        while let Some(chunk) = self.framed_reader.next().await {
            match chunk {
                Ok(bytes) => {
                    let frame = Frame::from_bytes(&bytes)?;
                    if let Some(Payload::DataChunk(data)) = frame.payload {
                        buffer.extend_from_slice(&data);
                        total_bytes_read += data.len();
                    }
                    if frame.fin {
                        break;
                    }
                }
                Err(e) => {
                    //Safely cast the error to a quinn::ReadError
                    match e.downcast::<quinn::ReadError>() {
                        Ok(read_err) => match read_err {
                            quinn::ReadError::ClosedStream => {
                                tracing::info!("Stream closed by peer");
                            }
                            quinn::ReadError::ConnectionLost(_) => {
                                tracing::info!("Connection closed by peer");
                            }
                            _ => {
                                tracing::error!("Error reading from stream: {:?}", read_err);
                            }
                        },
                        Err(e) => {
                            tracing::error!("Error reading from stream: {:?}", e);
                        }
                    }
                    break;
                }
            }
        }
        Ok(total_bytes_read)
    }
    async fn receive_frame(&mut self) -> Result<Frame> {
        while let Some(chunk) = self.framed_reader.next().await {
            match chunk {
                Ok(bytes) => {
                    let frame = Frame::from_bytes(&bytes)?;
                    return Ok(frame);
                }
                Err(e) => {
                    //Safely cast the error to a quinn::ReadError
                    match e.downcast::<quinn::ReadError>() {
                        Ok(read_err) => match read_err {
                            quinn::ReadError::ClosedStream => {
                                tracing::info!("Stream closed by peer");
                            }
                            quinn::ReadError::ConnectionLost(_) => {
                                tracing::info!("Connection closed by peer");
                            }
                            _ => {
                                tracing::error!("Error reading from stream: {:?}", read_err);
                            }
                        },
                        Err(e) => {
                            tracing::error!("Error reading from stream: {:?}", e);
                        }
                    }
                    break;
                }
            }
        }
        Err(StreamError::Closed.into())
    }
    async fn receive_file(&mut self, file_path: &std::path::Path) -> Result<u64> {
        let mut total_bytes: u64 = 0;
        let mut file = tokio::fs::File::create(file_path).await?;
        while let Some(chunk) = self.framed_reader.next().await {
            match chunk {
                Ok(bytes) => {
                    let frame = Frame::from_bytes(&bytes)?;
                    if let Some(Payload::FileChunk(data)) = frame.payload {
                        file.write_all(&data).await?;
                        total_bytes += data.len() as u64;
                    }
                    if frame.fin {
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!("Error reading from stream: {:?}", e);
                    break;
                }
            }
        }
        file.flush().await?;
        Ok(total_bytes)
    }
    async fn receive_file_range(
        &mut self,
        file_path: &std::path::Path,
        offset: u64,
        _length: u64,
    ) -> Result<u64> {
        let mut total_bytes: u64 = 0;
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(file_path)
            .await?;
        file.seek(tokio::io::SeekFrom::Start(offset)).await?;
        while let Some(chunk) = self.framed_reader.next().await {
            match chunk {
                Ok(bytes) => {
                    let frame = Frame::from_bytes(&bytes)?;
                    if let Some(Payload::FileChunk(data)) = frame.payload {
                        file.write_all(&data).await?;
                        total_bytes += data.len() as u64;
                    }
                    if frame.fin {
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!("Error reading from stream: {:?}", e);
                    break;
                }
            }
        }
        file.flush().await?;
        Ok(total_bytes)
    }
    async fn receive_file_raw_bytes(&mut self, file_path: &std::path::Path) -> Result<u64> {
        let mut total_bytes: u64 = 0;
        let mut file = tokio::fs::File::create(file_path).await?;
        let recv_stream = self.framed_reader.get_mut();
        let mut buffer = vec![0u8; self.receive_buffer_size];
        loop {
            match recv_stream.read(&mut buffer).await {
                Ok(n) => {
                    match n {
                        Some(n) => {
                            if n == 0 {
                                break;
                            }
                            file.write_all(&buffer[..n]).await?;
                            total_bytes += n as u64;
                        }
                        None => {
                            break;
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Error reading from stream: {:?}", e);
                    break;
                }
            }
        }
        file.flush().await?;
        Ok(total_bytes)
    }
    async fn receive_file_framed_bytes(&mut self, file_path: &std::path::Path) -> Result<u64> {
        let mut total_bytes: u64 = 0;
        let mut file = tokio::fs::File::create(file_path).await?;
    
        while let Some(chunk) = self.framed_reader.next().await {
            match chunk {
                Ok(bytes) => {
                    if bytes.is_empty() {
                        break;
                    }
                    file.write_all(&bytes).await?;
                    total_bytes += bytes.len() as u64;
                }
                Err(e) => {
                    tracing::error!("Error reading from stream: {:?}", e);
                    break;
                }
            }
        }
        file.flush().await?;
        Ok(total_bytes)
    }

    async fn close(&mut self) -> Result<()> {
        self.framed_reader.get_mut().stop(VarInt::from_u32(0))?;
        self.is_closed = true;
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.is_closed
    }

    fn is_relay(&self) -> bool {
        self.is_relay
    }

    fn remote_address(&self) -> SocketAddr {
        self.remote_address
    }

    fn read_buffer_size(&self) -> usize {
        self.receive_buffer_size
    }

    fn set_read_buffer_size(&mut self, size: usize) {
        self.receive_buffer_size = size;
    }
}

#[derive(Debug)]
pub struct QuicStream {
    pub framed_writer: FramedWrite<QuinnSendStream, LengthDelimitedCodec>,
    pub framed_reader: FramedRead<QuinnRecvStream, LengthDelimitedCodec>,
    pub node_id: NodeId,
    pub stream_id: StreamId,
    pub session_id: SessionId,
    pub send_buffer_size: usize,
    pub receive_buffer_size: usize,
    pub established: bool,
    pub is_relay: bool,
    pub is_closed: bool,
    pub next_operation_id: OperationId,
    pub remote_address: SocketAddr,
}

impl AsyncRead for QuicStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().framed_reader.get_mut()).poll_read(cx, buf)
    }
}

impl AsyncWrite for QuicStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().framed_writer.get_mut()).poll_write(cx, buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().framed_writer.get_mut()).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().framed_writer.get_mut()).poll_shutdown(cx)
    }
}

impl FoctetStream for QuicStream {
    fn session_id(&self) -> SessionId {
        self.session_id.clone()
    }
    fn stream_id(&self) -> StreamId {
        self.stream_id
    }
    fn operation_id(&self) -> OperationId {
        self.next_operation_id
    }
    async fn handshake(&mut self, dst_node_id: NodeId, data: Option<Vec<u8>>) -> Result<()> {
        // Send a handshake frame to the peer
        let frame: Frame = Frame::builder()
            .with_fin(true)
            .with_frame_type(FrameType::Connect)
            .with_operation_id(self.next_operation_id)
            .with_payload(Payload::handshake(HandshakeData::new(self.node_id.clone(), dst_node_id, data)))
            .build();
        self.send_frame(frame).await?;
        // Receive a handshake frame from the peer
        // Wait for the `Connected` frame from the peer
        let frame = self.receive_frame().await?;
        if frame.frame_type == FrameType::Connected {
            self.established = true;
            Ok(())
        } else {
            Err(anyhow!("Failed to establish connection"))
        }
    }
    async fn send_bytes(&mut self, bytes: bytes::Bytes) -> Result<usize> {
        let len = bytes.len();
        self.framed_writer.send(bytes).await?;
        self.framed_writer.flush().await?;
        Ok(len)
    }
    async fn receive_bytes(&mut self) -> Result<bytes::BytesMut> {
        let bytes = self.framed_reader.next().await;
        match bytes {
            Some(Ok(bytes)) => {
                Ok(bytes)
            }
            Some(Err(e)) => {
                Err(e.into())
            }
            None => {
                Err(StreamError::Closed.into())
            }
        }
    }
    async fn send_data(&mut self, data: &[u8]) -> Result<OperationId> {
        let mut offset = 0;
        while offset < data.len() {
            let end = std::cmp::min(offset + self.send_buffer_size, data.len());
            let chunk = Payload::DataChunk(data[offset..end].to_vec());
            // Check if this is the last chunk
            let is_last_frame = end == data.len();
            let frame: Frame = Frame::builder()
                .with_fin(is_last_frame)
                .with_frame_type(FrameType::DataTransfer)
                .with_operation_id(self.next_operation_id)
                .with_payload(chunk)
                .build();
            let serialized_message = frame.to_bytes()?;
            self.framed_writer.send(serialized_message).await?;

            offset = end;
        }
        self.framed_writer.flush().await?;
        //framed_writer.get_mut().finish()?;
        let operation_id = self.operation_id();
        self.next_operation_id.increment();
        Ok(operation_id)
    }

    async fn receive_data(&mut self, buffer: &mut Vec<u8>) -> Result<usize> {
        let mut total_bytes_read: usize = 0;
        while let Some(chunk) = self.framed_reader.next().await {
            match chunk {
                Ok(bytes) => {
                    let frame = Frame::from_bytes(&bytes)?;
                    if let Some(Payload::DataChunk(data)) = frame.payload {
                        buffer.extend_from_slice(&data);
                        total_bytes_read += data.len();
                    }
                    if frame.fin {
                        break;
                    }
                }
                Err(e) => {
                    //Safely cast the error to a quinn::ReadError
                    match e.downcast::<quinn::ReadError>() {
                        Ok(read_err) => match read_err {
                            quinn::ReadError::ClosedStream => {
                                tracing::info!("Stream closed by peer");
                            }
                            quinn::ReadError::ConnectionLost(_) => {
                                tracing::info!("Connection closed by peer");
                            }
                            _ => {
                                tracing::error!("Error reading from stream: {:?}", read_err);
                            }
                        },
                        Err(e) => {
                            tracing::error!("Error reading from stream: {:?}", e);
                        }
                    }
                    break;
                }
            }
        }
        Ok(total_bytes_read)
    }

    async fn send_frame(&mut self, frame: Frame) -> Result<OperationId> {
        let serialized_message = frame.to_bytes()?;
        self.framed_writer.send(serialized_message).await?;

        self.framed_writer.flush().await?;
        //framed_writer.get_mut().finish()?;
        let operation_id = self.operation_id();
        self.next_operation_id.increment();
        Ok(operation_id)
    }

    async fn receive_frame(&mut self) -> Result<Frame> {
        while let Some(chunk) = self.framed_reader.next().await {
            match chunk {
                Ok(bytes) => {
                    let frame = Frame::from_bytes(&bytes)?;
                    return Ok(frame);
                }
                Err(e) => {
                    //Safely cast the error to a quinn::ReadError
                    match e.downcast::<quinn::ReadError>() {
                        Ok(read_err) => match read_err {
                            quinn::ReadError::ClosedStream => {
                                tracing::info!("Stream closed by peer");
                            }
                            quinn::ReadError::ConnectionLost(_) => {
                                tracing::info!("Connection closed by peer");
                            }
                            _ => {
                                tracing::error!("Error reading from stream: {:?}", read_err);
                            }
                        },
                        Err(e) => {
                            tracing::error!("Error reading from stream: {:?}", e);
                        }
                    }
                    break;
                }
            }
        }
        Err(StreamError::Closed.into())
    }

    async fn send_file(&mut self, file_path: &std::path::Path) -> Result<OperationId> {
        let mut file = tokio::fs::File::open(file_path).await?;
        let mut buffer = vec![0u8; self.send_buffer_size];

        loop {
            let n = file.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            let chunk = Payload::FileChunk(buffer[..n].to_vec());
            let frame: Frame = Frame::builder()
                .with_fin(false)
                .with_frame_type(FrameType::FileTransfer)
                .with_operation_id(self.next_operation_id)
                .with_payload(chunk)
                .build();
            let serialized_message = frame.to_bytes()?;
            self.framed_writer.send(serialized_message).await?;
        }

        // Send the last frame with the FIN flag and NO payload
        let frame: Frame = Frame::builder()
            .with_fin(true)
            .with_frame_type(FrameType::FileTransfer)
            .with_operation_id(self.next_operation_id)
            .build();
        let serialized_message = frame.to_bytes()?;
        self.framed_writer.send(serialized_message).await?;

        self.framed_writer.flush().await?;
        //framed_writer.get_mut().finish()?;
        let operation_id = self.operation_id();
        self.next_operation_id.increment();
        Ok(operation_id)
    }
    async fn receive_file(&mut self, file_path: &std::path::Path) -> Result<u64> {
        let mut total_bytes: u64 = 0;
        let mut file = tokio::fs::File::create(file_path).await?;
        while let Some(chunk) = self.framed_reader.next().await {
            match chunk {
                Ok(bytes) => {
                    let frame = Frame::from_bytes(&bytes)?;
                    if let Some(Payload::FileChunk(data)) = frame.payload {
                        file.write_all(&data).await?;
                        total_bytes += data.len() as u64;
                    }
                    if frame.fin {
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!("Error reading from stream: {:?}", e);
                    break;
                }
            }
        }
        file.flush().await?;
        Ok(total_bytes)
    }
    /// Send a specific range of a file, divided into chunks, to the receiver.
    /// This method is designed for parallel file transfers.
    async fn send_file_range(
        &mut self,
        file_path: &std::path::Path,
        offset: u64,
        length: u64,
    ) -> Result<OperationId> {
        let mut file = tokio::fs::File::open(file_path).await?;
        let mut buffer = vec![0u8; self.send_buffer_size];

        // Seek to the specified offset
        file.seek(tokio::io::SeekFrom::Start(offset)).await?;

        let mut remaining = length;
        while remaining > 0 {
            // Determine the size of the next chunk
            let read_size = std::cmp::min(remaining, self.send_buffer_size as u64) as usize;

            // Read the next chunk from the file
            let n = file.read(&mut buffer[..read_size]).await?;
            if n == 0 {
                break; // EOF
            }

            // Send the chunk to the receiver
            let chunk = Payload::FileChunk(buffer[..n].to_vec());
            let frame: Frame = Frame::builder()
                .with_fin(false)
                .with_frame_type(FrameType::FileTransfer)
                .with_operation_id(self.next_operation_id)
                .with_payload(chunk)
                .build();
            let serialized_message = frame.to_bytes()?;
            self.framed_writer.send(serialized_message).await?;

            remaining -= n as u64;
        }

        // Send the last frame with the FIN flag and NO payload
        let frame: Frame = Frame::builder()
            .with_fin(true)
            .with_frame_type(FrameType::FileTransfer)
            .with_operation_id(self.next_operation_id)
            .build();
        let serialized_message = frame.to_bytes()?;
        self.framed_writer.send(serialized_message).await?;

        self.framed_writer.flush().await?;
        let operation_id = self.operation_id();
        self.next_operation_id.increment();
        Ok(operation_id)
    }
    async fn receive_file_range(
        &mut self,
        file_path: &std::path::Path,
        offset: u64,
        _length: u64,
    ) -> Result<u64> {
        let mut total_bytes: u64 = 0;
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(file_path)
            .await?;
        file.seek(tokio::io::SeekFrom::Start(offset)).await?;
        while let Some(chunk) = self.framed_reader.next().await {
            match chunk {
                Ok(bytes) => {
                    let frame = Frame::from_bytes(&bytes)?;
                    if let Some(Payload::FileChunk(data)) = frame.payload {
                        file.write_all(&data).await?;
                        total_bytes += data.len() as u64;
                    }
                    if frame.fin {
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!("Error reading from stream: {:?}", e);
                    break;
                }
            }
        }
        file.flush().await?;
        Ok(total_bytes)
    }
    async fn send_file_raw_bytes(&mut self, file_path: &std::path::Path) -> Result<()> {
        let mut file = tokio::fs::File::open(file_path).await?;
        let send_stream = self.framed_writer.get_mut();
        let mut buffer = vec![0u8; self.send_buffer_size];
    
        loop {
            let n = file.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            send_stream.write(&buffer[..n]).await?;
        }
        send_stream.flush().await?;
        send_stream.finish()?;
        Ok(())
    }
    async fn send_file_framed_bytes(&mut self, file_path: &std::path::Path) -> Result<()> {
        let mut file = tokio::fs::File::open(file_path).await?;
        let mut buffer = vec![0u8; self.send_buffer_size];
    
        loop {
            let n = file.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            self.framed_writer.send(bytes::Bytes::copy_from_slice(&buffer[..n])).await?;
        }

        // Send an empty byte array to indicate the end of the file
        self.framed_writer.send(bytes::Bytes::new()).await?;
    
        self.framed_writer.flush().await?;
        Ok(())
    }
    async fn receive_file_raw_bytes(&mut self, file_path: &std::path::Path) -> Result<u64> {
        let mut total_bytes: u64 = 0;
        let mut file = tokio::fs::File::create(file_path).await?;
        let recv_stream = self.framed_reader.get_mut();
        let mut buffer = vec![0u8; self.receive_buffer_size];
        loop {
            match recv_stream.read(&mut buffer).await {
                Ok(n) => {
                    match n {
                        Some(n) => {
                            if n == 0 {
                                break;
                            }
                            file.write_all(&buffer[..n]).await?;
                            total_bytes += n as u64;
                        }
                        None => {
                            break;
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Error reading from stream: {:?}", e);
                    break;
                }
            }
        }
        file.flush().await?;
        Ok(total_bytes)
    }
    async fn receive_file_framed_bytes(&mut self, file_path: &std::path::Path) -> Result<u64> {
        let mut total_bytes: u64 = 0;
        let mut file = tokio::fs::File::create(file_path).await?;
    
        while let Some(chunk) = self.framed_reader.next().await {
            match chunk {
                Ok(bytes) => {
                    if bytes.is_empty() {
                        break;
                    }
                    file.write_all(&bytes).await?;
                    total_bytes += bytes.len() as u64;
                }
                Err(e) => {
                    tracing::error!("Error reading from stream: {:?}", e);
                    break;
                }
            }
        }
        file.flush().await?;
        Ok(total_bytes)
    }

    async fn close(&mut self) -> Result<()> {
        self.framed_writer.flush().await?;
        self.framed_writer.get_mut().finish()?;
        self.framed_reader.get_mut().stop(VarInt::from_u32(0))?;
        self.framed_writer.close().await?;
        self.is_closed = true;
        Ok(())
    }

    fn established(&self) -> bool {
        self.established
    }

    fn is_closed(&self) -> bool {
        self.is_closed
    }

    fn is_relay(&self) -> bool {
        self.is_relay
    }

    fn remote_address(&self) -> SocketAddr {
        self.remote_address
    }

    fn transport_protocol(&self) -> TransportProtocol {
        TransportProtocol::Quic
    }

    fn write_buffer_size(&self) -> usize {
        self.send_buffer_size
    }

    fn set_write_buffer_size(&mut self, size: usize) {
        self.send_buffer_size = size;
    }

    fn read_buffer_size(&self) -> usize {
        self.receive_buffer_size
    }

    fn set_read_buffer_size(&mut self, size: usize) {
        self.receive_buffer_size = size;
    }

    fn split(self) -> (SendStream, RecvStream) {
        let quic_send_stream = QuicSendStream {
            framed_writer: self.framed_writer,
            node_id: self.node_id.clone(),
            stream_id: self.stream_id,
            session_id: self.session_id.clone(),
            send_buffer_size: self.send_buffer_size,
            is_closed: self.is_closed,
            is_relay: self.is_relay,
            next_operation_id: self.next_operation_id,
            remote_address: self.remote_address,
        };
        let quic_recv_stream = QuicRecvStream {
            framed_reader: self.framed_reader,
            node_id: self.node_id.clone(),
            stream_id: self.stream_id,
            session_id: self.session_id.clone(),
            receive_buffer_size: self.receive_buffer_size,
            is_closed: self.is_closed,
            is_relay: self.is_relay,
            remote_address: self.remote_address,
        };
        let send_stream = SendStream::Quic(quic_send_stream);
        let recv_stream = RecvStream::Quic(quic_recv_stream);
        (send_stream, recv_stream)
    }

    fn merge(send_stream: SendStream, recv_stream: RecvStream) -> Result<Self> where Self: Sized {
        match (send_stream, recv_stream) {
            (SendStream::Quic(quic_send_stream), RecvStream::Quic(quic_recv_stream)) => {
                Ok(Self {
                    framed_writer: quic_send_stream.framed_writer,
                    framed_reader: quic_recv_stream.framed_reader,
                    node_id: quic_send_stream.node_id,
                    stream_id: quic_send_stream.stream_id,
                    session_id: quic_send_stream.session_id,
                    send_buffer_size: quic_send_stream.send_buffer_size,
                    receive_buffer_size: quic_recv_stream.receive_buffer_size,
                    established: true,
                    is_closed: quic_send_stream.is_closed,
                    is_relay: quic_send_stream.is_relay,
                    next_operation_id: quic_send_stream.next_operation_id,
                    remote_address: quic_send_stream.remote_address,
                })
            }
            _ => {
                Err(anyhow!("Mismatched stream types"))
            }
        }
    }
}
