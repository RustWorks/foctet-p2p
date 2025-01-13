use anyhow::Result;
use bytes::{Bytes, BytesMut};
use foctet_core::{
    frame::{Frame, OperationId, StreamId},
    node::{SessionId, NodeId},
};
use super::quic::stream::{QuicRecvStream, QuicSendStream, QuicStream};
use std::{net::SocketAddr, path::Path};
use super::tcp::stream::{TlsTcpStream, TlsTcpSendStream, TlsTcpRecvStream};

use crate::protocol::TransportProtocol;

#[allow(async_fn_in_trait)]
pub trait FoctetStream {
    // Returns the Session ID
    fn session_id(&self) -> SessionId;

    /// Returns the Stream ID
    fn stream_id(&self) -> StreamId;

    /// Returns the current operation ID.
    fn operation_id(&self) -> OperationId;

    /// Handshake with the remote node
    async fn handshake(&mut self, dst_node_id: NodeId, data: Option<Vec<u8>>) -> Result<()>;

    /// Sends bytes over the stream
    async fn send_bytes(&mut self, bytes: Bytes) -> Result<usize>;

    /// Receives bytes from the stream
    async fn receive_bytes(&mut self) -> Result<BytesMut>;

    /// Sends data over the stream
    async fn send_data(&mut self, data: &[u8]) -> Result<OperationId>;

    /// Receives data from the stream
    async fn receive_data(&mut self, buffer: &mut Vec<u8>) -> Result<usize>;

    /// Send a frame over the stream
    async fn send_frame(&mut self, frame: Frame) -> Result<OperationId>;

    /// Receive a frame over the stream
    async fn receive_frame(&mut self) -> Result<Frame>;

    /// Send a file over the stream
    async fn send_file(&mut self, file_path: &Path) -> Result<OperationId>;

    /// Receive a file over the stream
    async fn receive_file(&mut self, file_path: &Path) -> Result<u64>;

    /// Send a file over the stream in framed bytes
    async fn send_file_framed_bytes(&mut self, file_path: &std::path::Path) -> Result<()>;

    /// Receive a file over the stream in framed bytes
    async fn receive_file_framed_bytes(&mut self, file_path: &std::path::Path) -> Result<u64>;

    /// Send a file over the stream in raw bytes
    async fn send_file_raw_bytes(&mut self, file_path: &Path) -> Result<()>;

    /// Receive a file over the stream in raw bytes
    async fn receive_file_raw_bytes(&mut self, file_path: &Path) -> Result<u64>;

    /// Gracefully closes the stream.
    async fn close(&mut self) -> Result<()>;

    /// Returns the current state of the stream.
    fn established(&self) -> bool;

    /// Returns the current state of the stream.
    fn is_closed(&self) -> bool;

    /// Return whether the stream is a relay stream.
    fn is_relay(&self) -> bool;

    /// Returns the remote address of the connection.
    fn remote_address(&self) -> SocketAddr;

    /// Returns the transport protocol used by the connection.
    fn transport_protocol(&self) -> TransportProtocol;

    /// Return write buffer size
    fn write_buffer_size(&self) -> usize;

    /// Sets the write buffer size.
    fn set_write_buffer_size(&mut self, size: usize);

    /// Return read buffer size
    fn read_buffer_size(&self) -> usize;

    /// Sets the read buffer size.
    fn set_read_buffer_size(&mut self, size: usize);

    /// Splits the stream into send and receive streams.
    fn split(self) -> (SendStream, RecvStream);

    /// Merges a `SendStream` and a `RecvStream` back into a `NetworkStream`.
    fn merge(send_stream: SendStream, recv_stream: RecvStream) -> Result<Self> where Self: Sized;

}

#[allow(async_fn_in_trait)]
pub trait FoctetSendStream {
    // Returns the Connection ID
    fn session_id(&self) -> SessionId;

    /// Returns the Stream ID
    fn stream_id(&self) -> StreamId;

    /// Returns the current operation ID.
    fn operation_id(&self) -> OperationId;

    /// Sends bytes over the stream
    async fn send_bytes(&mut self, bytes: Bytes) -> Result<usize>;

    /// Sends data over the stream
    async fn send_data(&mut self, data: &[u8]) -> Result<OperationId>;

    /// Send a frame over the stream
    async fn send_frame(&mut self, frame: Frame) -> Result<OperationId>;

    /// Send a file over the stream
    async fn send_file(&mut self, file_path: &Path) -> Result<OperationId>;

    /// Send a file over the stream in framed bytes
    async fn send_file_framed_bytes(&mut self, file_path: &std::path::Path) -> Result<()>;

    /// Send a file over the stream in raw bytes
    async fn send_file_raw_bytes(&mut self, file_path: &Path) -> Result<()>;

    /// Gracefully closes the stream.
    async fn close(&mut self) -> Result<()>;

    /// Returns the current state of the connection.
    fn is_closed(&self) -> bool;

    /// Return whether the stream is a relay stream.
    fn is_relay(&self) -> bool;

    /// Returns the remote address of the connection.
    fn remote_address(&self) -> SocketAddr;

    /// Return write buffer size
    fn write_buffer_size(&self) -> usize;

    /// Sets the write buffer size.
    fn set_write_buffer_size(&mut self, size: usize);
}

#[allow(async_fn_in_trait)]
pub trait FoctetRecvStream {
    // Returns the Session ID
    fn session_id(&self) -> SessionId;

    /// Returns the Stream ID
    fn stream_id(&self) -> StreamId;

    /// Receives bytes from the stream
    async fn receive_bytes(&mut self) -> Result<BytesMut>;

    /// Receives data from the stream
    async fn receive_data(&mut self, buffer: &mut Vec<u8>) -> Result<usize>;

    /// Receive a frame over the stream
    async fn receive_frame(&mut self) -> Result<Frame>;

    /// Receive a file over the stream
    async fn receive_file(&mut self, file_path: &Path) -> Result<u64>;

    /// Receive a file over the stream in framed bytes
    async fn receive_file_framed_bytes(&mut self, file_path: &std::path::Path) -> Result<u64>;

    /// Receive a file over the stream in raw bytes
    async fn receive_file_raw_bytes(&mut self, file_path: &Path) -> Result<u64>;

    /// Gracefully closes the stream.
    async fn close(&mut self) -> Result<()>;

    /// Returns the current state of the connection.
    fn is_closed(&self) -> bool;

    /// Return whether the stream is a relay stream.
    fn is_relay(&self) -> bool;

    /// Returns the remote address of the connection.
    fn remote_address(&self) -> SocketAddr;

    /// Return read buffer size
    fn read_buffer_size(&self) -> usize;

    /// Sets the read buffer size.
    fn set_read_buffer_size(&mut self, size: usize);
}

#[derive(Debug)]
pub enum SendStream {
    Quic(QuicSendStream),
    Tcp(TlsTcpSendStream),
}

impl FoctetSendStream for SendStream {
    // Returns the session ID
    fn session_id(&self) -> SessionId {
        match self {
            SendStream::Quic(stream) => stream.session_id(),
            SendStream::Tcp(stream) => stream.session_id(),
        }
    }

    /// Returns the Stream ID
    fn stream_id(&self) -> StreamId {
        match self {
            SendStream::Quic(stream) => stream.stream_id(),
            SendStream::Tcp(stream) => stream.stream_id(),
        }
    }

    /// Returns the current operation ID.
    fn operation_id(&self) -> OperationId {
        match self {
            SendStream::Quic(stream) => stream.operation_id(),
            SendStream::Tcp(stream) => stream.operation_id(),
        }
    }

    /// Sends bytes over the stream
    async fn send_bytes(&mut self, bytes: Bytes) -> Result<usize> {
        match self {
            SendStream::Quic(stream) => stream.send_bytes(bytes).await,
            SendStream::Tcp(stream) => stream.send_bytes(bytes).await,
        }
    }

    /// Sends data over the stream
    async fn send_data(&mut self, data: &[u8]) -> Result<OperationId> {
        match self {
            SendStream::Quic(stream) => stream.send_data(data).await,
            SendStream::Tcp(stream) => stream.send_data(data).await,
        }
    }

    /// Send a frame over the stream
    async fn send_frame(&mut self, frame: Frame) -> Result<OperationId> {
        match self {
            SendStream::Quic(stream) => stream.send_frame(frame).await,
            SendStream::Tcp(stream) => stream.send_frame(frame).await,
        }
    }

    /// Send a file over the stream
    async fn send_file(&mut self, file_path: &Path) -> Result<OperationId> {
        match self {
            SendStream::Quic(stream) => stream.send_file(file_path).await,
            SendStream::Tcp(stream) => stream.send_file(file_path).await,
        }
    }

    /// Send a file over the stream in framed bytes
    async fn send_file_framed_bytes(&mut self, file_path: &std::path::Path) -> Result<()> {
        match self {
            SendStream::Quic(stream) => stream.send_file_framed_bytes(file_path).await,
            SendStream::Tcp(stream) => stream.send_file_framed_bytes(file_path).await,
        }
    }

    /// Send a file over the stream in raw bytes
    async fn send_file_raw_bytes(&mut self, file_path: &Path) -> Result<()> {
        match self {
            SendStream::Quic(stream) => stream.send_file_raw_bytes(file_path).await,
            SendStream::Tcp(stream) => stream.send_file_raw_bytes(file_path).await,
        }
    }

    /// Gracefully closes the stream.
    async fn close(&mut self) -> Result<()> {
        match self {
            SendStream::Quic(stream) => stream.close().await,
            SendStream::Tcp(stream) => stream.close().await,
        }
    }

    /// Returns the current state of the connection.
    fn is_closed(&self) -> bool {
        match self {
            SendStream::Quic(stream) => stream.is_closed(),
            SendStream::Tcp(stream) => stream.is_closed(),
        }
    }

    /// Return whether the stream is a relay stream.
    fn is_relay(&self) -> bool {
        match self {
            SendStream::Quic(stream) => stream.is_relay(),
            SendStream::Tcp(stream) => stream.is_relay(),
        }
    }

    /// Returns the remote address of the connection.
    fn remote_address(&self) -> SocketAddr {
        match self {
            SendStream::Quic(stream) => stream.remote_address(),
            SendStream::Tcp(stream) => stream.remote_address(),
        }
    }

    /// Return write buffer size
    fn write_buffer_size(&self) -> usize {
        match self {
            SendStream::Quic(stream) => stream.write_buffer_size(),
            SendStream::Tcp(stream) => stream.write_buffer_size(),
        }
    }

    /// Sets the write buffer size.
    fn set_write_buffer_size(&mut self, size: usize) {
        match self {
            SendStream::Quic(stream) => stream.set_write_buffer_size(size),
            SendStream::Tcp(stream) => stream.set_write_buffer_size(size),
        }
    }
}

#[derive(Debug)]
pub enum RecvStream {
    Quic(QuicRecvStream),
    Tcp(TlsTcpRecvStream),
}

impl FoctetRecvStream for RecvStream {
    // Returns the Connection ID
    fn session_id(&self) -> SessionId {
        match self {
            RecvStream::Quic(stream) => stream.session_id(),
            RecvStream::Tcp(stream) => stream.session_id(),
        }
    }

    /// Returns the Stream ID
    fn stream_id(&self) -> StreamId {
        match self {
            RecvStream::Quic(stream) => stream.stream_id(),
            RecvStream::Tcp(stream) => stream.stream_id(),
        }
    }

    /// Receives bytes from the stream
    async fn receive_bytes(&mut self) -> Result<BytesMut> {
        match self {
            RecvStream::Quic(stream) => stream.receive_bytes().await,
            RecvStream::Tcp(stream) => stream.receive_bytes().await,
        }
    }

    /// Receives data from the stream
    async fn receive_data(&mut self, buffer: &mut Vec<u8>) -> Result<usize> {
        match self {
            RecvStream::Quic(stream) => stream.receive_data(buffer).await,
            RecvStream::Tcp(stream) => stream.receive_data(buffer).await,
        }
    }

    /// Receive a frame over the stream
    async fn receive_frame(&mut self) -> Result<Frame> {
        match self {
            RecvStream::Quic(stream) => stream.receive_frame().await,
            RecvStream::Tcp(stream) => stream.receive_frame().await,
        }
    }

    /// Receive a file over the stream
    async fn receive_file(&mut self, file_path: &Path) -> Result<u64> {
        match self {
            RecvStream::Quic(stream) => stream.receive_file(file_path).await,
            RecvStream::Tcp(stream) => stream.receive_file(file_path).await,
        }
    }

    /// Receive a file over the stream in framed bytes
    async fn receive_file_framed_bytes(&mut self, file_path: &std::path::Path) -> Result<u64> {
        match self {
            RecvStream::Quic(stream) => stream.receive_file_framed_bytes(file_path).await,
            RecvStream::Tcp(stream) => stream.receive_file_framed_bytes(file_path).await,
        }
    }

    /// Receive a file over the stream in raw bytes
    async fn receive_file_raw_bytes(&mut self, file_path: &Path) -> Result<u64> {
        match self {
            RecvStream::Quic(stream) => stream.receive_file_raw_bytes(file_path).await,
            RecvStream::Tcp(stream) => stream.receive_file_raw_bytes(file_path).await,
        }
    }

    /// Gracefully closes the stream.
    async fn close(&mut self) -> Result<()> {
        match self {
            RecvStream::Quic(stream) => stream.close().await,
            RecvStream::Tcp(stream) => stream.close().await,
        }
    }

    /// Returns the current state of the connection.
    fn is_closed(&self) -> bool {
        match self {
            RecvStream::Quic(stream) => stream.is_closed(),
            RecvStream::Tcp(stream) => stream.is_closed(),
        }
    }

    /// Return whether the stream is a relay stream.
    fn is_relay(&self) -> bool {
        match self {
            RecvStream::Quic(stream) => stream.is_relay(),
            RecvStream::Tcp(stream) => stream.is_relay(),
        }
    }

    /// Returns the remote address of the connection.
    fn remote_address(&self) -> SocketAddr {
        match self {
            RecvStream::Quic(stream) => stream.remote_address(),
            RecvStream::Tcp(stream) => stream.remote_address(),
        }
    }

    /// Return read buffer size
    fn read_buffer_size(&self) -> usize {
        match self {
            RecvStream::Quic(stream) => stream.read_buffer_size(),
            RecvStream::Tcp(stream) => stream.read_buffer_size(),
        }
    }

    /// Sets the read buffer size.
    fn set_read_buffer_size(&mut self, size: usize) {
        match self {
            RecvStream::Quic(stream) => stream.set_read_buffer_size(size),
            RecvStream::Tcp(stream) => stream.set_read_buffer_size(size),
        }
    }

}

#[derive(Debug)]
pub enum NetworkStream {
    Quic(QuicStream),
    Tcp(TlsTcpStream),
}

#[allow(async_fn_in_trait)]
impl FoctetStream for NetworkStream {
    fn session_id(&self) -> SessionId {
        match self {
            NetworkStream::Quic(stream) => stream.session_id(),
            NetworkStream::Tcp(stream) => stream.session_id(),
        }
    }
    fn stream_id(&self) -> StreamId {
        match self {
            NetworkStream::Quic(stream) => stream.stream_id(),
            NetworkStream::Tcp(stream) => stream.stream_id(),
        }
    }

    fn operation_id(&self) -> OperationId {
        match self {
            NetworkStream::Quic(stream) => stream.operation_id(),
            NetworkStream::Tcp(stream) => stream.operation_id(),
        }
    }

    async fn handshake(&mut self, dst_node_id: NodeId, data: Option<Vec<u8>>) -> Result<()> {
        match self {
            NetworkStream::Quic(stream) => stream.handshake(dst_node_id, data).await,
            NetworkStream::Tcp(stream) => stream.handshake(dst_node_id, data).await,
        }
    }

    async fn send_bytes(&mut self, bytes: Bytes) -> Result<usize> {
        match self {
            NetworkStream::Quic(stream) => stream.send_bytes(bytes).await,
            NetworkStream::Tcp(stream) => stream.send_bytes(bytes).await,
        }
    }

    async fn receive_bytes(&mut self) -> Result<BytesMut> {
        match self {
            NetworkStream::Quic(stream) => stream.receive_bytes().await,
            NetworkStream::Tcp(stream) => stream.receive_bytes().await,
        }
    }

    async fn send_data(&mut self, data: &[u8]) -> Result<OperationId> {
        match self {
            NetworkStream::Quic(stream) => stream.send_data(data).await,
            NetworkStream::Tcp(stream) => stream.send_data(data).await,
        }
    }

    async fn receive_data(&mut self, buffer: &mut Vec<u8>) -> Result<usize> {
        match self {
            NetworkStream::Quic(stream) => stream.receive_data(buffer).await,
            NetworkStream::Tcp(stream) => stream.receive_data(buffer).await,
        }
    }

    async fn send_frame(&mut self, frame: Frame) -> Result<OperationId> {
        match self {
            NetworkStream::Quic(stream) => stream.send_frame(frame).await,
            NetworkStream::Tcp(stream) => stream.send_frame(frame).await,
        }
    }

    async fn receive_frame(&mut self) -> Result<Frame> {
        match self {
            NetworkStream::Quic(stream) => stream.receive_frame().await,
            NetworkStream::Tcp(stream) => stream.receive_frame().await,
        }
    }

    async fn send_file(&mut self, file_path: &Path) -> Result<OperationId> {
        match self {
            NetworkStream::Quic(stream) => stream.send_file(file_path).await,
            NetworkStream::Tcp(stream) => stream.send_file(file_path).await,
        }
    }

    async fn receive_file(&mut self, file_path: &Path) -> Result<u64> {
        match self {
            NetworkStream::Quic(stream) => stream.receive_file(file_path).await,
            NetworkStream::Tcp(stream) => stream.receive_file(file_path).await,
        }
    }

    async fn send_file_framed_bytes(&mut self, file_path: &std::path::Path) -> Result<()> {
        match self {
            NetworkStream::Quic(stream) => stream.send_file_framed_bytes(file_path).await,
            NetworkStream::Tcp(stream) => stream.send_file_framed_bytes(file_path).await,
        }
    }

    async fn receive_file_framed_bytes(&mut self, file_path: &std::path::Path) -> Result<u64> {
        match self {
            NetworkStream::Quic(stream) => stream.receive_file_framed_bytes(file_path).await,
            NetworkStream::Tcp(stream) => stream.receive_file_framed_bytes(file_path).await,
        }
    }

    async fn send_file_raw_bytes(&mut self, file_path: &Path) -> Result<()> {
        match self {
            NetworkStream::Quic(stream) => stream.send_file_raw_bytes(file_path).await,
            NetworkStream::Tcp(stream) => stream.send_file_raw_bytes(file_path).await,
        }
    }

    async fn receive_file_raw_bytes(&mut self, file_path: &Path) -> Result<u64> {
        match self {
            NetworkStream::Quic(stream) => stream.receive_file_raw_bytes(file_path).await,
            NetworkStream::Tcp(stream) => stream.receive_file_raw_bytes(file_path).await,
        }
    }

    async fn close(&mut self) -> Result<()> {
        match self {
            NetworkStream::Quic(stream) => stream.close().await,
            NetworkStream::Tcp(stream) => stream.close().await,
        }
    }

    fn established(&self) -> bool {
        match self {
            NetworkStream::Quic(stream) => stream.established(),
            NetworkStream::Tcp(stream) => stream.established(),
        }
    }

    fn is_closed(&self) -> bool {
        match self {
            NetworkStream::Quic(stream) => stream.is_closed(),
            NetworkStream::Tcp(stream) => stream.is_closed(),
        }
    }

    fn is_relay(&self) -> bool {
        match self {
            NetworkStream::Quic(stream) => stream.is_relay(),
            NetworkStream::Tcp(stream) => stream.is_relay(),
        }
    }

    fn remote_address(&self) -> SocketAddr {
        match self {
            NetworkStream::Quic(stream) => stream.remote_address(),
            NetworkStream::Tcp(stream) => stream.remote_address(),
        }
    }

    fn transport_protocol(&self) -> TransportProtocol {
        match self {
            NetworkStream::Quic(stream) => stream.transport_protocol(),
            NetworkStream::Tcp(stream) => stream.transport_protocol(),
        }
    }

    fn write_buffer_size(&self) -> usize {
        match self {
            NetworkStream::Quic(stream) => stream.write_buffer_size(),
            NetworkStream::Tcp(stream) => stream.write_buffer_size(),
        }
    }

    fn set_write_buffer_size(&mut self, size: usize) {
        match self {
            NetworkStream::Quic(stream) => stream.set_write_buffer_size(size),
            NetworkStream::Tcp(stream) => stream.set_write_buffer_size(size),
        }
    }

    fn read_buffer_size(&self) -> usize {
        match self {
            NetworkStream::Quic(stream) => stream.read_buffer_size(),
            NetworkStream::Tcp(stream) => stream.read_buffer_size(),
        }
    }

    fn set_read_buffer_size(&mut self, size: usize) {
        match self {
            NetworkStream::Quic(stream) => stream.set_read_buffer_size(size),
            NetworkStream::Tcp(stream) => stream.set_read_buffer_size(size),
        }
    }

    fn split(self) -> (SendStream, RecvStream) {
        match self {
            NetworkStream::Quic(stream) => {
                let (send, recv) = stream.split();
                (send, recv)
            }
            NetworkStream::Tcp(stream) => {
                let (send, recv) = stream.split();
                (send, recv)
            }
        }
    }

    /// Merges a `SendStream` and a `RecvStream` back into a `NetworkStream`.
    fn merge(send_stream: SendStream, recv_stream: RecvStream) -> Result<Self> {
        match (send_stream, recv_stream) {
            // Merge QUIC streams
            (SendStream::Quic(send), RecvStream::Quic(recv)) => {
                Ok(NetworkStream::Quic(QuicStream::merge(SendStream::Quic(send), RecvStream::Quic(recv))?))
            }

            // Merge TCP streams
            (SendStream::Tcp(send), RecvStream::Tcp(recv)) => {
                Ok(NetworkStream::Tcp(TlsTcpStream::merge(SendStream::Tcp(send), RecvStream::Tcp(recv))?))
            }

            // SendStream and RecvStream types do not match
            _ => Err(anyhow::anyhow!("SendStream and RecvStream types do not match")),
        }
    }
}
