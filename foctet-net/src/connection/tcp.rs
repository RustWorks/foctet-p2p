use super::FoctetRecvStream;
use super::FoctetSendStream;
use super::FoctetStream;
use crate::config::EndpointConfig;
use crate::config::TransportProtocol;
use anyhow::anyhow;
use anyhow::Result;
use foctet_core::error::StreamError;
use foctet_core::frame::HandshakeData;
use foctet_core::frame::OperationId;
use foctet_core::frame::{Frame, FrameType, Payload, StreamId};
use foctet_core::node::RelayAddr;
use foctet_core::node::{SessionId, NodeAddr, NodeId};
use futures::sink::SinkExt;
use tokio_util::sync::CancellationToken;
use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::io::{AsyncRead, AsyncWrite};
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct TlsTcpSendStream {
    pub framed_writer: FramedWrite<WriteHalf<TlsStream<TcpStream>>, LengthDelimitedCodec>,
    pub node_id: NodeId,
    pub stream_id: StreamId,
    pub session_id: SessionId,
    pub send_buffer_size: usize,
    pub is_closed: bool,
    pub is_relay: bool,
    pub next_operation_id: OperationId,
    pub remote_address: SocketAddr,
}

impl FoctetSendStream for TlsTcpSendStream {
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
        //framed_writer.get_mut().shutdown().await?;
        let operation_id = self.operation_id();
        self.next_operation_id.increment();
        Ok(operation_id)
    }
    async fn send_frame(&mut self, frame: Frame) -> Result<OperationId> {
        let serialized_message = frame.to_bytes()?;
        self.framed_writer.send(serialized_message).await?;

        self.framed_writer.flush().await?;
        //framed_writer.get_mut().shutdown().await?;
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
        //framed_writer.get_mut().shutdown().await?;
        let operation_id = self.operation_id();
        self.next_operation_id.increment();
        Ok(operation_id)
    }
    async fn close(&mut self) -> Result<()> {
        self.framed_writer.flush().await?;
        self.framed_writer.get_mut().shutdown().await?;
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
}

#[derive(Debug)]
pub struct TlsTcpRecvStream {
    pub framed_reader: FramedRead<ReadHalf<TlsStream<TcpStream>>, LengthDelimitedCodec>,
    pub node_id: NodeId,
    pub stream_id: StreamId,
    pub session_id: SessionId,
    pub receive_buffer_size: usize,
    pub is_closed: bool,
    pub is_relay: bool,
    pub remote_address: SocketAddr,
}

impl FoctetRecvStream for TlsTcpRecvStream {
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
                    tracing::error!("Error reading from stream: {:?}", e);
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
                    tracing::error!("Error reading from stream: {:?}", e);
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
    async fn close(&mut self) -> Result<()> {
        //self.recv_stream.shutdown().await?;
        //self.stream.get_mut().0.shutdown().await?;
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
}

#[derive(Debug)]
pub struct TlsTcpStream {
    pub framed_writer: FramedWrite<WriteHalf<TlsStream<TcpStream>>, LengthDelimitedCodec>,
    pub framed_reader: FramedRead<ReadHalf<TlsStream<TcpStream>>, LengthDelimitedCodec>,
    pub node_id: NodeId,
    pub stream_id: StreamId,
    pub session_id: SessionId,
    pub send_buffer_size: usize,
    pub receive_buffer_size: usize,
    pub established: bool,
    pub is_closed: bool,
    pub is_relay: bool,
    pub next_operation_id: OperationId,
    pub remote_address: SocketAddr,
}

impl TlsTcpStream {
    pub fn with_relay(mut self) -> Self {
        self.is_relay = true;
        self
    }
}

impl AsyncRead for TlsTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().framed_reader.get_mut()).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().framed_writer.get_mut()).poll_write(cx, buf)
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

impl FoctetStream for TlsTcpStream {
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
        //framed_writer.get_mut().shutdown().await?;
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
                    tracing::error!("Error reading from stream: {:?}", e);
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
        //framed_writer.get_mut().shutdown().await?;
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
                    tracing::error!("Error reading from stream: {:?}", e);
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
        //framed_writer.get_mut().shutdown().await?;
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

    async fn close(&mut self) -> Result<()> {
        self.framed_writer.flush().await?;
        self.framed_writer.get_mut().shutdown().await?;
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
        TransportProtocol::Tcp
    }

    fn split(self) -> (super::SendStream, super::RecvStream) {
        let tcp_send_stream = TlsTcpSendStream {
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
        let tcp_recv_stream = TlsTcpRecvStream {
            framed_reader: self.framed_reader,
            node_id: self.node_id.clone(),
            stream_id: self.stream_id,
            session_id: self.session_id,
            receive_buffer_size: self.receive_buffer_size,
            is_closed: self.is_closed,
            is_relay: self.is_relay,
            remote_address: self.remote_address,
        };
        let send_stream = super::SendStream::Tcp(tcp_send_stream);
        let recv_stream = super::RecvStream::Tcp(tcp_recv_stream);
        (send_stream, recv_stream)
    }
    fn merge(send_stream: super::SendStream, recv_stream: super::RecvStream) -> Result<Self> where Self: Sized {
        match (send_stream, recv_stream) {
            (super::SendStream::Tcp(tcp_send_stream), super::RecvStream::Tcp(tcp_recv_stream)) => {
                Ok(Self {
                    framed_writer: tcp_send_stream.framed_writer,
                    framed_reader: tcp_recv_stream.framed_reader,
                    node_id: tcp_send_stream.node_id,
                    stream_id: tcp_send_stream.stream_id,
                    session_id: tcp_send_stream.session_id,
                    send_buffer_size: tcp_send_stream.send_buffer_size,
                    receive_buffer_size: tcp_recv_stream.receive_buffer_size,
                    established: true,
                    is_closed: tcp_send_stream.is_closed,
                    is_relay: tcp_send_stream.is_relay,
                    next_operation_id: tcp_send_stream.next_operation_id,
                    remote_address: tcp_send_stream.remote_address,
                })
            }
            _ => Err(anyhow!("Invalid stream types")),
        }
    }
}

#[derive(Clone)]
pub struct TcpSocket {
    pub node_id: NodeId,
    pub config: EndpointConfig,
    pub tls_connector: TlsConnector,
    pub tls_acceptor: TlsAcceptor,
}

impl TcpSocket {
    /// Creates a new TCP socket with given node_id and config
    /// The socket acts as both a client and a server.
    pub fn new(node_id: NodeId, config: EndpointConfig) -> Result<Self> {
        let tls_connector = TlsConnector::from(Arc::new(config.tls_client_config().unwrap()));
        let tls_acceptor = TlsAcceptor::from(Arc::new(config.tls_server_config().unwrap()));
        Ok(Self {
            node_id: node_id,
            config: config,
            tls_connector: tls_connector,
            tls_acceptor: tls_acceptor,
        })
    }

    pub async fn connect(
        &mut self,
        server_addr: SocketAddr,
        server_name: &str,
    ) -> Result<TlsTcpStream> {
        let name = rustls_pki_types::ServerName::try_from(server_name.to_string())?;
        let stream = TcpStream::connect(server_addr).await?;
        let remote_address = stream.peer_addr()?;
        let tls_stream = self.tls_connector.connect(name, stream).await?;
        let (read_half, write_half) = tokio::io::split(TlsStream::Client(tls_stream));
        let framed_writer = FramedWrite::new(write_half, LengthDelimitedCodec::new());
        let framed_reader = FramedRead::new(read_half, LengthDelimitedCodec::new());
        let tls_tcp_stream = TlsTcpStream {
            framed_writer: framed_writer,
            framed_reader: framed_reader,
            node_id: self.node_id.clone(),
            stream_id: StreamId::new(0),
            session_id: SessionId::new(),
            send_buffer_size: self.config.write_buffer_size(),
            receive_buffer_size: self.config.read_buffer_size(),
            established: false,
            is_closed: false,
            is_relay: false,
            next_operation_id: OperationId(0),
            remote_address: remote_address,
        };
        Ok(tls_tcp_stream)
    }

    pub async fn connect_with_timeout(
        &mut self,
        server_addr: SocketAddr,
        server_name: &str,
        duration: Duration
    ) -> Result<TlsTcpStream> {
        match tokio::time::timeout(duration, self.connect(server_addr, server_name)).await {
            Ok(connect_result) => connect_result,
            Err(elapsed) => Err(anyhow!("Connection timed out after {:?}", elapsed)),
        }
    }

    pub async fn listen(&mut self, sender: mpsc::Sender<TlsTcpStream>, cancel_token: CancellationToken) -> Result<()> {
        let listener = TcpListener::bind(self.config.server_addr()).await?;
        tracing::info!("Listening on {}/TCP", self.config.server_addr());
    
        loop {
            tokio::select! {
                // Monitor the cancellation token
                _ = cancel_token.cancelled() => {
                    tracing::info!("TcpSocket listen cancelled");
                    break;
                }
    
                // Accept incoming connections
                incoming = listener.accept() => {
                    match incoming {
                        Ok((stream, addr)) => {
                            let remote_address = stream.peer_addr()?;
                            tracing::info!("Accepted connection from {}", addr);
                            match self.tls_acceptor.accept(stream).await {
                                Ok(tls_stream) => {
                                    let (read_half, write_half) = tokio::io::split(TlsStream::Server(tls_stream));
                                    let framed_writer = FramedWrite::new(write_half, LengthDelimitedCodec::new());
                                    let framed_reader = FramedRead::new(read_half, LengthDelimitedCodec::new());
                                    let tls_tcp_stream = TlsTcpStream {
                                        framed_writer: framed_writer,
                                        framed_reader: framed_reader,
                                        node_id: self.node_id.clone(),
                                        stream_id: StreamId::new(0),
                                        session_id: SessionId::new(),
                                        send_buffer_size: self.config.write_buffer_size(),
                                        receive_buffer_size: self.config.read_buffer_size(),
                                        established: false,
                                        is_closed: false,
                                        is_relay: false,
                                        next_operation_id: OperationId(0),
                                        remote_address,
                                    };
                                    if sender.send(tls_tcp_stream).await.is_err() {
                                        tracing::warn!("Failed to send TlsTcpStream to the channel");
                                        break;
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Failed to complete TLS handshake: {:?}", e);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Error accepting TCP connection: {:?}", e);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn connect_node(&mut self, node_addr: NodeAddr) -> Result<TlsTcpStream> {
        let sorted_addrs = super::priority::sort_socket_addrs(&node_addr.socket_addresses);
        let addrs = super::filter::filter_reachable_addrs(sorted_addrs, self.config.include_loopback);
        let server_name = node_addr.get_server_name();
        for addr in addrs {
            match self.connect(addr, &server_name).await {
                Ok(connection) => {
                    tracing::info!("Connected to {}", addr);
                    return Ok(connection);
                }
                Err(e) => {
                    tracing::error!("Error connecting to {}: {:?}", addr, e);
                }
            }
        }
        Err(anyhow!("Failed to connect to node"))
    }

    pub async fn connect_node_with_timeout(&mut self, node_addr: NodeAddr, duration: Duration) -> Result<TlsTcpStream> {
        let sorted_addrs = super::priority::sort_socket_addrs(&node_addr.socket_addresses);
        let addrs = super::filter::filter_reachable_addrs(sorted_addrs, self.config.include_loopback);
        let server_name = node_addr.get_server_name();
        for addr in addrs {
            match self.connect_with_timeout(addr, &server_name, duration).await {
                Ok(connection) => {
                    tracing::info!("Connected to {}", addr);
                    return Ok(connection);
                }
                Err(e) => {
                    tracing::error!("Error connecting to {}: {:?}", addr, e);
                }
            }
        }
        Err(anyhow!("Failed to connect to node"))
    }

    pub async fn connect_relay(&mut self, relay_addr: RelayAddr) -> Result<TlsTcpStream> {
        let sorted_addrs = super::priority::sort_socket_addrs(&relay_addr.socket_addresses);
        let addrs = super::filter::filter_reachable_addrs(sorted_addrs, self.config.include_loopback);
        let server_name = relay_addr.get_server_name();
        for addr in addrs {
            match self.connect(addr, &server_name).await {
                Ok(connection) => {
                    tracing::info!("Connected to {}", addr);
                    return Ok(connection.with_relay());
                }
                Err(e) => {
                    tracing::error!("Error connecting to {}: {:?}", addr, e);
                }
            }
        }
        Err(anyhow!("Failed to connect to node"))
    }

    pub async fn connect_relay_with_timeout(&mut self, relay_addr: RelayAddr, duration: Duration) -> Result<TlsTcpStream> {
        let sorted_addrs = super::priority::sort_socket_addrs(&relay_addr.socket_addresses);
        let addrs = super::filter::filter_reachable_addrs(sorted_addrs, self.config.include_loopback);
        let server_name = relay_addr.get_server_name();
        for addr in addrs {
            match self.connect_with_timeout(addr, &server_name, duration).await {
                Ok(connection) => {
                    tracing::info!("Connected to {}", addr);
                    return Ok(connection.with_relay());
                }
                Err(e) => {
                    tracing::error!("Error connecting to {}: {:?}", addr, e);
                }
            }
        }
        Err(anyhow!("Failed to connect to node"))
    }
}
