use super::endpoint;
use super::stream::QuicRecvStream;
use super::stream::QuicSendStream;
use super::stream::QuicStream;
use crate::config::EndpointConfig;
use crate::protocol::TransportProtocol;
use crate::transport::filter;
use crate::transport::priority;
use crate::transport::stream::FoctetRecvStream;
use crate::transport::stream::FoctetSendStream;
use anyhow::anyhow;
use anyhow::Result;
use foctet_core::default::{DEFAULT_CONNECTION_TIMEOUT, DEFAULT_KEEP_ALIVE_INTERVAL};
use foctet_core::error::ConnectionError;
use foctet_core::frame::ChunkMetadata;
use foctet_core::frame::Frame;
use foctet_core::frame::FrameType;
use foctet_core::frame::OperationId;
use foctet_core::frame::Payload;
use foctet_core::frame::StreamId;
use foctet_core::node::{NodeAddr, RelayAddr};
use foctet_core::node::{SessionId, NodeId};
use foctet_core::state::ConnectionState;
use quinn::{ClientConfig, Connection, Endpoint, RecvStream as QuinnRecvStream, SendStream as QuinnSendStream, ServerConfig, TransportConfig};
use tokio::fs::File;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

/// The maximum number of concurrent streams
const MAX_CONCURRENT_STREAMS: usize = 10;

#[derive(Debug)]
pub struct QuicConnection {
    pub node_id: NodeId,
    pub session_id: SessionId,
    /// The QUIC connection
    pub connection: Connection,
    pub state: ConnectionState,
    pub is_relay: bool,
    pub send_buffer_size: usize,
    pub receive_buffer_size: usize,
    pub next_stream_id: StreamId,
}

impl QuicConnection {
    pub fn new(node_id: NodeId, connection: Connection, config: &EndpointConfig) -> Self {
        Self {
            node_id: node_id,
            session_id: SessionId::new(),
            connection: connection,
            state: ConnectionState::Connected,
            is_relay: false,
            send_buffer_size: config.write_buffer_size(),
            receive_buffer_size: config.read_buffer_size(),
            next_stream_id: StreamId::new(0),
        }
    }

    pub fn with_relay(mut self) -> Self {
        self.is_relay = true;
        self
    }

    pub async fn open_stream(&mut self) -> Result<QuicStream> {
        let (send_stream, recv_stream) = self.connection.open_bi().await?;
        let framed_writer: FramedWrite<QuinnSendStream, LengthDelimitedCodec> =
            FramedWrite::new(send_stream, LengthDelimitedCodec::new());
        let framed_reader: FramedRead<QuinnRecvStream, LengthDelimitedCodec> = 
            FramedRead::new(recv_stream, LengthDelimitedCodec::new());
        let quic_stream = QuicStream {
            framed_writer: framed_writer,
            framed_reader: framed_reader,
            node_id: self.node_id.clone(),
            stream_id: self.next_stream_id,
            session_id: self.session_id.clone(),
            send_buffer_size: self.send_buffer_size,
            receive_buffer_size: self.receive_buffer_size,
            established: false,
            is_closed: false,
            is_relay: self.is_relay,
            next_operation_id: OperationId(0),
            remote_address: self.remote_address(),
        };
        tracing::info!(
            "Opened bi-directional stream with ID: {}",
            self.next_stream_id
        );
        self.next_stream_id.increment();
        Ok(quic_stream)
    }

    pub async fn accept_stream(&mut self) -> Result<QuicStream> {
        let (send_stream, recv_stream) = match self.connection.accept_bi().await {
            Ok(streams) => streams,
            Err(e) => match e {
                quinn::ConnectionError::ApplicationClosed(_) => {
                    self.state = ConnectionState::Disconnected;
                    return Err(ConnectionError::Closed.into());
                }
                quinn::ConnectionError::ConnectionClosed(_) => {
                    self.state = ConnectionState::Disconnected;
                    return Err(ConnectionError::Closed.into());
                }
                _ => {
                    return Err(anyhow!("Error accepting stream"));
                }
            },
        };
        let framed_writer: FramedWrite<QuinnSendStream, LengthDelimitedCodec> =
            FramedWrite::new(send_stream, LengthDelimitedCodec::new());
        let framed_reader: FramedRead<QuinnRecvStream, LengthDelimitedCodec> = 
            FramedRead::new(recv_stream, LengthDelimitedCodec::new());
        let quic_stream = QuicStream {
            framed_writer: framed_writer,
            framed_reader: framed_reader,
            node_id: self.node_id.clone(),
            stream_id: self.next_stream_id,
            session_id: self.session_id.clone(),
            send_buffer_size: self.send_buffer_size,
            receive_buffer_size: self.receive_buffer_size,
            established: false,
            is_closed: false,
            is_relay: self.is_relay,
            next_operation_id: OperationId(0),
            remote_address: self.remote_address(),
        };
        tracing::info!(
            "Accepted bi-directional stream with ID: {}",
            self.next_stream_id
        );
        self.next_stream_id.increment();
        Ok(quic_stream)
    }

    pub async fn open_uni_stream(&mut self) -> Result<QuicSendStream> {
        let send_stream = self.connection.open_uni().await?;
        let framed_writer: FramedWrite<QuinnSendStream, LengthDelimitedCodec> =
            FramedWrite::new(send_stream, LengthDelimitedCodec::new());
        let quic_stream = QuicSendStream {
            framed_writer: framed_writer,
            node_id: self.node_id.clone(),
            stream_id: self.next_stream_id,
            session_id: self.session_id.clone(),
            send_buffer_size: self.send_buffer_size,
            is_closed: false,
            is_relay: self.is_relay,
            next_operation_id: OperationId(0),
            remote_address: self.remote_address(),
        };
        tracing::info!(
            "Opened uni-directional stream with ID: {}",
            self.next_stream_id
        );
        self.next_stream_id.increment();
        Ok(quic_stream)
    }

    pub async fn accept_uni_stream(&mut self) -> Result<QuicRecvStream> {
        let recv_stream = match self.connection.accept_uni().await {
            Ok(streams) => streams,
            Err(e) => match e {
                quinn::ConnectionError::ApplicationClosed(_) => {
                    self.state = ConnectionState::Disconnected;
                    return Err(ConnectionError::Closed.into());
                }
                quinn::ConnectionError::ConnectionClosed(_) => {
                    self.state = ConnectionState::Disconnected;
                    return Err(ConnectionError::Closed.into());
                }
                _ => {
                    return Err(anyhow!("Error accepting stream"));
                }
            },
        };
        let framed_reader: FramedRead<QuinnRecvStream, LengthDelimitedCodec> = 
            FramedRead::new(recv_stream, LengthDelimitedCodec::new());
        let quic_stream = QuicRecvStream {
            framed_reader: framed_reader,
            node_id: self.node_id.clone(),
            stream_id: self.next_stream_id,
            session_id: self.session_id.clone(),
            receive_buffer_size: self.receive_buffer_size,
            is_closed: false,
            is_relay: self.is_relay,
            remote_address: self.remote_address(),
        };
        tracing::info!(
            "Accepted uni-directional stream with ID: {}",
            self.next_stream_id
        );
        self.next_stream_id.increment();
        Ok(quic_stream)
    }

    /// Close the QUIC connection
    pub async fn close(&mut self) -> Result<()> {
        // close the connection
        self.connection.close(0u32.into(), b"");
        self.state = ConnectionState::Disconnected;
        Ok(())
    }
    pub fn id(&self) -> SessionId {
        self.session_id.clone()
    }
    pub fn remote_address(&self) -> SocketAddr {
        self.connection.remote_address()
    }
    /// Check if the connection is still active.
    pub fn is_active(&self) -> bool {
        self.state != ConnectionState::Disconnected
    }
    pub fn transport_protocol(&self) -> TransportProtocol {
        TransportProtocol::Quic
    }
    /// Sends a file in parallel using multiple QUIC streams.
    /// Each stream handles a specific chunk of the file.
    pub async fn send_file_parallel(
        &mut self,
        file_path: &std::path::Path,
        chunk_size: usize,
    ) -> Result<()> {
        let file = tokio::fs::File::open(file_path).await?;
        let file_size = file.metadata().await?.len();
        let num_chunks = (file_size + chunk_size as u64 - 1) / chunk_size as u64; // ceil division
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_STREAMS));

        tracing::info!(
            "Sending file: {:?} (size: {} bytes, {} chunks, chunk_size: {})",
            file_path,
            file_size,
            num_chunks,
            chunk_size
        );

        let name: String = file_path
        .file_name()
        .unwrap_or_default()
        .to_str()
        .unwrap_or_default()
        .to_string();

        // Create a task for each chunk
        let mut tasks = Vec::new();
        for i in 0..num_chunks {
            let offset = i * chunk_size as u64;
            let length = std::cmp::min(chunk_size as u64, file_size - offset);
            let permit = semaphore.clone().acquire_owned().await?;

            let chunk_metadata = ChunkMetadata {
                chunk_id: i,
                total_chunks: num_chunks,
                chunk_size: length as usize,
                total_size: file_size as usize,
                offset: offset as usize,
                name: name.clone(),
            };
            let file_path = file_path.to_path_buf();

            // Open a new uni-directional stream
            let mut stream = self.open_uni_stream().await?;
            let task = tokio::spawn(async move {
                let metadata_frame: Frame = Frame::builder()
                    .with_fin(true)
                    .with_frame_type(FrameType::TransferStart)
                    .with_operation_id(stream.operation_id())
                    .with_payload(Payload::chunk_metadata(chunk_metadata))
                    .build();
                stream.send_frame(metadata_frame).await?;

                stream.send_file_range(file_path.as_path(), offset, length).await?;
                drop(permit); // Release the semaphore
                Ok::<(), anyhow::Error>(())
            });
            tasks.push(task);
        }
        // Wait for all tasks to complete
        let results = futures::future::join_all(tasks).await;
        for result in results {
            if let Err(e) = result {
                tracing::error!("Error in file transfer task: {:?}", e);
                return Err(e.into());
            }
        }

        tracing::info!("File transfer completed: {:?}", file_path);
        Ok(())
    }

    pub async fn receive_file_parallel(
        &mut self,
        output_path: &std::path::Path,
        expected_chunk_count: usize,
        expected_file_size: usize,
    ) -> Result<()> {
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_STREAMS));
        let file = File::create(output_path).await?;
        file.set_len(expected_file_size as u64).await?;
        let output_path = output_path.to_path_buf();
    
        // Create a task for each chunk
        let mut tasks = Vec::new();
        for _i in 0..expected_chunk_count {
            let permit = semaphore.clone().acquire_owned().await?;
            let mut recv_stream = self.accept_uni_stream().await?; // Open a new uni-directional stream
            let output_path = output_path.clone();
            let task = tokio::spawn(async move {
                let metadata_frame = recv_stream.receive_frame().await?; // Receive the metadata frame
                let metadata = if let Some(payload) = &metadata_frame.payload {
                    match payload {
                        Payload::ChunkMetadata(metadata) => metadata.clone(),
                        _ => {
                            tracing::error!(
                                "Expected a content metadata, but received: {:?}",
                                metadata_frame
                            );
                            return Err(anyhow::anyhow!(
                                "Expected a content metadata, but received: {:?}",
                                metadata_frame
                            ));
                        }
                    }
                } else {
                    tracing::error!(
                        "Expected a content metadata, but received: {:?}",
                        metadata_frame
                    );
                    return Err(anyhow::anyhow!(
                        "Expected a content metadata, but received: {:?}",
                        metadata_frame
                    ));
                };
                recv_stream.receive_file_range(&output_path, metadata.offset as u64, metadata.chunk_size as u64).await?;
    
                drop(permit); // Release the semaphore
                Ok::<(), anyhow::Error>(())
            });
            tasks.push(task);
        }
        
        // Wait for all tasks to complete
        let results = futures::future::join_all(tasks).await;
        for result in results {
            if let Err(e) = result {
                tracing::error!("Error in file transfer task: {:?}", e);
                return Err(e.into());
            }
        }
        tracing::info!("File transfer completed: {:?}", output_path);
        Ok(())
    }
}

#[derive(Clone)]
pub struct QuicSocket {
    pub node_id: NodeId,
    pub config: EndpointConfig,
    pub endpoint: Endpoint,
}

impl QuicSocket {
    /// Creates a new QUIC socket with given node_id and config.
    /// The socket acts as both a client and a server.
    pub fn new(node_id: NodeId, config: EndpointConfig) -> Result<Self> {
        let client_config: ClientConfig =
            endpoint::make_client_config(config.tls_client_config().unwrap())?;
        let mut server_config: ServerConfig =
            endpoint::make_server_config(config.tls_server_config().unwrap())?;

        // Create a transport configuration
        let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
        transport_config.max_idle_timeout(Some(DEFAULT_CONNECTION_TIMEOUT.try_into()?));
        transport_config.keep_alive_interval(Some(DEFAULT_KEEP_ALIVE_INTERVAL));
        // Create a QUIC endpoint
        let mut endpoint: Endpoint = Endpoint::server(server_config, config.server_addr())?;
        endpoint.set_default_client_config(client_config);
        Ok(Self {
            node_id: node_id,
            config: config,
            endpoint: endpoint,
        })
    }
    /// Creates a new QUIC client with given node_id and config.
    /// The socket acts as a client.
    pub fn new_client(node_id: NodeId, config: EndpointConfig) -> Result<Self> {
        let mut client_config = endpoint::make_client_config(config.tls_client_config().unwrap())?;
        // Create a transport configuration
        let mut transport_config = TransportConfig::default();
        transport_config.max_idle_timeout(Some(DEFAULT_CONNECTION_TIMEOUT.try_into()?));
        client_config.transport_config(Arc::new(transport_config));
        // Create a QUIC endpoint
        let mut endpoint = Endpoint::client(config.bind_addr)?;
        endpoint.set_default_client_config(client_config);
        Ok(Self {
            node_id: node_id,
            config: config,
            endpoint: endpoint,
        })
    }
    pub async fn connect(
        &mut self,
        server_addr: SocketAddr,
        server_name: &str,
    ) -> Result<QuicConnection> {
        let connection = self.endpoint.connect(server_addr, server_name)?.await?;
        let quic_connection = QuicConnection::new(self.node_id.clone(), connection, &self.config);
        Ok(quic_connection)
    }
    pub async fn connect_with_timeout(
        &mut self,
        server_addr: SocketAddr,
        server_name: &str,
        duration: Duration
    ) -> Result<QuicConnection> {
        match tokio::time::timeout(duration, self.connect(server_addr, server_name)).await {
            Ok(connect_result) => connect_result,
            Err(elapsed) => Err(anyhow!("Connection timed out after {:?}", elapsed)),
        }
    }
    pub async fn listen(&mut self, sender: mpsc::Sender<QuicConnection>, cancel_token: CancellationToken) -> Result<()> {
        tracing::info!("Listening on {}/UDP(QUIC)", self.config.server_addr());
        loop {
            tokio::select! {
                // Monitor the cancellation token
                _ = cancel_token.cancelled() => {
                    tracing::info!("QuicSocket listen cancelled");
                    break;
                }
                // Accept incoming connections
                incoming = self.endpoint.accept() => {
                    match incoming {
                        Some(incoming_connection) => {
                            match incoming_connection.await {
                                Ok(connection) => {
                                    tracing::info!("Accepted connection from {}", connection.remote_address());
                                    let quic_connection = QuicConnection::new(self.node_id.clone(), connection, &self.config);
                                    if sender.send(quic_connection).await.is_err() {
                                        tracing::warn!("Failed to send QuicConnection to the channel");
                                        break;
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Error accepting connection: {:?}", e);
                                }
                            }
                        }
                        None => {
                            tracing::warn!("No incoming connection; endpoint may have been closed");
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn connect_node(&mut self, node_addr: NodeAddr) -> Result<QuicConnection> {
        let sorted_addrs = priority::sort_socket_addrs(&node_addr.socket_addresses);
        let addrs = filter::filter_reachable_addrs(sorted_addrs, self.config.include_loopback);
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
    pub async fn connect_node_with_timeout(&mut self, node_addr: NodeAddr, duration: Duration) -> Result<QuicConnection> {
        let sorted_addrs = priority::sort_socket_addrs(&node_addr.socket_addresses);
        let addrs = filter::filter_reachable_addrs(sorted_addrs, self.config.include_loopback);
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
    pub async fn connect_relay(&mut self, relay_addr: RelayAddr) -> Result<QuicConnection> {
        let sorted_addrs = priority::sort_socket_addrs(&relay_addr.socket_addresses);
        let addrs = filter::filter_reachable_addrs(sorted_addrs, self.config.include_loopback);
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
    pub async fn connect_relay_with_timeout(&mut self, relay_addr: RelayAddr, duration: Duration) -> Result<QuicConnection> {
        let sorted_addrs = priority::sort_socket_addrs(&relay_addr.socket_addresses);
        let addrs = filter::filter_reachable_addrs(sorted_addrs, self.config.include_loopback);
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
