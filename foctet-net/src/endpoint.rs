use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::config::EndpointConfig;
use crate::protocol::TransportProtocol;
use crate::transport::connection::Connection;
use crate::transport::quic::connection::{QuicConnection, QuicSocket};
use crate::transport::tcp::connection::TcpSocket;
use crate::transport::tcp::stream::TlsTcpStream;
use crate::transport::stream::FoctetStream;
use crate::transport::stream::NetworkStream;
use crate::relay::client::RelayClient;
use anyhow::Result;
use anyhow::anyhow;
use foctet_core::error::StreamError;
use foctet_core::frame::FrameType;
use foctet_core::frame::Payload;
use foctet_core::node::NodeAddr;
use foctet_core::node::NodeId;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// A listener for incoming network streams.
pub struct Listener {
    receiver: mpsc::Receiver<NetworkStream>,
}

impl Listener {
    /// Accept a new (next) stream
    pub async fn accept(&mut self) -> Option<NetworkStream> {
        self.receiver.recv().await
    }
}

/// A listener for incoming network connection.
pub struct ConnectionListener {
    receiver: mpsc::Receiver<Connection>,
}

impl ConnectionListener {
    /// Accept a new (next) stream
    pub async fn accept(&mut self) -> Option<Connection> {
        self.receiver.recv().await
    }
}

/// Builder for [`Endpoint`].
pub struct EndpointBuilder {
    config: EndpointConfig,
    node_addr: NodeAddr,
}

impl EndpointBuilder {
    /// Create EndpointBuilder with default values
    pub fn new() -> Self {
        Self {
            config: EndpointConfig::new(),
            node_addr: NodeAddr::unspecified(),
        }
    }
    /// Set the configuration for the endpoint.
    pub fn with_config(mut self, config: EndpointConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the node address for the endpoint.
    pub fn with_node_addr(mut self, node_addr: NodeAddr) -> Self {
        self.node_addr = node_addr;
        self
    }

    /// Set the bind address for the endpoint.
    pub fn with_bind_addr(mut self, bind_addr: SocketAddr) -> Self {
        self.config = self.config.with_bind_addr(bind_addr);
        self
    }

    /// Set the server address for the endpoint.
    pub fn with_server_addr(mut self, server_addr: SocketAddr) -> Self {
        self.config = self.config.with_server_addr(server_addr);
        self
    }

    /// Set the transport protocol for the endpoint.
    pub fn with_protocol(mut self, protocol: TransportProtocol) -> Self {
        self.config = self.config.with_protocol(protocol);
        self
    }

    /// Add QUIC support.
    pub fn with_quic(mut self) -> Self {
        self.config = self.config.with_quic();
        self
    }

    /// Add TCP support.
    pub fn with_tcp(mut self) -> Self {
        self.config = self.config.with_tcp();
        self
    }

    /// Add WebSocket support.
    pub fn with_websocket(mut self) -> Self {
        self.config = self.config.with_websocket();
        self
    }

    /// Add WebTransport support.
    pub fn with_webtransport(mut self) -> Self {
        self.config = self.config.with_webtransport();
        self
    }

    /// Set the connection timeout for the endpoint.
    pub fn with_connection_timeout(mut self, timeout: Duration) -> Self {
        self.config = self.config.with_connection_timeout(timeout);
        self
    }

    /// Set the read timeout for the endpoint.
    pub fn with_read_timeout(mut self, timeout: Duration) -> Self {
        self.config = self.config.with_read_timeout(timeout);
        self
    }

    /// Set the write timeout for the endpoint.
    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.config = self.config.with_write_timeout(timeout);
        self
    }

    /// Set the maximum number of retries for the endpoint.
    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.config = self.config.with_max_retries(max_retries);
        self
    }

    pub fn with_cert_path(mut self, cert_path: PathBuf) -> Self {
        self.config = self.config.with_cert_path(cert_path);
        self
    }

    pub fn with_cert_path_option(mut self, cert_path_option: Option<PathBuf>) -> Self {
        self.config = self.config.with_cert_path_option(cert_path_option);
        self
    }

    pub fn with_key_path(mut self, key_path: PathBuf) -> Self {
        self.config = self.config.with_key_path(key_path);
        self
    }

    pub fn with_key_path_option(mut self, key_path_option: Option<PathBuf>) -> Self {
        self.config = self.config.with_key_path_option(key_path_option);
        self
    }

    pub fn with_subject_alt_names(mut self, subject_alt_names: Vec<String>) -> Self {
        self.config = self.config.with_subject_alt_names(subject_alt_names);
        self
    }

    pub fn with_subject_alt_name(mut self, subject_alt_name: String) -> Self {
        self.config = self.config.with_subject_alt_name(subject_alt_name);
        self
    }

    pub fn with_insecure(mut self, insecure: bool) -> Self {
        self.config = self.config.with_insecure(insecure);
        self
    }

    pub fn with_include_loopback(mut self, include_loopback: bool) -> Self {
        self.config = self.config.with_include_loopback(include_loopback);
        self
    }

    /// Sets a custom buffer size for sending data using builder pattern.
    pub fn with_write_buffer_size(mut self, size: usize) -> Result<Self> {
        self.config = self.config.with_write_buffer_size(size)?;
        Ok(self)
    }
    /// Sets a custom buffer size for receiving data using builder pattern.
    pub fn with_read_buffer_size(mut self, size: usize) -> Result<Self> {
        self.config = self.config.with_read_buffer_size(size)?;
        Ok(self)
    }
    /// Sets the write buffer size to the minimum value using builder pattern.
    pub fn with_min_write_buffer_size(mut self) -> Self {
        self.config = self.config.with_min_write_buffer_size();
        self
    }
    /// Sets the read buffer size to the minimum value using builder pattern.
    pub fn with_min_read_buffer_size(mut self) -> Self {
        self.config = self.config.with_min_read_buffer_size();
        self
    }
    /// Sets the write buffer size to the default value using builder pattern.
    pub fn with_default_write_buffer_size(mut self) -> Self {
        self.config = self.config.with_default_write_buffer_size();
        self
    }
    /// Sets the read buffer size to the default value using builder pattern.
    pub fn with_default_read_buffer_size(mut self) -> Self {
        self.config = self.config.with_default_read_buffer_size();
        self
    }
    /// Sets the write buffer size to the maximum value using builder pattern.
    pub fn with_max_write_buffer_size(mut self) -> Self {
        self.config = self.config.with_max_write_buffer_size();
        self
    }
    /// Sets the read buffer size to the maximum value using builder pattern.
    pub fn with_max_read_buffer_size(mut self) -> Self {
        self.config = self.config.with_max_read_buffer_size();
        self
    }

    /// Build the `Endpoint`.
    ///
    /// This initializes the underlying `QuicSocket` and `TcpSocket` based on the provided configuration.
    pub async fn build(self) -> Result<Endpoint> {
        tracing::info!("Building Endpoint...");
        let quic_socket = QuicSocket::new(self.node_addr.node_id.clone(), self.config.clone())?;
        let tcp_socket = TcpSocket::new(self.node_addr.node_id.clone(), self.config.clone())?;
        let relay_client = RelayClient::new(self.node_addr.clone(),self.config.clone())?;
        Ok(Endpoint {
            node_addr: self.node_addr,
            config: self.config,
            quic_socket,
            tcp_socket,
            cancellation_token: CancellationToken::new(),
            relay_client: relay_client,
            quic_connections: Mutex::new(HashMap::new()),
        })
    }
    /// Build the `Endpoint` for client.
    ///
    /// This initializes the underlying `QuicSocket` and `TcpSocket` based on the provided configuration.
    pub async fn build_client(self) -> Result<Endpoint> {
        tracing::info!("Building Endpoint...");
        let quic_socket = QuicSocket::new_client(self.node_addr.node_id.clone(), self.config.clone())?;
        let tcp_socket = TcpSocket::new(self.node_addr.node_id.clone(), self.config.clone())?;
        let relay_client = RelayClient::new(self.node_addr.clone(),self.config.clone())?;
        Ok(Endpoint {
            node_addr: self.node_addr,
            config: self.config,
            quic_socket,
            tcp_socket,
            cancellation_token: CancellationToken::new(),
            relay_client: relay_client,
            quic_connections: Mutex::new(HashMap::new()),
        })
    }
}

#[derive(Debug)]
pub struct EndpointHandle {
    cancel: CancellationToken,
}

impl EndpointHandle {
    pub fn new(cancel: CancellationToken) -> Self {
        Self {
            cancel,
        }
    }
    pub async fn shutdown(&self) {
        self.cancel.cancel();
    }
}

/// The endpoint for the node.
pub struct Endpoint {
    /// The node address for the endpoint.
    pub node_addr: NodeAddr,
    /// The configuration for the endpoint.
    pub config: EndpointConfig,
    /// QUIC socket
    quic_socket: QuicSocket,
    /// TCP socket
    tcp_socket: TcpSocket,
    /// The cancellation token for the endpoint.
    cancellation_token: CancellationToken,
    /// The relay client for the endpoint.
    relay_client: RelayClient,
    /// Active QUIC connections
    quic_connections: Mutex<HashMap<NodeId, QuicConnection>>,
}

impl Endpoint {
    /// Create a new `EndpointBuilder` for constructing an `Endpoint`.
    pub fn builder() -> EndpointBuilder {
        EndpointBuilder::new()
    }
    pub fn handle(&self) -> EndpointHandle {
        EndpointHandle::new(self.cancellation_token.clone())
    }
    pub async fn open(&mut self, node_addr: NodeAddr) -> Result<NetworkStream> {
        for protocol in self.config.enabled_protocols() {
            match protocol {
                TransportProtocol::Quic => {
                    match self.open_quic(node_addr.clone()).await {
                        Ok(stream) => return Ok(stream),
                        Err(e) => {
                            tracing::error!("Failed to connect to the node using QUIC: {:?}", e);
                        }
                    }
                }
                TransportProtocol::Tcp => {
                    match self.open_tcp(node_addr.clone()).await {
                        Ok(stream) => return Ok(stream),
                        Err(e) => {
                            tracing::error!("Failed to connect to the node using TCP: {:?}", e);
                        }
                    }
                }
                TransportProtocol::WebSocket => {
                    // TODO: Implement WebSocket
                    match self.open_tcp(node_addr.clone()).await {
                        Ok(stream) => return Ok(stream),
                        Err(e) => {
                            tracing::error!("Failed to connect to the node using QUIC: {:?}", e);
                        }
                    }
                }
                TransportProtocol::WebTransport => {
                    // TODO: Implement WebTransport
                    match self.open_quic(node_addr.clone()).await {
                        Ok(stream) => return Ok(stream),
                        Err(e) => {
                            tracing::error!("Failed to connect to the node using QUIC: {:?}", e);
                        }
                    }
                }
            }
        }
        Err(anyhow!("Failed to connect to the node"))
    }
    pub async fn open_with_protocol(&mut self, node_addr: NodeAddr, protocol: TransportProtocol) -> Result<NetworkStream> {
        match protocol {
            TransportProtocol::Quic => {
                match self.open_quic(node_addr.clone()).await {
                    Ok(stream) => return Ok(stream),
                    Err(e) => {
                        tracing::error!("Failed to connect to the node using QUIC: {:?}", e);
                        return Err(e);
                    }
                }
            }
            TransportProtocol::Tcp => {
                match self.open_tcp(node_addr.clone()).await {
                    Ok(stream) => return Ok(stream),
                    Err(e) => {
                        tracing::error!("Failed to connect to the node using TCP: {:?}", e);
                        return Err(e);
                    }
                }
            }
            TransportProtocol::WebSocket => {
                // TODO: Implement WebSocket
                match self.open_tcp(node_addr.clone()).await {
                    Ok(stream) => return Ok(stream),
                    Err(e) => {
                        tracing::error!("Failed to connect to the node using QUIC: {:?}", e);
                        return Err(e);
                    }
                }
            }
            TransportProtocol::WebTransport => {
                // TODO: Implement WebTransport
                match self.open_quic(node_addr.clone()).await {
                    Ok(stream) => return Ok(stream),
                    Err(e) => {
                        tracing::error!("Failed to connect to the node using QUIC: {:?}", e);
                        return Err(e);
                    }
                }
            }
        }
    }
    pub async fn open_quic_direct(&mut self, node_addr: NodeAddr) -> Result<NetworkStream> {
        let mut connections = self.quic_connections.lock().await;

        // Check if a connection already exists
        let node_id = node_addr.node_id.clone();
        if let Some(conn) = connections.get_mut(&node_id) {
            // Use the existing connection to open a new stream
            tracing::info!("Reusing existing QUIC connection to {:?}", node_addr);
            let stream = conn.open_stream().await?;
            return Ok(NetworkStream::Quic(stream));
        }

        let mut conn = self.quic_socket.connect_node(node_addr).await?;
        let mut stream = conn.open_stream().await?;
        stream.handshake(node_id.clone(),None).await?;
        connections.insert(node_id, conn);

        Ok(NetworkStream::Quic(stream))
    }
    pub async fn open_quic_relay(&mut self, node_addr: NodeAddr) -> Result<NetworkStream> {
        let relay_addr = match &self.node_addr.relay_addr {
            Some(addr) => addr.clone(),
            None => return Err(anyhow!("Relay address not found")),
        };
        let stream = self.relay_client.open_quic_stream(node_addr.node_id, relay_addr).await?;
        Ok(stream)
    }
    pub async fn open_quic(&mut self, node_addr: NodeAddr) -> Result<NetworkStream> {
        // 1. Try connect to the node directly
        match self.open_quic_direct(node_addr.clone()).await {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                tracing::error!("Failed to connect to the node directly: {:?}", e);
            }
        }
        // 2. Try connect to the node via relay
        match self.open_quic_relay(node_addr).await {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                tracing::error!("Failed to connect to the node via relay: {:?}", e);
            }
        }
        Err(anyhow!("Failed to connect to the node"))
    }
    pub async fn open_tcp_direct(&mut self, node_addr: NodeAddr) -> Result<NetworkStream> {
        let node_id = node_addr.node_id.clone();
        let mut stream = self.tcp_socket.connect_node(node_addr).await?;
        stream.handshake(node_id, None).await?;
        Ok(NetworkStream::Tcp(stream))
    }
    pub async fn open_tcp_relay(&mut self, node_addr: NodeAddr) -> Result<NetworkStream> {
        let relay_addr = match &self.node_addr.relay_addr {
            Some(addr) => addr.clone(),
            None => return Err(anyhow!("Relay address not found")),
        };
        let stream = self.relay_client.open_tcp_stream(node_addr.node_id, relay_addr).await?;
        Ok(stream)
    }
    pub async fn open_tcp(&mut self, node_addr: NodeAddr) -> Result<NetworkStream> {
        // 1. Try connect to the node directly
        match self.open_tcp_direct(node_addr.clone()).await {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                tracing::error!("Failed to connect to the node directly: {:?}", e);
            }
        }
        // 2. Try connect to the node via relay
        match self.open_tcp_relay(node_addr).await {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                tracing::error!("Failed to connect to the node via relay: {:?}", e);
            }
        }
        Err(anyhow!("Failed to connect to the node"))
    }
    pub async fn connect_quic_direct(&mut self, node_addr: NodeAddr) -> Result<Connection> {
        let connections = self.quic_connections.lock().await;

        // Check if a connection already exists
        let node_id = node_addr.node_id.clone();
        if connections.contains_key(&node_id) {
            return Err(anyhow!("QUIC connection already exists"));
        }

        let conn = self.quic_socket.connect_node(node_addr).await?;
        //connections.insert(node_id, conn);
        Ok(Connection::Quic(conn))
    }
    pub async fn connect_quic_relay(&mut self) -> Result<Connection> {
        let relay_addr = match &self.node_addr.relay_addr {
            Some(addr) => addr.clone(),
            None => return Err(anyhow!("Relay address not found")),
        };
        let conn = self.relay_client.connect_quic(relay_addr).await?;
        Ok(conn)
    }
    pub async fn connect_quic(&mut self, node_addr: NodeAddr) -> Result<Connection> {
        // 1. Try connect to the node directly
        match self.connect_quic_direct(node_addr.clone()).await {
            Ok(conn) => return Ok(conn),
            Err(e) => {
                tracing::error!("Failed to connect to the node directly: {:?}", e);
            }
        }
        // 2. Try connect to the node via relay
        match self.connect_quic_relay().await {
            Ok(conn) => return Ok(conn),
            Err(e) => {
                tracing::error!("Failed to connect to the node via relay: {:?}", e);
            }
        }
        Err(anyhow!("Failed to connect to the node"))
    }
    pub async fn connect_tcp(&mut self, _node_addr: NodeAddr) -> Result<Connection> {
        Err(anyhow!("Not implemented"))
    }
    pub async fn connect(&mut self, node_addr: NodeAddr) -> Result<Connection> {
        for protocol in self.config.enabled_protocols() {
            match protocol {
                TransportProtocol::Quic => {
                    return self.connect_quic(node_addr).await;
                }
                TransportProtocol::Tcp => {
                    return self.connect_tcp(node_addr).await;
                }
                _ => {}
            }
        }
        Err(anyhow!("Failed to connect to the node"))
    }
    pub async fn listen_conn(&mut self) -> Result<ConnectionListener> {
        let (sender, receiver) = mpsc::channel::<Connection>(100);

        // Start the relay listener if a relay address is available
        if self.node_addr.relay_addr.is_some() {
            let relay_client = self.relay_client.clone();
            let cancellation_token = self.cancellation_token.clone();
            let relay_stream_sender = sender.clone();
            tokio::spawn(async move {
                match start_relay_conn_listen_task(relay_client, cancellation_token, relay_stream_sender).await {
                    Ok(_) => {
                        tracing::info!("Relay listener stopped.");
                    }
                    Err(e) => {
                        tracing::error!("Error starting relay listener: {:?}", e);
                    }
                }
            });
        } else {
            tracing::warn!("Relay address not found. skipping relay listener.");
        }

        // Start the QUIC and TCP listeners for direct connections
        let quic_socket = self.quic_socket.clone();
        let tcp_socket = self.tcp_socket.clone();
        let cancellation_token = self.cancellation_token.clone();
        tokio::spawn(async move {
            match start_conn_listen_task(quic_socket, tcp_socket, cancellation_token, sender).await {
                Ok(_) => {
                    tracing::info!("Endpoint listener stopped.");
                }
                Err(e) => {
                    tracing::error!("Error starting listener: {:?}", e);
                }
            }
        });
        Ok(ConnectionListener { receiver })
    }
    /// Start listening for incoming network streams. 
    /// Returns a [`Listener`] that can be used to accept incoming streams.
    pub async fn listen(&mut self) -> Result<Listener> {
        let (sender, receiver) = mpsc::channel::<NetworkStream>(100);

        // Start the relay listener if a relay address is available
        if self.node_addr.relay_addr.is_some() {
            let relay_client = self.relay_client.clone();
            let cancellation_token = self.cancellation_token.clone();
            let relay_stream_sender = sender.clone();
            tokio::spawn(async move {
                match start_relay_listen_task(relay_client, cancellation_token, relay_stream_sender).await {
                    Ok(_) => {
                        tracing::info!("Relay listener stopped.");
                    }
                    Err(e) => {
                        tracing::error!("Error starting relay listener: {:?}", e);
                    }
                }
            });
        } else {
            tracing::warn!("Relay address not found. skipping relay listener.");
        }

        // Start the QUIC and TCP listeners for direct connections
        let quic_socket = self.quic_socket.clone();
        let tcp_socket = self.tcp_socket.clone();
        let cancellation_token = self.cancellation_token.clone();
        tokio::spawn(async move {
            match start_listen_task(quic_socket, tcp_socket, cancellation_token, sender).await {
                Ok(_) => {
                    tracing::info!("Endpoint listener stopped.");
                }
                Err(e) => {
                    tracing::error!("Error starting listener: {:?}", e);
                }
            }
        });
        
        Ok(Listener { receiver })
    }
    /// Remove stale or unused QUIC connections from the connection pool.
    pub async fn prune_quic_connections(&self) {
        let mut connections = self.quic_connections.lock().await;

        // Filter out connections that are no longer active or required.
        connections.retain(|node_id, connection| {
            let is_active = connection.is_active();
            if !is_active {
                tracing::info!("Removing inactive QUIC connection to node: {:?}", node_id);
            }
            is_active
        });

        tracing::info!(
            "Pruned QUIC connections. Remaining active connections: {}",
            connections.len()
        );
    }
    
    /// Gracefully shutdown the `Endpoint`.
    pub async fn shutdown(&self) -> Result<()> {
        tracing::info!("Shutting down Endpoint...");

        // Cancel any ongoing operations
        self.cancellation_token.cancel();
        tracing::info!("Cancellation token triggered.");

        // Clean up active QUIC connections
        let mut quic_connections = self.quic_connections.lock().await;
        for (node_id, mut connection) in quic_connections.drain() {
            tracing::info!("Closing QUIC connection to node: {:?}", node_id);
            if let Err(e) = connection.close().await {
                tracing::warn!("Error closing QUIC connection to {:?}: {:?}", node_id, e);
            }
        }
        tracing::info!("All QUIC connections closed.");

        tracing::info!("Endpoint shut down completed.");
        Ok(())
    }
}

async fn start_listen_task(quic_socket: QuicSocket, tcp_socket: TcpSocket, cancellation_token: CancellationToken, sender: mpsc::Sender<NetworkStream>) -> Result<()> {
    let (quic_conn_tx, mut quic_conn_rx) = mpsc::channel::<QuicConnection>(100);
    let mut quic_socket = quic_socket;
    let quic_cancel_token = cancellation_token.clone();
    tracing::info!("Starting QUIC listener...");
    tokio::spawn(async move {
        match quic_socket.listen(quic_conn_tx, quic_cancel_token).await {
            Ok(_) => {
                tracing::info!("QUIC listener stopped.");
            }
            Err(e) => {
                tracing::error!("Error listening: {:?}", e);
            }
        }
    });
    let (tcp_conn_tx, mut tcp_conn_rx) = mpsc::channel::<TlsTcpStream>(100);
    let mut tcp_socket = tcp_socket;
    let tcp_cancel_token = cancellation_token.clone();
    tracing::info!("Starting TCP listener...");
    tokio::spawn(async move {
        match tcp_socket.listen(tcp_conn_tx, tcp_cancel_token).await {
            Ok(_) => {
                tracing::info!("TCP listener stopped.");
            }
            Err(e) => {
                tracing::error!("Error listening: {:?}", e);
            }
        }
    });
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("Endpoint listener cancelled.");
                break;
            }
            Some(mut conn) = quic_conn_rx.recv() => {
                tracing::info!("Accepted QUIC connection from: {}", conn.remote_address());
                let sender = sender.clone();
                tokio::spawn(async move {
                    match conn.accept_stream().await {
                        Ok(stream) => {
                            tracing::info!("Accepted QUIC stream from: {}", stream.remote_address());
                            let stream = NetworkStream::Quic(stream);
                            if let Err(e) = sender.send(stream).await {
                                tracing::error!("Error sending QUIC stream: {:?}", e);
                            }
                        }
                        Err(e) => {
                            tracing::error!("Error accepting stream: {:?}", e);
                        }
                    }
                });
            }
            Some(conn) = tcp_conn_rx.recv() => {
                tracing::info!("Accepted TCP connection from: {}", conn.remote_address());
                let stream = NetworkStream::Tcp(conn);
                if let Err(e) = sender.send(stream).await {
                    tracing::error!("Error sending TCP connection: {:?}", e);
                }
            }
        }
    }
    Ok(())
}

async fn start_relay_listen_task(relay_client: RelayClient, cancellation_token: CancellationToken, sender: mpsc::Sender<NetworkStream>) -> Result<()> {
    let relay_addr = match &relay_client.node_addr.relay_addr {
        Some(addr) => addr.clone(),
        None => return Err(anyhow!("Relay address not found")),
    };
    let mut relay_client = relay_client;
    tracing::info!("Opening relay control stream...");
    let mut control_stream = relay_client.open_control_stream().await?;
    tracing::info!("Relay control stream opened.");

    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("Client actor loop cancelled, closing loop");
                break;
            }
            result = control_stream.receive_frame() => match result {
                Ok(frame) => {
                    match frame.frame_type {
                        FrameType::Connect => {
                            if let Some(payload) = frame.payload {
                                match payload {
                                    Payload::Handshake(handshake) => {
                                        tracing::info!("Received handshake from: {:?}", handshake.src_node_id);
                                        match relay_client.open_stream(handshake.src_node_id, relay_addr.clone()).await {
                                            Ok(stream) => {
                                                match sender.send(stream).await {
                                                    Ok(_) => {
                                                        tracing::info!("Opened relay stream");
                                                    }
                                                    Err(e) => {
                                                        tracing::error!("Error sending stream: {:?}", e);
                                                    }
                                                }
                                            },
                                            Err(e) => {
                                                tracing::error!("Error connecting to relay: {:?}", e);
                                            }
                                        }
                                    }
                                    _ => {
                                        tracing::warn!("Received unexpected payload type");
                                    }
                                }
                            }
                        }
                        _ => {
                            tracing::warn!("Received unexpected frame type");
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Error receiving frame: {:?}", e);
                    // Check if the stream is closed
                    if let Some(e) = e.downcast_ref::<StreamError>() {
                        match e {
                            StreamError::Closed => {
                                tracing::info!("Relay control stream closed");
                                break;
                            }
                            _ => {
                                tracing::warn!("Error receiving frame: {:?}", e);
                            }
                        }
                    } else {
                        tracing::warn!("Error receiving frame: {:?}", e);
                    }
                }
            }           
        }
    }
    Ok(())
}

async fn start_conn_listen_task(quic_socket: QuicSocket, _tcp_socket: TcpSocket, cancellation_token: CancellationToken, sender: mpsc::Sender<Connection>) -> Result<()> {
    let (quic_conn_tx, mut quic_conn_rx) = mpsc::channel::<QuicConnection>(100);
    let mut quic_socket = quic_socket;
    let quic_cancel_token = cancellation_token.clone();
    tracing::info!("Starting QUIC listener...");
    tokio::spawn(async move {
        match quic_socket.listen(quic_conn_tx, quic_cancel_token).await {
            Ok(_) => {
                tracing::info!("QUIC listener stopped.");
            }
            Err(e) => {
                tracing::error!("Error listening: {:?}", e);
            }
        }
    });
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("Endpoint listener cancelled.");
                break;
            }
            Some(conn) = quic_conn_rx.recv() => {
                tracing::info!("Accepted QUIC connection from: {}", conn.remote_address());
                if let Err(e) = sender.send(Connection::Quic(conn)).await {
                    tracing::error!("Error sending QUIC stream: {:?}", e);
                }
            }
        }
    }
    Ok(())
}

async fn start_relay_conn_listen_task(relay_client: RelayClient, cancellation_token: CancellationToken, sender: mpsc::Sender<Connection>) -> Result<()> {
    let relay_addr = match &relay_client.node_addr.relay_addr {
        Some(addr) => addr.clone(),
        None => return Err(anyhow!("Relay address not found")),
    };
    let mut relay_client = relay_client;
    tracing::info!("Opening relay control stream...");
    let mut control_stream = relay_client.open_control_stream().await?;
    tracing::info!("Relay control stream opened.");

    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("Client actor loop cancelled, closing loop");
                break;
            }
            result = control_stream.receive_frame() => match result {
                Ok(frame) => {
                    match frame.frame_type {
                        FrameType::Connect => {
                            if let Some(payload) = frame.payload {
                                match payload {
                                    Payload::Handshake(handshake) => {
                                        tracing::info!("Received handshake from: {:?}", handshake.src_node_id);
                                        match relay_client.connect(relay_addr.clone()).await {
                                            Ok(conn) => {
                                                match sender.send(conn).await {
                                                    Ok(_) => {
                                                        tracing::info!("Opened relay stream");
                                                    }
                                                    Err(e) => {
                                                        tracing::error!("Error sending stream: {:?}", e);
                                                    }
                                                }
                                            },
                                            Err(e) => {
                                                tracing::error!("Error connecting to relay: {:?}", e);
                                            }
                                        }
                                    }
                                    _ => {
                                        tracing::warn!("Received unexpected payload type");
                                    }
                                }
                            }
                        }
                        _ => {
                            tracing::warn!("Received unexpected frame type");
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Error receiving frame: {:?}", e);
                    // Check if the stream is closed
                    if let Some(e) = e.downcast_ref::<StreamError>() {
                        match e {
                            StreamError::Closed => {
                                tracing::info!("Relay control stream closed");
                                break;
                            }
                            _ => {
                                tracing::warn!("Error receiving frame: {:?}", e);
                            }
                        }
                    } else {
                        tracing::warn!("Error receiving frame: {:?}", e);
                    }
                }
            }           
        }
    }
    Ok(())
}
