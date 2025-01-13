use crate::config::EndpointConfig;
use anyhow::anyhow;
use anyhow::Result;
use foctet_core::frame::OperationId;
use foctet_core::frame::StreamId;
use foctet_core::node::RelayAddr;
use foctet_core::node::{SessionId, NodeAddr, NodeId};
use tokio_util::sync::CancellationToken;
use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use crate::transport::filter;
use crate::transport::priority;
use super::stream::TlsTcpStream;

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

    pub async fn connect_node_with_timeout(&mut self, node_addr: NodeAddr, duration: Duration) -> Result<TlsTcpStream> {
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

    pub async fn connect_relay(&mut self, relay_addr: RelayAddr) -> Result<TlsTcpStream> {
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

    pub async fn connect_relay_with_timeout(&mut self, relay_addr: RelayAddr, duration: Duration) -> Result<TlsTcpStream> {
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
