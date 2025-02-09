use anyhow::Result;
use foctet_core::node::{NodeAddr, NodeId, RelayAddr};
use crate::{config::EndpointConfig, protocol::TransportProtocol, transport::{connection::Connection, quic::connection::QuicSocket, tcp::connection::TcpSocket}};
use crate::transport::stream::{FoctetStream, NetworkStream};

#[derive(Clone)]
pub struct RelayClient {
    pub node_addr: NodeAddr,
    pub config: EndpointConfig,
    /// QUIC socket
    quic_socket: QuicSocket,
    /// TCP socket
    tcp_socket: TcpSocket,
}

impl RelayClient {
    /// Create a new `RelayClient` with the given node address and configuration.
    pub fn new(node_addr: NodeAddr, config: EndpointConfig) -> Result<Self> {
        let quic_socket = QuicSocket::new_client(node_addr.node_id.clone(), config.clone())?;
        let tcp_socket = TcpSocket::new(node_addr.node_id.clone(), config.clone())?;
        Ok(Self {
            node_addr,
            config,
            quic_socket: quic_socket,
            tcp_socket: tcp_socket,
        })
    }
    pub async fn open_control_stream(&mut self) -> Result<NetworkStream> {
        let relay_addr = if let Some(relay_addr) = &self.node_addr.relay_addr {
            relay_addr.clone() 
        } else{
            return Err(anyhow::anyhow!("No relay address found"));
        };
        for protocol in self.config.enabled_protocols() {
            match protocol {
                TransportProtocol::Quic => {
                    return self.open_control_stream_quic(relay_addr.clone()).await;
                }
                TransportProtocol::Tcp => {
                    return self.open_control_stream_tcp(relay_addr.clone()).await;
                }
                _ => {}
            }
        }
        Err(anyhow::anyhow!("No transport protocol found"))
    }
    pub async fn open_control_stream_quic(&mut self, relay_addr: RelayAddr) -> Result<NetworkStream> {
        let mut conn = self.quic_socket.connect_relay(relay_addr).await?;
        let mut stream = conn.open_stream().await?;
        stream.handshake(NodeId::zero(),None).await?;
        Ok(NetworkStream::Quic(stream))
    }
    pub async fn open_control_stream_tcp(&mut self, relay_addr: RelayAddr) -> Result<NetworkStream> {
        let mut stream = self.tcp_socket.connect_relay(relay_addr).await?;
        stream.handshake(NodeId::zero(),None).await?;
        Ok(NetworkStream::Tcp(stream))
    }
    pub async fn open_stream(&mut self, dst_node_addr: NodeId, relay_addr: RelayAddr) -> Result<NetworkStream> {
        for protocol in self.config.enabled_protocols() {
            match protocol {
                TransportProtocol::Quic => {
                    return self.open_quic_stream(dst_node_addr, relay_addr).await;
                }
                TransportProtocol::Tcp => {
                    return self.open_tcp_stream(dst_node_addr, relay_addr).await;
                }
                _ => {}
            }
        }
        Err(anyhow::anyhow!("No transport protocol found"))
    }
    pub async fn open_quic_stream(&mut self, dst_node_addr: NodeId, relay_addr: RelayAddr) -> Result<NetworkStream> {
        let mut conn = self.quic_socket.connect_relay(relay_addr).await?;
        let mut stream = conn.open_stream().await?;
        stream.handshake(dst_node_addr, None).await?;
        Ok(NetworkStream::Quic(stream))
    }
    pub async fn open_tcp_stream(&mut self, dst_node_addr: NodeId, relay_addr: RelayAddr) -> Result<NetworkStream> {
        let mut stream = self.tcp_socket.connect_relay(relay_addr).await?;
        stream.handshake(dst_node_addr, None).await?;
        Ok(NetworkStream::Tcp(stream))
    }
    pub async fn connect(&mut self, relay_addr: RelayAddr) -> Result<Connection> {
        for protocol in self.config.enabled_protocols() {
            match protocol {
                TransportProtocol::Quic => {
                    return self.connect_quic(relay_addr).await;
                }
                TransportProtocol::Tcp => {
                    return self.connect_tcp(relay_addr).await;
                }
                _ => {}
            }
        }
        Err(anyhow::anyhow!("No transport protocol found"))
    }
    pub async fn connect_quic(&mut self, relay_addr: RelayAddr) -> Result<Connection> {
        let conn = self.quic_socket.connect_relay(relay_addr).await?;
        Ok(Connection::Quic(conn))
    }
    pub async fn connect_tcp(&mut self, _relay_addr: RelayAddr) -> Result<Connection> {
        Err(anyhow::anyhow!("Not implemented"))
    }
}
