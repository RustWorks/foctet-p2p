use anyhow::Result;
use foctet_core::{
    frame::StreamId,
    node::{SessionId, NodeAddr, NodeId},
};
use crate::protocol::TransportProtocol;

use super::{quic::connection::QuicConnection, stream::{FoctetRecvStream, FoctetSendStream, FoctetStream, NetworkStream, RecvStream, SendStream}, tcp::connection::TlsTcpConnection};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Instant};
use tokio::sync::{Mutex, RwLock};

#[allow(async_fn_in_trait)]
pub trait FoctetConnection {
    async fn open_stream(&mut self) -> Result<NetworkStream>;
    async fn accept_stream(&mut self) -> Result<NetworkStream>;
    async fn open_uni_stream(&mut self) -> Result<SendStream>;
    async fn accept_uni_stream(&mut self) -> Result<RecvStream>;
    async fn send_file_parallel(
        &mut self,
        file_path: &std::path::Path,
        chunk_size: usize,
    ) -> Result<()>;
    async fn receive_file_parallel(
        &mut self,
        output_path: &std::path::Path,
        chunk_size: usize,
        expected_chunk_count: usize,
    ) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
    fn id(&self) -> SessionId;
    fn remote_address(&self) -> SocketAddr;
    fn is_active(&self) -> bool;
    fn transport_protocol(&self) -> TransportProtocol;
}

pub enum Connection {
    Quic(QuicConnection),
    Tcp(TlsTcpConnection),
}

#[allow(async_fn_in_trait)]
impl FoctetConnection for Connection {
    async fn open_stream(&mut self) -> Result<NetworkStream> {
        match self {
            Connection::Quic(quic_connection) => {
                let stream = quic_connection.open_stream().await?;
                Ok(NetworkStream::Quic(stream))
            },
            Connection::Tcp(tls_tcp_connection) => {
                let stream = tls_tcp_connection.open_stream().await?;
                Ok(NetworkStream::Tcp(stream))
            }
        }
    }
    async fn accept_stream(&mut self) -> Result<NetworkStream> {
        match self {
            Connection::Quic(quic_connection) => {
                let stream = quic_connection.accept_stream().await?;
                Ok(NetworkStream::Quic(stream))
            },
            Connection::Tcp(tls_tcp_connection) => {
                let stream = tls_tcp_connection.accept_stream().await?;
                Ok(NetworkStream::Tcp(stream))
            }
        }
    }
    async fn open_uni_stream(&mut self) -> Result<SendStream> {
        match self {
            Connection::Quic(quic_connection) => {
                let stream = quic_connection.open_uni_stream().await?;
                Ok(SendStream::Quic(stream))
            },
            Connection::Tcp(tls_tcp_connection) => {
                let stream = tls_tcp_connection.open_uni_stream().await?;
                Ok(SendStream::Tcp(stream))
            }
        }
    }
    async fn accept_uni_stream(&mut self) -> Result<RecvStream> {
        match self {
            Connection::Quic(quic_connection) => {
                let stream = quic_connection.accept_uni_stream().await?;
                Ok(RecvStream::Quic(stream))
            },
            Connection::Tcp(tls_tcp_connection) => {
                let stream = tls_tcp_connection.accept_uni_stream().await?;
                Ok(RecvStream::Tcp(stream))
            }
        }
    }
    async fn send_file_parallel(
        &mut self,
        file_path: &std::path::Path,
        chunk_size: usize,
    ) -> Result<()> {
        match self {
            Connection::Quic(quic_connection) => {
                quic_connection.send_file_parallel(file_path, chunk_size).await?;
            },
            Connection::Tcp(tls_tcp_connection) => {
                tls_tcp_connection.send_file_parallel(file_path, chunk_size).await?;
            }
        }
        Ok(())
    }
    async fn receive_file_parallel(
        &mut self,
        output_path: &std::path::Path,
        expected_chunk_count: usize,
        expected_file_size: usize,
    ) -> Result<()> {
        match self {
            Connection::Quic(quic_connection) => {
                quic_connection.receive_file_parallel(output_path, expected_chunk_count, expected_file_size).await?;
            },
            Connection::Tcp(tls_tcp_connection) => {
                tls_tcp_connection.receive_file_parallel(output_path, expected_chunk_count, expected_file_size).await?;
            }
        }
        Ok(())
    }
    async fn close(&mut self) -> Result<()> {
        match self {
            Connection::Quic(quic_connection) => {
                quic_connection.close().await?;
            },
            Connection::Tcp(tls_tcp_connection) => {
                tls_tcp_connection.close().await?;
            }
        }
        Ok(())
    }
    fn id(&self) -> SessionId {
        match self {
            Connection::Quic(quic_connection) => quic_connection.id(),
            Connection::Tcp(tls_tcp_connection) => tls_tcp_connection.id(),
        }
    }
    fn remote_address(&self) -> SocketAddr {
        match self {
            Connection::Quic(quic_connection) => quic_connection.remote_address(),
            Connection::Tcp(tls_tcp_connection) => tls_tcp_connection.remote_address(),
        }
    }
    fn is_active(&self) -> bool {
        match self {
            Connection::Quic(quic_connection) => quic_connection.is_active(),
            Connection::Tcp(tls_tcp_connection) => tls_tcp_connection.is_active(),
        }
    }
    fn transport_protocol(&self) -> TransportProtocol {
        match self {
            Connection::Quic(quic_connection) => quic_connection.transport_protocol(),
            Connection::Tcp(tls_tcp_connection) => tls_tcp_connection.transport_protocol(),
        }
    }
}

/// Represents the type of connection used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionType {
    /// Direct connection to the target node.
    Direct,
    /// Connection via a relay server.
    Relay,
}

/// Connection Information
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub node_id: NodeId,
    pub socket_addr: SocketAddr,
    pub connection_type: ConnectionType,
}

impl ConnectionInfo {
    pub fn new(node_id: NodeId, socket_addr: SocketAddr, connection_type: ConnectionType) -> Self {
        Self {
            node_id,
            socket_addr,
            connection_type,
        }
    }
}

#[derive(Debug)]
pub struct StreamMap {
    //streams: Arc<RwLock<HashMap<StreamId, Arc<Mutex<NetworkStream>>>>>,
    /// The map of send streams
    send_streams: Arc<RwLock<HashMap<StreamId, Arc<Mutex<SendStream>>>>>,
    /// The map of receive streams
    recv_streams: Arc<RwLock<HashMap<StreamId, Arc<Mutex<RecvStream>>>>>,
}

impl StreamMap {
    pub fn new() -> Self {
        Self {
            send_streams: Arc::new(RwLock::new(HashMap::new())),
            recv_streams: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add SendStream to the map
    pub async fn add_send_stream(&self, stream_id: StreamId, send_stream: Arc<Mutex<SendStream>>) {
        let mut send_streams = self.send_streams.write().await;
        send_streams.insert(stream_id, send_stream);
    }

    /// Add RecvStream to the map
    pub async fn add_recv_stream(&self, stream_id: StreamId, recv_stream: Arc<Mutex<RecvStream>>) {
        let mut recv_streams = self.recv_streams.write().await;
        recv_streams.insert(stream_id, recv_stream);
    }

    // Get SendStream from the map
    pub async fn get_send_stream(&self, stream_id: &StreamId) -> Option<Arc<Mutex<SendStream>>> {
        let send_streams = self.send_streams.read().await;
        send_streams.get(stream_id).cloned()
    }

    /// Get RecvStream from the map
    pub async fn get_recv_stream(&self, stream_id: &StreamId) -> Option<Arc<Mutex<RecvStream>>> {
        let recv_streams = self.recv_streams.read().await;
        recv_streams.get(stream_id).cloned()
    }

    /// Remove SendStream from the map
    pub async fn remove_send_stream(&self, stream_id: &StreamId) {
        let mut send_streams = self.send_streams.write().await;
        send_streams.remove(stream_id);
    }

    /// Remove RecvStream from the map
    pub async fn remove_recv_stream(&self, stream_id: &StreamId) {
        let mut recv_streams = self.recv_streams.write().await;
        recv_streams.remove(stream_id);
    }

    /// Remove Stream from the map
    pub async fn remove_stream(&self, stream_id: &StreamId) -> Result<()> {
        self.remove_send_stream(stream_id).await;
        self.remove_recv_stream(stream_id).await;
        Ok(())
    }

    /// Remove all streams from the map
    pub async fn remove_all_streams(&self) {
        let mut send_streams = self.send_streams.write().await;
        send_streams.clear();

        let mut recv_streams = self.recv_streams.write().await;
        recv_streams.clear();
    }
}

#[derive(Debug)]
pub struct Session {
    pub session_id: SessionId,
    pub node_addr: NodeAddr,
    pub quic_connection: Option<QuicConnection>,
    pub stream_map: StreamMap,
    pub last_accessed: Arc<Mutex<Instant>>,
}

impl Session {
    pub fn new(session_id: SessionId, node_addr: NodeAddr) -> Self {
        Self {
            session_id,
            node_addr,
            quic_connection: None,
            stream_map: StreamMap::new(),
            last_accessed: Arc::new(Mutex::new(Instant::now())),
        }
    }

    pub fn new_with_quic_connection(
        session_id: SessionId,
        node_addr: NodeAddr,
        quic_connection: QuicConnection,
    ) -> Self {
        Self {
            session_id,
            node_addr,
            quic_connection: Some(quic_connection),
            stream_map: StreamMap::new(),
            last_accessed: Arc::new(Mutex::new(Instant::now())),
        }
    }

    pub async fn add_stream(&self, stream_id: StreamId, stream: NetworkStream) {
        self.update_last_accessed().await;
        // Split the stream into send and receive streams
        let (send_stream, recv_stream) = stream.split();
        self.stream_map.add_send_stream(stream_id, Arc::new(Mutex::new(send_stream))).await;
        self.stream_map.add_recv_stream(stream_id, Arc::new(Mutex::new(recv_stream))).await;
    }

    pub async fn get_stream(&self, stream_id: &StreamId) -> Option<(Arc<Mutex<SendStream>>, Arc<Mutex<RecvStream>>)> {
        self.update_last_accessed().await;
        let send_stream = self.get_send_stream(stream_id).await;
        let recv_stream = self.get_recv_stream(stream_id).await;
        if send_stream.is_some() && recv_stream.is_some() {
            Some((send_stream.unwrap(), recv_stream.unwrap()))
        } else {
            None
        }
    }

    pub async fn get_send_stream(&self, stream_id: &StreamId) -> Option<Arc<Mutex<SendStream>>> {
        self.update_last_accessed().await;
        self.stream_map.get_send_stream(stream_id).await
    }

    pub async fn get_recv_stream(&self, stream_id: &StreamId) -> Option<Arc<Mutex<RecvStream>>> {
        self.update_last_accessed().await;
        self.stream_map.get_recv_stream(stream_id).await
    }

    /// Returns the first available stream.
    /// Which is not closed and not in use (not locked).
    pub async fn get_available_stream(&self) -> Option<(Arc<Mutex<SendStream>>, Arc<Mutex<RecvStream>>)> {
        self.update_last_accessed().await;
        let send_streams = self.stream_map.send_streams.read().await;
        let recv_streams = self.stream_map.recv_streams.read().await;
        for (stream_id, send_stream) in send_streams.iter() {
            if let Ok(send_stream_lock) = send_stream.try_lock() {
                if !send_stream_lock.is_closed() {
                    if let Some(recv_stream) = recv_streams.get(stream_id) {
                        if let Ok(recv_stream_lock) = recv_stream.try_lock() {
                            if !recv_stream_lock.is_closed() {
                                return Some((send_stream.clone(), recv_stream.clone()));
                            }
                        }
                    }
                }
            }
        }
        None
    }

    /// Returns the first available send stream.
    pub async fn get_available_send_stream(&self) -> Option<Arc<Mutex<SendStream>>> {
        self.update_last_accessed().await;
        let send_streams = self.stream_map.send_streams.read().await;
        send_streams.values().find(|stream| {
            match stream.try_lock() {
                Ok(stream_lock) => {
                    !stream_lock.is_closed()
                },
                Err(_) => false,
            }
        }).cloned()
    }

    /// Returns the first available receive stream.
    pub async fn get_available_recv_stream(&self) -> Option<Arc<Mutex<RecvStream>>> {
        self.update_last_accessed().await;
        let recv_streams = self.stream_map.recv_streams.read().await;
        recv_streams.values().find(|stream| {
            match stream.try_lock() {
                Ok(stream_lock) => {
                    !stream_lock.is_closed()
                },
                Err(_) => false,
            }
        }).cloned()
    }

    pub async fn remove_stream(&self, stream_id: &StreamId) -> Result<()> {
        self.stream_map.remove_stream(stream_id).await
    }
    pub async fn update_last_accessed(&self) {
        let mut last_accessed = self.last_accessed.lock().await;
        *last_accessed = Instant::now();
    }
    /// Close all streams in the session.
    pub async fn close(&mut self) {
        // Close all streams
        self.stream_map.remove_all_streams().await;
        // Close the QUIC connection
        if let Some(quic_connection) = &mut self.quic_connection {
            let _ = quic_connection.close().await;
        }
    }
    /// Cleans up unnecessary streams, keeping at least one open stream.
    pub async fn cleanup_streams(&self) {
        // Check available stream
        let available_stream_id: StreamId = if let Some(stream) = self.get_available_send_stream().await {
            stream.lock().await.stream_id()
        }else{
            return;
        };
        // Remove all streams except the 1 available stream.
        let streams = self.stream_map.send_streams.read().await;
        let mut stream_ids: Vec<StreamId> = Vec::new();
        for stream_id in streams.keys() {
            if *stream_id != available_stream_id {
                stream_ids.push(*stream_id);
            }
        }
        for stream_id in stream_ids {
            self.remove_stream(&stream_id).await.unwrap();
        }
    }
}
