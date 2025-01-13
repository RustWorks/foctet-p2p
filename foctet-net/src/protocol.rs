/// The type of transport protocols used by foctet.
/// The meaning `Transport` here is NOT a list of transport layer protocols.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TransportProtocol {
    /// QUIC
    Quic,
    /// TCP
    Tcp,
    /// WebSocket
    WebSocket,
    /// WebTransport
    WebTransport,
}

impl Default for TransportProtocol {
    fn default() -> Self {
        TransportProtocol::Quic
    }
}

impl TransportProtocol {
    /// Converts a string to a transport protocol.
    pub fn from_str(protocol: &str) -> Self {
        match protocol.to_lowercase().as_str() {
            "quic" => TransportProtocol::Quic,
            "tcp" => TransportProtocol::Tcp,
            "websocket" => TransportProtocol::WebSocket,
            "webtransport" => TransportProtocol::WebTransport,
            _ => TransportProtocol::Quic,
        }
    }
}
