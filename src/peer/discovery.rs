use crate::peer::extension::PeerExtension;
use crate::peer::{
    ConnectionProtocol, Peer, PeerId, PeerStream, ProtocolExtensionFlags, Result, TcpPeerDiscovery,
    UtpPeerDiscovery,
};
use crate::storage::Storage;
use crate::torrent::InnerTorrent;
use crate::torrent_data::DataPool;
use crate::TorrentMetadata;
use async_trait::async_trait;
#[cfg(test)]
pub use mock::*;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// A received peer entry incoming connection.
#[derive(Debug)]
pub struct PeerEntry {
    /// The peer address
    pub socket_addr: SocketAddr,
    /// The peer incoming tcp stream
    pub stream: PeerStream,
}

impl PeerEntry {
    /// Returns the connection protocol of the peer entry.
    pub fn protocol(&self) -> ConnectionProtocol {
        match &self.stream {
            PeerStream::Tcp(_) => ConnectionProtocol::Tcp,
            PeerStream::Utp(_) => ConnectionProtocol::Utp,
        }
    }
}

impl PartialEq for PeerStream {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PeerStream::Tcp(_), PeerStream::Tcp(_)) => true,
            (PeerStream::Utp(_), PeerStream::Utp(_)) => true,
            _ => false,
        }
    }
}

/// The discovery strategy for creating outgoing and receiving incoming peer connections.
#[derive(Debug, Clone)]
pub enum PeerDiscovery {
    Tcp(TcpPeerDiscovery),
    Utp(UtpPeerDiscovery),
    Other(Arc<dyn Discovery>),
}

impl PeerDiscovery {
    /// Returns the address on which this peer discovery is listening on.
    pub fn addr(&self) -> &SocketAddr {
        match self {
            PeerDiscovery::Tcp(discovery) => discovery.addr(),
            PeerDiscovery::Utp(discovery) => discovery.addr(),
            PeerDiscovery::Other(discovery) => discovery.addr(),
        }
    }

    /// Returns the underlying protocol of the peer discovery.
    pub fn protocol(&self) -> ConnectionProtocol {
        match self {
            PeerDiscovery::Tcp(_) => ConnectionProtocol::Tcp,
            PeerDiscovery::Utp(_) => ConnectionProtocol::Utp,
            PeerDiscovery::Other(discovery) => discovery.protocol(),
        }
    }

    /// Try to dial (_create outgoing connection with_) to the target peer address.
    pub async fn dial(
        &self,
        peer_id: PeerId,
        peer_addr: SocketAddr,
        peer_port: Option<u16>,
        peer_client_name: impl Into<String>,
        torrent: InnerTorrent,
        metadata: TorrentMetadata,
        data_pool: DataPool,
        storage: Storage,
        protocol_extensions: ProtocolExtensionFlags,
        extensions: Vec<PeerExtension>,
        connection_timeout: Duration,
    ) -> Result<Peer> {
        match self {
            PeerDiscovery::Tcp(discovery) => {
                discovery
                    .dial(
                        peer_id,
                        peer_addr,
                        peer_port,
                        peer_client_name.into(),
                        torrent,
                        metadata,
                        data_pool,
                        storage,
                        protocol_extensions,
                        extensions,
                        connection_timeout,
                    )
                    .await
            }
            PeerDiscovery::Utp(discovery) => {
                discovery
                    .dial(
                        peer_id,
                        peer_addr,
                        peer_port,
                        peer_client_name.into(),
                        torrent,
                        metadata,
                        data_pool,
                        storage,
                        protocol_extensions,
                        extensions,
                        connection_timeout,
                    )
                    .await
            }
            PeerDiscovery::Other(discovery) => {
                discovery
                    .dial(
                        peer_id,
                        peer_addr,
                        peer_port,
                        peer_client_name.into(),
                        torrent,
                        metadata,
                        data_pool,
                        storage,
                        protocol_extensions,
                        extensions,
                        connection_timeout,
                    )
                    .await
            }
        }
    }

    /// Try to receive an incoming peer connection from the peer discovery.
    /// Returns [None] if the peer discovery connection has been closed.
    pub async fn recv(&self) -> Option<PeerEntry> {
        match self {
            PeerDiscovery::Tcp(discovery) => discovery.recv().await,
            PeerDiscovery::Utp(discovery) => discovery.recv().await,
            PeerDiscovery::Other(discovery) => discovery.recv().await,
        }
    }

    /// Close the peer discovery connection.
    pub fn close(&self) {
        match self {
            PeerDiscovery::Tcp(discovery) => discovery.close(),
            PeerDiscovery::Utp(discovery) => discovery.close(),
            PeerDiscovery::Other(discovery) => discovery.close(),
        }
    }
}

impl From<TcpPeerDiscovery> for PeerDiscovery {
    fn from(discovery: TcpPeerDiscovery) -> Self {
        Self::Tcp(discovery)
    }
}

impl From<UtpPeerDiscovery> for PeerDiscovery {
    fn from(discovery: UtpPeerDiscovery) -> Self {
        Self::Utp(discovery)
    }
}

impl<D> From<D> for PeerDiscovery
where
    D: Discovery + 'static,
{
    fn from(discovery: D) -> Self {
        Self::Other(Arc::from(discovery))
    }
}

/// A peer discovery is responsible for discovering outgoing and incoming peer connections.
#[async_trait]
pub trait Discovery: Debug + Send + Sync {
    /// Get the address on which this peer listener is listening on.
    fn addr(&self) -> &SocketAddr;

    /// Returns the underlying protocol of the peer discovery.
    fn protocol(&self) -> ConnectionProtocol;

    /// Tries to dial (_create outgoing connection with_) the given peer address.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - The unique peer identifier of the torrent.
    /// * `peer_addr` - The address of the peer to dial.
    /// * `peer_port` - The peer port on which the torrent is listening for incoming connections.
    /// * `peer_client_name` - The client name of the peer.
    /// * `torrent` - The torrent to use for the connection.
    /// * `metadata` - The current known metadata of the torrent.
    /// * `data_pool` - The torrent data pool to use for the connection.
    /// * `storage` - The storage of the torrent.
    /// * `protocol_extensions` - The peer protocol extensions that should be enabled for the connection. (BEP4)
    /// * `extensions` - The peer extensions that should be enabled for the connection. (BEP10)
    /// * `connection_timeout` - The timeout of a peer connection.
    ///
    /// # Returns
    ///
    /// It returns a [TorrentPeer] if the connection was established.
    async fn dial(
        &self,
        peer_id: PeerId,
        peer_addr: SocketAddr,
        peer_port: Option<u16>,
        peer_client_name: String,
        torrent: InnerTorrent,
        metadata: TorrentMetadata,
        data_pool: DataPool,
        storage: Storage,
        protocol_extensions: ProtocolExtensionFlags,
        extensions: Vec<PeerExtension>,
        connection_timeout: Duration,
    ) -> Result<Peer>;

    /// Receive an incoming peer connection from the peer listener.
    ///
    /// # Returns
    ///
    /// It returns [None] when the listener has been dropped.
    async fn recv(&self) -> Option<PeerEntry>;

    /// Close the peer listener.
    /// This will prevent any new incoming connections from being received.
    fn close(&self);
}

#[cfg(test)]
pub mod mock {
    use super::*;

    use mockall::mock;

    mock! {
        #[derive(Debug)]
        pub Discovery {}

        #[async_trait]
        impl Discovery for Discovery {
            fn addr(&self) -> &SocketAddr;
            fn protocol(&self) -> ConnectionProtocol;
            async fn dial(
                &self,
                peer_id: PeerId,
                peer_addr: SocketAddr,
                peer_port: Option<u16>,
                peer_client_name: String,
                torrent: InnerTorrent,
                metadata: TorrentMetadata,
                data_pool: DataPool,
                storage: Storage,
                protocol_extensions: ProtocolExtensionFlags,
                extensions: Vec<PeerExtension>,
                connection_timeout: Duration,
            ) -> Result<Peer>;
            async fn recv(&self) -> Option<PeerEntry>;
            fn close(&self);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discovery_addr() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 6881));
        let mut discovery = MockDiscovery::new();
        discovery.expect_addr().times(1).return_const(addr);
        let discovery: PeerDiscovery = discovery.into();

        let result = discovery.addr();
        assert_eq!(&addr, result);
    }

    #[test]
    fn test_discovery_protocol() {
        let mut discovery = MockDiscovery::new();
        discovery
            .expect_protocol()
            .times(1)
            .return_const(ConnectionProtocol::Tcp);
        let discovery: PeerDiscovery = discovery.into();

        let result = discovery.protocol();
        assert_eq!(ConnectionProtocol::Tcp, result);
    }

    #[test]
    fn test_discovery_close() {
        let mut discovery = MockDiscovery::new();
        discovery.expect_close().times(1).return_const(());
        let discovery: PeerDiscovery = discovery.into();

        discovery.close();
    }
}
