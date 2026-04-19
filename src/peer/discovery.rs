use crate::peer::{
    Peer, PeerId, PeerStream, ProtocolExtensionFlags, Result, TcpPeerDiscovery, UtpPeerDiscovery,
};
use crate::torrent::InnerTorrent;
use crate::torrent_data::DataPool;
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

    /// Try to dial (_create outgoing connection with_) to the target peer address.
    pub async fn dial(
        &self,
        peer_id: PeerId,
        peer_addr: SocketAddr,
        torrent: InnerTorrent,
        data_pool: DataPool,
        protocol_extensions: ProtocolExtensionFlags,
        connection_timeout: Duration,
    ) -> Result<Box<dyn Peer>> {
        match self {
            PeerDiscovery::Tcp(discovery) => {
                discovery
                    .dial(
                        peer_id,
                        peer_addr,
                        torrent,
                        data_pool,
                        protocol_extensions,
                        connection_timeout,
                    )
                    .await
            }
            PeerDiscovery::Utp(discovery) => {
                discovery
                    .dial(
                        peer_id,
                        peer_addr,
                        torrent,
                        data_pool,
                        protocol_extensions,
                        connection_timeout,
                    )
                    .await
            }
            PeerDiscovery::Other(discovery) => {
                discovery
                    .dial(
                        peer_id,
                        peer_addr,
                        torrent,
                        data_pool,
                        protocol_extensions,
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

impl From<Box<dyn Discovery>> for PeerDiscovery {
    fn from(discovery: Box<dyn Discovery>) -> Self {
        Self::Other(Arc::from(discovery))
    }
}

/// A peer discovery is responsible for discovering outgoing and incoming peer connections.
#[async_trait]
pub trait Discovery: Debug + Send + Sync {
    /// Get the address on which this peer listener is listening on.
    fn addr(&self) -> &SocketAddr;

    /// Tries to dial (_create outgoing connection with_) the given peer address.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - The unique peer identifier of the torrent.
    /// * `peer_addr` - The address of the peer to dial.
    /// * `torrent` - The torrent to use for the connection.
    /// * `data_pool` - The torrent data pool to use for the connection.
    /// * `protocol_extensions` - The peer protocol extensions that should be enabled for the connection. (BEP4)
    /// * `connection_timeout` - The timeout of a peer connection.
    ///
    /// # Returns
    ///
    /// It returns a [Peer] if the connection was established.
    async fn dial(
        &self,
        peer_id: PeerId,
        peer_addr: SocketAddr,
        torrent: InnerTorrent,
        data_pool: DataPool,
        protocol_extensions: ProtocolExtensionFlags,
        connection_timeout: Duration,
    ) -> Result<Box<dyn Peer>>;

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
            async fn dial(
                &self,
                peer_id: PeerId,
                peer_addr: SocketAddr,
                torrent: InnerTorrent,
                data_pool: DataPool,
                protocol_extensions: ProtocolExtensionFlags,
                connection_timeout: Duration,
            ) -> Result<Box<dyn Peer>>;
            async fn recv(&self) -> Option<PeerEntry>;
            fn close(&self);
        }
    }
}
