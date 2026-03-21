use crate::peer::{Peer, PeerId, PeerStream, ProtocolExtensionFlags, Result};
use crate::torrent::InnerTorrent;
use crate::torrent_data::DataPool;
use async_trait::async_trait;
#[cfg(test)]
pub use mock::*;
use std::fmt::Debug;
use std::net::SocketAddr;
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
            _ => false,
        }
    }
}

/// A peer discovery is responsible for discovering outgoing and incoming peer connections.
#[async_trait]
pub trait PeerDiscovery: Debug + Send + Sync {
    /// Get the address on which this peer listener is listening on.
    fn addr(&self) -> &SocketAddr;

    /// Get the port this peer listener is listening on.
    fn port(&self) -> u16;

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
        pub PeerDiscovery {}

        #[async_trait]
        impl PeerDiscovery for PeerDiscovery {
            fn addr(&self) -> &SocketAddr;
            fn port(&self) -> u16;
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
