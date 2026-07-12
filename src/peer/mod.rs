pub use discovery::*;
pub use discovery_tcp::*;
pub use discovery_utp::*;
pub use error::*;
pub use metrics::*;
pub use peer::*;
pub use peer_id::*;
pub use protocol::CloseReason;
pub use types::*;

#[cfg(test)]
#[macro_use]
mod test_macros;

mod discovery;
mod discovery_tcp;
mod discovery_utp;
mod error;
pub mod extension;
mod metrics;
mod peer;
mod peer_connection;
mod peer_id;
mod protocol;
mod types;
pub mod webseed;

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::peer::protocol::UtpSocket;
    use crate::peer::Extension;
    use crate::{BitVec, PieceBlock, PieceIndex, Torrent};
    use async_trait::async_trait;
    use fx_callback::{Callback, Subscription};
    use mockall::mock;
    use std::fmt::{Display, Formatter};
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::sync::mpsc::unbounded_channel;

    mock! {
        #[derive(Debug)]
        pub Peer {}

        #[async_trait]
        impl Extension for Peer {
            fn handle(&self) -> &PeerHandle;
            fn addr(&self) -> &SocketAddr;
            fn client_info(&self) -> &PeerClientInfo;
            fn metrics(&self) -> &Metrics;
            async fn state(&self) -> PeerState;
            async fn is_seed(&self) -> bool;
            async fn remote_piece_bitfield(&self) -> BitVec;
            async fn remote_fast_bitfield(&self) -> BitVec;
            async fn set_choke_state(&self, state: ChokeState);
            async fn choke_state(&self) -> ChokeState;
            async fn remote_choke_state(&self) -> ChokeState;
            async fn remote_interest_state(&self) -> InterestState;
            async fn suggested_pieces(&self) -> Vec<PieceIndex>;
            async fn request(&self, blocks: &[PieceBlock]);
            async fn target_request_queue_len(&self) -> usize;
            async fn close(&self);
        }

        impl Callback<PeerEvent> for Peer {
            fn subscribe(&self) -> Subscription<PeerEvent>;
        }
    }

    impl Display for MockPeer {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockPeer")
        }
    }

    pub async fn create_utp_peer_pair(
        incoming_socket: &UtpSocket,
        outgoing_socket: &UtpSocket,
        incoming_torrent: &Torrent,
        outgoing_torrent: &Torrent,
        protocols: ProtocolExtensionFlags,
    ) -> (BitTorrentPeer, BitTorrentPeer) {
        let incoming_context = incoming_torrent.inner.clone();
        let incoming_data_pool = incoming_context.data_pool().await.unwrap();
        let incoming_storage = incoming_context.storage().await.unwrap();
        let outgoing_context = outgoing_torrent.inner.clone();
        let outgoing_data_pool = outgoing_context.data_pool().await.unwrap();
        let outgoing_storage = outgoing_context.storage().await.unwrap();
        let (tx, mut rx) = unbounded_channel();

        // create the uTP stream pair
        let outgoing_stream = outgoing_socket
            .connect(incoming_socket.addr())
            .await
            .expect("expected an outgoing utp stream");
        let incoming_stream = incoming_socket
            .recv()
            .await
            .expect("expected an incoming uTP stream");

        // create the incoming uTP peer handler thread
        let metadata = incoming_context.metadata().await.unwrap();
        let incoming_addr = outgoing_socket.addr();
        tokio::spawn(async move {
            let peer = BitTorrentPeer::new_inbound(
                PeerId::new(),
                incoming_addr,
                PeerStream::Utp(incoming_stream),
                incoming_context,
                metadata,
                incoming_data_pool,
                incoming_storage,
                protocols,
                vec![],
                Duration::from_secs(50),
            )
            .await
            .expect("expected an incoming uTP peer");
            tx.send(peer).unwrap();
        });

        let metadata = outgoing_context.metadata().await.unwrap();
        let outgoing_peer = BitTorrentPeer::new_outbound(
            PeerId::new(),
            incoming_socket.addr(),
            PeerStream::Utp(outgoing_stream),
            outgoing_context,
            metadata,
            outgoing_data_pool,
            outgoing_storage,
            protocols,
            vec![],
            Duration::from_secs(50),
        )
        .await
        .expect("expected an outgoing uTP peer");

        let incoming_peer = timeout!(Duration::from_secs(1), rx.recv()).unwrap();

        (incoming_peer, outgoing_peer)
    }

    pub async fn new_tcp_peer_discovery() -> Result<TcpPeerDiscovery> {
        TcpPeerDiscovery::new().await
    }
}
