pub use discovery::*;
pub use discovery_tcp::*;
pub use discovery_utp::*;
pub use errors::*;
pub use metrics::*;
pub use peer::*;
pub use peer_id::*;

mod discovery;
mod discovery_tcp;
mod discovery_utp;
mod errors;
pub mod extension;
mod metrics;
mod peer;
mod peer_connection;
mod peer_id;
mod protocol;
pub mod webseed;

#[cfg(test)]
pub mod tests {
    use super::*;

    use crate::peer::protocol::UtpSocket;
    use crate::peer::Peer;
    use crate::timeout;
    use crate::{PieceIndex, Torrent};

    use async_trait::async_trait;
    use bit_vec::BitVec;
    use fx_callback::{Callback, Subscriber, Subscription};
    use mockall::mock;
    use std::fmt::{Display, Formatter};
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::sync::mpsc::unbounded_channel;

    mock! {
        #[derive(Debug)]
        pub Peer {}

        #[async_trait]
        impl Peer for Peer {
            fn handle(&self) -> PeerHandle;
            fn handle_as_ref(&self) -> &PeerHandle;
            fn client(&self) -> PeerClientInfo;
            fn addr(&self) -> SocketAddr;
            fn addr_as_ref(&self) -> &SocketAddr;
            fn metrics(&self) -> &Metrics;
            async fn state(&self) -> PeerState;
            async fn is_seed(&self) -> bool;
            async fn remote_piece_bitfield(&self) -> BitVec;
            fn notify_piece_availability(&self, pieces: Vec<PieceIndex>);
            async fn close(&self);
        }

        impl Callback<PeerEvent> for Peer {
            fn subscribe(&self) -> Subscription<PeerEvent>;
            fn subscribe_with(&self, subscriber: Subscriber<PeerEvent>);
        }
    }

    impl Display for MockPeer {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockPeer")
        }
    }

    /// Create a new uTP socket.
    #[macro_export]
    macro_rules! create_utp_socket {
        () => {{
            create_utp_socket!(0)
        }};
        ($port:expr) => {{
            use crate::peer::protocol::UtpSocket;
            use core::net::{Ipv4Addr, SocketAddr};
            use core::time::Duration;

            let port: u16 = $port;

            UtpSocket::new(
                SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
                Duration::from_secs(1),
                vec![],
            )
            .await
            .expect("expected an utp socket")
        }};
    }

    /// Create a new uTP socket pair which don't overlap with port ranges.
    #[macro_export]
    macro_rules! create_utp_socket_pair {
        () => {{
            create_utp_socket_pair!(vec![], vec![])
        }};
        ($incoming_extensions:expr, $outgoing_extensions:expr) => {{
            use crate::peer::protocol::{UtpSocket, UtpSocketExtensions};
            use core::net::{Ipv4Addr, SocketAddr};
            use core::time::Duration;

            let incoming_extensions: UtpSocketExtensions = $incoming_extensions;
            let outgoing_extensions: UtpSocketExtensions = $outgoing_extensions;

            let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
            let left = UtpSocket::new(addr, Duration::from_secs(2), incoming_extensions)
                .await
                .expect("expected a new utp socket");

            let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
            let right = UtpSocket::new(addr, Duration::from_secs(2), outgoing_extensions)
                .await
                .expect("expected a new utp socket");

            (left, right)
        }};
    }

    /// Create a new uTP stream pair for the given incoming and outgoing sockets.
    ///
    /// Use `create_utp_socket_pair!()` to create a new uTP socket pair.
    #[macro_export]
    macro_rules! create_utp_stream_pair {
        ($incoming:expr, $outgoing:expr) => {{
            use crate::peer::protocol::UtpSocket;

            let incoming: &UtpSocket = $incoming;
            let outgoing: &UtpSocket = $outgoing;

            let target_addr = incoming.addr();
            let outgoing_stream = outgoing
                .connect(target_addr)
                .await
                .expect("expected an outgoing utp stream");
            let incoming_stream = incoming
                .recv()
                .await
                .expect("expected an incoming uTP stream");

            (incoming_stream, outgoing_stream)
        }};
    }

    pub async fn create_utp_peer_pair(
        incoming_socket: &UtpSocket,
        outgoing_socket: &UtpSocket,
        incoming_torrent: &Torrent,
        outgoing_torrent: &Torrent,
        protocols: ProtocolExtensionFlags,
    ) -> (BitTorrentPeer, BitTorrentPeer) {
        let incoming_context = incoming_torrent.instance().unwrap();
        let outgoing_context = outgoing_torrent.instance().unwrap();
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
        let incoming_extensions = incoming_context.extensions();
        let incoming_addr = outgoing_socket.addr();
        tokio::spawn(async move {
            let peer = BitTorrentPeer::new_inbound(
                PeerId::new(),
                incoming_addr,
                PeerStream::Utp(incoming_stream),
                incoming_context,
                protocols,
                incoming_extensions,
                Duration::from_secs(50),
            )
            .await
            .expect("expected an incoming uTP peer");
            tx.send(peer).unwrap();
        });

        let outgoing_extensions = outgoing_context.extensions();
        let outgoing_peer = BitTorrentPeer::new_outbound(
            PeerId::new(),
            incoming_socket.addr(),
            PeerStream::Utp(outgoing_stream),
            outgoing_context.clone(),
            protocols,
            outgoing_extensions,
            Duration::from_secs(50),
        )
        .await
        .expect("expected an outgoing uTP peer");

        let incoming_peer = timeout!(rx.recv(), Duration::from_secs(1)).unwrap();

        (incoming_peer, outgoing_peer)
    }

    pub async fn new_tcp_peer_discovery() -> Result<TcpPeerDiscovery> {
        TcpPeerDiscovery::new().await
    }
}
