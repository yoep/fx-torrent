/// Create a new uTP socket.
macro_rules! create_utp_socket {
    () => {{
        create_utp_socket!(0)
    }};
    ($port:expr) => {{
        use crate::peer::protocol::UtpSocket;
        use core::net::{Ipv4Addr, SocketAddr};

        let port: u16 = $port;

        UtpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, port)), vec![])
            .await
            .expect("expected an utp socket")
    }};
}

/// Create a new uTP socket pair which don't overlap with port ranges.
macro_rules! create_utp_socket_pair {
    () => {{
        create_utp_socket_pair!(vec![], vec![])
    }};
    ($incoming_extensions:expr, $outgoing_extensions:expr) => {{
        use crate::peer::protocol::{UtpSocket, UtpSocketExtensions};
        use core::net::{Ipv4Addr, SocketAddr};

        let incoming_extensions: UtpSocketExtensions = $incoming_extensions;
        let outgoing_extensions: UtpSocketExtensions = $outgoing_extensions;

        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let left = UtpSocket::bind(addr, incoming_extensions)
            .await
            .expect("expected a new utp socket");

        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let right = UtpSocket::bind(addr, outgoing_extensions)
            .await
            .expect("expected a new utp socket");

        (left, right)
    }};
}

/// Create a new uTP stream pair for the given incoming and outgoing sockets.
///
/// Use `create_utp_socket_pair!()` to create a new uTP socket pair.
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

/// Create a new peer context instance.
macro_rules! peer_context_pair {
    ($torrent:expr, $extensions:expr) => {{
        use crate::peer::extension::PeerExtension;
        use crate::InnerTorrent;

        let torrent: &InnerTorrent = $torrent;
        let extensions: &[PeerExtension] = $extensions;

        peer_context_pair!(torrent, torrent, extensions, extensions)
    }};
    ($incoming:expr, $outgoing:expr, $incoming_extensions:expr, $outgoing_Extensions:expr) => {{
        use crate::peer::extension::PeerExtension;
        use crate::peer::peer_connection::PeerConnection;
        use crate::peer::peer_context::PeerContext;
        use crate::peer::BitTorrentPeerContext;
        use crate::peer::ConnectionDirection;
        use crate::peer::ConnectionProtocol;
        use crate::peer::Metrics;
        use crate::peer::PeerId;
        use crate::InnerTorrent;
        use std::net::{Ipv4Addr, SocketAddr};
        use std::time::Duration;
        use tokio::net::TcpListener;
        use tokio::net::TcpStream;
        use tokio::sync::oneshot;

        let incoming_torrent: &InnerTorrent = $incoming;
        let incoming_extensions: &[PeerExtension] = $incoming_extensions;
        let outgoing_torrent: &InnerTorrent = $outgoing;
        let outgoing_extensions: &[PeerExtension] = $outgoing_Extensions;

        let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .unwrap();
        let listener_addr = listener.local_addr().unwrap();

        // wait for the incoming connection on a separate task
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = tx.send(listener.accept().await.unwrap());
        });

        // define the shared peer data
        let incoming_peer_id = PeerId::new();
        let outgoing_peer_id = PeerId::new();
        let incoming_metrics = Metrics::new();
        let outgoing_metrics = Metrics::new();

        let outgoing = TcpStream::connect(listener_addr).await.unwrap();
        let (incoming, incoming_addr) = rx.await.unwrap();
        (
            BitTorrentPeerContext::new(
                PeerContext::builder()
                    .id(incoming_peer_id)
                    .addr(incoming_addr)
                    .connection_type(ConnectionDirection::Inbound)
                    .protocol(ConnectionProtocol::Tcp)
                    .metrics(incoming_metrics.clone())
                    .build(),
                incoming_torrent.peer_port().await,
                incoming_torrent
                    .config()
                    .await
                    .unwrap()
                    .client_name()
                    .to_string(),
                PeerConnection::new_tcp(
                    incoming_peer_id,
                    incoming_addr,
                    incoming,
                    incoming_metrics,
                ),
                incoming_torrent.clone(),
                incoming_torrent.metadata().await.unwrap(),
                incoming_torrent.data_pool().await.unwrap(),
                incoming_torrent.storage().await.unwrap(),
                incoming_torrent.protocol_extensions().await.unwrap(),
                incoming_extensions,
                Duration::from_secs(1),
            )
            .await
            .expect("expected a peer context for the incoming connection"),
            BitTorrentPeerContext::new(
                PeerContext::builder()
                    .id(outgoing_peer_id)
                    .addr(listener_addr)
                    .connection_type(ConnectionDirection::Outbound)
                    .protocol(ConnectionProtocol::Tcp)
                    .metrics(outgoing_metrics.clone())
                    .build(),
                outgoing_torrent.peer_port().await,
                outgoing_torrent
                    .config()
                    .await
                    .unwrap()
                    .client_name()
                    .to_string(),
                PeerConnection::new_tcp(
                    outgoing_peer_id,
                    listener_addr,
                    outgoing,
                    outgoing_metrics,
                ),
                outgoing_torrent.clone(),
                outgoing_torrent.metadata().await.unwrap(),
                outgoing_torrent.data_pool().await.unwrap(),
                outgoing_torrent.storage().await.unwrap(),
                outgoing_torrent.protocol_extensions().await.unwrap(),
                outgoing_extensions,
                Duration::from_secs(1),
            )
            .await
            .expect("expected a peer context for the outgoing connection"),
        )
    }};
}
