/// Create a new uTP socket.
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
