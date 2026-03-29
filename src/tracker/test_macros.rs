/// Create a new UDP connection client-server instance pair.
macro_rules! udp_connection_pair {
    () => {{
        use crate::tracker::udp::UdpConnection;
        use crate::tracker::udp::UdpServer;
        use crate::tracker::TrackerHandle;
        use crate::tracker::TrackerListener;
        use std::net::{Ipv4Addr, SocketAddr};
        use std::time::Duration;

        let server = UdpServer::with_port(0).await.unwrap();
        let server_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, server.addr().port()));
        let client =
            UdpConnection::new(TrackerHandle::new(), &[server_addr], Duration::from_secs(1))
                .await
                .expect("expected the client to connect");

        (client, server)
    }};
}

/// Create a new UDP tracker client-server instance pair.
macro_rules! udp_tracker_pair {
    () => {{
        use crate::tracker::udp::UdpServer;
        use crate::tracker::TrackerClient;
        use crate::tracker::TrackerEntry;
        use crate::tracker::TrackerServer;
        use std::time::Duration;

        let udp_server = UdpServer::with_port(0).await.unwrap();
        let server = TrackerServer::with_listeners(vec![Box::new(udp_server)]).unwrap();
        let client = TrackerClient::new(Duration::from_secs(1));

        let client_handle = client
            .add_tracker_entry(TrackerEntry {
                tier: 0,
                url: server.url().clone(),
            })
            .await
            .expect("expected a client connection to be established");

        (client_handle, client, server)
    }};
}
