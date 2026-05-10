/// Create a new UDP connection client-server instance pair.
macro_rules! udp_connection_pair {
    () => {{
        udp_connection_pair!(std::time::Duration::from_secs(1))
    }};
    ($timeout:expr) => {{
        use crate::tracker::TrackerHandle;
        use crate::tracker::TrackerListener;
        use crate::tracker::UdpConnection;
        use crate::tracker::UdpServer;
        use std::net::{Ipv4Addr, SocketAddr};
        use std::time::Duration;

        let server = UdpServer::with_port(0).await.unwrap();
        let server_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, server.addr().port()));
        let timeout: Duration = $timeout;
        let client = UdpConnection::new(TrackerHandle::new(), &[server_addr], timeout)
            .await
            .expect("expected the client to connect");

        (client, server)
    }};
}

/// Create a new UDP tracker client-server instance pair.
macro_rules! udp_tracker_pair {
    () => {{
        udp_tracker_pair!(std::time::Duration::from_secs(1))
    }};
    ($timeout:expr) => {{
        use crate::tracker::TrackerClient;
        use crate::tracker::TrackerEntry;
        use crate::tracker::TrackerServer;
        use crate::tracker::UdpServer;
        use std::time::Duration;

        let udp_server = UdpServer::with_port(0).await.unwrap();
        let server = TrackerServer::with_listeners(vec![Box::new(udp_server)]).unwrap();
        let timeout: Duration = $timeout;
        let client = TrackerClient::new(timeout);

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
