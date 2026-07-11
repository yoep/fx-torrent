use crate::peer::extension::PeerExtension;
use crate::peer::{
    BitTorrentPeer, Error, Peer, PeerEntry, PeerId, PeerStream, ProtocolExtensionFlags, Result,
};
use crate::torrent::InnerTorrent;
use crate::torrent_data::DataPool;
use derive_more::Display;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use fx_handle::Handle;
use log::{debug, trace, warn};
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use tokio::{select, time};
use tokio_util::sync::CancellationToken;

/// The unique handle of an TCP peer discovery resource instance.
pub type TcpPeerDiscoveryHandle = Handle;

/// A peer dialer which establishes TCP peer connections.
#[derive(Debug, Clone)]
pub struct TcpPeerDiscovery {
    inner: Arc<InnerTcpPeerDiscovery>,
}

impl TcpPeerDiscovery {
    /// Create a new TCP peer discovery instance.
    ///
    /// It will listen on a random port assigned by the OS.
    /// If you want to listen on a specific port, use [TcpPeerDiscovery::with_port] instead.
    ///
    /// # Returns
    ///
    /// It returns a new TCP peer discovery instance, else an error when the listener couldn't be bound.
    pub async fn new() -> Result<Self> {
        Self::with_port(0).await
    }

    /// Create a new TCP peer discovery instance.
    ///
    /// It will listen on the given port.
    /// If the port is already in use, it will return [Error::Io].
    ///
    /// # Returns
    ///
    /// It returns a new TCP peer discovery instance, else an error when the listener couldn't be bound.
    pub async fn with_port(port: u16) -> Result<Self> {
        let (sender, receiver) = unbounded_channel();
        let sockets = InnerTcpPeerDiscovery::try_binding_sockets(port).await?;
        let addr = sockets
            .get(0)
            .map(|e| e.local_addr())
            .transpose()?
            .ok_or(Error::Io(io::Error::new(
                io::ErrorKind::Other,
                "unable to get bounded socket port",
            )))?;
        let inner = Arc::new(InnerTcpPeerDiscovery {
            handle: TcpPeerDiscoveryHandle::new(),
            addr,
            receiver: Mutex::new(receiver),
            cancellation_token: Default::default(),
        });

        let inner_loop = inner.clone();
        spawn!("TcpPeerDiscovery::run", async move {
            inner_loop.run(sender, sockets).await;
        });

        Ok(Self { inner })
    }

    /// Returns the address on which the discovery is listening on.
    pub fn addr(&self) -> &SocketAddr {
        &self.inner.addr
    }

    /// Try to dial the target peer address.
    pub async fn dial(
        &self,
        peer_id: PeerId,
        peer_addr: SocketAddr,
        torrent: InnerTorrent,
        data_pool: DataPool,
        protocol_extensions: ProtocolExtensionFlags,
        extensions: Vec<PeerExtension>,
        connection_timeout: Duration,
    ) -> Result<Peer> {
        select! {
            _ = time::sleep(connection_timeout) => {
                Err(Error::Io(io::Error::new(io::ErrorKind::TimedOut, format!("connection with {} timed out", peer_addr))))
            },
            stream = TcpStream::connect(&peer_addr) =>
                Self::create_peer_from_stream(
                    peer_id,
                    peer_addr,
                    stream?,
                    torrent,
                    data_pool,
                    protocol_extensions,
                    extensions,
                    connection_timeout
                ).await,
        }
    }

    /// Try to receive a new incoming peer entry from the discovery.
    pub async fn recv(&self) -> Option<PeerEntry> {
        self.inner.receiver.lock().await.recv().await
    }

    /// Close the peer discovery and stop accepting new connections.
    pub fn close(&self) {
        self.inner.cancellation_token.cancel();
    }

    /// Try to create a new BitTorrent peer from the given TCP stream.
    async fn create_peer_from_stream(
        peer_id: PeerId,
        peer_addr: SocketAddr,
        stream: TcpStream,
        torrent: InnerTorrent,
        data_pool: DataPool,
        protocol_extensions: ProtocolExtensionFlags,
        extensions: Vec<PeerExtension>,
        connection_timeout: Duration,
    ) -> Result<Peer> {
        Ok(BitTorrentPeer::new_outbound(
            peer_id,
            peer_addr,
            stream.into(),
            torrent,
            data_pool,
            protocol_extensions,
            extensions,
            connection_timeout,
        )
        .await?
        .into())
    }
}

impl Drop for TcpPeerDiscovery {
    fn drop(&mut self) {
        self.inner.cancellation_token.cancel();
    }
}

#[derive(Debug, Display)]
#[display("{} (port {})", handle, addr.port())]
struct InnerTcpPeerDiscovery {
    handle: TcpPeerDiscoveryHandle,
    addr: SocketAddr,
    receiver: Mutex<UnboundedReceiver<PeerEntry>>,
    cancellation_token: CancellationToken,
}

impl InnerTcpPeerDiscovery {
    /// Run the main loop of the tcp peer listener.
    async fn run(&self, sender: UnboundedSender<PeerEntry>, sockets: Vec<TcpListener>) {
        debug!(
            "TCP peer discovery {} started on port {}",
            self,
            self.addr.port()
        );
        let mut futures = FuturesUnordered::from_iter(
            sockets
                .into_iter()
                .map(|socket| self.accept_connections(socket, sender.clone())),
        )
        .fuse();
        loop {
            select! {
                _ = self.cancellation_token.cancelled() => break,
                Some(_) = futures.next() => {},
            }
        }
        debug!("TCP peer discovery {} has stopped", self);
    }

    async fn accept_connections(&self, socket: TcpListener, sender: UnboundedSender<PeerEntry>) {
        while let Ok((stream, socket_addr)) = socket.accept().await {
            trace!(
                "TCP peer discovery {} received connection from {}",
                self,
                socket_addr
            );
            if let Err(e) = sender.send(PeerEntry {
                socket_addr,
                stream: PeerStream::Tcp(stream),
            }) {
                warn!(
                    "TCP peer discovery {} failed to send peer connection, {}",
                    self, e
                );
                break;
            }
        }
    }

    async fn try_binding_sockets(mut port: u16) -> Result<Vec<TcpListener>> {
        let mut sockets = Vec::new();

        // attempt to bind the IPv6 address first in case dual stack is enabled
        let ipv6_addr = SocketAddr::from((Ipv6Addr::UNSPECIFIED, port));
        match TcpListener::bind(ipv6_addr).await {
            Ok(socket) => {
                trace!("Bounded TCP IPv6 listener on {}", ipv6_addr);
                port = socket.local_addr().map(|e| e.port())?; // update port in case it was zero
                sockets.push(socket);
            }
            Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
                return Err(Error::Io(e));
            }
            Err(e) => debug!("Failed to bind TCP IPv6 socket on {}, {}", ipv6_addr, e),
        }

        // then try binding to IPv4 on the same port
        let ipv4_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, port));
        match TcpListener::bind(ipv4_addr).await {
            Ok(socket) => {
                trace!("Bounded TCP IPv4 listener on {}", ipv4_addr);
                sockets.push(socket);
            }
            Err(e) => {
                if sockets.is_empty() {
                    return Err(Error::Io(e));
                }

                trace!("Failed to bind TCP IPv4 socket on {}, {}", ipv4_addr, e)
            }
        }

        Ok(sockets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::tests::new_tcp_peer_discovery;
    use crate::peer::PeerState;
    use crate::storage::MemoryStorage;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_tcp_discovery_dial() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let listener = TcpPeerDiscovery::new()
            .await
            .expect("expected a new tcp peer listener");
        let torrent = torrent!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            vec![listener.into()],
            |_| MemoryStorage::new().into(),
            None
        );
        let listener_port = torrent
            .peer_port()
            .await
            .expect("expected a torrent peer listener port");
        let protocol_extensions =
            timeout!(Duration::from_millis(100), torrent.protocol_extensions())
                .expect("expected the torrent protocol extensions");
        let dialer = TcpPeerDiscovery::new()
            .await
            .expect("expected a new tcp peer dialer");

        // try to create an outgoing peer connection through the dialer
        let data_pool = timeout!(
            Duration::from_millis(100),
            torrent.inner.data_pool(),
            "expected the torrent data pool"
        )
        .unwrap();
        let result = timeout!(
            Duration::from_millis(250),
            dialer.dial(
                PeerId::new(),
                SocketAddr::from((Ipv4Addr::LOCALHOST, listener_port)),
                torrent.inner.clone(),
                data_pool,
                protocol_extensions,
                vec![],
                Duration::from_secs(1),
            )
        )
        .expect("expected a tcp peer connection to have been established");

        // validate the state of the created outgoing peer connection
        let state = timeout!(
            Duration::from_millis(100),
            result.state(),
            "expected the peer state"
        );
        assert_ne!(PeerState::Error, state);

        // validate that the peer has been added to the torrent peer pool
        let total_peers = timeout!(
            Duration::from_millis(1500),
            torrent.active_peer_connections(),
            "expected the active peer connections"
        );
        assert_eq!(
            1, total_peers,
            "expected the connection to have been established with the torrent listener"
        );
    }

    #[tokio::test]
    async fn test_tcp_discovery_addr() {
        init_logger!();
        let listener = new_tcp_peer_discovery().await.unwrap();

        let result = listener.addr();

        assert_eq!(&listener.inner.addr, result);
    }

    #[tokio::test]
    async fn test_tcp_discovery_recv() {
        init_logger!();
        let (tx, mut rx) = unbounded_channel();
        let listener = new_tcp_peer_discovery().await.unwrap();
        let port = listener.addr().port();

        tokio::spawn(async move {
            if let Some(entry) = listener.recv().await {
                tx.send(entry).unwrap();
            }
        });

        let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
        TcpStream::connect(addr)
            .await
            .expect("expected the connection to succeed");

        let result = timeout!(
            Duration::from_millis(200),
            rx.recv(),
            "expected to receive an incoming connection"
        )
        .unwrap();

        if let PeerStream::Tcp(_) = result.stream {
        } else {
            assert!(
                false,
                "expected PeerStream::Tcp, but got {:?} instead",
                result
            );
        }
    }

    #[tokio::test]
    async fn test_tcp_discovery_drop() {
        init_logger!();
        let listener = new_tcp_peer_discovery().await.unwrap();
        let addr: SocketAddr = (Ipv4Addr::LOCALHOST, listener.addr().port()).into();

        drop(listener);
        time::sleep(Duration::from_millis(100)).await;

        let result = TcpStream::connect(addr).await;
        match result {
            Err(e) => {
                assert_eq!(io::ErrorKind::ConnectionRefused, e.kind());
            }
            Ok(_) => assert!(false, "expected the peer listener to have been closed"),
        }
    }
}
