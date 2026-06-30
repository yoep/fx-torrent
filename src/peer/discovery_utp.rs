use crate::peer::protocol::{UtpSocket, UtpStream};
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
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use tokio::{select, time};
use tokio_util::sync::CancellationToken;

/// The unique handle of an uTP peer discovery resource instance.
pub type UtpPeerDiscoveryHandle = Handle;

#[derive(Debug, Clone)]
pub struct UtpPeerDiscovery {
    inner: Arc<InnerUtpPeerDiscovery>,
}

impl UtpPeerDiscovery {
    /// Create a new uTP peer discovery instance.
    ///
    /// It will listen on a random port assigned by the OS.
    /// If you want to listen on a specific port, use [UtpPeerDiscovery::with_port] instead.
    ///
    /// # Returns
    ///
    /// It returns a new uTP peer discovery instance, else an error when the listener couldn't be bound.
    pub async fn new() -> Result<Self> {
        Self::with_port(0).await
    }

    /// Create a new uTP peer discovery instance.
    ///
    /// It will listen on the given port.
    /// If the port is already in use, it will return [Error::Io].
    ///
    /// # Returns
    ///
    /// It returns a new uTP peer discovery instance, else an error when the listener couldn't be bound.
    pub async fn with_port(port: u16) -> Result<Self> {
        let (sender, receiver) = unbounded_channel();
        let cancellation_token = CancellationToken::new();
        let sockets =
            InnerUtpPeerDiscovery::try_binding_sockets(port, Duration::from_secs(6)).await?;
        let addr = sockets
            .get(0)
            .map(|e| e.addr())
            .ok_or(Error::Io(io::Error::new(
                io::ErrorKind::Other,
                "unable to get bounded socket port",
            )))?;
        let inner = Arc::new(InnerUtpPeerDiscovery {
            handle: Default::default(),
            addr,
            sockets,
            receiver: Mutex::new(receiver),
            connection_timeout: Duration::from_secs(6),
            cancellation_token,
        });

        let inner_main_loop = inner.clone();
        spawn!("UtpPeerDiscovery::run", async move {
            inner_main_loop.run(sender).await;
        });

        Ok(Self { inner })
    }

    /// Returns the address on which the discovery is listening on.
    pub fn addr(&self) -> &SocketAddr {
        &self.inner.addr
    }

    /// Try to dial the peer target address.
    pub async fn dial(
        &self,
        peer_id: PeerId,
        peer_addr: SocketAddr,
        torrent: InnerTorrent,
        data_pool: DataPool,
        protocol_extensions: ProtocolExtensionFlags,
        connection_timeout: Duration,
    ) -> Result<Peer> {
        let socket = self
            .inner
            .sockets
            .iter()
            .find(|e| e.addr().is_ipv4() == peer_addr.is_ipv4());

        if let Some(socket) = socket {
            let stream = socket.connect(peer_addr).await?;

            return Ok(BitTorrentPeer::new_outbound(
                peer_id,
                peer_addr,
                stream.into(),
                torrent,
                data_pool,
                protocol_extensions,
                connection_timeout,
            )
            .await?
            .into());
        }

        Err(Error::Io(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "unable to connect to \"{}\", no compatible uTP socket available",
                peer_addr
            ),
        )))
    }

    /// Try to receive a new incoming peer entry from the discovery.
    pub async fn recv(&self) -> Option<PeerEntry> {
        let mut receiver = self.inner.receiver.lock().await;
        match receiver.recv().await {
            None => None,
            Some(stream) => Some(PeerEntry {
                socket_addr: stream.addr(),
                stream: PeerStream::Utp(stream),
            }),
        }
    }

    /// Close the peer discovery and stop accepting new connections.
    pub fn close(&self) {
        self.inner.cancellation_token.cancel();
    }
}

impl Drop for UtpPeerDiscovery {
    fn drop(&mut self) {
        self.close();
    }
}

#[derive(Debug, Display)]
#[display("{} (port {})", handle, addr.port())]
struct InnerUtpPeerDiscovery {
    handle: UtpPeerDiscoveryHandle,
    addr: SocketAddr,
    sockets: Vec<UtpSocket>,
    receiver: Mutex<UnboundedReceiver<UtpStream>>,
    connection_timeout: Duration,
    cancellation_token: CancellationToken,
}

impl InnerUtpPeerDiscovery {
    /// Run the main loop of the utp peer discovery.
    async fn run(&self, sender: UnboundedSender<UtpStream>) {
        debug!(
            "UTP peer discovery {} started on port {}",
            self,
            self.addr.port()
        );
        let mut futures = FuturesUnordered::from_iter(
            self.sockets
                .iter()
                .map(|socket| self.accept_connections(socket, sender.clone())),
        )
        .fuse();
        loop {
            select! {
                _ = self.cancellation_token.cancelled() => break,
                Some(_) = futures.next() => {},
            }
        }

        debug!("UTP peer discovery {} main loop ended", self);
    }

    async fn accept_connections(&self, socket: &UtpSocket, sender: UnboundedSender<UtpStream>) {
        while let Some(stream) = socket.recv().await {
            if let Err(e) = sender.send(stream) {
                warn!(
                    "UTP peer discovery {} failed to send peer connection, {}",
                    self, e
                );
                break;
            }
        }
    }

    #[allow(unused_assignments)]
    async fn try_binding_sockets(mut port: u16, timeout: Duration) -> Result<Vec<UtpSocket>> {
        let mut sockets = Vec::new();
        let addrs = vec![
            // dual stack support is currently not configurable with tokio UdpSocket
            // therefore, we currently only support IPv4
            // SocketAddr::from((Ipv6Addr::UNSPECIFIED, port)),
            SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)),
        ];

        for addr in addrs {
            let socket = select! {
                _ = time::sleep(timeout) => return Err(Error::Io(
                    io::Error::new(io::ErrorKind::TimedOut, "timed out while binding uTP socket"),
                )),
                result = UtpSocket::bind(addr, vec![]) => result,
            };
            match socket {
                Ok(socket) => {
                    trace!("Created uTP listener on {}", addr);
                    port = socket.addr().port();
                    sockets.push(socket);
                }
                Err(e) => {
                    if sockets.is_empty() {
                        return Err(e);
                    }

                    trace!("Failed to bind uTP socket on {}, {}", addr, e)
                }
            }
        }

        Ok(sockets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_utp_discovery_new() {
        init_logger!();

        let utp_discovery = UtpPeerDiscovery::new().await;
        assert_eq!(
            true,
            utp_discovery.is_ok(),
            "expected an utp listener, got {:?} instead",
            utp_discovery
        );

        let result = utp_discovery.unwrap();
        assert_ne!(
            0,
            result.addr().port(),
            "expected a port number to have been assigned"
        );
    }

    #[tokio::test]
    async fn test_utp_discovery_dial() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let listener = UtpPeerDiscovery::new()
            .await
            .expect("expected a new utp peer listener");
        let port = listener.addr().port();
        let torrent = torrent!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            vec![listener.clone().into()]
        );
        let protocol_extensions = torrent.protocol_extensions().await.unwrap();

        let dialer = UtpPeerDiscovery::new()
            .await
            .expect("expected a new utp peer dialer");
        dialer
            .dial(
                PeerId::new(),
                SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
                torrent.inner.clone(),
                torrent.inner.data_pool().await.unwrap(),
                protocol_extensions,
                Duration::from_secs(2),
            )
            .await
            .expect("expected an utp connection to be established");
    }
}
