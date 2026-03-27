use crate::{InfoHash, Result, TorrentError};
use derive_more::Display;
use fx_callback::{Callback, MultiThreadedCallback, Subscription};
use itertools::Itertools;
use log::{debug, trace, warn};
use rand::{rng, RngExt};
use socket2::{Domain, Protocol, Socket, Type};
use std::io;
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::select;
use tokio_util::sync::CancellationToken;

const MULTICAST_IPV4: Ipv4Addr = Ipv4Addr::new(239, 192, 152, 143);
const MULTICAST_IPV6: &str = "ff15::efc0:988f";
const LSD_METHOD: &str = "BT-SEARCH";
const LSD_PORT: u16 = 6771;

#[derive(Debug)]
pub enum LocalServiceDiscoveryEvent {
    /// Invoked when a peer is discovered for the given info hash.
    PeerDiscovered(InfoHash, SocketAddr),
    /// Invoked when the local service discovery is closed.
    Closed,
}

/// The local service discovery (BEP14) used for finding peers.
#[derive(Debug, Clone)]
pub struct LocalServiceDiscovery {
    inner: Arc<InnerLocalServiceDiscovery>,
}

impl LocalServiceDiscovery {
    /// Create a new local service discovery instance.
    /// The instance will join the multicast group of `listen_address`.
    pub async fn new(listen_address: IpAddr) -> Result<Self> {
        let socket = Self::bind_socket(listen_address).await?;
        let inner = Arc::new(InnerLocalServiceDiscovery {
            socket,
            listen_address,
            cookie: rng().random(),
            callbacks: MultiThreadedCallback::new(),
            cancellation_token: Default::default(),
        });

        let inner_main = inner.clone();
        tokio::spawn(async move { inner_main.run().await });

        Ok(Self { inner })
    }

    /// Announce a torrent to the local network.
    pub async fn announce(&self, info_hash: &InfoHash, port: u16) {
        self.inner.announce(info_hash, port).await;
    }

    /// Returns `true` if the local service discovery is closed and no longer accepts any messages.
    pub fn is_closed(&self) -> bool {
        self.inner.cancellation_token.is_cancelled()
    }

    /// Close the local service discovery.
    pub fn close(&self) {
        self.inner.cancellation_token.cancel();
    }

    /// Try to bind a new socket for the local service discovery.
    /// The socket will try to join the broadcast group of `listen_address`.
    async fn bind_socket(listen_address: IpAddr) -> Result<UdpSocket> {
        let (domain, bind_addr): (Domain, IpAddr) = match listen_address {
            IpAddr::V4(_) => (Domain::IPV4, Ipv4Addr::UNSPECIFIED.into()),
            IpAddr::V6(_) => (Domain::IPV6, Ipv6Addr::UNSPECIFIED.into()),
        };
        let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_reuse_address(true)?;
        #[cfg(not(any(
            target_os = "windows",
            target_os = "solaris",
            target_os = "illumos",
            target_os = "cygwin",
            target_os = "wasi"
        )))]
        socket.set_reuse_port(true)?;
        socket.set_nonblocking(true)?;
        socket.set_broadcast(true)?;
        socket.bind(&SocketAddr::from((bind_addr, LSD_PORT)).into())?;
        let socket = UdpSocket::from_std(Into::<std::net::UdpSocket>::into(socket))?;
        trace!("Local service discovery bound to {}", socket.local_addr()?);

        // setup loopback
        let result = match listen_address {
            IpAddr::V4(_) => socket.set_multicast_loop_v4(true),
            IpAddr::V6(_) => socket.set_multicast_loop_v6(true),
        };
        match result {
            Ok(_) => (),
            Err(e) => debug!("Local service discovery failed to set loopback, {}", e),
        }

        // setup multicast
        match listen_address {
            IpAddr::V4(addr) => socket.join_multicast_v4(MULTICAST_IPV4, addr)?,
            IpAddr::V6(_) => socket.join_multicast_v6(
                &MULTICAST_IPV6
                    .parse::<Ipv6Addr>()
                    .map_err(|e| TorrentError::AddressParse(e.to_string()))?,
                0,
            )?,
        };

        Ok(socket)
    }
}

impl Callback<LocalServiceDiscoveryEvent> for LocalServiceDiscovery {
    fn subscribe(&self) -> Subscription<LocalServiceDiscoveryEvent> {
        self.inner.callbacks.subscribe()
    }
}

impl Drop for LocalServiceDiscovery {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) <= 2 {
            self.inner.cancellation_token.cancel();
        }
    }
}

#[derive(Debug, Display)]
#[display("{}", self.listen_address)]
struct InnerLocalServiceDiscovery {
    socket: UdpSocket,
    listen_address: IpAddr,
    cookie: u32,
    callbacks: MultiThreadedCallback<LocalServiceDiscoveryEvent>,
    cancellation_token: CancellationToken,
}

impl InnerLocalServiceDiscovery {
    /// Run the main loop of the local service discovery.
    async fn run(&self) {
        loop {
            let mut buffer = [0u8; 1500];
            select! {
                _ = self.cancellation_token.cancelled() => break,
                Ok((len, addr)) = self.socket.recv_from(&mut buffer) => self.on_packet(&buffer[..len], addr),
            }
        }

        self.callbacks.invoke(LocalServiceDiscoveryEvent::Closed);
        debug!("Local service discovery ({}) main loop ended", self);
    }

    /// Handle a received packet.
    fn on_packet(&self, buffer: &[u8], addr: SocketAddr) {
        trace!(
            "Local service discovery ({}) received packet from {}",
            self,
            addr
        );
        let msg = match str::from_utf8(buffer) {
            Ok(msg) => msg,
            Err(e) => {
                debug!(
                    "Local service discovery ({}) failed to parse packet, {}",
                    self, e
                );
                return;
            }
        };
        // check if the received packet is a bittorrent search
        // if not, ignore the packet
        if !msg.lines().next().map_or(false, |l| l.contains(LSD_METHOD)) {
            return;
        }

        // try to parse the message
        trace!(
            "Local service discovery ({}) is parsing message from {}",
            self,
            addr
        );
        let message = match Message::from_str(msg) {
            Ok(message) => message,
            Err(e) => {
                debug!(
                    "Local service discovery ({}) failed to parse message, {}",
                    self, e
                );
                return;
            }
        };

        // check if the received packet is our own packet
        if message.cookie == self.cookie {
            trace!(
                "Local service discovery ({}) received its own packet, skipping",
                self
            );
            return;
        }

        debug!(
            "Local service discovery ({}) received {} announce(s) from {}",
            self,
            message.info_hashes.len(),
            addr
        );
        let peer_addr = SocketAddr::new(addr.ip(), message.port);
        for info_hash in message.info_hashes {
            self.callbacks
                .invoke(LocalServiceDiscoveryEvent::PeerDiscovered(
                    info_hash,
                    peer_addr.clone(),
                ));
        }
    }

    async fn announce(&self, info_hash: &InfoHash, port: u16) {
        let target_addr = match self.listen_address {
            IpAddr::V4(_) => SocketAddr::new(MULTICAST_IPV4.into(), LSD_PORT),
            IpAddr::V6(_) => match MULTICAST_IPV6.parse::<Ipv6Addr>() {
                Ok(addr) => SocketAddr::new(addr.into(), LSD_PORT),
                Err(_) => unreachable!(),
            },
        };
        let message = Message {
            host: target_addr,
            port,
            info_hashes: vec![info_hash.clone()],
            cookie: self.cookie,
        };
        let bytes: Vec<u8> = message.into();

        match self.socket.send_to(bytes.as_ref(), target_addr).await {
            Ok(_) => debug!(
                "Local service discovery ({}) sent announce for {}",
                self, info_hash
            ),
            Err(e) => warn!(
                "Local service discovery ({}) failed to send message, {}",
                self, e
            ),
        }
    }
}

/// The local service discovery message used to make announcements.
#[derive(Debug)]
struct Message {
    host: SocketAddr,
    port: u16,
    info_hashes: Vec<InfoHash>,
    cookie: u32,
}

impl FromStr for Message {
    type Err = TorrentError;

    fn from_str(msg: &str) -> Result<Self> {
        let mut host: Option<SocketAddr> = None;
        let mut port: Option<u16> = None;
        let mut cookie: Option<u32> = None;
        let mut info_hashes: Vec<InfoHash> = vec![];

        // parse the packet lines
        for line in msg.lines() {
            if line.is_empty() {
                break;
            }
            // try to split the line into key and value
            let parts = line.splitn(2, ':').collect::<Vec<_>>();
            if parts.len() != 2 {
                continue;
            }
            let key = parts[0].trim().to_lowercase();
            let value = parts[1].trim();

            match key.as_str() {
                "host" => host = value.parse().ok(),
                "port" => port = value.parse().ok(),
                "cookie" => cookie = u32::from_str_radix(value, 16).ok(),
                "infohash" => {
                    match hex::decode(value.as_bytes())
                        .map_err(|e| TorrentError::InvalidInfoHash(e.to_string()))
                        .and_then(|bytes| InfoHash::try_from_bytes(&bytes))
                    {
                        Ok(hash) => info_hashes.push(hash),
                        Err(e) => {
                            debug!("Local service discovery failed to parse info hash: {}", e)
                        }
                    }
                }
                _ => continue,
            }
        }

        let port = port.ok_or(TorrentError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "port field is missing",
        )))?;

        Ok(Message {
            host: host.unwrap_or(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), LSD_PORT)),
            port,
            info_hashes,
            cookie: cookie.unwrap_or(0),
        })
    }
}

impl From<Message> for Vec<u8> {
    fn from(value: Message) -> Self {
        let mut buffer = vec![];
        let encoded_info_hashes = value
            .info_hashes
            .iter()
            .map(|info_hash| hex::encode(info_hash.short_info_hash_bytes()))
            .collect_vec();

        // write the header
        let _ = buffer.write_all(format!("{} * HTTP/1.1\r\n", LSD_METHOD).as_bytes());
        // write the host
        let _ = buffer.write_all(format!("Host: {}\r\n", value.host).as_bytes());
        // write the port
        let _ = buffer.write_all(format!("Port: {}\r\n", value.port).as_bytes());
        // write the info hashes
        for info_hash in encoded_info_hashes {
            let _ = buffer.write_all(format!("Infohash: {}\r\n", info_hash).as_bytes());
        }
        // write the cookie
        let _ = buffer.write_all(format!("cookie: {}\r\n\r\n\r\n", value.cookie).as_bytes());

        buffer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod new {
        use super::*;

        #[tokio::test]
        async fn test_broadcasting() {
            init_logger!();
            let service = LocalServiceDiscovery::new(Ipv4Addr::LOCALHOST.into())
                .await
                .unwrap();

            let result = service.inner.socket.broadcast().unwrap();

            assert_eq!(true, result, "expected the socket to be broadcasting");
        }
    }

    mod close {
        use super::*;

        #[tokio::test]
        async fn test_invoke_close_event() {
            init_logger!();
            let service = LocalServiceDiscovery::new(Ipv4Addr::LOCALHOST.into())
                .await
                .unwrap();

            let mut receiver = service.subscribe();

            service.close();

            let result = timeout!(receiver.recv(), Duration::from_millis(250))
                .expect("expected to receive an event");
            match &*result {
                LocalServiceDiscoveryEvent::Closed => (),
                _ => assert!(
                    false,
                    "expected LocalServiceDiscoveryEvent::Closed, but got {:?}",
                    result
                ),
            }
        }
    }

    mod announce {
        use super::*;

        #[tokio::test]
        async fn test_announce() {
            init_logger!();
            let info_hash = InfoHash::from_str("A1DFEFEC1A9DD7FA8A041EBEEEA271DB55126D2F").unwrap();
            let source = LocalServiceDiscovery::new(Ipv4Addr::LOCALHOST.into())
                .await
                .unwrap();

            // announce the torrent
            source.announce(&info_hash, 6881).await;

            // receiving broadcast messages on the loopback address is OS specific
            // so we can't test it here
        }
    }

    mod on_packet {
        use super::*;

        #[tokio::test]
        async fn test_on_announce_packet() {
            init_logger!();
            let expected_info_hash =
                InfoHash::from_str("A1DFEFEC1A9DD7FA8A041EBEEEA271DB55126D2F").unwrap();
            let cookie: u32 = rng().random();
            let packet = format!(
                r#"BT-SEARCH * HTTP/1.1
Host: 239.192.152.143:6771
Port: 9900
Infohash: {}
cookie: {}


"#,
                expected_info_hash.v1_as_str().unwrap(),
                cookie
            );
            let service = LocalServiceDiscovery::new(Ipv4Addr::LOCALHOST.into())
                .await
                .unwrap();

            // subscribe to the target events
            let mut receiver = service.subscribe();

            // process the packet
            service.inner.on_packet(
                packet.as_bytes(),
                SocketAddr::from((Ipv4Addr::LOCALHOST, 6771)),
            );

            // wait for the packet event to be received
            let result = timeout!(receiver.recv(), Duration::from_millis(250))
                .expect("expected to receive an event");
            match &*result {
                LocalServiceDiscoveryEvent::PeerDiscovered(info_hash, addr) => {
                    assert_eq!(
                        &expected_info_hash, info_hash,
                        "expected the info hash to match"
                    );
                    assert_eq!(
                        SocketAddr::from((Ipv4Addr::LOCALHOST, 9900)),
                        *addr,
                        "expected the address to match"
                    );
                }
                _ => assert!(
                    false,
                    "expected LocalServiceDiscoveryEvent::PeerDiscovered, but got {:?}",
                    result
                ),
            }
        }
    }
}
