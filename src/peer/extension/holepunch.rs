use crate::bencode;
use crate::channel::Reply;
use crate::peer::extension::{Error, ExtensionNumber, Result};
use crate::peer::protocol::Message;
use crate::peer::{Peer, PeerContext};
use log::{debug, trace, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::{io, result};

/// The BEP55 holepunch extension message.
#[derive(Debug, Serialize, Deserialize)]
struct HolepunchMessage {
    /// Type of the holepunch message
    #[serde(rename = "msg_type")]
    message_type: MessageType,
    /// IP address family type
    addr_type: AddrType,
    #[serde(with = "serde_bytes")]
    addr: Vec<u8>,
    port: u16,
    #[serde(default, rename = "err_code", skip_serializing_if = "Option::is_none")]
    err_code: Option<ErrorCode>,
}

impl HolepunchMessage {
    /// Try to parse the target address given in the message.
    fn addr(&self) -> Result<SocketAddr> {
        match self.addr_type {
            AddrType::Ipv4 => {
                let octets: [u8; 4] = self.addr.as_slice().try_into().map_err(|_| {
                    Error::Parsing(format!("invalid Ipv4Addr, got {} bytes", self.addr.len()))
                })?;
                Ok(SocketAddr::new(
                    Ipv4Addr::from_octets(octets).into(),
                    self.port,
                ))
            }
            AddrType::Ipv6 => {
                let octets: [u8; 16] = self.addr.as_slice().try_into().map_err(|_| {
                    Error::Parsing(format!("invalid Ipv6Addr, got {} bytes", self.addr.len()))
                })?;
                Ok(SocketAddr::new(
                    Ipv6Addr::from_octets(octets).into(),
                    self.port,
                ))
            }
        }
    }
}

#[repr(u8)]
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(into = "u8", try_from = "u8")]
enum MessageType {
    Rendezvous = 0,
    Connect = 1,
    Error = 2,
}

impl From<MessageType> for u8 {
    fn from(value: MessageType) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for MessageType {
    type Error = String;

    fn try_from(value: u8) -> result::Result<Self, String> {
        match value {
            0 => Ok(MessageType::Rendezvous),
            1 => Ok(MessageType::Connect),
            2 => Ok(MessageType::Error),
            _ => Err(format!("Invalid MessageType {}", value)),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(into = "u8", try_from = "u8")]
enum AddrType {
    Ipv4 = 0,
    Ipv6 = 1,
}

impl From<AddrType> for u8 {
    fn from(value: AddrType) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for AddrType {
    type Error = String;

    fn try_from(value: u8) -> result::Result<Self, String> {
        match value {
            0 => Ok(AddrType::Ipv4),
            1 => Ok(AddrType::Ipv6),
            _ => Err(format!("Invalid AddrType {}", value)),
        }
    }
}

#[repr(u32)]
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(into = "u32", try_from = "u32")]
enum ErrorCode {
    /// The target endpoint is invalid.
    NoSuchPeer = 1,
    /// The relaying peer is not connected to the target peer.
    NotConnected = 2,
    /// The target peer does not support the holepunch extension.
    NoSupport = 3,
    /// The target endpoint belongs to the relaying peer.
    NoSelf = 4,
}

impl From<ErrorCode> for u32 {
    fn from(value: ErrorCode) -> Self {
        value as u32
    }
}

impl TryFrom<u32> for ErrorCode {
    type Error = String;

    fn try_from(value: u32) -> result::Result<Self, String> {
        match value {
            1 => Ok(ErrorCode::NoSuchPeer),
            2 => Ok(ErrorCode::NotConnected),
            3 => Ok(ErrorCode::NoSupport),
            4 => Ok(ErrorCode::NoSelf),
            _ => Err(format!("Invalid ErrorCode {}", value)),
        }
    }
}

/// The holepunch extension as defined in BEP55
#[derive(Debug)]
pub struct HolepunchExtension {
    /// The outgoing rendezvous requests which have not yet been answered with a connect message.
    pending_rendezvous: HashMap<SocketAddr, Reply<Result<SocketAddr>>>,
}

impl HolepunchExtension {
    pub const NAME: &'static str = "ut_holepunch";

    /// Create a new extension instance.
    pub fn new() -> Self {
        Self {
            pending_rendezvous: Default::default(),
        }
    }

    /// Handle the given extension message payload which has been received from the remote peer.
    pub async fn on_message(&mut self, payload: &[u8], peer: &PeerContext) -> Result<()> {
        let message = bencode::from_bytes::<HolepunchMessage>(payload)?;
        let extension_number = match peer.find_remote_extension_number(Self::NAME) {
            None => return Err(Error::Unsupported),
            Some(e) => e,
        };

        match message.message_type {
            MessageType::Rendezvous => {
                let target_addr = message.addr()?;
                if let Err(error_code) = self.on_rendezvous(target_addr, peer).await {
                    self.send_err_code(error_code, &message, extension_number, peer)
                        .await?;
                }
            }
            MessageType::Connect => {
                if let Err(e) = self.on_connect(&message, peer).await {
                    debug!(
                        "Peer {} {} extension failed to process connect, {}",
                        peer,
                        Self::NAME,
                        e
                    );
                }
            }
            MessageType::Error => self.on_error(&message, peer).await,
        }
        Ok(())
    }

    /// Send a rendezvous message for the target addr to the remote peer.
    pub async fn send_rendezvous(
        &mut self,
        target: SocketAddr,
        response: Reply<Result<SocketAddr>>,
        peer: &PeerContext,
    ) {
        let extension_number = match peer.find_remote_extension_number(Self::NAME) {
            None => {
                response.send(Err(Error::Unsupported));
                return;
            }
            Some(e) => e,
        };
        let addr_type = if target.is_ipv4() {
            AddrType::Ipv4
        } else {
            AddrType::Ipv6
        };
        let addr = match target.ip() {
            IpAddr::V4(ip) => ip.octets().to_vec(),
            IpAddr::V6(ip) => ip.octets().to_vec(),
        };
        let message = match bencode::to_bytes(&HolepunchMessage {
            message_type: MessageType::Rendezvous,
            addr_type,
            addr,
            port: target.port(),
            err_code: None,
        }) {
            Err(e) => {
                response.send(Err(Error::Parsing(e.to_string())));
                return;
            }
            Ok(e) => e,
        };

        match peer
            .send(Message::ExtendedPayload(extension_number, message))
            .await
        {
            Ok(_) => {
                self.pending_rendezvous.insert(target, response);
                trace!(
                    "Peer {} {} extension stored rendezvous task for {}",
                    peer,
                    Self::NAME,
                    target
                );
            }
            Err(e) => response.send(Err(Error::Io(io::Error::new(io::ErrorKind::Other, e)))),
        }
    }

    /// Try to connect to both the initiating peer and target peer.
    async fn on_rendezvous(
        &self,
        target_addr: SocketAddr,
        peer: &PeerContext,
    ) -> result::Result<(), ErrorCode> {
        // try to find the target peer in the torrent
        let target_peer = match peer.torrent().peer_by_addr(&target_addr).await {
            None => return Err(ErrorCode::NotConnected),
            Some(Peer::BitTorrent(peer)) => peer,
            _ => return Err(ErrorCode::NoSupport),
        };
        // check if the target peer supports the HolePunch extension
        let initiating_extension_number = match peer.find_remote_extension_number(Self::NAME) {
            None => return Err(ErrorCode::NoSupport),
            Some(e) => e,
        };
        let target_extension_number = match target_peer.remote_extension_number(Self::NAME).await {
            None => return Err(ErrorCode::NoSupport),
            Some(e) => e,
        };

        // send connect to the target peer with the remote peer addr
        let connect_message = Self::create_connect_message(peer.addr())?;
        if let Err(e) = target_peer
            .send(Message::ExtendedPayload(
                target_extension_number,
                connect_message,
            ))
            .await
        {
            debug!(
                "Peer {} {} extension failed to send message, {}",
                target_peer,
                Self::NAME,
                e
            );
            return Err(ErrorCode::NotConnected);
        }

        // send a connect to the initiating peer
        let connect_message = Self::create_connect_message(target_peer.addr())?;
        if let Err(e) = peer
            .send(Message::ExtendedPayload(
                initiating_extension_number,
                connect_message,
            ))
            .await
        {
            debug!(
                "Peer {} {} extension failed to send message, {}",
                peer,
                Self::NAME,
                e
            );
            return Err(ErrorCode::NotConnected);
        }

        Ok(())
    }

    /// Try to process a received [MessageType::Connect] message.
    async fn on_connect(&mut self, message: &HolepunchMessage, peer: &PeerContext) -> Result<()> {
        let addr = message.addr()?;
        // check if we've got a pending rendezvous task for the target addr
        if let Some(response) = self.pending_rendezvous.remove(&addr) {
            trace!(
                "Peer {} {} extension processed rendezvous message for {}",
                peer,
                Self::NAME,
                addr
            );
            response.send(Ok(addr));
        }

        if let Err(e) = peer.torrent().add_peer(addr).await {
            debug!("Peer {} failed to add target peer, {}", peer, e);
        }
        Ok(())
    }

    /// Process a received error message.
    async fn on_error(&mut self, message: &HolepunchMessage, peer: &PeerContext) {
        let addr = match message.addr() {
            Ok(addr) => addr,
            Err(_) => return,
        };
        let err_code = match message.err_code {
            Some(err_code) => err_code,
            None => {
                debug!(
                    "Peer {} {} extension received error without error code, {:?}",
                    peer,
                    Self::NAME,
                    message
                );
                return;
            }
        };
        if let Some(response) = self.pending_rendezvous.remove(&addr) {
            let reason = match err_code {
                ErrorCode::NoSuchPeer => "invalid target peer",
                ErrorCode::NotConnected => "not connected to target peer",
                ErrorCode::NoSupport => "holepunch not supported by target peer",
                ErrorCode::NoSelf => "no self connection allowed",
            };
            response.send(Err(Error::Operation(reason.to_string())));
        } else {
            debug!(
                "Peer {} {} extension received error from {}, {:?}",
                peer,
                Self::NAME,
                addr,
                message.err_code
            );
        }
    }

    async fn send_err_code(
        &self,
        err_code: ErrorCode,
        message: &HolepunchMessage,
        extension_number: ExtensionNumber,
        peer: &PeerContext,
    ) -> Result<()> {
        let payload = bencode::to_bytes(&HolepunchMessage {
            message_type: MessageType::Error,
            addr_type: message.addr_type,
            addr: message.addr.clone(),
            port: message.port,
            err_code: Some(err_code),
        })?;

        peer.send(Message::ExtendedPayload(extension_number, payload))
            .await
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))
    }

    fn create_connect_message(addr: &SocketAddr) -> result::Result<Vec<u8>, ErrorCode> {
        let addr_type = if addr.is_ipv4() {
            AddrType::Ipv4
        } else {
            AddrType::Ipv6
        };

        let message = HolepunchMessage {
            message_type: MessageType::Connect,
            addr_type,
            addr: match addr.ip() {
                IpAddr::V4(ip) => ip.octets().to_vec(),
                IpAddr::V6(ip) => ip.octets().to_vec(),
            },
            port: addr.port(),
            err_code: None,
        };
        bencode::to_bytes(&message).map_err(|e| {
            warn!(
                "{} extension failed to serialize message, {}",
                Self::NAME,
                e
            );
            ErrorCode::NoSupport
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod on_message {
        use super::*;
        use crate::operation::ConnectPeersOperation;
        use crate::peer::{
            PeerDiscovery, PeerId, ProtocolExtensionFlags, TcpPeerDiscovery, UtpPeerDiscovery,
        };
        use crate::storage::MemoryStorage;
        use crate::{Torrent, TorrentConfig};
        use std::time::Duration;
        use tempfile::tempdir;

        #[tokio::test]
        async fn test_send_rendezvous() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let initiating_torrent =
                create_torrent(temp_path, UtpPeerDiscovery::new().await.unwrap().into()).await;
            let relay_discovery = UtpPeerDiscovery::new().await.unwrap();
            let relay_torrent =
                create_relay_torrent(temp_path, &[relay_discovery.clone().into()]).await;

            // connect the initiating torrent to the relay torrent
            let (initiating_peer, relay_peer1, _in_socket, _out_socket) = utp_peer_pair!(
                &initiating_torrent,
                &relay_torrent,
                vec![HolepunchExtension::new().into()],
                vec![HolepunchExtension::new().into()],
                ProtocolExtensionFlags::LTEP
            );
            initiating_torrent
                .inner
                .peer_connected(relay_peer1.into())
                .await;
            assert_timeout!(
                Duration::from_millis(250),
                initiating_torrent.active_peer_connections().await == 1,
                "expected the initiating torrent to have 1 active peer connection"
            );
            assert_timeout!(
                Duration::from_millis(550),
                initiating_peer
                    .remote_peer()
                    .await
                    .map(|e| e.extended_handshake)
                    .unwrap_or_default(),
                "expected the extended handshake to have been exchanged"
            );

            // create the target torrent
            let target_torrent =
                create_torrent(temp_path, UtpPeerDiscovery::new().await.unwrap().into()).await;
            let target_addr = SocketAddr::from((
                Ipv4Addr::LOCALHOST,
                target_torrent
                    .peer_port()
                    .await
                    .expect("expected a target torrent peer port"),
            ));

            // connect the relay torrent to the target torrent
            let relay_data_pool = relay_torrent.inner.data_pool().await.unwrap();
            let peer = relay_discovery
                .dial(
                    PeerId::new(),
                    target_addr,
                    relay_torrent.inner.clone(),
                    relay_torrent.metadata().await.unwrap(),
                    relay_data_pool,
                    ProtocolExtensionFlags::LTEP,
                    vec![HolepunchExtension::new().into()],
                    Duration::from_millis(250),
                )
                .await
                .expect("expected the target peer to be dialed");
            relay_torrent.inner.peer_connected(peer.clone()).await;
            assert_timeout!(
                Duration::from_millis(250),
                relay_torrent.active_peer_connections().await == 1,
                "expected the relay torrent to have 1 active peer connection"
            );
            let peer = match peer {
                Peer::BitTorrent(e) => e,
                _ => unreachable!(),
            };
            assert_timeout!(
                Duration::from_millis(550),
                peer.remote_peer()
                    .await
                    .map(|e| e.extended_handshake)
                    .unwrap_or_default(),
                "expected the extended handshake to have been exchanged"
            );

            // send the rendezvous message from the initiating peer to the relay peer
            let response = initiating_peer.holepunch(target_addr).await;
            // await the rendezvous response
            let result = timeout!(
                Duration::from_secs(1),
                response,
                "expected the rendezvous to complete"
            );
            assert_eq!(Ok(target_addr), result);
            assert_timeout!(
                Duration::from_secs(3),
                initiating_torrent.active_peer_connections().await == 2,
                "expected the HolePunch to succeed"
            );
        }

        #[tokio::test]
        async fn test_rendezvous_not_supported_by_target() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let initiating_torrent =
                create_torrent(temp_path, UtpPeerDiscovery::new().await.unwrap().into()).await;
            let relay_discovery = TcpPeerDiscovery::new().await.unwrap();
            let relay_torrent = create_relay_torrent(
                temp_path,
                &[
                    relay_discovery.clone().into(),
                    UtpPeerDiscovery::new().await.unwrap().into(),
                ],
            )
            .await;

            // connect the initiating torrent to the relay torrent
            let (initiating_peer, relay_peer1, _in_socket, _out_socket) = utp_peer_pair!(
                &initiating_torrent,
                &relay_torrent,
                vec![HolepunchExtension::new().into()],
                vec![HolepunchExtension::new().into()],
                ProtocolExtensionFlags::LTEP
            );
            initiating_torrent
                .inner
                .peer_connected(relay_peer1.into())
                .await;
            assert_timeout!(
                Duration::from_millis(250),
                initiating_torrent.active_peer_connections().await == 1,
                "expected the initiating torrent to have 1 active peer connection"
            );
            assert_timeout!(
                Duration::from_millis(550),
                initiating_peer
                    .remote_peer()
                    .await
                    .map(|e| e.extended_handshake)
                    .unwrap_or_default(),
                "expected the extended handshake to have been exchanged"
            );

            // create the target torrent
            let target_torrent =
                create_torrent(temp_path, TcpPeerDiscovery::new().await.unwrap().into()).await;
            let target_addr = SocketAddr::from((
                Ipv4Addr::LOCALHOST,
                target_torrent
                    .peer_port()
                    .await
                    .expect("expected a target torrent peer port"),
            ));

            // connect the relay torrent to the target torrent
            let relay_data_pool = relay_torrent.inner.data_pool().await.unwrap();
            let extensions = vec![HolepunchExtension::new().into()];
            let peer = relay_discovery
                .dial(
                    PeerId::new(),
                    target_addr,
                    relay_torrent.inner.clone(),
                    relay_torrent.metadata().await.unwrap(),
                    relay_data_pool,
                    ProtocolExtensionFlags::LTEP,
                    extensions,
                    Duration::from_millis(250),
                )
                .await
                .unwrap();
            relay_torrent.inner.peer_connected(peer.clone()).await;
            assert_timeout!(
                Duration::from_millis(250),
                relay_torrent.active_peer_connections().await == 1,
                "expected the relay torrent to have 1 active peer connection"
            );
            let peer = match peer {
                Peer::BitTorrent(e) => e,
                _ => unreachable!(),
            };
            assert_timeout!(
                Duration::from_millis(550),
                peer.remote_peer()
                    .await
                    .map(|e| e.extended_handshake)
                    .unwrap_or_default(),
                "expected the extended handshake to have been exchanged"
            );

            // send the rendezvous message from the initiating peer to the relay peer
            let response = initiating_peer.holepunch(target_addr).await;
            // await the rendezvous response
            let result = timeout!(
                Duration::from_secs(1),
                response,
                "expected the rendezvous to complete"
            );
            match result {
                Err(e) => match e {
                    Error::Operation(e) => assert_eq!(e, "holepunch not supported by target peer"),
                    _ => assert!(false, "expected Error::Operation, but got {:?}", e),
                },
                _ => assert!(
                    false,
                    "expected the rendezvous to fail, but got {:?}",
                    result
                ),
            }
        }

        async fn create_torrent(temp_path: &str, discovery: PeerDiscovery) -> Torrent {
            Torrent::request()
                .metadata(metadata!("debian-udp.torrent"))
                .config(TorrentConfig::builder().path(temp_path).build())
                .protocol_extensions(ProtocolExtensionFlags::LTEP)
                .extension(|| HolepunchExtension::new().into())
                .operations(vec![ConnectPeersOperation::new(false).into()])
                .peer_discovery(discovery)
                .storage(|_| MemoryStorage::new().into())
                .build()
                .unwrap()
        }

        async fn create_relay_torrent(temp_path: &str, discoveries: &[PeerDiscovery]) -> Torrent {
            Torrent::request()
                .metadata(metadata!("debian-udp.torrent"))
                .config(TorrentConfig::builder().path(temp_path).build())
                .protocol_extensions(ProtocolExtensionFlags::LTEP)
                .extension(|| HolepunchExtension::new().into())
                .operations(vec![])
                .peer_discoveries(discoveries.to_vec())
                .storage(|_| MemoryStorage::new().into())
                .build()
                .unwrap()
        }
    }
}
