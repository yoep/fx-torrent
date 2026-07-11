use crate::peer::extension::{Error, ExtensionNumber, Result};
use crate::peer::protocol::Message;
use crate::peer::{
    ConnectionDirection, ConnectionProtocol, PeerClientInfo, PeerContext, ProtocolExtensionFlags,
};
use crate::{bencode, CompactIpv4Addrs, CompactIpv6Addrs, TorrentEvent};
use bitmask_enum::bitmask;
use fx_callback::{Callback, Subscription};
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// The Peer Exchange message.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct PexMessage {
    /// The added ipv4 peer addresses
    #[serde(default)]
    pub added: CompactIpv4Addrs,
    /// The flags of the added ipv4 peer addresses
    #[serde(default, rename = "added.f", with = "pex_flags")]
    pub added_flags: Vec<PexFlag>,
    /// The added ipv6 peer addresses
    #[serde(default)]
    pub added6: CompactIpv6Addrs,
    /// The flags of the added ipv6 peer addresses
    #[serde(default, rename = "added6.f", with = "pex_flags")]
    pub added6_flags: Vec<PexFlag>,
    /// The dropped ipv4 peer addresses
    #[serde(default)]
    pub dropped: CompactIpv4Addrs,
    /// The dropped ipv6 peer addresses
    #[serde(default)]
    pub dropped6: CompactIpv6Addrs,
}

impl PexMessage {
    /// Get all the discovered peers by the swarm
    fn discovered_peers(&self) -> Vec<SocketAddr> {
        let mut peers: Vec<SocketAddr> = self.added.iter().map(|e| SocketAddr::from(e)).collect();
        peers.extend(self.added6.iter().map(|e| SocketAddr::from(e)));
        peers
    }

    /// Get all the dropped peers from the swarm
    fn dropped_peers(&self) -> Vec<SocketAddr> {
        let mut peers: Vec<SocketAddr> = self.dropped.iter().map(|e| SocketAddr::from(e)).collect();
        peers.extend(self.dropped6.iter().map(|e| SocketAddr::from(e)));
        peers
    }

    /// Check if the message is empty.
    /// It returns `true` if the message is empty, else `false`.
    fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.added6.is_empty()
            && self.dropped.is_empty()
            && self.dropped6.is_empty()
    }
}

#[bitmask(u8)]
#[bitmask_config(vec_debug, flags_iter)]
pub enum PexFlag {
    /// prefers encryption, as indicated by e field in extension handshake
    EncryptionPreferred = 0x01,
    /// seed/upload_only
    UploadOnly = 0x02,
    /// supports utp
    UtpSupported = 0x04,
    /// peer indicated ut_holepunch support in extension handshake
    HolepunchSupported = 0x08,
    /// outgoing connection
    OutgoingConnection = 0x10,
}

impl Serialize for PexFlag {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde::Serialize::serialize(&self.bits(), serializer)
    }
}

impl<'de> Deserialize<'de> for PexFlag {
    fn deserialize<D>(deserializer: D) -> std::result::Result<PexFlag, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value: u8 = Deserialize::deserialize(deserializer)?;
        Ok(PexFlag::from(value))
    }
}

/// The PEX extensions as defined in BEP11.
#[derive(Debug)]
pub struct PexExtension {
    /// The pool which is used to manage the pex peer addresses
    pool: PexPool,
    torrent_event_receiver: Option<Subscription<TorrentEvent>>,
    interval: Duration,
    last_informed: Instant,
    initialized: bool,
    pex_supported: bool,
}

impl PexExtension {
    pub const NAME: &'static str = "ut_pex";

    /// Create a new pex extension with the given announce interval.
    pub fn new(interval: Duration) -> Self {
        Self {
            pool: PexPool::new(),
            torrent_event_receiver: None,
            interval,
            last_informed: Instant::now(),
            initialized: false,
            pex_supported: false,
        }
    }

    /// Process an incoming extension message payload which has been received from the remote peer.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn on_message(&self, payload: &[u8], peer: &PeerContext) -> Result<()> {
        let message: PexMessage = bencode::from_bytes(payload)?;
        debug!("Peer {} received PEX message {:?}", peer, message);

        let discovered_peers = message.discovered_peers();
        if discovered_peers.len() > 0 {
            peer.torrent().add_peers(discovered_peers).await;
        }

        let dropped_peers = message.dropped_peers();
        if dropped_peers.len() > 0 {
            peer.torrent().decrease_peer_priority(dropped_peers).await;
        }

        Ok(())
    }

    /// Invoked once per tick (typically once per second), providing a tick interval for the extension
    /// to process data.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn tick(&mut self, peer: &PeerContext) {
        if self.initialized && !self.pex_supported {
            return;
        }

        self.initialize(peer);
        self.process_torrent_events();
        self.inform_peer(peer).await;
    }

    fn initialize(&mut self, peer: &PeerContext) {
        if self.initialized || peer.remote_peer().is_none() {
            return;
        }
        // early exit if the peer doesn't support LTEP
        if !peer
            .remote_peer()
            .map(|e| e.protocol_extensions.contains(ProtocolExtensionFlags::LTEP))
            .unwrap_or_default()
        {
            self.initialized = true;
            return;
        }
        // wait for the extended handshake to be completed
        if !peer
            .remote_peer()
            .map(|e| e.extended_handshake)
            .unwrap_or_default()
        {
            return;
        }
        if !peer.is_extension_supported(Self::NAME) {
            self.initialized = true;
            return;
        }

        self.torrent_event_receiver = Some(peer.torrent().subscribe());
        self.pex_supported = true;
        self.initialized = true;
        debug!("Peer {} PEX has been initialized", peer);
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    fn process_torrent_events(&mut self) {
        let receiver = match self.torrent_event_receiver.as_mut() {
            None => return,
            Some(r) => r,
        };

        while let Ok(event) = receiver.try_recv() {
            self.pool.on_torrent_event(&*event)
        }
    }

    async fn inform_peer(&mut self, peer: &PeerContext) {
        if !self.initialized || self.last_informed.elapsed() < self.interval {
            return;
        }
        let extension_number = match peer.find_remote_extension_number(Self::NAME) {
            None => return,
            Some(e) => e,
        };

        self.pool.inform_peer(&extension_number, peer).await;
        self.last_informed = Instant::now();
    }
}

#[derive(Debug)]
struct PexPool {
    added_peers: Vec<PexPeer>,
    dropped_peers: Vec<PexPeer>,
}

impl PexPool {
    fn new() -> Self {
        Self {
            added_peers: Default::default(),
            dropped_peers: Default::default(),
        }
    }

    fn on_torrent_event(&mut self, event: &TorrentEvent) {
        match event {
            TorrentEvent::PeerConnected(peer) => self.peer_added(peer),
            TorrentEvent::PeerDisconnected(peer) => self.peer_removed(peer),
            _ => {}
        }
    }

    fn peer_added(&mut self, peer: &PeerClientInfo) {
        let mut flags = PexFlag::none();

        if peer.connection_type == ConnectionDirection::Outbound {
            flags |= PexFlag::OutgoingConnection;
        }
        if peer.connection_protocol == ConnectionProtocol::Utp {
            flags |= PexFlag::UtpSupported;
        }

        self.added_peers.push(PexPeer {
            addr: peer.addr.clone(),
            flags,
        });
    }

    fn peer_removed(&mut self, peer: &PeerClientInfo) {
        let mut flags = PexFlag::none();

        if peer.connection_type == ConnectionDirection::Outbound {
            flags |= PexFlag::OutgoingConnection;
        }
        if peer.connection_protocol == ConnectionProtocol::Utp {
            flags |= PexFlag::UtpSupported;
        }

        self.dropped_peers.push(PexPeer {
            addr: peer.addr.clone(),
            flags,
        });
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn inform_peer(&mut self, extension_number: &ExtensionNumber, peer: &PeerContext) {
        let message = self.message().await;

        if !message.is_empty() {
            let message_info = format!("{:?}", message);
            match self.try_inform_peer(message, extension_number, peer).await {
                Ok(_) => {
                    debug!("Peer {} sent PEX message {}", peer, message_info);
                }
                Err(e) => {
                    debug!(
                        "Peer {} failed to send PEX message {}, {}",
                        peer, message_info, e
                    );
                }
            }
        }
    }

    async fn try_inform_peer(
        &self,
        message: PexMessage,
        extension_number: &ExtensionNumber,
        peer: &PeerContext,
    ) -> Result<()> {
        let message_bytes = bencode::to_bytes(&message)?;

        peer.send(Message::ExtendedPayload(
            extension_number.clone(),
            message_bytes,
        ))
        .await
        .map_err(|e| Error::Operation(e.to_string()))
    }

    /// Get the PEX message to send to the peer and reset the pool.
    async fn message(&mut self) -> PexMessage {
        let added_peers = std::mem::take(&mut self.added_peers);
        let dropped_peers = std::mem::take(&mut self.dropped_peers);
        let mut added = vec![];
        let mut added_flags = vec![];
        let mut added6 = vec![];
        let mut added6_flags = vec![];
        let mut dropped = vec![];
        let mut dropped6 = vec![];

        for peer in added_peers {
            if peer.addr.is_ipv4() {
                match peer.addr.try_into() {
                    Ok(compact) => {
                        added.push(compact);
                        added_flags.push(peer.flags);
                    }
                    Err(e) => warn!("Failed to convert peer address to compact, {}", e),
                }
            } else {
                match peer.addr.try_into() {
                    Ok(compact) => {
                        added6.push(compact);
                        added6_flags.push(peer.flags);
                    }
                    Err(e) => warn!("Failed to convert peer address to compact, {}", e),
                }
            }
        }
        for peer in dropped_peers {
            if peer.addr.is_ipv4() {
                match peer.addr.try_into() {
                    Ok(compact) => dropped.push(compact),
                    Err(e) => warn!("Failed to convert peer address to compact, {}", e),
                }
            } else {
                match peer.addr.try_into() {
                    Ok(compact) => dropped6.push(compact),
                    Err(e) => warn!("Failed to convert peer address to compact, {}", e),
                }
            }
        }

        PexMessage {
            added: added.into(),
            added_flags,
            added6: added6.into(),
            added6_flags,
            dropped: dropped.into(),
            dropped6: dropped6.into(),
        }
    }
}

#[derive(Debug)]
struct PexPeer {
    addr: SocketAddr,
    flags: PexFlag,
}

mod pex_flags {
    use super::*;
    use serde::Deserializer;

    struct PexFlagsVisitor;

    impl<'de> serde::de::Visitor<'de> for PexFlagsVisitor {
        type Value = Vec<PexFlag>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("expected a bytes array or sequence of bytes")
        }

        fn visit_bytes<E>(self, value: &[u8]) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            let mut flags = Vec::new();

            for byte in value {
                flags.push(PexFlag::from(*byte));
            }

            Ok(flags)
        }

        fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut flags = Vec::new();

            while let Some(byte) = seq.next_element::<u8>()? {
                flags.push(PexFlag::from(byte));
            }

            Ok(flags)
        }
    }

    pub fn serialize<S>(flags: &Vec<PexFlag>, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes = flags.iter().map(|f| f.bits()).collect::<Vec<u8>>();
        serde::Serialize::serialize(&bytes, serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<Vec<PexFlag>, D::Error>
    where
        D: Deserializer<'de>,
    {
        D::deserialize_any(deserializer, PexFlagsVisitor {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::ProtocolExtensionFlags;
    use crate::storage::MemoryStorage;
    use crate::{Torrent, TorrentConfig};
    use tempfile::tempdir;

    #[test]
    fn test_pex_flags() {
        let expected_result = PexFlag::UtpSupported | PexFlag::OutgoingConnection;
        let bytes = bencode::to_bytes(&expected_result).unwrap();

        let result = bencode::from_bytes::<PexFlag>(&bytes).unwrap();
        assert_eq!(expected_result, result);
    }

    #[tokio::test]
    async fn test_on_message() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let source_torrent = create_pex_torrent(temp_path).await;
        let in_between_torrent = create_pex_torrent(temp_path).await;
        let target_torrent = create_pex_torrent(temp_path).await;

        // connect the source to the in-between torrent
        let (source, in_between) = tcp_peer_pair!(
            &source_torrent,
            &in_between_torrent,
            vec![PexExtension::new(Duration::from_secs(1)).into()],
            vec![PexExtension::new(Duration::from_secs(1)).into()],
            ProtocolExtensionFlags::LTEP
        );
        source_torrent.inner.peer_connected(source.into()).await;
        in_between_torrent
            .inner
            .peer_connected(in_between.into())
            .await;
        assert_timeout!(
            Duration::from_millis(500),
            source_torrent.active_peer_connections().await == 1,
            "expected the source torrent to be connected to the in-between"
        );

        // connect the in-between to the target torrent
        let (in_between, target) = tcp_peer_pair!(
            &in_between_torrent,
            &target_torrent,
            vec![PexExtension::new(Duration::from_secs(1)).into()],
            vec![PexExtension::new(Duration::from_secs(1)).into()],
            ProtocolExtensionFlags::LTEP
        );
        in_between_torrent
            .inner
            .peer_connected(in_between.into())
            .await;
        target_torrent.inner.peer_connected(target.into()).await;

        // wait for the pex extension to exchange target addr to the source
        assert_timeout!(
            Duration::from_secs(2),
            source_torrent.inner.peer_addrs_len().await == 2,
            "expected the source torrent to be connected to the target"
        );
    }

    async fn create_pex_torrent(temp_path: &str) -> Torrent {
        Torrent::request()
            .metadata(metadata!("debian-udp.torrent"))
            .config(TorrentConfig::builder().path(temp_path).build())
            .protocol_extensions(ProtocolExtensionFlags::LTEP)
            .extension(|| PexExtension::new(Duration::from_secs(1)).into())
            .operations(vec![])
            .storage(|_| MemoryStorage::new().into())
            .build()
            .unwrap()
    }
}
