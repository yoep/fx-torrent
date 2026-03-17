use crate::bloom_filter::BloomFilter;
use crate::dht::compact::{CompactIPv4Node, CompactIPv4Nodes, CompactIPv6Node, CompactIPv6Nodes};
use crate::dht::handler::QueryResult;
use crate::dht::krpc::{
    AnnouncePeerRequest, AnnouncePeerResponse, ErrorMessage, FindNodeRequest, FindNodeResponse,
    GetPeersRequest, GetPeersResponse, GetRequest, GetResponse, PingMessage, PutRequest,
    PutResponse, QueryMessage, ResponseMessage, SampleInfoHashesRequest, SampleInfoHashesResponse,
};
use crate::dht::routing_table::RoutingTable;
use crate::dht::utils::{generate_mutable_item_key, parse_mutable_item_properties};
use crate::dht::{
    DhtEvent, DhtMetrics, Error, Node, NodeId, NodeKey, NodeToken, PeerEntry, PublicKey, Result,
};
use crate::{CompactIpAddr, CompactIpv4Addr, CompactIpv6Addr, InfoHash, Sha1Hash};
use derive_more::Display;
use ed25519::SignatureBytes;
use fx_callback::MultiThreadedCallback;
use log::{debug, trace, warn};
use serde_bencode::value::Value;
use sha1::{Digest, Sha1};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};
#[cfg(feature = "tracing")]
use tracing::{instrument, Level};

/// The interval at which outdated peers are removed from the storage.
const CLEANUP_INTERVAL: Duration = Duration::from_secs(10);
/// The time after which a peer entry is considered expired.
const PEER_ENTRY_EXPIRED_AFTER: Duration = Duration::from_mins(30);

/// A DHT server node which handles incoming queries.
#[derive(Debug, Display)]
#[display("DHT node server [{}]", socket_addr.port())]
pub struct ServerNode {
    socket_addr: SocketAddr,
    metrics: DhtMetrics,
    peers: HashMap<InfoHash, HashSet<PeerEntry>>,
    items: HashMap<Sha1Hash, ItemEntry>,
    /// The last time the storage was cleaned up.
    last_cleanup: Instant,
    /// The total number of torrents to track from the DHT.
    max_torrents: usize,
    callbacks: MultiThreadedCallback<DhtEvent>,
}

impl ServerNode {
    /// Create a new server node.
    /// The underlying storage is limited to the given torrent count.
    pub fn new(
        metrics: DhtMetrics,
        socket_addr: SocketAddr,
        callbacks: MultiThreadedCallback<DhtEvent>,
        max_torrents: usize,
    ) -> Self {
        Self {
            socket_addr,
            metrics,
            peers: Default::default(),
            items: Default::default(),
            last_cleanup: Instant::now(),
            max_torrents,
            callbacks,
        }
    }

    /// Returns the total number of peers stored in the storage.
    pub fn peers_len(&self) -> usize {
        self.peers.values().map(|e| e.len()).sum()
    }

    /// Returns the number of torrents stored in the storage.
    pub fn torrents_len(&self) -> usize {
        self.peers.len()
    }

    /// Returns the torrent slice over all known info hashes.
    pub fn torrents(&self) -> impl Iterator<Item = &InfoHash> {
        self.peers.keys()
    }

    /// Returns the peers slice for the given torrent.
    pub fn peers(&self, info_hash: &InfoHash) -> impl Iterator<Item = &PeerEntry> {
        self.peers
            .get(info_hash)
            .map(|e| e.iter())
            .unwrap_or_default()
    }

    /// Get an item from the storage based on the given sha1 key.
    pub fn get(&self, key: &Sha1Hash) -> Option<&ItemEntry> {
        self.items.get(key)
    }

    /// Handle an incoming query from a remote node.
    pub async fn on_incoming_query(
        &mut self,
        query: QueryMessage,
        addr: &SocketAddr,
        routing_table: &RoutingTable,
    ) -> Result<QueryResult> {
        match query {
            QueryMessage::Ping { .. } => self.on_ping_request(routing_table).await,
            QueryMessage::FindNode { request } => {
                self.on_find_node_request(request, routing_table).await
            }
            QueryMessage::GetPeers { request } => {
                self.on_get_peers_request(request, routing_table).await
            }
            QueryMessage::AnnouncePeer { request } => {
                self.on_announce_peer_request(addr, request, routing_table)
                    .await
            }
            QueryMessage::SampleInfoHashes { request } => {
                self.on_sample_info_hashes_request(request, routing_table)
                    .await
            }
            QueryMessage::Put { request } => {
                self.on_put_request(request, addr, routing_table).await
            }
            QueryMessage::Get { request } => {
                self.on_get_request(request, addr, routing_table).await
            }
        }
    }

    /// Updates the peer information for the given info hash.
    pub fn update_peer(&mut self, info_hash: InfoHash, addr: SocketAddr, seed: bool) {
        if !self.peers.contains_key(&info_hash) && self.torrents_len() >= self.max_torrents {
            trace!("DHT storage is full, ignoring info hash {}", info_hash);
            return;
        }

        let entry = self.peers.entry(info_hash).or_default();
        entry.insert(PeerEntry::new(addr, seed));
    }

    /// Register a new info hash entry.
    pub fn register(&mut self, info_hash: &InfoHash) {
        if self.torrents_len() >= self.max_torrents {
            trace!("DHT storage is full, ignoring info hash {}", info_hash);
            return;
        }

        if !self.peers.contains_key(info_hash) {
            self.peers.insert(info_hash.clone(), Default::default());
            trace!("Added info hash {} to the storage", info_hash);
        }
    }

    /// Store the given value item.
    /// Mutable properties can be provided to allow the value to be updated in the future.
    ///
    /// Returns the hash of the stored item, or the error that occurred.
    pub fn store(
        &mut self,
        value: Value,
        mutable_properties: Option<MutableItemProperties>,
    ) -> Result<Sha1Hash> {
        let key: Sha1Hash = match mutable_properties.as_ref() {
            None => Self::generate_value_key(&value)?,
            Some(properties) => generate_mutable_item_key(
                properties.public_key.as_slice(),
                properties.salt.as_ref().map(|e| e.as_slice()),
            )?,
        };
        if let Some(item) = self.items.get(&key) {
            // if the mutable properties are not set, this item is immutable
            // otherwise, verify that the sequence number is higher than the current one
            match (
                item.mutable_properties.as_ref(),
                mutable_properties.as_ref(),
            ) {
                (None, _) => return Err(Error::AlreadyExists),
                (Some(existing), Some(updated)) => {
                    if existing.sequence_nr >= updated.sequence_nr {
                        return Err(Error::InvalidSequenceNr);
                    }
                }
                _ => {}
            }
        }

        self.items.insert(
            key.clone(),
            ItemEntry {
                value,
                mutable_properties,
            },
        );
        Ok(key)
    }

    /// Handle a periodic tick.
    /// This tick can be used for periodic cleanup or other maintenance tasks.
    pub async fn tick(&mut self) {
        if self.last_cleanup.elapsed() < CLEANUP_INTERVAL {
            return;
        }

        self.do_cleanup();
    }

    /// Process a received ping query.
    /// This invokes a simple ping-pong between the server and the sender.
    #[cfg_attr(feature = "tracing", instrument(err(level = Level::INFO)))]
    async fn on_ping_request(&self, routing_table: &RoutingTable) -> Result<QueryResult> {
        self.metrics.ping_requests.inc();
        Ok(ResponseMessage::Ping {
            response: PingMessage {
                id: routing_table.id,
            },
        }
        .into())
    }

    /// Process an incoming find nodes query.
    #[cfg_attr(feature = "tracing", instrument)]
    async fn on_find_node_request(
        &self,
        request: FindNodeRequest,
        routing_table: &RoutingTable,
    ) -> Result<QueryResult> {
        self.metrics.find_node_requests.inc();
        let target_node = request.target;
        let (compact_nodes, compact_nodes6) =
            Self::closest_node_pairs(&routing_table, &target_node);

        Ok(ResponseMessage::FindNode {
            response: FindNodeResponse {
                id: routing_table.id,
                nodes: compact_nodes.into(),
                nodes6: compact_nodes6.into(),
                token: None,
            },
        }
        .into())
    }

    /// Process a received get_peers query.
    /// The query will be processed only when the node is already known within the routing table.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn on_get_peers_request(
        &self,
        request: GetPeersRequest,
        routing_table: &RoutingTable,
    ) -> Result<QueryResult> {
        self.metrics.get_peers_requests.inc();
        let token: NodeToken;
        let (nodes, nodes6) = match routing_table.find_node(&request.id) {
            None => {
                return Ok(ErrorMessage::Generic("Bad node".to_string()).into());
            }
            Some(node) => {
                let info_hash_as_node =
                    match NodeId::try_from(request.info_hash.short_info_hash_bytes().as_slice()) {
                        Ok(e) => e,
                        Err(e) => {
                            warn!("{} failed to parse info hash as node id, {}", self, e);
                            return Ok(
                                ErrorMessage::Server("A Server Error Occurred".to_string()).into()
                            );
                        }
                    };

                token = node.generate_token().await;
                Self::closest_node_pairs(routing_table, &info_hash_as_node)
            }
        };

        let mut peers = self
            .peers(&request.info_hash)
            .filter(|e| e.addr.is_ipv4() == self.socket_addr.is_ipv4())
            .collect::<Vec<_>>();

        let mut downloaders = None;
        let mut seeders = None;
        if request.scrape {
            let mut downloaders_bloom_filter = BloomFilter::<256>::new();
            let mut seeders_bloom_filter = BloomFilter::<256>::new();

            for peer in peers.drain(..) {
                let ip_hash = Self::hash_ip_addr(&peer.addr);
                match peer.seed {
                    true => seeders_bloom_filter.insert(ip_hash),
                    false => downloaders_bloom_filter.insert(ip_hash),
                }
            }

            downloaders = Some(downloaders_bloom_filter);
            seeders = Some(seeders_bloom_filter);
        }

        Ok(ResponseMessage::GetPeers {
            response: GetPeersResponse {
                id: routing_table.id,
                name: None,
                token: Some(token),
                values: peers
                    .into_iter()
                    // BEP33 - If the requester is requesting no seed values, filter out all seeds
                    .filter(|e| !request.no_seed || !e.seed)
                    .map(|e| CompactIpAddr::from(e.addr))
                    .collect::<Vec<_>>(),
                nodes: nodes.into(),
                nodes6: nodes6.into(),
                downloaders: downloaders.map(|e| e.as_str().to_string()),
                seeds: seeders.map(|e| e.as_str().to_string()),
            },
        }
        .into())
    }

    /// Process an incoming announce peer query.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn on_announce_peer_request(
        &mut self,
        addr: &SocketAddr,
        request: AnnouncePeerRequest,
        routing_table: &RoutingTable,
    ) -> Result<QueryResult> {
        self.metrics.announce_peer_requests.inc();
        let node = match self
            .find_node_in_routing_table(&request.id, addr, routing_table)
            .await
        {
            Some(node) => node,
            None => {
                return Ok(ErrorMessage::Server("Invalid node".to_string()).into());
            }
        };
        if !node.verify_token(&request.token, &addr.ip()).await {
            return Ok(ErrorMessage::Protocol("Bad token".to_string()).into());
        };

        let info_hash = request.info_hash;
        let peer_addr = if request.implied_port {
            *addr
        } else {
            SocketAddr::new(addr.ip(), request.port)
        };
        trace!(
            "{} adding peer address {} for info hash {}",
            self,
            peer_addr,
            info_hash
        );
        self.update_peer(info_hash.clone(), peer_addr, request.seed);
        self.callbacks
            .invoke(DhtEvent::PeerUpdated(info_hash, peer_addr));
        self.metrics.discovered_peers.inc();

        Ok(ResponseMessage::Announce {
            response: AnnouncePeerResponse {
                id: routing_table.id,
            },
        }
        .into())
    }

    /// Process an incoming sample info hashes query.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn on_sample_info_hashes_request(
        &self,
        request: SampleInfoHashesRequest,
        routing_table: &RoutingTable,
    ) -> Result<QueryResult> {
        self.metrics.sample_info_hashes_requests.inc();
        let num = self.torrents().count();
        let samples = self.torrents().take(20).cloned().collect::<Vec<_>>();
        let (nodes, nodes6) = Self::closest_node_pairs(routing_table, &request.target);

        Ok(ResponseMessage::SampleInfoHashes {
            response: SampleInfoHashesResponse {
                id: routing_table.id,
                interval: 360,
                nodes: nodes.into(),
                nodes6,
                num: num as u32,
                samples,
            },
        }
        .into())
    }

    /// Process a received put immutable item query.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn on_put_request(
        &mut self,
        request: PutRequest,
        addr: &SocketAddr,
        routing_table: &RoutingTable,
    ) -> Result<QueryResult> {
        self.metrics.put_requests.inc();
        let node = match self
            .find_node_in_routing_table(&request.id, addr, routing_table)
            .await
        {
            Some(node) => node,
            None => {
                return Ok(ErrorMessage::Server("Invalid node".to_string()).into());
            }
        };
        if !node.verify_token(&request.token, &addr.ip()).await {
            trace!(
                "{} failed to verify token for put request from {:?}",
                self,
                addr
            );
            return Ok(ErrorMessage::Server("Bad Token".to_string()).into());
        }
        let public_key = match request
            .public_key
            .map(TryInto::<PublicKey>::try_into)
            .transpose()
        {
            Ok(key) => key,
            Err(_) => {
                return Ok(ErrorMessage::Server("Invalid public key".to_string()).into());
            }
        };
        let mutable_properties = match parse_mutable_item_properties(
            request.sequence_nr,
            public_key,
            request.salt,
            request.signature,
        ) {
            Ok(properties) => properties,
            Err(e) => {
                return match e {
                    Error::InvalidSequenceNr => {
                        return Ok(Self::invalid_sequence_nr());
                    }
                    Error::InvalidSignature => {
                        return Ok(Self::invalid_signature());
                    }
                    _ => Err(e),
                }
            }
        };

        if let Err(e) = self.store(request.value, mutable_properties) {
            warn!("{} failed to store immutable item, {}", self, e);
            return Ok(ErrorMessage::Server("Internal Server Error".to_string()).into());
        }

        Ok(ResponseMessage::Put {
            response: PutResponse {
                id: routing_table.id,
            },
        }
        .into())
    }

    /// Process a received get request.
    #[cfg_attr(feature = "tracing", instrument(skip_all, err(level = Level::INFO)))]
    async fn on_get_request(
        &self,
        request: GetRequest,
        addr: &SocketAddr,
        routing_table: &RoutingTable,
    ) -> Result<QueryResult> {
        self.metrics.get_requests.inc();
        let node = match self
            .find_node_in_routing_table(&request.id, addr, routing_table)
            .await
        {
            Some(node) => node,
            None => {
                return Ok(ErrorMessage::Server("Invalid node".to_string()).into());
            }
        };
        let hash: Sha1Hash = match Sha1Hash::try_from(request.target.as_slice()) {
            Ok(sha1) => sha1,
            Err(e) => {
                let hash_hex = hex::encode(request.target);
                trace!(
                    "{} failed to parse sha1 hash from \"{}\", {}",
                    self,
                    hash_hex,
                    e
                );
                return Ok(ErrorMessage::Server("Invalid target".to_string()).into());
            }
        };
        let token = node.generate_token().await;
        let (value, mutable_properties) = match self.get(&hash) {
            Some(item) => (Some(item.value.clone()), item.mutable_properties.clone()),
            None => (None, None),
        };
        let (sequence_nr, public_key, signature) = match mutable_properties {
            Some(properties) => (
                Some(properties.sequence_nr),
                Some(properties.public_key.to_vec()),
                Some(properties.signature.to_vec()),
            ),
            None => (None, None, None),
        };
        let (nodes, nodes6) = Self::closest_node_pairs(&routing_table, &NodeId::from(&hash));

        Ok(ResponseMessage::Get {
            response: GetResponse {
                id: routing_table.id,
                token: Some(token),
                value,
                nodes: nodes.into(),
                nodes6,
                sequence_nr,
                public_key,
                signature,
            },
        }
        .into())
    }

    /// Try to find the node within the routing table for the request.
    /// If the node is not found, an error response is returned to the requester.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn find_node_in_routing_table<'a>(
        &self,
        node_id: &NodeId,
        addr: &SocketAddr,
        routing_table: &'a RoutingTable,
    ) -> Option<&'a Node> {
        routing_table.find_node_by_key(&NodeKey {
            id: *node_id,
            addr: *addr,
        })
    }

    /// Purge old peers within the storage.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    pub fn do_cleanup(&mut self) {
        let mut removed = 0;
        for entry in self.peers.values_mut() {
            let initial = entry.len();
            entry.retain(|e| e.added.elapsed() <= PEER_ENTRY_EXPIRED_AFTER);
            removed += initial - entry.len();
        }
        debug!("{} removed {} peers from storage", self, removed);
        self.last_cleanup = Instant::now();
    }

    /// Try to generate the hash key from the given value.
    /// Returns the [Sha1Hash] for the [Value], else an error.
    fn generate_value_key(value: &Value) -> Result<Sha1Hash> {
        serde_bencode::to_bytes(&value)
            .map_err(|e| Error::Parse(e.to_string()))
            .and_then(|bytes| {
                Sha1Hash::try_from(Sha1::digest(bytes.as_slice()))
                    .map_err(|e| Error::Parse(e.to_string()))
            })
    }

    fn hash_ip_addr(addr: &SocketAddr) -> Vec<u8> {
        match addr.ip() {
            IpAddr::V4(ip) => Sha1::digest(ip.octets()).to_vec(),
            IpAddr::V6(ip) => Sha1::digest(ip.octets()).to_vec(),
        }
    }

    /// Returns the nodes closest to the given target node id.
    /// It creates a pair of compact IPv4 and IPv6 nodes based on the nodes within the bucket closest to the target.
    fn closest_node_pairs(
        routing_table: &RoutingTable,
        target: &NodeId,
    ) -> (CompactIPv4Nodes, CompactIPv6Nodes) {
        let mut compact_nodes = Vec::new();
        let mut compact_nodes6 = Vec::new();
        for node in routing_table.find_bucket_nodes(&target) {
            let addr = node.addr();
            match addr.ip() {
                IpAddr::V4(ip) => {
                    compact_nodes.push(CompactIPv4Node {
                        id: *node.id(),
                        addr: CompactIpv4Addr {
                            ip,
                            port: addr.port(),
                        },
                    });
                }
                IpAddr::V6(ip) => {
                    compact_nodes6.push(CompactIPv6Node {
                        id: *node.id(),
                        addr: CompactIpv6Addr {
                            ip,
                            port: addr.port(),
                        },
                    });
                }
            }
        }
        (compact_nodes.into(), compact_nodes6.into())
    }

    fn invalid_sequence_nr() -> QueryResult {
        ErrorMessage::InvalidSequenceNr("Invalid Sequence Nr".to_string()).into()
    }

    fn invalid_signature() -> QueryResult {
        ErrorMessage::InvalidSignature("Invalid Signature".to_string()).into()
    }
}

#[derive(Debug, Clone)]
pub struct ItemEntry {
    pub value: Value,
    pub mutable_properties: Option<MutableItemProperties>,
}

impl PartialEq for ItemEntry {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.mutable_properties == other.mutable_properties
    }
}

#[derive(Debug, Clone)]
pub struct MutableItemProperties {
    pub sequence_nr: u64,
    /// The public key of the item.
    pub public_key: PublicKey,
    /// The salt used to generate the item's hash.
    pub salt: Option<Vec<u8>>,
    /// The validation signature of the item.
    pub signature: SignatureBytes,
}

impl PartialEq for MutableItemProperties {
    fn eq(&self, other: &Self) -> bool {
        self.sequence_nr == other.sequence_nr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519::Signature;
    use std::net::Ipv4Addr;
    use std::str::FromStr;

    mod torrents {
        use super::*;
        use itertools::Itertools;
        use std::net::Ipv4Addr;
        use std::str::FromStr;

        #[test]
        fn test_torrents() {
            let info_hash =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let mut node = ServerNode::new(
                DhtMetrics::new(),
                SocketAddr::from((Ipv4Addr::LOCALHOST, 6881)),
                MultiThreadedCallback::new(),
                10,
            );

            // register a new torrent
            node.register(&info_hash);

            // retrieve the torrents
            let result = node.torrents().collect_vec();

            assert_eq!(
                1,
                result.len(),
                "expected the torrent to have been returned"
            );
            assert_eq!(&info_hash, result[0]);
        }
    }
    mod update_peer {
        use super::*;
        use itertools::Itertools;
        use std::net::Ipv4Addr;

        #[test]
        fn test_new_peer() {
            init_logger!();
            let info_hash =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let addr: SocketAddr = (Ipv4Addr::LOCALHOST, 6881).into();
            let mut node = ServerNode::new(
                DhtMetrics::new(),
                SocketAddr::from((Ipv4Addr::LOCALHOST, 6881)),
                MultiThreadedCallback::new(),
                16,
            );

            node.update_peer(info_hash.clone(), addr, false);

            let result = node.peers(&info_hash).find(|e| e.addr == addr);
            assert!(
                result.is_some(),
                "expected peer {} to be present within the storage",
                addr
            );

            let result = node.peers_len();
            assert_eq!(1, result, "expected the storage to contain one peer");
        }

        #[test]
        fn test_torrent_limit() {
            init_logger!();
            let info_hash1 =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let info_hash2 =
                InfoHash::from_str("urn:btih:2C6B6858D61DA9543D4231A71DB4B1C9264B0685").unwrap();
            let mut node = ServerNode::new(
                DhtMetrics::new(),
                SocketAddr::from((Ipv4Addr::LOCALHOST, 6881)),
                MultiThreadedCallback::new(),
                1,
            );

            // add the initial info hash
            node.update_peer(
                info_hash1.clone(),
                (Ipv4Addr::LOCALHOST, 6881).into(),
                false,
            );
            assert_eq!(
                1,
                node.torrents_len(),
                "expected the torrent to have been added"
            );
            assert_eq!(
                true,
                node.torrents().contains(&info_hash1),
                "expected the torrent to be present within the storage"
            );

            // try to add the second info hash, which should be ignored
            node.update_peer(
                info_hash2.clone(),
                (Ipv4Addr::LOCALHOST, 6882).into(),
                false,
            );
            assert_eq!(
                1,
                node.torrents_len(),
                "expected the torrent to have been ignored"
            );
            assert_eq!(
                false,
                node.torrents().contains(&info_hash2),
                "expected the torrent to not have been added to the storage"
            );
        }
    }

    mod store {
        use super::*;
        use rand::{rng, Rng};
        use std::net::Ipv4Addr;

        #[tokio::test]
        async fn test_non_existing() {
            let expected_result = Value::Int(13);
            let mut node = ServerNode::new(
                DhtMetrics::new(),
                SocketAddr::from((Ipv4Addr::LOCALHOST, 6881)),
                MultiThreadedCallback::new(),
                16,
            );

            let key = node
                .store(expected_result.clone(), None)
                .expect("expected the item to have been stored");

            let result = node
                .get(&key)
                .expect("expected the item to be present within the storage");
            assert_eq!(expected_result, result.value);
        }

        #[tokio::test]
        async fn test_existing_immutable_item() {
            let expected_result = Value::Int(69);
            let mut node = ServerNode::new(
                DhtMetrics::new(),
                SocketAddr::from((Ipv4Addr::LOCALHOST, 6881)),
                MultiThreadedCallback::new(),
                16,
            );

            // store the item as immutable
            let _ = node
                .store(expected_result.clone(), None)
                .expect("expected the item to have been stored");

            // try to store the item a second time
            let result = node
                .store(expected_result.clone(), None)
                .expect_err("expected the item to be immutable");
            assert_eq!(Error::AlreadyExists, result);
        }

        #[test]
        fn test_existing_mutable_item() {
            let initial_value = Value::Int(69);
            let updated_value = Value::Int(70);
            let mut public_key = PublicKey::default();
            rng().fill_bytes(&mut public_key);
            let initial_properties = MutableItemProperties {
                sequence_nr: 0,
                public_key: public_key.clone(),
                salt: Some("FooBar".as_bytes().to_vec()),
                signature: [0u8; Signature::BYTE_SIZE],
            };
            let updated_properties = MutableItemProperties {
                sequence_nr: 1,
                public_key,
                salt: Some("FooBar".as_bytes().to_vec()),
                signature: [0u8; Signature::BYTE_SIZE],
            };
            let mut node = ServerNode::new(
                DhtMetrics::new(),
                SocketAddr::from((Ipv4Addr::LOCALHOST, 6881)),
                MultiThreadedCallback::new(),
                16,
            );

            // store the initial value as mutable
            // this should return a key based on the public key
            let key = node
                .store(initial_value.clone(), Some(initial_properties))
                .expect("expected the item to have been stored");
            let result = node
                .get(&key)
                .expect("expected the item to be present within the storage");
            assert_eq!(
                initial_value, result.value,
                "expected the initial value match"
            );

            // try to update the mutable item
            let result = node
                .store(updated_value.clone(), Some(updated_properties))
                .expect("expected the item to have been updated");
            assert_eq!(key, result, "expected the key to be the same");
            let result = node.get(&key).unwrap();
            assert_eq!(
                updated_value, result.value,
                "expected the value to have been updated"
            );
        }
    }

    mod mutable_properties {
        use super::*;

        #[test]
        fn test_partial_eq() {
            let properties1 = create_properties(0);
            let properties2 = create_properties(0);
            let properties3 = create_properties(1);

            assert_eq!(
                properties1, properties2,
                "expected the mutable properties to be equal"
            );
            assert_ne!(
                properties1, properties3,
                "expected the mutable properties to not be equal"
            );
        }
    }

    mod generate_key {
        use super::*;

        /// BEP44: test 3 (immutable)
        #[test]
        fn test_generate_value_key() {
            let item = "Hello World!";
            let expected_result = Sha1Hash::try_from(
                hex::decode("e5f96f6f38320f0f33959cb4d3d656452117aadb").unwrap(),
            )
            .unwrap();
            let bencode = serde_bencode::to_string(&item.to_string())
                .expect("expected the item to be serialized");
            let value = serde_bencode::from_str::<Value>(&bencode)
                .expect("expected the item to be deserialized");

            let result = ServerNode::generate_value_key(&value)
                .expect("expected the value key to be generated");

            assert_eq!(expected_result, result);
        }
    }

    #[test]
    fn test_register() {
        init_logger!();
        let info_hash =
            InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
        let mut node = ServerNode::new(
            DhtMetrics::new(),
            SocketAddr::from((Ipv4Addr::LOCALHOST, 6881)),
            MultiThreadedCallback::new(),
            16,
        );

        node.register(&info_hash);

        let result = node.torrents().cloned().collect::<Vec<_>>();
        assert!(
            result.contains(&info_hash),
            "expected info hash {} to have been present within the storage",
            info_hash
        );
    }

    fn create_properties(sequence_nr: u64) -> MutableItemProperties {
        MutableItemProperties {
            sequence_nr,
            public_key: PublicKey::default(),
            salt: None,
            signature: [0u8; Signature::BYTE_SIZE],
        }
    }
}
