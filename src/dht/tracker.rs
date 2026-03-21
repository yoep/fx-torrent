use crate::bloom_filter::BloomFilter;
use crate::channel::{ChannelReceiver, ChannelSender, Reply, Response};
use crate::dht::compact::{CompactIPv6Nodes, CompactIpNodes};
use crate::dht::krpc::{
    AnnouncePeerRequest, AnnouncePeerResponse, ErrorMessage, FindNodeRequest, FindNodeResponse,
    GetPeersRequest, GetPeersResponse, GetRequest, GetResponse, Message, MessagePayload,
    PingMessage, PutRequest, PutResponse, QueryMessage, ResponseMessage, ResponsePayload,
    SampleInfoHashesRequest, SampleInfoHashesResponse, TransactionId, Version, WantFamily,
};
use crate::dht::observer::Observer;
use crate::dht::routing_table::RoutingTable;
use crate::dht::traversal::TraversalAlgorithm;
use crate::dht::utils::{generate_mutable_item_key, parse_mutable_item_properties};
use crate::dht::ServerNode;
use crate::dht::{
    Config, DhtMetrics, Error, ItemSignature, Mode, Node, NodeId, NodeKey, NodeState, NodeToken,
    PeerEntry, PublicKey, Result, SecretKey, DEFAULT_ROUTING_NODE_SERVERS,
};
use crate::dht::{DhtNodeHandler, QueryResult};
use crate::metrics::Metric;
use crate::{InfoHash, Sha1Hash};
use derive_more::Display;
use ed25519::SignatureBytes;
use futures::StreamExt;
use fx_callback::{Callback, MultiThreadedCallback, Subscription};
use itertools::{Either, Itertools};
use log::{debug, error, trace, warn};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_bencode::value::Value;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{lookup_host, UdpSocket};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::time::{interval, timeout};
use tokio::{select, time};
use tokio_util::sync::CancellationToken;
#[cfg(feature = "tracing")]
use tracing::{instrument, Level};
use url::Url;

/// The maximum size of a single UDP packet.
const MAX_PACKET_SIZE: usize = 65_535;
const VERSION_IDENTIFIER: &str = "FX0001";
const SEND_PACKAGE_TIMEOUT: Duration = Duration::from_secs(2);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(8);
const REFRESH_TIMEOUT: Duration = Duration::from_secs(60 * 15);
const BOOTSTRAP_INTERVAL: Duration = Duration::from_secs(2);
const REFRESH_INTERVAL: Duration = Duration::from_secs(60 * 5);
const TICK_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_BUCKET_SIZE: usize = 8;

#[derive(Debug)]
pub enum DhtEvent {
    /// Invoked when the node ID of the DHT server changes.
    IDChanged,
    /// Invoked when the external IP address of the DHT server changes.
    ExternalIpChanged(IpAddr),
    /// Invoked when a new node is added to the routing table.
    NodeAdded(NodeKey),
    /// Invoked when a new info hash is added to the DHT storage.
    InfoHashAdded(InfoHash),
    /// Invoked when a peer is updated within the DHT storage.
    PeerUpdated(InfoHash, SocketAddr),
    /// Invoked when the stats of the DHT server are updated.
    Stats(DhtMetrics),
}

/// A tracker instance for managing DHT nodes.
/// This instance can be shared between torrents by using [DhtTracker::clone].
#[derive(Debug, Display)]
#[display("DHT node [{}]", addr.port())]
pub struct DhtTracker {
    addr: SocketAddr,
    mode: Mode,
    metrics: DhtMetrics,
    pub(crate) sender: ChannelSender<TrackerCommand>,
    callbacks: MultiThreadedCallback<DhtEvent>,
    cancellation_token: CancellationToken,
}

impl DhtTracker {
    /// Create a new builder instance to create a new node server.
    pub fn builder() -> DhtTrackerBuilder {
        DhtTrackerBuilder::default()
    }

    /// Create a new node with the given ID.
    /// This function allows creating a server with a specific node id.
    pub async fn new(config: Config) -> Result<Self> {
        let socket = Arc::new(Self::bind_socket().await?);
        let socket_addr = socket.local_addr()?;
        let (command_sender, command_receiver) = channel!(256);
        let item_signature = match config.item_signature {
            None => ItemSignature::new()?,
            Some(e) => e,
        };
        let mut context = TrackerContext::new(
            config.id,
            socket,
            socket_addr,
            config.mode,
            config.info_hash_indexing_enabled,
            item_signature,
            config.max_torrents,
        );
        let metrics = context.metrics.clone();
        let callbacks = context.callbacks.clone();
        let cancellation_token = context.cancellation_token.clone();

        // create the observer and traversal algorithm for the node
        let observer = Observer::new(command_sender.clone());
        let traversal = TraversalAlgorithm::new(
            DEFAULT_BUCKET_SIZE,
            config.routing_nodes,
            command_sender.clone(),
        );

        // start the context in a separate task
        tokio::spawn(async move {
            context
                .run(
                    config.info_hash_indexing_interval,
                    observer,
                    traversal,
                    command_receiver,
                )
                .await;
        });

        Ok(Self {
            addr: socket_addr,
            mode: config.mode,
            metrics,
            sender: command_sender,
            callbacks,
            cancellation_token,
        })
    }

    /// Get the ID of the DHT server.
    pub async fn id(&self) -> Result<NodeId> {
        let response = self
            .sender
            .send(|tx| TrackerCommand::Id { response: tx })
            .await;
        response.await.map_err(|_| Error::Closed)
    }

    /// Returns the socket address on which this DHT node is running.
    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    /// Returns the port on which the DHT node is running.
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// Returns the DHT network metrics of the node.
    pub fn metrics(&self) -> &DhtMetrics {
        &self.metrics
    }

    /// Returns the mode of the DHT node.
    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// Get the number of nodes within the routing table.
    pub async fn total_nodes(&self) -> usize {
        let response = self
            .sender
            .send(|tx| TrackerCommand::TotalNodes { response: tx })
            .await;
        response.await.unwrap_or_default()
    }

    /// Returns the node with the given key from the routing table, if found.
    pub async fn node(&self, node: &NodeKey) -> Option<Node> {
        self.sender
            .send(|tx| TrackerCommand::GetNode {
                node: *node,
                response: tx,
            })
            .await
            .await
            .ok()
            .flatten()
    }

    /// Returns the node with the given id from the routing table, if found.
    pub async fn node_by_id(&self, id: &NodeId) -> Option<Node> {
        self.sender
            .send(|tx| TrackerCommand::GetNodeById {
                id: *id,
                response: tx,
            })
            .await
            .await
            .unwrap_or_else(|_| None)
    }

    /// Returns all nodes within the routing table of the tracker.
    /// This doesn't include any router/search nodes.
    pub async fn nodes(&self) -> Vec<Node> {
        let response = self
            .sender
            .send(|tx| TrackerCommand::GetNodes { response: tx })
            .await;
        response.await.unwrap_or_default()
    }

    /// Add an unverified node to the routing table.
    /// The node will be pinged before it's actually added to the routing table.
    pub async fn add_node(&self, addr: &SocketAddr) -> Result<()> {
        let response = self
            .sender
            .send(|tx| TrackerCommand::Ping {
                addr: *addr,
                response: tx,
            })
            .await
            .await
            .map_err(|_| Error::Closed)?;

        match response {
            Ok(node) => {
                let _ = self
                    .sender
                    .fire_and_forget(TrackerCommand::AddTraversalNode((node.id, node.addr)));
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Returns a list of info hashes currently known to the DHT tracker.
    pub async fn info_hashes(&self) -> Vec<InfoHash> {
        self.sender
            .send(|tx| TrackerCommand::GetStorageInfoHashes { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns a list of peers currently known to the DHT tracker.
    pub async fn peers(&self) -> HashMap<InfoHash, HashSet<PeerEntry>> {
        self.sender
            .send(|tx| TrackerCommand::GetStoragePeers { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Try to ping the given node address.
    /// This function waits for a response from the node, so it might be recommended to wrap this fn call in a timeout.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    ///  use std::time::Duration;
    ///  use tokio::select;
    ///  use tokio::time;
    ///  use fx_torrent::dht::DhtTracker;
    ///
    ///  let target_addr = ([10, 0 , 0, 99], 6881).into();
    ///  let tracker = DhtTracker::builder()
    ///     .default_routing_nodes()
    ///     .build()
    ///     .await.unwrap();
    ///  select! {
    ///      _ = time::sleep(Duration::from_secs(10)) => return,
    ///      result = tracker.ping(&target_addr) => {
    ///         match result {
    ///             Ok(node_key) => println!("Successfully pinged node: {:?}", node_key),
    ///             Err(e) => println!("Failed to ping node: {}", e),
    ///         }
    ///      }
    ///  }
    /// ```
    #[cfg_attr(feature = "tracing", instrument(skip(self), err(level = Level::INFO)))]
    pub async fn ping(&self, addr: SocketAddr) -> Result<NodeKey> {
        self.sender
            .send(|tx| TrackerCommand::Ping { addr, response: tx })
            .await
            .await
            .map_err(|_| Error::Closed)?
    }

    /// Try to find nearby nodes for the given node id.
    /// This function waits for a response from one or more nodes within the routing table.
    /// Each queried node is limited to the given timeout.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    ///  use std::time::Duration;
    ///  use fx_torrent::dht::{DhtTracker, NodeId};
    ///
    ///  let target_node = NodeId::new();
    ///  let tracker = DhtTracker::builder()
    ///     .default_routing_nodes()
    ///     .build()
    ///     .await.unwrap();
    ///
    ///  match tracker.find_nodes(&target_node, Duration::from_secs(10)).await {
    ///      Ok(node_key) => println!("Successfully found nodes: {:?}", node_key),
    ///      Err(e) => println!("Failed to find nodes: {}", e),
    ///  }
    /// ```
    #[cfg_attr(feature = "tracing", instrument(skip_all, err(level = Level::INFO)))]
    pub async fn find_nodes(&self, target_id: &NodeId, timeout: Duration) -> Result<Vec<NodeKey>> {
        let nodes = self
            .sender
            .send(|tx| TrackerCommand::GoodSearchNodes { response: tx })
            .await
            .await?;

        let futures = nodes.iter().map(|node| async {
            let response = self
                .sender
                .send(|tx| TrackerCommand::FindNode {
                    node: *node,
                    target_id: *target_id,
                    response: tx,
                })
                .await;

            select! {
                _ = time::sleep(timeout) => Err(Error::Timeout),
                result = response => result,
            }
        });

        Ok(futures::future::join_all(futures)
            .await
            .into_iter()
            .flat_map(|result| result.ok())
            .concat())
    }

    /// Returns the peer addresses for the given torrent info hash from a specific node within the network.
    /// Each queried node is limited to the given timeout.
    ///
    /// Use `n_depth` to determine the depth of the search within the network.
    #[cfg_attr(feature = "tracing", instrument(skip_all, err(level = Level::INFO)))]
    pub async fn get_peers_from(
        &self,
        info_hash: &InfoHash,
        node: &NodeKey,
        n_depth: usize,
        timeout: Duration,
    ) -> Result<Vec<SocketAddr>> {
        let mut peers = self
            .sender
            .send(|tx| TrackerCommand::LookupPeers {
                info_hash: info_hash.clone(),
                response: tx,
            })
            .await
            .await
            .unwrap_or_default();
        let mut found_peers = self
            .internal_get_peers(info_hash, node, n_depth, timeout)
            .await?;
        peers.append(&mut found_peers);
        Ok(peers.into_iter().unique().collect())
    }

    /// Returns the peer addresses for the given torrent info hash within the network.
    /// This function waits for a response from one oe more nodes within the routing table.
    /// Each queried node is limited to the given timeout.
    ///
    /// Use `n_depth` to determine the depth of the search within the network.
    #[cfg_attr(feature = "tracing", instrument(skip_all, err(level = Level::INFO)))]
    pub async fn get_peers(
        &self,
        info_hash: &InfoHash,
        n_depth: usize,
        timeout: Duration,
    ) -> Result<Vec<SocketAddr>> {
        let nodes = self
            .sender
            .send(|tx| TrackerCommand::GoodSearchNodes { response: tx })
            .await
            .await?;
        let mut peers = self
            .sender
            .send(|tx| TrackerCommand::LookupPeers {
                info_hash: info_hash.clone(),
                response: tx,
            })
            .await
            .await
            .unwrap_or_default();

        let futures = nodes.iter().map(|node| async {
            self.internal_get_peers(info_hash, node, n_depth, timeout)
                .await
        });

        let mut found_peers = futures::future::join_all(futures)
            .await
            .into_iter()
            .flat_map(|result| result.ok())
            .concat();
        peers.append(&mut found_peers);
        Ok(peers.into_iter().unique().collect())
    }

    /// Scrape the downloaders and seeders for the given info hash.
    /// Each queried node is limited to the given timeout.
    #[cfg_attr(feature = "tracing", instrument(skip_all, err(level = Level::INFO)))]
    pub async fn scrape_peers(
        &self,
        info_hash: &InfoHash,
        timeout: Duration,
    ) -> Result<ScrapeResult> {
        let nodes = self
            .sender
            .send(|tx| TrackerCommand::GoodSearchNodes { response: tx })
            .await
            .await?;

        let futures = nodes.iter().map(|node| async {
            let response = self
                .sender
                .send(|tx| TrackerCommand::GetPeers {
                    node: *node,
                    info_hash: info_hash.clone(),
                    scrape: true,
                    response: tx,
                })
                .await;

            select! {
                _ = time::sleep(timeout) => Err(Error::Timeout),
                result = response => result,
            }
        });

        let mut scrape_result = ScrapeResult::default();
        for result in futures::future::join_all(futures)
            .await
            .into_iter()
            .filter_map(|e| e.ok())
        {
            scrape_result.downloaders += result.downloaders;
            scrape_result.seeders += result.seeders;
        }

        Ok(scrape_result)
    }

    /// Announce the given peer to the DHT network.
    ///
    /// As defined in BEP33, the `announce_peer` supports indicating if the peer is a seeder.
    #[cfg_attr(feature = "tracing", instrument(skip_all, err(level = Level::INFO)))]
    pub async fn announce_peer(
        &self,
        info_hash: &InfoHash,
        peer_addr: &SocketAddr,
        is_seed: bool,
    ) -> Result<()> {
        self.sender
            .send(|tx| TrackerCommand::AnnouncePeer {
                info_hash: info_hash.clone(),
                peer_addr: *peer_addr,
                is_seed,
                node: None,
                response: tx,
            })
            .await
            .await
    }

    /// Announce the given peer to a specific node within the network.
    ///
    /// As defined in BEP33, the `announce_peer` supports indicating if the peer is a seeder.
    #[cfg_attr(feature = "tracing", instrument(skip(self), err(level = Level::INFO)))]
    pub async fn announce_peer_to(
        &self,
        info_hash: &InfoHash,
        peer_addr: &SocketAddr,
        is_seed: bool,
        node: &NodeKey,
    ) -> Result<()> {
        self.sender
            .send(|tx| TrackerCommand::AnnouncePeer {
                info_hash: info_hash.clone(),
                peer_addr: *peer_addr,
                is_seed,
                node: Some(*node),
                response: tx,
            })
            .await
            .await
    }

    /// Returns a sample of available info hashes from the given node.
    #[cfg_attr(feature = "tracing", instrument(skip(self), err(level = Level::INFO)))]
    pub async fn scrape_info_hashes_from(
        &self,
        target: &NodeId,
        node: &NodeKey,
    ) -> Result<Vec<InfoHash>> {
        self.sender
            .send(|tx| TrackerCommand::ScrapeInfoHashes {
                target: *target,
                node: *node,
                response: tx,
            })
            .await
            .await
    }

    /// Returns the available info hashes from the DHT network.
    /// Each queried node is limited to the given timeout.
    #[cfg_attr(feature = "tracing", instrument(skip(self), err(level = Level::INFO)))]
    pub async fn scrape_info_hashes(
        &self,
        target: &NodeId,
        timeout: Duration,
    ) -> Result<Vec<InfoHash>> {
        let nodes = self
            .sender
            .send(|tx| TrackerCommand::GoodSearchNodes { response: tx })
            .await
            .await?;

        let futures = nodes.iter().map(|node| async {
            let response = self
                .sender
                .send(|tx| TrackerCommand::ScrapeInfoHashes {
                    target: *target,
                    node: *node,
                    response: tx,
                })
                .await;

            select! {
                _ = time::sleep(timeout) => Err(Error::Timeout),
                result = response => result,
            }
        });

        Ok(futures::future::join_all(futures)
            .await
            .into_iter()
            .flat_map(|result| result.ok())
            .concat())
    }

    /// Put an immutable item within the DHT network.
    /// Each queried node is limited to the given timeout.
    #[cfg_attr(feature = "tracing", instrument(skip_all, err(level = Level::INFO)))]
    pub async fn put<V>(&self, value: &V, timeout: Duration) -> Result<()>
    where
        V: Serialize,
    {
        let nodes = self
            .sender
            .send(|tx| TrackerCommand::GoodSearchNodes { response: tx })
            .await
            .await?;
        let bytes = serde_bencode::to_bytes(value)?;
        let value = serde_bencode::from_bytes::<Value>(bytes.as_slice())?;

        let futures = nodes.iter().map(|node| async {
            let response = self
                .sender
                .send(|tx| TrackerCommand::Put {
                    node: *node,
                    value: value.clone(),
                    sequence_nr: None,
                    signature: None,
                    public_key: None,
                    salt: None,
                    response: tx,
                })
                .await;

            select! {
                _ = time::sleep(timeout) => Err(Error::Timeout),
                result = response => result,
            }
        });

        let _ = futures::future::join_all(futures).await;
        Ok(())
    }

    /// Put a mutable item within the DHT network.
    /// Each queried node is limited to the given timeout.
    ///
    /// Returns the [PublicKey] to use for item validation.
    #[cfg_attr(feature = "tracing", instrument(skip_all, err(level = Level::INFO)))]
    pub async fn put_mutable<V>(
        &self,
        value: &V,
        timeout: Duration,
        sequence_nr: Option<u64>,
        salt: Option<Vec<u8>>,
        secret_key: &SecretKey,
    ) -> Result<PublicKey>
    where
        V: Serialize,
    {
        let nodes = self
            .sender
            .send(|tx| TrackerCommand::GoodSearchNodes { response: tx })
            .await
            .await?;
        let bytes = serde_bencode::to_bytes(value)?;
        let value = serde_bencode::from_bytes::<Value>(bytes.as_slice())?;
        let (signature, public_key) = self
            .sender
            .send(|tx| TrackerCommand::SignValue {
                value: value.clone(),
                sequence_nr: sequence_nr.unwrap_or(1),
                salt: salt.clone(),
                secret_key: secret_key.clone(),
                response: tx,
            })
            .await
            .await?;

        let futures = nodes.iter().map(|node| async {
            let response = self
                .sender
                .send(|tx| TrackerCommand::Put {
                    node: *node,
                    value: value.clone(),
                    sequence_nr: sequence_nr.clone(),
                    signature: Some(signature.clone()),
                    public_key: Some(public_key.clone()),
                    salt: salt.clone(),
                    response: tx,
                })
                .await;

            select! {
                _ = time::sleep(timeout) => Err(Error::Timeout),
                result = response => result,
            }
        });

        let _ = futures::future::join_all(futures).await;
        Ok(public_key)
    }

    /// Put an immutable item to the given node within the DHT network.
    #[cfg_attr(feature = "tracing", instrument(skip_all, err(level = Level::INFO)))]
    pub async fn put_to<V>(&self, value: &V, node: &NodeKey) -> Result<()>
    where
        V: Serialize,
    {
        let bytes = serde_bencode::to_bytes(value)?;
        let value = serde_bencode::from_bytes::<Value>(bytes.as_slice())?;
        self.sender
            .send(|tx| TrackerCommand::Put {
                node: *node,
                value,
                sequence_nr: None,
                signature: None,
                public_key: None,
                salt: None,
                response: tx,
            })
            .await
            .await
    }

    /// Get an **immutable** item from the DHT network.
    /// Each queried node is limited to the given timeout.
    ///
    /// Use `n_depth` to determine the depth of the search within the network.
    #[cfg_attr(feature = "tracing", instrument(skip_all, err(level = Level::INFO)))]
    pub async fn get<V>(
        &self,
        hash: Sha1Hash,
        timeout: Duration,
        n_depth: usize,
    ) -> Result<Option<V>>
    where
        V: DeserializeOwned,
    {
        let nodes = self
            .sender
            .send(|tx| TrackerCommand::GoodSearchNodes { response: tx })
            .await
            .await?;

        let mut item_stream = futures::stream::iter(nodes)
            .map(|node| async move {
                self.internal_get_item(&hash, None, None, None, &node, n_depth, timeout)
                    .await
            })
            .buffer_unordered(5);

        while let Some(item) = item_stream.next().await {
            match item {
                Ok(Some(item)) => {
                    let bytes = serde_bencode::to_bytes(&item)?;
                    return Ok(Some(serde_bencode::from_bytes::<V>(bytes.as_slice())?));
                }
                Err(e) => {
                    trace!("{} failed to get item, {}", self, e);
                }
                _ => {}
            }
        }

        Ok(None)
    }

    /// Get a **mutable** item from the DHT network.
    /// Each queried node is limited to the given timeout.
    ///
    /// Use `n_depth` to determine the depth of the search within the network.
    #[cfg_attr(feature = "tracing", instrument(skip_all, err(level = Level::INFO)))]
    pub async fn get_mutable<V>(
        &self,
        public_key: &PublicKey,
        salt: Option<Vec<u8>>,
        sequence_nr: Option<u64>,
        timeout: Duration,
        n_depth: usize,
    ) -> Result<Option<V>>
    where
        V: DeserializeOwned,
    {
        let nodes = self
            .sender
            .send(|tx| TrackerCommand::GoodSearchNodes { response: tx })
            .await
            .await?;
        let hash: Sha1Hash =
            generate_mutable_item_key(public_key, salt.as_ref().map(|e| e.as_ref()))?;

        let mut item_stream = futures::stream::iter(nodes)
            .map(|node| {
                let fn_sequence_nr = sequence_nr.as_ref();
                let fn_salt = salt.as_ref().map(|e| e.as_ref());
                async move {
                    self.internal_get_item(
                        &hash,
                        fn_sequence_nr,
                        Some(public_key),
                        fn_salt,
                        &node,
                        n_depth,
                        timeout,
                    )
                    .await
                }
            })
            .buffer_unordered(5);

        while let Some(item) = item_stream.next().await {
            match item {
                Ok(Some(item)) => {
                    let bytes = serde_bencode::to_bytes(&item)?;
                    return Ok(Some(serde_bencode::from_bytes::<V>(bytes.as_slice())?));
                }
                Err(e) => {
                    trace!("{} failed to get item, {}", self, e);
                }
                _ => {}
            }
        }

        Ok(None)
    }

    /// Get an immutable item from the given node within the DHT network.
    #[cfg_attr(feature = "tracing", instrument(skip_all, err(level = Level::INFO)))]
    pub async fn get_from<V>(&self, hash: Sha1Hash, node: &NodeKey) -> Result<Option<V>>
    where
        V: DeserializeOwned,
    {
        self.sender
            .send(|tx| TrackerCommand::Get {
                node: *node,
                hash,
                sequence_nr: None,
                public_key: None,
                salt: None,
                response: tx,
            })
            .await
            .await
            .map(|e: GetResult| e.value)
            .and_then(|e| {
                if let Some(value) = e {
                    let bytes = serde_bencode::to_bytes(&value)?;
                    let item = serde_bencode::from_bytes::<V>(bytes.as_slice())?;
                    return Ok(Some(item));
                }
                Ok(None)
            })
    }

    /// Close/stop the DHT node.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    pub fn close(&self) {
        self.cancellation_token.cancel();
    }

    /// Retrieve peers starting from `node` going `depth_left` hops deep.
    async fn internal_get_peers(
        &self,
        info_hash: &InfoHash,
        node: &NodeKey,
        depth_left: usize,
        timeout: Duration,
    ) -> Result<Vec<SocketAddr>> {
        const MAX_IN_FLIGHT: usize = 5;
        let result = select! {
            _ = time::sleep(timeout) => return Err(Error::Timeout),
            result = self.do_get_peers(info_hash, node, false) => result?,
        };
        let mut peers: HashSet<SocketAddr> = HashSet::from_iter(result.peers);

        if depth_left == 0 || result.nodes.is_empty() {
            return Ok(peers.into_iter().collect());
        }

        let result = futures::stream::iter(result.nodes.into_iter())
            .map(|node| async move {
                let ping: Result<NodeKey> = self.ping(node.addr).await;
                if ping.is_err() {
                    return vec![];
                }

                let get_peers_result = select! {
                    _ = time::sleep(timeout) => Err(Error::Timeout),
                    result = self.do_get_peers(info_hash, &node, false) => result,
                };
                match get_peers_result {
                    Err(_) => vec![],
                    Ok(result) => {
                        let mut peers = result.peers;
                        if depth_left == 0 {
                            return peers;
                        }

                        let futures = result
                            .nodes
                            .iter()
                            .map(|node| async {
                                self.internal_get_peers(
                                    info_hash,
                                    node,
                                    depth_left.saturating_sub(1),
                                    timeout,
                                )
                                .await
                            })
                            .collect::<Vec<_>>();
                        let mut result = futures::future::join_all(futures)
                            .await
                            .into_iter()
                            .filter_map(|e| e.ok())
                            .concat();
                        peers.append(&mut result);

                        peers
                    }
                }
            })
            .buffer_unordered(MAX_IN_FLIGHT)
            .collect::<Vec<_>>()
            .await
            .concat();

        for peer in result {
            peers.insert(peer);
        }
        if peers.is_empty() {
            return Ok(vec![]);
        }

        debug!(
            "{} discovered a total of {} peers for {} from {:?}",
            self,
            peers.len(),
            info_hash,
            node.addr
        );
        Ok(peers.into_iter().collect())
    }

    /// Retrieve the peers from the given node for the [InfoHash].
    async fn do_get_peers(
        &self,
        info_hash: &InfoHash,
        node: &NodeKey,
        scrape: bool,
    ) -> Result<GetPeersResult> {
        self.sender
            .send(|tx| TrackerCommand::GetPeers {
                node: *node,
                info_hash: info_hash.clone(),
                scrape,
                response: tx,
            })
            .await
            .await
    }

    /// Try to retrieve the immutable item for the given [Sha1Hash] starting from the `node`.
    /// It goes `depth_left` level deep from the given node, trying to retrieve the information of the item.
    async fn internal_get_item(
        &self,
        hash: &Sha1Hash,
        sequence_nr: Option<&u64>,
        public_key: Option<&PublicKey>,
        salt: Option<&[u8]>,
        node: &NodeKey,
        depth_left: usize,
        timeout: Duration,
    ) -> Result<Option<Value>> {
        const MAX_IN_FLIGHT: usize = 5;
        let result = select! {
            _ = time::sleep(timeout) => Err(Error::Timeout),
            result = self.do_get_item(
                hash.clone(),
                sequence_nr.as_ref().map(|e| **e),
                public_key.map(|e| e.clone()),
                salt.map(|e| e.to_vec()),
                node,
            ) => result,
        }?;
        if let Some(value) = result.value {
            return Ok(Some(value));
        }

        if depth_left == 0 || result.nodes.is_empty() {
            return Ok(None);
        }

        let mut item_stream = futures::stream::iter(result.nodes)
            .map(|node| async move {
                let ping: Result<NodeKey> = self.ping(node.addr).await;
                if ping.is_err() {
                    return None;
                }

                self.internal_get_item(
                    hash,
                    sequence_nr,
                    public_key,
                    salt,
                    &node,
                    depth_left.saturating_sub(1),
                    timeout,
                )
                .await
                .ok()
                .flatten()
            })
            .buffer_unordered(MAX_IN_FLIGHT);

        while let Some(item) = item_stream.next().await {
            if item.is_some() {
                return Ok(item);
            }
        }

        Ok(None)
    }

    /// Retrieve the immutable item from the given node for [Sha1Hash].
    async fn do_get_item(
        &self,
        hash: Sha1Hash,
        sequence_nr: Option<u64>,
        public_key: Option<PublicKey>,
        salt: Option<Vec<u8>>,
        node: &NodeKey,
    ) -> Result<GetResult> {
        self.sender
            .send(|tx| TrackerCommand::Get {
                node: *node,
                hash,
                sequence_nr,
                public_key,
                salt,
                response: tx,
            })
            .await
            .await
    }

    /// Create a new UDP socket.
    pub(crate) async fn bind_socket() -> Result<UdpSocket> {
        match Self::bind_dual_stack().await {
            Ok(socket) => Ok(socket),
            Err(e) => {
                debug!("DHT node failed to bind dual stack socket, {}", e);
                Ok(UdpSocket::bind("0.0.0.0:0").await?)
            }
        }
    }

    /// Try to bind a dual stack IPv4 & IPv6 udp socket.
    async fn bind_dual_stack() -> Result<UdpSocket> {
        // TODO: reimplement dual stack support
        Err(Error::Io(io::Error::new(
            io::ErrorKind::Other,
            "Dual stack support is currently not implemented",
        )))
    }
}

impl Callback<DhtEvent> for DhtTracker {
    fn subscribe(&self) -> Subscription<DhtEvent> {
        self.callbacks.subscribe()
    }
}

impl Clone for DhtTracker {
    fn clone(&self) -> Self {
        Self {
            addr: self.addr.clone(),
            mode: self.mode,
            metrics: self.metrics.clone(),
            sender: self.sender.clone(),
            callbacks: self.callbacks.clone(),
            cancellation_token: self.cancellation_token.clone(),
        }
    }
}

#[derive(Debug, Default)]
pub struct DhtTrackerBuilder {
    id: Option<NodeId>,
    mode: Option<Mode>,
    public_ip: Option<IpAddr>,
    routing_nodes: Vec<SocketAddr>,
    routing_node_urls: Vec<String>,
    max_torrents: Option<usize>,
    enable_indexing: Option<bool>,
    indexing_interval: Option<Duration>,
    verifier: Option<ItemSignature>,
}

impl DhtTrackerBuilder {
    /// Set the ID of the node server.
    pub fn node_id(&mut self, id: NodeId) -> &mut Self {
        self.id = Some(id);
        self
    }

    /// Set the public ip address of the dht tracker.
    pub fn public_ip(&mut self, ip: IpAddr) -> &mut Self {
        self.public_ip = Some(ip);
        self
    }

    /// Set the mode of the DHT node.
    pub fn mode(&mut self, mode: Mode) -> &mut Self {
        self.mode = Some(mode);
        self
    }

    /// Set if the default routing nodes should be enabled.
    pub fn enable_default_routing_nodes(&mut self, enabled: bool) -> &mut Self {
        if enabled {
            self.default_routing_nodes();
        } else {
            let default_routing_nodes = DEFAULT_ROUTING_NODE_SERVERS();
            self.routing_node_urls
                .retain(|e| !default_routing_nodes.contains(&e.as_str()));
        }
        self
    }

    /// Add the default routing nodes used for searching new nodes.
    pub fn default_routing_nodes(&mut self) -> &mut Self {
        self.routing_node_urls.extend(
            DEFAULT_ROUTING_NODE_SERVERS()
                .into_iter()
                .map(|e| e.to_string()),
        );
        self
    }

    /// Add the given address to the routing nodes used for searching new nodes.
    pub fn routing_node(&mut self, addr: SocketAddr) -> &mut Self {
        self.routing_nodes.push(addr);
        self
    }

    /// Set the routing nodes to use for searching new nodes.
    /// This replaces any already existing configured routing nodes.
    pub fn routing_nodes(&mut self, nodes: Vec<SocketAddr>) -> &mut Self {
        self.routing_nodes = nodes;
        self
    }

    /// Add the given node url to use for searching new nodes.
    pub fn routing_node_url<S: AsRef<str>>(&mut self, url: S) -> &mut Self {
        self.routing_node_urls.push(url.as_ref().to_string());
        self
    }

    /// Set the maximum number of torrents to track within the DHT network.
    pub fn max_torrents(&mut self, max_torrents: usize) -> &mut Self {
        self.max_torrents = Some(max_torrents);
        self
    }

    /// Set if the DHT tracker should enable indexing of info hashes.
    pub fn enable_indexing(&mut self, enable: bool) -> &mut Self {
        self.enable_indexing = Some(enable);
        self
    }

    /// Set the interval at which the DHT tracker should index info hashes.
    pub fn indexing_interval(&mut self, interval: Duration) -> &mut Self {
        self.indexing_interval = Some(interval);
        self
    }

    /// Set the item verifier to use for validating mutable items stored in the DHT network.
    pub fn item_verifier(&mut self, verifier: ItemSignature) -> &mut Self {
        self.verifier = Some(verifier);
        self
    }

    /// Try to create a new DHT node from this builder.
    ///
    /// # Panics
    ///
    /// This method panics if no [ItemSignature] has been set and no crypto provider features are enabled.
    /// For more info, see [ItemSignature::new].
    pub async fn build(&mut self) -> Result<DhtTracker> {
        let defaults = Config::default();
        let id = self.id.take().unwrap_or_else(|| {
            self.public_ip
                .take()
                .map(|e| NodeId::from_ip(&e))
                .unwrap_or(defaults.id)
        });
        let mode = self.mode.take().unwrap_or(defaults.mode);
        let max_torrents = self.max_torrents.unwrap_or(defaults.max_torrents);
        let info_hash_indexing_enabled = self
            .enable_indexing
            .unwrap_or(defaults.info_hash_indexing_enabled);
        let info_hash_indexing_interval = self
            .indexing_interval
            .unwrap_or(defaults.info_hash_indexing_interval);
        let item_signature = self.verifier.take();
        let mut routing_nodes: HashSet<SocketAddr> = self.routing_nodes.drain(..).collect();

        for node_url in self.routing_node_urls.drain(..).filter_map(Self::host) {
            match lookup_host(node_url.as_str()).await {
                Ok(addrs) => {
                    routing_nodes.extend(addrs);
                }
                Err(e) => trace!("DHT router node failed to resolve \"{}\", {}", node_url, e),
            }
        }

        DhtTracker::new(Config {
            id,
            mode,
            max_torrents,
            info_hash_indexing_enabled,
            info_hash_indexing_interval,
            item_signature,
            routing_nodes: routing_nodes.into_iter().collect::<Vec<_>>(),
        })
        .await
    }

    fn host<S: AsRef<str>>(url: S) -> Option<String> {
        let url = Url::parse(url.as_ref()).ok()?;
        if let Some(host) = url.host_str() {
            let port = url.port().unwrap_or(80);
            return Some(format!("{}:{}", host, port));
        }

        Some(url.as_ref().to_string())
    }
}

/// The information returned by a `scrape` operation within the DHT network.
#[derive(Debug, Default)]
pub struct ScrapeResult {
    pub downloaders: usize,
    pub seeders: usize,
}

/// The information returned by a `get_peers` operation within the DHT network.
#[derive(Debug)]
pub(crate) struct GetPeersResult {
    pub peers: Vec<SocketAddr>,
    pub nodes: Vec<NodeKey>,
    pub downloaders: usize,
    pub seeders: usize,
}

/// The information returned by a `get` operation within the DHT network.
#[derive(Debug)]
pub(crate) struct GetResult {
    pub value: Option<Value>,
    pub nodes: Vec<NodeKey>,
}

/// The internal DHT tracker commands executed on the main loop of the [TrackerContext].
pub(crate) enum TrackerCommand {
    Id {
        response: Reply<NodeId>,
    },
    Ping {
        addr: SocketAddr,
        response: Reply<Result<NodeKey>>,
    },
    /// Find the target node closest to the given node.
    FindNode {
        node: NodeKey,
        target_id: NodeId,
        response: Reply<Result<Vec<NodeKey>>>,
    },
    /// Find peers within the DHT storage for the given torrent info hash.
    LookupPeers {
        info_hash: InfoHash,
        response: Reply<Vec<SocketAddr>>,
    },
    /// Find peer addresses for the given torrent info hash within the network.
    GetPeers {
        node: NodeKey,
        info_hash: InfoHash,
        scrape: bool,
        response: Reply<Result<GetPeersResult>>,
    },
    /// Announce the given peer to the DHT network.
    AnnouncePeer {
        info_hash: InfoHash,
        peer_addr: SocketAddr,
        is_seed: bool,
        node: Option<NodeKey>,
        response: Reply<Result<()>>,
    },
    /// Scrape the info hashes from the given node.
    ScrapeInfoHashes {
        target: NodeId,
        node: NodeKey,
        response: Reply<Result<Vec<InfoHash>>>,
    },
    /// Put an item to the DHT node.
    Put {
        node: NodeKey,
        value: Value,
        sequence_nr: Option<u64>,
        signature: Option<SignatureBytes>,
        public_key: Option<PublicKey>,
        salt: Option<Vec<u8>>,
        response: Reply<Result<()>>,
    },
    /// Sign the given value
    SignValue {
        value: Value,
        sequence_nr: u64,
        salt: Option<Vec<u8>>,
        secret_key: SecretKey,
        response: Reply<Result<(SignatureBytes, PublicKey)>>,
    },
    /// Get an item from the DHT node.
    Get {
        node: NodeKey,
        hash: Sha1Hash,
        sequence_nr: Option<u64>,
        public_key: Option<PublicKey>,
        salt: Option<Vec<u8>>,
        response: Reply<Result<GetResult>>,
    },
    TotalNodes {
        response: Reply<usize>,
    },
    /// Returns the node with the given key from the routing table, if found.
    GetNode {
        node: NodeKey,
        response: Reply<Option<Node>>,
    },
    /// Returns the node with the given id from the routing table, if found.
    GetNodeById {
        id: NodeId,
        response: Reply<Option<Node>>,
    },
    /// Returns all nodes within the routing table.
    GetNodes {
        response: Reply<Vec<Node>>,
    },
    /// Returns the info hashes stored within the [DhtStorage].
    GetStorageInfoHashes {
        response: Reply<Vec<InfoHash>>,
    },
    /// Returns the peers stored within the [DhtStorage].
    GetStoragePeers {
        response: Reply<HashMap<InfoHash, HashSet<PeerEntry>>>,
    },
    /// Returns the node keys of "good" nodes which can be used in search queries.
    GoodSearchNodes {
        response: Reply<Vec<NodeKey>>,
    },
    AddTraversalNode((NodeId, SocketAddr)),
    UpdateExternalIp(IpAddr),
}

impl Debug for TrackerCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrackerCommand").finish()
    }
}

/// The internal tracker context of the DHT node.
#[derive(Debug, Display)]
#[display("DHT node [{}]", socket_addr.port())]
pub(crate) struct TrackerContext {
    /// The current transaction ID of the node server
    transaction_id: u32,
    /// The underlying socket used by the server
    socket: Arc<UdpSocket>,
    /// The address on which the server is listening
    pub(crate) socket_addr: SocketAddr,
    /// The routing table of the node server
    pub(crate) routing_table: RoutingTable,
    /// The handler for processing DHT network data.
    handler: DhtNodeHandler,
    /// The currently pending requests of the server
    pending_requests: HashMap<TransactionKey, PendingRequest>,
    /// The timeout while trying to send packages to a target address
    send_timeout: Duration,
    /// Indicates if the DHT tracker should enable indexing of info hashes.
    info_hash_indexing_enabled: bool,
    /// The verifier to use for validating mutable items stored in the DHT network.
    item_signature: ItemSignature,
    /// The tracker metrics of the DHT network
    pub(crate) metrics: DhtMetrics,
    /// The channel receiver for incoming messages
    receiver: UnboundedReceiver<ReaderMessage>,
    /// The callback of the tracker
    pub(crate) callbacks: MultiThreadedCallback<DhtEvent>,
    /// The cancellation token of the server
    pub(crate) cancellation_token: CancellationToken,
}

impl TrackerContext {
    pub(crate) fn new(
        id: NodeId,
        socket: Arc<UdpSocket>,
        socket_addr: SocketAddr,
        mode: Mode,
        info_hash_indexing_enabled: bool,
        item_verifier: ItemSignature,
        max_torrents: usize,
    ) -> Self {
        let (sender, receiver) = unbounded_channel();
        let cancellation_token = CancellationToken::new();
        let metrics = DhtMetrics::new();
        let callbacks = MultiThreadedCallback::new();
        let mode = match mode {
            Mode::Client => DhtNodeHandler::client(),
            Mode::Server => DhtNodeHandler::server(ServerNode::new(
                metrics.clone(),
                socket_addr.clone(),
                callbacks.clone(),
                max_torrents,
            )),
        };

        // start the reader in a separate task
        let reader_socket = socket.clone();
        let reader_cancellation_token = cancellation_token.clone();
        tokio::spawn(async move {
            let reader = NodeReader {
                socket: reader_socket,
                addr_port: socket_addr.port(),
                sender,
                cancellation_token: reader_cancellation_token,
            };
            reader.run().await;
        });

        Self {
            transaction_id: Default::default(),
            socket,
            socket_addr,
            routing_table: RoutingTable::new(id, DEFAULT_BUCKET_SIZE),
            handler: mode,
            pending_requests: Default::default(),
            send_timeout: SEND_PACKAGE_TIMEOUT,
            info_hash_indexing_enabled,
            item_signature: item_verifier,
            metrics,
            receiver,
            callbacks,
            cancellation_token,
        }
    }

    /// Run the task loop of the context.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    pub(crate) async fn run(
        &mut self,
        index_info_hashes_interval: Duration,
        mut observer: Observer,
        mut traversal: TraversalAlgorithm,
        mut command_receiver: ChannelReceiver<TrackerCommand>,
    ) {
        let mut bootstrap_interval = interval(BOOTSTRAP_INTERVAL);
        let mut refresh_interval = interval(REFRESH_INTERVAL);
        let mut index_info_hashes_interval = interval(index_info_hashes_interval);
        let mut tick_interval = interval(TICK_INTERVAL);

        debug!("{} started", self);
        loop {
            select! {
                _ = self.cancellation_token.cancelled() => break,
                Some(message) = self.receiver.recv() => self.on_message_received(message, &mut observer, &mut traversal).await,
                command = command_receiver.recv() => {
                    if let Some(command) = command {
                        self.handle_command(command, &mut traversal).await
                    } else {
                        break;
                    }
                },
                _ = refresh_interval.tick() => self.refresh_routing_table().await,
                _ = bootstrap_interval.tick() => self.bootstrap(&mut traversal).await,
                _ = index_info_hashes_interval.tick(), if self.info_hash_indexing_enabled => self.index_info_hashes().await,
                _ = tick_interval.tick() => self.tick().await,
            }
        }
        debug!("{} main loop ended", self);
    }

    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn bootstrap(&mut self, traversal: &mut TraversalAlgorithm) {
        traversal.run(self.routing_table.id, self).await;
    }

    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn on_message_received(
        &mut self,
        message: ReaderMessage,
        observer: &mut Observer,
        traversal: &mut TraversalAlgorithm,
    ) {
        match message {
            ReaderMessage::Message {
                message,
                message_len,
                addr,
            } => {
                self.metrics.bytes_in.inc_by(message_len as u64);
                observer.observe(addr, message.ip.as_ref(), &self).await;
                if let Err(e) = self.on_incoming_message(message, addr, traversal).await {
                    debug!(
                        "{} failed to process incoming message from {}, {}",
                        self, addr, e
                    );
                    self.metrics.errors.inc();
                }
            }
            ReaderMessage::Error {
                error,
                payload_len,
                addr,
            } => {
                warn!(
                    "{} failed to read incoming message (len {}) from {}, {}",
                    self, payload_len, addr, error
                );
                self.metrics.bytes_in.inc_by(payload_len as u64);
                self.metrics.errors.inc();
            }
        }
    }

    /// Try to process an incoming DHT message from the given node address.
    #[cfg_attr(feature = "tracing", instrument(skip_all, err))]
    async fn on_incoming_message(
        &mut self,
        message: Message,
        addr: SocketAddr,
        traversal: &mut TraversalAlgorithm,
    ) -> Result<()> {
        trace!(
            "{} received message (transaction {}) from {}, {:?}",
            self,
            message
                .transaction_id_as_str()
                .map(|e| e.to_string())
                .unwrap_or_else(|| message.transaction_id_as_u32().to_string()),
            addr,
            message
        );
        let node_id = message.id().cloned();
        let transaction_id = message.transaction_id;
        let read_only = message.read_only;
        let key = TransactionKey {
            id: transaction_id.clone(),
            addr,
        };

        // check the type of the message
        match message.payload {
            MessagePayload::Query(query) => {
                self.on_incoming_query(transaction_id, query, &addr).await?
            }
            MessagePayload::Response(response_payload) => {
                if let Some(pending_request) = self.pending_requests.remove(&key) {
                    let response = response_payload.parse(pending_request.query_name.as_str())?;
                    debug!(
                        "{} received response \"{}\" from {} for {}",
                        self,
                        response.name(),
                        addr,
                        key
                    );

                    match response {
                        ResponseMessage::Ping { response } => {
                            self.on_ping_response(&key, &addr, pending_request, response)
                                .await;
                        }
                        ResponseMessage::FindNode { response } => {
                            self.on_find_node_response(&key, &addr, pending_request, response)
                                .await;
                        }
                        ResponseMessage::GetPeers { response } => {
                            self.on_get_peers_response(
                                &key,
                                &addr,
                                pending_request,
                                response,
                                traversal,
                            )
                            .await;
                        }
                        ResponseMessage::Announce { response } => {
                            self.on_announce_response(&key, &addr, pending_request, response)
                                .await;
                        }
                        ResponseMessage::SampleInfoHashes { response } => {
                            self.on_sample_info_hashes_response(
                                &key,
                                &addr,
                                pending_request,
                                response,
                                traversal,
                            )
                            .await;
                        }
                        ResponseMessage::Put { response } => {
                            self.on_put_response(&key, &addr, pending_request, response)
                                .await;
                        }
                        ResponseMessage::Get { response } => {
                            self.on_get_response(&key, &addr, pending_request, response, traversal)
                                .await;
                        }
                    }
                } else {
                    debug!(
                        "{} received response for unknown request, invalid transaction {}",
                        self, key
                    );
                    self.metrics.errors.inc();
                }
            }
            MessagePayload::Error { error } => {
                self.on_error_response(&key, &addr, error).await;
            }
        }

        if let Some(id) = node_id {
            // do not add read-only nodes to the routing
            // as they cannot be queried
            if !read_only {
                self.update_node(id, addr, traversal).await;
            }
        }
        Ok(())
    }

    /// Handle an incoming query message.
    async fn on_incoming_query(
        &mut self,
        transaction_id: TransactionId,
        query: QueryMessage,
        addr: &SocketAddr,
    ) -> Result<()> {
        let result = self
            .handler
            .on_incoming_query(query, &addr, &self.routing_table)
            .await?;

        match result {
            QueryResult::Response(message) => {
                self.send_response(transaction_id, message, addr).await
            }
            QueryResult::Error(message) => self.send_error(transaction_id, message, addr).await,
        }
    }

    /// Process the received announce_peer response.
    ///
    /// # Arguments
    ///
    /// * `key` - The transaction key of the query.
    /// * `addr`- The source address of the node.
    /// * `pending_request` - The pending request of the query.
    /// * `response` - The announce peer response of the node.
    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    async fn on_announce_response(
        &self,
        key: &TransactionKey,
        addr: &SocketAddr,
        pending_request: PendingRequest,
        response: AnnouncePeerResponse,
    ) {
        if !response.id.verify_id(&addr.ip()) {
            debug!("{} detected spoofed announce_peer from {}", self, key);
            Self::resolve_as_err(pending_request.request_type, Error::InvalidNodeId);
            return;
        }

        self.node_query_result(&addr, true).await;
        match pending_request.request_type {
            Some(PendingRequestType::AnnouncePeer(tx)) => {
                let _ = tx.send(Ok(()));
            }
            Some(_) => Self::resolve_as_err(
                pending_request.request_type,
                Error::InvalidMessage(format!(
                    "expected {} response, got announce_peer instead",
                    pending_request.query_name
                )),
            ),
            _ => {}
        }
    }

    /// Process the given sample info hashes response.
    ///
    /// # Arguments
    ///
    /// * `key` - The transaction key of the query.
    /// * `addr`- The source address of the node.
    /// * `pending_request` - The pending request of the query.
    /// * `response` - The received sample info hashes response.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn on_sample_info_hashes_response(
        &mut self,
        key: &TransactionKey,
        addr: &SocketAddr,
        pending_request: PendingRequest,
        response: SampleInfoHashesResponse,
        traversal: &mut TraversalAlgorithm,
    ) {
        if !response.id.verify_id(&addr.ip()) {
            debug!("{} detected spoofed sample_infohashes from {}", self, key);
            Self::resolve_as_err(pending_request.request_type, Error::InvalidNodeId);
            return;
        }

        // update the refresh interval for the node
        // this will allow us to index info hashes from this node in the future
        if let Some(node) = self.routing_table.find_node(&response.id) {
            node.update_indexing_interval(Duration::from_secs(response.interval as u64))
                .await;
        }

        // add the announced nodes to the traversal algorithm
        let nodes = Self::collect_nodes(&response.nodes, &response.nodes6);
        for node in nodes {
            traversal.add_node(Some(node.id), node.addr);
        }

        // add the info hashes to the peer storage
        for info_hash in &response.samples {
            self.handler.register(info_hash);
            self.callbacks
                .invoke(DhtEvent::InfoHashAdded(info_hash.clone()));
        }

        // update the metrics
        self.metrics
            .discovered_info_hashes
            .set(self.handler.torrents().count() as u64);

        match pending_request.request_type {
            Some(PendingRequestType::ScrapeInfoHashes(tx)) => {
                let _ = tx.send(Ok(response.samples));
            }
            Some(_) => Self::resolve_as_err(
                pending_request.request_type,
                Error::InvalidMessage(format!(
                    "expected {} response, got sample_infohashes instead",
                    pending_request.query_name
                )),
            ),
            _ => {}
        }
    }

    /// Process a received put response.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn on_put_response(
        &self,
        key: &TransactionKey,
        addr: &SocketAddr,
        pending_request: PendingRequest,
        response: PutResponse,
    ) {
        if !response.id.verify_id(&addr.ip()) {
            debug!("{} detected spoofed put from {}", self, key);
            Self::resolve_as_err(pending_request.request_type, Error::InvalidNodeId);
            return;
        }
        self.node_query_result(&addr, true).await;
        match pending_request.request_type {
            Some(PendingRequestType::Put(tx)) => {
                let _ = tx.send(Ok(()));
            }
            Some(_) => Self::resolve_as_err(
                pending_request.request_type,
                Error::InvalidMessage(format!(
                    "expected {} response, got put instead",
                    pending_request.query_name
                )),
            ),
            _ => {}
        }
    }

    /// Process a received get response.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn on_get_response(
        &mut self,
        key: &TransactionKey,
        addr: &SocketAddr,
        pending_request: PendingRequest,
        response: GetResponse,
        traversal: &mut TraversalAlgorithm,
    ) {
        if !response.id.verify_id(&addr.ip()) {
            debug!("{} detected spoofed get from {}", self, key);
            Self::resolve_as_err(pending_request.request_type, Error::InvalidNodeId);
            return;
        }
        self.node_query_result(&addr, true).await;

        // update the write token for the node
        if let Some(token) = response.token.as_ref() {
            if let Err(e) = self.update_announce_token(&response.id, token).await {
                Self::resolve_as_err(pending_request.request_type, e);
                return;
            }
        }

        // extract the mutable info from the pending request
        let (request_public_key, salt, tx) = match pending_request.request_type {
            Some(PendingRequestType::Get {
                public_key,
                salt,
                response,
            }) => (public_key, salt, response),
            Some(_) => {
                Self::resolve_as_err(
                    pending_request.request_type,
                    Error::InvalidMessage(format!(
                        "expected {} response, got get instead",
                        pending_request.query_name
                    )),
                );
                return;
            }
            _ => {
                error!("{} received get response for empty request", self);
                return;
            }
        };

        // extract response data
        let sequence_nr = response.sequence_nr;
        let public_key = match response
            .public_key
            .map(TryInto::<PublicKey>::try_into)
            .transpose()
        {
            Ok(key) => key,
            Err(e) => {
                debug!("{} failed to parse public key from {}", self, addr);
                let _ = tx.send(Err(Error::Parse(format!(
                    "failed to parse public key (len {})",
                    e.len()
                ))));
                return;
            }
        };
        let signature = response.signature;
        let value = response.value;

        // store the received value
        if let Some(value) = value.as_ref().cloned() {
            // validate that both public keys match
            if request_public_key != public_key {
                debug!("{} public keys are not matching for {}", self, addr);
                let _ = tx.send(Err(Error::InvalidMessage(
                    "public keys are not matching".to_string(),
                )));
                return;
            }

            // if the value is mutable, verify it before we store it
            if let Some(public_key) = public_key.as_ref() {
                if let Err(e) = self.verify_value(
                    &value,
                    sequence_nr.as_ref(),
                    &public_key,
                    salt.as_ref().map(AsRef::as_ref),
                    signature.as_ref().map(AsRef::as_ref),
                ) {
                    debug!("{} value validation failed for {}, {}", self, addr, e);
                    let _ = tx.send(Err(e));
                    return;
                }
            }

            let mutable_properties = match parse_mutable_item_properties(
                sequence_nr.clone(),
                public_key,
                None,
                signature.clone(),
            ) {
                Ok(properties) => properties,
                Err(e) => {
                    debug!("{} failed to parse mutable item properties, {}", self, e);
                    return;
                }
            };

            if let Err(e) = self.handler.store(value, mutable_properties) {
                if e != Error::AlreadyExists {
                    warn!("{} failed to store immutable item, {}", self, e);
                }
            }
        }

        // add the closest nodes to the traversal algorithm
        let nodes = Self::collect_nodes(&response.nodes, &response.nodes6);
        for node in nodes.iter() {
            traversal.add_node(Some(node.id), node.addr);
        }

        let _ = tx.send(Ok(GetResult { value, nodes }));
    }

    /// Process a received ping response.
    ///
    /// # Arguments
    ///
    /// * `key` - The transaction key of the query.
    /// * `addr`- The source address of the node.
    /// * `pending_request` - The pending request of the query.
    /// * `response` - The ping response of the node.
    #[cfg_attr(feature = "tracing", instrument)]
    async fn on_ping_response(
        &self,
        key: &TransactionKey,
        addr: &SocketAddr,
        pending_request: PendingRequest,
        response: PingMessage,
    ) {
        if !response.id.verify_id(&addr.ip()) {
            debug!("{} detected spoofed ping from {}", self, key);
            Self::resolve_as_err(pending_request.request_type, Error::InvalidNodeId);
            return;
        }

        self.node_query_result(&addr, true).await;
        match pending_request.request_type {
            Some(PendingRequestType::Ping(tx)) => {
                let _ = tx.send(Ok(NodeKey {
                    id: response.id,
                    addr: *addr,
                }));
            }
            Some(_) => Self::resolve_as_err(
                pending_request.request_type,
                Error::InvalidMessage(format!(
                    "expected {} response, got ping instead",
                    pending_request.query_name
                )),
            ),
            _ => {}
        }
    }

    /// Process the received find node response.
    ///
    /// # Arguments
    ///
    /// * `key` - The transaction key of the query.
    /// * `addr`- The source address of the node.
    /// * `pending_request` - The pending request of the query.
    /// * `response` - The received find node response.
    #[cfg_attr(feature = "tracing", instrument)]
    async fn on_find_node_response(
        &self,
        key: &TransactionKey,
        addr: &SocketAddr,
        pending_request: PendingRequest,
        response: FindNodeResponse,
    ) {
        if !response.id.verify_id(&addr.ip()) {
            debug!("{} detected spoofed find_node from {}", self, key);
            Self::resolve_as_err(pending_request.request_type, Error::InvalidNodeId);
            return;
        }

        self.node_query_result(&addr, true).await;
        let nodes = response
            .nodes
            .as_slice()
            .into_iter()
            .map(|e| NodeKey {
                id: e.id,
                addr: SocketAddr::from(&e.addr),
            })
            .chain(response.nodes6.as_slice().into_iter().map(|e| NodeKey {
                id: e.id,
                addr: SocketAddr::from(&e.addr),
            }))
            .collect::<Vec<_>>();

        debug!(
            "{} node {} discovered a total of {} nodes",
            self,
            addr,
            nodes.len()
        );
        match pending_request.request_type {
            Some(PendingRequestType::FindNode(tx)) => {
                let _ = tx.send(Ok(nodes));
            }
            Some(_) => Self::resolve_as_err(
                pending_request.request_type,
                Error::InvalidMessage(format!(
                    "expected {} response, got find_node instead",
                    pending_request.query_name
                )),
            ),
            _ => {}
        }
    }

    /// Process a received response message for a query.
    #[cfg_attr(feature = "tracing", instrument(skip(self, traversal)))]
    async fn on_get_peers_response(
        &mut self,
        key: &TransactionKey,
        addr: &SocketAddr,
        pending_request: PendingRequest,
        response: GetPeersResponse,
        traversal: &mut TraversalAlgorithm,
    ) {
        if !response.id.verify_id(&addr.ip()) {
            debug!("{} detected spoofed get_peers from {}", self, key);
            Self::resolve_as_err(pending_request.request_type, Error::InvalidNodeId);
            return;
        }

        self.node_query_result(&addr, true).await;
        if let Some(token) = response.token.as_ref() {
            if let Err(e) = self.update_announce_token(&response.id, token).await {
                Self::resolve_as_err(pending_request.request_type, e);
                return;
            }
        }

        let peers: Vec<SocketAddr> = response
            .values
            .iter()
            .map(|e| SocketAddr::from(e))
            .collect();

        let nodes = Self::collect_nodes(&response.nodes, &response.nodes6);
        for node in nodes.iter() {
            traversal.add_node(Some(node.id), node.addr);
        }

        let downloaders = response
            .downloaders
            .as_ref()
            .map(|e| e.as_bytes())
            .and_then(|e| BloomFilter::<256>::try_from(e).ok())
            .map(|e| e.len())
            .unwrap_or_default();
        let seeds = response
            .seeds
            .as_ref()
            .map(|e| e.as_bytes())
            .and_then(|e| BloomFilter::<256>::try_from(e).ok())
            .map(|e| e.len())
            .unwrap_or_default();

        match pending_request.request_type {
            Some(PendingRequestType::GetPeers {
                info_hash,
                response,
            }) => {
                for peer in &peers {
                    self.handler.update_peer(info_hash.clone(), *peer, false);
                    self.callbacks
                        .invoke(DhtEvent::PeerUpdated(info_hash.clone(), *peer));
                }

                self.metrics
                    .discovered_peers
                    .set(self.handler.peers_len() as u64);
                let _ = response.send(Ok(GetPeersResult {
                    peers,
                    nodes,
                    downloaders,
                    seeders: seeds,
                }));
            }
            Some(_) => Self::resolve_as_err(
                pending_request.request_type,
                Error::InvalidMessage(format!(
                    "expected {} response, got get_peers instead",
                    pending_request.query_name
                )),
            ),
            _ => {}
        }
    }

    /// Process a received error response message for a query.
    ///
    /// # Arguments
    ///
    /// * `key` - The transaction key of the query.
    /// * `addr` - The address of the peer that sent the error response.
    /// * `message` - The received error message.
    #[cfg_attr(feature = "tracing", instrument)]
    async fn on_error_response(
        &mut self,
        key: &TransactionKey,
        addr: &SocketAddr,
        message: ErrorMessage,
    ) {
        self.metrics.errors.inc();
        self.node_query_result(&addr, false).await;

        if let Some(pending_request) = self.pending_requests.remove(&key) {
            debug!("{} received error for {}", self, key);
            Self::resolve_as_err(pending_request.request_type, Error::from(message))
        } else {
            warn!(
                "{} received error for unknown request, invalid transaction {}",
                self, key
            );
        }
    }

    /// Process a received tracker command.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn handle_command(
        &mut self,
        command: TrackerCommand,
        traversal: &mut TraversalAlgorithm,
    ) {
        match command {
            TrackerCommand::Id { response } => {
                let id = &self.routing_table.id;
                response.send(*id);
            }
            TrackerCommand::Ping { addr, response } => {
                self.ping(&addr, response).await;
            }
            TrackerCommand::FindNode {
                node,
                target_id,
                response,
            } => match self.routing_table.find_node_by_key(&node).cloned() {
                None => response.send(Err(Error::InvalidNodeId)),
                Some(node) => {
                    self.find_node_internal(target_id, &node, Some(response.take()))
                        .await
                }
            },
            TrackerCommand::LookupPeers {
                info_hash,
                response,
            } => {
                response.send(
                    self.handler
                        .peers(&info_hash)
                        .map(|e| e.addr)
                        .collect::<Vec<_>>(),
                );
            }
            TrackerCommand::GetPeers {
                node,
                info_hash,
                scrape,
                response,
            } => {
                let node = self.find_node_or_create(&node);
                self.get_peers(info_hash, &node, false, scrape, response)
                    .await
            }
            TrackerCommand::AnnouncePeer {
                info_hash,
                peer_addr,
                is_seed,
                node,
                response,
            } => match node {
                None => {
                    self.announce_peer_to_network(info_hash, &peer_addr, is_seed)
                        .await;
                    response.send(Ok(()));
                }
                Some(node) => match self.routing_table.find_node_by_key(&node).cloned() {
                    None => response.send(Err(Error::InvalidNodeId)),
                    Some(node) => {
                        self.announce_peer_to(info_hash, &peer_addr, &node, is_seed, response)
                            .await
                    }
                },
            },
            TrackerCommand::ScrapeInfoHashes {
                target,
                node,
                response,
            } => match self.routing_table.find_node_by_key(&node).cloned() {
                None => response.send(Err(Error::InvalidNodeId)),
                Some(node) => {
                    self.scrape_info_hashes(&target, &node, Some(response.take()))
                        .await
                }
            },
            TrackerCommand::Put {
                node,
                value,
                sequence_nr,
                signature,
                public_key,
                salt,
                response,
            } => match self.routing_table.find_node_by_key(&node).cloned() {
                None => response.send(Err(Error::InvalidNodeId)),
                Some(node) => {
                    self.put(
                        &node,
                        value,
                        sequence_nr,
                        signature,
                        public_key,
                        salt,
                        response,
                    )
                    .await
                }
            },
            TrackerCommand::Get {
                node,
                hash,
                sequence_nr,
                public_key,
                salt,
                response,
            } => match self.routing_table.find_node_by_key(&node).cloned() {
                None => response.send(Err(Error::InvalidNodeId)),
                Some(node) => {
                    self.get(&node, hash, sequence_nr, public_key, salt, response)
                        .await
                }
            },
            TrackerCommand::SignValue {
                value,
                sequence_nr,
                salt,
                secret_key,
                response,
            } => response.send(self.item_signature.sign(
                &value,
                &sequence_nr,
                salt.as_ref().map(|e| e.as_ref()),
                &secret_key,
            )),
            TrackerCommand::TotalNodes { response } => response.send(self.routing_table.len()),
            TrackerCommand::GetNode { node, response } => {
                match self.routing_table.find_node_by_key(&node) {
                    None => response.send(None),
                    Some(node) => response.send(Some(node.clone())),
                }
            }
            TrackerCommand::GetNodeById { id, response } => {
                response.send(self.routing_table.find_node(&id).cloned())
            }
            TrackerCommand::GetNodes { response } => {
                response.send(self.routing_table.nodes().cloned().collect::<Vec<_>>());
            }
            TrackerCommand::GetStorageInfoHashes { response } => {
                response.send(self.handler.torrents().cloned().collect());
            }
            TrackerCommand::GetStoragePeers { response } => response.send(
                self.handler
                    .torrents()
                    .map(|info_hash| {
                        (
                            info_hash.clone(),
                            self.handler.peers(&info_hash).cloned().collect(),
                        )
                    })
                    .collect::<HashMap<_, _>>(),
            ),
            TrackerCommand::GoodSearchNodes { response } => {
                response.send(
                    Self::find_good_search_nodes(&self.routing_table)
                        .await
                        .map(|e| *e.key())
                        .collect(),
                );
            }
            TrackerCommand::AddTraversalNode((id, addr)) => traversal.add_node(Some(id), addr),
            TrackerCommand::UpdateExternalIp(ip) => {
                self.update_external_ip(ip).await;
                traversal.restart();
            }
        }
    }

    /// Ping the given node address.
    ///
    /// # Arguments
    ///
    /// * `addr` - the node address to ping.
    /// * `sender` - The result sender for the ping operation.
    #[cfg_attr(feature = "tracing", instrument(skip(self, handler)))]
    async fn ping(&mut self, addr: &SocketAddr, handler: Reply<Result<NodeKey>>) {
        self.send_query(
            QueryMessage::Ping {
                request: PingMessage {
                    id: self.routing_table.id,
                },
            },
            addr,
            Some(PendingRequestType::Ping(handler.take())),
            || async {},
        )
        .await;
    }

    /// Find the closest nodes for the given target node id.
    ///
    /// # Arguments
    ///
    /// * `target` - The target node id to retrieve the closest nodes of.
    /// * `node` - The node to which the address belongs to, if available.
    #[cfg_attr(feature = "tracing", instrument(skip(self, node)))]
    pub(crate) async fn find_node(
        &mut self,
        target: NodeId,
        node: &Node,
    ) -> Response<Vec<NodeKey>, Error> {
        let (tx, rx) = oneshot::channel();
        self.find_node_internal(target, node, Some(tx)).await;
        Response::from(rx)
    }

    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn find_node_internal(
        &mut self,
        target: NodeId,
        node: &Node,
        reply: Option<oneshot::Sender<Result<Vec<NodeKey>>>>,
    ) {
        self.send_query(
            QueryMessage::FindNode {
                request: FindNodeRequest {
                    id: self.routing_table.id,
                    target,
                    want: WantFamily::Ipv4 | WantFamily::Ipv6,
                },
            },
            node.addr(),
            reply.map(PendingRequestType::FindNode),
            || node.failed(),
        )
        .await;
    }

    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn get_peers(
        &mut self,
        info_hash: InfoHash,
        node: &Node,
        no_seed: bool,
        scrape: bool,
        response: Reply<Result<GetPeersResult>>,
    ) {
        self.send_query(
            QueryMessage::GetPeers {
                request: GetPeersRequest {
                    id: self.routing_table.id,
                    info_hash: info_hash.clone(),
                    no_seed,
                    scrape,
                    want: Default::default(),
                },
            },
            node.addr(),
            Some(PendingRequestType::GetPeers {
                info_hash,
                response: response.take(),
            }),
            || node.failed(),
        )
        .await
    }

    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn announce_peer_to(
        &mut self,
        info_hash: InfoHash,
        peer_addr: &SocketAddr,
        node: &Node,
        is_seed: bool,
        response: Reply<Result<()>>,
    ) {
        let token = match node.announce_token().await {
            None => {
                response.send(Err(Error::InvalidToken));
                return;
            }
            Some(token) => token,
        };
        self.send_query(
            QueryMessage::AnnouncePeer {
                request: AnnouncePeerRequest {
                    id: self.routing_table.id,
                    implied_port: false,
                    info_hash: info_hash.clone(),
                    port: peer_addr.port(),
                    token,
                    name: None,
                    seed: is_seed,
                },
            },
            node.addr(),
            Some(PendingRequestType::AnnouncePeer(response.take())),
            || node.failed(),
        )
        .await
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    async fn announce_peer_to_network(
        &mut self,
        info_hash: InfoHash,
        peer_addr: &SocketAddr,
        is_seed: bool,
    ) {
        for node in self.routing_table.nodes().cloned().collect::<Vec<_>>() {
            if let Some(token) = node.announce_token().await {
                self.send_query(
                    QueryMessage::AnnouncePeer {
                        request: AnnouncePeerRequest {
                            id: self.routing_table.id,
                            implied_port: false,
                            info_hash: info_hash.clone(),
                            port: peer_addr.port(),
                            token,
                            name: None,
                            seed: is_seed,
                        },
                    },
                    node.addr(),
                    None,
                    || node.failed(),
                )
                .await
            }
        }
    }

    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn scrape_info_hashes(
        &mut self,
        target: &NodeId,
        node: &Node,
        response: Option<oneshot::Sender<Result<Vec<InfoHash>>>>,
    ) {
        self.send_query(
            QueryMessage::SampleInfoHashes {
                request: SampleInfoHashesRequest {
                    id: self.routing_table.id,
                    target: *target,
                },
            },
            node.addr(),
            response.map(PendingRequestType::ScrapeInfoHashes),
            || node.failed(),
        )
        .await
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    async fn index_info_hashes(&mut self) {
        let node_id = self.routing_table.id;
        let search_nodes = Self::find_good_search_nodes(&self.routing_table)
            .await
            .cloned()
            .collect::<Vec<_>>();

        for node in &search_nodes {
            // check if an index interval is known for the node
            // if so, and we have a last index timestamp, check that we're allowed to index the node again
            match (node.last_indexed().await, node.indexing_interval().await) {
                (Some(last_indexed), Some(interval)) => {
                    if Instant::now().duration_since(last_indexed) < interval {
                        continue;
                    }
                }
                _ => {}
            }

            self.scrape_info_hashes(&node_id, node, None).await;
        }
    }

    /// Send an immutable `put` item request to the given node.
    /// The response will eventually be sent to the given reply channel.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn put(
        &mut self,
        node: &Node,
        value: Value,
        sequence_nr: Option<u64>,
        signature: Option<SignatureBytes>,
        public_key: Option<PublicKey>,
        salt: Option<Vec<u8>>,
        response: Reply<Result<()>>,
    ) {
        let token = match node.announce_token().await {
            None => {
                response.send(Err(Error::InvalidToken));
                return;
            }
            Some(token) => token,
        };
        let mutable_properties = match parse_mutable_item_properties(
            sequence_nr,
            public_key,
            salt.clone(),
            signature.map(|e| e.to_vec()),
        ) {
            Ok(properties) => properties,
            Err(e) => {
                response.send(Err(e));
                return;
            }
        };
        let key = match self.handler.store(value.clone(), mutable_properties) {
            Err(e) => {
                response.send(Err(e));
                return;
            }
            Ok(key) => key,
        };
        let (cas, sequence_nr, public_key, signature) = match self
            .handler
            .get(&key)
            .and_then(|e| e.mutable_properties.as_ref())
        {
            None => (None, None, None, None),
            Some(properties) => {
                let cas = if properties.sequence_nr > 0 {
                    Some(properties.sequence_nr - 1)
                } else {
                    None
                };

                (
                    cas,
                    Some(properties.sequence_nr),
                    Some(properties.public_key.to_vec()),
                    Some(properties.signature.to_vec()),
                )
            }
        };

        self.send_query(
            QueryMessage::Put {
                request: PutRequest {
                    id: self.routing_table.id,
                    token,
                    value,
                    cas,
                    sequence_nr,
                    public_key,
                    signature,
                    salt,
                },
            },
            node.addr(),
            Some(PendingRequestType::Put(response.take())),
            || node.failed(),
        )
        .await
    }

    /// Send a `get` item request to the given node.
    ///
    ///
    /// The response will eventually be sent to the given reply channel.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn get(
        &mut self,
        node: &Node,
        hash: Sha1Hash,
        sequence_nr: Option<u64>,
        public_key: Option<PublicKey>,
        salt: Option<Vec<u8>>,
        response: Reply<Result<GetResult>>,
    ) {
        // let target = hex::encode(hash);
        self.send_query(
            QueryMessage::Get {
                request: GetRequest {
                    id: self.routing_table.id,
                    target: hash,
                    sequence_nr,
                },
            },
            node.addr(),
            Some(PendingRequestType::Get {
                public_key,
                salt,
                response: response.take(),
            }),
            || node.failed(),
        )
        .await
    }

    #[cfg_attr(feature = "tracing", instrument)]
    async fn update_external_ip(&mut self, ip: IpAddr) {
        let new_node_id = NodeId::from_ip(&ip);
        let existing_nodes = self.routing_table.nodes().cloned().collect::<Vec<_>>();
        let bucket_size = self.routing_table.bucket_size;

        // replace the routing table
        self.routing_table = RoutingTable::new(new_node_id, bucket_size);
        for node in existing_nodes {
            let _ = self.routing_table.add_node(node).await;
        }

        debug!("{} detected external IP {}", self, ip);
        let _ = self.callbacks.invoke(DhtEvent::ExternalIpChanged(ip));
    }

    /// Try to send a new query to the given node address.
    /// Returns a [Response] for the send query to the given node address.
    ///
    /// # Arguments
    ///
    /// * `query` - The query to send to the node address.
    /// * `addr` - The address to send the query to.
    /// * `pending_request` - The request to resolve once a response has been received for this query.
    /// * `on_failed` - The closure to execute when the query couldn't be sent.
    #[cfg_attr(feature = "tracing", instrument(skip(self, on_failed)))]
    async fn send_query<'a, F>(
        &mut self,
        query: QueryMessage,
        addr: &SocketAddr,
        pending_request: Option<PendingRequestType>,
        on_failed: F,
    ) where
        F: AsyncFnOnce(),
    {
        // validate the remote node address
        if addr.ip().is_unspecified() || addr.port() == 0 {
            Self::resolve_as_err(pending_request, Error::InvalidAddr);
            return;
        }

        let name = query.name().to_string();
        let id = self.next_transaction_id();
        let message = match Message::builder()
            .transaction_id(id.clone())
            .payload(MessagePayload::Query(query))
            .read_only(self.handler.is_read_only())
            .build()
        {
            Ok(message) => message,
            Err(e) => {
                Self::resolve_as_err(pending_request, e);
                return;
            }
        };

        debug!(
            "{} is sending query \"{}\" (transaction {}) to {}",
            self, name, id, addr
        );
        match self.send(message, addr).await {
            Ok(_) => {
                self.pending_requests.insert(
                    TransactionKey { id, addr: *addr },
                    PendingRequest {
                        query_name: name,
                        request_type: pending_request,
                        timestamp_sent: Instant::now(),
                    },
                );
                self.metrics
                    .pending_queries
                    .set(self.pending_requests.len() as u64);
            }
            Err(e) => {
                on_failed().await;
                Self::resolve_as_err(pending_request, e);
            }
        }
    }

    /// Send the given response for a query message.
    ///
    /// # Arguments
    ///
    /// * `transaction_id` - The original query transaction id.
    /// * `response` - The response payload.
    /// * `addr` - The node address to send the response to.
    ///
    /// # Returns
    ///
    /// It returns an error if the response failed to send.
    async fn send_response(
        &self,
        transaction_id: TransactionId,
        message: ResponseMessage,
        addr: &SocketAddr,
    ) -> Result<()> {
        let message = Message::builder()
            .transaction_id(transaction_id)
            .version(Version::from(VERSION_IDENTIFIER))
            .payload(MessagePayload::Response(ResponsePayload::Message(message)))
            .ip((*addr).into())
            .port(addr.port())
            .build()?;

        self.send(message, addr).await
    }

    /// Send the given error response for a query message.
    ///
    /// # Arguments
    ///
    /// * `transaction_id` - The original transaction id of the message.
    /// * `error` - The error payload.
    /// * `addr` - The node address to send the response to.
    #[cfg_attr(feature = "tracing", instrument(skip_all, err))]
    async fn send_error(
        &self,
        transaction_id: TransactionId,
        error: ErrorMessage,
        addr: &SocketAddr,
    ) -> Result<()> {
        let message = Message::builder()
            .transaction_id(transaction_id)
            .payload(MessagePayload::error(error))
            .ip((*addr).into())
            .port(addr.port())
            .read_only(self.handler.is_read_only())
            .build()?;

        self.send(message, addr).await
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self), err))]
    async fn send(&self, message: Message, addr: &SocketAddr) -> Result<()> {
        if self.cancellation_token.is_cancelled() {
            return Err(Error::Closed);
        }

        let bytes = serde_bencode::to_bytes(&message)?;

        trace!(
            "{} is sending message ({} bytes, transaction {}) to {}, {:?}",
            self,
            bytes.len(),
            message
                .transaction_id_as_str()
                .map(|e| e.to_string())
                .unwrap_or_else(|| message.transaction_id_as_u32().to_string()),
            addr,
            message
        );
        let start_time = Instant::now();
        timeout(
            self.send_timeout,
            self.socket.send_to(bytes.as_slice(), addr),
        )
        .await
        .map_err(|_| {
            Error::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("connection to {} has timed out", addr),
            ))
        })?
        .map_err(Error::from)?;
        let elapsed = start_time.elapsed();
        trace!(
            "{} sent {} bytes to {} in {}.{:03}ms",
            self,
            bytes.len(),
            addr,
            elapsed.as_millis(),
            elapsed.as_micros()
        );

        self.metrics.bytes_out.inc_by(bytes.len() as u64);
        Ok(())
    }

    /// Find an existing node within the routing table, or create a temporary new one.
    ///
    /// The temporary new node will **not be stored** within the routing table.
    fn find_node_or_create(&self, node: &NodeKey) -> Node {
        self.routing_table
            .find_node_by_key(&node)
            .cloned()
            .unwrap_or_else(|| Node::from(*node))
    }

    /// Update the nodes information of the given node.
    async fn update_node(
        &mut self,
        id: NodeId,
        addr: SocketAddr,
        traversal: &mut TraversalAlgorithm,
    ) {
        match self.routing_table.find_node(&id) {
            Some(node) => {
                node.seen().await;
            }
            None => {
                let node = Node::new(id, addr);
                let node_key = node.key().clone();

                // traverse the node
                traversal.add_node(Some(id), addr);

                match self.routing_table.add_node(node).await {
                    Ok(bucket_index) => {
                        self.metrics.nodes.set(self.routing_table.len() as u64);
                        debug!(
                            "{} added verified node {} to bucket {}",
                            self, node_key.addr, bucket_index
                        );

                        self.callbacks.invoke(DhtEvent::NodeAdded(node_key));
                    }
                    Err(e) => {
                        trace!(
                            "{} failed to add verified node {}, {}",
                            self,
                            node_key.addr,
                            e
                        );
                    }
                }
            }
        }
    }

    async fn node_query_result(&self, node_addr: &SocketAddr, success: bool) {
        if let Some(node) = self.routing_table.nodes().find(|e| e.addr() == node_addr) {
            if success {
                node.confirmed().await;
            } else {
                node.failed().await;
            }
        };
    }

    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn tick(&mut self) {
        self.callbacks
            .invoke(DhtEvent::Stats(self.metrics.snapshot()));

        self.metrics.tick(TICK_INTERVAL);
        self.routing_table.tick(TICK_INTERVAL);
        self.handler.tick().await;
        self.cleanup_pending_requests().await;
    }

    /// Refresh the nodes within the routing table.
    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    async fn refresh_routing_table(&mut self) {
        trace!("{} is refreshing nodes within routing table", self);

        let mut nodes_to_query = vec![];
        for bucket in self.routing_table.buckets() {
            let nodes_last_seen =
                futures::future::join_all(bucket.nodes.iter().map(|e| e.last_seen())).await;

            // rotate all bucket node secret tokens when needed
            futures::future::join_all(bucket.nodes.iter().map(|e| e.rotate_token_secret())).await;

            // check if all nodes within the bucket need to be refreshed
            // Buckets that have not been changed in 15 minutes should be "refreshed."
            if nodes_last_seen
                .into_iter()
                .all(|e| e.elapsed() > REFRESH_TIMEOUT)
            {
                let target_node = bucket
                    .nodes
                    .first()
                    .map(|e| *e.id())
                    .unwrap_or_else(|| self.routing_table.id);
                nodes_to_query.push((target_node, bucket.nodes.clone()));
            }
        }

        for (target_node, nodes) in nodes_to_query {
            for node in nodes {
                let _ = self.find_node_internal(target_node, &node, None).await;
            }
        }
    }

    /// Cleanup pending requests which have not received a response.
    async fn cleanup_pending_requests(&mut self) {
        let now = Instant::now();
        let timed_out_request_keys: Vec<_> = self
            .pending_requests
            .iter()
            .filter(|(_, request)| now - request.timestamp_sent >= RESPONSE_TIMEOUT)
            .map(|(key, _)| key.clone())
            .collect();

        if timed_out_request_keys.is_empty() {
            return;
        }

        trace!(
            "{} is cleaning a total of {} timed-out requests",
            self,
            timed_out_request_keys.len()
        );
        for key in timed_out_request_keys {
            self.node_query_result(&key.addr, false).await;
            if let Some(request) = self.pending_requests.remove(&key) {
                Self::resolve_as_err(request.request_type, Error::Timeout);
            }
        }

        self.metrics
            .pending_queries
            .set(self.pending_requests.len() as u64);
    }

    /// Try to update the announce token for the given node ID.
    ///
    /// It returns an error when the node ID couldn't be found within the routing table or the token value is invalid.
    async fn update_announce_token(&self, id: &NodeId, token: &NodeToken) -> Result<()> {
        let node = self
            .routing_table
            .find_node(id)
            .ok_or(Error::InvalidNodeId)?;
        node.update_announce_token(token.clone()).await;
        trace!("{} updated announce token for {}", self, id);
        Ok(())
    }

    /// Get the next transaction ID for sending a new message.
    /// The transaction ID within the server will be automatically wrapped when [u32::MAX] has been reached.
    fn next_transaction_id(&mut self) -> TransactionId {
        let new = self.transaction_id.wrapping_add(1);
        self.transaction_id = new;
        let id = format!("{:02x}", new);
        id.into_bytes().into()
    }

    /// Verify the given mutable value.
    fn verify_value<V>(
        &self,
        value: &V,
        sequence_nr: Option<&u64>,
        public_key: &PublicKey,
        salt: Option<&[u8]>,
        signature: Option<&[u8]>,
    ) -> Result<()>
    where
        V: Serialize,
    {
        let signature = signature.ok_or(Error::InvalidSignature).and_then(|e| {
            TryInto::<SignatureBytes>::try_into(e).map_err(|_| Error::InvalidSignature)
        })?;
        let sequence_nr = sequence_nr.ok_or(Error::InvalidSequenceNr)?;

        self.item_signature.verify(
            &value,
            &sequence_nr,
            &public_key,
            salt.as_ref().map(AsRef::as_ref),
            &signature,
        )
    }

    fn resolve_as_err(request_type: Option<PendingRequestType>, err: Error) {
        if let Some(request_type) = request_type {
            match request_type {
                PendingRequestType::Ping(tx) => {
                    let _ = tx.send(Err(err));
                }
                PendingRequestType::FindNode(tx) => {
                    let _ = tx.send(Err(err));
                }
                PendingRequestType::AnnouncePeer(tx) => {
                    let _ = tx.send(Err(err));
                }
                PendingRequestType::GetPeers { response, .. } => {
                    let _ = response.send(Err(err));
                }
                PendingRequestType::ScrapeInfoHashes(tx) => {
                    let _ = tx.send(Err(err));
                }
                PendingRequestType::Put(tx) => {
                    let _ = tx.send(Err(err));
                }
                PendingRequestType::Get { response, .. } => {
                    let _ = response.send(Err(err));
                }
            }
        }
    }

    /// Returns all non [NodeState::Bad] search nodes from the routing table
    async fn find_good_search_nodes(routing_table: &RoutingTable) -> impl Iterator<Item = &Node> {
        let nodes_with_state =
            futures::future::join_all(routing_table.nodes().map(|node| async move {
                let state = node.state().await;
                (node, state)
            }))
            .await;

        nodes_with_state.into_iter().flat_map(|(node, state)| {
            if state != NodeState::Bad {
                Some(node)
            } else {
                None
            }
        })
    }

    /// Collect the IPv4 and IPv6 nodes from the compact nodes.
    fn collect_nodes(nodes: &CompactIpNodes, nodes6: &CompactIPv6Nodes) -> Vec<NodeKey> {
        let nodes = match nodes {
            CompactIpNodes::IPv4(nodes) => {
                Either::Left(nodes.as_slice().into_iter().map(|e| NodeKey {
                    id: e.id,
                    addr: SocketAddr::from(&e.addr),
                }))
            }
            CompactIpNodes::IPv6(nodes) => {
                Either::Right(nodes.as_slice().into_iter().map(|e| NodeKey {
                    id: e.id,
                    addr: SocketAddr::from(&e.addr),
                }))
            }
        };

        nodes
            .chain(nodes6.as_slice().into_iter().map(|e| NodeKey {
                id: e.id,
                addr: SocketAddr::from(&e.addr),
            }))
            .collect()
    }
}

#[derive(Debug)]
enum ReaderMessage {
    Message {
        message: Message,
        message_len: usize,
        addr: SocketAddr,
    },
    Error {
        error: Error,
        payload_len: usize,
        addr: SocketAddr,
    },
}

#[derive(Debug, Display)]
#[display("DHT node reader [{}]", addr_port)]
struct NodeReader {
    socket: Arc<UdpSocket>,
    addr_port: u16,
    sender: UnboundedSender<ReaderMessage>,
    cancellation_token: CancellationToken,
}

impl NodeReader {
    /// Start the main reader loop of a node server.
    /// This will handle incoming packets and parse them before delivering them to the node server.
    async fn run(&self) {
        loop {
            let mut buffer = [0u8; MAX_PACKET_SIZE];
            select! {
                _ = self.cancellation_token.cancelled() => break,
                Ok((len, addr)) = self.socket.recv_from(&mut buffer) => {
                    if let Err(e) = self.handle_incoming_message(&buffer[0..len], addr).await {
                        let _ = self.sender.send(ReaderMessage::Error { error: e, payload_len: len, addr });
                    }
                },
            }
        }
        debug!("{} main loop ended", self);
    }

    async fn handle_incoming_message(&self, bytes: &[u8], addr: SocketAddr) -> Result<()> {
        // check if the port of the sender is known
        if addr.port() == 0 {
            trace!(
                "{} received packet with unknown port, ignoring packet message",
                self
            );
            return Ok(());
        }

        let start_time = Instant::now();
        let message = serde_bencode::from_bytes::<Message>(bytes).map_err(|e| {
            trace!(
                "{} failed to parse incoming message, {}\n{}",
                self,
                e,
                String::from_utf8_lossy(bytes)
            );
            e
        })?;
        let elapsed = start_time.elapsed();
        trace!(
            "{} read {} bytes from {} in {}.{:03}ms",
            self,
            bytes.len(),
            addr,
            elapsed.as_millis(),
            elapsed.as_micros(),
        );

        let message_len = bytes.len();
        let _ = self.sender.send(ReaderMessage::Message {
            message,
            message_len,
            addr,
        });
        Ok(())
    }
}

/// Represents a request that has been sent to a DHT node and is awaiting a response.
#[derive(Debug)]
struct PendingRequest {
    query_name: String,
    request_type: Option<PendingRequestType>,
    timestamp_sent: Instant,
}

/// The type of a pending request.
/// It determines which result should be sent back to the waiter.
#[derive(Debug)]
enum PendingRequestType {
    Ping(oneshot::Sender<Result<NodeKey>>),
    FindNode(oneshot::Sender<Result<Vec<NodeKey>>>),
    AnnouncePeer(oneshot::Sender<Result<()>>),
    GetPeers {
        info_hash: InfoHash,
        response: oneshot::Sender<Result<GetPeersResult>>,
    },
    ScrapeInfoHashes(oneshot::Sender<Result<Vec<InfoHash>>>),
    Put(oneshot::Sender<Result<()>>),
    Get {
        public_key: Option<PublicKey>,
        salt: Option<Vec<u8>>,
        response: oneshot::Sender<Result<GetResult>>,
    },
}

#[derive(Debug, Display, Clone, PartialEq, Eq, Hash)]
#[display("{}[{}]", addr, id)]
struct TransactionKey {
    pub id: TransactionId,
    pub addr: SocketAddr,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    mod new {
        use super::*;

        #[tokio::test]
        async fn test_new() {
            init_logger!();
            let node_id = NodeId::new();
            let verifier = ItemSignature::new().unwrap();
            let tracker = DhtTracker::new(Config {
                id: node_id,
                mode: Mode::Server,
                max_torrents: 1,
                info_hash_indexing_enabled: true,
                info_hash_indexing_interval: Duration::from_secs(20),
                item_signature: Some(verifier),
                routing_nodes: vec![],
            })
            .await
            .expect("expected a new DHT server");

            // verify the tracker id
            let result = tracker.id().await.unwrap();
            assert_eq!(
                node_id, result,
                "expected a new random node ID to have been generated"
            );
            assert_ne!(
                0,
                tracker.port(),
                "expected a server port to have been present"
            );
        }

        #[tokio::test]
        async fn test_read_only() {
            init_logger!();
            let source_config = {
                let mut config = Config::default();
                config.mode = Mode::Client;
                config
            };
            let source = DhtTracker::new(source_config).await.unwrap();
            let target = DhtTracker::new(Config::default()).await.unwrap();
            let source_id = source.id().await.expect("expected a source node id");
            let source_addr = (Ipv4Addr::LOCALHOST, source.addr().port()).into();
            let target_addr = (Ipv4Addr::LOCALHOST, target.addr().port()).into();

            // ping the target node
            let _ = source
                .ping(target_addr)
                .await
                .expect("expected the ping to succeed");

            // retrieve the node info from the target
            let key = NodeKey {
                id: source_id,
                addr: source_addr,
            };
            let result = target.node(&key).await;

            assert_eq!(
                None, result,
                "expected the read-only node to not have been added to the routing table"
            );
        }
    }

    mod ping {
        use super::*;

        #[tokio::test]
        async fn test_ping_valid_address() {
            init_logger!();
            let (source, target) = create_node_server_pair!();
            let incoming_id = source.id().await.unwrap();
            let outgoing_id = target.id().await.unwrap();
            let source_addr = (Ipv4Addr::LOCALHOST, source.port()).into();

            let result = timeout!(
                target.ping(source_addr),
                Duration::from_millis(750),
                "failed to ping node"
            );
            assert!(
                result.is_ok(),
                "expected ping to succeed, got {:?} instead",
                result
            );

            // check if the incoming server has added the node that pinged it
            let result = source.node_by_id(&outgoing_id).await;
            assert!(
                result.is_some(),
                "expected the outgoing node {:?} to be added",
                outgoing_id
            );

            // check if the outgoing server has added the pinged target node
            let result = target.nodes().await;
            assert!(
                result.iter().find(|e| e.id() == &incoming_id).is_some(),
                "expected the incoming node {:?} to be added",
                incoming_id
            );
        }

        #[tokio::test]
        async fn test_ping_invalid_address() {
            init_logger!();
            let addr = SocketAddr::from(([0, 0, 0, 0], 9000));
            let tracker = DhtTracker::builder()
                .max_torrents(1)
                .enable_indexing(false)
                .build()
                .await
                .unwrap();

            let result = timeout!(
                tracker.ping(addr),
                Duration::from_millis(750),
                "failed to ping node"
            );

            assert_eq!(
                Err(Error::InvalidAddr),
                result,
                "expected an invalid address error"
            );
        }
    }

    mod find_node {
        use super::*;

        #[tokio::test]
        async fn test_find_node() {
            init_logger!();
            let rand = 2;
            let search_node_id = NodeId::from_ip_with_rand(&[132, 141, 12, 40].into(), rand);
            let incoming_id = NodeId::from_ip_with_rand(&Ipv4Addr::LOCALHOST.into(), rand);
            let outgoing_id = NodeId::from_ip_with_rand(&Ipv4Addr::LOCALHOST.into(), rand);
            let outgoing = DhtTracker::builder()
                .node_id(outgoing_id)
                .max_torrents(16)
                .enable_indexing(false)
                .build()
                .await
                .unwrap();
            let mut incoming = create_tracker_context!(incoming_id);

            // register the incoming tracker with the outgoing tracker
            let (tx, rx) = oneshot::channel();
            let incoming_addr = (Ipv4Addr::LOCALHOST, incoming.socket_addr.port()).into();
            tokio::spawn(async move {
                let result = outgoing.add_node(&incoming_addr).await;
                let _ = tx.send((result, outgoing));
            });

            // process the incoming ping message
            let (sender, _receiver) = channel!(1);
            let mut observer = Observer::new(sender.clone());
            let mut traversal = TraversalAlgorithm::new(8, vec![], sender);
            let message = timeout(Duration::from_millis(500), incoming.receiver.recv())
                .await
                .unwrap()
                .expect("expected a message");
            incoming
                .on_message_received(message, &mut observer, &mut traversal)
                .await;

            // verify the result of the add_node operation
            let (result, outgoing) = timeout!(
                rx,
                Duration::from_millis(750),
                "timed out while adding the node"
            )
            .unwrap();
            assert_eq!(result, Ok(()), "expected the node to be added successfully");

            // calculate the bucket which will be retrieved by the search node
            let bucket_index = incoming_id.distance(&search_node_id);
            // create a node which matches the search bucket index
            let nearby_node = create_bucket_matching_node(bucket_index, incoming_id);
            incoming
                .update_node(
                    nearby_node,
                    ([132, 141, 45, 30], 8090).into(),
                    &mut traversal,
                )
                .await;

            // process the incoming node task
            tokio::spawn(async move {
                let (_sender, receiver) = channel!(1);
                incoming
                    .run(Duration::from_secs(60), observer, traversal, receiver)
                    .await;
            });

            // request the node info from the nearby node
            let result = outgoing
                .find_nodes(&search_node_id, Duration::from_millis(500))
                .await
                .expect("expected to retrieve relevant nodes");
            assert_eq!(1, result.len(), "expected one node to have been present");
        }
    }

    mod get_peers {
        use super::*;
        use crate::channel::channel;
        use std::str::FromStr;

        #[tokio::test]
        async fn test_get_peers() {
            init_logger!();
            let info_hash = InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7")
                .expect("expected a valid hash");
            let (incoming, outgoing) = create_node_server_pair!();

            // register the incoming tracker with the outgoing tracker
            let incoming_addr = (Ipv4Addr::LOCALHOST, incoming.addr().port()).into();
            let result = outgoing.add_node(&incoming_addr).await;
            assert_eq!(result, Ok(()), "expected the node to be added successfully");

            let result = outgoing
                .get_peers(&info_hash, 2, Duration::from_secs(2))
                .await
                .expect("expected to get peers");
            assert_eq!(
                Vec::<SocketAddr>::with_capacity(0),
                result,
                "expected an empty peers list to have been returned"
            );
        }

        #[tokio::test]
        async fn test_get_peers_from() {
            init_logger!();
            let info_hash =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let (source, target) = create_node_server_pair!();
            let source_addr = (Ipv4Addr::LOCALHOST, source.port()).into();

            let source_key = target.ping(source_addr).await.unwrap();
            let result = target
                .get_peers_from(&info_hash, &source_key, 1, Duration::from_secs(1))
                .await;
            assert!(
                result.is_ok(),
                "expected the peers to have been queried, but got {:?}",
                result
            );
        }

        #[tokio::test]
        async fn test_get_peers_from_storage() {
            init_logger!();
            let info_hash =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let (sender, receiver) = channel(2);
            let mut context = create_tracker_context!();
            let tracker = DhtTracker {
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 7890)),
                mode: Mode::Server,
                metrics: Default::default(),
                sender: sender.clone(),
                callbacks: MultiThreadedCallback::new(),
                cancellation_token: Default::default(),
            };
            let peer = SocketAddr::from((Ipv4Addr::LOCALHOST, 8000)).into();

            // spawn a new task for the context with an already stored peer
            let inner_info_hash = info_hash.clone();
            tokio::spawn(async move {
                context.handler.update_peer(inner_info_hash, peer, false);

                context
                    .run(
                        Duration::from_secs(60),
                        Observer::new(sender.clone()),
                        TraversalAlgorithm::new(8, vec![], sender),
                        receiver,
                    )
                    .await;
            });

            // request the peers from the tracker which has no nodes within the network
            let result = tracker
                .get_peers(&info_hash, 1, Duration::from_secs(2))
                .await
                .unwrap();

            assert_eq!(
                1,
                result.len(),
                "expected the stored peer to have been returned"
            );
            assert_eq!(peer, result[0]);
        }
    }

    mod announce_peer {
        use super::*;
        use std::str::FromStr;

        #[tokio::test]
        async fn test_announce_peer() {
            init_logger!();
            let info_hash =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let (source, target) = create_node_server_pair!();
            let target_id = target.id().await.unwrap();
            let target_addr = (Ipv4Addr::LOCALHOST, target.port()).into();
            let peer_addr = (Ipv4Addr::LOCALHOST, 8080).into();

            // request peers from the target node
            // this will set the initial announce token in the source tracker for the target node
            let target_key = source.ping(target_addr).await.unwrap();
            let result = source
                .get_peers_from(&info_hash, &target_key, 1, Duration::from_secs(1))
                .await;
            assert!(
                result.is_ok(),
                "expected the peers to have been queried, but got {:?}",
                result
            );

            // verify that an announce token has been written to the source node for the target
            let node = source
                .node_by_id(&target_id)
                .await
                .expect("expected the source node to exist within the target");
            assert!(
                node.announce_token().await.is_some(),
                "expected the target node {} to have an announce token",
                node.id()
            );

            // announce the torrent peer to the target node
            let result = source.announce_peer(&info_hash, &peer_addr, false).await;
            verify_announce_peer(&info_hash, &target, &[peer_addr], result).await;
        }

        #[tokio::test]
        async fn test_announce_peer_to() {
            init_logger!();
            let info_hash =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let (source, target) = create_node_server_pair!();
            let target_id = target.id().await.unwrap();
            let target_addr = (Ipv4Addr::LOCALHOST, target.port()).into();

            // request peers from the target node
            // this will set the initial announce token in the source tracker for the target node
            let target_key = source.ping(target_addr).await.unwrap();
            let result = source
                .get_peers_from(&info_hash, &target_key, 1, Duration::from_secs(1))
                .await;
            assert!(
                result.is_ok(),
                "expected the peers to have been queried, but got {:?}",
                result
            );

            // verify that an announce token has been written to the source node for the target
            let node = source
                .node_by_id(&target_id)
                .await
                .expect("expected the source node to exist within the target");
            assert!(
                node.announce_token().await.is_some(),
                "expected the target node {} to have an announce token",
                node.id()
            );

            // announce the peers to the target node
            let peer1 = (Ipv4Addr::LOCALHOST, 8080).into();
            let peer2 = (Ipv4Addr::LOCALHOST, 17000).into();
            let _ = source
                .announce_peer_to(&info_hash, &peer1, false, &target_key)
                .await
                .unwrap();
            let result = source
                .announce_peer_to(&info_hash, &peer2, true, &target_key)
                .await;
            verify_announce_peer(&info_hash, &target, &[peer1, peer2], result).await;
        }

        async fn verify_announce_peer(
            info_hash: &InfoHash,
            target: &DhtTracker,
            peers: &[SocketAddr],
            result: Result<()>,
        ) {
            assert!(
                result.is_ok(),
                "expected the announce to have been successful, but got {:?}",
                result
            );

            // get the stored peers of the target to which the announcement was made
            // due to a strange race condition in Github, we try a few times
            let result = {
                let mut attempt = 0;
                let mut result = HashMap::new();
                while attempt < 3 {
                    result = target.peers().await;
                    if result.len() > 0 {
                        break;
                    }
                    attempt += 1;
                    time::sleep(Duration::from_millis(2)).await;
                }
                result
            };
            assert!(
                result.contains_key(info_hash),
                "expected the info hash {} to be present, {:?}",
                info_hash,
                result
            );

            // verify if all announced peers are present
            let result = result.get(info_hash).unwrap();
            for peer in peers {
                assert!(
                    result.iter().find(|e| &e.addr == peer).is_some(),
                    "expected peer {} to be present",
                    peer
                );
            }
        }
    }

    mod scrape {
        use super::*;
        use std::str::FromStr;

        #[tokio::test]
        async fn test_scrape_peers() {
            init_logger!();
            let info_hash =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let source = DhtTracker::builder()
                .node_id(NodeId::new())
                .build()
                .await
                .unwrap();
            let (announcer, target) = create_node_server_pair!();
            let target_addr = (Ipv4Addr::LOCALHOST, target.port()).into();

            // announce the peer addr to the target node through the announcer
            let peer_addr = (Ipv4Addr::LOCALHOST, 6881).into();
            announce_peer(&announcer, &target_addr, &info_hash, peer_addr, false).await;

            // add the target to the source node
            let _ = source.ping(target_addr).await.unwrap();

            // scrape the target
            let result = source
                .scrape_peers(&info_hash, Duration::from_millis(750))
                .await
                .expect("expected a scrape result");
            assert_eq!(1, result.downloaders, "expected at least one downloader");
            assert_eq!(0, result.seeders, "expected no seeders");

            // announce another peer as seed
            let peer_addr = (Ipv4Addr::LOCALHOST, 6882).into();
            announce_peer(&announcer, &target_addr, &info_hash, peer_addr, true).await;

            // scrape the target
            let result = source
                .scrape_peers(&info_hash, Duration::from_millis(750))
                .await
                .expect("expected a scrape result");
            assert!(result.downloaders >= 1, "expected at least one downloader");
            assert_eq!(1, result.seeders, "expected at least one seeder");
        }
    }

    mod scrape_info_hashes {
        use super::*;
        use std::str::FromStr;

        #[tokio::test]
        async fn test_index_interval() {
            init_logger!();
            let info_hash =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let mut source = create_tracker_context!(NodeId::new(), true);
            let (sender, receiver) = channel!(2);
            let mut traversal = TraversalAlgorithm::new(8, vec![], sender.clone());
            let observer = Observer::new(sender.clone());
            let (announcer, target) = create_node_server_pair!();
            let target_id = target.id().await.unwrap();
            let target_addr = (Ipv4Addr::LOCALHOST, target.port()).into();
            let peer_addr = (Ipv4Addr::LOCALHOST, 8080).into();

            // announce the peer addr to the target node through the announcer
            announce_peer(&announcer, &target_addr, &info_hash, peer_addr, false).await;

            // add the target to the source node
            source
                .update_node(target_id, target_addr, &mut traversal)
                .await;

            // subscribe to the events
            let mut events_receiver = source.callbacks.subscribe();
            let (tx, rx) = oneshot::channel();
            tokio::spawn(async move {
                while let Ok(event) = events_receiver.recv().await {
                    if let DhtEvent::InfoHashAdded(_) = &*event {
                        let _ = tx.send(event.clone());
                        break;
                    }
                }
            });

            // run the indexer
            source.index_info_hashes().await;

            // start the source on a separate task
            tokio::spawn(async move {
                source
                    .run(Duration::from_secs(60), observer, traversal, receiver)
                    .await;
            });

            let result = timeout!(rx, Duration::from_millis(500)).expect("expected an event");
            match &*result {
                DhtEvent::InfoHashAdded(result) => {
                    assert_eq!(&info_hash, result, "expected the info hash to match");
                }
                _ => assert!(
                    false,
                    "expected DhtEvent::InfoHashAdded, but got {:?}",
                    result
                ),
            }
        }

        #[tokio::test]
        async fn test_indexing_disabled() {
            init_logger!();
            let indexing_interval = Duration::from_millis(50);
            let source = DhtTracker::builder()
                .enable_indexing(false)
                .indexing_interval(indexing_interval)
                .build()
                .await
                .unwrap();
            let target = DhtTracker::builder()
                .enable_indexing(false)
                .build()
                .await
                .unwrap();

            // add the target to the source network
            let target_addr = (Ipv4Addr::LOCALHOST, target.port()).into();
            let _ = source
                .ping(target_addr)
                .await
                .expect("expected the target to have been pinged");

            // await the interval
            time::sleep(indexing_interval).await;

            let result = target.metrics().sample_info_hashes_requests.total();
            assert_eq!(0, result, "expected no info hashes to be indexed");
        }

        #[tokio::test]
        async fn test_scrape_info_hashes_from() {
            init_logger!();
            let info_hash =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let (source, target) = create_node_server_pair!(NodeId::new(), NodeId::new(), false);
            let source_id = source.id().await.unwrap();
            let target_addr = (Ipv4Addr::LOCALHOST, target.port()).into();

            // connect the source to the target
            let target_key = source.ping(target_addr).await.unwrap();

            // announce the peer addr to the target node through the announcer
            let peer_addr = (Ipv4Addr::LOCALHOST, 7800).into();
            announce_peer(&source, &target_addr, &info_hash, peer_addr, false).await;

            // scrape the info hashes from the target
            let result = source
                .scrape_info_hashes_from(&source_id, &target_key)
                .await
                .expect("expected the scrape to have been successful");
            assert_eq!(
                Some(&info_hash),
                result.first(),
                "expected the info hash to be present"
            );
        }
    }

    mod put {
        use super::*;
        use rand::{rng, Rng};
        use serde::Deserialize;
        use sha1::{Digest, Sha1};

        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct TestItem {
            pub id: u64,
            pub name: String,
        }

        impl TestItem {
            fn hash(&self) -> Sha1Hash {
                let bytes = serde_bencode::to_bytes(self).unwrap();
                Sha1Hash::try_from(Sha1::digest(bytes.as_slice())).unwrap()
            }
        }

        #[tokio::test]
        async fn test_put() {
            init_logger!();
            let item = TestItem {
                id: 123,
                name: "Foo".to_string(),
            };
            let hash = item.hash();
            let (source, target) = create_node_server_pair!(NodeId::new(), NodeId::new(), false);
            let target_addr = (Ipv4Addr::LOCALHOST, target.port()).into();

            // connect the source to the target and request a write token
            let target_key = source.ping(target_addr).await.unwrap();
            let _ = source
                .get_from::<TestItem>(hash.clone(), &target_key)
                .await
                .expect("expected the get operation to succeed");

            // put the item on the target
            let result = source.put(&item, Duration::from_millis(250)).await;
            assert_eq!(Ok(()), result, "expected the put operation to succeed");

            // retrieve the item from the target
            let result = source
                .get_from(hash, &target_key)
                .await
                .expect("expected the get_from operation to succeed");
            assert_eq!(Some(item), result);
        }

        #[tokio::test]
        async fn test_put_to() {
            init_logger!();
            let item = TestItem {
                id: 666,
                name: "Bar".to_string(),
            };
            let hash = item.hash();
            let (source, target) = create_node_server_pair!(NodeId::new(), NodeId::new(), false);
            let target_addr = (Ipv4Addr::LOCALHOST, target.port()).into();

            // connect the source to the target and request a write token
            let target_key = source.ping(target_addr).await.unwrap();
            let _ = source
                .get_from::<TestItem>(hash.clone(), &target_key)
                .await
                .expect("expected the get operation to succeed");

            // put the item on the target
            let result = source.put_to(&item, &target_key).await;
            assert_eq!(Ok(()), result, "expected the put_to operation to succeed");

            // retrieve the item from the target
            let result = source
                .get_from(hash, &target_key)
                .await
                .expect("expected the get_from operation to succeed");
            assert_eq!(Some(item), result);
        }

        #[tokio::test]
        async fn test_put_to_no_write_token() {
            init_logger!();
            let item = TestItem {
                id: 123456,
                name: "NoWriteToken".to_string(),
            };
            let (source, target) = create_node_server_pair!(NodeId::new(), NodeId::new(), false);
            let target_addr = (Ipv4Addr::LOCALHOST, target.port()).into();

            // connect the source to the target
            let target_key = source.ping(target_addr).await.unwrap();

            // try to put the item on the target
            let result = source.put_to(&item, &target_key).await;
            assert_eq!(
                Err(Error::InvalidToken),
                result,
                "expected Error::InvalidToken, but got {:?}",
                result
            );
        }

        #[tokio::test]
        async fn test_put_mutable_no_salt() {
            init_logger!();
            let item = TestItem {
                id: 8989,
                name: "MyMutableItem".to_string(),
            };
            let hash = item.hash();
            let mut secret_key: SecretKey = SecretKey::default();
            rng().fill_bytes(&mut secret_key);
            let (source, target) = create_node_server_pair!(NodeId::new(), NodeId::new(), false);
            let target_addr = (Ipv4Addr::LOCALHOST, target.port()).into();

            // connect the source to the target
            let target_key = source.ping(target_addr).await.unwrap();
            let _ = source
                .get_from::<TestItem>(hash.clone(), &target_key)
                .await
                .expect("expected the get operation to succeed");

            // put the mutable item on the target
            let public_key = source
                .put_mutable(
                    &item,
                    Duration::from_millis(250),
                    Some(1),
                    None,
                    &secret_key,
                )
                .await
                .expect("expected the put operation to succeed");
            assert_ne!(
                PublicKey::default(),
                public_key,
                "expected a public key to have been returned"
            );

            // request the mutable item from the network
            let result = source
                .get_mutable::<TestItem>(&public_key, None, None, Duration::from_millis(500), 1)
                .await
                .unwrap();
            assert_eq!(Some(item), result, "expected the item to have been found");
        }
    }

    mod get {
        use super::*;
        use serde::Deserialize;
        use sha1::{Digest, Sha1};

        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct TestItem {
            pub description: String,
        }

        impl TestItem {
            fn hash(&self) -> Sha1Hash {
                let bytes = serde_bencode::to_bytes(self).unwrap();
                Sha1Hash::try_from(Sha1::digest(bytes.as_slice())).unwrap()
            }
        }

        #[tokio::test]
        async fn test_get() {
            init_logger!();
            let item = TestItem {
                description: "Lorem ipsum dolor sit amet".to_string(),
            };
            let hash = item.hash();
            let (source, target) = create_node_server_pair!(NodeId::new(), NodeId::new(), false);
            let target_addr = (Ipv4Addr::LOCALHOST, target.port()).into();

            // connect the source to the target and request a write token
            let target_key = source.ping(target_addr).await.unwrap();
            let _ = source
                .get_from::<TestItem>(hash.clone(), &target_key)
                .await
                .expect("expected the get operation to succeed");

            // put the item on the target
            let result = source.put_to(&item, &target_key).await;
            assert_eq!(Ok(()), result, "expected the put_to operation to succeed");

            // try to retrieve the item from the DHT network
            let result = source
                .get(hash, Duration::from_millis(500), 1)
                .await
                .expect("expected the get operation to succeed");
            assert_eq!(Some(item), result);
        }
    }

    mod bootstrap {
        use super::*;

        use tokio::time;

        #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
        async fn test_bootstrap_nodes() {
            init_logger!();
            let rand = 13;
            let node_id = NodeId::from_ip_with_rand(&[141, 130, 12, 89].into(), rand);
            let bootstrap_node_id = NodeId::from_ip_with_rand(&[180, 13, 0, 3].into(), rand);
            let bootstrap_node = DhtTracker::builder()
                .node_id(bootstrap_node_id)
                .build()
                .await
                .unwrap();

            // fill the bootstrap node with nodes which can be found through the `find_node` search
            let futures = (1..111u8)
                .into_iter()
                .map(|e| async move {
                    DhtTracker::builder()
                        .node_id(NodeId::from_ip_with_rand(
                            &IpAddr::V4(Ipv4Addr::new(127, 0, 0, e)),
                            rand,
                        ))
                        .build()
                        .await
                        .unwrap()
                })
                .collect::<Vec<_>>();
            let nodes = futures::future::join_all(futures).await;

            for node in &nodes {
                let addr = (Ipv4Addr::LOCALHOST, node.port()).into();
                let result = bootstrap_node.add_node(&addr).await;
                assert_eq!(result, Ok(()), "expected the node to be added successfully");
            }

            // create the DHT tracker which will use the bootstrap node for its bootstrap process
            let tracker = DhtTracker::builder()
                .node_id(node_id)
                .routing_nodes(vec![(Ipv4Addr::LOCALHOST, bootstrap_node.port()).into()])
                .build()
                .await
                .expect("expected a new DHT tracker to have been created");

            select! {
                _ = time::sleep(Duration::from_secs(10)) => assert!(false, "timed-out while bootstrapping nodes"),
                _ = async {
                    while tracker.nodes().await.len() <= 1 {
                        time::sleep(Duration::from_millis(50)).await;
                    }
                } => {},
            }

            let result = tracker.nodes().await;
            assert!(!result.is_empty(), "expected at least one bootstrap node");
        }
    }

    mod info_hashes {
        use super::*;
        use std::str::FromStr;

        #[tokio::test]
        async fn test_info_hashes() {
            init_logger!();
            let info_hash1 =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let info_hash2 =
                InfoHash::from_str("urn:btih:2C6B6858D61DA9543D4231A71DB4B1C9264B0685").unwrap();
            let (source, target) = create_node_server_pair!();
            let target_addr = (Ipv4Addr::LOCALHOST, target.port()).into();

            // create a write token within the source for the target
            let target_key = source.ping(target_addr).await.unwrap();
            let result = source
                .get_peers_from(&info_hash1, &target_key, 1, Duration::from_secs(1))
                .await;
            assert!(
                result.is_ok(),
                "expected the peers to have been queried, but got {:?}",
                result
            );

            // announce the info hashes to the target
            let peer = (Ipv4Addr::LOCALHOST, 6800).into();
            source
                .announce_peer(&info_hash1, &peer, false)
                .await
                .expect("expected the announce to succeed");
            source
                .announce_peer(&info_hash2, &peer, false)
                .await
                .expect("expected the announce to succeed");

            // retrieve the known info hashes from the target storage
            // due to some strange race condition in Github, we'll try a few times to retrieve the info hashes
            let result: Vec<InfoHash> = {
                let mut attempt = 0;
                let mut result = vec![];
                while attempt < 3 {
                    result = target.info_hashes().await;
                    if result.len() == 2 {
                        break;
                    }

                    attempt += 1;
                    time::sleep(Duration::from_millis(2)).await;
                }
                result
            };
            for info_hash in &[info_hash1, info_hash2] {
                assert!(
                    result.contains(info_hash),
                    "expected info hash {} to be present in the storage: {:?}",
                    info_hash,
                    result
                );
            }
        }
    }

    fn create_bucket_matching_node(bucket_index: u8, routing_table_id: NodeId) -> NodeId {
        let mut node_id = NodeId::new();

        while routing_table_id.distance(&node_id) != bucket_index {
            node_id = NodeId::new();
        }

        node_id
    }

    async fn announce_peer(
        announcer: &DhtTracker,
        target_addr: &SocketAddr,
        info_hash: &InfoHash,
        peer_addr: SocketAddr,
        is_seed: bool,
    ) {
        let target_key = announcer
            .ping(*target_addr)
            .await
            .expect("expected the target to have been pinged");
        let _ = announcer
            .get_peers_from(info_hash, &target_key, 1, Duration::from_secs(1))
            .await
            .expect("expected to have retrieved a token for the target");
        let _ = announcer
            .announce_peer_to(info_hash, &peer_addr, is_seed, &target_key)
            .await
            .expect("expected the peer to have been announced");
    }
}
