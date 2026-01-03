use crate::channel::{ChannelReceiver, ChannelSender, Reply, Response};
use crate::dht::compact::{CompactIPv4Node, CompactIPv4Nodes, CompactIPv6Node, CompactIPv6Nodes};
use crate::dht::krpc::{
    AnnouncePeerRequest, AnnouncePeerResponse, ErrorMessage, FindNodeRequest, FindNodeResponse,
    GetPeersRequest, GetPeersResponse, Message, MessagePayload, PingMessage, QueryMessage,
    ResponseMessage, ResponsePayload, SampleInfoHashesRequest, SampleInfoHashesResponse, Version,
    WantFamily,
};
use crate::dht::observer::Observer;
use crate::dht::peers::PeerStorage;
use crate::dht::routing_table::RoutingTable;
use crate::dht::traversal::TraversalAlgorithm;
use crate::dht::{
    DhtMetrics, Error, Node, NodeId, NodeKey, NodeState, NodeToken, Result,
    DEFAULT_ROUTING_NODE_SERVERS,
};
use crate::metrics::Metric;
use crate::{
    channel, CompactIpAddr, CompactIpv4Addr, CompactIpv4Addrs, CompactIpv6Addr, CompactIpv6Addrs,
    InfoHash,
};
use derive_more::Display;
use fx_callback::{Callback, MultiThreadedCallback, Subscriber, Subscription};
use itertools::Itertools;
use log::{debug, trace, warn};
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
const CLEANUP_INTERVAL: Duration = Duration::from_secs(5);
const STATS_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_BUCKET_SIZE: usize = 8;

#[derive(Debug)]
pub enum DhtEvent {
    /// Invoked when the node ID of the DHT server changes.
    IDChanged,
    /// Invoked when the external IP address of the DHT server changes.
    ExternalIpChanged(IpAddr),
    /// Invoked when a new node is added to the routing table.
    NodeAdded(NodeKey),
    /// Invoked when the stats of the DHT server are updated.
    Stats(DhtMetrics),
}

/// A tracker instance for managing DHT nodes.
/// This instance can be shared between torrents by using [DhtTracker::clone].
#[derive(Debug)]
pub struct DhtTracker {
    addr: SocketAddr,
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

    /// Create a new DHT node server with the given node ID.
    /// This function allows creating a server with a specific node id.
    ///
    /// # Arguments
    ///
    /// * `id` - The node ID of the DHT server.
    /// * `routing_nodes` - The routing nodes to use to bootstrap the DHT network.
    pub async fn new(id: NodeId, routing_nodes: Vec<SocketAddr>) -> Result<Self> {
        let socket = Arc::new(Self::bind_socket().await?);
        let socket_addr = socket.local_addr()?;
        let (command_sender, command_receiver) = channel!(256);
        let mut context = TrackerContext::new(id, socket, socket_addr);
        let metrics = context.metrics.clone();
        let callbacks = context.callbacks.clone();
        let cancellation_token = context.cancellation_token.clone();

        // create the observer and traversal algorithm for the node
        let observer = Observer::new(command_sender.clone());
        let traversal =
            TraversalAlgorithm::new(DEFAULT_BUCKET_SIZE, routing_nodes, command_sender.clone());

        // start the context in a separate task
        tokio::spawn(async move {
            context.start(observer, traversal, command_receiver).await;
        });

        Ok(Self {
            addr: socket_addr,
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

    /// Get the socket address on which this DHT server is running.
    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    /// Get the port on which the DHT server is running.
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// Get the DHT network metrics of the DHT server.
    pub fn metrics(&self) -> &DhtMetrics {
        &self.metrics
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
    #[cfg_attr(feature = "tracing", instrument(skip(self), err(level = Level::INFO)))]
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
    #[cfg_attr(feature = "tracing", instrument(err(level = Level::INFO)))]
    pub async fn get_peers_from(
        &self,
        info_hash: &InfoHash,
        node: &NodeKey,
    ) -> Result<Vec<SocketAddr>> {
        self.sender
            .send(|tx| TrackerCommand::GetPeers {
                node: *node,
                info_hash: info_hash.clone(),
                response: tx,
            })
            .await
            .await
    }

    /// Returns the peer addresses for the given torrent info hash within the network.
    /// This function waits for a response from one oe more nodes within the routing table.
    /// Each queried node is limited to the given timeout.
    #[cfg_attr(feature = "tracing", instrument(err(level = Level::INFO)))]
    pub async fn get_peers(
        &self,
        info_hash: &InfoHash,
        timeout: Duration,
    ) -> Result<Vec<SocketAddr>> {
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

    /// Announce the given peer to the DHT network.
    #[cfg_attr(feature = "tracing", instrument(err(level = Level::INFO)))]
    pub async fn announce_peer(&self, info_hash: &InfoHash, peer_addr: &SocketAddr) -> Result<()> {
        self.sender
            .send(|tx| TrackerCommand::AnnouncePeer {
                info_hash: info_hash.clone(),
                peer_addr: *peer_addr,
                node: None,
                response: tx,
            })
            .await
            .await
    }

    /// Announce the given peer to a specific node within the network.
    #[cfg_attr(feature = "tracing", instrument(err(level = Level::INFO)))]
    pub async fn announce_peer_to(
        &self,
        info_hash: &InfoHash,
        peer_addr: &SocketAddr,
        node: &NodeKey,
    ) -> Result<()> {
        self.sender
            .send(|tx| TrackerCommand::AnnouncePeer {
                info_hash: info_hash.clone(),
                peer_addr: *peer_addr,
                node: Some(*node),
                response: tx,
            })
            .await
            .await
    }

    /// Returns the info hashes from given node.
    #[cfg_attr(feature = "tracing", instrument(err(level = Level::INFO)))]
    pub async fn scrape_info_hash_from(
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

    /// Close the DHT node server.
    pub fn close(&self) {
        self.cancellation_token.cancel();
    }

    /// Create a new UDP socket.
    pub(crate) async fn bind_socket() -> Result<UdpSocket> {
        match Self::bind_dual_stack().await {
            Ok(socket) => Ok(socket),
            Err(e) => {
                debug!("DHT node server failed to bind dual stack socket, {}", e);
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

    fn subscribe_with(&self, subscriber: Subscriber<DhtEvent>) {
        self.callbacks.subscribe_with(subscriber)
    }
}

impl Clone for DhtTracker {
    fn clone(&self) -> Self {
        Self {
            addr: self.addr.clone(),
            metrics: self.metrics.clone(),
            sender: self.sender.clone(),
            callbacks: self.callbacks.clone(),
            cancellation_token: self.cancellation_token.clone(),
        }
    }
}

#[derive(Debug, Default)]
pub struct DhtTrackerBuilder {
    node_id: Option<NodeId>,
    public_ip: Option<IpAddr>,
    routing_nodes: Vec<SocketAddr>,
    routing_node_urls: Vec<String>,
}

impl DhtTrackerBuilder {
    /// Set the ID of the node server.
    pub fn node_id(&mut self, id: NodeId) -> &mut Self {
        self.node_id = Some(id);
        self
    }

    /// Set the public ip address of the dht tracker.
    pub fn public_ip(&mut self, ip: IpAddr) -> &mut Self {
        self.public_ip = Some(ip);
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

    /// Try to create a new DHT node server from this builder.
    pub async fn build(&mut self) -> Result<DhtTracker> {
        let node_id = self.node_id.take().unwrap_or_else(|| {
            self.public_ip
                .take()
                .map(|e| NodeId::from_ip(&e))
                .unwrap_or(NodeId::new())
        });
        let mut routing_nodes: HashSet<SocketAddr> = self.routing_nodes.drain(..).collect();

        for node_url in self.routing_node_urls.drain(..).filter_map(Self::host) {
            match lookup_host(node_url.as_str()).await {
                Ok(addrs) => {
                    routing_nodes.extend(addrs);
                }
                Err(e) => trace!("DHT router node failed to resolve \"{}\", {}", node_url, e),
            }
        }

        DhtTracker::new(node_id, routing_nodes.into_iter().collect()).await
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
    /// Find peer addresses for the given torrent info hash within the network.
    GetPeers {
        node: NodeKey,
        info_hash: InfoHash,
        response: Reply<Result<Vec<SocketAddr>>>,
    },
    /// Announce the given peer to the DHT network.
    AnnouncePeer {
        info_hash: InfoHash,
        peer_addr: SocketAddr,
        node: Option<NodeKey>,
        response: Reply<Result<()>>,
    },
    /// Scrape the info hashes from the given node.
    ScrapeInfoHashes {
        target: NodeId,
        node: NodeKey,
        response: Reply<Result<Vec<InfoHash>>>,
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
    /// Returns the node keys of "good" nodes which can be used in search queries.
    GoodSearchNodes {
        response: Reply<Vec<NodeKey>>,
    },
    AddTraversalNode((NodeId, SocketAddr)),
    UpdateExternalIp(IpAddr),
}

impl Debug for TrackerCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TrackerCommand::Id { .. } => write!(f, "TrackerCommand::Id"),
            TrackerCommand::Ping { addr, .. } => {
                write!(f, "TrackerCommand::Ping{{ addr: {:?} }}", addr)
            }
            TrackerCommand::FindNode { target_id, .. } => {
                write!(
                    f,
                    "TrackerCommand::FindNodes{{ target_id: {:?} }}",
                    target_id
                )
            }
            TrackerCommand::GetPeers { info_hash, .. } => {
                write!(
                    f,
                    "TrackerCommand::GetPeers{{ info_hash: {:?} }}",
                    info_hash
                )
            }
            TrackerCommand::AnnouncePeer { info_hash, .. } => write!(
                f,
                "TrackerCommand::AnnouncePeer{{ info_hash: {:?} }}",
                info_hash
            ),
            TrackerCommand::ScrapeInfoHashes { node, .. } => {
                write!(f, "TrackerCommand::ScrapeInfoHashes{{ node: {:?} }}", node)
            }
            TrackerCommand::TotalNodes { .. } => write!(f, "TrackerCommand::TotalNodes"),
            TrackerCommand::GetNode { node, .. } => {
                write!(f, "TrackerCommand::GetNode{{ node: {:?} }}", node)
            }
            TrackerCommand::GetNodeById { id, .. } => {
                write!(f, "TrackerCommand::GetNodeById{{ id: {:?} }}", id)
            }
            TrackerCommand::GetNodes { .. } => write!(f, "TrackerCommand::GetNodes"),
            TrackerCommand::GoodSearchNodes { .. } => write!(f, "TrackerCommand::GoodSearchNodes"),
            TrackerCommand::AddTraversalNode { .. } => {
                write!(f, "TrackerCommand::AddTraversalNode")
            }
            TrackerCommand::UpdateExternalIp(addr) => {
                write!(f, "TrackerCommand::UpdateExternalIp({:?})", addr)
            }
        }
    }
}

#[derive(Debug, Display)]
pub(crate) enum Event {
    #[display("Incoming")]
    Incoming(ReaderMessage),
    #[display("Command")]
    Command(TrackerCommand),
    #[display("CleanupTick")]
    CleanupTick,
    #[display("RefreshTick")]
    RefreshTick,
    #[display("BootstrapTick")]
    BootstrapTick,
    #[display("StatsTick")]
    StatsTick,
}

/// The internal tracker context of the DHT node server.
#[derive(Debug, Display)]
#[display("DHT node server [{}]", socket_addr.port())]
pub(crate) struct TrackerContext {
    /// The current transaction ID of the node server
    transaction_id: u16,
    /// The underlying socket used by the server
    socket: Arc<UdpSocket>,
    /// The address on which the server is listening
    pub(crate) socket_addr: SocketAddr,
    /// The routing table of the node server
    pub(crate) routing_table: RoutingTable,
    /// The currently pending requests of the server
    pending_requests: HashMap<TransactionKey, PendingRequest>,
    /// The timeout while trying to send packages to a target address
    send_timeout: Duration,
    /// The tracker metrics of the DHT network
    pub(crate) metrics: DhtMetrics,
    /// The channel receiver for incoming messages
    pub(crate) receiver: UnboundedReceiver<ReaderMessage>,
    /// The callback of the tracker
    pub(crate) callbacks: MultiThreadedCallback<DhtEvent>,
    /// The cancellation token of the server
    pub(crate) cancellation_token: CancellationToken,
}

impl TrackerContext {
    pub(crate) fn new(id: NodeId, socket: Arc<UdpSocket>, socket_addr: SocketAddr) -> Self {
        let (sender, receiver) = unbounded_channel();
        let cancellation_token = CancellationToken::new();

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
            reader.start().await;
        });

        Self {
            transaction_id: Default::default(),
            socket,
            socket_addr,
            routing_table: RoutingTable::new(id, DEFAULT_BUCKET_SIZE),
            pending_requests: Default::default(),
            send_timeout: SEND_PACKAGE_TIMEOUT,
            metrics: Default::default(),
            receiver,
            callbacks: MultiThreadedCallback::new(),
            cancellation_token,
        }
    }

    /// Start the main event loop of the context.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    pub(crate) async fn start(
        &mut self,
        mut observer: Observer,
        mut traversal: TraversalAlgorithm,
        mut command_receiver: ChannelReceiver<TrackerCommand>,
    ) {
        let mut peers = PeerStorage::new();

        let mut bootstrap_interval = interval(BOOTSTRAP_INTERVAL);
        let mut refresh_interval = interval(REFRESH_INTERVAL);
        let mut cleanup_interval = interval(CLEANUP_INTERVAL);
        let mut stats_interval = interval(STATS_INTERVAL);

        debug!("{} started", self);
        loop {
            let event: Event;

            select! {
                _ = self.cancellation_token.cancelled() => break,
                Some(message) = self.receiver.recv() => event = Event::Incoming(message),
                command = command_receiver.recv() => {
                    if let Some(command) = command {
                        event = Event::Command(command);
                    } else {
                        break;
                    }
                },
                _ = cleanup_interval.tick() => event = Event::CleanupTick,
                _ = refresh_interval.tick() => event = Event::RefreshTick,
                _ = bootstrap_interval.tick() => event = Event::BootstrapTick,
                _ = stats_interval.tick() => event = Event::StatsTick,
            }

            self.run(event, &mut observer, &mut traversal, &mut peers)
                .await
        }
        debug!("{} main loop ended", self);
    }

    /// Run a single event loop iteration.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    pub(crate) async fn run(
        &mut self,
        event: Event,
        observer: &mut Observer,
        traversal: &mut TraversalAlgorithm,
        peers: &mut PeerStorage,
    ) {
        match event {
            Event::Incoming(message) => {
                self.on_message_received(message, observer, traversal, peers)
                    .await
            }
            Event::Command(command) => self.handle_command(command, traversal).await,
            Event::CleanupTick => self.cleanup_pending_requests().await,
            Event::RefreshTick => self.refresh_routing_table().await,
            Event::BootstrapTick => self.bootstrap(traversal).await,
            Event::StatsTick => self.stats_tick().await,
        }
    }

    async fn bootstrap(&mut self, traversal: &mut TraversalAlgorithm) {
        traversal.run(self.routing_table.id, self).await;
    }

    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn on_message_received(
        &mut self,
        message: ReaderMessage,
        observer: &mut Observer,
        traversal: &mut TraversalAlgorithm,
        peers: &mut PeerStorage,
    ) {
        match message {
            ReaderMessage::Message {
                message,
                message_len,
                addr,
            } => {
                self.metrics.bytes_in.inc_by(message_len as u64);
                observer.observe(addr, message.ip.as_ref(), &self).await;
                if let Err(e) = self
                    .handle_incoming_message(message, addr, traversal, peers)
                    .await
                {
                    warn!("{} failed to process incoming message, {}", self, e);
                    self.metrics.errors.inc();
                }
            }
            ReaderMessage::Error {
                error,
                payload_len,
                addr,
            } => {
                warn!(
                    "{} failed to read incoming message from {}, {}",
                    self, addr, error
                );
                self.metrics.bytes_in.inc_by(payload_len as u64);
                self.metrics.errors.inc();
            }
        }
    }

    /// Try to process an incoming DHT message from the given node address.
    #[cfg_attr(feature = "tracing", instrument(skip_all, err))]
    async fn handle_incoming_message(
        &mut self,
        message: Message,
        addr: SocketAddr,
        traversal: &mut TraversalAlgorithm,
        peers: &mut PeerStorage,
    ) -> Result<()> {
        trace!(
            "{} received message (transaction {}) from {}, {:?}",
            self,
            message.transaction_id(),
            addr,
            message
        );
        let node_id = message.id().cloned();
        let transaction_id = message.transaction_id();
        let key = TransactionKey {
            id: transaction_id,
            addr,
        };

        // check the type of the message
        match message.payload {
            MessagePayload::Query(query) => match query {
                QueryMessage::Ping { .. } => {
                    self.on_ping_request(transaction_id, &addr).await?;
                }
                QueryMessage::FindNode { request } => {
                    self.on_find_node_request(transaction_id, &addr, request)
                        .await?;
                }
                QueryMessage::GetPeers { request } => {
                    self.on_get_peers_request(transaction_id, &addr, request, &peers)
                        .await?;
                }
                QueryMessage::AnnouncePeer { request } => {
                    self.on_announce_peer_request(transaction_id, &addr, request, peers)
                        .await?;
                }
                QueryMessage::SampleInfoHashes { request } => {
                    self.on_sample_info_hashes_request(transaction_id, request, &addr, &peers)
                        .await?;
                }
            },
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
                            )
                            .await;
                        }
                    }
                } else {
                    warn!(
                        "{} received response for unknown request, invalid transaction {}",
                        self, key
                    );
                    self.metrics.errors.inc();
                }
            }
            MessagePayload::Error(err) => {
                self.on_error_response(&key, &addr, err).await;
            }
        }

        if let Some(id) = node_id {
            self.update_node(id, addr, traversal).await;
        }
        Ok(())
    }

    /// Process a received announce peer request.
    ///
    /// # Arguments
    ///
    /// * `transaction_id` - The transaction id of the query.
    /// * `addr`- The source address of the node.
    /// * `request` - The `announce_peer` query arguments.
    /// * `peers` - The peer storage of the server.
    #[cfg_attr(feature = "tracing", instrument(skip(self, peers)))]
    async fn on_announce_peer_request(
        &self,
        transaction_id: u16,
        addr: &SocketAddr,
        request: AnnouncePeerRequest,
        peers: &mut PeerStorage,
    ) -> Result<()> {
        if let Some(node) = self.routing_table.find_node(&request.id) {
            // check if the address matches the node
            if node.addr() != addr {
                return self
                    .send_error(
                        transaction_id,
                        ErrorMessage::Protocol("Bad node".to_string()),
                        &addr,
                    )
                    .await;
            }

            let is_valid = match NodeToken::try_from(request.token.as_slice()) {
                Ok(token) => node.verify_token(&token, &addr.ip()).await,
                Err(_) => false,
            };
            if !is_valid {
                return self
                    .send_error(
                        transaction_id,
                        ErrorMessage::Protocol("Bad token".to_string()),
                        &addr,
                    )
                    .await;
            };
        }

        trace!(
            "{} adding peer address {} for info hash {}",
            self,
            addr,
            request.info_hash
        );
        peers.update_peer(request.info_hash, *addr, request.seed.unwrap_or(false));
        self.send_response(
            transaction_id,
            ResponseMessage::Announce {
                response: AnnouncePeerResponse {
                    id: self.routing_table.id,
                },
            },
            &addr,
        )
        .await?;
        Ok(())
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

    /// Process a received sample info hashes request.
    ///
    /// # Arguments
    ///
    /// * `transaction_id` - The transaction id of the query.
    /// * `addr`- The source address of the node.
    /// * `request` - The `sample_infohashes` query arguments.
    /// * `peers` - The peer storage of the server.
    #[cfg_attr(feature = "tracing", instrument(skip(self, peers)))]
    async fn on_sample_info_hashes_request(
        &self,
        transaction_id: u16,
        request: SampleInfoHashesRequest,
        addr: &SocketAddr,
        peers: &PeerStorage,
    ) -> Result<()> {
        let num = peers.info_hashes().count();
        let samples = peers.info_hashes().take(20).cloned().collect::<Vec<_>>();
        let (nodes, nodes6) = Self::closest_node_pairs(&self.routing_table, &request.target);

        self.send_response(
            transaction_id,
            ResponseMessage::SampleInfoHashes {
                response: SampleInfoHashesResponse {
                    id: self.routing_table.id,
                    interval: 180,
                    nodes,
                    nodes6,
                    num: num as u32,
                    samples,
                },
            },
            &addr,
        )
        .await?;
        Ok(())
    }

    /// Process the given sample info hashes response.
    ///
    /// # Arguments
    ///
    /// * `key` - The transaction key of the query.
    /// * `addr`- The source address of the node.
    /// * `pending_request` - The pending request of the query.
    /// * `response` - The received sample info hashes response.
    #[cfg_attr(feature = "tracing", instrument)]
    async fn on_sample_info_hashes_response(
        &self,
        key: &TransactionKey,
        addr: &SocketAddr,
        pending_request: PendingRequest,
        response: SampleInfoHashesResponse,
    ) {
        if !response.id.verify_id(&addr.ip()) {
            debug!("{} detected spoofed sample_infohashes from {}", self, key);
            Self::resolve_as_err(pending_request.request_type, Error::InvalidNodeId);
            return;
        }

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

    /// Process a received ping query.
    /// This invokes a simple ping-pong between the server and the sender.
    ///
    /// # Arguments
    ///
    /// * `transaction_id` - The transaction id of the query.
    /// * `addr`- The source address of the node.
    #[cfg_attr(feature = "tracing", instrument(err(level = Level::INFO)))]
    async fn on_ping_request(&self, transaction_id: u16, addr: &SocketAddr) -> Result<()> {
        self.send_response(
            transaction_id,
            ResponseMessage::Ping {
                response: PingMessage {
                    id: self.routing_table.id,
                },
            },
            &addr,
        )
        .await?;
        Ok(())
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

    /// Process a received find nodes query.
    ///
    /// # Arguments
    ///
    /// * `transaction_id` - The transaction id of the query.
    /// * `addr`- The source address of the node.
    /// * `request` - The `find_node` query arguments.
    #[cfg_attr(feature = "tracing", instrument)]
    async fn on_find_node_request(
        &self,
        transaction_id: u16,
        addr: &SocketAddr,
        request: FindNodeRequest,
    ) -> Result<()> {
        let target_node = request.target;
        let (compact_nodes, compact_nodes6) =
            Self::closest_node_pairs(&self.routing_table, &target_node);

        self.send_response(
            transaction_id,
            ResponseMessage::FindNode {
                response: FindNodeResponse {
                    id: self.routing_table.id,
                    nodes: compact_nodes.into(),
                    nodes6: compact_nodes6.into(),
                    token: None,
                },
            },
            &addr,
        )
        .await?;
        Ok(())
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

    /// Process a received get_peers query.
    /// The query will be processed only when the node is already known within the routing table.
    ///
    /// # Arguments
    ///
    /// * `transaction_id` - The transaction id of the query.
    /// * `addr`- The source address of the node.
    /// * `request` - The `get_peers` query arguments.
    /// * `peers` - The peer storage of the server.
    #[cfg_attr(feature = "tracing", instrument(skip(self, peers)))]
    async fn on_get_peers_request(
        &self,
        transaction_id: u16,
        addr: &SocketAddr,
        request: GetPeersRequest,
        peers: &PeerStorage,
    ) -> Result<()> {
        let token: NodeToken;
        let (nodes, nodes6) = match self.routing_table.find_node(&request.id) {
            None => {
                return self
                    .send_error(
                        transaction_id,
                        ErrorMessage::Generic("Bad node".to_string()),
                        &addr,
                    )
                    .await;
            }
            Some(node) => {
                let info_hash_as_node =
                    match NodeId::try_from(request.info_hash.short_info_hash_bytes().as_slice()) {
                        Ok(e) => e,
                        Err(e) => {
                            warn!("{} failed to parse info hash as node id, {}", self, e);
                            return self
                                .send_error(
                                    transaction_id,
                                    ErrorMessage::Server("A Server Error Occurred".to_string()),
                                    &addr,
                                )
                                .await;
                        }
                    };

                token = node.generate_token().await;
                Self::closest_node_pairs(&self.routing_table, &info_hash_as_node)
            }
        };

        let values = Some(
            peers
                .peers(&request.info_hash)
                .filter(|e| e.addr.is_ipv4() == self.socket_addr.is_ipv4())
                .map(|e| CompactIpAddr::from(e.addr))
                .map(|e| e.as_bytes())
                .concat(),
        )
        .filter(|e| !e.is_empty());

        self.send_response(
            transaction_id,
            ResponseMessage::GetPeers {
                response: GetPeersResponse {
                    id: self.routing_table.id,
                    token: token.to_vec(),
                    values,
                    nodes: nodes.into(),
                    nodes6: nodes6.into(),
                },
            },
            &addr,
        )
        .await?;
        Ok(())
    }

    /// Process a received response message for a query.
    #[cfg_attr(feature = "tracing", instrument(skip(self, traversal)))]
    async fn on_get_peers_response(
        &self,
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
        if let Err(e) = self
            .update_announce_token(&response.id, &response.token)
            .await
        {
            Self::resolve_as_err(pending_request.request_type, e);
            return;
        }

        let nodes = response
            .nodes
            .as_slice()
            .into_iter()
            .map(|e| Node::new(e.id, e.addr.clone().into()))
            .chain(
                response
                    .nodes6
                    .as_slice()
                    .into_iter()
                    .map(|e| Node::new(e.id, e.addr.clone().into())),
            )
            .collect::<Vec<_>>();
        for node in nodes {
            traversal.add_node(Some(*node.id()), *node.addr());
        }

        let peers: Vec<SocketAddr> = if let Some(values) = response.values {
            match addr.ip() {
                IpAddr::V4(_) => match CompactIpv4Addrs::try_from(values.as_slice()) {
                    Ok(addrs) => addrs.into_iter().map(|e| e.into()).collect::<Vec<_>>(),
                    Err(e) => {
                        Self::resolve_as_err(
                            pending_request.request_type,
                            Error::Parse(e.to_string()),
                        );
                        return;
                    }
                },
                IpAddr::V6(_) => match CompactIpv6Addrs::try_from(values.as_slice()) {
                    Ok(addrs) => addrs.into_iter().map(|e| e.into()).collect::<Vec<_>>(),
                    Err(e) => {
                        Self::resolve_as_err(
                            pending_request.request_type,
                            Error::Parse(e.to_string()),
                        );
                        return;
                    }
                },
            }
        } else {
            vec![]
        };

        self.metrics.discovered_peers.inc_by(peers.len() as u64);
        match pending_request.request_type {
            Some(PendingRequestType::GetPeers(tx)) => {
                let _ = tx.send(Ok(peers));
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
            TrackerCommand::GetPeers {
                node,
                info_hash,
                response,
            } => match self.routing_table.find_node_by_key(&node).cloned() {
                None => response.send(Err(Error::InvalidNodeId)),
                Some(node) => self.get_peers(info_hash, &node, response).await,
            },
            TrackerCommand::AnnouncePeer {
                info_hash,
                peer_addr,
                node,
                response,
            } => match node {
                None => {
                    self.announce_peer_to_network(info_hash, &peer_addr).await;
                    response.send(Ok(()));
                }
                Some(node) => match self.routing_table.find_node_by_key(&node).cloned() {
                    None => response.send(Err(Error::InvalidNodeId)),
                    Some(node) => {
                        self.announce_peer_to(info_hash, &peer_addr, &node, response)
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
                Some(node) => self.scrape_info_hashes(&target, &node, response).await,
            },
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

    #[cfg_attr(feature = "tracing", instrument)]
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

    #[cfg_attr(feature = "tracing", instrument)]
    async fn get_peers(
        &mut self,
        info_hash: InfoHash,
        node: &Node,
        response: Reply<Result<Vec<SocketAddr>>>,
    ) {
        self.send_query(
            QueryMessage::GetPeers {
                request: GetPeersRequest {
                    id: self.routing_table.id,
                    info_hash,
                    want: Default::default(),
                },
            },
            node.addr(),
            Some(PendingRequestType::GetPeers(response.take())),
            || node.failed(),
        )
        .await
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self, node, response)))]
    async fn announce_peer_to(
        &mut self,
        info_hash: InfoHash,
        peer_addr: &SocketAddr,
        node: &Node,
        response: Reply<Result<()>>,
    ) {
        if let Some(token) = node.announce_token().await {
            self.send_query(
                QueryMessage::AnnouncePeer {
                    request: AnnouncePeerRequest {
                        id: self.routing_table.id,
                        implied_port: false,
                        info_hash: info_hash.clone(),
                        port: peer_addr.port(),
                        token: token.to_vec(),
                        name: None,
                        seed: None,
                    },
                },
                node.addr(),
                Some(PendingRequestType::AnnouncePeer(response.take())),
                || node.failed(),
            )
            .await
        } else {
            response.send(Err(Error::InvalidToken));
        }
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    async fn announce_peer_to_network(&mut self, info_hash: InfoHash, peer_addr: &SocketAddr) {
        for node in self.routing_table.nodes().cloned().collect::<Vec<_>>() {
            if let Some(token) = node.announce_token().await {
                self.send_query(
                    QueryMessage::AnnouncePeer {
                        request: AnnouncePeerRequest {
                            id: self.routing_table.id,
                            implied_port: false,
                            info_hash: info_hash.clone(),
                            port: peer_addr.port(),
                            token: token.to_vec(),
                            name: None,
                            seed: None,
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

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    async fn scrape_info_hashes(
        &mut self,
        target: &NodeId,
        node: &Node,
        response: Reply<Result<Vec<InfoHash>>>,
    ) {
        self.send_query(
            QueryMessage::SampleInfoHashes {
                request: SampleInfoHashesRequest {
                    id: self.routing_table.id,
                    target: *target,
                },
            },
            node.addr(),
            Some(PendingRequestType::ScrapeInfoHashes(response.take())),
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
            .transaction_id(id)
            .payload(MessagePayload::Query(query))
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
        transaction_id: u16,
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
    async fn send_error(
        &self,
        transaction_id: u16,
        error: ErrorMessage,
        addr: &SocketAddr,
    ) -> Result<()> {
        let message = Message::builder()
            .transaction_id(transaction_id)
            .payload(MessagePayload::Error(error))
            .ip((*addr).into())
            .port(addr.port())
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
            message.transaction_id(),
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

    async fn stats_tick(&self) {
        self.callbacks
            .invoke(DhtEvent::Stats(self.metrics.snapshot()));

        self.metrics.tick(STATS_INTERVAL);
        self.routing_table.tick(STATS_INTERVAL);
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
    async fn update_announce_token(&self, id: &NodeId, value: impl AsRef<[u8]>) -> Result<()> {
        let token = NodeToken::try_from(value.as_ref())?;
        let node = self
            .routing_table
            .find_node(id)
            .ok_or(Error::InvalidNodeId)?;
        node.update_announce_token(token).await;
        trace!("{} updated announce token for {}", self, id);
        Ok(())
    }

    /// Get the next transaction ID for sending a new message.
    /// The transaction ID within the server will be automatically wrapped when [u16::MAX] has been reached.
    fn next_transaction_id(&mut self) -> u16 {
        let new = self.transaction_id.wrapping_add(1);
        self.transaction_id = new;
        new
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
                PendingRequestType::GetPeers(tx) => {
                    let _ = tx.send(Err(err));
                }
                PendingRequestType::ScrapeInfoHashes(tx) => {
                    let _ = tx.send(Err(err));
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
}

#[derive(Debug)]
pub(crate) enum ReaderMessage {
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
    async fn start(&self) {
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
        let message = serde_bencode::from_bytes::<Message>(bytes)?;
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
    GetPeers(oneshot::Sender<Result<Vec<SocketAddr>>>),
    ScrapeInfoHashes(oneshot::Sender<Result<Vec<InfoHash>>>),
}

#[derive(Debug, Display, Clone, PartialEq, Eq, Hash)]
#[display("{}[{}]", addr, id)]
struct TransactionKey {
    pub id: u16,
    pub addr: SocketAddr,
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::create_node_server_pair;
    use crate::{init_logger, timeout};
    use std::net::Ipv4Addr;

    mod new {
        use super::*;

        #[tokio::test]
        async fn test_tracker_new() {
            init_logger!();
            let node_id = NodeId::new();
            let tracker = DhtTracker::new(node_id, Vec::new())
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
            let tracker = DhtTracker::new(NodeId::new(), Vec::new()).await.unwrap();

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
        use crate::create_tracker_context;

        #[tokio::test]
        async fn test_find_node() {
            init_logger!();
            let rand = 2;
            let search_node_id = NodeId::from_ip_with_rand(&[132, 141, 12, 40].into(), rand);
            let incoming_id = NodeId::from_ip_with_rand(&Ipv4Addr::LOCALHOST.into(), rand);
            let outgoing_id = NodeId::from_ip_with_rand(&Ipv4Addr::LOCALHOST.into(), rand);
            let outgoing = DhtTracker::new(outgoing_id, Vec::new()).await.unwrap();
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
            let mut peers = PeerStorage::new();
            let message = timeout(Duration::from_millis(500), incoming.receiver.recv())
                .await
                .unwrap()
                .expect("expected a message");
            incoming
                .run(
                    Event::Incoming(message),
                    &mut observer,
                    &mut traversal,
                    &mut peers,
                )
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
                incoming.start(observer, traversal, receiver).await;
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
                .get_peers(&info_hash, Duration::from_secs(2))
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
            let result = target.get_peers_from(&info_hash, &source_key).await;
            assert!(
                result.is_ok(),
                "expected the peers to have been queried, but got {:?}",
                result
            );
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
            let source_id = source.id().await.unwrap();
            let target_id = target.id().await.unwrap();
            let target_addr = (Ipv4Addr::LOCALHOST, target.port()).into();
            let peer_addr = (Ipv4Addr::LOCALHOST, 8080).into();

            // request peers from the target node
            // this will set the initial announce token in the source tracker for the target node
            let target_key = source.ping(target_addr).await.unwrap();
            let result = source.get_peers_from(&info_hash, &target_key).await;
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
            let result = source.announce_peer(&info_hash, &peer_addr).await;
            verify_announce_peer(&info_hash, &source, &source_id, &target_key, result).await;
        }

        #[tokio::test]
        async fn test_announce_peer_to() {
            init_logger!();
            let info_hash =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let (source, target) = create_node_server_pair!();
            let source_id = source.id().await.unwrap();
            let target_id = target.id().await.unwrap();
            let target_addr = (Ipv4Addr::LOCALHOST, target.port()).into();
            let peer_addr = (Ipv4Addr::LOCALHOST, 8080).into();

            // request peers from the target node
            // this will set the initial announce token in the source tracker for the target node
            let target_key = source.ping(target_addr).await.unwrap();
            let result = source.get_peers_from(&info_hash, &target_key).await;
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
            let result = source
                .announce_peer_to(&info_hash, &peer_addr, &target_key)
                .await;
            verify_announce_peer(&info_hash, &source, &source_id, &target_key, result).await;
        }

        async fn verify_announce_peer(
            info_hash: &InfoHash,
            source: &DhtTracker,
            source_id: &NodeId,
            target_key: &NodeKey,
            result: Result<()>,
        ) {
            assert!(
                result.is_ok(),
                "expected the announce to have been successful, but got {:?}",
                result
            );

            // retrieve the announced peer from the target
            let info_hashes = source
                .scrape_info_hash_from(&source_id, &target_key)
                .await
                .unwrap();
            assert_eq!(
                Some(info_hash),
                info_hashes.first(),
                "expected the info hash to be present"
            );
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

    fn create_bucket_matching_node(bucket_index: u8, routing_table_id: NodeId) -> NodeId {
        let mut node_id = NodeId::new();

        while routing_table_id.distance(&node_id) != bucket_index {
            node_id = NodeId::new();
        }

        node_id
    }
}
