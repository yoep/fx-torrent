use crate::metrics::{Counter, Gauge, Metric};
use std::time::Duration;

/// The metrics of the DHT node tracker.
#[derive(Debug, Default, Clone)]
pub struct DhtMetrics {
    pub nodes: Gauge,
    pub pending_queries: Gauge,
    pub errors: Counter,
    pub discovered_peers: Gauge,
    pub ping_requests: Counter,
    pub find_node_requests: Counter,
    pub get_peers_requests: Counter,
    pub announce_peer_requests: Counter,
    pub sample_info_hashes_requests: Counter,
    pub put_requests: Counter,
    pub get_requests: Counter,
    pub bytes_in: Counter,
    pub bytes_out: Counter,
}

impl DhtMetrics {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Metric for DhtMetrics {
    fn is_snapshot(&self) -> bool {
        self.nodes.is_snapshot()
    }

    fn snapshot(&self) -> Self {
        Self {
            nodes: self.nodes.snapshot(),
            pending_queries: self.pending_queries.snapshot(),
            errors: self.errors.snapshot(),
            discovered_peers: self.discovered_peers.snapshot(),
            ping_requests: self.ping_requests.snapshot(),
            find_node_requests: self.find_node_requests.snapshot(),
            get_peers_requests: self.get_peers_requests.snapshot(),
            announce_peer_requests: self.announce_peer_requests.snapshot(),
            sample_info_hashes_requests: self.sample_info_hashes_requests.snapshot(),
            put_requests: self.put_requests.snapshot(),
            get_requests: self.get_requests.snapshot(),
            bytes_in: self.bytes_in.snapshot(),
            bytes_out: self.bytes_out.snapshot(),
        }
    }

    fn tick(&self, interval: Duration) {
        self.nodes.tick(interval);
        self.pending_queries.tick(interval);
        self.errors.tick(interval);
        self.discovered_peers.tick(interval);
        self.ping_requests.tick(interval);
        self.find_node_requests.tick(interval);
        self.get_peers_requests.tick(interval);
        self.announce_peer_requests.tick(interval);
        self.sample_info_hashes_requests.tick(interval);
        self.put_requests.tick(interval);
        self.get_requests.tick(interval);
        self.bytes_in.tick(interval);
        self.bytes_out.tick(interval);
    }
}

/// The metrics of a DHT node.
#[derive(Debug, Default, Clone)]
pub struct NodeMetrics {
    /// The amount of times the node has successfully responded to a query.
    pub confirmed_queries: Counter,
    /// The number of times the node failed to respond to a query.
    pub errors: Counter,
}

impl NodeMetrics {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Metric for NodeMetrics {
    fn is_snapshot(&self) -> bool {
        self.confirmed_queries.is_snapshot()
    }

    fn snapshot(&self) -> Self {
        Self {
            confirmed_queries: self.confirmed_queries.snapshot(),
            errors: self.errors.snapshot(),
        }
    }

    fn tick(&self, interval: Duration) {
        self.confirmed_queries.tick(interval);
        self.errors.tick(interval);
    }
}
