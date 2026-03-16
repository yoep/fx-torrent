use crate::dht::{ItemSignature, NodeId};
use std::net::SocketAddr;
use std::time::Duration;

const DEFAULT_MAX_TORRENTS: usize = 16524;
const DEFAULT_INDEX_INFO_HASHES_INTERVAL: Duration = Duration::from_secs(60);

/// The mode of the DHT node.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Mode {
    /// Read-only client mode
    Client,
    /// Server mode
    Server,
}

/// The DHT configuration options.
#[derive(Debug)]
pub struct Config {
    /// The unique id of the DHT node.
    pub id: NodeId,
    /// The mode of the DHT node.
    pub mode: Mode,
    /// The maximum number of torrents that can be tracked.
    pub max_torrents: usize,
    /// Enable automatic indexing of info hashes from the DHT network.
    pub info_hash_indexing_enabled: bool,
    /// The interval at which info hashes are scraped from the DHT network.
    pub info_hash_indexing_interval: Duration,
    /// The ed25519 item signature used for mutable DHT network items.
    pub item_signature: Option<ItemSignature>,
    /// The routing nodes, _aka bootstrap nodes_, to create initial connections.
    pub routing_nodes: Vec<SocketAddr>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            id: NodeId::new(),
            mode: Mode::Server,
            max_torrents: DEFAULT_MAX_TORRENTS,
            info_hash_indexing_enabled: true,
            info_hash_indexing_interval: DEFAULT_INDEX_INFO_HASHES_INTERVAL,
            item_signature: Default::default(),
            routing_nodes: Default::default(),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        let result = Config::default();

        assert_eq!(result.mode, Mode::Server);
        assert_eq!(result.max_torrents, DEFAULT_MAX_TORRENTS);
        assert_eq!(result.info_hash_indexing_enabled, true);
        assert_eq!(
            result.info_hash_indexing_interval,
            DEFAULT_INDEX_INFO_HASHES_INTERVAL
        );
    }
}
