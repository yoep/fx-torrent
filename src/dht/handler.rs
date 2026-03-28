use crate::dht::krpc::{ErrorMessage, QueryMessage, ResponseMessage};
use crate::dht::routing_table::RoutingTable;
use crate::dht::server::{ItemEntry, MutableItemProperties, ServerNode};
use crate::dht::{Error, PeerEntry, Result};
use crate::{InfoHash, Sha1Hash};
use itertools::Either;
use serde_bencode::value::Value;
use std::fmt::Debug;
use std::iter;
use std::net::SocketAddr;

const UNSUPPORTED_MESSAGE: &str = "Method not supported";

/// The DHT node handler for processing network data.
#[derive(Debug)]
pub enum DhtNodeHandler {
    Client,
    Server(ServerNode),
}

impl DhtNodeHandler {
    /// Create a new client node handler.
    pub fn client() -> Self {
        Self::Client
    }

    /// Create a new server node handler.
    pub fn server(server: ServerNode) -> Self {
        Self::Server(server)
    }

    /// Return `true` if the node is a read-only node handler, else `false`.
    pub fn is_read_only(&self) -> bool {
        match self {
            DhtNodeHandler::Client => true,
            DhtNodeHandler::Server(_) => false,
        }
    }

    /// Returns the total number of peers stored in the storage.
    pub fn peers_len(&self) -> usize {
        match self {
            DhtNodeHandler::Server(server) => server.peers_len(),
            _ => 0,
        }
    }

    /// Returns the torrent slice over all known info hashes.
    pub fn torrents(&self) -> impl Iterator<Item = &InfoHash> {
        match self {
            DhtNodeHandler::Server(server) => Either::Left(server.torrents()),
            _ => Either::Right(iter::empty()),
        }
    }

    /// Returns the peers slice for the given torrent.
    pub fn peers(&self, info_hash: &InfoHash) -> impl Iterator<Item = &PeerEntry> {
        match self {
            DhtNodeHandler::Server(server) => Either::Left(server.peers(info_hash)),
            _ => Either::Right(iter::empty()),
        }
    }

    /// Get an item based on the given sha1 key.
    pub fn get(&self, key: &Sha1Hash) -> Option<&ItemEntry> {
        match self {
            DhtNodeHandler::Server(server) => server.get(key),
            _ => None,
        }
    }

    /// Handle an incoming query from a remote node.
    pub async fn on_incoming_query(
        &mut self,
        query: QueryMessage,
        addr: &SocketAddr,
        routing_table: &RoutingTable,
    ) -> Result<QueryResult> {
        match self {
            DhtNodeHandler::Server(server) => {
                server.on_incoming_query(query, addr, routing_table).await
            }
            _ => Ok(ErrorMessage::Method(UNSUPPORTED_MESSAGE.to_string()).into()),
        }
    }

    /// Updates the peer information for the given info hash.
    pub fn update_peer(&mut self, info_hash: InfoHash, addr: SocketAddr, seed: bool) {
        match self {
            DhtNodeHandler::Server(server) => {
                server.update_peer(info_hash, addr, seed);
            }
            _ => {}
        }
    }

    /// Register a new info hash entry.
    pub fn register(&mut self, info_hash: &InfoHash) {
        match self {
            DhtNodeHandler::Server(server) => server.register(info_hash),
            _ => {}
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
        match self {
            DhtNodeHandler::Server(server) => server.store(value, mutable_properties),
            _ => Err(Error::Unsupported),
        }
    }

    /// Handle a periodic tick.
    /// This tick can be used for periodic cleanup or other maintenance tasks.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn tick(&mut self) {
        match self {
            DhtNodeHandler::Server(server) => {
                server.tick().await;
            }
            _ => {}
        }
    }
}

/// The result of a processed incoming query message.
#[derive(Debug, PartialEq)]
pub enum QueryResult {
    Response(ResponseMessage),
    Error(ErrorMessage),
}

impl From<ResponseMessage> for QueryResult {
    fn from(response: ResponseMessage) -> Self {
        QueryResult::Response(response)
    }
}

impl From<ErrorMessage> for QueryResult {
    fn from(error: ErrorMessage) -> Self {
        QueryResult::Error(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht::krpc::PingMessage;
    use crate::dht::NodeId;
    use std::net::Ipv4Addr;

    mod client {
        use super::*;

        #[tokio::test]
        async fn test_on_incoming_query() {
            let mut node = DhtNodeHandler::client();
            let routing_table = RoutingTable::new(NodeId::new(), 8);

            let result = node
                .on_incoming_query(
                    QueryMessage::Ping {
                        request: PingMessage { id: NodeId::new() },
                    },
                    &(Ipv4Addr::LOCALHOST, 9000).into(),
                    &routing_table,
                )
                .await
                .expect("expected a query result");

            assert_eq!(
                QueryResult::Error(ErrorMessage::Method(UNSUPPORTED_MESSAGE.to_string())),
                result
            );
        }

        #[tokio::test]
        async fn test_store() {
            let value = Value::Int(67);
            let mut node = DhtNodeHandler::client();

            let result = node.store(value, None);

            assert_eq!(Err(Error::Unsupported), result);
        }
    }

    mod server {
        use super::*;
        use fx_callback::MultiThreadedCallback;
        use itertools::Itertools;
        use std::str::FromStr;

        #[tokio::test]
        async fn test_register() {
            let info_hash =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let mut node = DhtNodeHandler::server(ServerNode::new(
                Default::default(),
                (Ipv4Addr::LOCALHOST, 9000).into(),
                MultiThreadedCallback::new(),
                16,
            ));

            // register a new torrent
            node.register(&info_hash);

            // retrieve the torrents
            let torrents = node.torrents().collect_vec();

            assert_eq!(1, torrents.len());
            assert_eq!(&info_hash, torrents[0]);
        }

        #[tokio::test]
        async fn test_tick() {
            let mut node = DhtNodeHandler::server(ServerNode::new(
                Default::default(),
                (Ipv4Addr::LOCALHOST, 9000).into(),
                MultiThreadedCallback::new(),
                16,
            ));

            node.tick().await;
        }
    }
}
