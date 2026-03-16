use crate::dht::{Error, NodeId, NodeMetrics, Result};
use rand::{rng, RngExt};
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha1::{Digest, Sha1};
use std::fmt::{Display, Formatter};
use std::net::{IpAddr, SocketAddr};
use std::result;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};

const TOKEN_SECRET_SIZE: usize = 20;
const TOKEN_SIZE: usize = 8;
const TOKEN_MAX_LEN: usize = 64;
const QUESTIONABLE_NODE_AFTER: Duration = Duration::from_secs(15 * 60); // 15 mins.
const BAD_NODE_AFTER_TIMEOUTS: usize = 5;
const BAD_NODE_ERROR_RATE_THRESHOLD: usize = 2;
const TOKEN_SECRET_REFRESH: Duration = Duration::from_secs(60 * 5); // 5 mins.

/// The opaque token secret value used within the hashing process.
type TokenSecretValue = [u8; TOKEN_SECRET_SIZE];

/// The unique identifier key of a node.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct NodeKey {
    /// The unique ID of the node
    pub id: NodeId,
    /// The address of the node within the DHT network
    pub addr: SocketAddr,
}

/// The announce token for a node.
/// This is derived from token secret.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeToken(Vec<u8>);

impl NodeToken {
    /// Create a new [NodeToken] from the given byte slice.
    /// The length of this new token is limited to [TOKEN_SIZE].
    ///
    /// If you want a longer token, use [NodeToken::try_from] instead.
    fn new<T: AsRef<[u8]>>(value: T) -> Self {
        let len = value.as_ref().len().min(TOKEN_SIZE);
        Self(value.as_ref()[0..len].to_vec())
    }

    /// Returns the underlying byte slice of the token.
    pub fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    /// Returns an owned vector containing the bytes of the token.
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    /// Returns the length of the token in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl Serialize for NodeToken {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for NodeToken {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct NodeTokenVisitor;
        impl<'de> Visitor<'de> for NodeTokenVisitor {
            type Value = NodeToken;

            fn expecting(&self, f: &mut Formatter) -> std::fmt::Result {
                write!(f, "expected a string or byte array as node token")
            }

            fn visit_str<E>(self, value: &str) -> result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                NodeToken::try_from(value.as_bytes()).map_err(E::custom)
            }

            fn visit_bytes<E>(self, value: &[u8]) -> result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                NodeToken::try_from(value).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(NodeTokenVisitor)
    }
}

impl Display for NodeToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", String::from_utf8_lossy(&self.0))
    }
}

impl TryFrom<&[u8]> for NodeToken {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self> {
        // as token should always be short binary strings,
        // we limit the length to `TOKEN_MAX_LEN` bytes to prevent token abuse
        if value.len() > TOKEN_MAX_LEN {
            return Err(Error::InvalidToken);
        }

        Ok(Self(value.to_vec()))
    }
}

/// The node information within the DHT network
#[derive(Debug, Clone)]
pub struct Node {
    inner: Arc<InnerNode>,
}

impl Node {
    /// Create a new node for the given ID and address.
    pub fn new(id: NodeId, addr: SocketAddr) -> Self {
        Self::new_with_read_only(id, addr, false)
    }

    /// Create a new node for the given ID and address.
    pub fn new_with_read_only(id: NodeId, addr: SocketAddr, read_only: bool) -> Self {
        Self::new_with_opts(id, addr, read_only, NodeState::Good)
    }

    /// Create a new node for the given ID and address.
    /// This fn allow settings additional properties of the node.
    pub(crate) fn new_with_opts(
        id: NodeId,
        addr: SocketAddr,
        read_only: bool,
        state: NodeState,
    ) -> Self {
        Self {
            inner: Arc::new(InnerNode {
                key: NodeKey { id, addr },
                token: RwLock::new(TokenSecret::new()),
                announce_token: Default::default(),
                read_only,
                state: Mutex::new(state),
                last_seen: Mutex::new(Instant::now()),
                last_indexed: Default::default(),
                indexing_interval: Default::default(),
                metrics: Default::default(),
            }),
        }
    }

    /// Returns a reference to the id of this node.
    pub fn id(&self) -> &NodeId {
        &self.inner.key.id
    }

    /// Returns a reference to the address of this node.
    pub fn addr(&self) -> &SocketAddr {
        &self.inner.key.addr
    }

    /// Returns a reference to the key of this node.
    pub fn key(&self) -> &NodeKey {
        &self.inner.key
    }

    /// Returns the metrics of this node.
    pub fn metrics(&self) -> &NodeMetrics {
        &self.inner.metrics
    }

    /// Returns the current state of this node.
    pub async fn state(&self) -> NodeState {
        *self.inner.state.lock().await
    }

    /// Returns `true` when the node is a read-only node, else `false`.
    /// For more info, see [BEP43](https://www.bittorrent.org/beps/bep_0043.html).
    pub fn is_read_only(&self) -> bool {
        self.inner.read_only
    }

    /// Returns the last time we received a message from this node.
    pub async fn last_seen(&self) -> Instant {
        *self.inner.last_seen.lock().await
    }

    /// Verify that the given token is valid for this node.
    pub(crate) async fn verify_token(&self, token: &NodeToken, ip: &IpAddr) -> bool {
        self.inner.token.read().await.verify(token, ip)
    }

    /// Generate a new secret token for announcing peers.
    /// This token is always based on the ip of the node.
    pub(crate) async fn generate_token(&self) -> NodeToken {
        self.inner
            .token
            .read()
            .await
            .generate(&self.inner.key.addr.ip())
    }

    /// Rotate the token secret for this node, if needed.
    /// This is done every 5 minutes
    pub(crate) async fn rotate_token_secret(&self) {
        let mut token_secret = self.inner.token.write().await;

        if token_secret.needs_rotation() {
            token_secret.rotate();
        }
    }

    /// Returns the opaque token for this node, if available.
    pub(crate) async fn announce_token(&self) -> Option<NodeToken> {
        self.inner.announce_token.lock().await.clone()
    }

    /// Update the opaque token for this node.
    pub(crate) async fn update_announce_token(&self, token: NodeToken) {
        self.inner.update_announce_token(token).await;
    }

    /// Returns the interval announced by the node for scraping info hashes.
    pub(crate) async fn indexing_interval(&self) -> Option<Duration> {
        self.inner.indexing_interval.lock().await.clone()
    }

    /// Update the interval announced by the node for scraping info hashes.
    pub(crate) async fn update_indexing_interval(&self, interval: Duration) {
        *self.inner.indexing_interval.lock().await = Some(interval);
        self.indexed().await;
    }

    /// Returns the last time we indexed the node.
    pub(crate) async fn last_indexed(&self) -> Option<Instant> {
        self.inner.last_indexed.lock().await.clone()
    }

    /// The node has successfully responded to a query.
    pub(crate) async fn confirmed(&self) {
        self.inner.confirmed().await;
    }

    /// The node has sent a query message.
    pub(crate) async fn seen(&self) {
        self.inner.update_last_seen().await;
    }

    /// Increase the number of times the node failed to respond to a query.
    pub(crate) async fn failed(&self) {
        self.inner.failed().await;
    }

    /// Mark the node as successfully indexed.
    pub(crate) async fn indexed(&self) {
        *self.inner.last_indexed.lock().await = Some(Instant::now());
    }

    /// Get the distance between this node and the target node.
    /// See [NodeId::distance] for more information.
    pub fn distance(&self, node: &Node) -> u8 {
        self.inner.key.id.distance(&node.inner.key.id)
    }

    /// Check if the [NodeId] is valid for its own ip address.
    /// See BEP42 for more info.
    pub fn is_secure(&self) -> bool {
        self.inner.key.id.verify_id(&self.inner.key.addr.ip())
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl From<NodeKey> for Node {
    fn from(key: NodeKey) -> Self {
        Self::new(key.id, key.addr)
    }
}

#[derive(Debug)]
struct InnerNode {
    key: NodeKey,
    /// The unique token of the node
    token: RwLock<TokenSecret>,
    /// The token to use for announcing a peer
    announce_token: Mutex<Option<NodeToken>>,
    /// Whether the node is read-only.
    read_only: bool,
    /// The current state of the node
    state: Mutex<NodeState>,
    /// The last time we received a message from the node
    last_seen: Mutex<Instant>,
    /// The last time we indexed the node.
    last_indexed: Mutex<Option<Instant>>,
    /// The interval announced by the node for scraping info hashes.
    indexing_interval: Mutex<Option<Duration>>,
    /// The metrics of the node
    metrics: NodeMetrics,
}

impl InnerNode {
    async fn confirmed(&self) {
        self.update_last_seen().await;
        self.metrics.confirmed_queries.inc();
        self.update_state(NodeState::Good).await;
    }

    async fn failed(&self) {
        self.metrics.errors.inc();

        let last_seen = *self.last_seen.lock().await;
        let new_state = NodeState::calculate(Instant::now() - last_seen, &self.metrics);
        self.update_state(new_state).await;
    }

    async fn update_last_seen(&self) {
        let now = Instant::now();
        let mut last_seen = self.last_seen.lock().await;
        *last_seen = now;
    }

    async fn update_announce_token(&self, token: NodeToken) {
        let mut announce_token = self.announce_token.lock().await;
        *announce_token = Some(token);
    }

    async fn update_state(&self, new_state: NodeState) {
        let mut state = self.state.lock().await;
        *state = new_state;
    }
}

impl PartialEq for InnerNode {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

/// The state of a node
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum NodeState {
    Good = 0,
    Questionable = 1,
    Bad = 2,
}

impl NodeState {
    /// Calculate the node state from the given metrics.
    ///
    /// A good node is a node has responded to one of our queries within the last 15 minutes.
    /// After 15 minutes of inactivity, a node becomes questionable.
    /// Nodes become bad when they fail to respond to multiple queries in a row.
    pub(crate) fn calculate(last_seen_since: Duration, metrics: &NodeMetrics) -> Self {
        let total_queries = metrics.confirmed_queries.total();
        let total_errors = metrics.errors.total();

        // if we've never had successful query response and X timeouts,
        // the node is considered bad
        if total_queries == 0 && total_errors as usize > BAD_NODE_AFTER_TIMEOUTS {
            return Self::Bad;
        }

        // if the error rate (5s avg success rate - 5s avg timeout rate) also exceeds the threshold,
        // the node is considered bad
        let error_rate = metrics
            .confirmed_queries
            .rate()
            .saturating_sub(metrics.errors.rate());
        if error_rate as usize > BAD_NODE_ERROR_RATE_THRESHOLD {
            return Self::Bad;
        }

        if last_seen_since < QUESTIONABLE_NODE_AFTER {
            return Self::Good;
        }

        Self::Questionable
    }
}

/// The token information of a node.
#[derive(Debug, Clone)]
struct TokenSecret {
    secret: TokenSecretValue,
    old_secret: TokenSecretValue,
    last_refreshed: Instant,
}

impl TokenSecret {
    fn new() -> Self {
        let mut random = rng();
        Self {
            secret: random.random(),
            old_secret: random.random(),
            last_refreshed: Instant::now(),
        }
    }

    fn verify(&self, token: &NodeToken, addr: &IpAddr) -> bool {
        Self::verify_with(self, token, addr, &self.secret)
            || Self::verify_with(self, token, addr, &self.old_secret)
    }

    fn generate(&self, addr: &IpAddr) -> NodeToken {
        let hash = Self::hash(&self.secret, addr);
        NodeToken::new(hash.as_slice())
    }

    /// Rotate the token secret.
    fn rotate(&mut self) {
        self.old_secret = self.secret;
        self.secret = rng().random();
        self.last_refreshed = Instant::now();
    }

    /// Verify if the token secret needs to be rotated.
    /// This is done every 5 minutes.
    fn needs_rotation(&self) -> bool {
        self.last_refreshed.elapsed() > TOKEN_SECRET_REFRESH
    }

    fn verify_with(&self, token: &NodeToken, addr: &IpAddr, secret: &TokenSecretValue) -> bool {
        let hash = Self::hash(secret, &addr);
        let validation_token = NodeToken::new(hash.as_slice());
        token == &validation_token
    }

    fn hash(secret: &TokenSecretValue, addr: &IpAddr) -> Vec<u8> {
        let mut hasher = Sha1::new();
        hasher.update(addr.to_string().as_bytes());
        hasher.update(secret);
        hasher.finalize().to_vec()
    }
}

impl TryFrom<&[u8]> for TokenSecret {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self> {
        if value.len() != TOKEN_SECRET_SIZE {
            return Err(Error::InvalidToken);
        }

        let secret: [u8; TOKEN_SECRET_SIZE] = value.try_into().map_err(|_| Error::InvalidToken)?;

        Ok(Self {
            secret,
            old_secret: secret,
            last_refreshed: Instant::now(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    mod token {
        use super::*;

        #[test]
        fn test_verify() {
            let ip = IpAddr::V4(Ipv4Addr::new(190, 180, 170, 5));
            let mut token_secret = TokenSecret::new();
            let token = token_secret.generate(&ip);

            let result = token_secret.verify(&token, &ip);
            assert!(result, "expected the token to be valid");

            // rotate the secret
            token_secret.rotate();
            let result = token_secret.verify(&token, &ip);
            assert!(
                result,
                "expected the old secret token to be valid after rotation"
            );

            // rotate the secret a 2nd time
            token_secret.rotate();
            let result = token_secret.verify(&token, &ip);
            assert!(
                !result,
                "expected the old secret token to be invalid after 2nd rotation"
            );
        }

        #[test]
        fn test_generate() {
            let ip = IpAddr::V4(Ipv4Addr::new(120, 188, 12, 1));
            let token_secret = TokenSecret::new();

            let result = token_secret.generate(&ip);

            assert!(
                !result.0.iter().all(|e| *e == 0),
                "expected the token to be non-zero"
            );
        }

        #[test]
        fn test_from_byte_slice() {
            let token = "aoeusnthaoeusnthaoeu".as_bytes();

            let result =
                TokenSecret::try_from(token).expect("expected the token value to be valid");

            assert_eq!(
                result.secret,
                token[..token.len()],
                "expected the token secret to match the parsed value"
            );
            assert_eq!(
                result.old_secret,
                token[..token.len()],
                "expected the old secret to match the parsed value"
            );
        }
    }

    mod node_state {
        use super::*;
        use crate::metrics::Metric;

        #[test]
        fn test_calculate_good_state() {
            let metrics = NodeMetrics::new();
            metrics.confirmed_queries.inc();
            metrics.tick(Duration::from_secs(1));
            let result = NodeState::calculate(Duration::from_secs(3 * 60), &metrics);
            assert_eq!(NodeState::Good, result);

            let metrics = NodeMetrics::new();
            metrics.errors.inc_by(2);
            metrics.tick(Duration::from_secs(1));
            let result = NodeState::calculate(Duration::from_secs(10 * 60), &metrics);
            assert_eq!(NodeState::Good, result);
        }

        #[test]
        fn test_calculate_questionable_state() {
            let metrics = NodeMetrics::new();
            let result = NodeState::calculate(Duration::from_secs(15 * 60), &metrics);
            assert_eq!(NodeState::Questionable, result);

            let metrics = NodeMetrics::new();
            let result = NodeState::calculate(Duration::from_secs(16 * 60), &metrics);
            assert_eq!(NodeState::Questionable, result);
        }

        #[test]
        fn test_calculate_bad_state() {
            let metrics = NodeMetrics::new();
            metrics.errors.inc_by(6);
            metrics.tick(Duration::from_secs(1));
            let result = NodeState::calculate(Duration::from_secs(5 * 60), &metrics);
            assert_eq!(NodeState::Bad, result);
        }
    }

    mod node_from {
        use super::*;

        #[test]
        fn test_from_node_key() {
            let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 10081));
            let id = NodeId::from_ip(&addr.ip());
            let key = NodeKey { id, addr };

            let result = Node::from(key);

            assert_eq!(result.id(), &id);
        }
    }

    mod node_token {
        use super::*;

        #[test]
        fn test_serialize() {
            let expected_result = "10:LoremIpsum";
            let token = NodeToken("LoremIpsum".as_bytes().to_vec());

            let result = serde_bencode::to_string(&token)
                .expect("expected the token to have been serialized");

            assert_eq!(expected_result, result.as_str());
        }

        #[test]
        fn test_deserialize_str() {
            let value = "8:LoremIps";
            let expected_result = NodeToken("LoremIps".as_bytes().to_vec());

            let result: NodeToken =
                serde_bencode::from_str(value).expect("expected the token to be valid");

            assert_eq!(expected_result, result);
        }

        #[test]
        fn test_deserialize_bytes() {
            let expected_result = NodeToken("Qwerty".as_bytes().to_vec());
            let bytes = serde_bencode::to_bytes(&expected_result)
                .expect("expected the token to be serialized");

            let result: NodeToken = serde_bencode::from_bytes(bytes.as_slice())
                .expect("expected the token to be valid");

            assert_eq!(expected_result, result);
        }
    }
}
