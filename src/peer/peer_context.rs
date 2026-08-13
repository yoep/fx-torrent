use crate::peer::{ConnectionDirection, ConnectionProtocol, Metrics, PeerId, PeerState};
use derive_more::Display;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

/// The core information of an established peer connection.
#[derive(Debug, Display, Clone)]
#[display("{}[{}:{}]", inner.id, inner.protocol, inner.addr)]
pub struct PeerContext {
    inner: Arc<InnerContext>,
}

impl PeerContext {
    /// Create a new builder instance.
    pub fn builder() -> PeerContextBuilder {
        PeerContextBuilder::new()
    }

    /// Returns the unique peer identifier within the torrent network.
    pub fn id(&self) -> &PeerId {
        &self.inner.id
    }

    /// Returns the address of the peer.
    pub fn addr(&self) -> &SocketAddr {
        &self.inner.addr
    }

    /// Returns the connection direction of the peer.
    pub fn connection_type(&self) -> &ConnectionDirection {
        &self.inner.connection_type
    }

    /// Returns the underlying protocol used by the peer connection.
    pub fn protocol(&self) -> &ConnectionProtocol {
        &self.inner.protocol
    }

    /// Returns the current state of the peer.
    pub async fn state(&self) -> PeerState {
        *self.inner.state.read().await
    }

    /// Set the new state of the peer.
    pub async fn set_state(&self, state: PeerState) {
        *self.inner.state.write().await = state;
    }

    /// Returns the metrics of the peer.
    pub fn metrics(&self) -> &Metrics {
        &self.inner.metrics
    }
}

impl PartialEq for PeerContext {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
            && self.addr() == other.addr()
            && self.protocol() == other.protocol()
    }
}

#[derive(Debug)]
struct InnerContext {
    id: PeerId,
    addr: SocketAddr,
    connection_type: ConnectionDirection,
    protocol: ConnectionProtocol,
    state: RwLock<PeerState>,
    metrics: Metrics,
}

#[derive(Debug, Default)]
pub struct PeerContextBuilder {
    id: Option<PeerId>,
    addr: Option<SocketAddr>,
    connection_type: Option<ConnectionDirection>,
    connection_protocol: Option<ConnectionProtocol>,
    state: Option<PeerState>,
    metrics: Option<Metrics>,
}

impl PeerContextBuilder {
    /// Create a new peer context builder instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the unique peer id.
    pub fn id(&mut self, id: PeerId) -> &mut Self {
        self.id = Some(id);
        self
    }

    /// Set the peer address.
    pub fn addr(&mut self, addr: SocketAddr) -> &mut Self {
        self.addr = Some(addr);
        self
    }

    /// Set the type of the connection for the peer.
    pub fn connection_type(&mut self, connection_type: ConnectionDirection) -> &mut Self {
        self.connection_type = Some(connection_type);
        self
    }

    /// Set the underlying protocol of the peer connection.
    pub fn protocol(&mut self, protocol: ConnectionProtocol) -> &mut Self {
        self.connection_protocol = Some(protocol);
        self
    }

    /// Set the initial state of the peer context.
    pub fn state(&mut self, state: PeerState) -> &mut Self {
        self.state = Some(state);
        self
    }

    /// Set the metrics of the peer.
    pub fn metrics(&mut self, metrics: Metrics) -> &mut Self {
        self.metrics = Some(metrics);
        self
    }

    /// Build the new peer context.
    ///
    /// # Panics
    ///
    /// Panics when one of the following fields have not been set:
    /// * `id`
    /// * `addr`
    /// * `connection_type`
    /// * `protocol`
    pub fn build(&mut self) -> PeerContext {
        let id = self.id.take().expect("id has not been set");
        let addr = self.addr.take().expect("addr has not been set");
        let connection_type = self
            .connection_type
            .take()
            .expect("connection_type has not been set");
        let protocol = self
            .connection_protocol
            .take()
            .expect("connection_protocol has not been set");
        let state = self.state.take().unwrap_or(PeerState::Handshake);
        let metrics = self.metrics.take().unwrap_or_default();

        PeerContext {
            inner: Arc::new(InnerContext {
                id,
                addr,
                connection_type,
                protocol,
                state: RwLock::new(state),
                metrics,
            }),
        }
    }
}
