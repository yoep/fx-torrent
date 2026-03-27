use crate::operation::{TorrentOperation, TorrentOperationResult};
use crate::peer::PeerDiscovery;
use crate::{LocalServiceDiscoveryEvent, TorrentContext};
use async_trait::async_trait;
use fx_callback::{Callback, Subscription};
use std::sync::Arc;
#[cfg(feature = "tracing")]
use tracing::instrument;

/// Retrieve torrent peers from the local service discovery.
#[derive(Debug)]
pub struct TorrentLSDPeersOperation {
    initialized: bool,
    receiver: Option<Subscription<LocalServiceDiscoveryEvent>>,
}

impl TorrentLSDPeersOperation {
    /// Create a new instance for retrieving torrent peers from the local service discovery.
    pub fn new() -> Self {
        Self {
            initialized: false,
            receiver: None,
        }
    }

    fn initialize(&mut self, context: &TorrentContext) {
        self.initialized = true;
        let lsd = match context.lsd() {
            None => return,
            Some(lsd) => lsd,
        };

        self.receiver = Some(lsd.subscribe());
    }

    fn process_events(&mut self, context: &mut TorrentContext) {}
}

#[async_trait]
impl TorrentOperation for TorrentLSDPeersOperation {
    fn name(&self) -> &str {
        "retrieve lsd peers operation"
    }

    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn execute(
        &mut self,
        context: &mut TorrentContext,
        _: &[Arc<dyn PeerDiscovery>],
    ) -> TorrentOperationResult {
        if !self.initialized {
            self.initialize(context);
        }

        self.process_events(context);

        TorrentOperationResult::Continue
    }
}
