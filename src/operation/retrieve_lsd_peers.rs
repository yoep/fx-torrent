use crate::operation::{TorrentOperation, TorrentOperationResult};
use crate::peer::PeerDiscovery;
use crate::{LocalServiceDiscoveryEvent, TorrentContext};
use async_trait::async_trait;
use fx_callback::{Callback, Subscription};
use std::sync::Arc;

/// Retrieve torrent peers from the local service discovery.
#[derive(Debug)]
pub struct TorrentLsdPeersOperation {
    initialized: bool,
    receiver: Option<Subscription<LocalServiceDiscoveryEvent>>,
}

impl TorrentLsdPeersOperation {
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

    fn process_events(&mut self, context: &mut TorrentContext) {
        let receiver = match self.receiver.as_mut() {
            None => return,
            Some(receiver) => receiver,
        };

        let torrent_info_hash = context.metadata().info_hash.clone();
        while let Ok(event) = receiver.try_recv() {
            match &*event {
                LocalServiceDiscoveryEvent::PeerDiscovered(info_hash, peer) => {
                    if info_hash != &torrent_info_hash {
                        continue;
                    }

                    context.add_peer_addresses(vec![*peer]);
                }
                _ => {}
            }
        }
    }
}

#[async_trait]
impl TorrentOperation for TorrentLsdPeersOperation {
    fn name(&self) -> &str {
        "retrieve lsd peers operation"
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LocalServiceDiscovery;
    use std::net::Ipv4Addr;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_execute() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let lsd = LocalServiceDiscovery::new(Ipv4Addr::LOCALHOST.into())
            .await
            .unwrap();
        let (mut context, _) = create_torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            None,
            Some(lsd)
        );
        let mut operation = TorrentLsdPeersOperation::new();

        // execute the operation
        let result = operation.execute(&mut context, vec![].as_slice()).await;
        assert_eq!(TorrentOperationResult::Continue, result);

        // verify that the operation subscribed to the events
        assert!(
            operation.receiver.is_some(),
            "expected receiver to be initialized"
        );
    }
}
