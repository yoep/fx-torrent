use crate::operation::TorrentOperationResult;
use crate::{LocalServiceDiscoveryEvent, TorrentContext};
use fx_callback::{Callback, Subscription};

/// Retrieve torrent peers from the local service discovery.
#[derive(Debug)]
pub struct LsdPeersOperation {
    initialized: bool,
    receiver: Option<Subscription<LocalServiceDiscoveryEvent>>,
}

impl LsdPeersOperation {
    /// Create a new instance for retrieving torrent peers from the local service discovery.
    pub fn new() -> Self {
        Self {
            initialized: false,
            receiver: None,
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn execute(&mut self, context: &mut TorrentContext) -> TorrentOperationResult {
        if !self.initialized {
            self.initialize(context);
        }

        self.process_events(context);
        TorrentOperationResult::Continue
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
        let (mut context, _) = torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            vec![],
            None,
            Some(lsd)
        );
        let mut operation = LsdPeersOperation::new();

        // execute the operation
        let result = operation.execute(&mut context).await;
        assert_eq!(TorrentOperationResult::Continue, result);

        // verify that the operation subscribed to the events
        assert!(
            operation.receiver.is_some(),
            "expected receiver to be initialized"
        );
    }
}
