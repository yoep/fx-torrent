use crate::operation::{TorrentOperation, TorrentOperationResult};
use crate::peer::PeerDiscovery;
use crate::tracker::TrackerClientEvent;
use crate::TorrentContext;
use async_trait::async_trait;
use fx_callback::{Callback, Subscription};
use log::{debug, warn};
use std::sync::Arc;
use std::time::{Duration, Instant};

const PEER_DISCOVERY_INTERVAL: Duration = Duration::from_secs(10);

/// Retrieve torrent peers from connected trackers.
#[derive(Debug)]
pub struct TorrentTrackerPeersOperation {
    initialized: bool,
    receiver: Option<Subscription<TrackerClientEvent>>,
    last_peers_discovery: Option<Instant>,
}

impl TorrentTrackerPeersOperation {
    pub fn new() -> Self {
        Self {
            initialized: false,
            receiver: None,
            last_peers_discovery: None,
        }
    }

    /// Returns `true` if peer discovery is allowed based on the last time it was scraped.
    fn is_peer_discovery_allowed(&self) -> bool {
        self.last_peers_discovery.map_or(true, |last_discovery| {
            last_discovery.elapsed() >= PEER_DISCOVERY_INTERVAL
        })
    }

    async fn initialize(&mut self, context: &mut TorrentContext) {
        self.initialized = true;
        let tracker = match context.tracker() {
            None => return,
            Some(tracker) => tracker,
        };
        // subscribe to the tracker events
        self.receiver = Some(tracker.subscribe());
        // retrieve previously discovered peers from the tracker client
        match tracker
            .discovered_peers(&context.metadata().info_hash)
            .await
        {
            Some(peers) => {
                debug!(
                    "Torrent {} discovered {} initial peer(s)",
                    context,
                    peers.len()
                );
                context.add_peer_addresses(peers);
            }
            None => debug!("Torrent {} discovered no initial peers", context),
        }
    }

    /// Get the discovered peers from the trackers.
    async fn retrieve_discovered_peers(&mut self, context: &mut TorrentContext) {
        if !self.is_peer_discovery_allowed() {
            return;
        }
        let tracker = match context.tracker() {
            None => return,
            Some(tracker) => tracker,
        };

        match tracker
            .discovered_peers(&context.metadata().info_hash)
            .await
        {
            Some(peers) => {
                context.add_peer_addresses(peers);
            }
            None => {
                warn!(
                    "Torrent {} is no longer being tracked by {}",
                    context, tracker
                );
            }
        };
        self.last_peers_discovery = Some(Instant::now());
    }

    /// Process pending tracker events.
    fn process_tracker_events(&mut self, context: &mut TorrentContext) {
        let receiver = match self.receiver.as_mut() {
            None => return,
            Some(receiver) => receiver,
        };

        while let Ok(event) = receiver.try_recv() {
            match &*event {
                TrackerClientEvent::PeersDiscovered(info_hash, peers) => {
                    if *info_hash == context.metadata().info_hash {
                        context.add_peer_addresses(peers.clone());
                    }
                }
                _ => {}
            }
        }
    }
}

#[async_trait]
impl TorrentOperation for TorrentTrackerPeersOperation {
    fn name(&self) -> &str {
        "retrieve tracker peers operation"
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn execute(
        &mut self,
        context: &mut TorrentContext,
        _: &[Arc<dyn PeerDiscovery>],
    ) -> TorrentOperationResult {
        if !self.initialized {
            self.initialize(context).await;
        }

        self.process_tracker_events(context);
        self.retrieve_discovered_peers(context).await;

        TorrentOperationResult::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::PeerId;
    use crate::tracker::{AnnounceEvent, TrackerEntry, TrackerServer};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_is_peer_discovery_allowed() {
        init_logger!();
        let mut operation = TorrentTrackerPeersOperation::new();

        // get the initial state
        let result = operation.is_peer_discovery_allowed();
        assert_eq!(true, result);

        // should return false when the last discovery was within the PEER_DISCOVERY_INTERVAL
        operation.last_peers_discovery = Some(Instant::now());
        let result = operation.is_peer_discovery_allowed();
        assert_eq!(false, result);
    }

    #[tokio::test]
    async fn test_execute() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "debian-udp.torrent";
        let (mut context, _) = create_torrent_context!(
            uri,
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            None
        );
        let mut operation = TorrentTrackerPeersOperation::new();

        // execute the operation once to initialize
        let result = operation.execute(&mut context, vec![].as_slice()).await;
        assert_eq!(TorrentOperationResult::Continue, result);

        // verify that a subscription has been made to the tracker client
        assert!(
            operation.receiver.is_some(),
            "expected a subscription to the tracker events"
        );
        // verify that the last discovered peers scrape time has been set
        assert!(
            operation.last_peers_discovery.is_some(),
            "expected the last peers discovery time to be set"
        );
    }

    #[tokio::test]
    async fn test_execute_peer_discovery() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "debian-udp.torrent";
        let server = TrackerServer::new().await.unwrap();
        let (mut context, _) = create_torrent_context!(
            uri,
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            None
        );
        let mut operation = TorrentTrackerPeersOperation::new();

        // start tracking the torrent with the tracker
        context
            .tracker()
            .unwrap()
            .add_torrent(
                PeerId::new(),
                6881,
                context.metadata().info_hash.clone(),
                context.metrics().clone(),
            )
            .await
            .expect("expected the torrent to have been tracked");

        // add a new peer to the server
        let peer_port = 6890;
        server
            .add_peer(
                context.metadata().info_hash.clone(),
                ([127, 0, 0, 5], peer_port).into(),
                PeerId::new(),
                peer_port,
                false,
            )
            .await;

        // add the tracker to the client
        let result = context
            .tracker()
            .unwrap()
            .add_tracker_entry(TrackerEntry {
                tier: 0,
                url: server.url().clone(),
            })
            .await;
        assert!(result.is_ok(), "expected Ok(), but got {:?}", result);

        // make sure the peer is already discovered by the tracker
        let result = context
            .tracker()
            .unwrap()
            .announce_all(&context.metadata().info_hash, AnnounceEvent::Started)
            .await;
        assert_eq!(
            1, result.total_leechers,
            "expected the peer to have been discovered"
        );

        // execute the operation to scrape the cached peers
        let _ = operation.execute(&mut context, vec![].as_slice()).await;

        // verify that the peer from the cache was added to the peer pool
        let result = context.peer_pool().num_connect_candidates();
        assert_eq!(
            1, result,
            "expected the peer to have been added to the peer pool"
        );
    }
}
