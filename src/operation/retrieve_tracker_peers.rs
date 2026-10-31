use crate::operation::TorrentOperationResult;
use crate::tracker::TrackerClientEvent;
use crate::TorrentContext;
use futures::FutureExt;
use fx_callback::{Callback, Subscription};
use log::{debug, warn};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

const PEER_DISCOVERY_INTERVAL: Duration = Duration::from_secs(10);

/// Retrieve torrent peers from connected trackers.
#[derive(Debug)]
pub struct TrackerPeersOperation {
    receiver: Option<Subscription<TrackerClientEvent>>,
    task: Option<JoinHandle<Vec<SocketAddr>>>,
    last_peers_discovery: Option<Instant>,
}

impl TrackerPeersOperation {
    pub fn new() -> Self {
        Self {
            receiver: None,
            task: None,
            last_peers_discovery: None,
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn execute(&mut self, context: &mut TorrentContext) -> TorrentOperationResult {
        self.initialize(context);
        self.poll_task(context);
        self.process_tracker_events(context);
        self.retrieve_discovered_peers(context).await;

        TorrentOperationResult::Continue
    }

    /// Returns `true` if peer discovery is allowed based on the last time it was scraped.
    fn is_peer_discovery_allowed(&self) -> bool {
        self.task.is_none()
            && self.last_peers_discovery.map_or(true, |last_discovery| {
                last_discovery.elapsed() >= PEER_DISCOVERY_INTERVAL
            })
    }

    fn initialize(&mut self, context: &mut TorrentContext) {
        // check if the operation is already initialized
        if self.receiver.is_some() {
            return;
        }

        let Some(tracker) = context.tracker() else {
            return;
        };
        // subscribe to the tracker events
        self.receiver = Some(tracker.subscribe());
        // retrieve previously discovered peers from the tracker client
        self.discover_peers(context);
    }

    /// Poll the current discover task for completion, if one is present.
    fn poll_task(&mut self, context: &mut TorrentContext) {
        let Some(peers) = self
            .task
            .as_mut()
            .and_then(|task| task.now_or_never())
            .and_then(|result| result.ok())
        else {
            return;
        };

        debug!(
            "Torrent {} discovered {} peers through trackers",
            context,
            peers.len()
        );
        context.add_peer_addresses(peers);
        self.task = None;
        self.last_peers_discovery = Some(Instant::now());
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

    /// Try to discover peers through the registered trackers for the torrent context.
    fn discover_peers(&mut self, context: &mut TorrentContext) {
        let Some(tracker) = context.tracker() else {
            return;
        };

        let handle = context.handle();
        let info_hash = context.metadata().info_hash.clone();
        let tracker = tracker.clone();
        self.task = Some(spawn!(
            "TrackerPeersOperation::discover_peers",
            async move {
                match tracker.discovered_peers(&info_hash).await {
                    Some(peers) => peers,
                    None => {
                        warn!(
                            "Torrent {} is no longer being tracked by {}",
                            handle, tracker
                        );
                        vec![]
                    }
                }
            }
        ));
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

impl Drop for TrackerPeersOperation {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
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
        let mut operation = TrackerPeersOperation::new();

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
        let (mut context, _) = torrent_context!(
            uri,
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            vec![],
            None
        );
        let mut operation = TrackerPeersOperation::new();

        // execute the operation once to initialize
        let result = operation.execute(&mut context).await;
        assert_eq!(TorrentOperationResult::Continue, result);

        // verify that a subscription has been made to the tracker client
        assert!(
            operation.receiver.is_some(),
            "expected a subscription to the tracker events"
        );
        // verify that the discovery task has been spawned
        assert!(
            operation.task.is_some(),
            "expected the discovery task to have been spawned"
        );

        // wait for the discovery task to complete
        assert_timeout!(
            Duration::from_secs(2),
            {
                let _ = operation.execute(&mut context).await;
                operation.last_peers_discovery.is_some()
            },
            "expected the last peers discovery time to be set"
        );
        assert!(
            operation.task.is_none(),
            "expected the discovery task to have been completed"
        );
    }

    #[tokio::test]
    async fn test_execute_does_not_wait_for_discovery() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "debian-udp.torrent";
        let (mut context, _) = torrent_context!(
            uri,
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            vec![],
            None
        );
        let mut operation = TrackerPeersOperation::new();

        let result = timeout!(
            Duration::from_millis(250),
            operation.execute(&mut context),
            "expected the operation not to wait for the peer discovery"
        );

        assert_eq!(TorrentOperationResult::Continue, result);
        assert!(
            operation.task.is_some(),
            "expected the discovery to run in the background"
        );
    }

    #[tokio::test]
    async fn test_execute_peer_discovery() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "debian-udp.torrent";
        let server = TrackerServer::new().await.unwrap();
        let (mut context, _) = torrent_context!(
            uri,
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            vec![],
            None
        );
        let mut operation = TrackerPeersOperation::new();

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

        // execute the operation until the cached peers have been retrieved by the background task
        assert_timeout!(
            Duration::from_secs(2),
            {
                let _ = operation.execute(&mut context).await;
                context.peer_pool().num_connect_candidates() == 1
            },
            "expected the peer to have been added to the peer pool"
        );
    }
}
