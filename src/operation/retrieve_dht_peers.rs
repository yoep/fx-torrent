use crate::dht::Error;
use crate::operation::{TorrentOperation, TorrentOperationResult};
use crate::peer::PeerDiscovery;
use crate::{InnerTorrent, TorrentContext};
use async_trait::async_trait;
use log::debug;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

const RETRIEVE_INTERVAL: Duration = Duration::from_secs(90);
const RETRIEVE_SHORT_INTERVAL: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(8);

/// Retrieve potential peer addresses for the torrent through the DHT network.
#[derive(Debug)]
pub struct TorrentDhtPeersOperation {
    initialized: bool,
    last_executed: Option<Instant>,
    active_tasks: Vec<JoinHandle<()>>,
    retrieve_timeout: Duration,
}

impl TorrentDhtPeersOperation {
    /// Create a new operation for retrieving peers from the DHT network.
    ///
    /// Each queried node will be limited to 8 seconds.
    pub fn new() -> Self {
        Self::new_with_timeout(QUERY_TIMEOUT)
    }

    /// Create a new operation for retrieving peers from the DHT network.
    ///
    /// Each queried node will be limited to `query_timeout`.
    pub fn new_with_timeout(query_timeout: Duration) -> Self {
        Self {
            initialized: false,
            last_executed: Default::default(),
            active_tasks: Default::default(),
            retrieve_timeout: query_timeout,
        }
    }

    /// Periodically remove handles for tasks that have already finished
    fn cleanup_finished_tasks(&mut self) {
        self.active_tasks.retain(|handle| !handle.is_finished());
    }

    /// Returns `true` when new peers should be requested from the DHT network, else `false`.
    async fn should_retrieve_peers(&self, context: &TorrentContext) -> bool {
        if context.dht().is_none() {
            return false;
        }
        let elapsed = match self.last_executed.as_ref() {
            None => return true,
            Some(last_executed) => last_executed.elapsed(),
        };
        let active_peer_connections = context.active_peer_connections();

        if active_peer_connections > 0 {
            elapsed >= RETRIEVE_INTERVAL
        } else {
            elapsed >= RETRIEVE_SHORT_INTERVAL
        }
    }

    async fn initialize(&mut self, context: &mut TorrentContext) {
        if self.initialized {
            return;
        }

        self.initialized = true;
        let dht = match context.dht() {
            None => return,
            Some(dht) => dht,
        };

        let peers = dht.peers_for(&context.metadata().info_hash).await;
        debug!(
            "Torrent {} discovered initial {} DHT peers",
            context,
            peers.len()
        );
        context.add_peer_addresses(peers.into_iter().map(|e| e.addr).collect());
    }

    /// Retrieve peers from the DHT network for the torrent context.
    fn retrieve_peers(&mut self, context: &mut TorrentContext) {
        let dht = match context.dht() {
            None => return,
            Some(dht) => dht.clone(),
        };

        let info_hash = context.metadata().info_hash.clone();
        let torrent = InnerTorrent::new(
            context.handle(),
            context.command_sender().clone(),
            context.callbacks().clone(),
        );
        let timeout = self.retrieve_timeout;
        self.active_tasks.push(tokio::spawn(async move {
            let result = dht
                .get_peers(&info_hash, 5, timeout)
                .await
                .map_err(|_| Error::Timeout);
            match result {
                Ok(peers) => {
                    debug!("Torrent {} discovered {} DHT peers", torrent, peers.len());
                    torrent.add_peers(peers).await;
                }
                Err(err) => {
                    debug!("Torrent {} failed to retrieve peers, {}", torrent, err);
                }
            }
        }));

        self.last_executed = Some(Instant::now());
    }
}

#[async_trait]
impl TorrentOperation for TorrentDhtPeersOperation {
    fn name(&self) -> &str {
        "retrieve DHT peers operation"
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn execute(
        &mut self,
        context: &mut TorrentContext,
        _: &[PeerDiscovery],
    ) -> TorrentOperationResult {
        self.initialize(context).await;
        self.cleanup_finished_tasks();

        if self.should_retrieve_peers(context).await {
            self.retrieve_peers(context);
        }

        TorrentOperationResult::Continue
    }
}

impl Drop for TorrentDhtPeersOperation {
    fn drop(&mut self) {
        self.active_tasks
            .drain(..)
            .for_each(|handle| handle.abort());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht::DhtTracker;
    use tempfile::tempdir;
    use tokio::time;

    #[tokio::test]
    async fn test_execute() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
        let dht = DhtTracker::builder()
            .default_routing_nodes()
            .build()
            .await
            .unwrap();
        let (mut context, _) = torrent_context!(
            uri,
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            vec![],
            Some(dht)
        );
        let mut operation = TorrentDhtPeersOperation::new_with_timeout(Duration::from_millis(100));

        // execute the operation
        let result = operation.execute(&mut context, vec![].as_slice()).await;
        assert_eq!(TorrentOperationResult::Continue, result);
        assert_eq!(
            true, operation.initialized,
            "expected the operation to have been initialized"
        );

        // check if the last_executed has been set
        let result = &operation.last_executed;
        assert_ne!(&None, result, "expected `last_executed` to have been set");

        // check if the active task has been added
        let result = operation.active_tasks.len();
        assert_eq!(1, result, "expected `active_tasks` to have one operation");

        // run the operation again
        let result = operation.execute(&mut context, vec![].as_slice()).await;
        assert_eq!(TorrentOperationResult::Continue, result);

        // run till completion
        timeout!(
            Duration::from_millis(250),
            async {
                loop {
                    let _ = operation.execute(&mut context, vec![].as_slice()).await;
                    if operation.active_tasks.len() == 0 {
                        break;
                    }
                    time::sleep(Duration::from_millis(10)).await;
                }
            },
            "expected the operation to complete"
        );
        assert_eq!(
            0,
            operation.active_tasks.len(),
            "expected `active_tasks` to be empty"
        );
    }

    mod should_retrieve_peers {
        use super::*;

        #[tokio::test]
        async fn test_dht_none() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
            let (context, _) = torrent_context!(
                uri,
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![],
                vec![],
                None
            );
            let operation = TorrentDhtPeersOperation::new();

            let result = operation.should_retrieve_peers(&context).await;
            assert_eq!(
                false, result,
                "expected `should_retrieve_peers` to return false"
            );
        }

        #[tokio::test]
        async fn test_last_executed() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
            let dht = DhtTracker::builder()
                .default_routing_nodes()
                .build()
                .await
                .unwrap();
            let (context, _) = torrent_context!(
                uri,
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![],
                vec![],
                Some(dht)
            );
            let mut operation = TorrentDhtPeersOperation::new();

            // when the operation has not been executed yet, it should return true
            operation.last_executed = None;
            let result = operation.should_retrieve_peers(&context).await;
            assert_eq!(
                true, result,
                "expected `should_retrieve_peers` to return true"
            );

            // when the operation has been executed longer than RETRIEVE_SHORT_INTERVAL, it should return true
            operation.last_executed =
                Some(Instant::now() - RETRIEVE_SHORT_INTERVAL - Duration::from_secs(1));
            let result = operation.should_retrieve_peers(&context).await;
            assert_eq!(
                true, result,
                "expected `should_retrieve_peers` to return true"
            );

            // when the operation has been executed shorter than RETRIEVE_SHORT_INTERVAL, it should return false
            operation.last_executed =
                Some(Instant::now() - RETRIEVE_SHORT_INTERVAL + Duration::from_secs(1));
            let result = operation.should_retrieve_peers(&context).await;
            assert_eq!(
                false, result,
                "expected `should_retrieve_peers` to return false"
            );
        }
    }
}
