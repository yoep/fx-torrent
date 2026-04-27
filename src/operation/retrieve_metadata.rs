use crate::operation::{TorrentOperation, TorrentOperationResult};
use crate::peer::PeerDiscovery;
use crate::{InnerTorrent, TorrentContext, TorrentFlags, TorrentMetadataInfo, TorrentState};
use async_trait::async_trait;
use log::{debug, info, trace, warn};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

const OPERATION_NAME: &str = "retrieve metadata operation";
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(10);
const RETRIEVE_INTERVAL: Duration = Duration::from_secs(20);
const RETRIEVE_TIMEOUT: Duration = Duration::from_secs(15);

/// The torrent metadata operation is responsible for checking if the metadata for a torrent is present and if not, retrieving it from peers.
#[derive(Debug)]
pub struct TorrentMetadataOperation {
    metadata_present: bool,
    active_tasks: Vec<JoinHandle<()>>,
    last_executed: Option<Instant>,
    last_announce: Option<Instant>,
    retrieve_timeout: Duration,
}

impl TorrentMetadataOperation {
    /// Create a new torrent metadata operation.
    pub fn new(retrieve_timeout: Option<Duration>) -> Self {
        Self {
            metadata_present: false,
            active_tasks: Default::default(),
            last_executed: None,
            last_announce: None,
            retrieve_timeout: retrieve_timeout.unwrap_or(RETRIEVE_TIMEOUT),
        }
    }

    /// Returns `true` if the metadata should be retrieved, else `false`.
    fn should_retrieve_metadata(&self, torrent: &TorrentContext) -> bool {
        let is_execute_tick_allowed = self
            .last_executed
            .map_or(true, |last| last.elapsed() >= RETRIEVE_INTERVAL);

        torrent.options().contains(TorrentFlags::Metadata) && is_execute_tick_allowed
    }

    /// Returns `true` if the torrent should be announced to trackers, else `false`.
    fn is_announcement_allowed(&self) -> bool {
        match self.last_announce {
            None => true,
            Some(last) => last.elapsed() >= ANNOUNCE_INTERVAL,
        }
    }

    /// Periodically remove handles for tasks that have already finished
    fn cleanup_finished_tasks(&mut self) {
        self.active_tasks.retain(|handle| !handle.is_finished());
    }

    /// Synchronizes the internal `metadata_present` flag with the [`TorrentContext`].
    fn update_local_state(&mut self, torrent: &TorrentContext) {
        let metadata = torrent.metadata();
        self.metadata_present = metadata.info.is_some();
    }

    /// Try to retrieve the metadata from either peers or the DHT network if enabled.
    async fn retrieve_metadata(&mut self, torrent: &mut TorrentContext) {
        debug!("Torrent {} initiating metadata retrieval", torrent);
        self.retrieve_peer_metadata(torrent).await;
        self.retrieve_dht_metadata(torrent).await;
        self.last_executed = Some(Instant::now());
    }

    #[cfg(feature = "extension-metadata")]
    async fn retrieve_peer_metadata(&mut self, torrent: &TorrentContext) {
        // check if there have been any peers discovered yet
        // if not, we want to retrieve the peers from trackers
        if torrent.discovered_peers().await.len() == 0 {
            if !self.is_announcement_allowed() {
                return;
            }

            trace!("No peers discovered yet, requesting from trackers");
            torrent.announce_all(None);
            self.last_announce = Some(Instant::now());
        }

        // once at least 1 connection is established,
        // the peer [MetadataExtensionMessage] handles metadata retrieval, if enabled
    }

    #[cfg(not(feature = "extension-metadata"))]
    async fn retrieve_peer_metadata(&self, _: &TorrentContext) {
        // no-op, as we're unable to retrieve metadata from peers without the metadata extension
    }

    #[cfg(feature = "dht")]
    async fn retrieve_dht_metadata(&mut self, torrent: &TorrentContext) {
        let dht = match torrent.dht() {
            None => return,
            Some(dht) => dht.clone(),
        };

        let info_hash = torrent.metadata().info_hash.clone();
        let torrent = InnerTorrent::new(
            torrent.handle(),
            torrent.command_sender().clone(),
            torrent.callbacks().clone(),
        );
        let timeout = self.retrieve_timeout;
        self.active_tasks.push(tokio::spawn(async move {
            trace!("Torrent {} retrieving metadata from DHT network", torrent);
            let result = dht
                .get::<TorrentMetadataInfo>(info_hash.short_info_hash_bytes(), timeout, 5)
                .await;

            match result {
                Ok(Some(metadata)) => {
                    torrent.set_metadata(metadata).await;
                    info!(
                        "Torrent {} DHT network retrieved metadata for {}",
                        torrent, info_hash
                    );
                }
                Ok(None) => {
                    debug!(
                        "Torrent {} DHT network couldn't find metadata for {}",
                        torrent, info_hash
                    );
                }
                Err(e) => {
                    warn!(
                        "Torrent {} DHT network failed to retrieve metadata for {}, {}",
                        torrent, info_hash, e
                    );
                }
            };
        }));
    }

    #[cfg(not(feature = "dht"))]
    async fn retrieve_dht_metadata(&self, _: &TorrentContext) {
        // no-op, as we're unable to retrieve metadata from the DHT network without the dht feature enabled
    }
}

#[async_trait]
impl TorrentOperation for TorrentMetadataOperation {
    fn name(&self) -> &str {
        OPERATION_NAME
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn execute(
        &mut self,
        torrent: &mut TorrentContext,
        _: &[PeerDiscovery],
    ) -> TorrentOperationResult {
        self.cleanup_finished_tasks();
        self.update_local_state(torrent);
        if self.metadata_present {
            return TorrentOperationResult::Continue;
        }

        if self.should_retrieve_metadata(torrent) {
            torrent.update_state(TorrentState::RetrievingMetadata).await;
            self.retrieve_metadata(torrent).await;
        }

        // in both cases, we want to stop the operations chain as the torrent cannot continue
        // until the metadata is known
        TorrentOperationResult::Stop
    }
}

impl Drop for TorrentMetadataOperation {
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

    #[test]
    fn test_name() {
        let operation = TorrentMetadataOperation::new(None);
        assert_eq!(OPERATION_NAME, operation.name());
    }

    #[tokio::test]
    async fn test_execute_metadata_known() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (mut context, _) = create_torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            None
        );
        let mut operation = TorrentMetadataOperation::new(None);

        let result = operation.execute(&mut context, vec![].as_slice()).await;

        assert_eq!(TorrentOperationResult::Continue, result);
    }

    #[tokio::test]
    async fn test_execute_metadata_unknown_and_metadata_option_disabled() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
        let (mut context, _) = create_torrent_context!(
            uri,
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            None
        );
        let mut operation = TorrentMetadataOperation::new(None);

        let result = operation.execute(&mut context, vec![].as_slice()).await;

        assert_eq!(TorrentOperationResult::Stop, result);
    }

    #[tokio::test]
    #[cfg(feature = "dht")]
    async fn test_execute_dht() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
        let dht = DhtTracker::builder()
            .enable_indexing(false)
            .default_routing_nodes()
            .build()
            .await
            .unwrap();
        let (mut context, _) = create_torrent_context!(
            uri,
            temp_path,
            TorrentFlags::Metadata,
            TorrentConfig::builder().build(),
            vec![],
            Some(dht)
        );
        let mut operation = TorrentMetadataOperation::new(Some(Duration::from_millis(100)));

        // execute the operation to create a new DHT operation
        let result = operation.execute(&mut context, vec![].as_slice()).await;
        assert_eq!(TorrentOperationResult::Stop, result);
        assert_ne!(
            None, operation.last_executed,
            "expected last_executed to been set"
        );
        assert_eq!(
            1,
            operation.active_tasks.len(),
            "expected a DHT operation to have been in_flight"
        );

        // execute it again, the in_flight should not have been updated
        let _ = operation.execute(&mut context, vec![].as_slice()).await;
        assert_eq!(
            1,
            operation.active_tasks.len(),
            "expected a DHT operation to still be in_flight"
        );

        // run till completion
        timeout!(
            Duration::from_millis(200),
            async {
                loop {
                    let _ = operation.execute(&mut context, vec![].as_slice()).await;
                    if operation.active_tasks.is_empty() {
                        break;
                    }
                    time::sleep(Duration::from_millis(10)).await;
                }
            },
            "expected the DHT operation to have been completed"
        );
        assert_eq!(
            0,
            operation.active_tasks.len(),
            "expected the DHT operation to be completed"
        );
    }
}
