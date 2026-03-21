use crate::operation::{TorrentOperation, TorrentOperationResult};
use crate::peer::PeerDiscovery;
use crate::tracker::TrackerEntry;
use crate::{TorrentContext, TorrentState};
use async_trait::async_trait;
use log::{debug, trace};
use std::sync::Arc;
#[cfg(feature = "tracing")]
use tracing::instrument;

/// The torrent trackers operation is responsible for adding the known trackers to the torrent.
/// This operation add the trackers in a "fire-and-forget" mode and only waits for one tracker connection to have been established.
#[derive(Debug)]
pub struct TorrentTrackersOperation {
    initialized: bool,
    cached_tiered_trackers: Vec<TrackerEntry>,
}

impl TorrentTrackersOperation {
    pub fn new() -> Self {
        Self {
            initialized: Default::default(),
            cached_tiered_trackers: Vec::new(),
        }
    }

    /// Get the tiered trackers from the metadata of the torrent.
    /// Returns false if the tiered trackers could not be created.
    async fn create_trackers_cache(&mut self, torrent: &mut TorrentContext) -> bool {
        let tiered_trackers = torrent.metadata().tiered_trackers();
        if tiered_trackers.is_empty() {
            debug!(
                "Torrent {} is unable to create tiered trackers, no tiered trackers found in metadata",
                torrent
            );
            torrent.update_state(TorrentState::Error).await;
            return false;
        }

        // create the tracker entries of the torrent to which we want to connect
        {
            let tracker_entries = tiered_trackers
                .into_iter()
                .map(|(tier, trackers)| {
                    trackers
                        .into_iter()
                        .map(|url| TrackerEntry { tier, url })
                        .collect::<Vec<_>>()
                })
                .flatten()
                .collect();
            self.cached_tiered_trackers = tracker_entries;
        }

        // retrieve the initial/previously discovered peers from the tracker
        {
            let tracker_manager = torrent.tracker_manager();
            if let Some(initial_discovered_peers) = tracker_manager
                .discovered_peers(&torrent.metadata().info_hash)
                .await
            {
                torrent.add_peer_addresses(initial_discovered_peers);
            }
        }

        self.initialized = true;
        true
    }

    /// Try to add the trackers from the cache to the torrent.
    async fn add_trackers_from_cache(&mut self, context: &TorrentContext) {
        let entries: Vec<_> = self.cached_tiered_trackers.drain(..).collect();
        if entries.is_empty() {
            return;
        }

        let total_entries = entries.len();
        let handle = context.handle();
        let tracker_manager = context.tracker_manager().clone();
        tokio::spawn(async move {
            let futures = entries
                .into_iter()
                .map(|entry| async {
                    let url = entry.url.clone();
                    match tracker_manager.add_tracker_entry(entry).await {
                        Ok(tracker_handle) => {
                            trace!(
                                "Tracker {}({}) has been added to torrent {}",
                                url,
                                tracker_handle,
                                handle
                            );
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                })
                .collect::<Vec<_>>();
            for e in futures::future::join_all(futures)
                .await
                .into_iter()
                .flat_map(|e| e.err())
            {
                debug!("Torrent {} failed to add tracker, {}", handle, e);
            }
        });

        debug!(
            "Queued a total of {} new trackers for {}",
            total_entries, context
        );
    }
}

#[async_trait]
impl TorrentOperation for TorrentTrackersOperation {
    fn name(&self) -> &str {
        "connect trackers operation"
    }

    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn execute(
        &mut self,
        context: &mut TorrentContext,
        _: &[Arc<dyn PeerDiscovery>],
    ) -> TorrentOperationResult {
        // build the tiered trackers cache if needed
        if !self.initialized {
            // if we're unable to create the tiered trackers
            // then stop the operation chain as we're unable to continue
            if !self.create_trackers_cache(context).await {
                return TorrentOperationResult::Stop;
            }
        }

        self.add_trackers_from_cache(&context).await;
        // check if the metadata is known or if there are active tracker connections
        // if not, we wait for at least one tracker connection
        let is_metadata_known = context.metadata().info.is_some();
        if is_metadata_known || context.active_tracker_connections().await > 0 {
            TorrentOperationResult::Continue
        } else {
            TorrentOperationResult::Stop
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_torrent_context;
    use crate::tracker::TrackerClientEvent;
    use fx_callback::Callback;
    use tempfile::tempdir;
    use tokio::sync::mpsc::unbounded_channel;

    #[tokio::test]
    async fn test_execute_metadata_info_unknown() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "magnet:?xt=urn:btih:2C6B6858D61DA9543D4231A71DB4B1C9264B0685&dn=Ubuntu%2022.04%20LTS&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
        let (mut context, _) = create_torrent_context!(
            uri,
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![]
        );
        let (tx, mut rx) = unbounded_channel();
        let mut operation = TorrentTrackersOperation::new();

        // subscribe to the tracker events
        let mut receiver = context.tracker_manager().subscribe();
        tokio::spawn(async move {
            while let Some(event) = receiver.recv().await {
                if let TrackerClientEvent::TrackerAdded(_) = *event {
                    tx.send(()).unwrap();
                    break;
                }
            }
        });

        // verify that the chain is stopped if the metadata is unknown and no tracker connections have yet been established
        // to achieve this, prevent the initial operation execution from creating the tiered trackers cache
        operation.initialized = true;
        let result = operation.execute(&mut context, vec![].as_slice()).await;
        assert_eq!(TorrentOperationResult::Stop, result, "expected the chain to stop if the metadata is unknown and no tracker connections have yet been established");

        // create the tiered trackers
        operation.initialized = false;
        let _ = operation.execute(&mut context, vec![].as_slice()).await;

        // wait for a tracker connection to be established
        timeout!(
            rx.recv(),
            Duration::from_secs(2),
            "expected a tracker connection to have been established"
        )
        .unwrap();

        let result = operation.execute(&mut context, vec![].as_slice()).await;
        assert_eq!(TorrentOperationResult::Continue, result);
    }

    #[tokio::test]
    async fn test_execute_metadata_info_known() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "debian-udp.torrent";
        let (mut context, _) = create_torrent_context!(
            uri,
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![]
        );
        let mut operation = TorrentTrackersOperation::new();

        let result = operation.execute(&mut context, vec![].as_slice()).await;
        assert_eq!(
            TorrentOperationResult::Continue,
            result,
            "expected the chain to continue if the metadata info is known"
        );
    }
}
