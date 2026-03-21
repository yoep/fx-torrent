use crate::operation::{TorrentOperation, TorrentOperationResult};
use crate::peer::PeerDiscovery;
use crate::tracker::{TrackerClient, TrackerEntry};
use crate::{InnerTorrent, TorrentContext, TorrentState};
use async_trait::async_trait;
use log::{debug, trace, warn};
use std::sync::Arc;
use std::time::{Duration, Instant};
#[cfg(feature = "tracing")]
use tracing::instrument;

const PEER_DISCOVERY_INTERVAL: Duration = Duration::from_secs(10);

/// The torrent trackers operation is responsible for adding the known trackers to the torrent.
/// This operation add the trackers in a "fire-and-forget" mode and only waits for one tracker connection to have been established.
#[derive(Debug)]
pub struct TorrentTrackersOperation {
    initialized: bool,
    cached_tiered_trackers: Vec<TrackerEntry>,
    last_peers_discovery: Option<Instant>,
}

impl TorrentTrackersOperation {
    pub fn new() -> Self {
        Self {
            initialized: Default::default(),
            cached_tiered_trackers: Vec::new(),
            last_peers_discovery: None,
        }
    }

    /// Returns `true` if peer discovery is allowed based on the last time it was scraped.
    fn is_peer_discovery_allowed(&self) -> bool {
        self.last_peers_discovery.map_or(true, |last_discovery| {
            last_discovery.elapsed() >= PEER_DISCOVERY_INTERVAL
        })
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
        let take = self.cached_tiered_trackers.len().min(3);
        let entries: Vec<_> = self.cached_tiered_trackers.drain(..take).collect();
        if entries.is_empty() {
            return;
        }

        let torrent = InnerTorrent::new(
            context.handle(),
            context.command_sender().clone(),
            context.callbacks().clone(),
        );
        let total_entries = entries.len();
        let tracker_manager = context.tracker_manager().clone();
        tokio::spawn(async move {
            Self::add_trackers(torrent, entries, tracker_manager).await;
        });

        debug!(
            "Torrent {} queued a total of {} new trackers",
            context, total_entries
        );
    }

    /// Get the discovered peers from the trackers.
    async fn retrieve_discovered_peers(&mut self, context: &mut TorrentContext) {
        if !self.is_peer_discovery_allowed() {
            return;
        }

        match context
            .tracker_manager()
            .discovered_peers(&context.metadata().info_hash)
            .await
        {
            Some(peers) => {
                context.add_peer_addresses(peers);
            }
            None => {
                warn!(
                    "Torrent {} is no longer being tracked by {}",
                    context,
                    context.tracker_manager()
                );
            }
        };
        self.last_peers_discovery = Some(Instant::now());
    }

    async fn add_trackers(
        torrent: InnerTorrent,
        entries: Vec<TrackerEntry>,
        manager: TrackerClient,
    ) {
        let futures = entries
            .into_iter()
            .map(|entry| async {
                let url = entry.url.clone();
                if manager.is_tracker_url_known(&url).await {
                    return Ok(());
                }

                match manager.add_tracker_entry(entry).await {
                    Ok(handle) => {
                        trace!("Torrent {} added tracker {}({})", torrent, handle, url);
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            })
            .collect::<Vec<_>>();

        // log all failed trackers
        futures::future::join_all(futures)
            .await
            .into_iter()
            .flat_map(|e| e.err())
            .for_each(|e| debug!("Torrent {} failed to add tracker, {}", torrent, e));
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

        self.add_trackers_from_cache(context).await;
        self.retrieve_discovered_peers(context).await;

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
    use crate::peer::PeerId;
    use crate::tracker::{AnnounceEvent, TrackerClientEvent, TrackerServer};
    use fx_callback::Callback;
    use tempfile::tempdir;
    use tokio::sync::mpsc::unbounded_channel;

    #[tokio::test]
    async fn test_is_peer_discovery_allowed() {
        init_logger!();
        let mut operation = TorrentTrackersOperation::new();

        // get the initial state
        let result = operation.is_peer_discovery_allowed();
        assert_eq!(true, result);

        // should return false when the last discovery was within the PEER_DISCOVERY_INTERVAL
        operation.last_peers_discovery = Some(Instant::now());
        let result = operation.is_peer_discovery_allowed();
        assert_eq!(false, result);
    }

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
            DhtOption::none()
        );
        let mut operation = TorrentTrackersOperation::new();

        // start tracking the torrent with the tracker
        context
            .tracker_manager()
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
            .tracker_manager()
            .add_tracker_entry(TrackerEntry {
                tier: 0,
                url: server.url().clone(),
            })
            .await;
        assert!(result.is_ok(), "expected Ok(), but got {:?}", result);

        // make sure the peer is already discovered by the tracker
        let result = context
            .tracker_manager()
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
