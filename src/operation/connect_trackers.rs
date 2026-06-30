use crate::operation::TorrentOperationResult;
use crate::tracker::{TrackerClient, TrackerClientEvent, TrackerEntry};
use crate::{InnerTorrent, TorrentContext, TorrentEvent};
use fx_callback::{Callback, Subscription};
use log::{debug, trace, warn};

/// The torrent trackers operation is responsible for adding the known trackers to the torrent.
/// This operation add the trackers in a "fire-and-forget" mode and only waits for one tracker connection to have been established.
#[derive(Debug)]
pub struct TrackersOperation {
    initialized: bool,
    receiver: Option<Subscription<TrackerClientEvent>>,
    cached_tiered_trackers: Vec<TrackerEntry>,
}

impl TrackersOperation {
    pub fn new() -> Self {
        Self {
            initialized: Default::default(),
            receiver: None,
            cached_tiered_trackers: Vec::new(),
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn execute(&mut self, context: &mut TorrentContext) -> TorrentOperationResult {
        self.initialize(context).await;
        self.add_trackers_from_cache(context).await;
        self.process_tracker_events(context).await;

        // verify if the metadata is known or if there are active tracker connections
        // if not, we wait for at least one tracker connection
        let is_metadata_known = context.metadata().info.is_some();
        if is_metadata_known || context.active_tracker_connections().await > 0 {
            TorrentOperationResult::Continue
        } else {
            TorrentOperationResult::Stop
        }
    }

    async fn initialize(&mut self, context: &mut TorrentContext) {
        if self.initialized {
            return;
        }

        self.initialized = true;
        let tracker = match context.tracker() {
            None => return,
            Some(tracker) => tracker,
        };
        // register to the tracker events
        self.receiver = Some(tracker.subscribe());
        // register the torrent with the tracker client
        if let Err(e) = tracker
            .add_torrent(
                context.peer_id(),
                context.peer_port().copied().unwrap_or(6881),
                context.metadata().info_hash.clone(),
                context.metrics().clone(),
            )
            .await
        {
            warn!("Torrent {} failed to register, {}", context, e);
        }

        self.create_trackers_cache(context).await;
    }

    /// Get the tiered trackers from the metadata of the torrent.
    /// Returns false if the tiered trackers could not be created.
    async fn create_trackers_cache(&mut self, context: &TorrentContext) {
        let tiered_trackers = context.metadata().tiered_trackers();
        if tiered_trackers.is_empty() {
            debug!(
                "Torrent {} is unable to create tiered trackers, no tiered trackers found in metadata",
                context
            );
            return;
        }

        // create the tracker entries of the torrent to which we want to connect
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

    /// Try to add the trackers from the cache to the torrent.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn add_trackers_from_cache(&mut self, context: &TorrentContext) {
        let take = self.cached_tiered_trackers.len().min(3);
        let entries: Vec<_> = self.cached_tiered_trackers.drain(..take).collect();
        if entries.is_empty() {
            return;
        }
        let tracker = match context.tracker() {
            None => return,
            Some(tracker) => tracker.clone(),
        };

        let torrent = InnerTorrent::new(
            context.handle(),
            context.command_sender().clone(),
            context.callbacks().clone(),
        );
        let total_entries = entries.len();
        spawn!("TrackersOperation::add_trackers", async move {
            Self::add_trackers(torrent, entries, tracker).await;
        });

        debug!(
            "Torrent {} queued a total of {} new trackers",
            context, total_entries
        );
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

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn process_tracker_events(&mut self, context: &mut TorrentContext) {
        let receiver = match self.receiver.as_mut() {
            None => return,
            Some(receiver) => receiver,
        };
        let tracker = match context.tracker() {
            None => return,
            Some(tracker) => tracker,
        };

        let mut trackers_changed = false;
        let announce_event = context.announce_event();
        while let Ok(event) = receiver.try_recv() {
            match &*event {
                TrackerClientEvent::TrackerAdded(handle) => {
                    tracker
                        .make_announcement(*handle, &context.metadata().info_hash, announce_event)
                        .await;
                    trackers_changed = true;
                }
                _ => {}
            }
        }

        if trackers_changed {
            context.invoke_event(TorrentEvent::TrackersChanged);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tracker::UdpServer;
    use crate::tracker::{TrackerClientEvent, TrackerServer};
    use fx_callback::Callback;
    use percent_encoding::{percent_encode, AsciiSet, NON_ALPHANUMERIC};
    use tempfile::tempdir;
    use tokio::sync::mpsc::unbounded_channel;

    const URL_ENCODE_RESERVED: &AsciiSet = &NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'~')
        .remove(b'.');

    #[tokio::test]
    async fn test_execute_metadata_info_unknown() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let server =
            TrackerServer::with_listeners(vec![Box::new(UdpServer::with_port(0).await.unwrap())])
                .unwrap();
        let server_uri = percent_encode(server.url().as_str().as_bytes(), URL_ENCODE_RESERVED);
        let uri = format!("magnet:?xt=urn:btih:2C6B6858D61DA9543D4231A71DB4B1C9264B0685&dn=Ubuntu%2022.04%20LTS&tr={}", server_uri);
        let (mut context, _) = torrent_context!(
            uri.as_str(),
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![]
        );
        let (tx, mut rx) = unbounded_channel();
        let mut operation = TrackersOperation::new();

        // subscribe to the tracker events
        let mut receiver = context.tracker().unwrap().subscribe();
        tokio::spawn(async move {
            while let Ok(event) = receiver.recv().await {
                if let TrackerClientEvent::TrackerAdded(_) = *event {
                    tx.send(()).unwrap();
                    break;
                }
            }
        });

        // verify that the chain is stopped if the metadata is unknown and no tracker connections have not yet been established
        // to achieve this, prevent the initial operation execution from creating the tiered trackers cache
        operation.initialized = true;
        let result = operation.execute(&mut context).await;
        assert_eq!(TorrentOperationResult::Stop, result, "expected the chain to stop if the metadata is unknown and no tracker connections have yet been established");

        // create the tiered trackers
        operation.initialized = false;
        let _ = operation.execute(&mut context).await;

        // wait for a tracker connection to be established
        timeout!(
            Duration::from_secs(2),
            rx.recv(),
            "expected a tracker connection to have been established"
        )
        .unwrap();

        let result = operation.execute(&mut context).await;
        assert_eq!(TorrentOperationResult::Continue, result);
    }

    #[tokio::test]
    async fn test_execute_metadata_info_known() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "debian-udp.torrent";
        let (mut context, _) = torrent_context!(
            uri,
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![]
        );
        let mut operation = TrackersOperation::new();

        let result = operation.execute(&mut context).await;
        assert_eq!(
            TorrentOperationResult::Continue,
            result,
            "expected the chain to continue if the metadata info is known"
        );
    }
}
