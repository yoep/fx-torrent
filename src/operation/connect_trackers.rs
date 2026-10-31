use crate::operation::TorrentOperationResult;
use crate::tracker::{TrackerClient, TrackerClientEvent, TrackerEntry};
use crate::{InnerTorrent, TorrentContext, TorrentEvent};
use fx_callback::{Callback, Subscription};
use log::{debug, trace, warn};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;

/// The torrent trackers operation is responsible for adding the known trackers to the torrent.
/// This operation add the trackers in a "fire-and-forget" mode and only waits for one tracker connection to have been established.
///
/// The operation never waits on the tracker client within the torrent tick loop,
/// the registration of the torrent, adding trackers and announcements are executed in background tasks.
#[derive(Debug)]
pub struct TrackersOperation {
    state: TrackerTaskState,
    receiver: Option<Subscription<TrackerClientEvent>>,
    cached_tiered_trackers: Vec<TrackerEntry>,
    connections: Arc<AtomicUsize>,
}

impl TrackersOperation {
    pub fn new() -> Self {
        Self {
            state: Default::default(),
            receiver: None,
            cached_tiered_trackers: Vec::new(),
            connections: Default::default(),
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn execute(&mut self, context: &mut TorrentContext) -> TorrentOperationResult {
        self.initialize(context);
        self.add_trackers_from_cache(context);
        self.process_tracker_events(context);

        // verify if the metadata is known or if there are active tracker connections
        // if not, we wait for at least one tracker connection
        let is_metadata_known = context.metadata().info.is_some();
        if is_metadata_known || self.connections.load(Ordering::Relaxed) > 0 {
            TorrentOperationResult::Continue
        } else {
            TorrentOperationResult::Stop
        }
    }

    fn initialize(&mut self, context: &mut TorrentContext) {
        if self.state != TrackerTaskState::None {
            return;
        }
        let Some(tracker) = context.tracker() else {
            return;
        };

        // subscribe to the tracker events
        self.receiver = Some(tracker.subscribe());
        // register the torrent with the tracker client in the background
        let handle = context.handle();
        let tracker = tracker.clone();
        let peer_id = context.peer_id();
        let peer_port = context.peer_port().copied().unwrap_or(6881);
        let info_hash = context.metadata().info_hash.clone();
        let metrics = context.metrics().clone();
        let name = context.to_string();
        let handle = spawn!("TrackersOperation::register", async move {
            trace!(
                "Torrent {} is being registered with tracker {}",
                handle,
                tracker
            );
            match tracker
                .add_torrent(peer_id, peer_port, info_hash, metrics)
                .await
            {
                Ok(_) => debug!(
                    "Torrent {} has been registered with tracker {}",
                    handle, tracker
                ),
                Err(e) => warn!("Torrent {} failed to register, {}", name, e),
            }
        });

        self.state = TrackerTaskState::RegisterTorrent { handle };
        self.create_trackers_cache(context);
    }

    /// Get the tiered trackers from the metadata of the torrent.
    /// Returns false if the tiered trackers could not be created.
    fn create_trackers_cache(&mut self, context: &TorrentContext) {
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
    fn add_trackers_from_cache(&mut self, context: &TorrentContext) {
        // wait for the registration of the torrent to complete
        // or the  current tracker registration task to complete
        if !self.is_torrent_registration_finished() {
            return;
        }

        let entries: Vec<_> = self.cached_tiered_trackers.drain(..).collect();
        if entries.is_empty() {
            self.state = TrackerTaskState::Finished;
            return;
        }
        let Some(tracker) = context.tracker().cloned() else {
            debug!(
                "Torrent {} is unable to register trackers, no tracker client available",
                context
            );
            self.state = TrackerTaskState::Finished;
            return;
        };

        let torrent = InnerTorrent::new(
            context.handle(),
            context.command_sender().clone(),
            context.callbacks().clone(),
        );
        let total_entries = entries.len();
        let connections = self.connections.clone();
        let handle = spawn!("TrackersOperation::add_trackers", async move {
            trace!(
                "Torrent {} is registering {} trackers",
                torrent.handle(),
                entries.len()
            );
            Self::add_trackers(torrent, entries, &tracker).await;
            connections.fetch_max(tracker.trackers_len().await, Ordering::Relaxed);
        });

        self.state = TrackerTaskState::RegisterTrackers { handle };
        debug!(
            "Torrent {} queued a total of {} new trackers",
            context, total_entries
        );
    }

    async fn add_trackers(
        torrent: InnerTorrent,
        entries: Vec<TrackerEntry>,
        manager: &TrackerClient,
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

    fn process_tracker_events(&mut self, context: &mut TorrentContext) {
        if self.is_trackers_registered() {
            self.state = TrackerTaskState::Finished;
        }

        let Some(receiver) = self.receiver.as_mut() else {
            return;
        };
        let Some(tracker) = context.tracker() else {
            return;
        };

        let mut added_trackers = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            match &*event {
                TrackerClientEvent::TrackerAdded(handle) => added_trackers.push(*handle),
                _ => {}
            }
        }

        if added_trackers.is_empty() {
            return;
        }

        self.connections
            .fetch_add(added_trackers.len(), Ordering::Relaxed);

        // announce to the new trackers in the background
        let handle = context.handle();
        let tracker = tracker.clone();
        let info_hash = context.metadata().info_hash.clone();
        let announce_event = context.announce_event();
        spawn!("TrackersOperation::announce", async move {
            trace!(
                "Torrent {} is being announced to {} trackers",
                handle,
                tracker.trackers_len().await
            );
            for handle in added_trackers {
                tracker
                    .make_announcement(handle, &info_hash, announce_event)
                    .await;
            }
        });

        context.invoke_event(TorrentEvent::TrackersChanged);
    }

    /// Returns `true` if the torrent has been registered with the [TrackerClient].
    fn is_torrent_registration_finished(&self) -> bool {
        if let TrackerTaskState::RegisterTorrent { handle } = &self.state {
            return handle.is_finished();
        }

        false
    }

    /// Returns `true` if the current trackers registration has been finished.
    fn is_trackers_registered(&self) -> bool {
        if let TrackerTaskState::RegisterTrackers { handle } = &self.state {
            return handle.is_finished();
        }

        false
    }
}

impl Drop for TrackersOperation {
    fn drop(&mut self) {
        match &self.state {
            TrackerTaskState::RegisterTorrent { handle } => handle.abort(),
            TrackerTaskState::RegisterTrackers { handle } => handle.abort(),
            _ => {}
        }
    }
}

#[derive(Debug)]
enum TrackerTaskState {
    None,
    RegisterTorrent { handle: JoinHandle<()> },
    RegisterTrackers { handle: JoinHandle<()> },
    Finished,
}

impl PartialEq for TrackerTaskState {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (TrackerTaskState::None, TrackerTaskState::None) => true,
            (
                TrackerTaskState::RegisterTorrent { .. },
                TrackerTaskState::RegisterTorrent { .. },
            ) => true,
            (
                TrackerTaskState::RegisterTrackers { .. },
                TrackerTaskState::RegisterTrackers { .. },
            ) => true,
            (TrackerTaskState::Finished, TrackerTaskState::Finished) => true,
            _ => false,
        }
    }
}

impl Default for TrackerTaskState {
    fn default() -> Self {
        Self::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tracker::UdpServer;
    use crate::tracker::{TrackerClientEvent, TrackerServer};
    use fx_callback::Callback;
    use percent_encoding::{percent_encode, AsciiSet, NON_ALPHANUMERIC};
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::{select, time};

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
            vec![],
            vec![],
            None
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
        operation.state = TrackerTaskState::RegisterTorrent {
            handle: spawn!("TrackerOperation::test", async {
                time::sleep(Duration::from_secs(2)).await;
            }),
        };
        let result = operation.execute(&mut context).await;
        assert_eq!(TorrentOperationResult::Stop, result, "expected the chain to stop if the metadata is unknown and no tracker connections have yet been established");

        // create the tiered trackers
        operation.state = TrackerTaskState::None;
        select! {
            _ = time::sleep(Duration::from_secs(5)) => assert!(false, "timeout waiting for tracker registration"),
            _ = async {
                loop {
                    operation.execute(&mut context).await;
                    if let TrackerTaskState::RegisterTrackers { .. } = &operation.state {
                        break;
                    }
                    time::sleep(Duration::from_millis(25)).await;
                }
            } => {}
        }

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

    #[tokio::test]
    async fn test_execute_does_not_block() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "magnet:?xt=urn:btih:2C6B6858D61DA9543D4231A71DB4B1C9264B0685&dn=Ubuntu&tr=udp%3A%2F%2F10.255.255.1%3A6969";
        let (mut context, _) = torrent_context!(
            uri,
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![]
        );
        let mut operation = TrackersOperation::new();

        let result = timeout!(
            Duration::from_millis(250),
            operation.execute(&mut context),
            "expected the operation not to block on the tracker client"
        );

        assert_eq!(TorrentOperationResult::Stop, result);
    }
}
