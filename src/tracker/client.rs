use crate::channel::{ChannelReceiver, ChannelSender, Reply};
use crate::metrics::Metric;
use crate::peer::PeerId;
use crate::tracker::{
    AnnounceEvent, Announcement, AnnouncementResponse, Result, ScrapeFileMetrics, ScrapeResult,
    Tracker, TrackerClientMetrics, TrackerError, TrackerHandle, TrackerState,
};
use crate::{InfoHash, Metrics};
use derive_more::Display;
use futures::future;
use fx_callback::{Callback, MultiThreadedCallback, Subscription};
use itertools::Itertools;
use log::{debug, info, trace, warn};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::{select, time};
use tokio_util::sync::CancellationToken;
use url::Url;

const DEFAULT_ANNOUNCEMENT_INTERVAL: Duration = Duration::from_secs(60);
const STATS_INTERVAL: Duration = Duration::from_secs(1);

/// Aggregated announcement result returned by one or more trackers.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct AnnouncementResult {
    /// The total number of leechers reported by the trackers.
    pub total_leechers: u64,
    /// The total number of seeders reported by the trackers.
    pub total_seeders: u64,
    /// The list of peers' addresses reported by the trackers.
    pub peers: Vec<SocketAddr>,
}

impl AnnouncementResult {
    /// Returns the total number of peers reported by the trackers.
    ///
    /// This is simply the length of [`Self::peers`].
    pub fn total_peers(&self) -> u64 {
        self.peers.len() as u64
    }
}

impl FromIterator<AnnouncementResult> for AnnouncementResult {
    fn from_iter<T: IntoIterator<Item = AnnouncementResult>>(iter: T) -> Self {
        let mut result = Self::default();
        for item in iter {
            result.total_leechers += item.total_leechers;
            result.total_seeders += item.total_seeders;
            result.peers.extend(item.peers);
        }
        result
    }
}

#[derive(Debug, Display, Clone, PartialEq)]
#[display("({}) {}", tier, url)]
pub struct TrackerEntry {
    /// The tier of the tracker.
    ///
    /// Lower values indicate higher priority.
    pub tier: u8,
    /// The tracker url to connect to.
    pub url: Url,
}

/// The event that can be emitted by the tracker client.
#[derive(Debug, Clone)]
pub enum TrackerClientEvent {
    /// Emitted when new peers have been discovered for a torrent.
    ///
    /// Contains the [`InfoHash`] of the torrent and the list of newly discovered peer addresses.
    PeersDiscovered(InfoHash, Vec<SocketAddr>),
    /// Emitted when a new tracker has been added to the client.
    ///
    /// Contains the handle of the added tracker.
    TrackerAdded(TrackerHandle),
    /// Emitted when the tracker client's metric stats are updated.
    ///
    /// This is emitted periodically based on [`STATS_INTERVAL`].
    Stats(TrackerClientMetrics),
}

impl PartialEq for TrackerClientEvent {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                TrackerClientEvent::PeersDiscovered(a, _),
                TrackerClientEvent::PeersDiscovered(b, _),
            ) => a == b,
            (TrackerClientEvent::TrackerAdded(a), TrackerClientEvent::TrackerAdded(b)) => a == b,
            (TrackerClientEvent::Stats(_), TrackerClientEvent::Stats(_)) => true,
            _ => false,
        }
    }
}

/// A tracker client that manages communication with one or more trackers
/// for a set of torrents.
///
/// It allows registering torrents, adding trackers, announcing events,
/// scraping statistics, and retrieving discovered peers.
#[derive(Debug, Display, Clone)]
#[display("{}", handle)]
pub struct TrackerClient {
    handle: TrackerHandle,
    sender: ChannelSender<TrackerClientCommand>,
    metrics: TrackerClientMetrics,
    callbacks: MultiThreadedCallback<TrackerClientEvent>,
    cancellation_token: CancellationToken,
}

impl TrackerClient {
    /// Creates a new [`TrackerClient`] instance.
    ///
    /// # Arguments
    ///
    /// * `connection_timeout` - The timeout for tracker connections.
    ///
    /// # Returns
    ///
    /// A [`TrackerClient`] instance with its internal event loop spawned.
    pub fn new(connection_timeout: Duration) -> Self {
        let (command_sender, command_receiver) = channel!(128);
        let mut inner = InnerClient::new(connection_timeout);
        let handle = inner.handle.clone();
        let metrics = inner.metrics.clone();
        let callbacks = inner.callbacks.clone();
        let cancellation_token = inner.cancellation_token.clone();

        // spawn the main loop in a separate task
        tokio::spawn(async move {
            inner.run(command_receiver).await;
        });

        Self {
            handle,
            sender: command_sender,
            metrics,
            callbacks,
            cancellation_token,
        }
    }

    /// Returns the aggregated metric stats of this tracker client.
    pub fn metrics(&self) -> &TrackerClientMetrics {
        &self.metrics
    }

    /// Returns the tracker for the given handle if found, else [None].
    pub async fn get(&self, handle: &TrackerHandle) -> Option<Tracker> {
        self.sender
            .send(|tx| TrackerClientCommand::GetTracker {
                handle: *handle,
                response: tx,
            })
            .await
            .await
            .ok()
            .flatten()
    }

    /// Returns the tracker for the given url if found, else [None].
    pub async fn get_by_url(&self, url: &Url) -> Option<Tracker> {
        self.sender
            .send(|tx| TrackerClientCommand::GetTrackerByUrl {
                url: url.clone(),
                response: tx,
            })
            .await
            .await
            .ok()
            .flatten()
    }

    /// Checks if a given tracker URL is known within this client.
    pub async fn is_tracker_url_known(&self, url: &Url) -> bool {
        self.sender
            .send(|tx| TrackerClientCommand::IsUrlKnown {
                url: url.clone(),
                response: tx,
            })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the URLs of all trackers managed by this client.
    ///
    /// This might return an empty list if no trackers have been added yet.
    pub async fn tracker_urls(&self) -> Vec<Url> {
        self.sender
            .send(|tx| TrackerClientCommand::GetTrackerUrls { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns all trackers managed by this client.
    ///
    /// This might return an empty list if no trackers have been added yet.
    pub async fn trackers(&self) -> Vec<Tracker> {
        self.sender
            .send(|tx| TrackerClientCommand::GetTrackers { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the number of trackers managed by this client.
    ///
    /// This might return `0` if no trackers have been added yet.
    pub async fn trackers_len(&self) -> usize {
        self.sender
            .send(|tx| TrackerClientCommand::GetTrackersLen { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the number of torrents tracked by this client.
    ///
    /// This might return `0` if no torrents have been added yet.
    pub async fn torrents_len(&self) -> usize {
        self.sender
            .send(|tx| TrackerClientCommand::GetTorrentsLen { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Registers a new torrent with the tracker client so that it can discover peers.
    ///
    /// If a torrent with the same [`InfoHash`] is already registered,
    /// this call is effectively a no-op.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - The peer ID of the client.
    /// * `peer_port` - The port on which the client is listening.
    /// * `info_hash` - The info hash of the torrent.
    /// * `metrics` - The metrics of the torrent.
    pub async fn add_torrent(
        &self,
        peer_id: PeerId,
        peer_port: u16,
        info_hash: InfoHash,
        metrics: Metrics,
    ) -> Result<()> {
        self.sender
            .send(|tx| TrackerClientCommand::AddTorrent {
                peer_id,
                peer_port,
                info_hash,
                metrics,
                response: tx,
            })
            .await
            .await
    }

    /// Removes the given torrent [`InfoHash`] from the tracker client.
    /// This stops tracking the torrent and clears any discovered peers for it.
    ///
    /// # Arguments
    ///
    /// * `info_hash` - The info hash of the torrent.
    pub async fn remove_torrent(&self, info_hash: &InfoHash) {
        let _ = self
            .sender
            .fire_and_forget(TrackerClientCommand::RemoveTorrent {
                info_hash: info_hash.clone(),
            })
            .await;
    }

    /// Returns the discovered peers for the given info hash.
    ///
    /// The info hash should first be registered through [`TrackerClient::add_torrent`].
    pub async fn discovered_peers(&self, info_hash: &InfoHash) -> Option<Vec<SocketAddr>> {
        self.sender
            .send(|tx| TrackerClientCommand::DiscoveredTorrentPeers {
                info_hash: info_hash.clone(),
                response: tx,
            })
            .await
            .await
            .ok()
            .flatten()
            .map(|e| e.into_iter().collect_vec())
    }

    /// Adds a new tracker to the client.
    ///
    /// # Arguments
    ///
    /// * `entry` - The tracker entry describing the URL and tier.
    ///
    /// # Returns
    ///
    /// Returns the created tracker handle on success, otherwise a [`TrackerError`].
    pub async fn add_tracker_entry(&self, entry: TrackerEntry) -> Result<TrackerHandle> {
        self.sender
            .send(|tx| TrackerClientCommand::AddTracker {
                entry,
                response: tx,
            })
            .await
            .await
    }

    /// Adds a new tracker to the client on a background task.
    ///
    /// This is the asynchronous "fire-and-forget" variant of
    /// [`TrackerClient::add_tracker_entry`]. Any error is only logged.
    ///
    /// # Arguments
    ///
    /// * `entry` - The tracker entry describing the URL and tier.
    pub async fn add_tracker_async(&self, entry: TrackerEntry) {
        self.sender
            .fire_and_forget(TrackerClientCommand::AddTracker {
                entry,
                response: Reply::empty(),
            })
            .await;
    }

    /// Announces an event for the given torrent to all trackers.
    ///
    /// # Returns
    ///
    /// Returns the aggregated announcement response result from all trackers.
    pub async fn announce_all(
        &self,
        info_hash: &InfoHash,
        event: AnnounceEvent,
    ) -> AnnouncementResult {
        let start_time = Instant::now();
        let result = self
            .sender
            .send(|tx| TrackerClientCommand::AnnounceAll {
                info_hash: info_hash.clone(),
                event,
                response: tx,
            })
            .await
            .await
            .unwrap_or_default();
        let elapsed = start_time.elapsed();
        trace!(
            "Announced to all trackers in {}.{:03} seconds",
            elapsed.as_secs(),
            elapsed.subsec_millis()
        );
        result
    }

    /// Announces the given event for a specific torrent to the specified tracker.
    ///
    /// # Returns
    ///
    /// Returns the announcement result from that tracker or a [`TrackerError`].
    pub async fn announce(
        &self,
        handle: TrackerHandle,
        info_hash: &InfoHash,
        event: AnnounceEvent,
    ) -> Result<AnnouncementResult> {
        self.sender
            .send(|tx| TrackerClientCommand::Announce {
                handle,
                info_hash: info_hash.clone(),
                event,
                response: tx,
            })
            .await
            .await
    }

    /// Announces an event for the given torrent to all trackers.
    ///
    /// This method will spawn the announcement task and return immediately
    /// without waiting for the responses.
    pub async fn make_announcement_to_all(&self, info_hash: &InfoHash, event: AnnounceEvent) {
        self.sender
            .fire_and_forget(TrackerClientCommand::AnnounceAll {
                info_hash: info_hash.clone(),
                event,
                response: Reply::empty(),
            })
            .await;
    }

    /// Announces an event for the given torrent to the specified tracker.
    ///
    /// This method will spawn the announcement task and return immediately
    /// without waiting for the response.
    pub async fn make_announcement(
        &self,
        handle: TrackerHandle,
        info_hash: &InfoHash,
        event: AnnounceEvent,
    ) {
        self.sender
            .fire_and_forget(TrackerClientCommand::Announce {
                handle,
                info_hash: info_hash.clone(),
                event,
                response: Reply::empty(),
            })
            .await;
    }

    /// Scrapes all trackers for stats about the given [`InfoHash`].
    ///
    /// The results from all trackers are aggregated per file.
    ///
    /// # Errors
    ///
    /// Returns [`TrackerError::NoTrackers`] if no trackers are registered.
    pub async fn scrape(&self, info_hash: &InfoHash) -> Result<ScrapeResult> {
        let mut result = ScrapeResult::default();

        let trackers = self
            .sender
            .send(|tx| TrackerClientCommand::GetActiveTrackers { response: tx })
            .await
            .await?;
        if trackers.is_empty() {
            return Err(TrackerError::NoTrackers);
        }

        let scrape_results: Vec<Result<ScrapeResult>> =
            future::join_all(trackers.into_iter().map(|tracker| {
                let tracker_handle = tracker.handle();
                async move {
                    self.sender
                        .send(|tx| TrackerClientCommand::Scrape {
                            handle: tracker_handle,
                            info_hash: info_hash.clone(),
                            response: tx,
                        })
                        .await
                        .await
                }
            }))
            .await;

        for scrape_result in scrape_results.into_iter() {
            match scrape_result {
                Ok(metrics) => {
                    for (hash, metrics) in metrics.files {
                        let file_metrics = result
                            .files
                            .entry(hash)
                            .or_insert(ScrapeFileMetrics::default());
                        file_metrics.complete += metrics.complete;
                        file_metrics.incomplete += metrics.incomplete;
                        file_metrics.downloaded += metrics.downloaded;
                    }
                }
                Err(e) => debug!("Tracker manager {} failed to scrape tracker, {}", self, e),
            }
        }

        Ok(result)
    }

    /// Starts automatically announcing the torrent with the given info hash.
    ///
    /// This re-enables periodic announcements if they were previously stopped
    /// via [`TrackerClient::stop_announcing`].
    pub async fn start_announcing(&self, info_hash: &InfoHash) {
        self.sender
            .fire_and_forget(TrackerClientCommand::UpdateAnnouncingState {
                info_hash: info_hash.clone(),
                announcing: true,
            })
            .await;
    }

    /// Stops automatically announcing the torrent with the given info hash.
    ///
    /// This does not remove the torrent from the tracker client,
    /// but temporarily disables any new automatic announcements.
    /// Use [`TrackerClient::start_announcing`] to enable automatic announcements again.
    pub async fn stop_announcing(&self, info_hash: &InfoHash) {
        self.sender
            .fire_and_forget(TrackerClientCommand::UpdateAnnouncingState {
                info_hash: info_hash.clone(),
                announcing: false,
            })
            .await;
    }

    /// Returns `true` if the torrent is being automatically announced, else `false`.
    pub async fn is_announcing(&self, info_hash: &InfoHash) -> bool {
        self.sender
            .send(|tx| TrackerClientCommand::GetAnnouncingState {
                info_hash: info_hash.clone(),
                response: tx,
            })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns `true` if the tracker client is closed and no longer accepts any operations.
    pub fn is_closed(&self) -> bool {
        self.cancellation_token.is_cancelled() || self.sender.is_closed()
    }

    /// Closes the tracker client, resulting in termination of its operations.
    ///
    /// This cancels the internal event loop and closes all managed tracker connections.
    pub fn close(&self) {
        self.cancellation_token.cancel();
    }
}

impl Callback<TrackerClientEvent> for TrackerClient {
    fn subscribe(&self) -> Subscription<TrackerClientEvent> {
        self.callbacks.subscribe()
    }
}

#[derive(Debug)]
enum TrackerClientCommand {
    /// Get the tracker by handle from the client.
    GetTracker {
        handle: TrackerHandle,
        response: Reply<Option<Tracker>>,
    },
    /// Get the tracker by url from the client.
    GetTrackerByUrl {
        url: Url,
        response: Reply<Option<Tracker>>,
    },
    /// Get all trackers from the client.
    GetTrackers { response: Reply<Vec<Tracker>> },
    /// Get all active tracker from the client.
    GetActiveTrackers { response: Reply<Vec<Tracker>> },
    /// Get the number of trackers in the client.
    GetTrackersLen { response: Reply<usize> },
    /// Add a new tracker to the client.
    AddTracker {
        entry: TrackerEntry,
        response: Reply<Result<TrackerHandle>>,
    },
    /// Check if the given url is already known.
    IsUrlKnown { url: Url, response: Reply<bool> },
    /// Get all known tracker urls from the client.
    GetTrackerUrls { response: Reply<Vec<Url>> },
    /// Get the number of torrents known to the client.
    GetTorrentsLen { response: Reply<usize> },
    /// Add a new tracked torrent to the client.
    AddTorrent {
        peer_id: PeerId,
        peer_port: u16,
        info_hash: InfoHash,
        metrics: Metrics,
        response: Reply<Result<()>>,
    },
    /// Remove a tracked torrent from the client.
    RemoveTorrent { info_hash: InfoHash },
    /// Get the already discovered peers for the given info hash.
    DiscoveredTorrentPeers {
        info_hash: InfoHash,
        response: Reply<Option<HashSet<SocketAddr>>>,
    },
    /// Announce the event to all active trackers.
    AnnounceAll {
        info_hash: InfoHash,
        event: AnnounceEvent,
        response: Reply<AnnouncementResult>,
    },
    /// Announce the event to the specified tracker.
    Announce {
        handle: TrackerHandle,
        info_hash: InfoHash,
        event: AnnounceEvent,
        response: Reply<Result<AnnouncementResult>>,
    },
    /// Update the automatically announcing state of the given torrent.
    UpdateAnnouncingState {
        info_hash: InfoHash,
        announcing: bool,
    },
    /// Returns the current automatically announcing state of the given torrent.
    GetAnnouncingState {
        info_hash: InfoHash,
        response: Reply<bool>,
    },
    /// Scrape the info hash from the given tracker.
    Scrape {
        handle: TrackerHandle,
        info_hash: InfoHash,
        response: Reply<Result<ScrapeResult>>,
    },
}

/// Inner implementation of the tracker client.
///
/// This type is reference-counted and shared by the public [`TrackerClient`].
#[derive(Debug, Display)]
#[display("{}", handle)]
struct InnerClient {
    /// The unique handle of this client.
    handle: TrackerHandle,
    /// Active trackers managed by this client.
    trackers: Vec<Tracker>,
    /// The torrents being tracked by this client.
    torrents: HashMap<InfoHash, TrackerTorrent>,
    /// The timeout for tracker connections.
    connection_timeout: Duration,
    /// Callback dispatcher used to notify subscribers of client events.
    callbacks: MultiThreadedCallback<TrackerClientEvent>,
    /// Aggregated tracker client metrics.
    metrics: TrackerClientMetrics,
    /// Cancellation token used to stop the client event loop and background tasks.
    cancellation_token: CancellationToken,
}

impl InnerClient {
    fn new(connection_timeout: Duration) -> Self {
        Self {
            handle: Default::default(),
            trackers: Default::default(),
            torrents: Default::default(),
            connection_timeout,
            callbacks: MultiThreadedCallback::new(),
            metrics: Default::default(),
            cancellation_token: Default::default(),
        }
    }

    /// Run the main event loop of the tracker client.
    ///
    /// This loop processes commands, performs automatic announcements, and updates stats
    /// until the cancellation token is triggered.
    async fn run(&mut self, mut command_receiver: ChannelReceiver<TrackerClientCommand>) {
        let mut announcement_tick = time::interval(DEFAULT_ANNOUNCEMENT_INTERVAL);
        let mut stats_interval = time::interval(STATS_INTERVAL);

        loop {
            select! {
                _ = self.cancellation_token.cancelled() => break,
                Some(command) = command_receiver.recv() => self.on_command(command).await,
                _ = announcement_tick.tick() => self.do_automatic_announcements().await,
                _ = stats_interval.tick() => self.update_stats().await,
            }
        }

        self.announce_all_stopped().await;
        self.close().await;
        debug!("Tracker client {} main loop has stopped", self);
    }

    async fn on_command(&mut self, command: TrackerClientCommand) {
        match command {
            TrackerClientCommand::GetTracker { handle, response } => {
                response.send(self.find_tracker(handle).cloned())
            }
            TrackerClientCommand::GetTrackerByUrl { url, response } => response.send(
                self.trackers
                    .iter()
                    .find(|tracker| tracker.url() == &url)
                    .cloned(),
            ),
            TrackerClientCommand::GetActiveTrackers { response } => {
                response.send(self.active_trackers().await.cloned().collect_vec())
            }
            TrackerClientCommand::GetTrackers { response } => response.send(self.trackers.clone()),
            TrackerClientCommand::GetTrackersLen { response } => response.send(self.trackers.len()),
            TrackerClientCommand::AddTracker { entry, response } => {
                response.send(self.create_tracker_from_entry(entry).await)
            }
            TrackerClientCommand::IsUrlKnown { url, response } => {
                response.send(self.is_tracker_url_known(&url))
            }
            TrackerClientCommand::GetTrackerUrls { response } => response.send(
                self.trackers
                    .iter()
                    .map(|tracker| tracker.url())
                    .cloned()
                    .collect(),
            ),
            TrackerClientCommand::GetTorrentsLen { response } => response.send(self.torrents.len()),
            TrackerClientCommand::AddTorrent {
                peer_id,
                peer_port,
                info_hash,
                metrics,
                response,
            } => response.send(self.add_torrent(peer_id, peer_port, info_hash, metrics)),
            TrackerClientCommand::RemoveTorrent { info_hash } => {
                self.remove_torrent(info_hash);
            }
            TrackerClientCommand::DiscoveredTorrentPeers {
                info_hash,
                response,
            } => response.send(self.discovered_peers(&info_hash)),
            TrackerClientCommand::AnnounceAll {
                info_hash,
                event,
                response,
            } => response.send(self.announce_all(&info_hash, event).await),
            TrackerClientCommand::Announce {
                handle,
                info_hash,
                event,
                response,
            } => response.send(self.announce(handle, &info_hash, event).await),
            TrackerClientCommand::UpdateAnnouncingState {
                info_hash,
                announcing,
            } => {
                self.update_torrent_announcing_state(info_hash.clone(), announcing);
            }
            TrackerClientCommand::GetAnnouncingState {
                info_hash,
                response,
            } => {
                response.send(
                    self.torrents
                        .get(&info_hash)
                        .map(|torrent| torrent.is_announcing)
                        .unwrap_or_default(),
                );
            }
            TrackerClientCommand::Scrape {
                handle,
                info_hash,
                response,
            } => match self.find_tracker(handle) {
                None => response.send(Err(TrackerError::InvalidHandle(handle))),
                Some(tracker) => response.send(tracker.scrape(&[info_hash]).await),
            },
        }
    }

    fn update_torrent_announcing_state(&mut self, info_hash: InfoHash, is_announcing: bool) {
        if let Some(torrent) = self.torrents.get_mut(&info_hash) {
            torrent.is_announcing = is_announcing;
        }
    }

    fn find_tracker(&self, handle: TrackerHandle) -> Option<&Tracker> {
        self.trackers
            .iter()
            .find(|tracker| tracker.handle() == handle)
    }

    /// Returns the discovered peers for the given torrent.
    fn discovered_peers(&self, info_hash: &InfoHash) -> Option<HashSet<SocketAddr>> {
        self.torrents
            .get(info_hash)
            .map(|torrent| torrent.peers.clone())
    }

    /// Returns all active trackers of the client.
    async fn active_trackers(&self) -> impl Iterator<Item = &Tracker> {
        future::join_all(
            self.trackers
                .iter()
                .map(|tracker| async move { (tracker, tracker.state().await) }),
        )
        .await
        .into_iter()
        .filter(|(_, state)| *state == TrackerState::Active)
        .map(|(tracker, _)| tracker)
    }

    fn add_torrent(
        &mut self,
        peer_id: PeerId,
        peer_port: u16,
        info_hash: InfoHash,
        metrics: Metrics,
    ) -> Result<()> {
        // early exit if the port is invalid
        if peer_port == 0 {
            debug!(
                "Tracker client {} failed to track torrent {}, invalid port",
                self, info_hash
            );
            return Err(TrackerError::InvalidPort(peer_port));
        }

        // check if the given info hash if unique within the registered torrents
        // if not, we ignore this registration
        if !self.torrents.contains_key(&info_hash) {
            let info_hash_txt = info_hash.to_string();
            self.torrents.insert(
                info_hash.clone(),
                TrackerTorrent {
                    peer_id,
                    peer_port,
                    metrics,
                    peers: Default::default(),
                    is_announcing: true,
                },
            );
            debug!("Tracker client {} added torrent {}", self, info_hash_txt);
        }

        Ok(())
    }

    fn remove_torrent(&mut self, info_hash: InfoHash) {
        if let Some(_) = self.torrents.remove(&info_hash) {
            debug!("Tracker client {} removed torrent {}", self, info_hash);
        }
    }

    /// Checks if the given URL is already registered/known.
    fn is_tracker_url_known(&self, url: &Url) -> bool {
        self.trackers.iter().any(|tracker| tracker.url() == url)
    }

    /// Tries to create a new tracker for the given entry.
    ///
    /// The URL of the entry must not already be known. On success,
    /// the created tracker is added to the client.
    ///
    /// # Returns
    ///
    /// Returns the created tracker handle on success, otherwise a [`TrackerError`].
    async fn create_tracker_from_entry(&mut self, entry: TrackerEntry) -> Result<TrackerHandle> {
        // if the url is already known, reject the request to create the tracker
        let url_already_exists = self.is_tracker_url_known(&entry.url);
        if url_already_exists {
            return Err(TrackerError::DuplicateUrl(entry.url));
        }

        match Tracker::builder()
            .url(entry.url)
            .tier(entry.tier)
            .timeout(self.connection_timeout.clone())
            .build()
            .await
        {
            Ok(tracker) => self.add_tracker(tracker),
            Err(e) => {
                debug!("Failed to create new tracker, {}", e);
                Err(e)
            }
        }
    }

    /// Adds the given tracker to the tracker's pool.
    ///
    /// # Returns
    ///
    /// Returns a unique tracker handle for the added tracker.
    fn add_tracker(&mut self, tracker: Tracker) -> Result<TrackerHandle> {
        let handle = tracker.handle();
        let tracker_info = tracker.to_string();

        self.trackers.push(tracker);
        debug!("Tracker {} has been added to {}", tracker_info, self);
        self.callbacks
            .invoke(TrackerClientEvent::TrackerAdded(handle));
        Ok(handle)
    }

    async fn announce(
        &mut self,
        handle: TrackerHandle,
        info_hash: &InfoHash,
        event: AnnounceEvent,
    ) -> Result<AnnouncementResult> {
        let tracker = self
            .trackers
            .iter()
            .find(|e| e.handle() == handle)
            .ok_or(TrackerError::InvalidHandle(handle))?;
        let torrent = self
            .torrents
            .get_mut(info_hash)
            .ok_or(TrackerError::InfoHashNotFound(info_hash.clone()))?;

        let result = Self::announce_tracker(
            tracker,
            info_hash,
            event,
            torrent.peer_id,
            torrent.peer_port,
            torrent.metrics.completed_size.total(),
            torrent.metrics.bytes_remaining(),
        )
        .await
        .map(|e| AnnouncementResult {
            total_leechers: e.leechers,
            total_seeders: e.seeders,
            peers: e.peers,
        })?;
        Self::add_peers(info_hash, result.peers.as_slice(), torrent, &self.callbacks);

        Ok(result)
    }

    async fn announce_all(
        &mut self,
        info_hash: &InfoHash,
        event: AnnounceEvent,
    ) -> AnnouncementResult {
        // early exit if the torrent is not tracked
        if !self.torrents.contains_key(info_hash) {
            warn!(
                "Tracker {} failed to announce event, torrent {} info hash not found",
                self, info_hash
            );
            return AnnouncementResult::default();
        }

        let mut result = AnnouncementResult::default();
        let mut total_peers = 0;

        // start announcing the given hash to each tracker simultaneously
        let futures: Vec<_> = {
            let torrent = &self.torrents[info_hash];
            self.active_trackers()
                .await
                .map(|tracker| {
                    Self::announce_tracker(
                        tracker,
                        info_hash,
                        event,
                        torrent.peer_id,
                        torrent.peer_port,
                        torrent.metrics.completed_size.total(),
                        torrent.metrics.bytes_remaining(),
                    )
                })
                .collect()
        };

        // wait for all responses to complete
        let responses = future::join_all(futures).await;
        let torrent = match self.torrents.get_mut(info_hash) {
            Some(torrent) => torrent,
            None => unreachable!(),
        };
        for response in responses {
            match response {
                Ok(response) => {
                    result.total_leechers += response.leechers;
                    result.total_seeders += response.seeders;
                    result.peers.extend_from_slice(response.peers.as_slice());

                    total_peers += Self::add_peers(
                        info_hash,
                        response.peers.as_slice(),
                        torrent,
                        &self.callbacks,
                    );
                }
                Err(e) => debug!(
                    "Failed to announce info hash {:?} to tracker, {}",
                    info_hash, e
                ),
            }
        }

        info!(
            "Discovered a total of {} peers for {}",
            total_peers, info_hash
        );
        result
    }

    async fn announce_all_stopped(&mut self) {
        let mut futures = Vec::with_capacity(self.trackers.len() * self.torrents.len());

        for (info_hash, torrent) in self.torrents.iter() {
            futures.extend(self.trackers.iter().map(|tracker| {
                Self::announce_tracker(
                    tracker,
                    &info_hash,
                    AnnounceEvent::Stopped,
                    torrent.peer_id,
                    torrent.peer_port,
                    torrent.metrics.completed_size.total(),
                    torrent.metrics.bytes_remaining(),
                )
            }));
        }

        for response in future::join_all(futures).await {
            if let Err(e) = response {
                debug!("Failed announce stop event to tracker, {}", e);
            }
        }
    }

    /// Performs automatic announcements to all trackers periodically.
    ///
    /// This method is called by the periodic task loop and respects the
    /// per-tracker announcement interval and last announcement timestamp.
    async fn do_automatic_announcements(&self) {
        let now = Instant::now();

        for (info_hash, torrent) in self
            .torrents
            .iter()
            .filter(|(_, torrent)| torrent.is_announcing)
        {
            for tracker in self.active_trackers().await {
                let interval = tracker.announcement_interval().await;
                let last_announcement = tracker.last_announcement().await;
                let delta = now - last_announcement;

                if delta.as_secs() >= interval {
                    if let Err(err) = Self::announce_tracker(
                        tracker,
                        &info_hash,
                        AnnounceEvent::Started,
                        torrent.peer_id,
                        torrent.peer_port,
                        torrent.metrics.completed_size.total(),
                        torrent.metrics.bytes_remaining(),
                    )
                    .await
                    {
                        debug!("Tracker {} failed to make announcement, {}", tracker, err);
                    }
                }
            }
        }
    }

    /// Updates metrics and emits a [`TrackerClientEvent::Stats`] event.
    ///
    /// This aggregates tracker metrics into the client metrics and ticks
    /// both tracker and client metric time windows.
    async fn update_stats(&self) {
        for tracker in self.trackers.iter() {
            let tracker_metrics = tracker.metrics();

            self.metrics.bytes_in.inc_by(tracker_metrics.bytes_in.get());
            self.metrics
                .bytes_out
                .inc_by(tracker_metrics.bytes_out.get());

            tracker.tick(STATS_INTERVAL);
        }

        self.callbacks
            .invoke(TrackerClientEvent::Stats(self.metrics.snapshot()));
        self.metrics.tick(STATS_INTERVAL);
    }

    /// Closes all tracker connections managed by this client.
    async fn close(&mut self) {
        for tracker in self.trackers.drain(..) {
            tracker.close().await;
        }
    }

    /// Adds one or more discovered peers to the tracker client.
    ///
    /// This will only add unique peer addresses and filters out any duplicate
    /// addresses that have already been discovered for the torrent.
    ///
    /// # Returns
    ///
    /// The number of newly added unique peer addresses.
    fn add_peers(
        info_hash: &InfoHash,
        peers: &[SocketAddr],
        torrent: &mut TrackerTorrent,
        callbacks: &MultiThreadedCallback<TrackerClientEvent>,
    ) -> usize {
        trace!("Discovered a total of {} peers, {:?}", peers.len(), peers);
        let mut unique_new_peer_addrs = Vec::new();

        for peer in peers.into_iter() {
            if !torrent.peers.contains(peer) {
                torrent.peers.insert(peer.clone());
                unique_new_peer_addrs.push(peer.clone());
            }
        }

        debug!(
            "Discovered a total of {} new peers",
            unique_new_peer_addrs.len()
        );
        let total_peers = unique_new_peer_addrs.len();
        if total_peers > 0 {
            callbacks.invoke(TrackerClientEvent::PeersDiscovered(
                info_hash.clone(),
                unique_new_peer_addrs,
            ));
        }

        total_peers
    }

    async fn announce_tracker(
        tracker: &Tracker,
        info_hash: &InfoHash,
        event: AnnounceEvent,
        peer_id: PeerId,
        peer_port: u16,
        bytes_completed: u64,
        bytes_remaining: u64,
    ) -> Result<AnnouncementResponse> {
        trace!("Announcing event {} to tracker {}", event, tracker);
        let announce = Announcement {
            info_hash: info_hash.clone(),
            peer_id,
            peer_port,
            event,
            bytes_completed,
            bytes_remaining,
        };

        match tracker.announce(announce).await {
            Ok(response) => {
                debug!(
                    "Tracker {} announcement found {} peers",
                    tracker,
                    response.peers.len()
                );
                Ok(response)
            }
            Err(e) => {
                warn!(
                    "Announcement of event {} failed for tracker {}, {:?}",
                    event, tracker, e
                );
                Err(e)
            }
        }
    }
}

/// A torrent peer registered with the tracker.
#[derive(Debug, PartialEq)]
struct TrackerTorrent {
    /// The unique peer id of the torrent
    peer_id: PeerId,
    /// The port the torrent is listening on to accept incoming connections
    peer_port: u16,
    /// The discovered peers for this torrent by the tracker
    peers: HashSet<SocketAddr>,
    /// A reference to the torrent metrics
    metrics: Metrics,
    /// Indicates if the torrent peer should be announced to the trackers.
    is_announcing: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tracker::udp::UdpServer;
    use crate::tracker::TrackerServer;
    use std::net::Ipv4Addr;
    use std::str::FromStr;
    use tokio::sync::mpsc::unbounded_channel;
    use url::Url;

    mod add_torrent {
        use super::*;

        #[tokio::test]
        async fn test_valid_torrent() {
            init_logger!();
            let peer_id = PeerId::new();
            let peer_port = 6881;
            let info_hash =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let manager = TrackerClient::new(Duration::from_secs(1));

            // add a new torrent
            let result = manager
                .add_torrent(peer_id, peer_port, info_hash.clone(), Metrics::new())
                .await;
            assert_eq!(Ok(()), result);

            // verify that the torrent was added
            let result = manager.torrents_len().await;
            assert_eq!(1, result, "expected the torrent to have been registered");

            {
                let _ = manager
                    .add_torrent(PeerId::new(), peer_port, info_hash, Metrics::new())
                    .await;
                let result = manager.torrents_len().await;
                assert_eq!(
                    1, result,
                    "expected the torrent to not have been added as duplicate"
                );
            }
        }

        #[tokio::test]
        async fn test_invalid_port() {
            init_logger!();
            let info_hash =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let manager = TrackerClient::new(Duration::from_secs(1));

            let result = manager
                .add_torrent(PeerId::new(), 0, info_hash, Metrics::new())
                .await;

            assert_eq!(Err(TrackerError::InvalidPort(0)), result);
        }
    }

    mod get {
        use super::*;

        #[tokio::test]
        async fn test_get_by_handle() {
            init_logger!();
            let server = TrackerServer::new().await.unwrap();
            let client = TrackerClient::new(Duration::from_secs(1));

            // add the tracker to the client
            let handle = client
                .add_tracker_entry(TrackerEntry {
                    tier: 0,
                    url: server.url().clone(),
                })
                .await
                .expect("expected the tracker to have been added");

            // retrieve the tracker
            let result = client
                .get(&handle)
                .await
                .expect("expected the tracker to have been found");
            assert_eq!(handle, result.handle(), "expected the handle to match");
        }

        #[tokio::test]
        async fn test_get_by_url() {
            init_logger!();
            let server = TrackerServer::new().await.unwrap();
            let client = TrackerClient::new(Duration::from_secs(1));

            // add the tracker to the client
            let url = server.url();
            let result = client
                .add_tracker_entry(TrackerEntry {
                    tier: 0,
                    url: url.clone(),
                })
                .await;
            assert!(result.is_ok(), "expected Ok(), but got {:?}", result);

            // retrieve the tracker
            let result = client
                .get_by_url(&url)
                .await
                .expect("expected the tracker to have been found");
            assert_eq!(url, result.url(), "expected the url to match");
        }
    }

    #[tokio::test]
    async fn test_remove_torrent() {
        init_logger!();
        let info_hash =
            InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
        let manager = TrackerClient::new(Duration::from_secs(1));

        // try to remove a non-existing torrent
        manager.remove_torrent(&info_hash).await;

        {
            manager
                .add_torrent(PeerId::new(), 6881, info_hash.clone(), Metrics::new())
                .await
                .unwrap();
            let result = manager.torrents_len().await;
            assert_eq!(1, result, "expected the torrent to have been registered");
        }

        {
            manager.remove_torrent(&info_hash).await;
            assert_timeout!(
                Duration::from_millis(500),
                manager.torrents_len().await == 0,
                "expected the torrent to have been removed"
            );
        }
    }

    #[tokio::test]
    async fn test_tracker_manager_announce_all() {
        init_logger!();
        let peer_id = PeerId::new();
        let info_hash =
            InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
        let server = TrackerServer::new().await.unwrap();
        let manager = TrackerClient::new(Duration::from_secs(3));

        // add a dummy peer to the server
        server
            .add_peer(
                info_hash.clone(),
                SocketAddr::from((Ipv4Addr::LOCALHOST, 6881)),
                PeerId::new(),
                6881,
                false,
            )
            .await;

        // add the tracker to the tracker client
        let result = manager
            .add_torrent(peer_id, 6881, info_hash.clone(), Metrics::new())
            .await;
        assert!(
            result.is_ok(),
            "expected Ok() for add_torrent, but got {:?}",
            result
        );

        // add the tracker entry to the client
        let entry = TrackerEntry {
            tier: 0,
            url: server.url().clone(),
        };
        let result = manager.add_tracker_entry(entry).await;
        assert!(
            result.is_ok(),
            "expected Ok() for add_tracker_entry, but got {:?}",
            result
        );

        let result = manager
            .announce_all(&info_hash, AnnounceEvent::Started)
            .await;
        assert_eq!(1, result.total_leechers, "expected 1 leecher");
        assert_eq!(0, result.total_seeders, "expected 0 seeders");
        assert_eq!(
            1,
            result.peers.len(),
            "expected the peer to have been found"
        );
    }

    #[tokio::test]
    async fn test_tracker_manager_scrape() {
        init_logger!();
        let peer_id = PeerId::new();
        let info_hash =
            InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
        let server =
            TrackerServer::with_listeners(vec![Box::new(UdpServer::with_port(0).await.unwrap())])
                .unwrap();
        let manager = TrackerClient::new(Duration::from_secs(3));

        // register the torrent
        manager
            .add_torrent(peer_id, 6881, info_hash.clone(), Metrics::new())
            .await
            .unwrap();

        // add the tracker server entry
        let entry = TrackerEntry {
            tier: 0,
            url: server.url().clone(),
        };
        manager.add_tracker_entry(entry).await.unwrap();
        let result = manager
            .scrape(&info_hash)
            .await
            .expect("expected the scrape to succeed");

        assert_eq!(
            1,
            result.files.len(),
            "expected the scrape result to match the torrent"
        );
    }

    #[tokio::test]
    async fn test_discovered_peers() {
        init_logger!();
        let info_hash =
            InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 6882));
        let server = TrackerServer::new().await.unwrap();
        let manager = TrackerClient::new(Duration::from_secs(1));

        // add a new tracker
        let result = manager
            .add_tracker_entry(TrackerEntry {
                tier: 0,
                url: server.url().clone(),
            })
            .await;
        assert!(result.is_ok(), "expected Ok(), but got {:?}", result);

        // start tracking the torrent
        let result = manager
            .add_torrent(PeerId::new(), 10999, info_hash.clone(), Metrics::new())
            .await;
        assert!(result.is_ok(), "expected Ok(), but got {:?}", result);

        // add the peer to the server
        server
            .add_peer(
                info_hash.clone(),
                peer_addr.clone(),
                PeerId::new(), // this needs to be different torrent Peer ID added to the manager
                peer_addr.port(),
                false,
            )
            .await;

        // announce the torrent to the trackers
        let result = manager
            .announce_all(&info_hash, AnnounceEvent::Started)
            .await;
        assert_eq!(
            1, result.total_leechers,
            "expected 1 leecher to have been discovered during announcement"
        );

        // request the cached discovered peers
        let result = manager
            .discovered_peers(&info_hash)
            .await
            .expect("expected the torrent to have been tracked");
        assert_eq!(1, result.len(), "expected 1 peer to have been discovered");
        assert_eq!(peer_addr, result[0], "expected the peer to be discovered");
    }

    #[tokio::test]
    async fn test_add_callback() {
        init_logger!();
        let (tx, mut rx) = unbounded_channel();
        let server =
            TrackerServer::with_listeners(vec![Box::new(UdpServer::with_port(0).await.unwrap())])
                .unwrap();
        let entry = TrackerEntry {
            tier: 0,
            url: server.url().clone(),
        };
        let manager = TrackerClient::new(Duration::from_secs(1));

        let mut receiver = manager.subscribe();
        tokio::spawn(async move {
            while let Ok(event) = receiver.recv().await {
                if let TrackerClientEvent::TrackerAdded(_) = &*event {
                    tx.send((*event).clone()).unwrap();
                    break;
                }
            }
        });

        manager
            .add_tracker_entry(entry)
            .await
            .expect("expected the tracker to have been created");

        let result = timeout!(
            Duration::from_millis(750),
            rx.recv(),
            "expected to receive an event"
        )
        .unwrap();
        if let TrackerClientEvent::TrackerAdded(handle) = result {
            let result = manager
                .trackers()
                .await
                .into_iter()
                .any(|e| e.handle() == handle);
            assert!(
                result,
                "expected tracker {} to have been present within the managed",
                handle
            );
        } else {
            assert!(
                false,
                "expected TrackerManagerEvent::TrackerAdded, got {:?} instead",
                result
            );
        }
    }

    #[tokio::test]
    async fn test_start_stop_announcing() {
        init_logger!();
        let peer_id = PeerId::new();
        let peer_port = 6881;
        let info_hash =
            InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
        let manager = TrackerClient::new(Duration::from_secs(1));

        let result = manager
            .add_torrent(peer_id, peer_port, info_hash.clone(), Metrics::new())
            .await;
        assert_eq!(Ok(()), result, "expected the torrent to have been added");

        manager.stop_announcing(&info_hash).await;
        assert_timeout!(
            Duration::from_millis(500),
            manager.is_announcing(&info_hash).await == false,
            "expected the torrent to be no longer announcing"
        );

        manager.start_announcing(&info_hash).await;
        assert_timeout!(
            Duration::from_millis(500),
            manager.is_announcing(&info_hash).await == true,
            "expected the torrent to be no longer announcing"
        );
    }

    #[tokio::test]
    async fn test_drop() {
        init_logger!();
        let url = Url::parse("udp://tracker.opentrackr.org:1337").unwrap();
        let manager = TrackerClient::new(Duration::from_secs(1));

        manager
            .add_tracker_async(TrackerEntry { tier: 0, url })
            .await;
        drop(manager);
    }
}
