#[cfg(feature = "dht")]
use crate::dht::DhtTracker;
#[cfg(feature = "lsd")]
use crate::lsd::LocalServiceDiscovery;
use crate::tracker::{
    AnnounceEvent, AnnouncementResult, ScrapeFileMetrics, ScrapeResult, TrackerClient,
};
use crate::Result;
use crate::{InfoHash, TorrentError};
use std::time::Duration;

/// Allows discovering peers in a swarm.
#[derive(Debug, Clone)]
pub enum TorrentTracker {
    #[cfg(feature = "dht")]
    Dht(DhtTracker),
    #[cfg(feature = "lsd")]
    Lsd(LocalServiceDiscovery),
    TrackerClient(TrackerClient),
}

impl TorrentTracker {
    /// Announce the given event for the info hash to the tracker.
    pub async fn announce(
        &self,
        info_hash: &InfoHash,
        port: u16,
        event: AnnounceEvent,
    ) -> AnnouncementResult {
        match self {
            TorrentTracker::Dht(dht) => {
                match event {
                    AnnounceEvent::Started => {
                        let _ = dht.announce_peer(info_hash, port, false).await;
                    }
                    AnnounceEvent::Completed => {
                        let _ = dht.announce_peer(info_hash, port, true).await;
                    }
                    _ => {}
                }

                AnnouncementResult::default()
            }
            TorrentTracker::Lsd(lsd) => {
                if event == AnnounceEvent::Started {
                    lsd.announce(info_hash, port).await;
                }

                AnnouncementResult::default()
            }
            TorrentTracker::TrackerClient(tracker) => tracker.announce_all(info_hash, event).await,
        }
    }

    /// Scrape the given info hash from the tracker.
    pub async fn scrape(&self, info_hash: &InfoHash) -> Result<ScrapeResult> {
        match self {
            TorrentTracker::Dht(dht) => {
                let scrape = dht.scrape_peers(info_hash, Duration::from_secs(6)).await?;
                Ok(ScrapeResult {
                    files: vec![(
                        info_hash.clone(),
                        ScrapeFileMetrics {
                            complete: scrape.seeders as u32,
                            downloaded: scrape.seeders as u32,
                            incomplete: scrape.downloaders as u32,
                        },
                    )]
                    .into_iter()
                    .collect(),
                })
            }
            TorrentTracker::Lsd(_) => {
                // no-op
                Ok(ScrapeResult::default())
            }
            TorrentTracker::TrackerClient(tracker) => {
                tracker.scrape(info_hash).await.map_err(TorrentError::from)
            }
        }
    }

    /// Closes the torrent tracker, resulting in termination of its operations.
    pub fn close(&self) {
        match self {
            TorrentTracker::Dht(dht) => dht.close(),
            TorrentTracker::Lsd(lsd) => lsd.close(),
            TorrentTracker::TrackerClient(tracker) => tracker.close(),
        }
    }
}

#[cfg(feature = "dht")]
impl From<DhtTracker> for TorrentTracker {
    fn from(dht: DhtTracker) -> Self {
        Self::Dht(dht)
    }
}

#[cfg(feature = "lsd")]
impl From<LocalServiceDiscovery> for TorrentTracker {
    fn from(lsd: LocalServiceDiscovery) -> Self {
        Self::Lsd(lsd)
    }
}

impl From<TrackerClient> for TorrentTracker {
    fn from(tracker: TrackerClient) -> Self {
        Self::TrackerClient(tracker)
    }
}
