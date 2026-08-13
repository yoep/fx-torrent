use crate::metrics::Metric;
use crate::peer::PeerId;
use crate::tracker::HttpClient;
use crate::tracker::UdpConnection;
use crate::tracker::{Connection, Result, TrackerError, TrackerMetrics};
use crate::InfoHash;
use derive_more::Display;
use fx_handle::Handle;
use itertools::Itertools;
use log::{debug, trace};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::net::{IpAddr, SocketAddr};
use std::ops::Sub;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::lookup_host;
use tokio::sync::{Mutex, RwLock};
use url::Url;

/// The timeout for each url connection attempt.
const URL_CONNECTION_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_ANNOUNCEMENT_INTERVAL_SECONDS: u64 = 120;
const DISABLE_TRACKER_AFTER_FAILURES: usize = 6;

/// The tracker identifier handle
pub type TrackerHandle = Handle;

/// Kinds of tracker announce events.
///
/// For HTTP trackers, this usually maps to the `&event=` query-string
/// parameter in the announce URL.
#[repr(u8)]
#[derive(Debug, Display, Copy, Clone, PartialEq)]
#[display("{}", self.as_str())]
pub enum AnnounceEvent {
    None = 0,
    Completed = 1,
    Started = 2,
    Stopped = 3,
    Paused = 4,
}

impl AnnounceEvent {
    /// Returns the string slice value of the announce event.
    pub fn as_str(&self) -> &str {
        match self {
            AnnounceEvent::None => "none",
            AnnounceEvent::Completed => "completed",
            AnnounceEvent::Started => "started",
            AnnounceEvent::Stopped => "stopped",
            AnnounceEvent::Paused => "paused",
        }
    }
}

impl FromStr for AnnounceEvent {
    type Err = TrackerError;

    fn from_str(value: &str) -> Result<Self> {
        match value.to_lowercase().trim() {
            "none" => Ok(AnnounceEvent::None),
            "completed" => Ok(AnnounceEvent::Completed),
            "started" => Ok(AnnounceEvent::Started),
            "stopped" => Ok(AnnounceEvent::Stopped),
            "paused" => Ok(AnnounceEvent::Paused),
            _ => Err(TrackerError::UnsupportedEvent(value.to_string())),
        }
    }
}

impl TryFrom<u8> for AnnounceEvent {
    type Error = TrackerError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(AnnounceEvent::None),
            1 => Ok(AnnounceEvent::Completed),
            2 => Ok(AnnounceEvent::Started),
            3 => Ok(AnnounceEvent::Stopped),
            4 => Ok(AnnounceEvent::Paused),
            _ => Err(TrackerError::UnsupportedEvent(value.to_string())),
        }
    }
}

/// Announcement payload sent to a tracker.
///
/// This represents the most recent torrent state that should be shared with
/// the tracker when making an announce request.
#[derive(Debug, Clone, PartialEq)]
pub struct Announcement {
    /// The info hash of the torrent
    pub info_hash: InfoHash,
    /// The peer ID of the torrent
    pub peer_id: PeerId,
    /// The port of the torrent
    pub peer_port: u16,
    /// The tracker announcement event
    pub event: AnnounceEvent,
    /// The number of piece bytes completed by the torrent
    pub bytes_completed: u64,
    /// The number of piece bytes remaining to be downloaded by the torrent
    pub bytes_remaining: u64,
}

/// Represents the response from a tracker announcement.
///
/// This struct contains the information returned by a tracker when announcing a peer.
/// It includes the interval at which the peer should re-announce, the number of leechers and seeders,
/// and a list of peer addresses.
#[derive(Debug, Clone)]
pub struct AnnouncementResponse {
    /// The interval (in seconds) at which the peer should re-announce itself to the tracker.
    pub interval_seconds: u64,
    /// The external ip address of the torrent detected by the tracker (see BEP24).
    pub external_ip: Option<IpAddr>,
    /// The number of leechers currently downloading the torrent.
    pub leechers: u64,
    /// The number of seeders currently sharing the torrent.
    pub seeders: u64,
    /// A list of addresses (as `SocketAddr`) of peers to connect to.
    pub peers: Vec<SocketAddr>,
}

/// The metrics result of a tracker scrape operation.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScrapeResult {
    /// The file metrics from the scrape result
    pub files: HashMap<InfoHash, ScrapeFileMetrics>,
}

/// The metrics of a specific torrent file.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScrapeFileMetrics {
    /// The number of active peers that have completed downloading.
    pub complete: u32,
    /// The number of active peers that have not completed downloading.
    pub incomplete: u32,
    /// The number of peers that have ever completed downloading.
    pub downloaded: u32,
}

/// The state of a tracker.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum TrackerState {
    /// Tracker is active and can be used for sending queries
    Active,
    /// Tracker is bad and disabled for further
    Disabled,
    /// Tracker connection is closed
    Closed,
}

impl TrackerState {
    /// Calculate the state of a tracker based on the given metrics
    pub fn calculate(failure_count: usize) -> Self {
        if failure_count > DISABLE_TRACKER_AFTER_FAILURES {
            return TrackerState::Disabled;
        }

        TrackerState::Active
    }
}

#[derive(Debug, Display, Clone)]
#[display("{}", inner)]
pub struct Tracker {
    inner: Arc<InnerTracker>,
}

impl Tracker {
    /// Create a new builder instance for creating a [Tracker].
    /// See [TrackerBuilder] for available configurations.
    ///
    /// # Example
    ///
    /// Create a new udp tracker client connection.
    ///
    /// ```rust,no_run
    /// # use fx_torrent::tracker::Result;
    /// # use fx_torrent::tracker::Tracker;
    /// # use url::Url;
    ///
    /// # async fn example() -> Result<Tracker> {
    /// Tracker::builder()
    ///     .url(Url::parse("udp://tracker.opentrackr.org:1337").unwrap())
    ///     .build()
    ///     .await
    /// # }
    /// ```
    pub fn builder() -> TrackerBuilder {
        TrackerBuilder::builder()
    }

    pub async fn new(url: Url, tier: u8, announcement_interval_seconds: u64) -> Result<Self> {
        trace!("Trying to create new tracker for {}", url);
        let handle = TrackerHandle::new();
        let connection = Self::create_connection(handle, &url).await?;

        trace!("Tracker {} connection established to {}", handle, url);
        Ok(Self {
            inner: Arc::new(InnerTracker {
                handle,
                url,
                tier,
                connection,
                announcement_interval_seconds: RwLock::new(announcement_interval_seconds),
                last_announcement: RwLock::new(
                    Instant::now().sub(Duration::from_secs(DEFAULT_ANNOUNCEMENT_INTERVAL_SECONDS)),
                ),
                state: Mutex::new(TrackerState::Active),
                metrics: Default::default(),
            }),
        })
    }

    /// Get the unique handle of the tracker.
    pub fn handle(&self) -> TrackerHandle {
        self.inner.handle
    }

    /// Get the url of the tracker.
    pub fn url(&self) -> &Url {
        &self.inner.url
    }

    /// Get the metrics of the tracker.
    pub fn metrics(&self) -> &TrackerMetrics {
        &self.inner.metrics
    }

    /// Get the current state of the tracker.
    pub async fn state(&self) -> TrackerState {
        *self.inner.state.lock().await
    }

    /// Returns the expected announcement interval in seconds.
    ///
    /// This value is updated based on tracker responses.
    pub async fn announcement_interval(&self) -> u64 {
        self.inner
            .announcement_interval_seconds
            .read()
            .await
            .clone()
    }

    /// Returns the time of the last successful announcement to this tracker.
    pub async fn last_announcement(&self) -> Instant {
        self.inner.last_announcement.read().await.clone()
    }

    /// Announces the given torrent state to this tracker.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use fx_torrent::InfoHash;
    /// # use fx_torrent::peer::PeerId;
    /// # use fx_torrent::tracker::AnnounceEvent;
    /// # use fx_torrent::tracker::Announcement;
    /// # use fx_torrent::tracker::Tracker;
    /// # use std::str::FromStr;
    ///
    /// # async fn example() {
    ///     let tracker = Tracker::new().await.unwrap();
    ///     tracker.announce(Announcement {
    ///         info_hash: InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap(),
    ///         peer_id: PeerId::new(),
    ///         peer_port: 6881,
    ///         event: AnnounceEvent::Started,
    ///         bytes_completed: 0,
    ///         bytes_remaining: 0,
    ///     }).await;
    /// # }
    /// ```
    ///
    /// # Arguments
    ///
    /// * `announce` - The announcement payload describing the torrent and event.
    ///
    /// # Returns
    ///
    /// The announcement response from the tracker.
    pub async fn announce(&self, announce: Announcement) -> Result<AnnouncementResponse> {
        trace!("Tracker {} is announcing {:?}", self, announce);
        match self.inner.connection.announce(announce).await {
            Ok(e) => {
                {
                    let mut mutex = self.inner.last_announcement.write().await;
                    *mutex = Instant::now();
                }
                {
                    let mut mutex = self.inner.announcement_interval_seconds.write().await;
                    *mutex = e.interval_seconds;
                }

                self.confirm().await;
                self.inner.metrics.peers.set(e.peers.len() as u64);
                self.inner.metrics.seeders.set(e.seeders);
                self.inner.metrics.leechers.set(e.leechers);
                Ok(e)
            }
            Err(e) => {
                self.fail().await;
                Err(e)
            }
        }
    }

    /// Scrape the tracker for metrics of the given info hashes.
    ///
    /// # Arguments
    ///
    /// * `hashes` - The info hashes to retrieve the metrics from.
    ///
    /// # Returns
    ///
    /// It returns the scrape metrics result from the tracker for the given info hashes.
    pub async fn scrape(&self, hashes: &[InfoHash]) -> Result<ScrapeResult> {
        trace!("Tracker {} is scraping {:?}", self, hashes);
        match self.inner.connection.scrape(hashes).await {
            Ok(e) => {
                self.confirm().await;
                Ok(e)
            }
            Err(e) => {
                self.fail().await;
                Err(e)
            }
        }
    }

    /// Advances the metric’s internal state by the provided time interval.
    ///
    /// This is typically called periodically (e.g. once per second) to update
    /// rate counters, decay windows, or any other time-dependent metric logic.
    pub(crate) fn tick(&self, interval: Duration) {
        let metrics = self.metrics();
        let connection_metrics = self.inner.connection.metrics();

        metrics.bytes_in.inc_by(connection_metrics.bytes_in.get());
        metrics.bytes_out.inc_by(connection_metrics.bytes_out.get());

        metrics.tick(interval);
        connection_metrics.tick(interval);
    }

    /// Close the tracker connection.
    pub(crate) async fn close(&self) {
        self.inner.connection.close().await;
        *self.inner.state.lock().await = TrackerState::Closed;
    }

    /// Confirm the last query made by the tracker.
    async fn confirm(&self) {
        self.inner.metrics.confirmed.inc()
    }

    /// Increase the failure count of the tracker.
    async fn fail(&self) {
        self.inner.metrics.errors.inc();

        {
            let new_state = TrackerState::calculate(self.inner.metrics.errors.total() as usize);
            let mut state = self.inner.state.lock().await;
            if *state != new_state {
                *state = new_state;
                debug!("Tracker {} state changed to {:?}", self, new_state);
            }
        }
    }

    async fn create_connection(handle: TrackerHandle, url: &Url) -> Result<Connection> {
        trace!(
            "Tracker {} is trying to establish connection with {}",
            handle,
            url
        );
        let scheme = url.scheme();

        match scheme {
            "udp" => {
                let host = url
                    .host_str()
                    .ok_or(TrackerError::InvalidUrl("host is missing".to_string()))?;
                let port = match url.port() {
                    Some(p) => p,
                    None => match url.scheme() {
                        "http" => 80,
                        "https" => 443,
                        _ => {
                            return Err(TrackerError::InvalidUrl("udp port is missing".to_string()))
                        }
                    },
                };
                trace!(
                    "Tracker {} is performing DNS resolution for {}",
                    handle,
                    url
                );
                let addrs = lookup_host(format!("{}:{}", host, port))
                    .await?
                    .collect_vec();

                UdpConnection::new(handle, addrs.as_slice(), URL_CONNECTION_TIMEOUT)
                    .await
                    .map(|c| c.into())
            }
            "http" | "https" => HttpClient::new(handle, url.clone(), URL_CONNECTION_TIMEOUT)
                .await
                .map(|c| c.into()),
            _ => Err(TrackerError::UnsupportedScheme(scheme.to_string())),
        }
    }
}

#[derive(Debug, Default)]
pub struct TrackerBuilder {
    url: Option<Url>,
    tier: Option<u8>,
    default_announcement_interval_seconds: Option<u64>,
}

impl TrackerBuilder {
    pub fn builder() -> Self {
        Self::default()
    }

    /// Set the url of the tracker.
    pub fn url(&mut self, url: Url) -> &mut Self {
        self.url = Some(url);
        self
    }

    /// Set the tier of the tracker.
    pub fn tier(&mut self, tier: u8) -> &mut Self {
        self.tier = Some(tier);
        self
    }

    /// Try to create a new [Tracker] instance from this builder.
    ///
    /// Returns an error when the [TrackerBuilder::url] has not been set.
    pub async fn build(&mut self) -> Result<Tracker> {
        let url = self
            .url
            .take()
            .ok_or(TrackerError::InvalidUrl("url is missing".to_string()))?;
        let tier = self.tier.take().unwrap_or(0);
        let default_announcement_interval_seconds = self
            .default_announcement_interval_seconds
            .take()
            .unwrap_or(DEFAULT_ANNOUNCEMENT_INTERVAL_SECONDS);

        Tracker::new(url, tier, default_announcement_interval_seconds).await
    }
}

#[derive(Debug, Display)]
#[display("[{}] ({}){}", handle, tier, url)]
struct InnerTracker {
    /// The unique tracker handle
    handle: TrackerHandle,
    /// The tracker url
    url: Url,
    /// The tier of the tracker
    tier: u8,
    /// The underlying communication connection
    connection: Connection,
    /// The interval in seconds to do another announcement to the tracker
    announcement_interval_seconds: RwLock<u64>,
    /// The last time an announcement was made by this tracker
    last_announcement: RwLock<Instant>,
    /// The state of the tracker.
    state: Mutex<TrackerState>,
    /// The metric stats of this tracker.
    metrics: TrackerMetrics,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tracker::HttpServer;
    use crate::tracker::TrackerServer;
    use crate::tracker::UdpServer;
    use std::net::Ipv4Addr;

    mod new {
        use super::*;

        #[tokio::test]
        async fn test_new_valid_udp_url() {
            init_logger!();
            let udp_server = UdpServer::with_port(0).await.unwrap();
            let server = TrackerServer::with_listeners(vec![Box::new(udp_server)]).unwrap();
            let url = server.url().clone();

            let result = Tracker::builder()
                .url(url.clone())
                .build()
                .await
                .expect("expected the tracker to be created");

            assert_eq!(&url, result.url(), "expected the tracker url to match");
            assert_eq!(
                TrackerState::Active,
                result.state().await,
                "expected the tracker to be active"
            );
        }

        #[tokio::test]
        async fn test_new_valid_http_url() {
            init_logger!();
            let http_server = HttpServer::with_port(0).await.unwrap();
            let server = TrackerServer::with_listeners(vec![Box::new(http_server)]).unwrap();
            let url =
                Url::parse(format!("http://localhost:{}/announce", server.addr().port()).as_str())
                    .unwrap();

            let result = Tracker::builder()
                .url(url.clone())
                .build()
                .await
                .expect("expected the tracker to be created");

            assert_eq!(&url, result.url(), "expected the tracker url to match");
            assert_eq!(
                TrackerState::Active,
                result.state().await,
                "expected the tracker to be active"
            );
        }

        #[tokio::test]
        async fn test_new_invalid_url() {
            init_logger!();
            let url = Url::parse("udp://tracker.opentrackr.org").unwrap();

            let result = Tracker::builder().url(url).build().await;

            if let Err(e) = result {
                assert_eq!("tracker url is invalid, udp port is missing", e.to_string());
            } else {
                assert!(false, "expected Err(TrackerError), but got {:?}", result);
            }
        }
    }

    #[tokio::test]
    async fn test_tracker_url() {
        init_logger!();
        let server = TrackerServer::new().await.unwrap();
        let tracker = Tracker::builder()
            .url(server.url().clone())
            .build()
            .await
            .expect("expected the tracker to be created");

        let result = tracker.url();

        assert_eq!(server.url(), result, "expected the tracker url to match");
    }

    #[tokio::test]
    async fn test_tracker_announce_udp() {
        init_logger!();
        let info_hash = InfoHash::from_str("EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
        let udp_server = UdpServer::with_port(0).await.unwrap();
        let server = TrackerServer::with_listeners(vec![Box::new(udp_server)]).unwrap();
        let tracker = Tracker::builder()
            .url(server.url().clone())
            .build()
            .await
            .unwrap();

        // add a dummy peer to the tracker server
        server
            .add_peer(
                info_hash.clone(),
                (Ipv4Addr::LOCALHOST, 6881).into(),
                PeerId::new(),
                6881,
                false,
            )
            .await;

        // make an announcement to the tracker server
        let result = tracker
            .announce(Announcement {
                info_hash,
                peer_id: PeerId::new(),
                peer_port: 7788,
                event: AnnounceEvent::Started,
                bytes_completed: 0,
                bytes_remaining: u64::MAX,
            })
            .await
            .expect("expected the announce to succeed");
        assert_eq!(
            1, result.leechers,
            "expected the announce to return 1 leecher"
        );
        assert_eq!(
            1,
            result.peers.len(),
            "expected the announce to return peers"
        );
    }

    #[tokio::test]
    async fn test_tracker_announce_https() {
        init_logger!();
        let info_hash = InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7")
            .expect("expected a valid hash");
        let http_server = HttpServer::with_port(0).await.unwrap();
        let server = TrackerServer::with_listeners(vec![Box::new(http_server)]).unwrap();
        let tracker = Tracker::builder()
            .url(server.url().clone())
            .build()
            .await
            .unwrap();

        // add dummy peers to the tracker server
        server
            .add_peer(
                info_hash.clone(),
                ([127, 0, 0, 1], 8080).into(),
                PeerId::new(),
                6881,
                false,
            )
            .await;
        server
            .add_peer(
                info_hash.clone(),
                ([127, 0, 0, 2], 8080).into(),
                PeerId::new(),
                6882,
                true,
            )
            .await;

        let result = tracker
            .announce(Announcement {
                info_hash,
                peer_id: PeerId::new(),
                peer_port: 6881,
                event: AnnounceEvent::Started,
                bytes_completed: 0,
                bytes_remaining: u64::MAX,
            })
            .await
            .unwrap();

        assert_ne!(
            0,
            result.peers.len(),
            "expected the announce to return peers"
        );
    }

    #[tokio::test]
    async fn test_tracker_scrape_udp() {
        init_logger!();
        let info_hash =
            InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
        let server = TrackerServer::new().await.unwrap();
        let tracker = Tracker::builder()
            .url(server.url().clone())
            .build()
            .await
            .unwrap();

        let result = tracker
            .scrape(&vec![info_hash])
            .await
            .expect("expected a scrape response");

        assert_eq!(
            1,
            result.files.len(),
            "expected the scrape files to match the files from the info hash"
        );
    }

    mod tracker_state_calculate {
        use super::*;

        #[test]
        fn test_active() {
            let failure_count = DISABLE_TRACKER_AFTER_FAILURES.saturating_sub(2);

            assert_eq!(TrackerState::Active, TrackerState::calculate(failure_count));
        }

        #[test]
        fn test_disabled() {
            let failure_count = DISABLE_TRACKER_AFTER_FAILURES + 5;

            assert_eq!(
                TrackerState::Disabled,
                TrackerState::calculate(failure_count)
            );
        }
    }
}
