use crate::tracker::{
    Announcement, AnnouncementResponse, ConnectionMetrics, HttpClient, Result, ScrapeResult,
    UdpConnection,
};
use crate::InfoHash;
use async_trait::async_trait;
use std::fmt::Debug;

/// Factory for creating tracker connections.
pub type ConnectionFactory = dyn Fn() -> Connection + Send + Sync;

/// The underlying connection of a tracker.
#[derive(Debug)]
pub enum Connection {
    Http(HttpClient),
    Udp(UdpConnection),
    Other(Box<dyn Extension>),
}

impl Connection {
    /// Returns the announcement response for the torrent hash from the tracker.
    /// This will send the known peer info to the tracker with the type of announcement.
    ///
    /// # Arguments
    ///
    /// * `info_hash` - The `InfoHash` of the torrent to announce.
    /// * `event` - The announcement event type to announce.
    pub async fn announce(&self, announcement: Announcement) -> Result<AnnouncementResponse> {
        match self {
            Connection::Http(http) => http.announce(announcement).await,
            Connection::Udp(udp) => udp.announce(announcement).await,
            Connection::Other(other) => other.announce(announcement).await,
        }
    }

    /// Returns the scrape result for one or more info hashes from the tracker.
    ///
    /// # Arguments
    ///
    /// * `hashes` - The info hashes to retrieve the metrics from.
    pub async fn scrape(&self, hashes: &[InfoHash]) -> Result<ScrapeResult> {
        match self {
            Connection::Http(http) => http.scrape(hashes).await,
            Connection::Udp(udp) => udp.scrape(hashes).await,
            Connection::Other(other) => other.scrape(hashes).await,
        }
    }

    /// Returns the connection metrics.
    pub fn metrics(&self) -> &ConnectionMetrics {
        match self {
            Connection::Http(http) => http.metrics(),
            Connection::Udp(udp) => udp.metrics(),
            Connection::Other(other) => other.metrics(),
        }
    }

    /// Close the tracker connection and cancel any pending tasks.
    ///
    /// This method should gracefully shut down the connection to the tracker and cancel any ongoing operations.
    pub async fn close(&self) {
        match self {
            Connection::Http(http) => http.close(),
            Connection::Udp(udp) => udp.close(),
            Connection::Other(other) => other.close().await,
        }
    }
}

impl From<HttpClient> for Connection {
    fn from(value: HttpClient) -> Self {
        Self::Http(value)
    }
}

impl From<UdpConnection> for Connection {
    fn from(value: UdpConnection) -> Self {
        Self::Udp(value)
    }
}

impl<E> From<E> for Connection
where
    E: Extension + 'static,
{
    fn from(value: E) -> Self {
        Self::Other(Box::new(value))
    }
}

/// Extension trait for tracker connections.
#[async_trait]
pub trait Extension: Debug + Send + Sync {
    /// Announce the given torrent hash to the tracker.
    /// This will send the known peer info to the tracker with the type of announcement.
    ///
    /// # Arguments
    ///
    /// * `info_hash` - The `InfoHash` of the torrent to announce.
    /// * `event` - The announcement event type to announce.
    ///
    /// # Returns
    ///
    /// Returns the tracker announcement response for the given announcement.
    async fn announce(&self, announcement: Announcement) -> Result<AnnouncementResponse>;

    /// Scrape the tracker for metrics for one or more info hashes.
    ///
    /// # Arguments
    ///
    /// * `hashes` - The info hashes to retrieve the metrics from.
    ///
    /// # Returns
    ///
    /// Returns the scrape result from the tracker for the given hashes.
    async fn scrape(&self, hashes: &[InfoHash]) -> Result<ScrapeResult>;

    /// Returns the connection metrics.
    fn metrics(&self) -> &ConnectionMetrics;

    /// Close the tracker connection and cancel any pending tasks.
    ///
    /// This method should gracefully shut down the connection to the tracker and cancel any ongoing operations.
    async fn close(&self);
}

#[cfg(test)]
mod tests {
    use super::*;

    mod extension {
        use super::*;
        use crate::peer::PeerId;
        use crate::tracker::AnnounceEvent;
        use mockall::mock;
        use std::str::FromStr;
        use tokio::sync::oneshot;

        mock! {
            #[derive(Debug)]
            pub Extension {}

            #[async_trait]
            impl Extension for Extension {
                async fn announce(&self, announcement: Announcement) -> Result<AnnouncementResponse>;
                async fn scrape(&self, hashes: &[InfoHash]) -> Result<ScrapeResult>;
                fn metrics(&self) -> &ConnectionMetrics;
                async fn close(&self);
            }
        }

        #[tokio::test]
        async fn test_announce() {
            let info_hash = InfoHash::from_str("A1DFEFEC1A9DD7FA8A041EBEEEA271DB55126D2F").unwrap();
            let (tx, rx) = oneshot::channel();
            let mut extension = MockExtension::default();
            extension
                .expect_announce()
                .times(1)
                .return_once(|announcement: Announcement| {
                    let _ = tx.send(announcement);
                    Ok(AnnouncementResponse {
                        interval_seconds: 120,
                        external_ip: None,
                        leechers: 12,
                        seeders: 43,
                        peers: vec![],
                    })
                });
            let expected_announcement = Announcement {
                info_hash,
                peer_id: PeerId::new(),
                peer_port: 6881,
                event: AnnounceEvent::Started,
                bytes_completed: 0,
                bytes_remaining: 0,
            };
            let connection: Connection = extension.into();

            // sent the announcement to the connection
            let result = connection
                .announce(expected_announcement.clone())
                .await
                .expect("expected the announce to succeed");
            assert_eq!(result.interval_seconds, 120);
            assert_eq!(result.leechers, 12);
            assert_eq!(result.seeders, 43);

            // verify the received announcement in the extension
            let announcement = timeout!(Duration::from_millis(100), rx).unwrap();
            assert_eq!(
                expected_announcement, announcement,
                "expected the announcement to match"
            );
        }

        #[test]
        fn test_metrics() {
            let mut extension = MockExtension::default();
            extension.expect_metrics().times(1).return_const({
                let metrics = ConnectionMetrics::default();
                metrics.bytes_in.inc_by(64);
                metrics
            });
            let connection: Connection = extension.into();

            let result = connection.metrics();
            assert_eq!(result.bytes_in.get(), 64);
        }

        #[tokio::test]
        async fn test_close() {
            let mut extension = MockExtension::default();
            extension.expect_close().times(1).return_const(());
            let connection: Connection = extension.into();

            connection.close().await;
        }
    }
}
