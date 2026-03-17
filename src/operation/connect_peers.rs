use crate::channel::ChannelSender;
use crate::operation::{TorrentOperation, TorrentOperationResult};
use crate::peer::webseed::HttpPeer;
use crate::peer::{CloseReason, PeerDiscovery};
use crate::torrent::InnerTorrent;
use crate::{Result, TorrentCommand, TorrentContext, TorrentError};
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use log::{debug, trace, warn};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
#[cfg(feature = "tracing")]
use tracing::instrument;
use url::Url;

const BURST_DURATION: Duration = Duration::from_secs(10);

/// Establishes additional peer connections for the torrent.
pub struct TorrentConnectPeersOperation {
    /// Indicates whether webseed connections are enabled for the torrent
    webseeds_enabled: bool,
    /// The webseed urls to connect to
    webseed_urls: Option<VecDeque<Url>>,
    /// The maximum amount of in-flight peer connections being established
    max_in_flight: usize,
    /// Indicates the time since the operation started bursting the initial connections
    bursting_since: Option<Instant>,
    /// The in-flight peer-webseed connections
    in_flight: JoinSet<()>,
}

impl TorrentConnectPeersOperation {
    pub fn new(webseeds_enabled: bool) -> Self {
        Self {
            webseeds_enabled,
            webseed_urls: None,
            max_in_flight: 0,
            bursting_since: None,
            in_flight: Default::default(),
        }
    }

    /// Poll the currently in-flight peer connections for completion.
    fn poll_in_flight(&mut self, context: &TorrentContext) {
        while let Some(res) = self.in_flight.join_next().now_or_never() {
            match res {
                Some(Ok(())) => {}
                Some(Err(e)) => warn!("Torrent {} peer connection failed, {}", context, e),
                None => break,
            }
        }
    }

    /// Create the webseed url cache from the torrent context.
    fn create_webseed_urls(&mut self, torrent: &TorrentContext) {
        if self.webseed_urls.is_some() {
            return;
        }

        let mut urls: VecDeque<_> = torrent
            .metadata()
            .url_list
            .as_ref()
            .map(|list| {
                list.iter()
                    .flat_map(|url| Self::parse_url(torrent, url))
                    .collect()
            })
            .unwrap_or_default();

        let mut http_seeds = torrent
            .metadata()
            .http_seeds
            .as_ref()
            .map(|e| {
                e.iter()
                    .flat_map(|url| Self::parse_url(torrent, url))
                    .collect()
            })
            .unwrap_or_default();

        urls.append(&mut http_seeds);
        self.webseed_urls = Some(urls);
    }

    /// Update the available in-flight permits from the latest torrent config.
    fn update_max_in_flight(&mut self, context: &TorrentContext) {
        let config_peers_in_flight = context.config().peers_in_flight;
        if config_peers_in_flight == self.max_in_flight || self.bursting_since.is_some() {
            return;
        }

        self.max_in_flight = config_peers_in_flight;
    }

    /// Execute a burst of initial peer connections, when needed.
    async fn burst(&mut self, context: &TorrentContext) {
        if let Some(since) = self.bursting_since {
            if since.elapsed() < BURST_DURATION {
                return;
            }
            // burst window has ended
            // restore the config back to its original torrent config value
            self.max_in_flight = context.config().peers_in_flight;
            self.bursting_since = None;
        }

        // only allow bursting when we have no active peer connections
        if context.active_peer_connections().await == 0 {
            trace!("Torrent {} is bursting it's initial connections", context);
            self.max_in_flight = context.config().peers_upper_limit;
            self.bursting_since = Some(Instant::now());
        }
    }

    /// Try to create additional peer connections
    async fn create_additional_peer_connections(
        &mut self,
        mut wanted_connections: usize,
        context: &mut TorrentContext,
        dialers: &[Arc<dyn PeerDiscovery>],
    ) {
        // try to create webseed peers
        if self.webseeds_enabled {
            self.create_webseed_peers(&mut wanted_connections, context);
        }

        let available_permits = self.max_in_flight.saturating_sub(self.in_flight.len());
        let len = wanted_connections.min(available_permits);
        let peer_addrs = context.peer_pool_mut().new_connection_candidates(len);
        if peer_addrs.is_empty() {
            // early exit when not peer addrs are available
            trace!(
                "Torrent {} has no peer addresses available to establish peer connections",
                context
            );
            return;
        }

        debug!(
            "Torrent {} is establishing {} (of wanted {}, remaining {} addresses) peer connections",
            context,
            peer_addrs.len(),
            wanted_connections,
            context.peer_pool().num_connect_candidates()
        );
        for addr in peer_addrs {
            if let Err(e) = self.create_peer_with_dialers(context, addr, dialers) {
                warn!(
                    "Torrent {} failed to create peer connection to {}, {}",
                    context, addr, e
                );
            }
        }
    }

    fn create_webseed_peers(&mut self, wanted_connections: &mut usize, context: &TorrentContext) {
        let available_permits = self.max_in_flight.saturating_sub(self.in_flight.len());
        let new_connections_len = (*wanted_connections).min(available_permits);
        let webseed_urls = match self.webseed_urls.as_mut() {
            Some(urls) => urls
                .drain(0..new_connections_len.min(urls.len()))
                .collect::<Vec<_>>(),
            None => return,
        };

        // update the wanted connections
        let picked_webseed_urls = webseed_urls.len().min(new_connections_len);
        *wanted_connections = wanted_connections.saturating_sub(picked_webseed_urls);

        for url in webseed_urls {
            let torrent = InnerTorrent::new(
                context.handle(),
                context.command_sender().clone(),
                context.callbacks().clone(),
            );
            let sender = context.command_sender().clone();
            if let Err(e) = self.create_http_peer(torrent, sender, url) {
                warn!(
                    "Torrent {} failed to create webseed peer connection, {}",
                    context, e
                );
            }
        }
    }

    /// Try to establish the peer connection through the torrent peer dialers.
    /// This will dial the address for every dialer and create the connection of the first received successful peer connection.
    fn create_peer_with_dialers<'a>(
        &mut self,
        context: &TorrentContext,
        peer_addr: SocketAddr,
        dialers: &[Arc<dyn PeerDiscovery>],
    ) -> Result<()> {
        let handle_info = context.handle();
        let protocol_extensions = context.protocol_extensions();
        let peer_id = context.peer_id();
        let peer_connection_timeout = context.config().peer_connection_timeout;

        debug!(
            "Torrent {} is trying to create new peer connection to {} through {} dialers",
            context,
            peer_addr,
            dialers.len()
        );
        let mut futures = FuturesUnordered::from_iter(dialers.iter().cloned().map(|dialer| {
            let torrent = InnerTorrent::new(
                context.handle(),
                context.command_sender().clone(),
                context.callbacks().clone(),
            );
            let data_pool = context.data_pool().clone();
            let extensions = context.extensions();

            async move {
                dialer
                    .dial(
                        peer_id,
                        peer_addr.clone(),
                        torrent,
                        data_pool,
                        protocol_extensions,
                        extensions,
                        peer_connection_timeout,
                    )
                    .await
            }
        }));
        if futures.is_empty() {
            return Err(TorrentError::Io(io::Error::new(
                io::ErrorKind::Other,
                "no active peer dialers",
            )));
        }

        let command_sender = context.command_sender().clone();
        self.in_flight.spawn(async move {
            let peer = {
                let mut result = None;
                while let Some(peer) = futures.next().await {
                    match peer {
                        Err(e) => {
                            debug!(
                                "Torrent {} failed to create peer connection, {}",
                                handle_info, e
                            );
                        }
                        Ok(peer) => {
                            result = Some(peer);
                            break;
                        }
                    }
                }
                result
            };

            match peer {
                None => {
                    command_sender
                        .fire_and_forget(TorrentCommand::PeerClosed {
                            peer: peer_addr.into(),
                            reason: CloseReason::ConnectionFailed,
                        })
                        .await;
                }
                Some(peer) => {
                    command_sender
                        .fire_and_forget(TorrentCommand::PeerConnected { peer })
                        .await;
                }
            }
        });

        Ok(())
    }

    /// Try to create a new HTTP (webseed) peer.
    fn create_http_peer(
        &mut self,
        torrent: InnerTorrent,
        sender: ChannelSender<TorrentCommand>,
        url: Url,
    ) -> Result<()> {
        let handle_info = torrent.handle();

        self.in_flight.spawn(async move {
            debug!(
                "Torrent {} is trying to create webseed peer connection to {}",
                handle_info, url
            );
            match HttpPeer::new(url, torrent) {
                Ok(peer) => {
                    sender
                        .fire_and_forget(TorrentCommand::PeerConnected {
                            peer: Box::new(peer),
                        })
                        .await;
                }
                Err(e) => {
                    debug!(
                        "Failed to create http peer connection for torrent {}, {}",
                        handle_info, e
                    );
                }
            }
        });
        Ok(())
    }

    fn parse_url(context: &TorrentContext, url: &str) -> Option<Url> {
        Url::parse(url)
            .map_err(|e| {
                debug!("Torrent {} has invalid webseed url {}, {}", context, url, e);
                e
            })
            .ok()
    }
}

#[async_trait]
impl TorrentOperation for TorrentConnectPeersOperation {
    fn name(&self) -> &str {
        "create peer connections operation"
    }

    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn execute(
        &mut self,
        context: &mut TorrentContext,
        peer_discoveries: &[Arc<dyn PeerDiscovery>],
    ) -> TorrentOperationResult {
        self.poll_in_flight(context);
        let wanted_connections = context.remaining_peer_connections_needed().await;
        if wanted_connections > 0 {
            self.create_webseed_urls(context);
            self.update_max_in_flight(context);

            // burst the initial connections if needed
            self.burst(context).await;

            self.create_additional_peer_connections(wanted_connections, context, peer_discoveries)
                .await;
        }

        TorrentOperationResult::Continue
    }
}

impl Debug for TorrentConnectPeersOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TorrentConnectPeersOperation")
            .field("webseeds_enabled", &self.webseeds_enabled)
            .field("webseed_urls", &self.webseed_urls)
            .field("max_in_flight", &self.max_in_flight)
            .field("bursting_since", &self.bursting_since)
            .finish()
    }
}

impl Drop for TorrentConnectPeersOperation {
    fn drop(&mut self) {
        self.in_flight.abort_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::MockPeerDiscovery;
    use crate::{create_torrent_context, peer};
    use std::net::Ipv4Addr;
    use tempfile::tempdir;
    use tokio::time;

    #[tokio::test]
    async fn test_execute() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
        let (mut context, _) = create_torrent_context!(
            uri,
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![]
        );
        let mut operation = TorrentConnectPeersOperation::new(true);

        let result = operation.execute(&mut context, vec![].as_slice()).await;

        assert_eq!(TorrentOperationResult::Continue, result);
    }

    #[tokio::test]
    async fn test_update_max_in_flight() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
        let (context, _) = create_torrent_context!(
            uri,
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().peers_in_flight(35).build(),
            vec![]
        );
        let mut operation = TorrentConnectPeersOperation::new(true);

        // update the permits from the torrent settings
        operation.update_max_in_flight(&context);

        assert_eq!(
            35, operation.max_in_flight,
            "expected the max in flight to have been updated"
        );
    }

    #[tokio::test]
    async fn test_burst() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
        let (context, _) = create_torrent_context!(
            uri,
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder()
                .peers_in_flight(20)
                .peers_upper_limit(200)
                .build(),
            vec![]
        );
        let mut operation = TorrentConnectPeersOperation::new(true);

        // update the permits from the torrent settings
        operation.update_max_in_flight(&context);
        let result = operation.max_in_flight;
        assert_eq!(20, result);

        // invoke the burst fn
        operation.burst(&context).await;
        assert_eq!(
            200, operation.max_in_flight,
            "expected the max in flight to have been temporary bursted"
        );
        assert_ne!(
            None, operation.bursting_since,
            "expected the bursting since to be set"
        );
    }

    #[tokio::test]
    async fn test_peer_connection_failed() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let mut dialer = MockPeerDiscovery::new();
        dialer.expect_dial().returning(|_, _, _, _, _, _, _| {
            Err(peer::Error::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                "timeout",
            )))
        });
        let dialers: Vec<Arc<dyn PeerDiscovery>> = vec![Arc::new(dialer)];
        let (mut context, mut receiver) = create_torrent_context!(
            "debian.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            None
        );
        let mut operation = TorrentConnectPeersOperation::new(false);

        // add an invalid peer address
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 13470));
        context.peer_pool_mut().add_peer_addresses(vec![addr], None);

        // execute the operation
        let result = operation.execute(&mut context, dialers.as_slice()).await;
        assert_eq!(TorrentOperationResult::Continue, result);

        // wait for the in_flight operation to complete
        timeout!(
            async {
                loop {
                    operation.poll_in_flight(&context);
                    if operation.in_flight.is_empty() {
                        break;
                    }
                    time::sleep(Duration::from_millis(5)).await;
                }
            },
            Duration::from_secs(1),
            "expected the in flight operation to complete"
        );

        let command = timeout!(receiver.recv(), Duration::from_millis(250))
            .expect("expected a command to have been sent");
        match command {
            TorrentCommand::PeerClosed { reason, .. } => {
                assert_eq!(CloseReason::ConnectionFailed, reason);
            }
            _ => assert!(
                false,
                "expected TorrentCommand::PeerClosed, but got {:?}",
                command
            ),
        }
    }

    mod webseed_url {
        use super::*;

        #[tokio::test]
        async fn test_create_webseed_urls() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let expected_result = vec![Url::parse("https://archive.org/download/").unwrap()]
                .into_iter()
                .collect();
            let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce&ws=https%3A%2F%2Farchive.org%2Fdownload%2F";
            let (context, _) = create_torrent_context!(
                uri,
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![],
                None
            );
            let mut operation = TorrentConnectPeersOperation::new(true);

            operation.create_webseed_urls(&context);

            let result = operation.webseed_urls.as_ref();
            assert_eq!(Some(&expected_result), result);
        }

        #[tokio::test]
        async fn test_create_webseed_peers() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce&ws=https%3A%2F%2Farchive.org%2Fdownload%2F";
            let (context, _) = create_torrent_context!(
                uri,
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().peers_in_flight(50).build(),
                vec![],
                None
            );
            let mut operation = TorrentConnectPeersOperation::new(true);

            // set the webseed urls
            operation.webseed_urls = Some(
                vec![
                    Url::parse("https://test-url-1.com/").unwrap(),
                    Url::parse("https://test-url-2.com/").unwrap(),
                ]
                .into_iter()
                .collect(),
            );

            // update the max in-flight
            operation.update_max_in_flight(&context);

            // create new webseed peers
            let mut wanted_connections = 100;
            operation.create_webseed_peers(&mut wanted_connections, &context);
            assert_eq!(
                98, wanted_connections,
                "expected 98 remaining connections to be wanted"
            );
            assert_eq!(
                0,
                operation.webseed_urls.as_ref().map(|e| e.len()).unwrap(),
                "expected the webseed urls to be consumed"
            );
        }
    }
}
