use crate::channel::ChannelSender;
use crate::operation::{TorrentOperation, TorrentOperationResult};
use crate::peer::webseed::HttpPeer;
use crate::peer::PeerDiscovery;
use crate::torrent::InnerTorrent;
use crate::{TorrentCommand, TorrentContext};
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use log::{debug, trace, warn};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
#[cfg(feature = "tracing")]
use tracing::instrument;
use url::Url;

const BURST_DURATION: Duration = Duration::from_secs(10);

/// Establishes additional peer connections for the torrent.
pub struct TorrentConnectPeersOperation {
    webseeds_enabled: bool,
    webseed_urls: Option<Vec<Url>>,
    /// The maximum amount of in-flight peer connections being established
    max_in_flight: usize,
    /// Indicates the time since the operation started bursting the initial connections
    bursting_since: Option<Instant>,
    /// The in-flight peer-webseed connections
    in_flight: FuturesUnordered<BoxFuture<'static, ()>>,
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
    fn poll_in_flight(&mut self) {
        while self.in_flight.next().now_or_never().flatten().is_some() {}
    }

    /// Create the webseed url cache from the torrent context.
    async fn create_webseed_urls(&mut self, torrent: &TorrentContext) {
        if self.webseed_urls.is_some() {
            return;
        }

        let mut urls = torrent
            .metadata()
            .url_list
            .as_ref()
            .map(|list| {
                list.iter()
                    .flat_map(|url| Self::parse_url(torrent, url))
                    .collect()
            })
            .unwrap_or(Vec::new());

        let mut http_seeds = torrent
            .metadata()
            .http_seeds
            .as_ref()
            .map(|e| {
                e.iter()
                    .flat_map(|url| Self::parse_url(torrent, url))
                    .collect()
            })
            .unwrap_or(Vec::new());

        urls.append(&mut http_seeds);
        self.webseed_urls = Some(urls);
    }

    /// Update the available in-flight permits from the latest torrent config.
    fn update_max_in_flight(&mut self, context: &TorrentContext) {
        let config_peers_in_flight = context.config().peers_in_flight;
        if config_peers_in_flight == self.max_in_flight {
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
            return;
        }

        debug!(
            "Creating an additional {} (of wanted {}, remaining {} addresses) peer connections for {}",
            peer_addrs.len(),
            wanted_connections,
            context.peer_pool().num_connect_candidates(),
            context
        );
        for addr in peer_addrs {
            self.in_flight.push(Box::pin(
                self.create_peer_with_dialers(context, addr, dialers),
            ));
        }
    }

    fn create_webseed_peers(&mut self, wanted_connections: &mut usize, context: &TorrentContext) {
        let available_permits = self.max_in_flight.saturating_sub(self.in_flight.len());
        let len = (*wanted_connections).min(available_permits);
        let webseed_urls = match self.webseed_urls.as_mut() {
            Some(urls) => urls.drain(0..len.min(urls.len())).collect::<Vec<_>>(),
            None => return,
        };

        // update the wanted connections
        *wanted_connections = wanted_connections.saturating_sub(len);

        for url in webseed_urls {
            let torrent = InnerTorrent::new(
                context.handle(),
                context.command_sender().clone(),
                context.callbacks().clone(),
            );
            let sender = context.command_sender().clone();
            self.in_flight
                .push(Box::pin(Self::create_http_peer(torrent, sender, url)));
        }
    }

    /// Try to establish the peer connection through the torrent peer dialers.
    /// This will dial the address for every dialer and create the connection of the first received successful peer connection.
    fn create_peer_with_dialers<'a>(
        &self,
        context: &TorrentContext,
        peer_addr: SocketAddr,
        dialers: &[Arc<dyn PeerDiscovery>],
    ) -> BoxFuture<'static, ()> {
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
                        peer_addr,
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
            warn!("Torrent {} has no active peer dialers", context);
            return Box::pin(async {});
        }

        let command_sender = context.command_sender().clone();
        Box::pin(async move {
            while let Some(peer) = futures.next().await {
                match peer {
                    Err(e) => {
                        debug!(
                            "Torrent {} failed to create peer connection, {}",
                            handle_info, e
                        );
                    }
                    Ok(peer) => {
                        command_sender
                            .fire_and_forget(TorrentCommand::PeerConnected { peer })
                            .await;
                    }
                }
            }
        })
    }

    /// Try to create a new HTTP (webseed) peer.
    async fn create_http_peer(
        torrent: InnerTorrent,
        sender: ChannelSender<TorrentCommand>,
        url: Url,
    ) {
        let handle_info = torrent.handle();

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
    }

    fn parse_url(context: &TorrentContext, url: &String) -> Option<Url> {
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
        self.poll_in_flight();
        let wanted_connections = context.remaining_peer_connections_needed().await;
        if wanted_connections > 0 {
            self.create_webseed_urls(context).await;
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
            .field("webseed_urls", &self.webseed_urls)
            .field("max_in_flight", &self.max_in_flight)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_torrent_context;
    use crate::init_logger;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_create_webseed_urls() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let expected_result = vec![Url::parse("https://archive.org/download/").unwrap()];
        let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce&ws=https%3A%2F%2Farchive.org%2Fdownload%2F";
        let (context, _) = create_torrent_context!(
            uri,
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![]
        );
        let mut operation = TorrentConnectPeersOperation::new(true);

        operation.create_webseed_urls(&context).await;

        let result = operation.webseed_urls;
        assert_eq!(Some(expected_result), result);
    }

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
    async fn test_update_permits() {
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
}
