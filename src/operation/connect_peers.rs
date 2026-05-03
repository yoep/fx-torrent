use crate::channel::ChannelSender;
use crate::operation::{TorrentOperation, TorrentOperationResult};
use crate::peer::extension::HolepunchExtension;
use crate::peer::webseed::HttpPeer;
use crate::peer::{extension, BitTorrentPeer, CloseReason, Peer, PeerDiscovery};
use crate::torrent::InnerTorrent;
use crate::{Result, TorrentCommand, TorrentContext, TorrentError};
use async_trait::async_trait;
use futures::FutureExt;
use itertools::Itertools;
use log::{debug, trace, warn};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinSet;
use tokio::time::timeout;
use url::Url;

const BURST_DURATION: Duration = Duration::from_secs(10);

/// Establishes additional peer connections for the torrent.
pub struct TorrentConnectPeersOperation {
    initialized: bool,
    /// Indicates whether webseed connections are enabled for the torrent
    webseeds_enabled: bool,
    /// The webseed urls to connect to
    webseed_urls: VecDeque<Url>,
    /// The maximum amount of in-flight peer connections being established
    max_in_flight: usize,
    /// Indicates the time since the operation started bursting the initial connections
    bursting_since: Option<Instant>,
    /// The in-flight peer-webseed connections
    in_flight: JoinSet<()>,
    /// Indicates whether HolePunching is supported for the torrent.
    holepunch_supported: bool,
    /// The sender for HolePunching requests.
    holepunch_sender: Sender<SocketAddr>,
    /// The receiver for HolePunching requests.
    holepunch_receiver: Receiver<SocketAddr>,
    /// The pending hole punch requests.
    holepunch_queue: JoinSet<(SocketAddr, Option<extension::Error>)>,
}

impl TorrentConnectPeersOperation {
    pub fn new(webseeds_enabled: bool) -> Self {
        let (tx, rx) = channel(32);
        Self {
            initialized: false,
            webseeds_enabled,
            webseed_urls: VecDeque::new(),
            max_in_flight: 0,
            bursting_since: None,
            in_flight: Default::default(),
            holepunch_supported: false,
            holepunch_sender: tx,
            holepunch_receiver: rx,
            holepunch_queue: Default::default(),
        }
    }

    /// Poll the currently in-flight peer connections for completion.
    fn poll_in_flight(&mut self, context: &TorrentContext) {
        while let Some(res) = self.in_flight.join_next().now_or_never().flatten() {
            match res {
                Ok(()) => {}
                Err(e) => warn!("Torrent {} peer connection failed, {}", context, e),
            }
        }
    }

    /// Poll the pending holepunch requests.
    fn poll_holepunches(&mut self, context: &mut TorrentContext) {
        while let Some(res) = self.holepunch_queue.join_next().now_or_never().flatten() {
            match res {
                Ok((addr, err)) => {
                    context.peer_pool_mut().peer_punched(&addr, err.is_none());
                    if let Some(err) = err {
                        debug!("Torrent {} failed to punch {}, {}", context, addr, err);
                    }
                }
                Err(_) => break,
            }
        }
    }

    /// Initialize the operation.
    async fn initialize(&mut self, context: &TorrentContext) {
        if self.initialized {
            return;
        }

        self.create_webseed_urls(context);
        self.holepunch_supported = context.is_extension_enabled(HolepunchExtension::NAME);
        self.initialized = true;
    }

    /// Create the webseed url cache from the torrent context.
    fn create_webseed_urls(&mut self, torrent: &TorrentContext) {
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
        self.webseed_urls = urls;
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
    fn burst(&mut self, context: &TorrentContext) {
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
        if context.active_peer_connections() == 0 {
            trace!("Torrent {} is bursting it's initial connections", context);
            self.max_in_flight = context.config().peers_upper_limit;
            self.bursting_since = Some(Instant::now());
        }
    }

    /// Execute the pending holepunch tasks.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn execute_pending_holepunches(&mut self, context: &mut TorrentContext) {
        let mut peers = None;

        while let Ok(addr) = self.holepunch_receiver.try_recv() {
            // collect the peers once
            let peers = peers.get_or_insert_with(|| {
                context
                    .peer_pool()
                    .peers()
                    .filter_map(|e| match e {
                        Peer::BitTorrent(e) => Some(e.clone()),
                        _ => None,
                    })
                    .collect_vec()
            });

            self.try_holepunch(addr, peers.as_slice(), context).await;
        }
    }

    /// Try to create additional peer connections
    async fn create_additional_peer_connections(
        &mut self,
        mut wanted_connections: usize,
        context: &mut TorrentContext,
        dialers: &[PeerDiscovery],
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
        let len = new_connections_len.min(self.webseed_urls.len());
        let webseed_urls = self.webseed_urls.drain(0..len).collect_vec();
        if webseed_urls.is_empty() {
            return;
        }

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
        dialers: &[PeerDiscovery],
    ) -> Result<()> {
        // early exit when no dialers are available
        if dialers.is_empty() {
            return Err(TorrentError::Io(io::Error::new(
                io::ErrorKind::Other,
                "no active peer dialers",
            )));
        }

        let handle_info = context.handle();
        let protocol_extensions = context.protocol_extensions();
        let peer_id = context.peer_id();
        let peer_connection_timeout = context.config().peer_connection_timeout;
        let holepunch_sender = self.holepunch_sender.clone();
        let holepunch_supported = self.holepunch_supported;

        debug!(
            "Torrent {} is trying to create new peer connection to {} through {} dialers",
            context,
            peer_addr,
            dialers.len()
        );
        let torrent = InnerTorrent::new(
            context.handle(),
            context.command_sender().clone(),
            context.callbacks().clone(),
        );
        let data_pool = context.data_pool().clone();
        let dialers = dialers.iter().cloned().collect_vec();
        let command_sender = context.command_sender().clone();
        self.in_flight.spawn(async move {
            let mut peer = None;
            for dialer in dialers {
                match dialer
                    .dial(
                        peer_id,
                        peer_addr.clone(),
                        torrent.clone(),
                        data_pool.clone(),
                        protocol_extensions,
                        peer_connection_timeout,
                    )
                    .await
                {
                    Ok(e) => {
                        peer = Some(e);
                        break;
                    }
                    Err(e) => {
                        debug!(
                            "Torrent {} failed to create peer connection, {}",
                            handle_info, e
                        );
                    }
                }
            }

            match peer {
                None => {
                    if holepunch_supported {
                        let _ = holepunch_sender.send(peer_addr).await;
                    } else {
                        command_sender
                            .fire_and_forget(TorrentCommand::PeerClosed {
                                addr: peer_addr,
                                reason: CloseReason::Timeout,
                            })
                            .await;
                    }
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
                        .fire_and_forget(TorrentCommand::PeerConnected { peer: peer.into() })
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

    /// Try to HolePunch the target peer.
    async fn try_holepunch(
        &mut self,
        target_addr: SocketAddr,
        peers: &[BitTorrentPeer],
        context: &mut TorrentContext,
    ) {
        let mut punching = false;
        for peer in peers {
            if !timeout(
                Duration::from_millis(200),
                peer.supports_extension(HolepunchExtension::NAME),
            )
            .await
            .unwrap_or_default()
            {
                continue;
            }

            context.peer_pool_mut().peer_punching(&target_addr);
            match timeout(Duration::from_millis(500), peer.holepunch(target_addr)).await {
                Ok(response) => {
                    punching = true;
                    self.holepunch_queue
                        .spawn(async move { (target_addr, response.await.err()) });
                }
                Err(e) => {
                    context.peer_pool_mut().peer_punched(&target_addr, false);
                    debug!("Torrent {} failed to start holepunch, {}", context, e);
                }
            }
        }

        // if we're unable to punch the target peer
        // record the initial attempt as a timeout failure
        if !punching {
            context
                .peer_pool_mut()
                .peer_closed(&target_addr, CloseReason::Timeout);
        }
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

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn execute(
        &mut self,
        context: &mut TorrentContext,
        peer_discoveries: &[PeerDiscovery],
    ) -> TorrentOperationResult {
        self.initialize(context).await;
        self.poll_in_flight(context);
        self.poll_holepunches(context);

        let wanted_connections = context.remaining_peer_connections_needed().await;
        if wanted_connections > 0 {
            self.update_max_in_flight(context);
            self.execute_pending_holepunches(context).await;

            // burst the initial connections if needed
            self.burst(context);

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
    use crate::peer;
    use crate::peer::extension::{DontHaveExtension, MetadataExtension};
    use crate::peer::{MockDiscovery, PeerDiscovery};
    use std::net::Ipv4Addr;
    use tempfile::tempdir;
    use tokio::time;

    #[tokio::test]
    async fn test_execute() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
        let (mut context, _) = torrent_context!(
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
        let (context, _) = torrent_context!(
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
        let (context, _) = torrent_context!(
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
        operation.burst(&context);
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
        let mut dialer = MockDiscovery::new();
        dialer.expect_dial().returning(|_, _, _, _, _, _| {
            Err(peer::Error::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                "timeout",
            )))
        });
        let dialers: Vec<PeerDiscovery> = vec![dialer.into()];
        let (mut context, mut receiver) = torrent_context!(
            "debian.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            vec![|| MetadataExtension::new().into(), || {
                DontHaveExtension::new().into()
            },],
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
            Duration::from_secs(1),
            async {
                loop {
                    operation.poll_in_flight(&context);
                    if operation.in_flight.is_empty() {
                        break;
                    }
                    time::sleep(Duration::from_millis(5)).await;
                }
            },
            "expected the in flight operation to complete"
        );

        let command = timeout!(Duration::from_millis(250), receiver.recv())
            .expect("expected a command to have been sent");
        match command {
            TorrentCommand::PeerClosed { reason, .. } => {
                assert_eq!(CloseReason::Timeout, reason);
            }
            _ => assert!(
                false,
                "expected TorrentCommand::PeerClosed, but got {:?}",
                command
            ),
        }
    }

    mod initialize {
        use super::*;

        #[tokio::test]
        async fn test_holepunch_supported() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let (mut context, _receiver) = torrent_context!(
                "debian.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![],
                vec![|| HolepunchExtension::new().into()],
                None,
                None
            );
            let mut operation = TorrentConnectPeersOperation::new(false);

            // execute the operation
            let dialers = vec![];
            let result = operation.execute(&mut context, dialers.as_slice()).await;
            assert_eq!(TorrentOperationResult::Continue, result);
            assert_eq!(
                true, operation.initialized,
                "expected the operation to have been initialized"
            );

            // check that the hole punch support is enabled
            assert_eq!(
                true, operation.holepunch_supported,
                "expected hole punch support to be enabled"
            );
        }

        #[tokio::test]
        async fn test_holepunch_unsupported() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let (mut context, _receiver) = torrent_context!(
                "debian.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![],
                vec![],
                None,
                None
            );
            let mut operation = TorrentConnectPeersOperation::new(false);

            // execute the operation
            let dialers = vec![];
            let result = operation.execute(&mut context, dialers.as_slice()).await;
            assert_eq!(TorrentOperationResult::Continue, result);
            assert_eq!(
                true, operation.initialized,
                "expected the operation to have been initialized"
            );

            // check the hole punch support of the torrent
            assert_eq!(
                false, operation.holepunch_supported,
                "expected hole punch to not have been supported"
            );
        }
    }

    mod webseed_url {
        use super::*;

        #[tokio::test]
        async fn test_create_webseed_urls() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let expected_result: VecDeque<Url> =
                vec![Url::parse("https://archive.org/download/").unwrap()]
                    .into_iter()
                    .collect();
            let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce&ws=https%3A%2F%2Farchive.org%2Fdownload%2F";
            let (context, _) = torrent_context!(
                uri,
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![],
                vec![],
                None
            );
            let mut operation = TorrentConnectPeersOperation::new(true);

            operation.create_webseed_urls(&context);

            assert_eq!(&expected_result, &operation.webseed_urls);
        }

        #[tokio::test]
        async fn test_create_webseed_peers() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce&ws=https%3A%2F%2Farchive.org%2Fdownload%2F";
            let (context, _) = torrent_context!(
                uri,
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().peers_in_flight(50).build(),
                vec![],
                vec![],
                None
            );
            let mut operation = TorrentConnectPeersOperation::new(true);

            // set the webseed urls
            operation.webseed_urls = vec![
                Url::parse("https://test-url-1.com/").unwrap(),
                Url::parse("https://test-url-2.com/").unwrap(),
            ]
            .into_iter()
            .collect();

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
                operation.webseed_urls.len(),
                "expected the webseed urls to be consumed"
            );
        }
    }
}
