use crate::peer::{CloseReason, Peer, PeerHandle, PeerState};
use crate::{PeerPriority, TorrentHandle, TorrentPeer};
use derive_more::Display;
use itertools::Itertools;
use log::{debug, trace};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::time;

const CONNECTION_FAILURE_THRESHOLD: usize = 3;

/// The failure reason when adding a peer failed.
#[derive(Debug, Error, PartialEq)]
pub enum AddReason {
    #[error("peer already exists within the pool")]
    Duplicate,
    #[error("pool limit has been reached")]
    LimitReached,
}

/// The torrent peer pool manager for a single torrent.
/// This manager is responsible for managing the torrent peer information and actual active peers.
#[derive(Debug, Display)]
#[display("{}", handle)]
pub struct PeerPool {
    /// The unique handle of the torrent
    handle: TorrentHandle,
    /// The peers of the torrent.
    peers: HashMap<SocketAddr, PeerInfo>,
    /// The maximum amount of peers allowed in the pool
    limit: usize,
}

impl PeerPool {
    /// Create a new peer pool for the given torrent handle.
    pub fn new(handle: TorrentHandle, pool_limit: usize) -> Self {
        Self {
            handle,
            peers: Default::default(),
            limit: pool_limit,
        }
    }

    /// Get a torrent peer for the given handle.
    ///
    /// It returns the torrent peer instance when found, else [None].
    pub fn get(&self, handle: &PeerHandle) -> Option<TorrentPeer> {
        self.peers
            .values()
            .filter_map(|e| e.connection.as_ref())
            .find(|peer| peer.handle() == *handle)
            .map(|peer| TorrentPeer::new(peer))
    }

    /// Returns an iterator over the peers in the pool.
    pub fn peers(&self) -> impl Iterator<Item = &Arc<dyn Peer>> {
        self.peers.values().filter_map(|e| e.connection.as_ref())
    }

    /// Returns an iterator over the peer addresses in the pool.
    pub fn peer_addrs(&self) -> impl Iterator<Item = &SocketAddr> {
        self.peers.keys()
    }

    /// Add the given [TcpPeer] to this peer pool.
    /// The pool will check if the peer is unique before adding it to the pool, if it's a duplicate,
    /// the peer won't be added to the pool and the function will return [None].
    ///
    /// It returns a [Subscription] to receive peer events when the peer is added to the pool.
    pub fn add_peer(&mut self, peer: Box<dyn Peer>) -> Result<(), AddReason> {
        let handle = peer.handle();
        // early exit if the pool is full
        if self.peers.len() >= self.limit {
            debug!(
                "Torrent {} is unable to add peer {}, pool limit reached",
                self, handle
            );
            return Err(AddReason::LimitReached);
        }

        // update the peer info
        let info = self.find_or_insert(&peer.addr(), None);
        info.is_in_use = true;
        info.last_connected = Some(Instant::now());
        info.connection = Some(Arc::from(peer));
        Ok(())
    }

    /// Get the total amount of candidates for creating new connections.
    pub fn num_connect_candidates(&self) -> usize {
        self.peers
            .iter()
            .filter(|(_, peer)| peer.is_connect_candidate())
            .count()
    }

    /// Add the given peer addresses to the pool's peer list.
    pub fn add_peer_addresses(&mut self, addrs: Vec<SocketAddr>, torrent_addr: Option<SocketAddr>) {
        let addrs = addrs
            .into_iter()
            // filter out already known peer addresses
            .filter(|addr| !self.peers.contains_key(addr))
            .unique()
            .collect_vec();
        if addrs.is_empty() {
            trace!(
                "Torrent {} peer addresses are already known, skipping",
                self
            );
            return;
        }

        let total_addresses = addrs.len();
        for addr in addrs {
            let _ = self.find_or_insert(&addr, torrent_addr);
        }
        debug!(
            "Torrent {} added {} new peer addresses to the pool",
            self, total_addresses
        );
    }

    /// Inform the pool that a peer connection has been closed.
    ///
    /// Returns the removed peer from the pool, if found.
    pub async fn peer_closed(
        &mut self,
        addr: &SocketAddr,
        reason: CloseReason,
    ) -> Option<Arc<dyn Peer>> {
        let info = match self.peers.get_mut(&addr) {
            Some(info) => info,
            None => {
                debug!("Torrent {} failed to find info for peer {}", self, addr);
                return None;
            }
        };

        trace!(
            "Torrent {} peer {} connection closed, reason: {:?}",
            self.handle,
            addr,
            reason
        );
        match reason {
            CloseReason::ConnectionFailed => {
                info.failed();
            }
            CloseReason::FastProtocol => {
                info.failed();
                info.last_connected = Some(Instant::now());
            }
            _ => {
                info.last_connected = Some(Instant::now());
            }
        }
        let peer = match info.connection.take() {
            None => None,
            Some(peer) => {
                info.is_seed = peer.is_seed().await;
                Some(peer)
            }
        };

        info.is_in_use = false;
        peer
    }

    /// Update the peer priority of the given address.
    pub fn update_peer_rank(&mut self, addr: &SocketAddr, change: i32) {
        if let Some(peer) = self.peers.get_mut(addr) {
            let mut rank = peer.rank.take().unwrap_or(0);

            if change < 0 {
                rank = rank.saturating_sub(1);
            } else {
                rank = rank.saturating_add(1);
            }

            peer.rank = PeerPriority::from(rank);
        }
    }

    /// Try to get the given amount of peer list addresses from the pool.
    /// If the peer list candidates are not enough, it will return the remaining available addresses.
    ///
    /// # Arguments
    ///
    /// * `len` - The total number of peer list address to retrieve.
    pub fn new_connection_candidates(&mut self, len: usize) -> Vec<SocketAddr> {
        let peers_len = self.peers.len();
        let remaining_slots = self.limit.saturating_sub(peers_len);
        let len = len.min(remaining_slots).min(peers_len);

        self.peers
            .iter_mut()
            .filter(|(_, peer)| peer.is_connect_candidate())
            .sorted()
            .take(len)
            .map(|(addr, peer)| {
                peer.is_in_use = true;
                addr.clone()
            })
            .collect()
    }

    /// Returns the number of total healthy peer connections from the pool.
    pub async fn active_peer_connections(&self) -> usize {
        let futures = self
            .peers
            .values()
            .filter_map(|e| e.connection.as_ref())
            .map(|peer| peer.state())
            .collect_vec();

        futures::future::join_all(futures)
            .await
            .into_iter()
            .filter(|state| state != &PeerState::Closed && state != &PeerState::Error)
            .count()
    }

    /// Set a new maximum amount of peers allowed in the pool.
    pub fn set_pool_limit(&mut self, limit: usize) {
        self.limit = limit;
    }

    /// Remove any closed or invalid peers from the pool.
    /// The cleanup tries to close the peer connection within a timely manner if possible.
    pub async fn clean(&mut self) -> Vec<Arc<dyn Peer>> {
        let mut total_cleaned_peers = 0;
        let mut removed_peers = vec![];

        let futures: Vec<_> = self
            .peers
            .values()
            .filter_map(|e| e.connection.as_ref())
            .map(|peer| async move { (peer.addr(), peer.state().await) })
            .collect();

        for (addr, state) in futures::future::join_all(futures).await {
            let reason = match state {
                PeerState::Closed => CloseReason::Remote,
                PeerState::Error => CloseReason::Error,
                _ => continue,
            };
            let peer = match self.peer_closed(&addr, reason).await {
                Some(peer) => peer,
                None => continue,
            };

            total_cleaned_peers += 1;
            if let Err(_) = time::timeout(Duration::from_secs(1), peer.close()).await {
                debug!(
                    "Torrent {} failed to close peer {} connection, close operation timed out",
                    self, peer
                );
            }

            removed_peers.push(peer);
        }

        debug!("Cleaned a total of {} peers", total_cleaned_peers);
        removed_peers
    }

    /// Shut down the peer pool, closing all peer connections.
    pub async fn shutdown(&mut self) {
        debug!("Torrent {} is shutting down peer pool", self);

        // clear all known peer list addresses
        self.peers.clear();
        self.set_pool_limit(0);

        let peers_to_close = {
            self.peers
                .drain()
                .map(|(_, e)| e)
                .filter_map(|mut info| info.connection.take())
                .collect_vec()
        };

        // close all peers within the pool
        let futures = peers_to_close
            .iter()
            .map(|peer| peer.close())
            .collect::<Vec<_>>();
        futures::future::join_all(futures).await;
    }

    /// Try to find the peer info for the given address.
    ///
    /// Returns the existing peer info if found, else creates a new entry.
    fn find_or_insert(
        &mut self,
        addr: &SocketAddr,
        torrent_addr: Option<SocketAddr>,
    ) -> &mut PeerInfo {
        self.peers.entry(*addr).or_insert_with(|| {
            if let Some(torrent_addr) = torrent_addr {
                PeerInfo::new_with_rank(*addr, &torrent_addr)
            } else {
                PeerInfo::new(*addr)
            }
        })
    }
}

/// The address information of a peer for the torrent.
#[derive(Debug, Clone)]
struct PeerInfo {
    /// The address of a remote peer.
    addr: SocketAddr,
    /// Indicates if this peer address is in use by the torrent.
    is_in_use: bool,
    /// Indicates if this peer has been identified as a seed.
    is_seed: bool,
    /// Indicates if this peer has been banned from establishing a connection.
    is_banned: bool,
    /// The number of failures when trying to connect to the remote peer.
    failure_count: usize,
    /// The peer priority rank.
    rank: PeerPriority,
    /// The last time the peer connected or disconnected from the torrent
    last_connected: Option<Instant>,
    /// The active connection to the remote peer.
    connection: Option<Arc<dyn Peer>>,
}

impl PeerInfo {
    /// Create a new torrent peer address information.
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            is_in_use: false,
            is_seed: false,
            is_banned: false,
            failure_count: 0,
            rank: PeerPriority::none(),
            last_connected: None,
            connection: None,
        }
    }

    /// Create a new torrent peer address information.
    /// This peer address contains a rank based against the current torrent listening address.
    pub fn new_with_rank(addr: SocketAddr, torrent_addr: &SocketAddr) -> Self {
        let rank = PeerPriority::from((torrent_addr, &addr));
        Self {
            addr,
            is_in_use: false,
            is_seed: false,
            is_banned: false,
            failure_count: 0,
            rank,
            last_connected: None,
            connection: None,
        }
    }

    /// Check if this peer is a candidate for establishing a new connection.
    ///
    /// # Returns
    ///
    /// It returns true when the peer is a candidate, else false.
    pub fn is_connect_candidate(&self) -> bool {
        !self.is_in_use && !self.is_banned && self.failure_count < CONNECTION_FAILURE_THRESHOLD
    }

    /// Increase the failure count for this peer.
    fn failed(&mut self) {
        self.failure_count += 1;
    }
}

impl PartialEq for PeerInfo {
    fn eq(&self, other: &Self) -> bool {
        self.addr == other.addr
    }
}

impl PartialOrd for PeerInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // always prefer known seeds over rank
        if self.is_seed && !other.is_seed {
            return Some(Ordering::Less);
        } else if other.is_seed && !self.is_seed {
            return Some(Ordering::Greater);
        }

        // always prefer lesser failed addresses above rank
        if self.failure_count != other.failure_count {
            return self.failure_count.partial_cmp(&other.failure_count);
        }

        self.rank.partial_cmp(&other.rank)
    }
}

impl Eq for PeerInfo {}

impl Ord for PeerInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod peer_pool {
        use super::*;
        use crate::peer::tests::MockPeer;
        use std::net::Ipv4Addr;

        mod add_peer {
            use super::*;
            use crate::peer::tests::MockPeer;
            use std::net::Ipv4Addr;

            #[tokio::test]
            async fn test_add_peer() {
                init_logger!();
                let peer_handle = PeerHandle::new();
                let mut peer = MockPeer::new();
                peer.expect_handle().return_const(peer_handle);
                peer.expect_addr()
                    .return_const(SocketAddr::from((Ipv4Addr::LOCALHOST, 6881)));
                let mut pool = PeerPool::new(TorrentHandle::new(), 2);

                let result = pool.add_peer(Box::new(peer));
                assert_eq!(Ok(()), result, "expected the peer to have been added");

                let result = pool.peers.len();
                assert_eq!(
                    1, result,
                    "expected the peer to have been present within the pool"
                );
            }

            #[tokio::test]
            async fn test_limit_reached() {
                init_logger!();
                let peer_handle1 = PeerHandle::new();
                let peer_handle2 = PeerHandle::new();
                let mut peer1 = MockPeer::new();
                peer1.expect_handle().return_const(peer_handle1);
                peer1
                    .expect_addr()
                    .return_const(SocketAddr::from((Ipv4Addr::LOCALHOST, 6881)));
                let mut peer2 = MockPeer::new();
                peer2.expect_handle().return_const(peer_handle2);
                peer2
                    .expect_addr()
                    .return_const(SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)));
                let mut pool = PeerPool::new(TorrentHandle::new(), 1);

                let result = pool.add_peer(Box::new(peer1));
                assert_eq!(Ok(()), result, "expected the peer to have been added");

                let result = pool.add_peer(Box::new(peer2));
                assert_eq!(
                    Err(AddReason::LimitReached),
                    result,
                    "expected the peer to not have been added"
                );
            }
        }

        mod peer_closed {
            use super::*;
            use std::net::Ipv4Addr;

            #[tokio::test]
            async fn test_reason_connection_failed() {
                init_logger!();
                let peer_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 6881));
                let mut pool = PeerPool::new(TorrentHandle::new(), 16);

                // add the peer address to the pool
                pool.add_peer_addresses(vec![peer_addr.clone()], None);

                // close the peer connection
                let result = pool
                    .peer_closed(&peer_addr.clone().into(), CloseReason::ConnectionFailed)
                    .await;
                assert!(
                    result.is_none(),
                    "expected no peer to have been removed, but got {:?}",
                    result
                );

                let info = pool
                    .peers
                    .get(&peer_addr)
                    .expect("expected the address info to have been found");
                assert_eq!(
                    1, info.failure_count,
                    "expected the failure count to have been incremented"
                );
            }

            #[tokio::test]
            async fn test_reason_fast_protocol() {
                init_logger!();
                let peer_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 6881));
                let mut peer = MockPeer::new();
                peer.expect_handle().return_const(PeerHandle::new());
                peer.expect_addr().return_const(peer_addr);
                peer.expect_is_seed().return_const(false);
                let mut pool = PeerPool::new(TorrentHandle::new(), 16);

                // add the peer address to the pool
                pool.add_peer_addresses(vec![peer_addr.clone()], None);
                pool.add_peer(Box::new(peer))
                    .expect("expected the peer to have been added");

                // close the peer connection
                let result = pool
                    .peer_closed(&peer_addr.clone().into(), CloseReason::FastProtocol)
                    .await;
                assert!(result.is_some(), "expected the peer to have been removed");

                // get the peer info
                let info = pool
                    .peers
                    .get(&peer_addr)
                    .expect("expected the address info to have been found");
                assert_eq!(
                    1, info.failure_count,
                    "expected the failure count to have been incremented"
                );
                assert!(
                    info.last_connected.is_some(),
                    "expected the last connected time to be set"
                );
            }
        }

        #[tokio::test]
        async fn test_peer_pool_add_available_peer_addrs() {
            init_logger!();
            let expected_result = vec![SocketAddr::from(([127, 0, 0, 1], 1900))];
            let mut pool = PeerPool::new(TorrentHandle::new(), 2);

            pool.add_peer_addresses(expected_result.clone(), None);
            let result = pool.num_connect_candidates();
            assert_eq!(1, result, "expected the address to have been added");

            let result = pool.new_connection_candidates(1);
            assert_eq!(expected_result, result);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
        async fn test_peer_pool_clean() {
            init_logger!();
            let peer1_handle = PeerHandle::new();
            let mut peer1 = MockPeer::new();
            peer1.expect_handle().return_const(peer1_handle);
            peer1
                .expect_addr()
                .return_const(SocketAddr::from((Ipv4Addr::LOCALHOST, 6881)));
            peer1.expect_state().returning(|| PeerState::Closed);
            peer1.expect_close().return_const(());
            peer1.expect_is_seed().returning(|| true);
            let mut peer2 = MockPeer::new();
            peer2.expect_handle().return_const(PeerHandle::new());
            peer2
                .expect_addr()
                .return_const(SocketAddr::from(([127, 0, 0, 2], 6899)));
            peer2.expect_is_seed().returning(|| false);
            peer2.expect_state().returning(|| PeerState::Idle);
            let mut pool = PeerPool::new(TorrentHandle::new(), 2);

            // add peers to the pool
            let _ = pool.add_peer(Box::new(peer1));
            let _ = pool.add_peer(Box::new(peer2));
            let result = pool.peers.len();
            assert_eq!(
                2, result,
                "expected the peers to have been added to the pool"
            );

            // clean the peer pool
            pool.clean().await;

            let result = pool.peers().count();
            assert_eq!(1, result, "expected the closed peer to have been removed");
        }
    }

    mod peer_info {
        use super::*;

        macro_rules! peer_info {
            ($addr:expr) => {{
                peer_info!($addr, crate::torrent_peer::PeerPriority::none())
            }};
            ($addr:expr, $rank:expr) => {{
                use crate::peer_pool::PeerInfo;
                use std::net::SocketAddr;

                let addr: SocketAddr = $addr;
                let rank: PeerPriority = $rank;

                PeerInfo {
                    addr,
                    is_in_use: false,
                    is_seed: false,
                    is_banned: false,
                    failure_count: 0,
                    rank: rank,
                    last_connected: None,
                    connection: None,
                }
            }};
            ($addr:expr, $rank:expr, $in_use:expr) => {{
                use crate::peer_pool::PeerInfo;
                use std::net::SocketAddr;

                let addr: SocketAddr = $addr;
                let rank: PeerPriority = $rank;
                let is_in_use: bool = $in_use;

                PeerInfo {
                    addr,
                    is_in_use,
                    is_seed: false,
                    is_banned: false,
                    failure_count: 0,
                    rank,
                    last_connected: None,
                    connection: None,
                }
            }};
        }

        #[test]
        fn test_is_connect_candidate() {
            let peer = peer_info!(([127, 0, 0, 1], 8090).into());
            assert_eq!(
                true,
                peer.is_connect_candidate(),
                "expected the peer to be a candidate"
            );

            let peer = peer_info!(([127, 0, 0, 1], 8090).into(), PeerPriority::none(), true);
            assert_eq!(
                false,
                peer.is_connect_candidate(),
                "expected a in-use peer to not have been a candidate"
            );

            let peer = PeerInfo {
                addr: ([127, 0, 0, 1], 8090).into(),
                is_in_use: false,
                is_seed: false,
                is_banned: true,
                failure_count: 0,
                rank: PeerPriority::none(),
                last_connected: None,
                connection: None,
            };
            assert_eq!(
                false,
                peer.is_connect_candidate(),
                "expected a banned peer to not have been a candidate"
            );
        }

        #[tokio::test]
        async fn test_update_peer_priority() {
            init_logger!();
            let peer_address = SocketAddr::from(([127, 0, 0, 3], 6881));
            let mut pool = PeerPool::new(TorrentHandle::new(), 2);

            // add the peer address to the pool
            pool.add_peer_addresses(vec![peer_address.clone()], None);

            // decrease the peer address priority
            pool.update_peer_rank(&peer_address, -1);
            let mut result = pool.peers.get(&peer_address).cloned().unwrap();
            assert_eq!(Some(0), result.rank.take());

            // increase the peer address priority
            pool.update_peer_rank(&peer_address, 1);
            let mut result = pool.peers.get(&peer_address).cloned().unwrap();
            assert_eq!(Some(1), result.rank.take());
        }

        mod order {
            use super::*;

            #[test]
            fn test_rank() {
                let peer1 = peer_info!(([127, 0, 0, 1], 8090).into(), PeerPriority::from(30));
                let peer2 = peer_info!(([127, 0, 0, 1], 8090).into(), PeerPriority::from(10));

                assert_eq!(Ordering::Less, peer1.cmp(&peer2));
                assert_eq!(Ordering::Greater, peer2.cmp(&peer1));
            }

            #[test]
            fn test_seed() {
                let peer1 = peer_info!(([127, 0, 0, 1], 8090).into());
                let peer2 = PeerInfo {
                    addr: ([127, 0, 0, 1], 8090).into(),
                    is_in_use: false,
                    is_seed: true,
                    is_banned: false,
                    failure_count: 0,
                    rank: PeerPriority::none(),
                    last_connected: None,
                    connection: None,
                };

                assert_eq!(Ordering::Greater, peer1.cmp(&peer2));
                assert_eq!(Ordering::Less, peer2.cmp(&peer1));
            }

            #[test]
            fn test_failure_count() {
                let peer1 = PeerInfo {
                    addr: ([127, 0, 0, 1], 8090).into(),
                    is_in_use: false,
                    is_seed: false,
                    is_banned: false,
                    failure_count: 2,
                    rank: PeerPriority::none(),
                    last_connected: None,
                    connection: None,
                };
                let peer2 = PeerInfo {
                    addr: ([127, 0, 0, 1], 8090).into(),
                    is_in_use: false,
                    is_seed: false,
                    is_banned: false,
                    failure_count: 0,
                    rank: PeerPriority::none(),
                    last_connected: None,
                    connection: None,
                };

                assert_eq!(Ordering::Greater, peer1.cmp(&peer2));
                assert_eq!(Ordering::Less, peer2.cmp(&peer1));
            }
        }
    }
}
