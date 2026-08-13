use crate::peer::webseed::HttpPeer;
use crate::peer::{
    BitTorrentPeer, CloseReason, ConnectionDirection, ConnectionProtocol, Metrics, PeerId,
    PeerState,
};
use crate::{BitVec, PieceBlock, PieceIndex};
use async_trait::async_trait;
use crc::{Crc, CRC_32_ISCSI};
use derive_more::Display;
use fx_callback::{Callback, Subscription};
use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

#[derive(Debug, Clone)]
pub enum PeerEvent {
    /// Invoked when the choke state of the client peer has changed.
    ClientChokeStateChanged(ChokeState),
    /// Invoked when the choke state of the remote peer has changed.
    RemoteChokeStateChanged(ChokeState),
    /// Invoked when the state of this peer has changed.
    StateChanged(PeerState),
    /// Invoked when the seed state of the remote peer has changed.
    SeedStateChanged(bool),
    /// Invoked when the peer metrics have been updated.
    Stats(Metrics),
    /// Invoked when the peer connection is closed.
    Closed(CloseReason),
}

/// The choke state of a peer, indicating if data can be sent or not.
/// See BEP3.
#[repr(u8)]
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq)]
pub enum ChokeState {
    #[display("choked")]
    Choked = 0,
    #[display("un-choked")]
    UnChoked = 1,
}

impl PartialOrd for ChokeState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ChokeState {
    fn cmp(&self, other: &Self) -> Ordering {
        if self == other {
            Ordering::Equal
        } else if self == &ChokeState::Choked && other == &ChokeState::UnChoked {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    }
}

/// The interest states of a peer.
#[repr(u8)]
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq)]
pub enum InterestState {
    #[display("not interested")]
    NotInterested = 0,
    #[display("interested")]
    Interested = 1,
}

impl PartialOrd<Self> for InterestState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InterestState {
    fn cmp(&self, other: &Self) -> Ordering {
        if self == other {
            Ordering::Equal
        } else if self == &InterestState::NotInterested && other == &InterestState::Interested {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    }
}

/// The peer connection to a remote peer for exchanging piece data of a specific torrent.
#[derive(Debug, Clone)]
pub enum Peer {
    BitTorrent(BitTorrentPeer),
    Http(HttpPeer),
    Other(Arc<dyn Extension>),
}

impl Peer {
    /// Returns the unique peer identifier within the torrent network.
    pub fn id(&self) -> &PeerId {
        match self {
            Peer::BitTorrent(peer) => peer.id(),
            Peer::Http(peer) => peer.id(),
            Peer::Other(peer) => peer.id(),
        }
    }

    /// Returns the address of the remote peer.
    pub fn addr(&self) -> &SocketAddr {
        match self {
            Peer::BitTorrent(peer) => peer.addr(),
            Peer::Http(peer) => peer.addr(),
            Peer::Other(peer) => peer.addr(),
        }
    }

    /// Returns the connection type of the peer.
    pub fn connection_type(&self) -> &ConnectionDirection {
        match self {
            Peer::BitTorrent(peer) => peer.connection_type(),
            Peer::Http(_) => &ConnectionDirection::Outbound,
            Peer::Other(peer) => peer.connection_type(),
        }
    }

    /// Returns the underlying protocol used by the peer connection.
    pub fn protocol(&self) -> &ConnectionProtocol {
        match self {
            Peer::BitTorrent(peer) => peer.protocol(),
            Peer::Http(_) => &ConnectionProtocol::Http,
            Peer::Other(peer) => peer.protocol(),
        }
    }

    /// Returns the metrics of the peer.
    pub fn metrics(&self) -> &Metrics {
        match self {
            Peer::BitTorrent(peer) => peer.metrics(),
            Peer::Http(peer) => peer.metrics(),
            Peer::Other(peer) => peer.metrics(),
        }
    }

    /// Returns the bitfield of the remote peer.
    /// This bitfield indicates the available pieces of the remote peer (if available).
    pub async fn remote_piece_bitfield(&self) -> BitVec {
        match self {
            Peer::BitTorrent(peer) => peer.remote_piece_bitfield().await,
            Peer::Http(peer) => peer.remote_piece_bitfield().await,
            Peer::Other(peer) => peer.remote_piece_bitfield().await,
        }
    }

    /// Returns the bitfield of the fast pieces for the remote peer.
    ///
    /// This bitfield indicates the pieces which are allowed to be downloaded,
    /// even when [Peer::remote_choke_state] returns [ChokeState::Choked].
    pub async fn remote_fast_bitfield(&self) -> BitVec {
        match self {
            Peer::BitTorrent(peer) => peer.remote_fast_bitfield().await,
            Peer::Http(_) => BitVec::repeat(false, 0),
            Peer::Other(peer) => peer.remote_fast_bitfield().await,
        }
    }

    /// Set the choke state of the client peer.
    pub async fn set_choke_state(&self, state: ChokeState) {
        match self {
            Peer::BitTorrent(peer) => peer.set_choke_state(state).await,
            Peer::Http(_) => {}
            Peer::Other(peer) => peer.set_choke_state(state).await,
        }
    }

    /// Returns the choke state of the client peer,
    /// indicating if data can be sent to the remote peer or not.
    pub async fn choke_state(&self) -> ChokeState {
        match self {
            Peer::BitTorrent(peer) => peer.choke_state().await,
            Peer::Http(_) => ChokeState::Choked,
            Peer::Other(peer) => peer.choke_state().await,
        }
    }

    /// Returns the choke state of the remote peer, indicating if data can be sent or not.
    pub async fn remote_choke_state(&self) -> ChokeState {
        match self {
            Peer::BitTorrent(peer) => peer.remote_choke_state().await,
            Peer::Http(_) => ChokeState::UnChoked,
            Peer::Other(peer) => peer.remote_choke_state().await,
        }
    }

    /// Returns the interest state of the remote peer.
    /// Indicating if the remote peer will start requesting piece data when unchoked.
    pub async fn remote_interest_state(&self) -> InterestState {
        match self {
            Peer::BitTorrent(peer) => peer.remote_interest_state().await,
            Peer::Http(_) => InterestState::NotInterested,
            Peer::Other(peer) => peer.remote_interest_state().await,
        }
    }

    /// Returns the state of the peer.
    pub async fn state(&self) -> PeerState {
        match self {
            Peer::BitTorrent(peer) => peer.state().await,
            Peer::Http(peer) => peer.state().await,
            Peer::Other(peer) => peer.state().await,
        }
    }

    /// Returns `true` if the peer is a seed (having all pieces available).
    pub async fn is_seed(&self) -> bool {
        match self {
            Peer::BitTorrent(peer) => peer.is_seed().await,
            Peer::Http(_) => true,
            Peer::Other(peer) => peer.is_seed().await,
        }
    }

    /// Returns the suggested pieces by the remote peer for downloading.
    pub async fn suggested_pieces(&self) -> Vec<PieceIndex> {
        match self {
            Peer::BitTorrent(peer) => peer.suggested_pieces().await,
            Peer::Http(_) => vec![],
            Peer::Other(peer) => peer.suggested_pieces().await,
        }
    }

    /// Request the given piece blocks to be downloaded from the remote peer.
    pub async fn request(&self, blocks: &[PieceBlock]) {
        match self {
            Peer::BitTorrent(peer) => peer.request(blocks).await,
            Peer::Http(peer) => peer.request(blocks).await,
            Peer::Other(peer) => peer.request(blocks).await,
        }
    }

    /// Returns the target number of requests which should be queued for the remote peer.
    pub async fn target_request_queue_len(&self) -> usize {
        match self {
            Peer::BitTorrent(peer) => peer.target_request_queue_len().await,
            Peer::Http(_) => 100,
            Peer::Other(peer) => peer.target_request_queue_len().await,
        }
    }

    /// Close the peer connection.
    pub async fn close(&self) {
        match self {
            Peer::BitTorrent(peer) => peer.close().await,
            Peer::Http(peer) => peer.close(),
            Peer::Other(peer) => peer.close().await,
        }
    }
}

impl Callback<PeerEvent> for Peer {
    fn subscribe(&self) -> Subscription<PeerEvent> {
        match self {
            Peer::BitTorrent(peer) => peer.subscribe(),
            Peer::Http(peer) => peer.subscribe(),
            Peer::Other(peer) => peer.subscribe(),
        }
    }
}

impl Display for Peer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Peer::BitTorrent(peer) => peer.to_string(),
                Peer::Http(peer) => peer.to_string(),
                Peer::Other(peer) => peer.to_string(),
            }
        )
    }
}

impl From<BitTorrentPeer> for Peer {
    fn from(peer: BitTorrentPeer) -> Self {
        Peer::BitTorrent(peer)
    }
}

impl From<HttpPeer> for Peer {
    fn from(peer: HttpPeer) -> Self {
        Peer::Http(peer)
    }
}

impl<T> From<T> for Peer
where
    T: Extension + 'static,
{
    fn from(peer: T) -> Self {
        Peer::Other(Arc::from(peer))
    }
}

/// The backwards compatible [Extension] trait type for custom peer implementations.
/// Use [Extension] trait instead.
#[deprecated(since = "0.10.0", note = "Use `Extension` instead.")]
pub type TorrentPeer = dyn Extension;

/// The [Extension] is a connection to a remote peer,
/// for exchanging piece data of a specific torrent.
///
/// This custom peer implementation type should not be confused
/// with an extension of the peer protocol.
#[async_trait]
pub trait Extension: Debug + Display + Send + Sync + Callback<PeerEvent> {
    /// Returns the unique peer identifier within the torrent network.
    fn id(&self) -> &PeerId;

    /// Returns the address of the remote peer.
    fn addr(&self) -> &SocketAddr;

    /// Returns the connection type of the peer.
    fn connection_type(&self) -> &ConnectionDirection;

    /// Returns the underlying protocol used by the peer connection.
    fn protocol(&self) -> &ConnectionProtocol;

    /// Returns the metrics of the peer.
    fn metrics(&self) -> &Metrics;

    /// Returns the current state of the peer.
    async fn state(&self) -> PeerState;

    /// Get whether the remote peer is a **seed** (i.e., it has every piece).
    ///
    /// A peer is considered a seed when its remote bitfield is **known** and **all bits are set**.
    /// If the bitfield has not been received yet (e.g., the handshake/bitfield exchange has not
    /// completed), this method returns `false`.
    async fn is_seed(&self) -> bool;

    /// Returns the available pieces of the remote peer as a bit vector.
    ///
    /// It should return an empty bit vector when the handshake has not yet been completed,
    /// else the known [BitVec] of available pieces.
    async fn remote_piece_bitfield(&self) -> BitVec;

    /// Returns the bitfield of the fast pieces for the remote peer.
    ///
    /// This bitfield indicates the pieces which are allowed to be downloaded,
    /// even when [Extension::remote_choke_state] returns [ChokeState::Choked].
    async fn remote_fast_bitfield(&self) -> BitVec;

    /// Set the choke state of the client peer.
    async fn set_choke_state(&self, state: ChokeState);

    /// Returns the choke state of the client peer,
    /// indicating if data can be sent to the remote peer or not.
    async fn choke_state(&self) -> ChokeState;

    /// Returns the choke state of the remote peer, indicating if data can be sent or not.
    ///
    /// Every peer should always start in the [ChokeState::Choked] state.
    async fn remote_choke_state(&self) -> ChokeState;

    /// Returns the interest state of the remote peer.
    /// Indicating if the remote peer will start requesting piece data when unchoked.
    async fn remote_interest_state(&self) -> InterestState;

    /// Returns the suggested pieces by the remote peer for downloading.
    async fn suggested_pieces(&self) -> Vec<PieceIndex>;

    /// Request the given piece blocks to be downloaded from the remote peer.
    ///
    /// ## Remarks
    ///
    /// When a block fails to be requested, use [InnerTorrent::piece_block_rejected] to inform the
    /// torrent about it.
    async fn request(&self, blocks: &[PieceBlock]);

    /// Returns the target number of requests which should be queued for the remote peer.
    async fn target_request_queue_len(&self) -> usize;

    /// Close the peer connection, cancelling any queued operation.
    /// The connection with the remote peer will be closed and this peer can no longer be used.
    async fn close(&self);
}

/// The canonical peer priority calculated by 2 addresses.
/// See BEP40 for more information.
///
/// # Usage
///
/// ```rust,no_run
/// use std::net::SocketAddr;
/// use fx_torrent::torrent::PeerPriority;
///
/// let left: SocketAddr = ([123, 213, 0, 1], 1234).into();
/// let right: SocketAddr = ([230, 32, 123, 23], 300).into();
///
/// PeerPriority::from((&left, &right))
/// ```
///
/// # Explanation
///
/// 1. if the IP addresses are identical, hash the ports in 16 bit network-order
///    binary representation, ordered lowest first.
/// 2. if the IPs are in the same /24, hash the IPs ordered, lowest first.
/// 3. if the IPs are in the ame /16, mask the IPs by 0xffffff55, hash them
///    ordered, lowest first.
/// 4. if IPs are not in the same /16, mask the IPs by 0xffff5555, hash them
///    ordered, lowest first.
#[derive(Debug, Clone, PartialEq)]
pub struct PeerPriority(Option<u32>);

impl PeerPriority {
    /// Create a new peer priority for the given socket addresses.
    ///
    /// The priority might be [None] if the ip versions don't match.
    pub fn new(left: &SocketAddr, right: &SocketAddr) -> Self {
        Self(Self::calculate_from(left, right))
    }

    /// The priority/rank of the peer.
    pub fn priority(&self) -> Option<u32> {
        self.0.clone()
    }

    /// Take the priority/rank of the peer, leaving [None] behind.
    pub fn take(&mut self) -> Option<u32> {
        self.0.take()
    }

    /// Try to calculate the peer priority.
    /// It returns [None] when the ip version doesn't match.
    fn calculate_from(left: &SocketAddr, right: &SocketAddr) -> Option<u32> {
        if left.is_ipv4() != right.is_ipv4() {
            return None; // cannot calculate the peer priority of different ip versions
        }

        if left.ip() == right.ip() {
            let (p1, p2) = if left.port() <= right.port() {
                (left.port().to_be_bytes(), right.port().to_be_bytes())
            } else {
                (right.port().to_be_bytes(), left.port().to_be_bytes())
            };
            return Some(Self::crc32_hash_pair(&p1, &p2));
        } else if left.is_ipv6() {
            let mut bytes = Self::ipv6_octets(left)?;
            let mut other_bytes = Self::ipv6_octets(right)?;

            let mut offset = 0xff;
            for i in 0..16 {
                if offset == 0xff && bytes[i] != other_bytes[i] {
                    offset = std::cmp::max(i + 1, 6);
                } else if i > offset {
                    bytes[i] &= 0x55;
                    other_bytes[i] &= 0x55;
                }
            }

            if left > right {
                return Some(Self::crc32_hash_pair(&other_bytes, &bytes));
            }

            return Some(Self::crc32_hash_pair(&bytes, &other_bytes));
        }

        const V4_MASKS: [[u8; 4]; 3] = [
            [0xff, 0xff, 0x55, 0x55],
            [0xff, 0xff, 0xff, 0x55],
            [0xff, 0xff, 0xff, 0xff],
        ];
        let effective_mask: &[u8; 4];

        let mut bytes = Self::ipv4_octets(left)?;
        let mut other_bytes = Self::ipv4_octets(right)?;

        // if the first 16 bytes don't match, use the default mask FF.FF.55.55,
        // if the first 16 bytes match, but not the first 24 bytes, use the mask FF.FF.FF.55,
        // if the first 24 bytes match, use the mask FF.FF.FF.FF
        if bytes[0..2] != other_bytes[0..2] {
            effective_mask = &V4_MASKS[0];
        } else if bytes[0..3] != other_bytes[0..3] {
            effective_mask = &V4_MASKS[1];
        } else {
            effective_mask = &V4_MASKS[2];
        }

        Self::apply_mask(&mut bytes, effective_mask);
        Self::apply_mask(&mut other_bytes, effective_mask);

        if left > right {
            return Some(Self::crc32_hash_pair(&other_bytes, &bytes));
        }

        Some(Self::crc32_hash_pair(&bytes, &other_bytes))
    }

    /// Create an empty peer priority.
    /// This priority has no underlying value.
    pub fn none() -> Self {
        Self(None)
    }

    /// Get the ipv4 address octets.
    fn ipv4_octets(addr: &SocketAddr) -> Option<[u8; 4]> {
        match addr.ip() {
            IpAddr::V4(addr) => Some(addr.octets()),
            _ => None,
        }
    }

    /// Get the ipv6 address octets.
    fn ipv6_octets(addr: &SocketAddr) -> Option<[u8; 16]> {
        match addr.ip() {
            IpAddr::V6(addr) => Some(addr.octets()),
            _ => None,
        }
    }

    fn apply_mask(bytes: &mut [u8], mask: &[u8]) {
        for (byte, &mask_byte) in bytes.iter_mut().zip(mask.iter()) {
            *byte &= mask_byte;
        }
    }

    fn crc32_hash_pair(left: &[u8], right: &[u8]) -> u32 {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(left);
        buffer.extend_from_slice(right);
        CRC32.checksum(&buffer)
    }
}

impl PartialOrd for PeerPriority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.0.is_none() {
            return Some(Ordering::Greater);
        }
        if other.0.is_none() {
            return Some(Ordering::Less);
        }

        other.0.partial_cmp(&self.0)
    }
}

impl From<(&SocketAddr, &SocketAddr)> for PeerPriority {
    fn from(value: (&SocketAddr, &SocketAddr)) -> Self {
        Self::new(value.0, value.1)
    }
}

impl From<u32> for PeerPriority {
    fn from(value: u32) -> Self {
        Self(Some(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    mod peer {
        use super::*;
        use crate::peer::tests::MockPeer;
        use std::net::Ipv4Addr;

        #[test]
        fn test_id() {
            let id = PeerId::new();
            let mut extension = MockPeer::new();
            extension.expect_id().return_const(id);

            let peer: Peer = extension.into();

            let result = peer.id();
            assert_eq!(&id, result);
        }

        #[test]
        fn test_addr() {
            let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 6881));
            let mut extension = MockPeer::new();
            extension.expect_addr().return_const(addr);

            let peer: Peer = extension.into();

            let result = peer.addr();
            assert_eq!(&addr, result);
        }

        #[test]
        fn test_metrics() {
            let metrics = Metrics::new();
            metrics.bytes_in.inc_by(128);
            let mut extension = MockPeer::new();
            extension.expect_metrics().return_const(metrics);

            let peer: Peer = extension.into();

            let result = peer.metrics();
            assert_eq!(128, result.bytes_in.get());
        }
    }

    mod peer_priority {
        use super::*;
        use std::cmp::Ordering;

        #[test]
        fn test_different_ip_version() {
            let peer1: SocketAddr = ([123, 213, 0, 1], 5000).into();
            let peer2: SocketAddr = (
                [0x20d, 0x20c, 0x20b, 0x20a, 0x209, 0x208, 0x207, 0x206],
                4000,
            )
                .into();

            assert_eq!(None, PeerPriority::from((&peer1, &peer2)).0);
        }

        #[test]
        fn test_compare() {
            let peer1 = PeerPriority(Some(10));
            let peer2 = PeerPriority(Some(20));
            let peer3 = PeerPriority::none();

            // compare some priorities
            assert_eq!(Some(Ordering::Equal), peer1.partial_cmp(&peer1));
            assert_eq!(Some(Ordering::Greater), peer1.partial_cmp(&peer2));
            assert_eq!(Some(Ordering::Less), peer2.partial_cmp(&peer1));

            // compare none priorities
            assert_eq!(Some(Ordering::Greater), peer3.partial_cmp(&peer1));
            assert_eq!(Some(Ordering::Less), peer1.partial_cmp(&peer3));
        }

        #[cfg(test)]
        mod ipv4 {
            use super::*;

            #[test]
            fn test_peer_priority_same_ip_address() {
                let peer1: SocketAddr = ([230, 12, 123, 3], 1234).into();
                let peer2: SocketAddr = ([230, 12, 123, 3], 300).into();

                assert_eq!(
                    hash_buffer("012c04d2"),
                    PeerPriority::from((&peer1, &peer2)).0
                );
            }

            #[test]
            fn test_peer_priority_matching_24_prefix() {
                let peer1: SocketAddr = ([230, 12, 123, 1], 1234).into();
                let peer2: SocketAddr = ([230, 12, 123, 3], 300).into();

                assert_eq!(
                    hash_buffer("e60c7b01e60c7b03"),
                    PeerPriority::from((&peer1, &peer2)).0
                );
            }

            #[test]
            fn test_peer_priority_matching_24_prefix_same_port() {
                let peer1: SocketAddr = ([123, 213, 32, 10], 0).into();
                let peer2: SocketAddr = ([123, 213, 32, 234], 0).into();

                assert_eq!(Some(0x99568189), PeerPriority::from((&peer1, &peer2)).0);
            }

            #[test]
            fn test_peer_priority_matching_16_prefix() {
                let peer1: SocketAddr = ([230, 12, 23, 1], 1234).into();
                let peer2: SocketAddr = ([230, 12, 123, 3], 300).into();

                assert_eq!(
                    hash_buffer("e60c1701e60c7b01"),
                    PeerPriority::from((&peer1, &peer2)).0
                );
            }

            #[test]
            fn test_peer_priority_different_16_prefix() {
                let peer1: SocketAddr = ([230, 120, 23, 1], 1234).into();
                let peer2: SocketAddr = ([230, 12, 123, 3], 300).into();

                assert_eq!(
                    hash_buffer("e60c5101e6781501"),
                    PeerPriority::from((&peer1, &peer2)).0
                );
            }

            #[test]
            fn test_peer_priority_different_16_prefix_same_port() {
                let peer1: SocketAddr = ([123, 213, 32, 10], 0).into();
                let peer2: SocketAddr = ([98, 76, 54, 32], 0).into();

                assert_eq!(Some(0xec2d7224), PeerPriority::from((&peer1, &peer2)).0);
            }
        }

        #[cfg(test)]
        mod ipv6 {
            use super::*;
            use std::net::Ipv6Addr;

            #[test]
            fn test_peer_priority_same_address_different_port() {
                let peer1: SocketAddr = (
                    Ipv6Addr::from_str("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff").unwrap(),
                    1234,
                )
                    .into();
                let peer2: SocketAddr = (
                    Ipv6Addr::from_str("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff").unwrap(),
                    300,
                )
                    .into();

                assert_eq!(
                    hash_buffer("012c04d2"),
                    PeerPriority::from((&peer1, &peer2)).0
                );
                assert_eq!(
                    hash_buffer("012c04d2"),
                    PeerPriority::from((&peer2, &peer1)).0
                ); // order shouldn't matter
            }

            #[test]
            fn test_peer_priority_different_32_prefix() {
                let peer1: SocketAddr = (
                    Ipv6Addr::from_str("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff").unwrap(),
                    1234,
                )
                    .into();
                let peer2: SocketAddr = (
                    Ipv6Addr::from_str("ffff:0fff:ffff:ffff:ffff:ffff:ffff:ffff").unwrap(),
                    300,
                )
                    .into();

                assert_eq!(Some(3916556436), PeerPriority::from((&peer1, &peer2)).0);
            }
        }

        fn hash_buffer(hex: &str) -> Option<u32> {
            if hex.len() % 2 != 0 {
                return None;
            }

            let buffer: Vec<u8> = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
                .collect::<Option<Vec<_>>>()?;
            Some(CRC32.checksum(&buffer))
        }
    }

    mod choke_state {
        use super::*;

        #[test]
        fn test_partial_order() {
            let state1 = ChokeState::Choked;
            let state2 = ChokeState::UnChoked;

            assert_eq!(Some(Ordering::Less), state1.partial_cmp(&state2));
            assert_eq!(Some(Ordering::Greater), state2.partial_cmp(&state1));
            assert_eq!(Some(Ordering::Equal), state2.partial_cmp(&state2));
        }

        #[test]
        fn test_order() {
            let state1 = ChokeState::Choked;
            let state2 = ChokeState::UnChoked;

            assert_eq!(Ordering::Less, state1.cmp(&state2));
            assert_eq!(Ordering::Greater, state2.cmp(&state1));
        }
    }

    mod interest_state {
        use super::*;

        #[test]
        fn test_partial_order() {
            let state1 = InterestState::Interested;
            let state2 = InterestState::NotInterested;

            assert_eq!(Some(Ordering::Less), state2.partial_cmp(&state1));
            assert_eq!(Some(Ordering::Greater), state1.partial_cmp(&state2));
            assert_eq!(Some(Ordering::Equal), state2.partial_cmp(&state2));
            assert_eq!(Some(Ordering::Equal), state1.partial_cmp(&state1));
        }

        #[test]
        fn test_order() {
            let state1 = InterestState::Interested;
            let state2 = InterestState::NotInterested;

            assert_eq!(Ordering::Less, state2.cmp(&state1));
            assert_eq!(Ordering::Greater, state1.cmp(&state2));
        }
    }
}
