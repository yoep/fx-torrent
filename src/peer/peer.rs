use crate::peer::webseed::HttpPeer;
use crate::peer::{BitTorrentPeer, Metrics, PeerClientInfo, PeerEvent, PeerState};
use async_trait::async_trait;
use bit_vec::BitVec;
use crc::{Crc, CRC_32_ISCSI};
use fx_callback::{Callback, Subscription};
use fx_handle::Handle;
use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// The peer's unique identifier handle.
pub type PeerHandle = Handle;

/// The peer connection to a remote peer for exchanging piece data of a specific torrent.
#[derive(Debug, Clone)]
pub enum Peer {
    BitTorrent(BitTorrentPeer),
    Http(HttpPeer),
    Other(Arc<dyn TorrentPeer>),
}

impl Peer {
    /// Returns the unique handle of the peer.
    pub fn handle(&self) -> &PeerHandle {
        match self {
            Peer::BitTorrent(peer) => peer.handle(),
            Peer::Http(peer) => peer.handle(),
            Peer::Other(peer) => peer.handle(),
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

    /// Returns the client information of the peer.
    pub fn client_info(&self) -> &PeerClientInfo {
        match self {
            Peer::BitTorrent(peer) => peer.client_info(),
            Peer::Http(peer) => peer.client_info(),
            Peer::Other(peer) => peer.client_info(),
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

    /// Returns the metrics of the peer.
    pub fn metrics(&self) -> &Metrics {
        match self {
            Peer::BitTorrent(peer) => peer.metrics(),
            Peer::Http(peer) => peer.metrics(),
            Peer::Other(peer) => peer.metrics(),
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
    T: TorrentPeer + 'static,
{
    fn from(peer: T) -> Self {
        Peer::Other(Arc::from(peer))
    }
}

/// The [TorrentPeer] is a connection to a remote peer for exchanging piece data of a specific torrent.
#[async_trait]
pub trait TorrentPeer: Debug + Display + Send + Sync + Callback<PeerEvent> {
    /// Returns the unique handle of the peer.
    fn handle(&self) -> &PeerHandle;

    /// Returns the address of the remote peer.
    fn addr(&self) -> &SocketAddr;

    /// Returns the client information of the peer.
    fn client_info(&self) -> &PeerClientInfo;

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

    /// Get the available pieces of the remote peer as a bit vector.
    ///
    /// # Returns
    ///
    /// It returns an empty bit vector when the handshake has not yet been completed, else the known [BitVec] of available pieces.
    async fn remote_piece_bitfield(&self) -> BitVec;

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
}
