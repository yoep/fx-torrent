use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::time::Instant;

/// The entry information of a peer within the DHT network.
#[derive(Debug, Clone)]
pub struct PeerEntry {
    pub addr: SocketAddr,
    pub added: Instant,
    pub seed: bool,
}

impl PeerEntry {
    /// Create a new peer entry instance.
    pub(crate) fn new(addr: SocketAddr, seed: bool) -> Self {
        Self {
            addr,
            added: Instant::now(),
            seed,
        }
    }
}

impl PartialEq for PeerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.addr == other.addr
    }
}

impl Eq for PeerEntry {}

impl Hash for PeerEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.addr.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partial_eq() {
        let entry1 = PeerEntry::new(([127, 0, 0, 1], 6800).into(), false);
        let entry2 = PeerEntry::new(([127, 0, 0, 1], 6800).into(), true);
        let entry3 = PeerEntry::new(([127, 0, 0, 1], 6900).into(), false);

        assert_eq!(entry1, entry2, "expected the entries to be equal");
        assert_ne!(entry1, entry3, "expected the entries to not be equal");
    }
}
