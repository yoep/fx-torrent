use crate::InfoHash;
use log::trace;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::time::Instant;

/// Stores peer information received by the DHT network.
#[derive(Debug)]
pub struct PeerStorage {
    peers: HashMap<InfoHash, HashSet<PeerEntry>>,
}

impl PeerStorage {
    pub fn new() -> Self {
        Self {
            peers: Default::default(),
        }
    }

    /// Returns a peers iterator for the given info hash.
    /// The iterator might be empty if no info has yet been received for the [InfoHash].
    pub fn peers(&self, info_hash: &InfoHash) -> impl Iterator<Item = &PeerEntry> {
        self.peers
            .get(info_hash)
            .map(|e| e.iter())
            .unwrap_or_default()
    }

    /// Updates the peer information for the given info hash.
    pub fn update_peer(&mut self, info_hash: InfoHash, addr: SocketAddr, seed: bool) {
        let entry = self.peers.entry(info_hash).or_default();
        entry.insert(PeerEntry::new(addr, seed));
    }

    /// Register a new info hash entry within the storage.
    pub fn register(&mut self, info_hash: &InfoHash) {
        if !self.peers.contains_key(info_hash) {
            self.peers.insert(info_hash.clone(), Default::default());
            trace!("Added info hash {} to the storage", info_hash);
        }
    }

    /// Returns an iterator over all info hashes stored in the storage.
    pub fn info_hashes(&self) -> impl Iterator<Item = &InfoHash> {
        self.peers.keys()
    }
}

#[derive(Debug)]
pub struct PeerEntry {
    pub addr: SocketAddr,
    pub added: Instant,
    pub seed: bool,
}

impl PeerEntry {
    pub fn new(addr: SocketAddr, seed: bool) -> Self {
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
    use crate::init_logger;
    use std::net::Ipv4Addr;
    use std::str::FromStr;

    #[test]
    fn test_update_peer() {
        init_logger!();
        let info_hash =
            InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
        let addr: SocketAddr = (Ipv4Addr::LOCALHOST, 6881).into();
        let mut storage = PeerStorage::new();

        storage.update_peer(info_hash.clone(), addr, false);

        let result = storage.peers(&info_hash).find(|e| e.addr == addr);
        assert!(
            result.is_some(),
            "expected peer {} to be present within the storage",
            addr
        );
    }

    #[test]
    fn test_register() {
        init_logger!();
        let info_hash =
            InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
        let mut storage = PeerStorage::new();

        storage.register(&info_hash);

        let result = storage.info_hashes().cloned().collect::<Vec<_>>();
        assert!(
            result.contains(&info_hash),
            "expected info hash {} to have been present within the storage",
            info_hash
        );
    }
}
