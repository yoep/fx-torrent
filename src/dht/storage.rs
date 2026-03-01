use crate::dht::{Error, Result};
use crate::{InfoHash, Sha1Hash};
use log::trace;
use serde_bencode::value::Value;
use sha1::{Digest, Sha1};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
#[cfg(feature = "tracing")]
use tracing::instrument;

/// The time after which a peer entry is considered expired.
const PEER_ENTRY_EXPIRED_AFTER: Duration = Duration::from_mins(30);

/// Stores peer information received by the DHT network.
#[derive(Debug)]
pub struct DhtStorage {
    peers: HashMap<InfoHash, HashSet<PeerEntry>>,
    items: HashMap<Sha1Hash, ItemEntry>,
    /// The total number of torrents to track from the DHT.
    max_torrents: usize,
}

impl DhtStorage {
    /// Creates a new storage instance with the given maximum number of tracked torrents.
    pub fn new(max_torrents: usize) -> Self {
        Self {
            peers: Default::default(),
            items: Default::default(),
            max_torrents,
        }
    }

    /// Returns the total number of peers stored in the storage.
    pub fn peers_len(&self) -> usize {
        self.peers.values().map(|e| e.len()).sum()
    }

    /// Returns the number of torrents stored in the storage.
    pub fn torrents_len(&self) -> usize {
        self.peers.len()
    }

    /// Returns a peers iterator for the given info hash.
    /// The iterator might be empty if no info has yet been received for the [InfoHash].
    pub fn peers(&self, info_hash: &InfoHash) -> impl Iterator<Item = &PeerEntry> {
        self.peers
            .get(info_hash)
            .map(|e| e.iter())
            .unwrap_or_default()
    }

    /// Returns an iterator over all torrents stored in the storage.
    pub fn torrents(&self) -> impl Iterator<Item = &InfoHash> {
        self.peers.keys()
    }

    /// Get an item from the storage based on the given sha1 key.
    pub fn get(&self, key: &Sha1Hash) -> Option<&ItemEntry> {
        self.items.get(key)
    }

    /// Store the given item value within the storage.
    /// It returns the key of the stored item.
    pub fn store(&mut self, value: Value, immutable: bool) -> Result<Sha1Hash> {
        let key = serde_bencode::to_bytes(&value)
            .map_err(|e| Error::Parse(e.to_string()))
            .and_then(|bytes| {
                Sha1Hash::try_from(Sha1::digest(bytes.as_slice()))
                    .map_err(|e| Error::Parse(e.to_string()))
            })?;
        if let Some(item) = self.items.get(&key) {
            if item.immutable {
                return Err(Error::AlreadyExists);
            }
        }

        self.items
            .insert(key.clone(), ItemEntry { value, immutable });
        Ok(key)
    }

    /// Updates the peer information for the given info hash.
    pub fn update_peer(&mut self, info_hash: InfoHash, addr: SocketAddr, seed: bool) {
        if !self.peers.contains_key(&info_hash) && self.torrents_len() >= self.max_torrents {
            trace!("DHT storage is full, ignoring info hash {}", info_hash);
            return;
        }

        let entry = self.peers.entry(info_hash).or_default();
        entry.insert(PeerEntry::new(addr, seed));
    }

    /// Register a new info hash entry within the storage.
    pub fn register(&mut self, info_hash: &InfoHash) {
        if self.torrents_len() >= self.max_torrents {
            trace!("DHT storage is full, ignoring info hash {}", info_hash);
            return;
        }

        if !self.peers.contains_key(info_hash) {
            self.peers.insert(info_hash.clone(), Default::default());
            trace!("Added info hash {} to the storage", info_hash);
        }
    }

    /// Returns an iterator over all info hashes stored in the storage.
    pub fn info_hashes(&self) -> impl Iterator<Item = &InfoHash> {
        self.peers.keys()
    }

    /// Purge old peers within the storage.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    pub fn do_cleanup(&mut self) -> usize {
        let mut removed = 0;
        for entry in self.peers.values_mut() {
            let initial = entry.len();
            entry.retain(|e| e.added.elapsed() <= PEER_ENTRY_EXPIRED_AFTER);
            removed += initial - entry.len();
        }
        removed
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

#[derive(Debug, Clone, PartialEq)]
pub struct ItemEntry {
    pub value: Value,
    pub immutable: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init_logger;
    use std::net::Ipv4Addr;
    use std::str::FromStr;

    mod update_peer {
        use super::*;
        use itertools::Itertools;

        #[test]
        fn test_new_peer() {
            init_logger!();
            let info_hash =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let addr: SocketAddr = (Ipv4Addr::LOCALHOST, 6881).into();
            let mut storage = DhtStorage::new(16);

            storage.update_peer(info_hash.clone(), addr, false);

            let result = storage.peers(&info_hash).find(|e| e.addr == addr);
            assert!(
                result.is_some(),
                "expected peer {} to be present within the storage",
                addr
            );

            let result = storage.peers_len();
            assert_eq!(1, result, "expected the storage to contain one peer");
        }

        #[test]
        fn test_torrent_limit() {
            init_logger!();
            let info_hash1 =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let info_hash2 =
                InfoHash::from_str("urn:btih:2C6B6858D61DA9543D4231A71DB4B1C9264B0685").unwrap();
            let mut storage = DhtStorage::new(1);

            // add the initial info hash
            storage.update_peer(
                info_hash1.clone(),
                (Ipv4Addr::LOCALHOST, 6881).into(),
                false,
            );
            assert_eq!(
                1,
                storage.torrents_len(),
                "expected the torrent to have been added"
            );
            assert_eq!(
                true,
                storage.torrents().contains(&info_hash1),
                "expected the torrent to be present within the storage"
            );

            // try to add the second info hash, which should be ignored
            storage.update_peer(
                info_hash2.clone(),
                (Ipv4Addr::LOCALHOST, 6882).into(),
                false,
            );
            assert_eq!(
                1,
                storage.torrents_len(),
                "expected the torrent to have been ignored"
            );
            assert_eq!(
                false,
                storage.torrents().contains(&info_hash2),
                "expected the torrent to not have been added to the storage"
            );
        }
    }

    mod store {
        use super::*;

        #[tokio::test]
        async fn test_non_existing() {
            let expected_result = Value::Int(13);
            let mut storage = DhtStorage::new(16);

            let key = storage
                .store(expected_result.clone(), true)
                .expect("expected the item to have been stored");

            let result = storage
                .get(&key)
                .expect("expected the item to be present within the storage");
            assert_eq!(expected_result, result.value);
        }

        #[tokio::test]
        async fn test_existing_immutable_item() {
            let expected_result = Value::Int(69);
            let mut storage = DhtStorage::new(16);

            // store the item as immutable
            let _ = storage
                .store(expected_result.clone(), true)
                .expect("expected the item to have been stored");

            // try to store the item a second time
            let result = storage
                .store(expected_result.clone(), false)
                .expect_err("expected the item to be immutable");
            assert_eq!(Error::AlreadyExists, result);
        }
    }

    #[test]
    fn test_register() {
        init_logger!();
        let info_hash =
            InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
        let mut storage = DhtStorage::new(16);

        storage.register(&info_hash);

        let result = storage.info_hashes().cloned().collect::<Vec<_>>();
        assert!(
            result.contains(&info_hash),
            "expected info hash {} to have been present within the storage",
            info_hash
        );
    }
}
