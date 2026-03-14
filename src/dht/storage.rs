use crate::dht::{Error, PeerEntry, PublicKey, Result};
use crate::{InfoHash, Sha1Hash};
use ed25519::SignatureBytes;
use log::trace;
use serde_bencode::value::Value;
use sha1::{Digest, Sha1};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::Duration;
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

    /// Returns an iterator over all torrent info hashes stored in the storage.
    pub fn torrents(&self) -> impl Iterator<Item = &InfoHash> {
        self.peers.keys()
    }

    /// Get an item from the storage based on the given sha1 key.
    pub fn get(&self, key: &Sha1Hash) -> Option<&ItemEntry> {
        self.items.get(key)
    }

    /// Store the value item within the DHT storage.
    /// Mutable properties can be provided to allow the value to be updated in the future.
    ///
    /// Returns the hash of the stored item, or the error that occurred.
    pub fn store(
        &mut self,
        value: Value,
        mutable_properties: Option<MutableItemProperties>,
    ) -> Result<Sha1Hash> {
        let key: Sha1Hash = match mutable_properties.as_ref() {
            None => Self::generate_value_key(&value)?,
            Some(properties) => Self::generate_mutable_key(
                properties.public_key.as_slice(),
                properties.salt.as_ref().map(|e| e.as_slice()),
            )?,
        };
        if let Some(item) = self.items.get(&key) {
            // if the mutable properties are not set, this item is immutable
            // otherwise, verify that the sequence number is higher than the current one
            match (
                item.mutable_properties.as_ref(),
                mutable_properties.as_ref(),
            ) {
                (None, _) => return Err(Error::AlreadyExists),
                (Some(existing), Some(updated)) => {
                    if existing.sequence_nr >= updated.sequence_nr {
                        return Err(Error::InvalidSequenceNr);
                    }
                }
                _ => {}
            }
        }

        self.items.insert(
            key.clone(),
            ItemEntry {
                value,
                mutable_properties,
            },
        );
        Ok(key)
    }

    /// Calculate the hash key for the given public key and optional salt.
    pub fn calculate_hash(&self, public_key: &PublicKey, salt: Option<&[u8]>) -> Result<Sha1Hash> {
        Self::generate_mutable_key(public_key.as_ref(), salt)
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

    /// Try to generate the hash key from the given value.
    /// Returns the [Sha1Hash] for the [Value], else an error.
    fn generate_value_key(value: &Value) -> Result<Sha1Hash> {
        serde_bencode::to_bytes(&value)
            .map_err(|e| Error::Parse(e.to_string()))
            .and_then(|bytes| {
                Sha1Hash::try_from(Sha1::digest(bytes.as_slice()))
                    .map_err(|e| Error::Parse(e.to_string()))
            })
    }

    /// Try to generate the hash key from the given `public key` and optional `salt`.
    ///
    /// Returns the [Sha1Hash] for the [MutableItemProperties], else an error.   
    fn generate_mutable_key(public_key: &[u8], salt: Option<&[u8]>) -> Result<Sha1Hash> {
        let mut bytes = public_key.to_vec();
        if let Some(salt) = salt.as_ref() {
            bytes.extend_from_slice(salt);
        }

        Sha1Hash::try_from(Sha1::digest(bytes.as_slice())).map_err(|e| Error::Parse(e.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct ItemEntry {
    pub value: Value,
    pub mutable_properties: Option<MutableItemProperties>,
}

impl PartialEq for ItemEntry {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.mutable_properties == other.mutable_properties
    }
}

#[derive(Debug, Clone)]
pub struct MutableItemProperties {
    pub sequence_nr: u64,
    /// The public key of the item.
    pub public_key: PublicKey,
    /// The salt used to generate the item's hash.
    pub salt: Option<Vec<u8>>,
    /// The validation signature of the item.
    pub signature: SignatureBytes,
}

impl PartialEq for MutableItemProperties {
    fn eq(&self, other: &Self) -> bool {
        self.sequence_nr == other.sequence_nr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init_logger;
    use ed25519::Signature;
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
        use rand::{rng, Rng};

        #[tokio::test]
        async fn test_non_existing() {
            let expected_result = Value::Int(13);
            let mut storage = DhtStorage::new(16);

            let key = storage
                .store(expected_result.clone(), None)
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
                .store(expected_result.clone(), None)
                .expect("expected the item to have been stored");

            // try to store the item a second time
            let result = storage
                .store(expected_result.clone(), None)
                .expect_err("expected the item to be immutable");
            assert_eq!(Error::AlreadyExists, result);
        }

        #[test]
        fn test_existing_mutable_item() {
            let initial_value = Value::Int(69);
            let updated_value = Value::Int(70);
            let mut public_key = PublicKey::default();
            rng().fill_bytes(&mut public_key);
            let initial_properties = MutableItemProperties {
                sequence_nr: 0,
                public_key: public_key.clone(),
                salt: Some("FooBar".as_bytes().to_vec()),
                signature: [0u8; Signature::BYTE_SIZE],
            };
            let updated_properties = MutableItemProperties {
                sequence_nr: 1,
                public_key,
                salt: Some("FooBar".as_bytes().to_vec()),
                signature: [0u8; Signature::BYTE_SIZE],
            };
            let mut storage = DhtStorage::new(16);

            // store the initial value as mutable
            // this should return a key based on the public key
            let key = storage
                .store(initial_value.clone(), Some(initial_properties))
                .expect("expected the item to have been stored");
            let result = storage
                .get(&key)
                .expect("expected the item to be present within the storage");
            assert_eq!(
                initial_value, result.value,
                "expected the initial value match"
            );

            // try to update the mutable item
            let result = storage
                .store(updated_value.clone(), Some(updated_properties))
                .expect("expected the item to have been updated");
            assert_eq!(key, result, "expected the key to be the same");
            let result = storage.get(&key).unwrap();
            assert_eq!(
                updated_value, result.value,
                "expected the value to have been updated"
            );
        }
    }

    mod mutable_properties {
        use super::*;

        #[test]
        fn test_partial_eq() {
            let properties1 = create_properties(0);
            let properties2 = create_properties(0);
            let properties3 = create_properties(1);

            assert_eq!(
                properties1, properties2,
                "expected the mutable properties to be equal"
            );
            assert_ne!(
                properties1, properties3,
                "expected the mutable properties to not be equal"
            );
        }
    }

    mod generate_key {
        use super::*;

        /// BEP44: test 3 (immutable)
        #[test]
        fn test_generate_value_key() {
            let item = "Hello World!";
            let expected_result = Sha1Hash::try_from(
                hex::decode("e5f96f6f38320f0f33959cb4d3d656452117aadb").unwrap(),
            )
            .unwrap();
            let bencode = serde_bencode::to_string(&item.to_string())
                .expect("expected the item to be serialized");
            let value = serde_bencode::from_str::<Value>(&bencode)
                .expect("expected the item to be deserialized");

            let result = DhtStorage::generate_value_key(&value)
                .expect("expected the value key to be generated");

            assert_eq!(expected_result, result);
        }

        /// BEP44: test 2 (mutable with salt)
        #[test]
        fn test_generate_mutable_key() {
            let expected_result = Sha1Hash::try_from(
                hex::decode("411eba73b6f087ca51a3795d9c8c938d365e32c1").unwrap(),
            )
            .unwrap();
            let public_key: PublicKey = PublicKey::try_from(
                hex::decode("77ff84905a91936367c01360803104f92432fcd904a43511876df5cdf3e7e548")
                    .unwrap(),
            )
            .unwrap();
            let salt = b"foobar";

            let result = DhtStorage::generate_mutable_key(&public_key, Some(salt))
                .expect("expected the value key to be generated");

            assert_eq!(expected_result, result);
        }
    }

    mod calculate_hash {
        use super::*;
        use rand::{rng, Rng};

        #[test]
        fn test_without_salt() {
            let mut public_key: PublicKey = PublicKey::default();
            rng().fill_bytes(&mut public_key);
            let expected_result = Sha1Hash::try_from(Sha1::digest(public_key.as_ref())).unwrap();
            let storage = DhtStorage::new(16);

            let result = storage.calculate_hash(&public_key, None).unwrap();

            assert_eq!(expected_result, result);
        }

        #[test]
        fn test_with_salt() {
            let mut public_key: PublicKey = PublicKey::default();
            rng().fill_bytes(&mut public_key);
            let salt = b"FooBar".to_vec();
            let expected_result = Sha1Hash::try_from(Sha1::digest(
                [public_key.as_ref(), salt.as_slice()].concat(),
            ))
            .unwrap();
            let storage = DhtStorage::new(16);

            let result = storage
                .calculate_hash(&public_key, Some(salt.as_ref()))
                .unwrap();

            assert_eq!(expected_result, result);
        }
    }

    #[test]
    fn test_register() {
        init_logger!();
        let info_hash =
            InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
        let mut storage = DhtStorage::new(16);

        storage.register(&info_hash);

        let result = storage.torrents().cloned().collect::<Vec<_>>();
        assert!(
            result.contains(&info_hash),
            "expected info hash {} to have been present within the storage",
            info_hash
        );
    }

    fn create_properties(sequence_nr: u64) -> MutableItemProperties {
        MutableItemProperties {
            sequence_nr,
            public_key: PublicKey::default(),
            salt: None,
            signature: [0u8; Signature::BYTE_SIZE],
        }
    }
}
