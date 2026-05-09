use crate::storage::{unavailable, Metrics, Result};
use crate::{PieceIndex, Sha1Hash, Sha256Hash};
use sha1::{Digest, Sha1};
use sha2::Sha256;
use std::cmp::min;
use std::collections::BTreeMap;
use std::io;
use tokio::sync::RwLock;
#[cfg(feature = "tracing")]
use tracing::instrument;

/// Fast in-memory storage of torrent piece data.
/// This storage type is not recommended for large torrents.
#[derive(Debug)]
pub struct MemoryStorage {
    pieces: RwLock<BTreeMap<PieceIndex, Vec<u8>>>,
    metrics: Metrics,
}

impl MemoryStorage {
    /// Create a new in-memory storage instance.
    pub fn new() -> Self {
        Self {
            pieces: Default::default(),
            metrics: Default::default(),
        }
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self, buffer)))]
    pub async fn read(
        &self,
        buffer: &mut [u8],
        piece: &PieceIndex,
        offset: usize,
    ) -> Result<usize> {
        let mut cursor = 0usize;
        let buffer_len = buffer.len();
        let pieces = self.pieces.read().await;
        let index = *piece;

        while cursor < buffer_len {
            match pieces.get(&index) {
                None => break,
                Some(piece) => {
                    let remaining_bytes = buffer_len.saturating_sub(cursor);
                    let copy_len = min(remaining_bytes, piece.len().saturating_sub(offset));
                    buffer[cursor..cursor + copy_len]
                        .copy_from_slice(&piece[offset..offset + copy_len]);

                    cursor += copy_len;
                    self.metrics.bytes_read.inc_by(copy_len as u64);
                }
            }
        }

        Ok(cursor)
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self, data)))]
    pub async fn write(&self, data: &[u8], piece: &PieceIndex, offset: usize) -> Result<usize> {
        let mut pieces = self.pieces.write().await;
        let piece = if !pieces.contains_key(&piece) {
            pieces.insert(*piece, vec![0u8; data.len() + offset]);
            pieces.get_mut(&piece)
        } else {
            pieces.get_mut(&piece)
        }
        .ok_or(unavailable())?;

        let end = data.len().saturating_add(offset);
        piece[offset..end].copy_from_slice(data);
        self.metrics.bytes_written.inc_by(data.len() as u64);

        Ok(data.len())
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn hash_v1(&self, piece: &PieceIndex) -> Result<Sha1Hash> {
        let pieces = self.pieces.read().await;
        let bytes = pieces.get(&piece).map(|e| &e[..]).unwrap_or(&[]);

        Sha1Hash::try_from(Sha1::digest(bytes))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn hash_v2(&self, piece: &PieceIndex) -> Result<Sha256Hash> {
        let pieces = self.pieces.read().await;
        let bytes = pieces.get(&piece).map(|e| &e[..]).unwrap_or(&[]);

        Sha256Hash::try_from(Sha256::digest(bytes))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    /// Returns the storage metrics.
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::{CreatePiecesAndFilesOperation, TorrentOperationResult};
    use crate::tests::read_test_file_to_bytes;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_read() {
        init_logger!();
        let piece = 1 as PieceIndex;
        let data = read_test_file_to_bytes("piece-1.iso");
        let storage = MemoryStorage::new();

        // write the piece data
        let result = storage.write(&data, &piece, 0).await;
        match result {
            Ok(result) => assert_eq!(data.len(), result, "expected the piece data to be written"),
            Err(_) => assert!(false, "expected Ok, but got {:?}", result),
        }

        // read the piece data
        let mut buffer = vec![0u8; data.len()];
        let result = storage.read(&mut buffer, &piece, 0).await;
        match result {
            Ok(result) => assert_eq!(data.len(), result, "expected the piece data to be read"),
            Err(_) => assert!(false, "expected Ok, but got {:?}", result),
        }
        assert_eq!(buffer, data, "expected the piece data to match");
    }

    #[tokio::test]
    async fn test_hash_v1() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let piece: PieceIndex = 0;
        let data = read_test_file_to_bytes("piece-1.iso");
        let (mut context, _) = torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().path(temp_path).build(),
            vec![]
        );
        let mut operation = CreatePiecesAndFilesOperation::new();
        let storage = MemoryStorage::new();

        // write the piece data
        let result = storage.write(&data, &piece, 0).await;
        match result {
            Ok(result) => assert_eq!(data.len(), result, "expected the piece data to be written"),
            Err(_) => assert!(false, "expected Ok, but got {:?}", result),
        }

        // create the pieces
        let result = operation.execute(&mut context).await;
        assert_eq!(
            TorrentOperationResult::Continue,
            result,
            "expected the pieces to have been created"
        );
        let piece_hash = context
            .data_pool()
            .piece(&piece)
            .await
            .expect("expected the piece to have been found")
            .hash;
        let expected_hash = piece_hash
            .hash_v1()
            .expect("expected the v1 hash to be present within the piece");

        // hash the piece
        let result = storage
            .hash_v1(&piece)
            .await
            .expect("expected the hash to have been calculated");
        assert_eq!(
            expected_hash, result,
            "expected the hash to equal the piece hash"
        );
    }
}
