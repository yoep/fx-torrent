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
        let mut index = *piece;
        let mut offset = offset;

        // keep reading consecutive pieces until the buffer is filled,
        // or the next piece is not available within the storage
        while cursor < buffer_len {
            match pieces.get(&index) {
                None => break,
                Some(piece) => {
                    let remaining_bytes = buffer_len.saturating_sub(cursor);
                    let copy_len = min(remaining_bytes, piece.len().saturating_sub(offset));
                    if copy_len == 0 {
                        break;
                    }

                    buffer[cursor..cursor + copy_len]
                        .copy_from_slice(&piece[offset..offset + copy_len]);

                    cursor += copy_len;
                    self.metrics.bytes_read.inc_by(copy_len as u64);
                    index += 1;
                    offset = 0;
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
        let bytes = self.piece_bytes(piece).await;
        let hash = tokio::task::spawn_blocking(move || Sha1::digest(bytes.as_slice()))
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        Sha1Hash::try_from(hash).map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn hash_v2(&self, piece: &PieceIndex) -> Result<Sha256Hash> {
        let bytes = self.piece_bytes(piece).await;
        let hash = tokio::task::spawn_blocking(move || Sha256::digest(bytes.as_slice()))
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        Sha256Hash::try_from(hash).map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    /// Returns a copy of the stored bytes of the given piece.
    async fn piece_bytes(&self, piece: &PieceIndex) -> Vec<u8> {
        let pieces = self.pieces.read().await;
        pieces.get(piece).map(|e| e.to_vec()).unwrap_or_default()
    }

    /// Returns the storage metrics.
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    async fn test_read_across_pieces() {
        init_logger!();
        let storage = MemoryStorage::new();
        storage.write(&[1, 2, 3], &0, 0).await.unwrap();
        storage.write(&[4, 5, 6], &1, 0).await.unwrap();
        storage.write(&[7, 8, 9], &2, 0).await.unwrap();

        // read from the middle of piece 0 up to the middle of piece 2
        let mut buffer = vec![0u8; 6];
        let result = storage
            .read(&mut buffer, &0, 1)
            .await
            .expect("expected the data to be read");

        assert_eq!(6, result, "expected the whole buffer to be read");
        assert_eq!(
            vec![2, 3, 4, 5, 6, 7],
            buffer,
            "expected the data of the consecutive pieces"
        );
    }

    #[tokio::test]
    async fn test_read_stops_at_missing_piece() {
        init_logger!();
        let storage = MemoryStorage::new();
        storage.write(&[1, 2, 3], &0, 0).await.unwrap();
        storage.write(&[7, 8, 9], &2, 0).await.unwrap();

        let mut buffer = vec![0u8; 6];
        let result = storage
            .read(&mut buffer, &0, 1)
            .await
            .expect("expected the data to be read");

        assert_eq!(2, result, "expected the read to stop at the missing piece");
        assert_eq!(&[2, 3], &buffer[..2]);
    }

    #[test]
    fn test_read_offset_beyond_piece() {
        init_logger!();
        let (tx, rx) = std::sync::mpsc::channel();

        // the read runs on a detached thread, as a read which never ends doesn't yield,
        // and would otherwise block the test instead of failing it
        std::thread::spawn(move || {
            let result = futures::executor::block_on(async {
                let storage = MemoryStorage::new();
                storage.write(&[1, 2, 3], &0, 0).await.unwrap();

                let mut buffer = vec![0u8; 2];
                storage.read(&mut buffer, &0, 3).await
            });
            let _ = tx.send(result);
        });
        let result = rx
            .recv_timeout(std::time::Duration::from_millis(500))
            .expect("expected the read to not loop indefinitely")
            .expect("expected the read to succeed");

        assert_eq!(0, result, "expected no data to be read");
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
        let storage = MemoryStorage::new();

        // write the piece data
        let result = storage.write(&data, &piece, 0).await;
        match result {
            Ok(result) => assert_eq!(data.len(), result, "expected the piece data to be written"),
            Err(_) => assert!(false, "expected Ok, but got {:?}", result),
        }

        // create the pieces
        create_pieces_and_files!(&mut context);
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
