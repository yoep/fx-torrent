use crate::storage::{Result, Storage};
use crate::{PieceBlock, PieceIndex};
use itertools::Itertools;
use log::trace;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Instant;

/// The index key of available slots within the cache buffer.
type SlotIndex = usize;

/// The allocation type for a piece within the cache.
#[derive(Debug, Copy, Clone)]
enum Allocation {
    /// The slot index where this piece is stored.
    Slot(SlotIndex),
    /// The piece has been stored in the storage and is no longer available in the cache buffer.
    Storage,
}

#[derive(Debug)]
pub struct PickerCache {
    total_pieces: usize,
    piece_len: usize,
    last_piece_len: usize,
    /// The allocations for piece data.
    pieces: HashMap<PieceIndex, Allocation>,
    /// The slots allocated within the buffer.
    slots: Vec<CacheSlot>,
    /// The in-memory cache buffer of incomplete data.
    buffer: Vec<u8>,
    /// The storage to use for flushing the cache when the limit is exceeded.
    storage: Arc<Storage>,
}

impl PickerCache {
    /// Create a new cache instance for storing incomplete piece data.
    pub fn new(storage: Arc<Storage>, limit: usize) -> Self {
        Self {
            total_pieces: 0,
            piece_len: 0,
            last_piece_len: 0,
            slots: Default::default(),
            pieces: Default::default(),
            buffer: vec![0u8; limit],
            storage,
        }
    }

    /// Resize the cache.
    /// This replaces any previous allocations and resets the cache state.
    pub fn resize(&mut self, total_pieces: usize, piece_len: usize, last_piece_len: usize) {
        self.total_pieces = total_pieces;
        self.piece_len = piece_len;
        self.last_piece_len = last_piece_len;

        // make sure the buffer is correctly sized to store X pieces
        let buffer_slots = (self.buffer.len() + 1) / self.piece_len;
        self.buffer.resize(buffer_slots * self.piece_len, 0);
        self.slots = (0..buffer_slots)
            .into_iter()
            .map(|i| CacheSlot {
                offset: i * self.piece_len,
                piece: None,
                last_activity: Instant::now(),
            })
            .collect();
        trace!(
            "Picker cache resized to {} pieces, allocated {} bytes to the memory buffer",
            total_pieces,
            self.buffer.len()
        );
    }

    /// Read the data for the given piece from the cache.
    /// The data is either retrieved from in-memory or the underlying storage.
    pub async fn read(&self, piece: &PieceIndex) -> Result<Vec<u8>> {
        match self.pieces.get(piece) {
            None => Err(io::Error::new(io::ErrorKind::NotFound, "piece not found")),
            Some(Allocation::Slot(_)) => self.read_piece_buffer(piece),
            Some(Allocation::Storage) => self.read_piece_storage(piece).await,
        }
    }

    /// Write the piece block data to the cache.
    pub async fn write(&mut self, block: &PieceBlock, data: Vec<u8>) -> Result<()> {
        // early exit if the data doesn't match the expected block length
        if data.len() != block.length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid data length",
            ));
        }
        // early exit if the block is out of bounds
        if block.begin + block.length > self.piece_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "block is out-of-bounds",
            ));
        }

        match self.pieces.get(&block.piece) {
            Some(Allocation::Slot(_)) => return self.write_block_to_buffer(block, data),
            Some(Allocation::Storage) => return self.write_block_to_storage(block, data).await,
            _ => {}
        };

        let slot_index = match self.slots.iter().position(|slot| slot.piece.is_none()) {
            Some(index) => index as SlotIndex,
            None => self.move_lru_to_storage().await?,
        };

        // reserve the free slot for the piece
        {
            let slot = &mut self.slots[slot_index];

            slot.piece = Some(block.piece);
            slot.last_activity = Instant::now();
            self.pieces
                .insert(block.piece, Allocation::Slot(slot_index));
        }

        // write the block data to the reserved buffer slot
        self.write_block_to_buffer(block, data)
    }

    /// Flush the piece data to the underlying [Storage].
    ///
    /// This persists the data within the storage, if it's currently stored in-memory.
    /// Otherwise, this is a no-op.
    pub async fn flush(&mut self, piece: &PieceIndex) -> Result<()> {
        let (slot_index, data) = match self.pieces.get(piece) {
            None => return Err(io::Error::new(io::ErrorKind::NotFound, "piece not found")),
            Some(Allocation::Storage) => return Ok(()), // data is already in storage, nothing to do
            Some(Allocation::Slot(slot_index)) => (*slot_index, self.read_piece_buffer(piece)?),
        };

        self.storage.write(data.as_slice(), piece, 0).await?;
        self.pieces.insert(*piece, Allocation::Storage);
        self.slots[slot_index].piece = None;

        Ok(())
    }

    /// Discard the piece data from the cache, without flushing it to the storage.
    pub fn discard(&mut self, piece: &PieceIndex) {
        match self.pieces.remove(piece) {
            Some(Allocation::Slot(slot_index)) => {
                self.slots[slot_index].piece = None;
            }
            _ => {}
        }
    }

    /// Try to read the data for the given piece index.
    /// Returns the data from the buffer cache if available, else [None].
    fn read_piece_buffer(&self, piece: &PieceIndex) -> Result<Vec<u8>> {
        let slot_index = match self.pieces.get(piece) {
            Some(Allocation::Slot(slot_index)) => *slot_index,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "buffer data not found",
                ))
            }
        };

        let len = self.piece_len(piece);
        self.slots
            .get(slot_index)
            .map(|slot| self.buffer[slot.offset..slot.offset + len].to_vec())
            .ok_or(io::Error::new(
                io::ErrorKind::NotFound,
                "buffer data not found",
            ))
    }

    /// Returns the data for the piece from the storage, if available.
    async fn read_piece_storage(&self, piece: &PieceIndex) -> Result<Vec<u8>> {
        let len = self.piece_len(&piece);
        let mut buffer = vec![0u8; len];

        let read = self.storage.read(&mut buffer, &piece, 0).await?;
        if read != len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected end of piece",
            ));
        }

        Ok(buffer)
    }

    /// Write the data of the block to the cache buffer.
    fn write_block_to_buffer(&mut self, block: &PieceBlock, data: Vec<u8>) -> Result<()> {
        let slot_index = match self.pieces.get(&block.piece) {
            Some(Allocation::Slot(index)) => *index,
            _ => return Ok(()),
        };
        let slot = match self.slots.get_mut(slot_index) {
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "buffer slot not found",
                ))
            }
            Some(slot) => slot,
        };

        let start = slot.offset + block.begin;
        let end = start + block.length;
        self.buffer[start..end].copy_from_slice(&data);
        slot.last_activity = Instant::now();
        Ok(())
    }

    /// Write the piece block data to the storage.
    async fn write_block_to_storage(&mut self, block: &PieceBlock, data: Vec<u8>) -> Result<()> {
        let piece = block.piece;
        let len = self.piece_len(&piece);

        let written = self
            .storage
            .write(data.as_slice(), &piece, block.begin)
            .await?;
        if written != len {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "failed to write piece to storage",
            ));
        }

        Ok(())
    }

    /// Move the **Least Recently Used** piece to the storage.
    /// Returns the slot index of the slot that has been freed up within the cache buffer.
    async fn move_lru_to_storage(&mut self) -> Result<SlotIndex> {
        if self.slots.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "no buffer slots allocated",
            ));
        }

        // move the least recently used piece to the storage
        let (slot_index, piece) = match self
            .slots
            .iter()
            .enumerate()
            .sorted_by(|(_, a), (_, b)| b.last_activity.cmp(&a.last_activity))
            .find(|(_, slot)| slot.piece.is_some())
            .and_then(|(index, slot)| slot.piece.map(|piece| (index, piece)))
        {
            Some((index, piece)) => (index, piece),
            None => unreachable!("slot should have a piece"),
        };

        // move the buffer data of the lru piece to the storage
        let data = self.read_piece_buffer(&piece)?;
        self.storage.write(data.as_slice(), &piece, 0).await?;
        self.pieces.insert(piece, Allocation::Storage);
        self.slots[slot_index].piece = None;

        Ok(slot_index)
    }

    /// Returns the length of the piece.
    fn piece_len(&self, piece: &PieceIndex) -> usize {
        if *piece == self.total_pieces - 1 {
            self.last_piece_len
        } else {
            self.piece_len
        }
    }
}

#[derive(Debug)]
struct CacheSlot {
    /// The offset within the `buffer` this piece entry starts at.
    offset: usize,
    /// The piece this slot is used by, `None` if not in use.
    piece: Option<PieceIndex>,
    /// The last time when data was written to the cache for this piece.
    last_activity: Instant,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rng, RngExt};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_resize() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (context, _rx) = torrent_context!(
            "debian.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::default(),
            vec![],
            vec![],
            None
        );
        let metadata = context.metadata().info.clone().unwrap();
        let mut cache = PickerCache::new(context.storage().clone(), 32 * 1024 * 1024);

        // resize the cache
        let total_pieces = context.metadata().total_pieces().unwrap();
        cache.resize(total_pieces, metadata.piece_length as usize, 1524);

        assert_eq!(total_pieces, cache.total_pieces);
        assert_eq!(metadata.piece_length as usize, cache.piece_len);
        assert_eq!(1524, cache.last_piece_len);
        assert_eq!(33554432, cache.buffer.len());
        assert_eq!(128, cache.slots.len());
    }

    #[tokio::test]
    async fn test_read() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (context, _rx) = torrent_context!(
            "debian.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::default(),
            vec![],
            vec![],
            None
        );
        let mut cache = PickerCache::new(context.storage().clone(), 32 * 1024 * 1024);

        // resize the cache
        let piece_len = 3072;
        let total_pieces = context.metadata().total_pieces().unwrap();
        cache.resize(total_pieces, piece_len, piece_len);

        // generate the piece data
        let mut piece_data = vec![0u8; piece_len];
        rng().fill(&mut piece_data[..]);

        // write all blocks to the cache
        let block_len = piece_len / 3;
        for block in 0..3 {
            let block = PieceBlock {
                piece: 0,
                block,
                begin: block_len * block,
                length: block_len,
            };
            let block_data = piece_data[block.begin..block.begin + block.length].to_vec();
            cache
                .write(&block, block_data)
                .await
                .expect("expected the data to have been written");
        }

        // read the data from the cache
        let result = cache.read(&0).await.unwrap();
        assert_eq!(
            piece_len,
            result.len(),
            "expected the read data to be {} bytes",
            piece_len
        );
        assert_eq!(piece_data, result, "expected the data to match");
    }

    #[tokio::test]
    async fn test_discard() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (context, _rx) = torrent_context!(
            "debian.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::default(),
            vec![],
            vec![],
            None
        );
        let block = PieceBlock {
            piece: 0,
            block: 3,
            begin: 3072,
            length: 1024,
        };
        let metadata = context.metadata().info.clone().unwrap();
        let mut cache = PickerCache::new(context.storage().clone(), 32 * 1024 * 1024);

        // resize the cache
        let total_pieces = context.metadata().total_pieces().unwrap();
        cache.resize(
            total_pieces,
            metadata.piece_length as usize,
            metadata.len() % metadata.piece_length as usize,
        );

        // write data to the cache
        let mut buffer = vec![0u8; block.length];
        rng().fill(&mut buffer[..]);
        cache
            .write(&block, buffer)
            .await
            .expect("expected the data to have been written");
        let slot_index = {
            let slot = cache
                .pieces
                .get(&block.piece)
                .expect("expected the piece to have been stored in the cache");

            match slot {
                Allocation::Slot(index) => {
                    assert_eq!(&0, index, "expected the first slot to have been allocated");
                    *index
                }
                _ => {
                    assert!(false, "expected Allocation::Slot, but got {:?}", slot);
                    0
                }
            }
        };

        // discard the piece from the cache
        cache.discard(&block.piece);
        let slot = cache
            .slots
            .get(slot_index)
            .expect("expected the slot to be in the cache");
        assert_eq!(None, slot.piece, "expected the piece to be discarded");
        let result = cache.pieces.contains_key(&block.piece);
        assert_eq!(false, result, "expected the piece to be discarded");
    }

    mod write {
        use super::*;

        #[tokio::test]
        async fn test_invalid_data_length() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let (context, _rx) = torrent_context!(
                "debian.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::default(),
                vec![],
                vec![],
                None
            );
            let block = PieceBlock {
                piece: 0,
                block: 0,
                begin: 0,
                length: 1024,
            };
            let mut cache = PickerCache::new(context.storage().clone(), 32 * 1024 * 1024);

            // resize the cache
            cache.resize(128, 1024, 1024);

            // write the block data
            let mut data = vec![0u8; 960];
            rng().fill(&mut data[..]);
            let result = cache.write(&block, data).await;
            match result {
                Err(e) => {
                    assert_eq!("invalid data length", e.to_string());
                }
                _ => assert!(false, "expected Err, but got {:?}", result),
            }
        }

        #[tokio::test]
        async fn test_block_out_of_bounds() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let (context, _rx) = torrent_context!(
                "debian.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::default(),
                vec![],
                vec![],
                None
            );
            let block = PieceBlock {
                piece: 0,
                block: 1,
                begin: 512,
                length: 1024,
            };
            let mut cache = PickerCache::new(context.storage().clone(), 32 * 1024 * 1024);

            // resize the cache
            cache.resize(128, 1024, 1024);

            // write the block data
            let mut data = vec![0u8; 1024];
            rng().fill(&mut data[..]);
            let result = cache.write(&block, data).await;
            match result {
                Err(e) => {
                    assert_eq!("block is out-of-bounds", e.to_string());
                }
                _ => assert!(false, "expected Err, but got {:?}", result),
            }
        }

        #[tokio::test]
        async fn test_write_to_storage() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let (context, _rx) = torrent_context!(
                "debian.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::default(),
                vec![],
                vec![],
                None
            );
            let block = PieceBlock {
                piece: 0,
                block: 0,
                begin: 0,
                length: 2048,
            };
            let mut cache = PickerCache::new(context.storage().clone(), 2048);

            // resize the cache
            cache.resize(128, 2048, 2048);

            // fill the memory cache
            let mut data = vec![0u8; 2048];
            rng().fill(&mut data[..]);
            cache
                .write(
                    &PieceBlock {
                        piece: 1,
                        block: 0,
                        begin: 0,
                        length: 2048,
                    },
                    data.clone(),
                )
                .await
                .unwrap();

            // write block data to storage
            let mut data = vec![0u8; 2048];
            rng().fill(&mut data[..]);
            let result = cache.write(&block, data.clone()).await;
            match result {
                Ok(_) => {}
                _ => assert!(false, "expected Ok, but got {:?}", result),
            }

            // read the data again from the storage
            let result = cache.read(&0).await;
            match result {
                Ok(result) => assert_eq!(data, result, "expected the stored data to match"),
                _ => assert!(false, "expected Ok, but got {:?}", result),
            }
        }
    }
}
