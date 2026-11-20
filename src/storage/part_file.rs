use crate::storage::{Error, Metrics, Result};
use crate::PieceIndex;
use itertools::Itertools;
use std::collections::HashMap;
use std::io;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
#[cfg(feature = "tracing")]
use tracing::instrument;

/// The unique index of a parts file slot.
type SlotIndex = usize;

/// Stores pieces which have [crate::PiecePriority::None] within a parts file.
///
/// Each parts file contains a header describing the contents of the file.
/// The header format is as follows:
///
/// ```text
/// 0            8           16
/// +------------+-----------+---------------+------------+
/// | header len | piece len | pieces        | piece data |
/// +------------+-----------+---------------+------------+
/// ```
///
/// The pieces are a list of little endian [u64], where each position represents the slot index.
/// Piece data is stored in fixed-size slots of `piece len` bytes. A slot is assigned to a piece
/// on first write, and the least-recently-used slot is re-assigned when no free slot remains.
///
/// ```text
/// 0         8         16
/// +---------+---------+
/// | piece 0 | piece n |
/// +---------+---------+
/// ```
#[derive(Debug, Clone)]
pub struct PartFile {
    inner: Arc<Mutex<Inner>>,
    metrics: Metrics,
}

impl PartFile {
    /// Create a new parts file at the given path, truncating any existing file.
    /// It can hold `num_of_pieces` pieces of `piece_len` bytes within the file.
    ///
    /// An empty header is written to the file directly.
    pub async fn new(
        path: impl AsRef<Path>,
        piece_len: usize,
        num_of_pieces: usize,
    ) -> Result<Self> {
        let file = Self::try_open(path, true).await?;
        let mut inner = Inner {
            file,
            piece_len,
            slots: (0..num_of_pieces)
                .into_iter()
                .map(|index| (index, Slot::new(index)))
                .collect(),
            allocated_slots: HashMap::new(),
            metrics: Default::default(),
        };
        let metrics = inner.metrics.clone();

        // write an empty header to the part file
        inner.write_header().await?;

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            metrics,
        })
    }

    /// Open an existing parts file from the given path and restore its slot assignments
    /// from the header.
    ///
    /// # Errors
    ///
    /// It returns an error when the file couldn't be opened, the header is invalid,
    /// or the piece length of the header doesn't match the given `piece_len`.
    pub async fn open(path: impl AsRef<Path>, piece_len: usize) -> Result<Self> {
        let mut file = Self::try_open(path, false).await?;
        let header_len = file.read_u64_le().await? as usize;
        let header_piece_len = file.read_u64_le().await? as usize;
        if header_piece_len != piece_len {
            return Err(Error::new(
                io::ErrorKind::InvalidInput,
                "invalid piece-length",
            ));
        }

        // read the header information of the file
        let mut slot_buffer = vec![0u8; header_len];
        file.read_exact(&mut slot_buffer[..]).await?;

        // parse the header into slots
        let slots = Slots::try_from(slot_buffer.as_slice())?;
        let allocated_slots = slots
            .0
            .iter()
            .filter_map(|slot| {
                let piece_index = slot.piece_index.as_ref()?;
                if piece_index == &PieceIndex::MAX {
                    None
                } else {
                    Some((*piece_index, slot.slot_index))
                }
            })
            .collect();

        let inner = Inner {
            file,
            piece_len,
            slots: slots
                .0
                .into_iter()
                .map(|slot| (slot.slot_index, slot))
                .collect(),
            allocated_slots,
            metrics: Default::default(),
        };
        let metrics = inner.metrics.clone();

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            metrics,
        })
    }

    /// Returns the metrics reference of the parts file.
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Read data from the parts file into the given buffer, starting at `offset` within the piece.
    ///
    /// The read continues into subsequent pieces when the buffer exceeds the piece length, and
    /// stops at the first piece which isn't stored within the parts file.
    /// It returns the number of bytes read.
    ///
    /// # Errors
    ///
    /// It returns an error when the offset is greater than the piece length.
    #[cfg_attr(feature = "tracing", instrument(skip(self, buffer)))]
    pub async fn read(
        &self,
        buffer: &mut [u8],
        piece: &PieceIndex,
        offset: usize,
    ) -> Result<usize> {
        self.inner.lock().await.read(buffer, piece, offset).await
    }

    /// Write the given data into the parts file, starting at `offset` within the piece.
    ///
    /// The write continues into subsequent pieces when the data exceeds the piece length.
    /// Slot assignments are persisted in the header of the file.
    /// It returns the number of bytes written.
    #[cfg_attr(feature = "tracing", instrument(skip(self, data)))]
    pub async fn write(&self, data: &[u8], piece: &PieceIndex, offset: usize) -> Result<usize> {
        self.inner.lock().await.write(data, piece, offset).await
    }

    /// Open the file for reading and writing, creating it when it doesn't exist.
    async fn try_open(path: impl AsRef<Path>, truncate: bool) -> Result<File> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(truncate)
            .open(path)
            .await
    }
}

/// The mutable state of the parts file, guarded by a mutex in [PartFile].
#[derive(Debug)]
struct Inner {
    file: File,
    piece_len: usize,
    slots: HashMap<SlotIndex, Slot>,
    allocated_slots: HashMap<PieceIndex, SlotIndex>,
    metrics: Metrics,
}

impl Inner {
    /// Read from the slots of the given piece, continuing into the next pieces for as long as
    /// a slot is allocated for them.
    async fn read(
        &mut self,
        buffer: &mut [u8],
        piece: &PieceIndex,
        mut offset: usize,
    ) -> Result<usize> {
        if offset > self.piece_len {
            return Err(Error::new(
                io::ErrorKind::InvalidInput,
                "offset cannot be greater than piece-length",
            ));
        }

        let mut piece = *piece;
        let mut cursor = 0;
        while cursor < buffer.len() {
            let Some(slot) = self.allocated_slots.get(&piece) else {
                break;
            };

            let starting_offset = self.slot_data_offset(*slot) + offset;
            let read_len = (self.piece_len - offset).min(buffer.len() - cursor);
            self.file
                .seek(io::SeekFrom::Start(starting_offset as u64))
                .await?;
            let bytes_read = self
                .file
                .read(&mut buffer[cursor..cursor + read_len])
                .await?;

            offset = 0;
            piece += 1;
            cursor += bytes_read;
            self.metrics.bytes_read.inc_by(bytes_read as u64);

            if let Some(slot) = self.slots.get_mut(slot) {
                slot.last_accessed = Instant::now();
            }
        }

        Ok(cursor)
    }

    /// Write the data into the slots of the given piece, continuing into the next pieces when
    /// the data exceeds the piece length. A slot is allocated for each piece that doesn't have one yet.
    async fn write(&mut self, data: &[u8], piece: &PieceIndex, mut offset: usize) -> Result<usize> {
        if offset >= self.piece_len {
            return Err(Error::new(
                io::ErrorKind::InvalidInput,
                "offset must be smaller than piece-length",
            ));
        }

        let mut piece = *piece;
        let mut cursor = 0;
        while cursor < data.len() {
            let slot_index = self.allocate_slot(&piece).await?;
            let write_len = (self.piece_len - offset).min(data.len() - cursor);
            let starting_offset = self.slot_data_offset(slot_index) + offset;

            // write the data to the parts file
            self.file
                .seek(io::SeekFrom::Start(starting_offset as u64))
                .await?;
            self.file
                .write_all(&data[cursor..cursor + write_len])
                .await?;

            // update the header information
            let slot_position = Self::slot_range(&slot_index);
            self.file
                .seek(io::SeekFrom::Start(slot_position.start as u64))
                .await?;
            self.file.write_all(piece.to_le_bytes().as_slice()).await?;

            offset = 0;
            piece += 1;
            cursor += write_len;
            self.metrics.bytes_written.inc_by(write_len as u64);

            if let Some(slot) = self.slots.get_mut(&slot_index) {
                slot.last_accessed = Instant::now();
            }
        }

        self.file.flush().await?;
        Ok(cursor)
    }

    /// Returns the allocated slot for the given piece.
    ///
    /// It either returns an already assigned slot, free slot or re-assigned the oldest unused slot.
    async fn allocate_slot(&mut self, piece: &PieceIndex) -> Result<SlotIndex> {
        // check if the piece has already an assigned slot
        if let Some(slot_index) = self.allocated_slots.get(piece) {
            return Ok(*slot_index);
        }

        // assign the piece to the first free slot
        if let Some((slot_index, slot)) = self
            .slots
            .iter_mut()
            .find(|(_, slot)| slot.piece_index.is_none())
        {
            slot.piece_index = Some(*piece);
            self.allocated_slots.insert(*piece, *slot_index);
            return Ok(*slot_index);
        }

        // re-assigned the least recently accessed slot
        self.free_lru_slot()
    }

    async fn write_header(&mut self) -> Result<()> {
        let header_len = self.slots.len() * 8;
        let mut buffer = vec![0u8; header_len + 16];

        // write the length of the header as the first 8th bytes
        buffer[0..8].copy_from_slice(&(header_len as u64).to_le_bytes());
        // write the piece length of the torrent in the second 8th bytes
        buffer[8..16].copy_from_slice(&(self.piece_len as u64).to_le_bytes());
        // write each slot to the header
        for slot_index in 0..self.slots.len() {
            let Some(slot) = self.slots.get(&slot_index) else {
                return Err(Error::new(
                    io::ErrorKind::InvalidData,
                    "missing slot in header",
                ));
            };

            let pos = Self::slot_range(&slot_index);
            buffer[pos].copy_from_slice(&Into::<[u8; 8]>::into(*slot));
        }

        self.file.seek(io::SeekFrom::Start(0)).await?;
        self.file.write_all(&buffer).await?;
        self.file.flush().await?;

        Ok(())
    }

    /// Returns the freed least-recently-used slot index.
    fn free_lru_slot(&mut self) -> Result<SlotIndex> {
        let slot = self
            .slots
            .values_mut()
            .min_by_key(|slot| slot.last_accessed)
            .ok_or(Error::new(io::ErrorKind::Other, "no slots available"))?;

        if let Some(piece_index) = slot.piece_index.take() {
            self.allocated_slots.remove(&piece_index);
        }

        slot.last_accessed = Instant::now();
        Ok(slot.slot_index)
    }

    /// Returns the position of the slot's piece data within the file, which comes after the header.
    fn slot_data_offset(&self, slot_index: SlotIndex) -> usize {
        Self::slot_range(&self.slots.len()).start + (self.piece_len * slot_index)
    }

    /// Returns the position of the slot in the header file.
    fn slot_range(slot_index: &usize) -> Range<usize> {
        let start = (slot_index * 8) + 16;
        start..start + 8
    }
}

/// A fixed-size region within the parts file, which holds the data of at most one piece.
#[derive(Debug, Copy, Clone)]
struct Slot {
    slot_index: SlotIndex,
    piece_index: Option<PieceIndex>,
    last_accessed: Instant,
}

impl Slot {
    /// Create a new empty slot with the given index.
    fn new(slot_index: SlotIndex) -> Self {
        Self {
            slot_index,
            piece_index: None,
            last_accessed: Instant::now(),
        }
    }

    /// Create a new assigned slot with the given index and piece.
    fn with_piece(slot_index: SlotIndex, piece: PieceIndex) -> Self {
        Self {
            slot_index,
            piece_index: Some(piece),
            last_accessed: Instant::now(),
        }
    }
}

impl From<Slot> for [u8; 8] {
    fn from(value: Slot) -> Self {
        let mut buffer = [0u8; 8];
        buffer.copy_from_slice(
            &value
                .piece_index
                .map(|piece| piece as u64)
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        buffer
    }
}

impl PartialEq for Slot {
    fn eq(&self, other: &Self) -> bool {
        self.slot_index == other.slot_index && self.piece_index == other.piece_index
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Slots(Vec<Slot>);

impl TryFrom<&[u8]> for Slots {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self> {
        if value.len() % 8 != 0 {
            return Err(Error::new(io::ErrorKind::InvalidInput, "invalid length"));
        }

        let num_of_slots = value.len() / 8;
        let mut slots = Vec::with_capacity(num_of_slots);

        for slot_index in 0..num_of_slots {
            let bytes: [u8; 8] = value[slot_index * 8..slot_index * 8 + 8]
                .try_into()
                .map_err(|e| Error::new(io::ErrorKind::InvalidInput, e))?;
            let piece_index = u64::from_le_bytes(bytes) as PieceIndex;
            slots.push(Slot::with_piece(slot_index, piece_index));
        }

        Ok(Self(slots))
    }
}

impl TryFrom<Slots> for Vec<u8> {
    type Error = Error;

    fn try_from(value: Slots) -> Result<Self> {
        Ok(value
            .0
            .iter()
            .sorted_by(|a, b| Ord::cmp(&a.slot_index, &b.slot_index))
            .flat_map(|slot| Into::<[u8; 8]>::into(*slot))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    mod part_file {
        use super::*;

        #[tokio::test]
        async fn test_new() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let filename = ".test.parts";
            let piece_len = 1024;

            let result = PartFile::new(temp_dir.path().join(filename), piece_len, 10)
                .await
                .expect("expected the PartFile to have been created");

            let inner = result.inner.lock().await;
            assert_eq!(
                10,
                inner.slots.len(),
                "expected 10 slots to have been allocated"
            );
            assert_eq!(
                piece_len, inner.piece_len,
                "expected the piece length to match"
            );
        }
    }

    mod part_file_write {
        use super::*;

        #[tokio::test]
        async fn test_write_read_across_pieces() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let path = temp_dir.path().join(".test.parts");
            let part_file = PartFile::new(&path, 4, 3).await.unwrap();
            let data = [1u8, 2, 3, 4, 5, 6];

            let written = part_file.write(&data, &7, 0).await.unwrap();
            let mut buffer = [0u8; 6];
            let read = part_file.read(&mut buffer, &7, 0).await.unwrap();

            assert_eq!(6, written);
            assert_eq!(6, read);
            assert_eq!(data, buffer);
        }

        #[tokio::test]
        async fn test_write_persisted_in_header() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let path = temp_dir.path().join(".test.parts");
            let part_file = PartFile::new(&path, 4, 3).await.unwrap();
            part_file.write(&[9u8, 8, 7, 6], &2, 0).await.unwrap();
            drop(part_file);

            let part_file = PartFile::open(&path, 4).await.unwrap();
            let mut buffer = [0u8; 4];
            let read = part_file.read(&mut buffer, &2, 0).await.unwrap();

            assert_eq!(4, read);
            assert_eq!([9u8, 8, 7, 6], buffer);
        }
    }

    mod slots {
        use super::*;

        #[test]
        fn test_try_from_bytes() {
            let bytes = [1u64, 13u64, 7u64]
                .into_iter()
                .flat_map(|i| i.to_le_bytes())
                .collect::<Vec<u8>>();
            let expected_result = Slots(vec![
                Slot::with_piece(0, 1),
                Slot::with_piece(1, 13),
                Slot::with_piece(2, 7),
            ]);

            let result = Slots::try_from(bytes.as_slice()).unwrap();

            assert_eq!(expected_result, result);
        }

        #[test]
        fn test_try_into_bytes() {
            let slots = Slots(vec![
                Slot::with_piece(1, 9),
                Slot::with_piece(0, 66),
                Slot::with_piece(2, 7),
            ]);
            let expected_result = [66u64, 9u64, 7u64]
                .into_iter()
                .flat_map(|i| i.to_le_bytes())
                .collect::<Vec<u8>>();

            let result: Vec<u8> = slots.try_into().unwrap();

            assert_eq!(result.len(), 3 * 8);
            assert_eq!(expected_result, result);
        }
    }
}
