use crate::storage::{Error, Metrics, Result};
use crate::PieceIndex;
use itertools::Itertools;
use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::time::Instant;
use tokio::fs::{File, OpenOptions};
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
///
/// ```text
/// 0         8         16
/// +---------+---------+
/// | piece 0 | piece n |
/// +---------+---------+
/// ```
#[derive(Debug)]
pub struct PartFile {
    file: File,
    piece_len: usize,
    slots: HashMap<SlotIndex, Slot>,
    allocated_slots: HashMap<PieceIndex, SlotIndex>,
    metrics: Metrics,
}

impl PartFile {
    /// Create a new parts file at the given path.
    /// It can hold the `num_of_pieces` within the file.
    pub async fn new(
        path: impl AsRef<Path>,
        piece_len: usize,
        num_of_pieces: usize,
    ) -> Result<Self> {
        Ok(Self {
            file: Self::try_open(path, true).await?,
            piece_len,
            slots: (0..num_of_pieces)
                .into_iter()
                .map(|index| (index, Slot::new(index)))
                .collect(),
            allocated_slots: HashMap::new(),
            metrics: Default::default(),
        })
    }

    /// Open an existing parts file from the given path.
    pub async fn open(path: impl AsRef<Path>, piece_len: usize) -> Result<Self> {
        let file = Self::try_open(path, false);

        todo!()
    }

    /// Returns the metrics reference of the parts file.
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Read data from the parts file into the given buffer for the given piece.
    #[cfg_attr(feature = "tracing", instrument(skip(self, buffer)))]
    pub async fn read(
        &self,
        buffer: &mut [u8],
        piece: &PieceIndex,
        offset: usize,
    ) -> Result<usize> {
        todo!()
    }

    /// Write the given data of the piece into the parts file.
    #[cfg_attr(feature = "tracing", instrument(skip(self, data)))]
    pub async fn write(&self, data: &[u8], piece: &PieceIndex, offset: usize) -> Result<usize> {
        todo!()
    }

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

#[derive(Debug, Copy, Clone)]
struct Slot {
    slot_index: SlotIndex,
    piece_index: PieceIndex,
    last_accessed: Instant,
}

impl Slot {
    fn new(index: SlotIndex) -> Self {
        Self::with_piece(index, 0)
    }

    fn with_piece(slot_index: SlotIndex, piece_index: PieceIndex) -> Self {
        Self {
            slot_index,
            piece_index,
            last_accessed: Instant::now(),
        }
    }
}

impl From<Slot> for [u8; 8] {
    fn from(value: Slot) -> Self {
        let mut buffer = [0u8; 8];
        buffer.copy_from_slice(&value.piece_index.to_le_bytes());
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
