use crate::{overlapping_range, InfoHash};
use crate::{BlockIndex, PieceIndex};
use std::cmp::Ordering;
use std::ops::Range;

/// The priority of a piece.
#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum PiecePriority {
    /// Indicates that there is no interest in this piece and the piece will be ignored
    None = 0,
    Normal = 1,
    High = 2,
    Readahead = 3,
    Next = 4,
    Now = 5,
}

impl PiecePriority {
    /// Returns an iterator over the variants of the enum.
    pub fn iter() -> impl Iterator<Item = Self> {
        [
            Self::None,
            Self::Normal,
            Self::High,
            Self::Readahead,
            Self::Next,
            Self::Now,
        ]
        .into_iter()
    }
}

impl Default for PiecePriority {
    fn default() -> Self {
        Self::Normal
    }
}

impl PartialOrd for PiecePriority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PiecePriority {
    fn cmp(&self, other: &Self) -> Ordering {
        let a = *self as u8;
        let b = *other as u8;

        a.cmp(&b)
    }
}

impl From<u8> for PiecePriority {
    fn from(value: u8) -> Self {
        match value {
            1 => Self::Normal,
            2 => Self::High,
            3 => Self::Readahead,
            4 => Self::Next,
            5 => Self::Now,
            _ => Self::None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Piece {
    /// The hash information of the piece
    pub hash: InfoHash,
    /// The index of the piece
    pub index: PieceIndex,
    /// The offset in bytes of the piece within the torrent
    pub offset: usize,
    /// The piece length in bytes
    pub length: usize,
    /// The priority of this piece
    pub priority: PiecePriority,
    /// The (request) blocks of the piece.
    pub blocks: Vec<PieceBlock>,
    /// The availability of this piece
    pub(crate) availability: u32,
}

impl Piece {
    /// Create a new piece with default priority.
    ///
    /// # Arguments
    ///
    /// * `hash` - The hash information of the piece, this is used to validate the piece data.
    /// * `index` - The index of the piece within the torrent.
    /// * `offset` - The beginning offset of the piece within the torrent.
    /// * `length` - The length of the piece bytes.
    pub fn new(hash: InfoHash, index: PieceIndex, offset: usize, length: usize) -> Self {
        let num_of_blocks = (length + PieceBlock::MAX_LEN - 1) / PieceBlock::MAX_LEN;
        let mut blocks = Vec::with_capacity(num_of_blocks);
        let mut part_offset = 0;

        // create the parts of this piece
        // the parts will represent the requests to peers which need to be made to complete this piece
        for block in 0..num_of_blocks {
            // calculate the part length.
            // if this part is the last one, it might be smaller
            let part_end = (block + 1) * PieceBlock::MAX_LEN;
            let part_length = if part_end > length {
                length - (block * PieceBlock::MAX_LEN)
            } else {
                PieceBlock::MAX_LEN
            };

            blocks.push(PieceBlock {
                piece: index,
                block,
                begin: part_offset,
                length: part_length,
            });

            part_offset += part_length;
        }

        Self {
            hash,
            index,
            offset,
            length,
            priority: PiecePriority::default(),
            blocks,
            availability: 0,
        }
    }

    /// Get the length of this piece in bytes.
    pub fn len(&self) -> usize {
        self.length
    }

    /// Get the known availability of this piece within the torrent peers.
    /// If no connections have been made yet to peers, this might return 0.
    pub fn availability(&self) -> u32 {
        self.availability
    }

    /// Returns the total number of blocks in this piece.
    pub fn num_of_blocks(&self) -> usize {
        self.blocks.len()
    }

    /// Check if the piece contains some bytes from the given torrent byte range.
    ///
    /// # Returns
    ///
    /// It returns `true` when at least 1 byte overlaps with the given range, else `false`.
    pub fn contains(&self, range: &Range<usize>) -> bool {
        let piece_range = self.torrent_range();
        overlapping_range(piece_range, range).is_some()
    }

    /// Get the byte range of the piece within the torrent.
    ///
    /// # Returns
    ///
    /// It returns a `Range<usize>` indicating the piece's position in bytes within the torrent,
    /// starting from its offset and extending to its length.
    pub fn torrent_range(&self) -> Range<usize> {
        self.offset..(self.offset + self.length)
    }
}

/// A data block within a piece.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct PieceBlock {
    /// The piece index of the block.
    pub piece: PieceIndex,
    /// The block index within the piece.
    pub block: BlockIndex,
    /// The offset of bytes where this block begins within the piece.
    pub begin: usize,
    /// The amount of bytes within this block.
    /// This is related to the [PieceBlock::MAX_LEN].
    pub length: usize,
}

impl PieceBlock {
    /// The amount of bytes that can be requested from a peer.
    pub const MAX_LEN: usize = 16 * 1024; // 16 KiB
}

impl PartialOrd<Self> for PieceBlock {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PieceBlock {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.piece != other.piece {
            return self.piece.cmp(&other.piece);
        }

        self.block.cmp(&other.block)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! block {
        ($piece:expr, $block:expr) => {{
            PieceBlock {
                piece: $piece,
                block: $block,
                begin: 0,
                length: 0,
            }
        }};
    }

    #[test]
    fn test_piece_priority_order() {
        let priority = PiecePriority::Normal;
        let result = priority.cmp(&PiecePriority::Normal);
        assert_eq!(Ordering::Equal, result);

        let priority = PiecePriority::Normal;
        let result = priority.cmp(&PiecePriority::None);
        assert_eq!(Ordering::Greater, result);

        let priority = PiecePriority::None;
        let result = priority.cmp(&PiecePriority::Normal);
        assert_eq!(Ordering::Less, result);

        let priority = PiecePriority::High;
        let result = priority.cmp(&PiecePriority::Normal);
        assert_eq!(Ordering::Greater, result);
    }

    #[test]
    fn test_piece_block_order() {
        // different pieces, same block
        let block1 = block!(0, 0);
        let block2 = block!(1, 0);
        assert_eq!(Some(Ordering::Less), block1.partial_cmp(&block2));
        assert_eq!(Some(Ordering::Greater), block2.partial_cmp(&block1));

        // same piece, different blocks
        let block1 = block!(1, 2);
        let block2 = block!(1, 0);
        assert_eq!(Some(Ordering::Greater), block1.partial_cmp(&block2));
        assert_eq!(Some(Ordering::Less), block2.partial_cmp(&block1));
    }
}
