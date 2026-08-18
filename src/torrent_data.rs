use crate::{
    BitVec, File, FileAttributeFlags, FileIndex, FilePriority, Piece, PieceBlock, PieceIndex,
    PiecePriority,
};
use itertools::Itertools;
use log::error;
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::RwLock;

/// The data pool of a torrent storing info about pieces and files.
/// It makes use of a separate loop task to handle operations on the data pool.
#[derive(Debug, Clone)]
pub struct DataPool {
    inner: Arc<RwLock<InnerDataPool>>,
}

impl DataPool {
    /// Create a new data pool for storing info about pieces and files.
    pub fn new() -> Self {
        Self::new_with_pieces(Vec::new())
    }

    /// Create a new data pool for the given pieces.
    fn new_with_pieces(pieces: Vec<Piece>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(InnerDataPool::new(pieces))),
        }
    }

    /// Returns the number of pieces within the pool.
    pub async fn num_of_pieces(&self) -> usize {
        self.inner.read().await.num_of_pieces()
    }

    /// Returns the number of files within the pool.
    /// Files with the attribute [FileAttributeFlags::PaddingFile] are not counted.
    pub async fn num_of_files(&self) -> usize {
        self.inner.read().await.num_of_files()
    }

    /// Returns the number of pieces which have been completed.
    pub async fn num_completed_pieces(&self) -> usize {
        self.inner.read().await.num_completed_pieces()
    }

    /// Returns the piece for the given index.
    pub async fn piece(&self, piece: &PieceIndex) -> Option<Piece> {
        self.inner.read().await.pieces.get(piece).cloned()
    }

    /// Returns the file for the given index, if found.
    pub async fn file(&self, file: &FileIndex) -> Option<File> {
        self.inner.read().await.files.get(file).cloned()
    }

    /// Returns the file for the given name, if found.
    pub async fn file_by_name<S: AsRef<str>>(&self, name: S) -> Option<File> {
        self.inner
            .read()
            .await
            .files
            .values()
            .find(|file| file.filename() == name.as_ref())
            .cloned()
    }

    /// Returns all pieces present within the pool.
    pub async fn pieces(&self) -> Vec<Piece> {
        self.inner.read().await.pieces.values().cloned().collect()
    }

    /// Returns all files present within the pool.
    pub async fn files(&self) -> Vec<File> {
        self.inner.read().await.files.values().cloned().collect()
    }

    /// Set the pieces of the pool.
    /// This will replace all existing pieces within the pool.
    pub async fn set_pieces(&self, pieces: Vec<Piece>) {
        self.inner.write().await.set_pieces(pieces);
    }

    /// Set the files of the pool.
    /// This will replace all existing files within the pool.
    pub async fn set_files(&self, files: Vec<File>) {
        self.inner.write().await.set_files(files);
    }

    /// Returns the piece which contains the given torrent offset.
    pub async fn find_piece_at_offset(&self, offset: usize) -> Option<Piece> {
        self.inner.read().await.find_piece_at_offset(offset)
    }

    /// Returns the [PieceBlock] matching the given offset within the piece, if found.
    pub async fn find_piece_block(&self, piece: &PieceIndex, offset: usize) -> Option<PieceBlock> {
        self.inner
            .read()
            .await
            .pieces
            .iter()
            .find(|(idx, _)| *idx == piece)
            .and_then(|(_, piece)| {
                piece
                    .blocks
                    .iter()
                    .find(|part| part.begin == offset)
                    .cloned()
            })
    }

    /// Returns the piece priorities for the torrent.
    pub async fn piece_priorities(&self) -> BTreeMap<PieceIndex, PiecePriority> {
        self.inner.read().await.piece_priorities()
    }

    /// Set the priorities for the given pieces of the torrent.
    pub async fn set_piece_priorities(&self, priorities: &[(PieceIndex, PiecePriority)]) {
        self.inner.write().await.set_piece_priorities(priorities);
    }

    /// Returns `true` if the given piece is present within the pool, else `false`.
    pub async fn contains_piece(&self, piece: &PieceIndex) -> bool {
        self.inner.read().await.pieces.contains_key(piece)
    }

    /// Returns `true` if all wanted pieces have been downloaded and validated, else `false`.
    ///
    /// Every piece with anything but a [PiecePriority::None] has
    /// been downloaded and validated their data in this case.
    pub async fn is_completed(&self) -> bool {
        self.inner.read().await.is_completed()
    }

    /// Returns `true` if the given piece has been downloaded and validated, else `false`.
    pub async fn is_piece_completed(&self, piece: &PieceIndex) -> bool {
        self.inner.read().await.is_piece_completed(piece)
    }

    /// Set the completion state for the given piece slice.
    pub async fn set_completed(&self, pieces: &[PieceIndex], completed: bool) {
        self.inner
            .write()
            .await
            .set_pieces_completion_state(pieces, completed)
    }

    /// Returns `true` if the given torrent byte range is available (downloaded and validated), else `false`.
    pub async fn has_bytes(&self, bytes: Range<usize>) -> bool {
        self.inner.read().await.has_bytes(&bytes)
    }

    /// Returns the piece indexes in which the torrent is interested.
    /// These are the pieces that don't have [PiecePriority::None] as a priority.
    pub async fn interested_pieces(&self) -> Vec<PieceIndex> {
        self.inner.read().await.interested_pieces()
    }

    /// Returns the amount of bytes in which the torrent is interested.
    pub async fn interested_size(&self) -> usize {
        self.inner.read().await.interested_size()
    }

    /// Returns the piece indexes which have completed downloading.
    /// This might include pieces with [PiecePriority::None], if they've been downloaded in the past.
    pub async fn completed_pieces(&self) -> Vec<PieceIndex> {
        self.inner.read().await.completed_pieces()
    }

    /// Returns the amount of bytes which have completed downloading.
    /// This might include pieces with [PiecePriority::None], if they've been downloaded in the past.
    pub async fn completed_size(&self) -> usize {
        self.inner.read().await.completed_size()
    }

    /// Returns `true` if the given piece index is wanted by the torrent and not yet completed, else `false`.
    pub async fn is_piece_wanted(&self, piece_index: &PieceIndex) -> bool {
        self.inner.read().await.is_piece_wanted(piece_index)
    }

    /// Returns the pieces which are still wanted (need to be downloaded) by the torrent.
    /// The list is sorted based on the piece priority.
    pub async fn wanted_pieces(&self) -> Vec<Piece> {
        self.inner.read().await.wanted_pieces()
    }

    /// Modify the availability of the given piece by X peers.
    pub async fn update_availability(&self, piece: &PieceIndex, change: i32) {
        self.inner
            .write()
            .await
            .update_piece_availability(piece, change)
    }

    /// Returns the pieces bitfield, indicating which piece has completed.
    pub async fn bitfield(&self) -> BitVec {
        self.inner.read().await.completed_pieces.clone()
    }

    /// Returns the file index for the starting byte of the given piece.
    pub async fn file_index_for(&self, piece: &PieceIndex) -> Option<FileIndex> {
        self.inner.read().await.file_index_for(piece)
    }

    /// Returns `true` if the torrent is a partial seed.
    /// This means that the torrent has completed some files, but not all files are wanted.
    pub async fn is_partial_seed(&self) -> bool {
        self.inner.read().await.is_partial_seed()
    }
}

impl<S: AsRef<[Piece]>> From<S> for DataPool {
    fn from(value: S) -> Self {
        Self::new_with_pieces(value.as_ref().to_vec())
    }
}

#[derive(Debug)]
struct InnerDataPool {
    completed_pieces: BitVec,
    pieces: BTreeMap<PieceIndex, Piece>,
    files: BTreeMap<FileIndex, File>,
}

impl InnerDataPool {
    fn new(pieces: Vec<Piece>) -> Self {
        Self {
            completed_pieces: BitVec::repeat(false, pieces.len()),
            pieces: pieces
                .into_iter()
                .map(|piece| (piece.index, piece))
                .collect(),
            files: Default::default(),
        }
    }

    fn num_of_pieces(&self) -> usize {
        self.pieces.len()
    }

    fn num_of_files(&self) -> usize {
        self.files
            .iter()
            .filter(|(_, file)| !file.attributes().contains(FileAttributeFlags::PaddingFile))
            .count()
    }

    fn num_completed_pieces(&self) -> usize {
        self.completed_pieces.count_ones()
    }

    fn set_pieces(&mut self, pieces: Vec<Piece>) {
        let pieces_len = pieces.len();
        let pieces = pieces
            .into_iter()
            .map(|piece| (piece.index, piece))
            .collect();

        self.pieces = pieces;
        self.completed_pieces = BitVec::repeat(false, pieces_len);
    }

    fn set_files(&mut self, files: Vec<File>) {
        self.files = files.into_iter().map(|file| (file.index, file)).collect();
    }

    fn find_piece_at_offset(&self, offset: usize) -> Option<Piece> {
        self.pieces
            .iter()
            .find(|(_, piece)| {
                let piece_start = piece.offset;
                let piece_end = piece_start + piece.len();
                offset >= piece_start && offset < piece_end
            })
            .map(|(_, piece)| piece.clone())
    }

    fn piece_priorities(&self) -> BTreeMap<PieceIndex, PiecePriority> {
        self.pieces
            .iter()
            .map(|(index, piece)| (*index, piece.priority.clone()))
            .collect()
    }

    fn set_piece_priorities(&mut self, priorities: &[(PieceIndex, PiecePriority)]) {
        for (index, priority) in priorities {
            if let Some(piece) = self.pieces.get_mut(index) {
                piece.priority = *priority;
            }
        }

        // recalculate the file priorities
        for file in self.files.values_mut() {
            // early skip if the file was not touched by this update
            if !file
                .pieces
                .clone()
                .any(|piece| priorities.iter().any(|(k, _)| k == &piece))
            {
                continue;
            }

            let len = file.pieces.len();
            let highest_priority = match len {
                0 => {
                    // this should never happen unless the torrent is malformed
                    error!("Torrent file \"{}\" is malformed", file.filename());
                    FilePriority::None
                }
                1 => self
                    .pieces
                    .get(&file.pieces.start)
                    .map(|piece| piece.priority)
                    .unwrap_or(FilePriority::None),
                _ => {
                    let exclusive_range = file.pieces.start + 1..file.pieces.end - 1;
                    exclusive_range
                        .into_iter()
                        .map(|piece| {
                            self.pieces
                                .get(&piece)
                                .map(|piece| piece.priority)
                                .unwrap_or(FilePriority::None)
                        })
                        .max()
                        .unwrap_or(FilePriority::None)
                }
            };

            file.priority = highest_priority;
        }
    }

    fn is_piece_completed(&self, piece: &PieceIndex) -> bool {
        self.completed_pieces
            .get(*piece)
            .map(|bit| *bit)
            .unwrap_or_default()
    }

    fn is_completed(&self) -> bool {
        self.pieces
            .iter()
            .filter(|(_, piece)| piece.priority != PiecePriority::None)
            .map(|(index, _)| *index)
            .into_iter()
            .all(|piece| {
                self.completed_pieces
                    .get(piece)
                    .map(|bit| *bit)
                    .unwrap_or(false)
            })
    }

    fn set_pieces_completion_state(&mut self, pieces: &[PieceIndex], completed: bool) {
        for piece in pieces {
            self.completed_pieces.set(*piece, completed);
        }
    }

    fn interested_pieces(&self) -> Vec<PieceIndex> {
        self.pieces
            .iter()
            .filter(|(_, piece)| piece.priority != PiecePriority::None)
            .map(|(index, _)| *index)
            .collect()
    }

    fn interested_size(&self) -> usize {
        self.pieces
            .iter()
            .filter(|(_, piece)| piece.priority != PiecePriority::None)
            .map(|(_, piece)| piece.len())
            .sum()
    }

    fn completed_pieces(&self) -> Vec<PieceIndex> {
        self.completed_pieces
            .iter()
            .enumerate()
            .filter(|(_, completed)| **completed)
            .map(|(index, _)| index)
            .collect()
    }

    fn completed_size(&self) -> usize {
        self.completed_pieces
            .iter()
            .enumerate()
            .filter(|(_, completed)| **completed)
            .filter_map(|(index, _)| self.pieces.get(&index))
            .map(|piece| piece.len())
            .sum()
    }

    fn is_piece_wanted(&self, piece_index: &PieceIndex) -> bool {
        self.pieces
            .get(piece_index)
            .filter(|piece| Self::is_wanted_piece(&self.completed_pieces, piece))
            .is_some()
    }

    fn wanted_pieces(&self) -> Vec<Piece> {
        self.pieces
            .iter()
            .filter(|(_, piece)| piece.priority != PiecePriority::None)
            .map(|(_, piece)| piece)
            .into_iter()
            .filter(|piece| Self::is_wanted_piece(&self.completed_pieces, piece))
            .sorted_by(|a, b| b.priority.cmp(&a.priority))
            .cloned()
            .collect()
    }

    fn update_piece_availability(&mut self, piece: &PieceIndex, change: i32) {
        if let Some(piece) = self.pieces.get_mut(piece) {
            if change >= 0 {
                piece.availability = piece.availability.saturating_add(change as u32);
            } else {
                piece.availability = piece.availability.saturating_sub(change.abs() as u32);
            }
        }
    }

    fn file_index_for(&self, piece: &PieceIndex) -> Option<FileIndex> {
        let piece = self.pieces.get(piece)?;

        self.files
            .iter()
            .find(|(_, file)| {
                let file_start = file.torrent_offset;
                let file_end = file_start + file.len();
                piece.offset >= file_start && piece.offset < file_end
            })
            .map(|(index, _)| *index)
    }

    fn has_bytes(&self, bytes: &Range<usize>) -> bool {
        let piece_len = self.pieces.get(&0).map(|piece| piece.length).unwrap_or(1);
        let start_piece = bytes.start / piece_len;
        let end_piece = (bytes.end - 1) / piece_len;

        (start_piece..=end_piece)
            .into_iter()
            .all(|piece| self.is_piece_completed(&piece))
    }

    /// Check if the piece is wanted by the torrent.
    /// In such a case, the piece priority should not be [PiecePriority::None]
    /// and the piece should not have been completed yet.
    fn is_wanted_piece(bitfield: &BitVec, piece: &Piece) -> bool {
        piece.priority != PiecePriority::None
            && bitfield
                .get(piece.index)
                .map(|bit| *bit)
                .unwrap_or_default()
                == false
    }

    fn is_partial_seed(&self) -> bool {
        // early exit if the torrent is a single-file torrent
        if self.files.len() <= 1 {
            return false;
        }

        let total_pieces = self.pieces.len();
        let wanted_pieces = self.wanted_pieces().len();
        let completed_pieces = self.completed_pieces.count_ones();

        total_pieces != completed_pieces && wanted_pieces == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TorrentFileInfo;
    use std::ops::Range;

    mod is_piece_wanted {
        use super::*;

        #[tokio::test]
        async fn test_piece_completed() {
            let piece = 0;
            let pool = DataPool::from(vec![
                Piece {
                    hash: Default::default(),
                    index: piece,
                    offset: 0,
                    length: 1024,
                    priority: PiecePriority::Normal,
                    blocks: vec![],
                    availability: 0,
                },
                Piece {
                    hash: Default::default(),
                    index: 1,
                    offset: 1024,
                    length: 1024,
                    priority: PiecePriority::None,
                    blocks: vec![],
                    availability: 0,
                },
            ]);

            let result = pool.is_piece_wanted(&piece).await;
            assert_eq!(true, result, "expected the piece to have been wanted");

            // set the piece as completed
            pool.set_completed(&[piece], true).await;

            let result = pool.is_piece_wanted(&piece).await;
            assert_eq!(false, result, "expected the piece to be no longer wanted");
        }

        #[tokio::test]
        async fn test_piece_priority_none() {
            let piece = 1;
            let pool = DataPool::from(vec![
                Piece {
                    hash: Default::default(),
                    index: 0,
                    offset: 0,
                    length: 1024,
                    priority: PiecePriority::Normal,
                    blocks: vec![],
                    availability: 0,
                },
                Piece {
                    hash: Default::default(),
                    index: piece,
                    offset: 1024,
                    length: 1024,
                    priority: PiecePriority::None,
                    blocks: vec![],
                    availability: 0,
                },
            ]);

            let result = pool.is_piece_wanted(&piece).await;
            assert_eq!(false, result, "expected the piece to not have been wanted");

            // set the piece as completed
            pool.set_completed(&[piece], true).await;

            let result = pool.is_piece_wanted(&piece).await;
            assert_eq!(false, result, "expected the piece to not have been wanted");
        }
    }

    mod is_piece_completed {
        use super::*;

        #[tokio::test]
        async fn test_piece_set_completed() {
            let pool = DataPool::from(vec![
                Piece {
                    hash: Default::default(),
                    index: 0,
                    offset: 0,
                    length: 1024,
                    priority: PiecePriority::Normal,
                    blocks: vec![],
                    availability: 0,
                },
                Piece {
                    hash: Default::default(),
                    index: 1,
                    offset: 1024,
                    length: 1024,
                    priority: PiecePriority::Normal,
                    blocks: vec![],
                    availability: 0,
                },
            ]);

            pool.set_completed(&[0], true).await;
            let result = pool.is_completed().await;
            assert_eq!(
                false, result,
                "expected the torrent to not have been completed yet"
            );

            pool.set_completed(&[1], true).await;
            let result = pool.is_completed().await;
            assert_eq!(true, result, "expected the torrent to have been completed");
        }
    }

    mod num_completed_pieces {
        use super::*;

        #[tokio::test]
        async fn test_num_completed_pieces() {
            let pool = DataPool::from(
                (0..5)
                    .into_iter()
                    .map(|index| create_piece(index, 256))
                    .collect::<Vec<_>>(),
            );

            // check that the initial number of completed pieces is 0
            let result = pool.num_completed_pieces().await;
            assert_eq!(
                0, result,
                "expected the initial number of completed pieces to be 0"
            );

            // complete the first 2 pieces
            for index in 0..2 {
                pool.set_completed(&[index], true).await;
            }
            let result = pool.num_completed_pieces().await;
            assert_eq!(2, result, "expected the number of completed pieces to be 2");
        }
    }

    mod pieces {
        use super::*;

        #[tokio::test]
        async fn test_pieces() {
            init_logger!();
            let pieces = vec![
                create_piece(0, 1024),
                create_piece(1, 1024),
                create_piece(2, 1024),
                create_piece(3, 1024),
            ];
            let pool = DataPool::new();

            // set the pieces of the pool
            pool.set_pieces(pieces.clone()).await;

            // retrieve the pieces of the pool
            let result = pool.pieces().await;
            assert_eq!(pieces, result, "expected the pieces to be retrieved");
        }
    }

    mod find_piece_at_offset {
        use super::*;

        #[tokio::test]
        async fn test_find_piece_at_offset() {
            let piece_len = 512;
            let pool = DataPool::from(
                (0..10)
                    .into_iter()
                    .map(|index| create_piece(index, piece_len))
                    .collect::<Vec<_>>(),
            );

            // check the offset of the last byte in the first piece
            let result = pool
                .find_piece_at_offset(511)
                .await
                .expect("expected a piece to be found");
            assert_eq!(
                0, result.index,
                "expected the piece at offset 511 to be piece 0"
            );

            // get the second piece on the first byte
            let result = pool
                .find_piece_at_offset(512)
                .await
                .expect("expected a piece to be found");
            assert_eq!(
                1, result.index,
                "expected the piece at offset 512 to be piece 1"
            );
        }

        #[tokio::test]
        async fn test_out_of_bounds() {
            let piece_len = 512;
            let pool = DataPool::from(
                (0..10)
                    .into_iter()
                    .map(|index| create_piece(index, piece_len))
                    .collect::<Vec<_>>(),
            );

            // retrieve the last piece through the last byte
            let result = pool
                .find_piece_at_offset(5_119)
                .await
                .expect("expected a piece to be found");
            assert_eq!(
                9, result.index,
                "expected the last piece to have been returned"
            );

            // retrieve an out-of-bounds offset
            let result = pool.find_piece_at_offset(5_120).await;
            assert!(
                result.is_none(),
                "expected no piece to be found, but got {:?}",
                result
            );
        }
    }

    mod contains_piece {
        use super::*;

        #[tokio::test]
        async fn test_contains_piece() {
            let pool = DataPool::from(
                (0..3)
                    .into_iter()
                    .map(|index| create_piece(index, 1024))
                    .collect::<Vec<_>>(),
            );

            // check a valid piece index
            let result = pool.contains_piece(&2).await;
            assert_eq!(true, result, "expected the piece to be present in the pool");

            // check an invalid piece index
            let result = pool.contains_piece(&3).await;
            assert_eq!(
                false, result,
                "expected the piece to not be present in the pool"
            );
        }
    }

    mod has_bytes {
        use super::*;

        #[tokio::test]
        async fn test_has_bytes() {
            init_logger!();
            let pool = DataPool::from(vec![
                create_piece(0, 1024),
                create_piece(1, 1024),
                create_piece(2, 1024),
            ]);

            // set pieces to completed state
            pool.set_completed(&[0, 1], true).await;

            // retrieve bytes available
            let result = pool.has_bytes(0..2048).await;
            assert_eq!(true, result, "expected the bytes to be available");

            // retrieve none of the bytes available
            let result = pool.has_bytes(2049..3094).await;
            assert_eq!(false, result, "expected the bytes to not be available");

            // retrieve some bytes available
            let result = pool.has_bytes(2040..2060).await;
            assert_eq!(false, result, "expected the bytes to not be available");
        }
    }

    mod interested {
        use super::*;

        #[tokio::test]
        async fn test_interested_pieces() {
            init_logger!();
            let pieces = vec![
                create_piece(0, 1024),
                create_piece(1, 1024),
                create_piece(2, 1024),
                create_piece(3, 512),
            ];
            let pool = DataPool::new();

            // set the pieces with priorities in the pool
            pool.set_pieces(pieces).await;
            pool.set_piece_priorities(&[(0, PiecePriority::None), (1, PiecePriority::None)])
                .await;

            let result = pool.interested_pieces().await;
            assert_eq!(
                vec![2, 3],
                result,
                "expected the interested pieces to match"
            );
        }

        #[tokio::test]
        async fn test_interested_size() {
            init_logger!();
            let pieces = vec![
                create_piece(0, 1024),
                create_piece(1, 1024),
                create_piece(2, 1024),
                create_piece(3, 512),
            ];
            let pool = DataPool::new();

            // set the pieces with priorities in the pool
            pool.set_pieces(pieces).await;
            pool.set_piece_priorities(&[(0, PiecePriority::None), (1, PiecePriority::None)])
                .await;

            let result = pool.interested_size().await;
            assert_eq!(1_536, result, "expected the interested size to match");
        }
    }

    mod completed {
        use super::*;

        #[tokio::test]
        async fn test_completed_pieces() {
            init_logger!();
            let pieces = vec![
                create_piece(0, 1024),
                create_piece(1, 1024),
                create_piece(2, 1024),
                create_piece(3, 512),
            ];
            let pool = DataPool::new();

            // set the pieces with priorities in the pool
            pool.set_pieces(pieces).await;
            pool.set_completed(&[0, 1], true).await;

            let result = pool.completed_pieces().await;
            assert_eq!(vec![0, 1], result, "expected the completed pieces to match");
        }

        #[tokio::test]
        async fn test_completed_size() {
            init_logger!();
            let pieces = vec![
                create_piece(0, 1024),
                create_piece(1, 1024),
                create_piece(2, 1024),
                create_piece(3, 512),
            ];
            let pool = DataPool::new();

            // set the pieces with priorities in the pool
            pool.set_pieces(pieces).await;
            pool.set_completed(&[1, 3], true).await;

            let result = pool.completed_size().await;
            assert_eq!(1_536, result, "expected the completed size to match");
        }
    }

    mod is_partial_seed {
        use super::*;

        #[tokio::test]
        async fn test_single_file_torrent() {
            init_logger!();
            let pieces = vec![
                create_piece(0, 1024),
                create_piece(1, 1024),
                create_piece(2, 1024),
                create_piece(3, 1024),
            ];
            let pool = DataPool::new();

            // set the pieces and file
            pool.set_pieces(pieces).await;
            pool.set_files(vec![create_file(0, 0, 4096, 0..4)]).await;

            // complete all pieces of the file
            pool.set_completed(&[0, 1, 2, 3], true).await;

            let result = pool.is_partial_seed().await;
            assert_eq!(
                false, result,
                "expected the torrent to not be a partial seed"
            );
        }
        #[tokio::test]
        async fn test_multi_file_torrent() {
            init_logger!();
            let pieces = vec![
                create_piece(0, 1024),
                create_piece(1, 1024),
                create_piece(2, 1024),
                create_piece(3, 1024),
            ];
            let pool = DataPool::new();

            // set the pieces and file
            pool.set_pieces(pieces).await;
            pool.set_files(vec![
                create_file(0, 0, 2048, 0..2),
                create_file(1, 2048, 2048, 2..4),
            ])
            .await;

            // check if partial seed is false, if none of the files have been completed
            let result = pool.is_partial_seed().await;
            assert_eq!(
                false, result,
                "expected the torrent to not be a partial seed"
            );

            // complete all pieces of file 1
            pool.set_completed(&[0, 1], true).await;

            // check if partial seed is false, if none of the files have been completed
            let result = pool.is_partial_seed().await;
            assert_eq!(
                false, result,
                "expected the torrent to not be a partial seed"
            );

            // set file 2 as not wanted
            let priorities = (2..4)
                .map(|piece| (piece, PiecePriority::None))
                .collect_vec();
            pool.set_piece_priorities(&priorities).await;

            // check if partial seed is true, if none of the files have been completed
            let result = pool.is_partial_seed().await;
            assert_eq!(true, result, "expected the torrent to be a partial seed");
        }
    }

    fn create_piece(index: PieceIndex, length: usize) -> Piece {
        Piece {
            hash: Default::default(),
            index,
            offset: index * length,
            length,
            priority: PiecePriority::Normal,
            blocks: vec![],
            availability: 1,
        }
    }

    fn create_file(
        index: FileIndex,
        offset: usize,
        length: usize,
        pieces: Range<PieceIndex>,
    ) -> File {
        File {
            index,
            torrent_path: Default::default(),
            torrent_offset: offset,
            info: TorrentFileInfo {
                length: length as u64,
                path: None,
                path_utf8: None,
                md5sum: None,
                attr: None,
                symlink_path: None,
                sha1: None,
            },
            priority: FilePriority::Normal,
            pieces,
        }
    }
}
