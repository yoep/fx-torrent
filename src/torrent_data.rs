use crate::channel::{ChannelReceiver, ChannelSender, Reply};
use crate::{
    channel, File, FileAttributeFlags, FileIndex, FilePriority, PartIndex, Piece, PieceIndex,
    PiecePriority,
};
use bit_vec::BitVec;
use itertools::Itertools;
use log::warn;
use std::collections::BTreeMap;

/// The data pool of a torrent storing info about pieces and files.
///
/// # Example
///
/// ```rust,no_run
/// let pool = DataPool::new();
/// pool.num_of_pieces().await;
/// ```
#[derive(Debug, Clone)]
pub struct DataPool {
    sender: ChannelSender<DataPoolCommand>,
}

impl DataPool {
    /// Create a new data pool for storing info about pieces and files.
    pub fn new() -> Self {
        Self::new_with_pieces(Vec::new())
    }

    /// Create a new data pool for the given pieces.
    fn new_with_pieces(pieces: Vec<Piece>) -> Self {
        let (sender, rx) = channel!(256);
        tokio::spawn(async move {
            let mut inner = InnerDataPool::new(pieces);
            inner.start(rx).await;
        });

        Self { sender }
    }

    /// Returns the number of pieces within the pool.
    pub async fn num_of_pieces(&self) -> usize {
        let rx = self
            .sender
            .send(|tx| DataPoolCommand::NumOfPieces { response: tx })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Returns the number of files within the pool.
    /// Files with the attribute [FileAttributeFlags::PaddingFile] are not counted.
    pub async fn num_of_files(&self) -> usize {
        let rx = self
            .sender
            .send(|tx| DataPoolCommand::NumOfFiles { response: tx })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Returns the number of pieces which have been completed.
    pub async fn num_completed_pieces(&self) -> usize {
        let rx = self
            .sender
            .send(|tx| DataPoolCommand::NumCompletedPieces { response: tx })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Returns the piece for the given index.
    pub async fn piece(&self, piece: &PieceIndex) -> Option<Piece> {
        let rx = self
            .sender
            .send(|tx| DataPoolCommand::GetPiece {
                index: *piece,
                response: tx,
            })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Returns the file for the given index.
    pub async fn file(&self, file: &FileIndex) -> Option<File> {
        let rx = self
            .sender
            .send(|tx| DataPoolCommand::GetFile {
                index: *file,
                response: tx,
            })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Returns all pieces present within the pool.
    pub async fn pieces(&self) -> Vec<Piece> {
        let rx = self
            .sender
            .send(|tx| DataPoolCommand::GetPieces { response: tx })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Returns all files present within the pool.
    pub async fn files(&self) -> Vec<File> {
        let rx = self
            .sender
            .send(|tx| DataPoolCommand::GetFiles { response: tx })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Set the pieces of the pool.
    /// This will replace all existing pieces within the pool.
    pub async fn set_pieces(&self, pieces: Vec<Piece>) {
        self.sender
            .fire_and_forget(DataPoolCommand::SetPieces { pieces })
            .await;
    }

    /// Set the files of the pool.
    /// This will replace all existing files within the pool.
    pub async fn set_files(&self, files: Vec<File>) {
        self.sender
            .fire_and_forget(DataPoolCommand::SetFiles { files })
            .await;
    }

    /// Returns the piece which contains the given torrent offset.
    pub async fn find_piece_at_offset(&self, offset: usize) -> Option<Piece> {
        let rx = self
            .sender
            .send(|tx| DataPoolCommand::FindPieceAtOffset {
                offset,
                response: tx,
            })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Returns the piece priorities for the torrent.
    pub async fn piece_priorities(&self) -> BTreeMap<PieceIndex, PiecePriority> {
        let rx = self
            .sender
            .send(|tx| DataPoolCommand::PiecePriorities { response: tx })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Set the priorities for the given pieces if the torrent.
    pub async fn set_piece_priorities(&self, priorities: &[(PieceIndex, PiecePriority)]) {
        self.sender
            .fire_and_forget(DataPoolCommand::SetPiecePriorities {
                priorities: priorities.to_vec(),
            })
            .await;
    }

    pub async fn set_file_priorities(&self, priorities: &[(FileIndex, FilePriority)]) {
        self.sender
            .fire_and_forget(DataPoolCommand::SetFilePriorities {
                priorities: priorities.to_vec(),
            })
            .await;
    }

    /// Returns `true` if the given piece is present within the pool, else `false`.
    pub async fn contains_piece(&self, piece: &PieceIndex) -> bool {
        let rx = self
            .sender
            .send(|tx| DataPoolCommand::ContainsPiece {
                index: *piece,
                response: tx,
            })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Check if the torrent has downloaded all wanted pieces.
    /// This means that every piece with anything but a [PiecePriority::None] has
    /// been downloaded and validated their data.
    pub async fn is_completed(&self) -> bool {
        let rx = self
            .sender
            .send(|tx| DataPoolCommand::IsCompleted { response: tx })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Returns `true` if the given piece has completed downloading, else `false`.
    pub async fn is_piece_completed(&self, piece: &PieceIndex) -> bool {
        let rx = self
            .sender
            .send(|tx| DataPoolCommand::IsPieceCompleted {
                index: *piece,
                response: tx,
            })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Set the completion state of the given piece index.
    pub async fn set_completed(&self, piece: &PieceIndex, completed: bool) {
        self.sender
            .fire_and_forget(DataPoolCommand::SetPieceCompleted {
                index: *piece,
                completed,
            })
            .await;
    }

    /// Set the given piece part as completed.
    pub async fn set_part_completed(&self, piece: &PieceIndex, part: &PartIndex) {
        self.sender
            .fire_and_forget(DataPoolCommand::SetPiecePartCompleted {
                piece: *piece,
                part: *part,
            })
            .await
    }

    /// Returns the piece indexes in which the torrent is interested.
    /// These are the pieces that don't have [PiecePriority::None] as a priority.
    pub async fn interested_pieces(&self) -> Vec<PieceIndex> {
        let rx = self
            .sender
            .send(|tx| DataPoolCommand::InterestedPieces { response: tx })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Returns the amount of bytes in which the torrent is interested.
    pub async fn interested_size(&self) -> usize {
        let rx = self
            .sender
            .send(|tx| DataPoolCommand::InterestedSize { response: tx })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Returns `true` if the given piece index is wanted by the torrent and not yet completed, else `false`.
    pub async fn is_piece_wanted(&self, piece_index: &PieceIndex) -> bool {
        let rx = self
            .sender
            .send(|tx| DataPoolCommand::IsPieceWanted {
                index: *piece_index,
                response: tx,
            })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Returns the pieces which are still wanted (need to be downloaded) by the torrent.
    /// The list is sorted based on the piece priority.
    pub async fn wanted_pieces(&self) -> Vec<Piece> {
        let rx = self
            .sender
            .send(|tx| DataPoolCommand::WantedPieces { response: tx })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Modify the availability of the given piece by X peers.
    pub async fn update_availability(&self, piece: &PieceIndex, change: i32) {
        self.sender
            .fire_and_forget(DataPoolCommand::UpdatePieceAvailability {
                index: *piece,
                change,
            })
            .await;
    }

    /// Returns the pieces bitfield, indicating which piece has completed.
    pub async fn bitfield(&self) -> BitVec {
        let rx = self
            .sender
            .send(|tx| DataPoolCommand::Bitfield { response: tx })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Returns the file index for the starting byte of the given piece.
    pub async fn file_index_for(&self, piece: &PieceIndex) -> Option<FileIndex> {
        let rx = self
            .sender
            .send(|tx| DataPoolCommand::FileIndexFor {
                piece: *piece,
                response: tx,
            })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Close the data pool.
    /// This terminates the pool and prevents any further operations.
    pub async fn close(&self) {
        self.sender.fire_and_forget(DataPoolCommand::Close).await;
    }
}

impl<S: AsRef<[Piece]>> From<S> for DataPool {
    fn from(value: S) -> Self {
        Self::new_with_pieces(value.as_ref().to_vec())
    }
}

#[derive(Debug)]
enum DataPoolCommand {
    NumOfPieces {
        response: Reply<usize>,
    },
    NumOfFiles {
        response: Reply<usize>,
    },
    NumCompletedPieces {
        response: Reply<usize>,
    },
    GetPiece {
        index: PieceIndex,
        response: Reply<Option<Piece>>,
    },
    GetFile {
        index: FileIndex,
        response: Reply<Option<File>>,
    },
    GetPieces {
        response: Reply<Vec<Piece>>,
    },
    GetFiles {
        response: Reply<Vec<File>>,
    },
    SetPieces {
        pieces: Vec<Piece>,
    },
    SetFiles {
        files: Vec<File>,
    },
    FindPieceAtOffset {
        offset: usize,
        response: Reply<Option<Piece>>,
    },
    PiecePriorities {
        response: Reply<BTreeMap<PieceIndex, PiecePriority>>,
    },
    SetPiecePriorities {
        priorities: Vec<(PieceIndex, PiecePriority)>,
    },
    SetFilePriorities {
        priorities: Vec<(FileIndex, FilePriority)>,
    },
    ContainsPiece {
        index: PieceIndex,
        response: Reply<bool>,
    },
    IsPieceCompleted {
        index: PieceIndex,
        response: Reply<bool>,
    },
    SetPieceCompleted {
        index: PieceIndex,
        completed: bool,
    },
    SetPiecePartCompleted {
        piece: PieceIndex,
        part: PartIndex,
    },
    InterestedPieces {
        response: Reply<Vec<PieceIndex>>,
    },
    InterestedSize {
        response: Reply<usize>,
    },
    IsPieceWanted {
        index: PieceIndex,
        response: Reply<bool>,
    },
    WantedPieces {
        response: Reply<Vec<Piece>>,
    },
    UpdatePieceAvailability {
        index: PieceIndex,
        change: i32,
    },
    Bitfield {
        response: Reply<BitVec>,
    },
    IsCompleted {
        response: Reply<bool>,
    },
    FileIndexFor {
        piece: PieceIndex,
        response: Reply<Option<FileIndex>>,
    },
    Close,
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
            completed_pieces: BitVec::from_elem(pieces.len(), false),
            pieces: pieces
                .into_iter()
                .map(|piece| (piece.index, piece))
                .collect(),
            files: Default::default(),
        }
    }

    async fn start(&mut self, mut receiver: ChannelReceiver<DataPoolCommand>) {
        while let Some(command) = receiver.recv().await {
            match command {
                DataPoolCommand::NumOfPieces { response } => {
                    response.send(self.pieces.len());
                }
                DataPoolCommand::NumOfFiles { response } => {
                    response.send(self.num_of_files());
                }
                DataPoolCommand::NumCompletedPieces { response } => {
                    response.send(self.completed_pieces.count_ones() as usize);
                }
                DataPoolCommand::GetPiece { index, response } => {
                    response.send(self.pieces.get(&index).cloned());
                }
                DataPoolCommand::GetFile { index, response } => {
                    response.send(self.files.get(&index).cloned());
                }
                DataPoolCommand::GetPieces { response } => {
                    response.send(self.pieces.values().cloned().collect());
                }
                DataPoolCommand::GetFiles { response } => {
                    response.send(self.files.values().cloned().collect());
                }
                DataPoolCommand::SetPieces { pieces } => {
                    self.set_pieces(pieces);
                }
                DataPoolCommand::SetFiles { files } => {
                    self.set_files(files);
                }
                DataPoolCommand::FindPieceAtOffset { offset, response } => {
                    response.send(self.find_piece_at_offset(offset));
                }
                DataPoolCommand::PiecePriorities { response } => {
                    response.send(self.piece_priorities());
                }
                DataPoolCommand::SetPiecePriorities { priorities } => {
                    self.set_piece_priorities(priorities);
                }
                DataPoolCommand::SetFilePriorities { priorities } => {
                    self.set_file_priorities(priorities);
                }
                DataPoolCommand::ContainsPiece { index, response } => {
                    response.send(self.pieces.contains_key(&index));
                }
                DataPoolCommand::IsPieceCompleted { index, response } => {
                    response.send(self.is_piece_completed(&index));
                }
                DataPoolCommand::SetPieceCompleted { index, completed } => {
                    self.set_piece_completed(&index, completed);
                }
                DataPoolCommand::SetPiecePartCompleted { piece: index, part } => {
                    self.set_part_completed(&index, &part);
                }
                DataPoolCommand::InterestedPieces { response } => {
                    response.send(self.interested_pieces());
                }
                DataPoolCommand::InterestedSize { response } => {
                    response.send(self.interested_size());
                }
                DataPoolCommand::IsPieceWanted { index, response } => {
                    response.send(self.is_piece_wanted(&index));
                }
                DataPoolCommand::WantedPieces { response } => {
                    response.send(self.wanted_pieces());
                }
                DataPoolCommand::UpdatePieceAvailability { index, change } => {
                    self.update_piece_availability(&index, change);
                }
                DataPoolCommand::Bitfield { response } => {
                    response.send(self.completed_pieces.clone());
                }
                DataPoolCommand::IsCompleted { response } => {
                    response.send(self.is_completed());
                }
                DataPoolCommand::FileIndexFor {
                    piece: index,
                    response,
                } => {
                    response.send(self.file_index_for(&index));
                }
                DataPoolCommand::Close => break,
            }
        }
    }

    fn num_of_files(&mut self) -> usize {
        self.files
            .iter()
            .filter(|(_, file)| !file.attributes().contains(FileAttributeFlags::PaddingFile))
            .count()
    }

    fn set_pieces(&mut self, pieces: Vec<Piece>) {
        let pieces_len = pieces.len();
        let pieces = pieces
            .into_iter()
            .map(|piece| (piece.index, piece))
            .collect();

        self.pieces = pieces;
        self.completed_pieces = BitVec::from_elem(pieces_len, false);
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

    fn set_piece_priorities(&mut self, priorities: Vec<(PieceIndex, PiecePriority)>) {
        for (index, priority) in &priorities {
            if let Some(piece) = self.pieces.get_mut(index) {
                piece.priority = *priority;
            }
        }
    }

    fn set_file_priorities(&mut self, priorities: Vec<(FileIndex, FilePriority)>) {
        let mut piece_priorities = BTreeMap::new();

        for (index, file_priority) in priorities {
            if let Some(file) = self.files.get_mut(&index) {
                file.priority = file_priority;

                for piece in file.pieces.clone() {
                    let piece_priority = piece_priorities
                        .entry(piece)
                        .or_insert(file_priority as PiecePriority);
                    *piece_priority = (*piece_priority).max(file_priority);
                }
            }
        }

        self.set_piece_priorities(
            piece_priorities
                .into_iter()
                .map(|(k, v)| (k, v))
                .collect::<Vec<_>>(),
        );
    }

    fn is_piece_completed(&self, piece: &PieceIndex) -> bool {
        self.completed_pieces.get(*piece).unwrap_or_default()
    }

    fn is_completed(&self) -> bool {
        self.pieces
            .iter()
            .filter(|(_, piece)| piece.priority != PiecePriority::None)
            .map(|(index, _)| *index)
            .into_iter()
            .all(|piece| self.completed_pieces.get(piece).unwrap_or(false))
    }

    fn set_piece_completed(&mut self, piece: &PieceIndex, completed: bool) {
        if let Some(piece) = self.pieces.get_mut(piece) {
            if completed {
                piece.mark_completed();
            } else {
                piece.reset_completed_parts();
            }
        }

        self.completed_pieces.set(*piece, completed)
    }

    fn set_part_completed(&mut self, piece_index: &PieceIndex, part: &PartIndex) {
        if let Some(piece) = self.pieces.get_mut(piece_index) {
            piece.part_completed(part);
            if piece.is_completed() {
                self.completed_pieces.set(*piece_index, true);
            }
        } else {
            warn!(
                "Data pool got piece part completed for unknown piece {}",
                piece_index
            );
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

    /// Check if the piece is wanted by the torrent.
    /// In such a case, the piece priority should not be [PiecePriority::None]
    /// and the piece should not have been completed yet.
    fn is_wanted_piece(bitfield: &BitVec, piece: &Piece) -> bool {
        piece.priority != PiecePriority::None
            && bitfield.get(piece.index).unwrap_or_default() == false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                    parts: vec![],
                    completed_parts: Default::default(),
                    availability: 0,
                },
                Piece {
                    hash: Default::default(),
                    index: 1,
                    offset: 1024,
                    length: 1024,
                    priority: PiecePriority::None,
                    parts: vec![],
                    completed_parts: Default::default(),
                    availability: 0,
                },
            ]);

            let result = pool.is_piece_wanted(&piece).await;
            assert_eq!(true, result, "expected the piece to have been wanted");

            // set the piece as completed
            pool.set_completed(&piece, true).await;

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
                    parts: vec![],
                    completed_parts: Default::default(),
                    availability: 0,
                },
                Piece {
                    hash: Default::default(),
                    index: piece,
                    offset: 1024,
                    length: 1024,
                    priority: PiecePriority::None,
                    parts: vec![],
                    completed_parts: Default::default(),
                    availability: 0,
                },
            ]);

            let result = pool.is_piece_wanted(&piece).await;
            assert_eq!(false, result, "expected the piece to not have been wanted");

            // set the piece as completed
            pool.set_completed(&piece, true).await;

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
                    parts: vec![],
                    completed_parts: Default::default(),
                    availability: 0,
                },
                Piece {
                    hash: Default::default(),
                    index: 1,
                    offset: 1024,
                    length: 1024,
                    priority: PiecePriority::Normal,
                    parts: vec![],
                    completed_parts: Default::default(),
                    availability: 0,
                },
            ]);

            pool.set_completed(&0, true).await;
            let result = pool.is_completed().await;
            assert_eq!(
                false, result,
                "expected the torrent to not have been completed yet"
            );

            pool.set_completed(&1, true).await;
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
                pool.set_completed(&index, true).await;
            }
            let result = pool.num_completed_pieces().await;
            assert_eq!(2, result, "expected the number of completed pieces to be 2");
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

    fn create_piece(index: usize, length: usize) -> Piece {
        Piece {
            hash: Default::default(),
            index,
            offset: index * length,
            length,
            priority: PiecePriority::Normal,
            parts: vec![],
            completed_parts: Default::default(),
            availability: 1,
        }
    }
}
