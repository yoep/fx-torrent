use crate::channel::{ChannelReceiver, ChannelSender, Reply};
use crate::{
    BitVec, File, FileAttributeFlags, FileIndex, FilePriority, Piece, PieceBlock, PieceIndex,
    PiecePriority,
};
use itertools::Itertools;
use std::collections::BTreeMap;
use std::ops::Range;

/// The data pool of a torrent storing info about pieces and files.
/// It makes use of a separate loop task to handle operations on the data pool.
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
        spawn!("InnerDataPool::run", async move {
            let mut inner = InnerDataPool::new(pieces);
            inner.run(rx).await;
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

    /// Returns the file for the given index, if found.
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

    /// Returns the file for the given name, if found.
    pub async fn file_by_name(&self, name: impl ToString) -> Option<File> {
        self.sender
            .send(|tx| DataPoolCommand::GetFileByName {
                name: name.to_string(),
                response: tx,
            })
            .await
            .await
            .ok()
            .flatten()
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
        let _ = self
            .sender
            .send(|tx| DataPoolCommand::SetPieces {
                pieces,
                response: tx,
            })
            .await
            .await;
    }

    /// Set the files of the pool.
    /// This will replace all existing files within the pool.
    pub async fn set_files(&self, files: Vec<File>) {
        let _ = self
            .sender
            .send(|tx| DataPoolCommand::SetFiles {
                files,
                response: tx,
            })
            .await
            .await;
    }

    /// Returns the piece which contains the given torrent offset.
    pub async fn find_piece_at_offset(&self, offset: usize) -> Option<Piece> {
        self.sender
            .send(|tx| DataPoolCommand::FindPieceAtOffset {
                offset,
                response: tx,
            })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the [PieceBlock] matching the given offset within the piece, if found.
    pub async fn find_piece_block(&self, piece: &PieceIndex, offset: usize) -> Option<PieceBlock> {
        self.sender
            .send(|tx| DataPoolCommand::FindPieceBlock {
                piece: *piece,
                offset,
                response: tx,
            })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the piece priorities for the torrent.
    pub async fn piece_priorities(&self) -> BTreeMap<PieceIndex, PiecePriority> {
        self.sender
            .send(|tx| DataPoolCommand::PiecePriorities { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Set the priorities for the given pieces of the torrent.
    pub async fn set_piece_priorities(&self, priorities: &[(PieceIndex, PiecePriority)]) {
        self.sender
            .fire_and_forget(DataPoolCommand::SetPiecePriorities {
                priorities: priorities.to_vec(),
            })
            .await;
    }

    /// Set the priorities for the given files of the torrent.
    pub async fn set_file_priorities(&self, priorities: &[(FileIndex, FilePriority)]) {
        let _ = self
            .sender
            .send(|tx| DataPoolCommand::SetFilePriorities {
                priorities: priorities.to_vec(),
                response: tx,
            })
            .await
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

    /// Returns `true` if all wanted pieces have been downloaded and validated, else `false`.
    ///
    /// Every piece with anything but a [PiecePriority::None] has
    /// been downloaded and validated their data in this case.
    pub async fn is_completed(&self) -> bool {
        self.sender
            .send(|tx| DataPoolCommand::IsCompleted { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns `true` if the given piece has been downloaded and validated, else `false`.
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

    /// Set the completion state for the given piece slice.
    pub async fn set_completed(&self, pieces: &[PieceIndex], completed: bool) {
        self.sender
            .send(|tx| DataPoolCommand::SetPieceCompleted {
                pieces: pieces.to_vec(),
                completed,
                response: tx,
            })
            .await;
    }

    /// Returns `true` if the given torrent byte range is available (downloaded and validated), else `false`.
    pub async fn has_bytes(&self, bytes: Range<usize>) -> bool {
        self.sender
            .send(|tx| DataPoolCommand::HasBytes {
                bytes,
                response: tx,
            })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the piece indexes in which the torrent is interested.
    /// These are the pieces that don't have [PiecePriority::None] as a priority.
    pub async fn interested_pieces(&self) -> Vec<PieceIndex> {
        self.sender
            .send(|tx| DataPoolCommand::InterestedPieces { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the amount of bytes in which the torrent is interested.
    pub async fn interested_size(&self) -> usize {
        self.sender
            .send(|tx| DataPoolCommand::InterestedSize { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the piece indexes which have completed downloading.
    /// This might include pieces with [PiecePriority::None], if they've been downloaded in the past.
    pub async fn completed_pieces(&self) -> Vec<PieceIndex> {
        self.sender
            .send(|tx| DataPoolCommand::CompletedPieces { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the amount of bytes which have completed downloading.
    /// This might include pieces with [PiecePriority::None], if they've been downloaded in the past.
    pub async fn completed_size(&self) -> usize {
        self.sender
            .send(|tx| DataPoolCommand::CompletedSize { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns `true` if the given piece index is wanted by the torrent and not yet completed, else `false`.
    pub async fn is_piece_wanted(&self, piece_index: &PieceIndex) -> bool {
        self.sender
            .send(|tx| DataPoolCommand::IsPieceWanted {
                index: *piece_index,
                response: tx,
            })
            .await
            .await
            .unwrap_or_default()
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
    GetFileByName {
        name: String,
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
        response: Reply<()>,
    },
    SetFiles {
        files: Vec<File>,
        response: Reply<()>,
    },
    FindPieceAtOffset {
        offset: usize,
        response: Reply<Option<Piece>>,
    },
    FindPieceBlock {
        piece: PieceIndex,
        offset: usize,
        response: Reply<Option<PieceBlock>>,
    },
    PiecePriorities {
        response: Reply<BTreeMap<PieceIndex, PiecePriority>>,
    },
    SetPiecePriorities {
        priorities: Vec<(PieceIndex, PiecePriority)>,
    },
    SetFilePriorities {
        priorities: Vec<(FileIndex, FilePriority)>,
        response: Reply<()>,
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
        pieces: Vec<PieceIndex>,
        completed: bool,
        response: Reply<()>,
    },
    InterestedPieces {
        response: Reply<Vec<PieceIndex>>,
    },
    InterestedSize {
        response: Reply<usize>,
    },
    CompletedPieces {
        response: Reply<Vec<PieceIndex>>,
    },
    CompletedSize {
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
    HasBytes {
        bytes: Range<usize>,
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
            completed_pieces: BitVec::repeat(false, pieces.len()),
            pieces: pieces
                .into_iter()
                .map(|piece| (piece.index, piece))
                .collect(),
            files: Default::default(),
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn run(&mut self, mut receiver: ChannelReceiver<DataPoolCommand>) {
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
                DataPoolCommand::GetFileByName { name, response } => response.send(
                    self.files
                        .values()
                        .find(|file| file.filename() == name.as_str())
                        .cloned(),
                ),
                DataPoolCommand::GetPieces { response } => {
                    response.send(self.pieces.values().cloned().collect());
                }
                DataPoolCommand::GetFiles { response } => {
                    response.send(self.files.values().cloned().collect());
                }
                DataPoolCommand::SetPieces { pieces, response } => {
                    self.set_pieces(pieces);
                    response.send(());
                }
                DataPoolCommand::SetFiles { files, response } => {
                    self.set_files(files);
                    response.send(());
                }
                DataPoolCommand::FindPieceAtOffset { offset, response } => {
                    response.send(self.find_piece_at_offset(offset));
                }
                DataPoolCommand::FindPieceBlock {
                    piece,
                    offset,
                    response,
                } => response.send(self.pieces.iter().find(|(idx, _)| *idx == &piece).and_then(
                    |(_, piece)| {
                        piece
                            .blocks
                            .iter()
                            .find(|part| part.begin == offset)
                            .cloned()
                    },
                )),
                DataPoolCommand::PiecePriorities { response } => {
                    response.send(self.piece_priorities());
                }
                DataPoolCommand::SetPiecePriorities { priorities } => {
                    self.set_piece_priorities(priorities);
                }
                DataPoolCommand::SetFilePriorities {
                    priorities,
                    response,
                } => {
                    self.set_file_priorities(priorities);
                    response.send(());
                }
                DataPoolCommand::ContainsPiece { index, response } => {
                    response.send(self.pieces.contains_key(&index));
                }
                DataPoolCommand::IsPieceCompleted { index, response } => {
                    response.send(self.is_piece_completed(&index));
                }
                DataPoolCommand::SetPieceCompleted {
                    pieces,
                    completed,
                    response,
                } => {
                    self.set_pieces_completion_state(&pieces, completed);
                    response.send(());
                }
                DataPoolCommand::InterestedPieces { response } => {
                    response.send(self.interested_pieces());
                }
                DataPoolCommand::InterestedSize { response } => {
                    response.send(self.interested_size());
                }
                DataPoolCommand::CompletedPieces { response } => {
                    response.send(self.completed_pieces());
                }
                DataPoolCommand::CompletedSize { response } => {
                    response.send(self.completed_size());
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
                DataPoolCommand::HasBytes { bytes, response } => {
                    response.send(self.has_bytes(&bytes));
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

    fn set_piece_priorities(&mut self, priorities: Vec<(PieceIndex, PiecePriority)>) {
        for (index, priority) in priorities {
            if let Some(piece) = self.pieces.get_mut(&index) {
                piece.priority = priority;
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

    mod prioritize_file {
        use super::*;

        #[tokio::test]
        async fn test_set_file_priorities_single_file() {
            init_logger!();
            let pieces = vec![
                create_piece(0, 1024),
                create_piece(1, 1024),
                create_piece(2, 1024),
                create_piece(3, 1024),
            ];
            let files = vec![
                create_file(0, 0, 2000, 0..2),
                create_file(1, 2000, 1048, 1..3),
                create_file(2, 3072, 1048, 3..4),
            ];
            let pool = DataPool::new();

            // update the pool data
            pool.set_pieces(pieces).await;
            pool.set_files(files).await;

            // prioritize the first file
            pool.set_file_priorities(&create_file_priority(0)).await;
            let result = pool.piece_priorities().await;
            assert_eq!(
                vec![
                    (0, PiecePriority::Normal),
                    (1, PiecePriority::Normal),
                    (2, PiecePriority::None),
                    (3, PiecePriority::None),
                ]
                .into_iter()
                .collect::<BTreeMap<_, _>>(),
                result,
                "expected the first file to have priority Normal"
            );

            // prioritize the second file
            pool.set_file_priorities(&create_file_priority(1)).await;
            let result = pool.piece_priorities().await;
            assert_eq!(
                vec![
                    (0, PiecePriority::None),
                    (1, PiecePriority::Normal),
                    (2, PiecePriority::Normal),
                    (3, PiecePriority::None),
                ]
                .into_iter()
                .collect::<BTreeMap<_, _>>(),
                result,
                "expected the first file to have priority Normal"
            );

            // prioritize the last file
            pool.set_file_priorities(&create_file_priority(2)).await;
            let result = pool.piece_priorities().await;
            assert_eq!(
                vec![
                    (0, PiecePriority::None),
                    (1, PiecePriority::None),
                    (2, PiecePriority::None),
                    (3, PiecePriority::Normal),
                ]
                .into_iter()
                .collect::<BTreeMap<_, _>>(),
                result,
                "expected the first file to have priority Normal"
            );
        }

        fn create_file_priority(index: FileIndex) -> Vec<(FileIndex, FilePriority)> {
            (0..3)
                .into_iter()
                .map(|i| {
                    let priority = if i == index {
                        FilePriority::Normal
                    } else {
                        FilePriority::None
                    };
                    (i, priority)
                })
                .collect()
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
