use crate::errors::Result;
use crate::{
    File, InfoHash, Piece, PieceError, PieceIndex, TorrentContext, TorrentError, TorrentFileInfo,
    TorrentMetadataInfo, TorrentOperation, TorrentOperationResult, TorrentState,
};
use async_trait::async_trait;
use log::{debug, trace, warn};
use std::sync::Arc;

#[derive(Debug)]
pub struct TorrentCreatePiecesAndFilesOperation;

impl TorrentCreatePiecesAndFilesOperation {
    pub fn new() -> Self {
        Self {}
    }

    /// Create the pieces information for the torrent.
    /// This operation can only be done when the metadata of the torrent is known.
    async fn create_pieces(&self, torrent: &TorrentContext) -> bool {
        torrent.update_state(TorrentState::Initializing).await;

        match self.try_create_pieces(torrent).await {
            Ok(pieces) => {
                trace!(
                    "Torrent {} created a total of {} pieces",
                    torrent,
                    pieces.len()
                );
                torrent.update_pieces(pieces).await;
                true
            }
            Err(e) => {
                warn!("Torrent {} failed to create torrent pieces, {}", torrent, e);
                false
            }
        }
    }

    /// Try to create the pieces of the torrent.
    /// This operation doesn't store the pieces results.
    ///
    /// # Returns
    ///
    /// Returns the pieces result for the torrent if available, else the error.
    async fn try_create_pieces(&self, data: &TorrentContext) -> Result<Vec<Piece>> {
        let info_hash: InfoHash;
        let num_pieces: usize;
        let metadata: TorrentMetadataInfo;

        {
            let mutex = data.metadata().await;
            info_hash = mutex.info_hash.clone();
            metadata = mutex
                .info
                .clone()
                .ok_or(PieceError::UnableToDeterminePieces(
                    "metadata is unavailable".to_string(),
                ))?;
            num_pieces = mutex
                .total_pieces()
                .ok_or(PieceError::UnableToDeterminePieces(
                    "failed to calculate number of pieces".to_string(),
                ))?;
        }

        let sha1_pieces = if info_hash.has_v1() {
            metadata.sha1_pieces()
        } else {
            Vec::new()
        };
        let sha256_pieces = if info_hash.has_v2() {
            metadata.sha256_pieces()
        } else {
            Vec::new()
        };
        let mut pieces = Vec::with_capacity(num_pieces);
        let total_file_size = metadata.len();
        let piece_length = metadata.piece_length as usize;
        let mut last_piece_length = total_file_size % piece_length;
        let mut offset = 0;

        if last_piece_length == 0 {
            last_piece_length = piece_length;
        }

        for piece_index in 0..num_pieces {
            let hash = if info_hash.has_v2() {
                InfoHash::try_from_bytes(sha256_pieces.get(piece_index).unwrap())?
            } else {
                InfoHash::try_from_bytes(sha1_pieces.get(piece_index).unwrap())?
            };
            let length = if piece_index != num_pieces - 1 {
                piece_length
            } else {
                last_piece_length
            };

            pieces.push(Piece::new(hash, piece_index as PieceIndex, offset, length));
            offset += length;
        }

        Ok(pieces)
    }

    /// Create the torrent files information.
    /// This can only be executed when the torrent metadata is known.
    async fn create_files(&self, torrent: &TorrentContext) -> bool {
        match self.try_create_files(torrent).await {
            Ok(files) => {
                let total_files = files.len();
                torrent.update_files(files).await;
                debug!(
                    "Torrent {} created a total of {} files",
                    torrent, total_files
                );
                true
            }
            Err(e) => {
                warn!("Torrent {} failed to create files, {}", torrent, e);
                false
            }
        }
    }

    /// Try to create the file information of the torrent.
    /// This operation doesn't store the created files within this torrent.
    async fn try_create_files(&self, torrent: &TorrentContext) -> Result<Vec<File>> {
        let info = torrent.metadata_lock().read().await;
        let is_v2_metadata: bool = info.info_hash.has_v2();
        let metadata = info.info.as_ref().ok_or(TorrentError::InvalidMetadata(
            "metadata is missing".to_string(),
        ))?;

        let mut offset = 0usize;
        let mut files = vec![];

        for (index, file_info) in metadata.files().into_iter().enumerate() {
            let file_length = file_info.length as usize;

            files.push(Self::create_file(file_info, &metadata, index, offset));

            if is_v2_metadata {
                offset = (offset + metadata.piece_length as usize - 1)
                    / metadata.piece_length as usize
                    * metadata.piece_length as usize;
            } else {
                offset += file_length;
            }
        }

        Ok(files)
    }

    fn create_file(
        file_info: TorrentFileInfo,
        metadata: &TorrentMetadataInfo,
        index: usize,
        offset: usize,
    ) -> File {
        let file_length = file_info.length as usize;
        let piece_len = metadata.piece_length as usize;
        let torrent_path = metadata.path(&file_info);
        let file_piece_start = offset / piece_len;
        let file_piece_end = offset.saturating_add(file_length + 1) / piece_len;

        File {
            index,
            torrent_path,
            torrent_offset: offset,
            info: file_info,
            priority: Default::default(),
            pieces: file_piece_start..file_piece_end + 1, // as the range is exclusive, add 1 to the end range
        }
    }
}

#[async_trait]
impl TorrentOperation for TorrentCreatePiecesAndFilesOperation {
    fn name(&self) -> &str {
        "create pieces operation"
    }

    async fn execute(&self, torrent: &Arc<TorrentContext>) -> TorrentOperationResult {
        // check if the pieces have already been created
        // if so, continue the chain
        if torrent.data_pool().num_of_pieces().await > 0 {
            return TorrentOperationResult::Continue;
        }

        // try to create the pieces and files
        if self.create_pieces(&torrent).await {
            if self.create_files(&torrent).await {
                return TorrentOperationResult::Continue;
            }
        }

        TorrentOperationResult::Stop
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_torrent;
    use crate::init_logger;
    use tempfile::tempdir;

    mod create_pieces {
        use super::*;

        #[tokio::test]
        async fn test_execute_create_pieces() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let torrent = create_torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::default(),
                vec![],
                vec![]
            );
            let inner = torrent.instance().unwrap();
            let operation =
                Box::new(TorrentCreatePiecesAndFilesOperation::new()) as Box<dyn TorrentOperation>;

            let result = operation.execute(&inner).await;

            assert_eq!(TorrentOperationResult::Continue, result);
            assert_eq!(
                15237,
                torrent.total_pieces().await,
                "expected the pieces to have been created"
            );
        }

        #[tokio::test]
        async fn test_execute_pieces_already_exist() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let torrent = create_torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::default(),
                vec![],
                vec![]
            );
            let context = torrent.instance().unwrap();
            let info_hash = context.metadata().await.info_hash.clone();
            let operation = TorrentCreatePiecesAndFilesOperation::new();

            // insert the initial data within the pool
            context
                .update_pieces(vec![Piece::new(info_hash, 0, 0, 1024)])
                .await;
            context
                .update_files(vec![File {
                    index: 0,
                    torrent_path: Default::default(),
                    torrent_offset: 0,
                    info: TorrentFileInfo {
                        length: 0,
                        path: None,
                        path_utf8: None,
                        md5sum: None,
                        attr: None,
                        symlink_path: None,
                        sha1: None,
                    },
                    priority: Default::default(),
                    pieces: Default::default(),
                }])
                .await;

            // execute the operation
            let result = operation.execute(&context).await;

            assert_eq!(TorrentOperationResult::Continue, result);
            assert_eq!(
                1,
                torrent.total_pieces().await,
                "expected the pieces to not have been updated"
            );
        }
    }

    mod create_files {
        use super::*;

        #[tokio::test]
        async fn test_execute() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let torrent = create_torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::default(),
                vec![],
                vec![]
            );
            let context = torrent.instance().unwrap();
            let create_pieces = TorrentCreatePiecesAndFilesOperation::new();
            let operation = TorrentCreatePiecesAndFilesOperation::new();

            let result = create_pieces.execute(&context).await;
            assert_eq!(
                TorrentOperationResult::Continue,
                result,
                "expected the pieces to have been created"
            );

            let result = operation.execute(&context).await;
            assert_eq!(
                TorrentOperationResult::Continue,
                result,
                "expected the operations chain to continue"
            );
            assert_eq!(
                1,
                context.total_files().await,
                "expected the files to have been created"
            );
        }

        #[tokio::test]
        async fn test_execute_no_metadata() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
            let torrent = create_torrent!(
                uri,
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::default(),
                vec![],
                vec![]
            );
            let context = torrent.instance().unwrap();
            let operation = TorrentCreatePiecesAndFilesOperation::new();

            let result = operation.execute(&context).await;

            assert_eq!(
                TorrentOperationResult::Stop,
                result,
                "expected the operations chain to stop"
            );
        }

        #[tokio::test]
        async fn test_try_create_files() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let torrent = create_torrent!(
                "multifile.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::default(),
                vec![],
                vec![]
            );
            let pieces_operation = TorrentCreatePiecesAndFilesOperation::new();
            let operation = TorrentCreatePiecesAndFilesOperation::new();
            let context = torrent.instance().unwrap();

            // create the torrent pieces
            let result = pieces_operation.execute(&context).await;
            assert_eq!(
                TorrentOperationResult::Continue,
                result,
                "expected the pieces operation to have succeeded"
            );
            let pieces = context.data_pool().pieces().await;

            // create the torrent files
            let files = operation
                .try_create_files(&context)
                .await
                .expect("failed to create torrent files");

            let mut previous_end_piece = 0 as PieceIndex;
            for file in files {
                let start_piece = file.pieces.start;
                let end_piece = file.pieces.end;

                // the file should either start at the previous and piece or the next one
                assert!(
                    start_piece == previous_end_piece.saturating_sub(1usize)
                        || start_piece == previous_end_piece,
                    "expected the start piece {} to continue from previous end piece {}",
                    start_piece,
                    previous_end_piece
                );
                previous_end_piece = end_piece;
            }

            // check if the piece range from the last file ends at the last piece of the torrent
            assert_eq!(
                pieces[pieces.len() - 1].index,
                previous_end_piece.saturating_sub(1usize),
                "expected the last file to end on the last piece of the torrent"
            );
        }

        #[tokio::test]
        async fn test_create_file() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let torrent = create_torrent!(
                "multifile.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::default(),
                vec![],
                vec![]
            );
            let context = torrent.instance().unwrap();
            let metadata = context.metadata().await;
            let metadata_info = metadata
                .info
                .as_ref()
                .expect("expected the metadata info to be present");
            let metadata_files = metadata_info.files();

            let result = TorrentCreatePiecesAndFilesOperation::create_file(
                metadata_files.get(1).unwrap().clone(),
                &metadata_info,
                1,
                3364128518,
            );

            assert_eq!(
                401, result.pieces.start,
                "expected the starting piece to match"
            );
            assert_eq!(725, result.pieces.end, "expected the ending piece to match");
        }
    }
}
