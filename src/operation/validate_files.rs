use crate::operation::TorrentOperationResult;
use crate::{PieceIndex, Sha1Hash, Sha256Hash, TorrentContext, TorrentFlags, TorrentState};
use futures::FutureExt;
use log::info;
use std::fmt::Debug;
use std::time::Instant;
use tokio::task::JoinSet;

/// The torrent file validation operation validates existing files of the torrent and checks which pieces have been completed before/valid.
#[derive(Debug)]
pub struct FileValidationOperation {
    cursor: usize,
    num_of_pieces: usize,
    max_concurrent: usize,
    start_time: Instant,
    state: ValidationState,
    validation_tasks: JoinSet<ValidationResult>,
}

impl FileValidationOperation {
    pub fn new() -> Self {
        Self {
            cursor: 0,
            num_of_pieces: 0,
            max_concurrent: 0,
            start_time: Instant::now(),
            state: ValidationState::Validating,
            validation_tasks: Default::default(),
        }
    }

    /// Execute a validation tick.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn execute(&mut self, torrent: &mut TorrentContext) -> TorrentOperationResult {
        // early exit if the torrent is paused or in an error state
        if self.state == ValidationState::Validated
            || torrent.is_cancelled()
            || !self.should_check_files(torrent)
        {
            return TorrentOperationResult::Continue;
        }

        self.initialize(torrent).await;
        self.validate(torrent).await;
        self.poll_futures(torrent).await;

        TorrentOperationResult::Stop
    }

    /// Returns `true` if the operation should validate existing files, else `false`.
    fn should_check_files(&self, context: &TorrentContext) -> bool {
        let is_paused = context.options().contains(TorrentFlags::Paused);
        let is_metadata_known = context.is_metadata_known();
        let state = context.state();

        !is_paused && is_metadata_known && state != &TorrentState::Error
    }

    /// Initialize the validation operation.
    async fn initialize(&mut self, torrent: &mut TorrentContext) {
        if self.max_concurrent > 0 {
            return;
        }

        let num_of_pieces = match torrent.metadata().total_pieces() {
            Some(num_of_pieces) => num_of_pieces,
            None => return,
        };
        let piece_len = match torrent
            .metadata()
            .info
            .as_ref()
            .map(|info| info.piece_length as usize)
        {
            Some(len) => len,
            None => return,
        };

        self.num_of_pieces = num_of_pieces;
        self.max_concurrent = (torrent.config().checking_mem_usage / piece_len).max(1);
        self.start_time = Instant::now();
        torrent.update_state(TorrentState::CheckingFiles);
    }

    async fn validate(&mut self, torrent: &TorrentContext) {
        if self.cursor >= self.num_of_pieces {
            return;
        }

        let len = self
            .max_concurrent
            .saturating_sub(self.validation_tasks.len());
        for _ in 0..len {
            let piece = match torrent.data_pool().piece(&self.cursor).await {
                Some(piece) => piece,
                None => return,
            };
            let storage = torrent.storage().clone();

            self.validation_tasks.spawn(async move {
                match (piece.hash.has_v1(), piece.hash.has_v2()) {
                    (true, true) | (false, true) => ValidationResult {
                        piece: piece.index,
                        v1_hash: None,
                        v2_hash: storage.hash_v2(&piece.index).await.ok(),
                    },
                    (true, false) => ValidationResult {
                        piece: piece.index,
                        v1_hash: storage.hash_v1(&piece.index).await.ok(),
                        v2_hash: None,
                    },
                    (false, false) => ValidationResult {
                        piece: piece.index,
                        v1_hash: None,
                        v2_hash: None,
                    },
                }
            });

            self.cursor += 1;
        }
    }

    async fn poll_futures(&mut self, torrent: &mut TorrentContext) {
        while let Some(result) = self.validation_tasks.join_next().now_or_never().flatten() {
            match result {
                Ok(result) => {
                    torrent
                        .on_piece_verified(&result.piece, result.v1_hash, result.v2_hash)
                        .await;
                }
                Err(_) => break,
            }
        }

        if self.cursor == self.num_of_pieces && self.validation_tasks.is_empty() {
            let elapsed = self.start_time.elapsed();
            let num_of_files = torrent
                .metadata()
                .info
                .as_ref()
                .map(|info| info.total_files())
                .unwrap_or_default();

            info!(
                "Torrent {} validated {} file(s) in {:.3} seconds",
                torrent,
                num_of_files,
                elapsed.as_secs_f64()
            );
            self.state = ValidationState::Validated;
            torrent.update_state(torrent.determine_state().await);
        }
    }
}

#[derive(Debug, PartialEq)]
enum ValidationState {
    Validating,
    Validated,
}

#[derive(Debug)]
struct ValidationResult {
    piece: PieceIndex,
    v1_hash: Option<Sha1Hash>,
    v2_hash: Option<Sha256Hash>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::CreatePiecesAndFilesOperation;
    use crate::storage::DiskStorage;
    use crate::tests::copy_test_file;
    use crate::TorrentError;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::{select, time};

    #[tokio::test]
    async fn test_execute_state_validating() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (mut context, _) = torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![]
        );
        let mut operation = FileValidationOperation::new();

        // create pieces and files
        create_pieces_and_files(&mut context).await;

        // execute the validation operation
        let result = operation.execute(&mut context).await;

        assert_eq!(TorrentOperationResult::Stop, result);
    }

    #[tokio::test]
    async fn test_execute_state_validated() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (mut context, _) = torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![]
        );
        let mut operation = FileValidationOperation::new();

        operation.state = ValidationState::Validated;
        let result = operation.execute(&mut context).await;

        assert_eq!(TorrentOperationResult::Continue, result);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_execute() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        copy_test_file(
            temp_path,
            "piece-1_30.iso",
            Some("debian-12.4.0-amd64-DVD-1.iso"),
        );
        let (mut context, mut command_receiver) = torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            vec![],
            None,
            None,
            |info_hash, data_pool| DiskStorage::new(info_hash, temp_path, data_pool).into()
        );
        let mut operation = FileValidationOperation::new();

        // create pieces & files
        create_pieces_and_files(&mut context).await;

        // validate the file
        let result = loop {
            select! {
                _ = time::sleep(Duration::from_secs(25)) => break Err(TorrentError::Timeout),
                _ = async {
                    loop {
                        if operation.execute(&mut context).await == TorrentOperationResult::Continue {
                            break;
                        }
                        time::sleep(Duration::from_millis(50)).await;
                    }
                } => break Ok(()),
                Some(command) = command_receiver.recv() => {
                    context.on_command(command).await;
                }
            }
        };
        assert!(
            result.is_ok(),
            "expected the validation to succeed, but got {:?}",
            result
        );

        let result = operation.execute(&mut context).await;
        assert_eq!(TorrentOperationResult::Continue, result);

        for piece in 0..30 {
            assert_eq!(
                true,
                context.data_pool().is_piece_completed(&piece).await,
                "expected piece bitfield {} to be completed",
                piece
            );
        }

        let result = context.metrics();
        assert_eq!(
            30,
            result.completed_pieces.total(),
            "expected completed pieces to be 30"
        );
        assert_ne!(
            0,
            result.completed_size.total(),
            "expected total completed size to be > 0"
        );
    }

    async fn create_pieces_and_files(context: &mut TorrentContext) {
        let mut operation = CreatePiecesAndFilesOperation::new();
        let result = operation.execute(context).await;
        assert_eq!(TorrentOperationResult::Continue, result);
    }
}
