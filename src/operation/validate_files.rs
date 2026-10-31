use crate::operation::TorrentOperationResult;
use crate::storage::Storage;
use crate::{
    BitVec, Piece, PieceIndex, Sha1Hash, Sha256Hash, TorrentContext, TorrentFlags, TorrentState,
};
use futures::{stream, StreamExt};
use log::{info, warn};
use std::fmt::Debug;
use std::time::Instant;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::task::AbortHandle;

/// The torrent file validation operation validates existing files of the torrent and checks
/// which pieces have been completed before/valid.
///
/// The pieces are validated by a background task, independent of the torrent tick interval,
/// while the operation applies the validation results on each tick.
#[derive(Debug)]
pub struct FileValidationOperation {
    num_of_pieces: usize,
    max_concurrent: usize,
    start_time: Instant,
    state: ValidationState,
    active_task: Option<FileValidationTask>,
}

impl FileValidationOperation {
    pub fn new() -> Self {
        Self {
            num_of_pieces: 0,
            max_concurrent: 0,
            start_time: Instant::now(),
            state: ValidationState::Validating,
            active_task: None,
        }
    }

    /// Execute a validation tick.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn execute(&mut self, torrent: &mut TorrentContext) -> TorrentOperationResult {
        if self.state == ValidationState::Validated {
            return TorrentOperationResult::Continue;
        }

        // pause the validation if the torrent is paused or in an error state
        if torrent.is_cancelled() || !self.should_check_files(torrent) {
            self.stop(torrent).await;
            return TorrentOperationResult::Continue;
        }

        if !self.initialize(torrent).await {
            return TorrentOperationResult::Continue;
        }

        self.start(torrent).await;
        self.poll_results(torrent).await;

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
    /// Returns `true` when the operation has been initialized, else `false`.
    async fn initialize(&mut self, torrent: &mut TorrentContext) -> bool {
        if self.max_concurrent > 0 {
            return true;
        }

        let num_of_pieces = match torrent.metadata().total_pieces() {
            Some(num_of_pieces) => num_of_pieces,
            None => return false,
        };
        let piece_len = match torrent
            .metadata()
            .info
            .as_ref()
            .map(|info| info.piece_length as usize)
        {
            Some(len) => len,
            None => return false,
        };

        self.num_of_pieces = num_of_pieces;
        self.max_concurrent = (torrent.config().checking_mem_usage / piece_len.max(1)).max(1);
        torrent.update_state(TorrentState::CheckingFiles);
        true
    }

    /// Start the background validation of the pieces which haven't been checked yet.
    async fn start(&mut self, torrent: &TorrentContext) {
        if self.active_task.is_some()
            || torrent.data_pool().num_of_pieces().await < self.num_of_pieces
        {
            return;
        }

        self.start_time = Instant::now();

        let pieces = torrent
            .data_pool()
            .pieces()
            .await
            .into_iter()
            .filter(|piece| !self.is_checked(&piece.index))
            .collect::<Vec<_>>();
        let storage = torrent.storage().clone();
        let max_concurrent = self.max_concurrent;
        let (sender, receiver) = unbounded_channel();

        let handle = spawn!("FileValidationOperation::run", async move {
            let mut results = stream::iter(pieces)
                .map(|piece| {
                    let index = piece.index;
                    let storage = storage.clone();
                    async move {
                        spawn!("FileValidationOperation::hash", Self::hash(storage, piece))
                            .await
                            .unwrap_or_else(|_| ValidationResult {
                                piece: index,
                                v1_hash: None,
                                v2_hash: None,
                            })
                    }
                })
                .buffer_unordered(max_concurrent);

            while let Some(result) = results.next().await {
                if sender.send(result).is_err() {
                    break;
                }
            }
        });

        self.active_task = Some(FileValidationTask {
            checked: BitVec::repeat(false, self.num_of_pieces),
            handle: handle.abort_handle(),
            results: receiver,
        });
    }

    /// Stop the background validation.
    /// The results which have already been received are still applied to the torrent.
    async fn stop(&mut self, torrent: &mut TorrentContext) {
        let Some(task) = self.active_task.take() else {
            return;
        };

        self.poll_results(torrent).await;
        task.handle.abort();
    }

    /// Apply the received validation results to the torrent.
    async fn poll_results(&mut self, torrent: &mut TorrentContext) {
        let Some(task) = self.active_task.as_mut() else {
            return;
        };
        let receiver = &mut task.results;

        let mut results = Vec::new();
        let mut is_finished = false;
        loop {
            match receiver.try_recv() {
                Ok(result) => results.push(result),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    is_finished = true;
                    break;
                }
            }
        }

        for result in results.iter() {
            if let Some(mut checked) = task.checked.get_mut(result.piece) {
                *checked = true;
            }
        }
        torrent
            .on_pieces_verified(
                results
                    .into_iter()
                    .map(|result| (result.piece, result.v1_hash, result.v2_hash))
                    .collect(),
            )
            .await;

        if is_finished {
            let num_unchecked = task.checked.count_zeros();
            if num_unchecked > 0 {
                warn!(
                    "Torrent {} was unable to validate {} piece(s)",
                    torrent, num_unchecked
                );
            }

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
            self.active_task = None;
            self.state = ValidationState::Validated;
            torrent.update_state(torrent.determine_state().await);
        }
    }

    /// Returns `true` if the given piece has already been validated.
    fn is_checked(&self, piece: &PieceIndex) -> bool {
        self.active_task
            .as_ref()
            .map(|task| &task.checked)
            .and_then(|checked| checked.get(*piece))
            .map(|e| *e)
            .unwrap_or(false)
    }

    /// Calculate the hash of the given piece from the storage.
    async fn hash(storage: Storage, piece: Piece) -> ValidationResult {
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
    }
}

impl Drop for FileValidationOperation {
    fn drop(&mut self) {
        if let Some(task) = self.active_task.take() {
            task.handle.abort();
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

#[derive(Debug)]
struct FileValidationTask {
    checked: BitVec,
    handle: AbortHandle,
    results: UnboundedReceiver<ValidationResult>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::ChannelReceiver;
    use crate::operation::CreatePiecesAndFilesOperation;
    use crate::storage::DiskStorage;
    use crate::tests::copy_test_file;
    use crate::torrent::TorrentCommand;
    use crate::TorrentError;
    use std::io;
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
        execute_until_validated(&mut operation, &mut context, &mut command_receiver).await;

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_execute_pause_resume() {
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

        // start the validation and pause it
        let result = operation.execute(&mut context).await;
        assert_eq!(TorrentOperationResult::Stop, result);
        context.add_options(TorrentFlags::Paused);
        let result = operation.execute(&mut context).await;
        assert_eq!(TorrentOperationResult::Continue, result);
        assert!(
            operation.active_task.is_none(),
            "expected the validation driver to have been stopped"
        );

        // resume the validation
        context.remove_options(TorrentFlags::Paused);
        execute_until_validated(&mut operation, &mut context, &mut command_receiver).await;

        let result = context.metrics();
        assert_eq!(
            30,
            result.completed_pieces.total(),
            "expected each completed piece to have been validated exactly once"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_execute_missing_files() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
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

        // validate the missing file
        execute_until_validated(&mut operation, &mut context, &mut command_receiver).await;

        let result = context.metrics();
        assert_eq!(
            0,
            result.completed_pieces.total(),
            "expected no pieces to have been completed"
        );
    }

    /// Execute the validation operation until all files have been validated.
    async fn execute_until_validated(
        operation: &mut FileValidationOperation,
        context: &mut TorrentContext,
        command_receiver: &mut ChannelReceiver<TorrentCommand>,
    ) {
        let result = loop {
            select! {
                _ = time::sleep(Duration::from_secs(10)) => break Err(TorrentError::Io(io::Error::new(io::ErrorKind::TimedOut, "timeout"))),
                _ = async {
                    loop {
                        if operation.execute(context).await == TorrentOperationResult::Continue {
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
    }

    async fn create_pieces_and_files(context: &mut TorrentContext) {
        let mut operation = CreatePiecesAndFilesOperation::new();
        let result = operation.execute(context).await;
        assert_eq!(TorrentOperationResult::Continue, result);
    }
}
