use crate::operation::{TorrentOperation, TorrentOperationResult};
use crate::peer::PeerDiscovery;
use crate::storage::Storage;
use crate::torrent::InnerTorrent;
use crate::{File, Piece, PieceIndex, TorrentContext, TorrentFlags, TorrentState};
use async_trait::async_trait;
use futures::{stream, StreamExt};
use log::{debug, info};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;
use tokio::select;
use tokio::sync::oneshot;
use tokio_util::sync::WaitForCancellationFutureOwned;
#[cfg(feature = "tracing")]
use tracing::instrument;

#[derive(Debug, PartialEq)]
enum ValidationState {
    None,
    Validating,
    Validated,
}

/// The torrent file validation operation validates existing files of the torrent and checks which pieces have been completed before/valid.
pub struct TorrentFileValidationOperation {
    state: ValidationState,
    ready_signal: Option<oneshot::Receiver<()>>,
}

impl TorrentFileValidationOperation {
    pub fn new() -> Self {
        Self {
            state: ValidationState::None,
            ready_signal: None,
        }
    }

    /// Poll the in-flight validation future
    async fn poll_future(&mut self, context: &mut TorrentContext) {
        if let Some(_) = self.ready_signal.as_mut().and_then(|e| e.try_recv().ok()) {
            self.state = ValidationState::Validated;
            self.ready_signal = None;

            let new_state = context.determine_state().await;
            context.update_state(new_state).await;
            // start announcing the torrent again
            context
                .tracker_manager()
                .start_announcing(&context.metadata().info_hash)
                .await;
        }
    }

    /// Returns `true` if the operation should validate existing files, else `false`.
    fn should_check_files(&self, context: &TorrentContext) -> bool {
        let is_paused = context.options().contains(TorrentFlags::Paused);
        let state = context.state();

        !is_paused && state != &TorrentState::Error
    }

    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn validate_files(&mut self, context: &TorrentContext, files: Vec<File>) {
        let handle = context.handle();
        let info_hash = context.metadata().info_hash.clone();
        let data_pool = context.data_pool().clone();
        let piece_len = match context.metadata().info.as_ref() {
            Some(info) => info.piece_length as usize,
            None => return,
        };

        // stop announcing the torrent
        context.tracker_manager().stop_announcing(&info_hash).await;
        self.state = ValidationState::Validating;

        let pieces = data_pool.pieces().await;
        if pieces.is_empty() {
            debug!(
                "Torrent {} failed to start file validation, pieces are unknown",
                context
            );
            return;
        }

        debug!(
            "Torrent {} is validating files {:?}",
            context,
            files
                .iter()
                .map(|e| e.torrent_path.to_string_lossy())
                .collect::<Vec<_>>(),
        );
        let cancelled = context.cancelled_owned();
        let max_parallel = (context.config().checking_mem_usage / piece_len).max(1);
        let storage = context.storage().clone();
        let torrent = InnerTorrent::new(
            handle,
            context.command_sender().clone(),
            context.callbacks().clone(),
        );

        let (tx, rx) = oneshot::channel();
        self.ready_signal = Some(rx);
        tokio::spawn(async move {
            Self::run_validation(
                torrent,
                storage,
                pieces,
                files.len(),
                max_parallel,
                tx,
                cancelled,
            )
            .await;
        });
    }

    /// Validate the piece data stored within the [Storage] of the torrent.
    /// Returns the [PieceIndex] when the stored piece data is valid, else [None].
    async fn validate_piece(
        torrent: InnerTorrent,
        storage: Arc<dyn Storage>,
        piece: Piece,
    ) -> Option<PieceIndex> {
        let expected_v1 = piece.hash.hash_v1();
        let expected_v2 = piece.hash.hash_v2();

        // early fail if the piece hash is missing
        if expected_v1.is_none() && expected_v2.is_none() {
            debug!(
                "Torrent {} is unable to validate piece {}, piece hash is missing or invalid",
                torrent, piece.index
            );
            return None;
        }

        let validation_result = match (expected_v1, expected_v2) {
            (Some(_), Some(hash_v2)) | (None, Some(hash_v2)) => storage
                .hash_v2(&piece.index)
                .await
                .ok()
                .map(|hash| hash_v2 == hash)
                .unwrap_or(false),
            (Some(hash_v1), None) => storage
                .hash_v1(&piece.index)
                .await
                .ok()
                .map(|hash| hash == hash_v1)
                .unwrap_or(false),
            _ => false,
        };

        if validation_result {
            let _ = torrent.piece_completed(&piece.index).await;
            Some(piece.index)
        } else {
            None
        }
    }

    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn run_validation(
        torrent: InnerTorrent,
        storage: Arc<dyn Storage>,
        pieces: Vec<Piece>,
        num_of_files: usize,
        max_parallel: usize,
        ready_sender: oneshot::Sender<()>,
        cancelled: WaitForCancellationFutureOwned,
    ) {
        let start = Instant::now();
        let futures: Vec<_> = pieces
            .into_iter()
            .map(|piece| Self::validate_piece(torrent.clone(), storage.clone(), piece))
            .collect();

        let valid_pieces = select! {
            _ = cancelled => {
                return;
            },
            futures = stream::iter(futures)
                .buffer_unordered(max_parallel)
                .collect::<Vec<_>>() => {
                    futures.into_iter()
                    .flat_map(|e| e)
                    .collect::<Vec<_>>()
            }
        };

        let _ = ready_sender.send(());
        let time_taken = start.elapsed();
        info!(
            "Torrent {} completed {} file validation(s) ({} valid chunks) in {}.{:03} seconds",
            torrent,
            num_of_files,
            valid_pieces.len(),
            time_taken.as_secs(),
            time_taken.subsec_millis()
        );
    }
}

#[async_trait]
impl TorrentOperation for TorrentFileValidationOperation {
    fn name(&self) -> &str {
        "torrent file validation operation"
    }

    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn execute(
        &mut self,
        torrent: &mut TorrentContext,
        _: &[Arc<dyn PeerDiscovery>],
    ) -> TorrentOperationResult {
        // early exit if the torrent is paused or in an error state
        if !self.should_check_files(torrent) {
            return TorrentOperationResult::Continue;
        }

        // poll the in-flight validation future
        self.poll_future(torrent).await;

        // check the current state of the validator
        match self.state {
            ValidationState::Validated => return TorrentOperationResult::Continue,
            ValidationState::Validating => return TorrentOperationResult::Stop,
            _ => {}
        }

        let files = torrent.files().await;
        if files.len() > 0 {
            torrent.update_state(TorrentState::CheckingFiles).await;
            self.validate_files(torrent, files).await;
            return TorrentOperationResult::Stop;
        }

        TorrentOperationResult::Continue
    }
}

impl Debug for TorrentFileValidationOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TorrentFileValidationOperation")
            .field("state", &self.state)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::TorrentCreatePiecesAndFilesOperation;
    use crate::storage::DiskStorage;
    use crate::tests::copy_test_file;
    use crate::{create_torrent_context, TorrentError};
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::{select, time};

    #[tokio::test]
    async fn test_execute_state_validating() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (mut context, _) = create_torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![]
        );
        let mut operation = TorrentFileValidationOperation::new();

        operation.state = ValidationState::Validating;
        let result = operation.execute(&mut context, vec![].as_slice()).await;

        assert_eq!(TorrentOperationResult::Stop, result);
    }

    #[tokio::test]
    async fn test_execute_state_validated() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (mut context, _) = create_torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![]
        );
        let mut operation = TorrentFileValidationOperation::new();

        operation.state = ValidationState::Validated;
        let result = operation.execute(&mut context, vec![].as_slice()).await;

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
        let (mut context, mut command_receiver) = create_torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            DhtOption::none(),
            |info_hash, data_pool| Arc::new(DiskStorage::new(info_hash, temp_path, data_pool))
        );
        let mut operation = TorrentFileValidationOperation::new();

        // create pieces & files
        create_pieces_and_files(&mut context).await;

        // validate the file
        let result = loop {
            select! {
                _ = time::sleep(Duration::from_secs(25)) => break Err(TorrentError::Timeout),
                _ = async {
                    loop {
                        if operation.execute(&mut context, vec![].as_slice()).await == TorrentOperationResult::Continue {
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

        let result = operation.execute(&mut context, vec![].as_slice()).await;
        assert_eq!(TorrentOperationResult::Continue, result);

        let pieces = context.data_pool().pieces().await;
        for piece in 0..30 {
            assert_eq!(
                true,
                pieces.get(piece).unwrap().is_completed(),
                "expected piece {} to be completed",
                piece
            );
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
        let mut operation = TorrentCreatePiecesAndFilesOperation::new();
        let result = operation.execute(context, vec![].as_slice()).await;
        assert_eq!(TorrentOperationResult::Continue, result);
    }
}
