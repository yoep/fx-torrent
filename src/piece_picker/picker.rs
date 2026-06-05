use crate::peer::{Peer, PeerHandle, PeerState};
use crate::piece_picker::cache::PickerCache;
use crate::piece_picker::strategy::{PeerInfo, Strategy};
use crate::piece_picker::PickerOptions;
use crate::storage::Storage;
use crate::torrent_data::DataPool;
use crate::{BitVec, BlockIndex, InnerTorrent, Piece, PieceBlock, PieceIndex, PiecePriority};
use derive_more::Display;
use itertools::Itertools;
use log::{debug, trace, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

/// The FX piece picker implementation for retrieving pieces from peers.
///
/// This piece picker makes use of multiple strategies for
/// optimizing piece selection and prioritization.
#[derive(Debug, Display)]
#[display("{}", self.torrent)]
pub struct FxPiecePicker {
    options: PickerOptions,
    downloads: HashMap<PieceIndex, PieceInfo>,
    cache: PickerCache,
    torrent: InnerTorrent,
    data_pool: DataPool,
    storage: Arc<Storage>,
    strategies: Vec<Strategy>,
}

impl FxPiecePicker {
    /// Create a new piece picker instance.
    pub fn new(
        torrent: InnerTorrent,
        data_pool: DataPool,
        storage: Arc<Storage>,
        strategies: Vec<Strategy>,
        cache_limit: usize,
        options: PickerOptions,
    ) -> Self {
        Self {
            options,
            downloads: Default::default(),
            cache: PickerCache::new(storage.clone(), cache_limit),
            torrent,
            data_pool,
            storage,
            strategies,
        }
    }

    /// Returns the options set for the piece picker.
    pub fn options(&self) -> &PickerOptions {
        &self.options
    }

    /// Set the pieces of the torrent to pick from.
    /// This replaces any previously set pieces within the picker.
    pub fn set_pieces(&mut self, pieces: &[Piece]) {
        self.cache.resize(
            pieces.len(),
            pieces.first().map(|e| e.length).unwrap_or_default(),
            pieces.last().map(|e| e.length).unwrap_or_default(),
        );

        for piece in pieces {
            self.add_piece_to_queue(piece);
        }
    }

    /// Set the priority for the given piece.
    /// This overrides any previously set priority for the piece.
    pub fn set_priority(&mut self, piece: &PieceIndex, priority: PiecePriority) {
        let piece_info = self.downloads.entry(*piece).or_insert(PieceInfo {
            piece: *piece,
            priority,
            blocks: Default::default(),
            availability: 0,
            state: PieceState::None,
        });
        piece_info.priority = priority;
    }

    /// Set the given piece as completed.
    /// This means that the piece has been validated and written to storage.
    pub fn set_completed(&mut self, piece: &PieceIndex) {
        let info = self.downloads.entry(*piece).or_insert(PieceInfo {
            piece: *piece,
            priority: Default::default(),
            blocks: Default::default(),
            availability: 0,
            state: PieceState::Finished,
        });
        info.blocks
            .iter_mut()
            .for_each(|(_, block)| block.state = BlockState::Finished);
        info.state = PieceState::Finished;
    }

    /// Set the given piece as failed,
    /// resetting the state of the piece to be downloaded again.
    ///
    /// This is typically called when a piece fails its cryptographic hash verification
    /// after download, or if the underlying storage encounters a corruption error.
    pub fn set_failed(&mut self, piece: &PieceIndex) {
        let piece_info = match self.downloads.get_mut(piece) {
            None => return,
            Some(info) => info,
        };
        Self::invalidate_piece(piece_info);
    }

    /// Set the options for the piece picker.
    /// This replaces any previously set options.
    pub fn set_options(&mut self, options: PickerOptions) {
        self.options = options;
    }

    /// Add the given options of the piece picker.
    pub fn add_options(&mut self, options: PickerOptions) {
        self.options |= options;
    }

    /// Remove the given options from the piece picker.
    pub fn remove_options(&mut self, options: PickerOptions) {
        self.options &= !options;
    }

    /// Process a received piece block from a peer.
    pub async fn block_received(&mut self, peer: &Peer, block: PieceBlock, data: Vec<u8>) {
        trace!(
            "Piece picker {} received block {:?} from peer {}",
            self,
            block,
            peer
        );

        // write the data to the cache
        self.write_block(&block, data).await;

        // check if this was the last block needed to complete the piece
        if self.is_piece_complete(&block.piece) {
            self.on_piece_completed(&block.piece).await;
        }
    }

    /// Process a piece block request that has been rejected by the peer.
    pub fn block_rejected(&mut self, peer: &Peer, block: PieceBlock) {
        // try to find the block
        let block_info = match self
            .downloads
            .get_mut(&block.piece)
            .and_then(|info| info.blocks.get_mut(&block.block))
        {
            None => {
                trace!(
                    "Piece picker {} couldn't find download block {:?}",
                    self,
                    block
                );
                return;
            }
            Some(block_info) => block_info,
        };

        block_info.requested_from.remove(peer.handle());
        block_info.rejected_by.insert(*peer.handle());

        if block_info.requested_from.is_empty() {
            block_info.state = BlockState::None;
        }
    }

    /// Request interesting pieces from the given peer.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn pick_pieces(&mut self, peer: &Peer) {
        let start_time = Instant::now();
        let state = peer.remote_choke_state().await;
        let piece_bitfield = peer.remote_piece_bitfield().await;
        let fast_bitfield = peer.remote_fast_bitfield().await;
        let suggested = peer.suggested_pieces().await;
        let desired_queue_len = 100usize; // TODO: the peer should tell us it's desired queue size

        let peer_info = PeerInfo {
            choke_state: state,
            fast_bitfield,
            suggested_pieces: suggested.as_slice(),
        };
        let mut num_requested_blocks = 0usize;
        for strategy in self.strategies.iter() {
            let interested_pieces = self.interested_pieces(&piece_bitfield);
            let picked_blocks = strategy
                .pick_pieces(peer, &peer_info, interested_pieces, self.options)
                .await;

            // convert the piece blocks into request slices that can be requested from the peer
            let requests = picked_blocks
                .into_iter()
                .take(desired_queue_len.saturating_sub(num_requested_blocks))
                .map(|block| {
                    num_requested_blocks += 1;
                    (block.piece, block)
                })
                .into_group_map();

            for (piece, blocks) in requests {
                let piece_info = match self.downloads.get_mut(&piece) {
                    None => {
                        debug!(
                            "Piece picker {} couldn't find download piece {:?}",
                            self, piece
                        );
                        continue;
                    }
                    Some(piece_info) => piece_info,
                };

                match peer.request(piece, &blocks).await {
                    Ok(_) => {
                        // mark all blocks as being downloaded
                        piece_info.state = PieceState::Downloading;
                        for block in &blocks {
                            let block_info = match piece_info.blocks.get_mut(&block.block) {
                                None => {
                                    debug!(
                                        "Piece picker couldn't find download block {}",
                                        block.block
                                    );
                                    continue;
                                }
                                Some(block_info) => block_info,
                            };

                            block_info.state = BlockState::Requested;
                            block_info.requested_from.insert(*peer.handle());
                        }

                        debug!(
                            "Piece picker {} requested {} blocks from peer {}",
                            self,
                            blocks.len(),
                            peer
                        );
                    }
                    Err(e) => {
                        debug!(
                            "Piece picker {} failed to request block from peer {}, {}",
                            self, peer, e
                        );
                    }
                }
            }
        }

        let elapsed = start_time.elapsed();
        trace!(
            "Piece picker {} picked pieces in {:.3}ms",
            self,
            elapsed.as_secs_f64() * 1000.0
        );
    }

    /// Tick the piece picker.
    pub async fn tick<'a, P: Iterator<Item = &'a Peer>>(&mut self, peers: P) {
        let start_time = Instant::now();
        for peer in peers {
            if matches!(
                peer.state().await,
                PeerState::Handshake | PeerState::Error | PeerState::Closed
            ) {
                continue;
            }

            self.pick_pieces(peer).await;
        }

        let elapsed = start_time.elapsed();
        trace!(
            "Piece picker {} tick took {:.3}ms",
            self,
            elapsed.as_secs_f64() * 1000.0
        );
    }

    /// Try to write the piece block data to the cache.
    async fn write_block(&mut self, block: &PieceBlock, data: Vec<u8>) -> bool {
        let piece = match self.downloads.get_mut(&block.piece) {
            None => {
                debug!(
                    "Piece picker {} couldn't find download piece {:?}",
                    self, block
                );
                return false;
            }
            Some(info) => info,
        };
        let block_info = match piece.blocks.get_mut(&block.block) {
            None => {
                debug!(
                    "Piece picker {} couldn't find download block {:?}",
                    self, block
                );
                return false;
            }
            Some(block) => block,
        };

        match self.cache.write(&block, data).await {
            Ok(()) => {
                block_info.state = BlockState::Finished;
            }
            Err(e) => {
                warn!(
                    "Piece picker failed to write piece {} block {} data, {}",
                    block.piece, block.block, e
                );
                block_info.state = BlockState::None;
                return false;
            }
        }

        true
    }

    /// Try to process a completed piece.
    async fn on_piece_completed(&mut self, piece_index: &PieceIndex) {
        let piece_info = match self.downloads.get_mut(piece_index) {
            None => {
                self.cache.discard(piece_index);
                return;
            }
            Some(piece) => piece,
        };
        let piece = match self.data_pool.piece(piece_index).await {
            None => {
                self.cache.discard(piece_index);
                return;
            }
            Some(piece) => piece,
        };

        // flush the piece data
        piece_info.state = PieceState::Validating;
        if let Err(e) = self.cache.flush(piece_index).await {
            warn!(
                "Piece picker failed to flush piece {} data, {}",
                piece_index, e
            );
            Self::invalidate_piece(piece_info);
            self.cache.discard(piece_index);
            return;
        }

        match (piece.hash.has_v1(), piece.hash.has_v2()) {
            (true, true) | (false, true) => {
                self.torrent
                    .piece_verified(
                        &piece.index,
                        None,
                        self.storage.hash_v2(&piece.index).await.ok(),
                    )
                    .await;
            }
            (true, false) => {
                self.torrent
                    .piece_verified(
                        &piece.index,
                        self.storage.hash_v1(&piece.index).await.ok(),
                        None,
                    )
                    .await;
            }
            (false, false) => {
                debug!(
                    "Piece picker is unable to validate piece {}, piece hash is missing or invalid",
                    piece.index
                );
                Self::invalidate_piece(piece_info);
            }
        }
    }

    /// Add the given piece to the download queue.
    fn add_piece_to_queue(&mut self, piece: &Piece) {
        if self.downloads.contains_key(&piece.index) {
            return;
        }

        self.downloads.insert(piece.index, piece.into());
    }

    /// Returns a list of pieces in which the picker is interested in downloading from the peer.
    fn interested_pieces<'a>(
        &'a self,
        piece_bitfield: &'a BitVec,
    ) -> impl Iterator<Item = &'a PieceInfo> {
        self.downloads
            .iter()
            .filter(move |&(&index, piece)| {
                // filter out pieces with priority None
                if piece.priority == PiecePriority::None {
                    return false;
                }
                // filter out pieces which are already downloaded
                if matches!(piece.state, PieceState::Validating | PieceState::Finished) {
                    return false;
                }

                // filter out pieces that are not available on the remote peer
                piece_bitfield
                    .get(index)
                    .as_deref()
                    .map(|b| *b)
                    .unwrap_or_default()
            })
            .map(|(_, piece)| piece)
    }

    /// Returns `true` if all blocks of the piece have been received, else `false`.
    /// This doesn't verify if the data of the piece is valid.
    fn is_piece_complete(&self, piece: &PieceIndex) -> bool {
        let piece = match self.downloads.get(piece) {
            None => return false,
            Some(piece) => piece,
        };

        !piece
            .blocks
            .iter()
            .any(|(_, block)| matches!(block.state, BlockState::None | BlockState::Requested))
    }

    /// Invalidate the given piece.
    /// This resets the state of the piece to be downloaded again.
    fn invalidate_piece(piece: &mut PieceInfo) {
        piece.state = PieceState::None;
        piece
            .blocks
            .iter_mut()
            .for_each(|(_, block)| block.state = BlockState::None);
    }
}

#[derive(Debug)]
pub struct PieceInfo {
    /// The index of the piece.
    pub piece: PieceIndex,
    /// The priority of the piece.
    pub priority: PiecePriority,
    /// The blocks of the piece.
    pub blocks: HashMap<BlockIndex, BlockInfo>,
    /// The number of peers that have this piece available.
    pub availability: usize,
    /// The current state of the piece.
    pub state: PieceState,
}

impl From<&Piece> for PieceInfo {
    fn from(piece: &Piece) -> Self {
        PieceInfo {
            piece: piece.index,
            priority: piece.priority,
            blocks: piece
                .blocks
                .iter()
                .map(|block| {
                    (
                        block.block,
                        BlockInfo {
                            block: block.clone(),
                            requested_from: Default::default(),
                            rejected_by: Default::default(),
                            state: BlockState::None,
                        },
                    )
                })
                .collect(),
            availability: 0,
            state: PieceState::None,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum PieceState {
    /// The piece is idle/not yet being downloaded.
    None,
    /// One or more blocks of the piece are being downloaded.
    Downloading,
    /// All blocks of the piece have been downloaded and are being validated.
    Validating,
    /// The piece has been validated and written to storage.
    Finished,
}

#[derive(Debug)]
pub struct BlockInfo {
    pub block: PieceBlock,
    /// The peers this block is requested from.
    /// This is most of the time one peer, but can be multiple during the end-game.
    pub requested_from: HashSet<PeerHandle>,
    /// The peers that have rejected this block.
    /// We should not retry to request this block from these peers.
    pub rejected_by: HashSet<PeerHandle>,
    /// The current state of the block.
    pub state: BlockState,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum BlockState {
    /// Block has not yet been requested from a peer.
    None,
    /// Block is being requested from at least one peer.
    Requested,
    /// Block has been received from at least one peer.
    Finished,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_set_pieces() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (context, _rx) = torrent_context!(
            "debian.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::default(),
            vec![],
            vec![],
            None
        );
        let pieces = pieces!(2, 1536, 1024);
        let mut picker = piece_picker!(&context, 32);

        // set the pieces for the picker
        picker.set_pieces(&pieces);

        assert_eq!(
            2,
            picker.downloads.len(),
            "expected the pieces to be converted into downloads"
        );
        assert_eq!(
            PiecePriority::default(),
            picker.downloads[&0].priority,
            "expected the default priority to be set on the pieces"
        );
        assert_eq!(
            1,
            picker.downloads[&0].blocks.len(),
            "expected 1 block to be present within the first piece"
        );
    }

    #[tokio::test]
    async fn test_set_priority() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (context, _rx) = torrent_context!(
            "debian.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::default(),
            vec![],
            vec![],
            None
        );
        let pieces = pieces!(3, 3072, 1024);
        let mut picker = piece_picker!(&context, 32);

        picker.set_pieces(&pieces);
        picker.set_priority(&0, PiecePriority::None);
        picker.set_priority(&1, PiecePriority::High);

        assert_eq!(3, picker.downloads.len());
        assert_eq!(PiecePriority::None, picker.downloads[&0].priority);
        assert_eq!(PiecePriority::High, picker.downloads[&1].priority);
        assert_eq!(PiecePriority::Normal, picker.downloads[&2].priority);
    }

    #[tokio::test]
    async fn test_set_completed() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (context, _rx) = torrent_context!(
            "debian.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::default(),
            vec![],
            vec![],
            None
        );
        let pieces = pieces!(3, 3072, 1024);
        let mut picker = piece_picker!(&context, 32);

        picker.set_pieces(&pieces);

        // set the completion of a known piece
        picker.set_completed(&0);
        {
            let piece = &picker.downloads[&0];
            assert_eq!(PieceState::Finished, piece.state);
            assert_eq!(1, piece.blocks.len());
            assert_eq!(BlockState::Finished, piece.blocks[&0].state);
        }

        // set the completion of an unknown piece
        picker.set_completed(&9);
        {
            let piece = &picker.downloads[&9];
            assert_eq!(PieceState::Finished, piece.state);
            assert_eq!(0, piece.blocks.len());
        }
    }

    mod piece_info {
        use super::*;
        use crate::InfoHash;
        use std::str::FromStr;

        #[test]
        fn test_from_piece_ref() {
            let info_hash =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let piece = Piece::new(info_hash, 1, 32_768, 32_768);

            let result = PieceInfo::from(&piece);

            assert_eq!(
                piece.index, result.piece,
                "expected the piece index to match"
            );
            assert_eq!(
                piece.blocks.len(),
                result.blocks.len(),
                "expected the piece blocks to match"
            );
            assert_eq!(
                piece.priority, result.priority,
                "expected the piece priority to match"
            );
            assert_eq!(
                PieceState::None,
                result.state,
                "expected the piece state to be None"
            );
        }
    }
}
