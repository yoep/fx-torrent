use crate::peer::{ChokeState, Peer};
use crate::piece_picker::cache::PickerCache;
use crate::piece_picker::strategy::Strategy;
use crate::piece_picker::PickerOptions;
use crate::storage::Storage;
use crate::torrent_data::DataPool;
use crate::{BitVec, BlockIndex, Piece, PieceBlock, PieceIndex, PiecePriority, TorrentHandle};
use derive_more::Display;
use futures::future::Either;
use itertools::Itertools;
use log::{debug, trace, warn};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::time::timeout;

/// The FX piece picker implementation for retrieving pieces from peers.
///
/// This piece picker makes use of multiple strategies for
/// optimizing piece selection and prioritization.
#[derive(Debug, Display)]
#[display("{}", self.handle)]
pub struct FxPiecePicker {
    handle: TorrentHandle,
    options: PickerOptions,
    max_outstanding_pieces: usize,
    downloads: HashMap<PieceIndex, Vec<PiecePickerBlock>>,
    cache: PickerCache,
    data_pool: DataPool,
    strategies: Vec<Strategy>,
}

impl FxPiecePicker {
    /// Create a new piece picker instance.
    pub fn new(
        handle: TorrentHandle,
        data_pool: DataPool,
        storage: Storage,
        strategies: Vec<Strategy>,
        cache_limit: usize,
        max_outstanding_pieces: usize,
        options: PickerOptions,
    ) -> Self {
        Self {
            handle,
            options,
            max_outstanding_pieces,
            downloads: Default::default(),
            cache: PickerCache::new(storage, cache_limit),
            data_pool,
            strategies,
        }
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
        let blocks = match self.downloads.get_mut(piece) {
            None => return,
            Some(blocks) => blocks,
        };
        blocks
            .iter_mut()
            .for_each(|block| block.priority = priority);
    }

    /// Set the given piece as completed.
    /// This means that the piece has been validated and written to storage.
    pub fn set_completed(&mut self, piece: &PieceIndex) {
        let blocks = match self.downloads.get_mut(piece) {
            None => return,
            Some(blocks) => blocks,
        };
        blocks
            .iter_mut()
            .for_each(|block| block.state = PieceBlockState::Finished);
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

    /// Returns the options set for the piece picker.
    pub fn options(&self) -> &PickerOptions {
        &self.options
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

    /// Set the maximum number of outstanding pieces that can be requested.
    pub fn set_max_outstanding(&mut self, max_outstanding_pieces: usize) {
        self.max_outstanding_pieces = max_outstanding_pieces;
    }

    /// Returns `true` if the torrent has reached the end game, else `false`.
    ///
    /// The end game is reached when the last 3 percent, counted with a precision of 2 decimals,
    /// of the pieces are left to be completed.
    pub fn is_end_game(&self) -> bool {
        let (interested_pieces, completed_pieces) =
            self.downloads
                .iter()
                .fold((0, 0), |(interested, completed), (_, blocks)| {
                    let is_completed = blocks
                        .iter()
                        .all(|block| block.state == PieceBlockState::Finished);
                    let priority = blocks
                        .first()
                        .map(|block| block.priority)
                        .unwrap_or(PiecePriority::None);

                    let is_interested = if priority != PiecePriority::None {
                        1
                    } else {
                        0
                    };
                    let is_completed = if is_completed { 1 } else { 0 };

                    (interested + is_interested, completed + is_completed)
                });
        if interested_pieces == 0 {
            return true;
        }

        // if only 3 percent, counted with a precision of 2 decimals, of the pieces are left to be completed,
        // then we enter the end-game phase
        let remaining_pieces = interested_pieces - completed_pieces;
        remaining_pieces * 10_000 <= interested_pieces * 300
    }

    /// Returns `true` if the given piece index has finished downloading all blocks.
    pub fn is_piece_finished(&self, piece: &PieceIndex) -> bool {
        self.downloads
            .get(piece)
            .map(|blocks| blocks.iter().all(|b| b.state == PieceBlockState::Finished))
            .unwrap_or_default()
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
            trace!("Piece picker {} finished piece {}", self, block.piece);
            self.on_piece_completed(&block.piece).await;
        }
    }

    /// Process a piece block request that has been rejected by the peer.
    pub fn block_rejected(&mut self, peer_addr: &SocketAddr, block: PieceBlock) {
        // try to find the block
        let block_info = match self
            .downloads
            .get_mut(&block.piece)
            .and_then(|blocks| blocks.get_mut(block.block))
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

        block_info.requested_from.remove(peer_addr);
        block_info.rejected_by.insert(*peer_addr);

        if block_info.requested_from.is_empty() {
            block_info.state = PieceBlockState::None;
        }
    }

    /// Request interesting pieces from the given peer.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn pick_pieces(&mut self, peer: &Peer) {
        let start_time = Instant::now();

        // early exit if the peer is unable to queue any requests
        let mut target_queue_len =
            match timeout(Duration::from_millis(500), peer.target_request_queue_len()).await {
                Ok(len) => len,
                Err(_) => {
                    warn!(
                        "Piece picker {} failed to get queue len for {}, timed-out",
                        self, peer
                    );
                    return;
                }
            };
        if target_queue_len == 0 {
            return;
        }

        let state = match timeout(Duration::from_millis(250), peer.remote_choke_state()).await {
            Ok(state) => state,
            Err(_) => {
                warn!(
                    "Piece picker {} failed to get remote choke state for {}, timed-out",
                    self, peer
                );
                return;
            }
        };

        // use the fast bitfield if the remote peer is still choked
        // early exit if the peer has no pieces we're interested in
        let bitfield_future = {
            if state == ChokeState::UnChoked {
                Either::Left(peer.remote_piece_bitfield())
            } else {
                Either::Right(peer.remote_fast_bitfield())
            }
        };
        let piece_bitfield = match timeout(Duration::from_millis(250), bitfield_future).await {
            Ok(piece_bitfield) => piece_bitfield,
            Err(_) => {
                warn!(
                    "Piece picker {} failed to get remote bitfield for {}, timed-out",
                    self, peer
                );
                return;
            }
        };
        let mut interested_pieces = self.interested_piece_blocks(&piece_bitfield);
        if interested_pieces.len() == 0 {
            trace!(
                "Piece picker {} found no interested pieces for peer {}",
                self,
                peer
            );
            return;
        }

        let suggested = match timeout(Duration::from_millis(250), peer.suggested_pieces()).await {
            Ok(suggested) => suggested,
            Err(_) => {
                warn!(
                    "Piece picker {} failed to get suggested pieces for {}, timed-out",
                    self, peer
                );
                return;
            }
        };
        let is_end_game = self.is_end_game();

        let mut num_requested_blocks = 0usize;
        for strategy in self.strategies.iter() {
            // exit the strategy loop if the targeted queue length is reached
            if target_queue_len == 0 {
                trace!(
                    "Piece picker {} reached target queue length for {}",
                    self,
                    peer
                );
                break;
            }

            let strategy_start_time = Instant::now();
            let picked_blocks = strategy.pick_pieces(
                peer,
                &interested_pieces,
                target_queue_len,
                suggested.as_slice(),
                is_end_game,
                self.options,
            );
            let elapsed = strategy_start_time.elapsed();
            trace!(
                "Piece picker {} strategy {} picked {} blocks in {:.3}ms",
                self,
                strategy.name(),
                picked_blocks.len(),
                elapsed.as_secs_f64() * 1000.0
            );
            if picked_blocks.is_empty() {
                continue;
            }

            let blocks = picked_blocks
                .iter()
                .map(|block| block.piece_block)
                .collect_vec();
            peer.request(blocks.as_slice()).await;
            debug!(
                "Piece picker {} requested {} blocks from peer {}",
                self,
                picked_blocks.len(),
                peer
            );

            let mut picked_piece_blocks = vec![];
            for block in picked_blocks {
                let block = match self
                    .downloads
                    .get_mut(block.piece())
                    .and_then(|blocks| blocks.get_mut(*block.block()))
                {
                    None => continue,
                    Some(block) => block,
                };

                block.state = PieceBlockState::Requested;
                block.requested_from.insert(*peer.addr());

                // remove the block from the list of interesting pieces
                picked_piece_blocks.push(block.piece_block);

                num_requested_blocks += 1;
                target_queue_len -= 1;
            }

            interested_pieces.retain(|e| !picked_piece_blocks.contains(&e.piece_block));
        }

        let elapsed = start_time.elapsed();
        trace!(
            "Piece picker {} picked {} piece blocks in {:.3}ms",
            self,
            num_requested_blocks,
            elapsed.as_secs_f64() * 1000.0
        );
    }

    /// Tick the piece picker.
    pub async fn tick<'a, P: Iterator<Item = &'a Peer>>(&mut self, peers: P) {
        let start_time = Instant::now();
        for peer in peers {
            let elapsed = start_time.elapsed();
            if elapsed > Duration::from_secs(1) {
                trace!("Piece picker {} has reached time-limit", self);
                break;
            }
            if self.outstanding_pieces_len() > self.max_outstanding_pieces {
                break;
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
        let handle = self.handle;
        let block = match self
            .downloads
            .get_mut(&block.piece)
            .and_then(|blocks| blocks.get_mut(block.block))
        {
            None => {
                debug!(
                    "Piece picker {} couldn't find download block {:?}",
                    self, block
                );
                return false;
            }
            Some(block) => block,
        };

        match self.cache.write(&block.piece_block, data).await {
            Ok(()) => {
                block.state = PieceBlockState::Finished;
            }
            Err(e) => {
                warn!(
                    "Piece picker {} failed to write piece {} block {} data, {}",
                    handle,
                    block.piece(),
                    block.block(),
                    e
                );
                block.state = PieceBlockState::None;
                return false;
            }
        }

        true
    }

    /// Try to process a completed piece.
    async fn on_piece_completed(&mut self, piece_index: &PieceIndex) {
        let handle = format!("{}", self);
        let piece_info = match self.downloads.get_mut(piece_index) {
            None => {
                debug!(
                    "Piece picker {} couldn't find completed piece {}",
                    self, piece_index
                );
                self.cache.discard(piece_index);
                return;
            }
            Some(piece) => piece,
        };

        // flush the piece data
        if let Err(e) = self.cache.flush(piece_index).await {
            warn!(
                "Piece picker failed to flush piece {} data, {}",
                piece_index, e
            );
            Self::invalidate_piece(piece_info);
            self.cache.discard(piece_index);
            return;
        }

        if self.downloads.iter().all(|(_, blocks)| {
            blocks.iter().all(|block| {
                block.priority == PiecePriority::None || block.state == PieceBlockState::Finished
            })
        }) {
            debug!("Piece picker {} completed all interested pieces", handle);
        }
    }

    /// Add the given piece to the download queue.
    fn add_piece_to_queue(&mut self, piece: &Piece) {
        if self.downloads.contains_key(&piece.index) {
            return;
        }

        self.downloads.insert(piece.index, piece.into());
    }

    /// Returns a list of piece blocks in which the picker is interested to download from the peer.
    fn interested_piece_blocks(&self, piece_bitfield: &BitVec) -> Vec<PiecePickerBlock> {
        self.downloads
            .values()
            .flatten()
            .filter_map(|block| {
                // filter out blocks which we're not interested in or have already been requested/completed
                if block.priority == PiecePriority::None || block.state == PieceBlockState::Finished
                {
                    return None;
                }

                // filter out pieces that are not available on the remote peer
                if piece_bitfield
                    .get(*block.piece())
                    .as_deref()
                    .map(|b| *b)
                    .unwrap_or_default()
                {
                    Some(block)
                } else {
                    None
                }
            })
            .cloned()
            .collect_vec()
    }

    /// Returns `true` if all blocks of the piece have been received, else `false`.
    /// This doesn't verify if the data of the piece is valid.
    fn is_piece_complete(&self, piece: &PieceIndex) -> bool {
        self.downloads
            .get(piece)
            .map(|blocks| {
                blocks
                    .iter()
                    .all(|block| block.state == PieceBlockState::Finished)
            })
            .unwrap_or_default()
    }

    /// Returns the total number of outstanding requested pieces.
    fn outstanding_pieces_len(&self) -> usize {
        self.downloads
            .values()
            .filter(|blocks| {
                blocks
                    .iter()
                    .any(|block| block.state == PieceBlockState::Requested)
            })
            .count()
    }

    /// Invalidate the given piece.
    /// This resets the state of the piece to be downloaded again.
    fn invalidate_piece(blocks: &mut Vec<PiecePickerBlock>) {
        for block in blocks {
            block.state = PieceBlockState::None;
        }
    }
}

/// The picker state if a [PieceBlock].
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum PieceBlockState {
    /// Block has not yet been requested from a peer.
    None,
    /// Block is being requested from at least one peer.
    Requested,
    /// Block has been received from at least one peer.
    Finished,
}

/// The state of a piece block within the piece picker.
#[derive(Debug, Clone)]
pub struct PiecePickerBlock {
    /// The piece block information.
    pub piece_block: PieceBlock,
    /// The priority of this block.
    pub priority: PiecePriority,
    /// The number of peers this block is available on.
    pub availability: usize,
    /// The peers this block is requested from.
    /// This is most of the time one peer, but can be multiple during the end-game.
    pub requested_from: HashSet<SocketAddr>,
    /// The peers that have rejected this block.
    /// We should not retry to request this block from these peers.
    pub rejected_by: HashSet<SocketAddr>,
    /// The current state of the block.
    pub state: PieceBlockState,
}

impl PiecePickerBlock {
    /// Returns the piece this block belongs to.
    pub fn piece(&self) -> &PieceIndex {
        &self.piece_block.piece
    }

    /// Returns the block index of this piece block.
    pub fn block(&self) -> &BlockIndex {
        &self.piece_block.block
    }
}

impl PartialEq for PiecePickerBlock {
    fn eq(&self, other: &Self) -> bool {
        self.piece_block == other.piece_block
    }
}

impl From<&Piece> for Vec<PiecePickerBlock> {
    fn from(piece: &Piece) -> Self {
        piece
            .blocks
            .iter()
            .map(|block| PiecePickerBlock {
                piece_block: block.clone(),
                priority: piece.priority,
                availability: 0,
                requested_from: Default::default(),
                rejected_by: Default::default(),
                state: PieceBlockState::None,
            })
            .collect()
    }
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
            picker.downloads[&0][0].priority,
            "expected the default priority to be set on the pieces"
        );
        assert_eq!(
            1,
            picker.downloads[&0].len(),
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
        assert_eq!(PiecePriority::None, picker.downloads[&0][0].priority);
        assert_eq!(PiecePriority::High, picker.downloads[&1][0].priority);
        assert_eq!(PiecePriority::Normal, picker.downloads[&2][0].priority);
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
            let blocks = &picker.downloads[&0];
            assert_eq!(PieceBlockState::Finished, blocks[0].state);
            assert_eq!(1, blocks.len());
            assert_eq!(PieceBlockState::Finished, blocks[0].state);
        }

        // set the completion of an unknown piece
        picker.set_completed(&9);
        assert_eq!(
            None,
            picker.downloads.get(&9),
            "expected unknown piece to not be added"
        );
    }

    mod options {
        use super::*;

        #[tokio::test]
        async fn test_set_options() {
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
            let mut picker = piece_picker!(&context, 32);

            picker.set_options(PickerOptions::RarestFirst | PickerOptions::PrioritizePartials);

            let result = picker.options();
            assert!(
                result.contains(PickerOptions::RarestFirst),
                "expected PickerOptions::RarestFirst"
            );
            assert!(
                result.contains(PickerOptions::PrioritizePartials),
                "expected PickerOptions::PrioritizePartials"
            );
        }

        #[tokio::test]
        async fn test_add_options() {
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
            let mut picker = piece_picker!(&context, 32);

            // set the initial options
            picker.set_options(PickerOptions::RarestFirst);
            assert!(
                picker.options().contains(PickerOptions::RarestFirst),
                "expected PickerOptions::RarestFirst"
            );

            // add options
            picker.add_options(PickerOptions::SuggestedOnly);
            assert!(
                picker
                    .options()
                    .contains(PickerOptions::RarestFirst | PickerOptions::SuggestedOnly),
                "expected PickerOptions::RarestFirst and PickerOptions::SuggestedOnly"
            );
        }

        #[tokio::test]
        async fn test_remove_options() {
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
            let mut picker = piece_picker!(&context, 32);

            // set the initial options
            picker.set_options(
                PickerOptions::RarestFirst
                    | PickerOptions::PrioritizePartials
                    | PickerOptions::Sequential,
            );
            assert!(
                picker.options().contains(PickerOptions::RarestFirst
                    | PickerOptions::PrioritizePartials
                    | PickerOptions::Sequential),
                "expected PickerOptions::RarestFirst, PickerOptions::PrioritizePartials and PickerOptions::Sequential"
            );

            // add options
            picker.add_options(PickerOptions::PrioritizePartials);
            assert!(
                picker
                    .options()
                    .contains(PickerOptions::RarestFirst | PickerOptions::Sequential),
                "expected PickerOptions::RarestFirst and PickerOptions::Sequential"
            );
        }
    }

    mod is_end_game {
        use super::*;

        #[tokio::test]
        async fn test_end_game_not_reached() {
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
            let pieces = pieces!(10, 10_240, 1024);
            let mut picker = piece_picker!(&context, 32);

            picker.set_pieces(&pieces);

            picker.set_completed(&0);
            picker.set_completed(&1);
            picker.set_completed(&2);
            picker.set_completed(&8);
            picker.set_completed(&9);

            let result = picker.is_end_game();
            assert_eq!(false, result, "expected the end game to not be reached");
        }

        #[tokio::test]
        async fn test_end_game_reached() {
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
            let pieces = pieces!(100, 102_400, 1024);
            let mut picker = piece_picker!(&context, 32);

            picker.set_pieces(&pieces);

            for piece in 0..96 {
                picker.set_completed(&piece);
            }
            assert_eq!(
                false,
                picker.is_end_game(),
                "expected the end game to not be reached"
            );

            picker.set_completed(&96);
            let result = picker.is_end_game();
            assert_eq!(true, result, "expected the end game to be reached");
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

            let result = Vec::<PiecePickerBlock>::from(&piece);

            assert_eq!(
                &piece.index,
                result[0].piece(),
                "expected the piece index to match"
            );
            assert_eq!(
                piece.blocks.len(),
                result.len(),
                "expected the piece blocks to match"
            );
            assert_eq!(
                piece.priority, result[0].priority,
                "expected the piece priority to match"
            );
            assert_eq!(
                PieceBlockState::None,
                result[0].state,
                "expected the piece state to be None"
            );
        }
    }
}
