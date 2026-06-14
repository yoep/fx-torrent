use crate::peer::Peer;
use crate::piece_picker::strategy::rarest_first::RarestFirstStrategy;
use crate::piece_picker::strategy::PriorityStrategy;
use crate::piece_picker::{PickerOptions, PiecePickerBlock};
use crate::PieceIndex;
use async_trait::async_trait;
use std::fmt::Debug;

/// The piece picker strategy.
#[derive(Debug)]
pub enum Strategy {
    RarestFirst(RarestFirstStrategy),
    Priority(PriorityStrategy),
    Other(Box<dyn Extension>),
}

impl Strategy {
    /// Returns the unique strategy name.
    pub fn name(&self) -> &str {
        match self {
            Strategy::RarestFirst(_) => "rarest_first",
            Strategy::Priority(_) => "priority",
            Strategy::Other(_) => "other",
        }
    }

    /// Returns the picked piece blocks, sorted by the strategy,
    /// which should be downloaded from the peer.
    /// The strategy will try to pick the desired target queue length for the peer.
    ///
    /// ## Notes
    ///
    /// The `blocks` are already filtered on peer availability and
    /// not [crate::piece_picker::PieceBlockState::Finished] therefore,
    /// they should not be filtered again on these criteria.
    ///
    /// Picking `blocks` which are already being [crate::piece_picker::PieceBlockState::Requested]
    /// is allowed, especially during the endgame phase of the torrent.
    pub async fn pick_pieces<'a>(
        &self,
        peer: &Peer,
        blocks: &'a Vec<PiecePickerBlock>,
        target_queue_len: usize,
        suggested_pieces: &[PieceIndex],
        is_end_game: bool,
        options: PickerOptions,
    ) -> Vec<&'a PiecePickerBlock> {
        match self {
            Strategy::RarestFirst(strategy) => {
                strategy.pick_pieces(blocks, target_queue_len, is_end_game, options)
            }
            Strategy::Priority(strategy) => {
                strategy.pick_pieces(blocks, target_queue_len, is_end_game, options)
            }
            Strategy::Other(strategy) => {
                strategy
                    .pick_pieces(
                        peer,
                        blocks,
                        target_queue_len,
                        suggested_pieces,
                        is_end_game,
                        options,
                    )
                    .await
            }
        }
    }
}

impl From<RarestFirstStrategy> for Strategy {
    fn from(rarest_first: RarestFirstStrategy) -> Self {
        Self::RarestFirst(rarest_first)
    }
}

impl From<PriorityStrategy> for Strategy {
    fn from(priority: PriorityStrategy) -> Self {
        Self::Priority(priority)
    }
}

impl<E> From<E> for Strategy
where
    E: Extension + 'static,
{
    fn from(value: E) -> Self {
        Self::Other(Box::new(value))
    }
}

#[async_trait]
pub trait Extension: Debug + Send + Sync {
    /// Returns the picked piece blocks, sorted by the strategy,
    /// which should be downloaded from the peer.
    /// The strategy will try to pick the desired target queue length for the peer.
    ///
    /// ## Note
    ///
    /// The `blocks` are already filtered on peer availability and
    /// not [crate::piece_picker::PieceBlockState::Finished] therefore,
    /// they should not be filtered again on these criteria.
    async fn pick_pieces<'a>(
        &self,
        peer: &Peer,
        blocks: &'a Vec<PiecePickerBlock>,
        target_queue_len: usize,
        suggested_pieces: &[PieceIndex],
        is_end_game: bool,
        options: PickerOptions,
    ) -> Vec<&'a PiecePickerBlock>;
}
