use crate::peer::{ChokeState, Peer};
use crate::piece_picker::strategy::rarest_first::RarestFirstStrategy;
use crate::piece_picker::strategy::PriorityStrategy;
use crate::piece_picker::{PickerOptions, PieceInfo};
use crate::{BitVec, PieceBlock, PieceIndex};
use async_trait::async_trait;
use itertools::Itertools;
use std::fmt::Debug;

/// The information about the state of the peer.
#[derive(Debug)]
pub struct PeerInfo<'a> {
    /// The choke state of the remote peer.
    pub choke_state: ChokeState,
    /// The piece bitfield of the remote peer that are allowed to be downloaded,
    /// even when the remote peer is choked.
    pub fast_bitfield: BitVec,
    /// The suggested pieces to download by the remote peer.
    pub suggested_pieces: &'a [PieceIndex],
}

/// The piece picker strategy.
#[derive(Debug)]
pub enum Strategy {
    RarestFirst(RarestFirstStrategy),
    Priority(PriorityStrategy),
    Other(Box<dyn Extension>),
}

impl Strategy {
    /// Returns the interesting pieces, sorted by the strategy,
    /// which should be downloaded from the peer.
    ///
    /// ## Notes
    ///
    /// The `pieces` are already filtered on available pieces of the peer,
    /// and therefore should not be filtered again.
    ///
    /// Picking `blocks` which are already being [crate::piece_picker::BlockState::Requested]
    /// is allowed, especially during the endgame phase of the torrent.
    pub async fn pick_pieces<'a>(
        &'a self,
        peer: &Peer,
        peer_info: &'a PeerInfo<'a>,
        pieces: impl IntoIterator<Item = &'a PieceInfo>,
        options: PickerOptions,
    ) -> Vec<PieceBlock> {
        match self {
            Strategy::RarestFirst(strategy) => strategy.pick_pieces(pieces, options),
            Strategy::Priority(strategy) => strategy.pick_pieces(pieces, options),
            Strategy::Other(strategy) => {
                strategy
                    .pick_pieces(peer, peer_info, pieces.into_iter().collect_vec(), options)
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
    /// Returns the interesting pieces, sorted by the strategy,
    /// which should be downloaded from the peer.
    ///
    /// ## Note
    ///
    /// The `pieces` are already filtered on the available pieces of the peer,
    /// and therefore should not be filtered again.
    async fn pick_pieces<'a>(
        &'a self,
        peer: &Peer,
        peer_info: &'a PeerInfo<'a>,
        pieces: Vec<&PieceInfo>,
        options: PickerOptions,
    ) -> Vec<PieceBlock>;
}
