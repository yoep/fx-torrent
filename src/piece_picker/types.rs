use crate::peer::Peer;
use crate::piece_picker::picker::FxPiecePicker;
use crate::{Piece, PieceBlock, PieceIndex, PiecePriority};
use async_trait::async_trait;
use bitmask_enum::bitmask;
use itertools::Itertools;
use std::fmt::Debug;

/// The piece picker used by a torrent.
/// This picker determines the order in which pieces or downloaded
/// and the peers they're requested from.
#[derive(Debug)]
pub enum PiecePicker {
    Picker(FxPiecePicker),
    Other(Box<dyn Extension>),
}

impl PiecePicker {
    /// Returns the options set for the piece picker.
    pub fn options(&self) -> &PickerOptions {
        match self {
            PiecePicker::Picker(picker) => picker.options(),
            PiecePicker::Other(picker) => picker.options(),
        }
    }

    /// Set the pieces of the torrent to pick from.
    /// This replaces any previously set pieces within the picker.
    pub fn set_pieces(&mut self, pieces: &[Piece]) {
        match self {
            PiecePicker::Picker(picker) => picker.set_pieces(pieces),
            PiecePicker::Other(picker) => picker.set_pieces(pieces),
        }
    }

    /// Set the priority for the given piece.
    /// This overrides any previously set priority for the piece.
    pub fn set_priority(&mut self, piece: &PieceIndex, priority: PiecePriority) {
        match self {
            PiecePicker::Picker(picker) => picker.set_priority(piece, priority),
            PiecePicker::Other(picker) => picker.set_priority(piece, priority),
        }
    }

    /// Set the given piece as completed.
    /// This means that the piece has been validated and written to storage.
    pub fn set_completed(&mut self, piece: &PieceIndex) {
        match self {
            PiecePicker::Picker(picker) => picker.set_completed(piece),
            PiecePicker::Other(picker) => picker.set_completed(piece),
        }
    }

    /// Set the given piece as failed,
    /// resetting the state of the piece to be downloaded again.
    ///
    /// This is typically called when a piece fails its cryptographic hash verification
    /// after download, or if the underlying storage encounters a corruption error.
    pub fn set_failed(&mut self, piece: &PieceIndex) {
        match self {
            PiecePicker::Picker(picker) => picker.set_failed(piece),
            PiecePicker::Other(picker) => picker.set_failed(piece),
        }
    }

    /// Set the options for the piece picker.
    /// This replaces any previously set options.
    pub fn set_options(&mut self, options: PickerOptions) {
        match self {
            PiecePicker::Picker(picker) => picker.set_options(options),
            PiecePicker::Other(picker) => picker.set_options(options),
        }
    }

    /// Add the given options of the piece picker.
    pub fn add_options(&mut self, options: PickerOptions) {
        match self {
            PiecePicker::Picker(picker) => picker.add_options(options),
            PiecePicker::Other(picker) => picker.add_options(options),
        }
    }

    /// Remove the given options from the piece picker.
    pub fn remove_options(&mut self, options: PickerOptions) {
        match self {
            PiecePicker::Picker(picker) => picker.remove_options(options),
            PiecePicker::Other(picker) => picker.remove_options(options),
        }
    }

    /// Process the data for a piece block that has been downloaded from a peer.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn block_received(&mut self, peer: &Peer, block: PieceBlock, data: Vec<u8>) {
        match self {
            PiecePicker::Picker(picker) => picker.block_received(peer, block, data).await,
            PiecePicker::Other(picker) => picker.block_received(peer, block, data).await,
        }
    }

    /// Process a piece block request that has been rejected by the peer.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn block_rejected(&mut self, peer: &Peer, block: PieceBlock) {
        match self {
            PiecePicker::Picker(picker) => picker.block_rejected(peer, block),
            PiecePicker::Other(picker) => picker.block_rejected(peer, block).await,
        }
    }

    /// Pick interesting pieces for the given peer.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn pick_pieces(&mut self, peer: &Peer) {
        match self {
            PiecePicker::Picker(picker) => picker.pick_pieces(peer).await,
            PiecePicker::Other(picker) => picker.pick_pieces(peer).await,
        }
    }

    /// Execute a periodic tick for the piece picker.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn tick<'a, P: Iterator<Item = &'a Peer>>(&mut self, peers: P) {
        match self {
            PiecePicker::Picker(picker) => picker.tick(peers).await,
            PiecePicker::Other(picker) => picker.tick(peers.collect_vec()).await,
        }
    }
}

impl From<FxPiecePicker> for PiecePicker {
    fn from(value: FxPiecePicker) -> Self {
        Self::Picker(value)
    }
}

impl<E> From<E> for PiecePicker
where
    E: Extension + 'static,
{
    fn from(value: E) -> Self {
        Self::Other(Box::new(value))
    }
}

#[async_trait]
pub trait Extension: Debug + Send + Sync {
    /// Returns the options set for the piece picker.
    fn options(&self) -> &PickerOptions;

    /// Set the pieces of the torrent to pick from.
    /// This should replace any previously set pieces within the picker.
    fn set_pieces<'a>(&'a mut self, pieces: &'a [Piece]);

    /// Set the priority for the given piece.
    /// This should override any previously configured priorities for the piece.
    fn set_priority(&mut self, piece: &PieceIndex, priority: PiecePriority);

    /// Set the given piece as completed.
    /// This means that the piece has been validated and written to storage.
    fn set_completed(&mut self, piece: &PieceIndex);

    /// Set the given piece as failed,
    /// resetting the state of the piece to be downloaded again.
    ///
    /// This is typically called when a piece fails its cryptographic hash verification
    /// after download, or if the underlying storage encounters a corruption error.
    fn set_failed(&mut self, piece: &PieceIndex);

    /// Set the options for the piece picker.
    /// This replaces any previously set options.
    fn set_options(&mut self, options: PickerOptions);

    /// Add the given options of the piece picker.
    fn add_options(&mut self, options: PickerOptions);

    /// Remove the given options from the piece picker.
    fn remove_options(&mut self, options: PickerOptions);

    /// Process the data for a piece block that has been downloaded from a peer.
    async fn block_received<'a>(&'a mut self, peer: &'a Peer, block: PieceBlock, data: Vec<u8>);

    /// Process a piece block request that has been rejected by the peer.
    async fn block_rejected(&mut self, peer: &Peer, block: PieceBlock);

    /// Pick interesting pieces for the given peer.
    async fn pick_pieces(&mut self, peer: &Peer);

    /// Execute a periodic tick for the piece picker.
    /// This tick can be used to request pieces from the list of available peers.
    async fn tick<'a>(&'a mut self, peers: Vec<&'a Peer>);
}

/// The options of the piece picker.
#[bitmask(u8)]
#[bitmask_config(vec_debug, flags_iter)]
pub enum PickerOptions {
    /// Pick the pieces which are the least available.
    /// This option is exclusive with [PickerOptions::MostAvailable].
    RarestFirst,
    /// Pick the pieces which have the highest availability.
    /// This option is exclusive with [PickerOptions::RarestFirst].
    MostAvailable,
    /// Pick only pieces which are suggested by the peer.
    SuggestedOnly,
    /// Pick the pieces according to their priority.
    Priority,
    /// Pick the pieces which have been partially downloaded first.
    PrioritizePartials,
    /// Pick the pieces in sequential order.
    Sequential,
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockall::mock;

    mock! {
        #[derive(Debug)]
        pub PiecePickerExtension {}

        #[async_trait]
        impl Extension for PiecePickerExtension {
            fn options(&self) -> &PickerOptions;
            fn set_pieces<'a>(&'a mut self, pieces: &'a [Piece]);
            fn set_priority(&mut self, piece: &PieceIndex, priority: PiecePriority);
            fn set_completed(&mut self, piece: &PieceIndex);
            fn set_failed(&mut self, piece: &PieceIndex);
            fn set_options(&mut self, options: PickerOptions);
            fn add_options(&mut self, options: PickerOptions);
            fn remove_options(&mut self, options: PickerOptions);
            async fn block_received<'a>(&'a mut self, peer: &'a Peer, block: PieceBlock, data: Vec<u8>);
            async fn block_rejected(&mut self, peer: &Peer, block: PieceBlock);
            async fn pick_pieces(&mut self, peer: &Peer);
            async fn tick<'a>(&'a mut self, peers: Vec<&'a Peer>);
        }
    }

    #[tokio::test]
    async fn test_piece_picker_from_extension() {
        let peers: Vec<Peer> = vec![];
        let mut extension = MockPiecePickerExtension::new();
        extension.expect_tick().times(1).return_const(());

        let mut picker: PiecePicker = extension.into();

        picker.tick(peers.iter()).await;
    }

    #[test]
    fn test_set_pieces() {
        let piece = Piece {
            hash: Default::default(),
            index: 0,
            offset: 0,
            length: 0,
            priority: Default::default(),
            blocks: vec![],
            availability: 0,
        };
        let mut extension = MockPiecePickerExtension::new();
        extension.expect_set_pieces().times(1).return_const(());
        let mut picker: PiecePicker = extension.into();

        picker.set_pieces(&[piece]);
    }
}
