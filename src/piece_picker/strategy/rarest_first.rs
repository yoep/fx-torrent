use crate::piece_picker::{BlockState, PickerOptions, PieceInfo};
use crate::PieceBlock;
use itertools::Itertools;

#[derive(Debug)]
pub struct RarestFirstStrategy;

impl RarestFirstStrategy {
    pub fn new() -> Self {
        RarestFirstStrategy
    }

    /// Returns the interesting pieces which should be downloaded from the peer,
    /// according to this strategy.
    pub(crate) fn pick_pieces<'a>(
        &'a self,
        pieces: impl IntoIterator<Item = &'a PieceInfo>,
        options: PickerOptions,
    ) -> Vec<PieceBlock> {
        if !options.contains(PickerOptions::RarestFirst) {
            return vec![];
        }

        pieces
            .into_iter()
            .sorted_by(|a, b| a.availability.cmp(&b.availability))
            .map(|piece| {
                piece
                    .blocks
                    .values()
                    .filter(|block| block.state == BlockState::None)
                    .map(|block| block.block.clone())
                    .sorted()
                    .collect_vec()
            })
            .flatten()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pick_pieces() {
        let mut pieces = piece_infos!(4, 128_000, 32_768);
        pieces[0].availability = 100;
        pieces[1].availability = 9;
        pieces[2].availability = 13;
        pieces[3].availability = 99;
        let strategy = RarestFirstStrategy::new();

        let result = strategy.pick_pieces(pieces.iter(), PickerOptions::RarestFirst);

        assert_eq!(
            &pieces[1].blocks[&0].block, &result[0],
            "expected piece 1 first block"
        );
        assert_eq!(
            &pieces[1].blocks[&1].block, &result[1],
            "expected piece 1 last block"
        );
        assert_eq!(&pieces[2].blocks[&0].block, &result[2], "expected piece 2");
        assert_eq!(&pieces[2].blocks[&1].block, &result[3], "expected piece 2");
    }

    #[test]
    fn test_pick_pieces_option_not_set() {
        let pieces = piece_infos!(4, 128_000, 32_768);
        let strategy = RarestFirstStrategy::new();

        let result = strategy.pick_pieces(pieces.iter(), PickerOptions::none());

        assert_eq!(0, result.len(), "expected no pieces to have been picked");
    }
}
