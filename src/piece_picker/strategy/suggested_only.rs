use crate::piece_picker::{PickerOptions, PieceBlockState, PiecePickerBlock};
use crate::PieceIndex;
use itertools::Itertools;

#[derive(Debug)]
pub struct SuggestedOnlyStrategy;

impl SuggestedOnlyStrategy {
    pub fn new() -> Self {
        SuggestedOnlyStrategy
    }

    /// Returns the interesting pieces which should be downloaded from the peer,
    /// according to this strategy.
    pub(crate) fn pick_pieces<'a>(
        &self,
        blocks: &'a Vec<PiecePickerBlock>,
        target_queue_len: usize,
        suggested_pieces: &[PieceIndex],
        is_end_game: bool,
        options: PickerOptions,
    ) -> Vec<&'a PiecePickerBlock> {
        if !options.contains(PickerOptions::SuggestedOnly) {
            return vec![];
        }

        blocks
            .iter()
            .filter(|block| {
                suggested_pieces.contains(block.piece())
                    && (is_end_game || block.state == PieceBlockState::None)
            })
            .take(target_queue_len)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pick_pieces() {
        let blocks = piece_infos!(4, 128_000, 32_768);
        let strategy = SuggestedOnlyStrategy::new();

        let result =
            strategy.pick_pieces(&blocks, 10, &[2, 3], false, PickerOptions::SuggestedOnly);

        assert_eq!(
            4,
            result.len(),
            "expected 4 piece blocks to have been picked"
        );
        assert_eq!(&blocks[4], result[0]);
        assert_eq!(&blocks[5], result[1]);
        assert_eq!(&blocks[6], result[2]);
        assert_eq!(&blocks[7], result[3]);
    }
}
