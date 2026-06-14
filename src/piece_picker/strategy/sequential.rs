use crate::piece_picker::{PickerOptions, PieceBlockState, PiecePickerBlock};
use itertools::Itertools;

#[derive(Debug)]
pub struct SequentialStrategy;

impl SequentialStrategy {
    pub fn new() -> Self {
        SequentialStrategy
    }

    pub(crate) fn pick_pieces<'a>(
        &self,
        blocks: &'a Vec<PiecePickerBlock>,
        target_queue_len: usize,
        is_end_game: bool,
        options: PickerOptions,
    ) -> Vec<&'a PiecePickerBlock> {
        if !options.contains(PickerOptions::Sequential) {
            return vec![];
        }

        blocks
            .into_iter()
            .filter(|block| is_end_game || block.state == PieceBlockState::None)
            .sorted_by(|a, b| a.piece().cmp(b.piece()))
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
        let strategy = SequentialStrategy::new();

        let result = strategy.pick_pieces(&blocks, 4, false, PickerOptions::Sequential);

        assert_eq!(
            4,
            result.len(),
            "expected 4 piece blocks to have been picked"
        );
        assert_eq!(&blocks[0], result[0]);
        assert_eq!(&blocks[1], result[1]);
        assert_eq!(&blocks[2], result[2]);
        assert_eq!(&blocks[3], result[3]);
    }
}
