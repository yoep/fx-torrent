use crate::piece_picker::strategy::select_top;
use crate::piece_picker::{PickerOptions, PieceBlockState, PiecePickerBlock};
use crate::PieceBlock;

#[derive(Debug)]
pub struct SequentialStrategy;

impl SequentialStrategy {
    pub fn new() -> Self {
        SequentialStrategy
    }

    pub(crate) fn pick_pieces<'a>(
        &self,
        blocks: &[PiecePickerBlock],
        target_queue_len: usize,
        is_end_game: bool,
        options: PickerOptions,
    ) -> Vec<PieceBlock> {
        if !options.contains(PickerOptions::Sequential) {
            return vec![];
        }

        select_top(
            blocks
                .iter()
                .filter(|block| is_end_game || block.state == PieceBlockState::None),
            target_queue_len,
            // the most urgent pieces are picked first, e.g. the pieces being waited on by a stream
            |a, b| {
                b.priority
                    .cmp(&a.priority)
                    .then(a.piece_block.index.cmp(&b.piece_block.index))
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;

    #[test]
    fn test_pick_pieces() {
        let blocks = piece_blocks!(4, 128_000, 32_768);
        let strategy = SequentialStrategy::new();

        let result = strategy.pick_pieces(&blocks, 4, false, PickerOptions::Sequential);

        assert_eq!(
            4,
            result.len(),
            "expected 4 piece blocks to have been picked"
        );
        assert_eq!(blocks[0].piece_block, result[0]);
        assert_eq!(blocks[1].piece_block, result[1]);
        assert_eq!(blocks[2].piece_block, result[2]);
        assert_eq!(blocks[3].piece_block, result[3]);
    }

    #[test]
    fn test_pick_pieces_priority_first() {
        use crate::PiecePriority;

        let mut blocks = piece_blocks!(4, 128_000, 32_768);
        for block in blocks.iter_mut() {
            block.priority = match block.piece() {
                2 => PiecePriority::Now,
                3 => PiecePriority::Next,
                _ => PiecePriority::High,
            };
        }
        let strategy = SequentialStrategy::new();

        let result = strategy.pick_pieces(&blocks, blocks.len(), false, PickerOptions::Sequential);

        let pieces = result
            .iter()
            .map(|block| block.piece)
            .dedup()
            .collect::<Vec<_>>();
        assert_eq!(
            vec![2, 3, 0, 1],
            pieces,
            "expected the most urgent pieces to be picked first, in sequential order"
        );
    }
}
