use crate::piece_picker::strategy::select_top;
use crate::piece_picker::strategy::RarestFirstStrategy;
use crate::piece_picker::{PickerOptions, PieceBlockState, PiecePickerBlock};
use crate::PieceBlock;
use std::cmp::Ordering;

#[derive(Debug)]
pub struct PriorityStrategy;

impl PriorityStrategy {
    pub fn new() -> Self {
        PriorityStrategy
    }

    /// Returns the interesting pieces which should be downloaded from the peer,
    /// according to this strategy.
    pub(crate) fn pick_pieces<'a>(
        &self,
        blocks: &[PiecePickerBlock],
        target_queue_len: usize,
        is_end_game: bool,
        options: PickerOptions,
    ) -> Vec<PieceBlock> {
        if !options.contains(PickerOptions::Priority) {
            return vec![];
        }

        let rarest_first = options.contains(PickerOptions::RarestFirst);

        select_top(
            blocks
                .iter()
                .filter(|block| is_end_game || block.state == PieceBlockState::None),
            target_queue_len,
            |a, b| {
                let mut order = b.priority.cmp(&a.priority);
                if order == Ordering::Equal && rarest_first {
                    order = RarestFirstStrategy::sort(a, b);
                }

                // tie-break on the position for a deterministic result
                order.then(a.piece_block.index.cmp(&b.piece_block.index))
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PiecePriority;

    #[test]
    fn test_pick_pieces() {
        let mut blocks = piece_blocks!(4, 128_000, 32_768);
        blocks
            .iter_mut()
            .for_each(|block| block.priority = PiecePriority::None);
        blocks[1].priority = PiecePriority::Normal;
        blocks[2].priority = PiecePriority::Now;
        blocks[5].priority = PiecePriority::High;
        let strategy = PriorityStrategy::new();

        let result = strategy.pick_pieces(&blocks, 10, false, PickerOptions::Priority);

        assert_eq!(blocks[2].piece_block, result[0]);
        assert_eq!(blocks[5].piece_block, result[1]);
        assert_eq!(blocks[1].piece_block, result[2]);
    }

    #[test]
    fn test_pick_pieces_option_not_set() {
        let mut blocks = piece_blocks!(4, 128_000, 32_768);
        let strategy = PriorityStrategy::new();

        let result = strategy.pick_pieces(&mut blocks, 10, false, PickerOptions::none());

        assert_eq!(0, result.len(), "expected no pieces to have been picked");
    }
}
