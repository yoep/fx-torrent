use crate::piece_picker::strategy::RarestFirstStrategy;
use crate::piece_picker::{PickerOptions, PieceBlockState, PiecePickerBlock};
use itertools::Itertools;
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
        blocks: &'a Vec<PiecePickerBlock>,
        target_queue_len: usize,
        is_end_game: bool,
        options: PickerOptions,
    ) -> Vec<&'a PiecePickerBlock> {
        if !options.contains(PickerOptions::Priority) {
            return vec![];
        }

        blocks
            .iter()
            .filter(|block| is_end_game || block.state == PieceBlockState::None)
            .sorted_by(|a, b| {
                let order = b.priority.cmp(&a.priority);
                if order == Ordering::Equal && options.contains(PickerOptions::RarestFirst) {
                    return RarestFirstStrategy::sort(a, b);
                }

                order
            })
            .take(target_queue_len)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PiecePriority;

    #[test]
    fn test_pick_pieces() {
        let mut blocks = piece_infos!(4, 128_000, 32_768);
        blocks
            .iter_mut()
            .for_each(|block| block.priority = PiecePriority::None);
        blocks[1].priority = PiecePriority::Normal;
        blocks[2].priority = PiecePriority::Now;
        blocks[5].priority = PiecePriority::High;
        let strategy = PriorityStrategy::new();

        let result = strategy.pick_pieces(&blocks, 10, false, PickerOptions::Priority);

        assert_eq!(&blocks[2], result[0]);
        assert_eq!(&blocks[5], result[1]);
        assert_eq!(&blocks[1], result[2]);
    }

    #[test]
    fn test_pick_pieces_option_not_set() {
        let mut blocks = piece_infos!(4, 128_000, 32_768);
        let strategy = PriorityStrategy::new();

        let result = strategy.pick_pieces(&mut blocks, 10, false, PickerOptions::none());

        assert_eq!(0, result.len(), "expected no pieces to have been picked");
    }
}
