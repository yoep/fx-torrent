use crate::piece_picker::{PickerOptions, PieceInfo};
use crate::{PieceBlock, PiecePriority};
use itertools::Itertools;

#[derive(Debug)]
pub struct PriorityStrategy;

impl PriorityStrategy {
    pub fn new() -> Self {
        PriorityStrategy
    }

    /// Returns the interesting pieces which should be downloaded from the peer,
    /// according to this strategy.
    pub(crate) fn pick_pieces<'a>(
        &'a self,
        pieces: impl IntoIterator<Item = &'a PieceInfo>,
        options: PickerOptions,
    ) -> Vec<PieceBlock> {
        if !options.contains(PickerOptions::Priority) {
            return vec![];
        }

        pieces
            .into_iter()
            .filter(|piece| piece.priority != PiecePriority::None)
            .sorted_by(|a, b| b.priority.cmp(&a.priority))
            .map(|piece| {
                piece
                    .blocks
                    .values()
                    .filter(|block| block.state == crate::piece_picker::BlockState::None)
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
    use crate::PiecePriority;

    #[test]
    fn test_pick_pieces() {
        let mut pieces = piece_infos!(4, 128_000, 32_768);
        pieces[0].priority = PiecePriority::None;
        pieces[1].priority = PiecePriority::Normal;
        pieces[2].priority = PiecePriority::Now;
        pieces[3].priority = PiecePriority::High;
        let strategy = PriorityStrategy::new();

        let result = strategy.pick_pieces(&pieces, PickerOptions::Priority);

        assert_eq!(&pieces[2].blocks[&0].block, &result[0]);
        assert_eq!(&pieces[2].blocks[&1].block, &result[1]);
        assert_eq!(&pieces[3].blocks[&0].block, &result[2]);
        assert_eq!(&pieces[3].blocks[&1].block, &result[3]);
        assert_eq!(&pieces[1].blocks[&0].block, &result[4]);
        assert_eq!(&pieces[1].blocks[&1].block, &result[5]);
    }
    #[test]
    fn test_pick_pieces_option_not_set() {
        let pieces = piece_infos!(4, 128_000, 32_768);
        let strategy = PriorityStrategy::new();

        let result = strategy.pick_pieces(&pieces, PickerOptions::none());

        assert_eq!(0, result.len(), "expected no pieces to have been picked");
    }
}
