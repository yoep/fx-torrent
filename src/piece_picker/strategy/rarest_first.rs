use crate::piece_picker::{PickerOptions, PieceBlockState, PiecePickerBlock};
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
        &self,
        blocks: &'a Vec<PiecePickerBlock>,
        target_queue_len: usize,
        is_end_game: bool,
        options: PickerOptions,
    ) -> Vec<&'a PiecePickerBlock> {
        if !options.contains(PickerOptions::RarestFirst) {
            return vec![];
        }

        blocks
            .into_iter()
            .filter(|block| is_end_game || block.state == PieceBlockState::None)
            .sorted_by(|a, b| a.availability.cmp(&b.availability))
            .take(target_queue_len)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pick_pieces() {
        let mut blocks = piece_infos!(4, 128_000, 32_768);
        blocks.iter_mut().for_each(|block| block.availability = 100);
        blocks[1].availability = 9;
        blocks[2].availability = 13;
        blocks[4].availability = 99;
        let strategy = RarestFirstStrategy::new();

        let result = strategy.pick_pieces(&blocks, 10, false, PickerOptions::RarestFirst);

        assert_eq!(&blocks[1], result[0]);
        assert_eq!(&blocks[2], result[1]);
        assert_eq!(&blocks[4], result[2]);
    }

    #[test]
    fn test_pick_pieces_option_not_set() {
        let mut blocks = piece_infos!(4, 128_000, 32_768);
        let strategy = RarestFirstStrategy::new();

        let result = strategy.pick_pieces(&mut blocks, 10, false, PickerOptions::none());

        assert_eq!(0, result.len(), "expected no pieces to have been picked");
    }
}
