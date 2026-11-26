use crate::piece_picker::PiecePickerBlock;
use crate::PieceBlock;
use itertools::Itertools;
use std::cmp::Ordering;

/// Returns the `k` smallest blocks of the iterator according to `cmp`, in sorted order.
///
/// This uses a partial selection (`O(n + k log k)`) instead of sorting all items (`O(n log n)`),
/// which matters as the number of candidate blocks is much larger than the requested queue length.
/// The `cmp` should be a total order for a deterministic result.
pub fn select_top<'a, T, F>(items: T, k: usize, mut cmp: F) -> Vec<PieceBlock>
where
    T: IntoIterator<Item = &'a PiecePickerBlock>,
    F: FnMut(&PiecePickerBlock, &PiecePickerBlock) -> Ordering,
{
    if k == 0 {
        return Vec::new();
    }

    let mut items = items.into_iter().collect_vec();
    if items.len() > k {
        items.select_nth_unstable_by(k - 1, |a, b| cmp(a, b));
        items.truncate(k);
    }
    items.sort_unstable_by(|a, b| cmp(a, b));
    items
        .into_iter()
        .map(|block| block.piece_block)
        .collect_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Order the blocks on their availability, so the test can control the order of the blocks.
    fn by_availability(a: &PiecePickerBlock, b: &PiecePickerBlock) -> Ordering {
        a.availability.cmp(&b.availability)
    }

    #[test]
    fn test_select_top() {
        let mut blocks = piece_blocks!(4, 128_000, 32_768);
        let availabilities = [5, 1, 9, 3, 7, 2];
        for (block, availability) in blocks.iter_mut().zip(availabilities) {
            block.availability = availability;
        }

        let result = select_top(blocks.iter().take(availabilities.len()), 3, by_availability);

        assert_eq!(
            vec![
                blocks[1].piece_block,
                blocks[5].piece_block,
                blocks[3].piece_block
            ],
            result
        );
    }

    #[test]
    fn test_select_top_k_larger_than_len() {
        let mut blocks = piece_blocks!(1, 98_304, 98_304);
        let availabilities = [3, 1, 2];
        for (block, availability) in blocks.iter_mut().zip(availabilities) {
            block.availability = availability;
        }

        let result = select_top(blocks.iter().take(3), 10, by_availability);

        assert_eq!(
            vec![
                blocks[1].piece_block,
                blocks[2].piece_block,
                blocks[0].piece_block
            ],
            result
        );
    }

    #[test]
    fn test_select_top_zero() {
        let blocks = piece_blocks!(1, 98_304, 98_304);

        let result = select_top(blocks.iter(), 0, by_availability);

        assert!(result.is_empty());
    }
}
