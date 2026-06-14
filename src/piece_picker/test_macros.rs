/// Create a new piece picker instance for the given torrent.
macro_rules! piece_picker {
    ($torrent:expr, $cache_limit:expr) => {{
        piece_picker!($torrent, $cache_limit, vec![])
    }};
    ($torrent:expr, $cache_limit:expr, $strategies:expr) => {{
        piece_picker!(
            $torrent,
            $cache_limit,
            $strategies,
            crate::piece_picker::PickerOptions::Priority
        )
    }};
    ($torrent:expr, $cache_limit:expr, $strategies:expr, $options:expr) => {{
        use crate::piece_picker::strategy::Strategy;
        use crate::piece_picker::FxPiecePicker;
        use crate::piece_picker::PickerOptions;
        use crate::InnerTorrent;
        use crate::TorrentContext;

        let torrent: &TorrentContext = $torrent;
        let strategies: Vec<Strategy> = $strategies;
        let cache_limit: usize = $cache_limit;
        let options: PickerOptions = $options;

        FxPiecePicker::new(
            InnerTorrent::new(
                torrent.handle(),
                torrent.command_sender().clone(),
                torrent.callbacks().clone(),
            ),
            torrent.data_pool().clone(),
            torrent.storage().clone(),
            strategies,
            cache_limit * 1024 * 1024,
            options,
        )
    }};
}

/// Create a set of pieces.
///
/// # Arguments
///
/// * `num_of_pieces` - The total number of pieces to create.
/// * `total_len` - The total length of all pieces combined.
/// * `piece_len` - The length of each piece, except the last one.
macro_rules! pieces {
    ($num_of_pieces:expr, $total_len:expr, $piece_len:expr) => {{
        use crate::InfoHash;
        use crate::Piece;
        use itertools::Itertools;
        use std::str::FromStr;

        let info_hash =
            InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
        let num_of_pieces: usize = $num_of_pieces;
        let total_len: usize = $total_len;
        let piece_len: usize = $piece_len;

        (0..num_of_pieces)
            .into_iter()
            .map(|piece| {
                let len = if piece == num_of_pieces - 1 {
                    total_len - piece * piece_len
                } else {
                    piece_len
                };
                Piece::new(info_hash.clone(), piece, piece * piece_len, len)
            })
            .collect_vec()
    }};
}

/// Create a set of [crate::piece_picker::PieceInfo] instances.
/// These are derived from pieces created through the `pieces` macro.
///
/// # Arguments
///
/// * `num_of_pieces` - The total number of pieces to create.
/// * `total_len` - The total length of all pieces combined.
/// * `piece_len` - The length of each piece, except the last one.
macro_rules! piece_infos {
    ($num_of_pieces:expr, $total_len:expr, $piece_len:expr) => {{
        use crate::piece_picker::PiecePickerBlock;
        use crate::Piece;

        let pieces: Vec<Piece> = pieces!($num_of_pieces, $total_len, $piece_len);

        pieces
            .iter()
            .map(|piece| Vec::<PiecePickerBlock>::from(piece))
            .flatten()
            .collect_vec()
    }};
}
