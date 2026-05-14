use fx_torrent::{InfoHash, TorrentState};
use std::path::PathBuf;

#[derive(Debug, Default)]
pub struct TorrentData {
    pub path: Option<PathBuf>,
    pub state: Option<TorrentState>,
    pub info_hash: Option<InfoHash>,
    pub total_pieces: u64,
    pub completed_pieces: u64,
    pub wanted_size: u64,
    pub wanted_completed_size: u64,
    pub total_files: usize,
    pub peers: usize,
    pub wasted: u64,
    pub down: Vec<u64>,
    pub up: Vec<u64>,
}
