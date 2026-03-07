use fx_torrent::{InfoHash, TorrentState};
use std::path::PathBuf;

#[derive(Debug)]
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
    pub progress: f32,
    pub wasted: u64,
    pub down: Vec<u64>,
    pub up: Vec<u64>,
}

impl Default for TorrentData {
    fn default() -> Self {
        Self {
            path: None,
            state: None,
            info_hash: None,
            wanted_size: 0,
            total_pieces: 0,
            completed_pieces: 0,
            wanted_completed_size: 0,
            total_files: 0,
            peers: 0,
            progress: 0.0,
            wasted: 0,
            down: vec![],
            up: vec![],
        }
    }
}
