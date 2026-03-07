use fx_torrent::{FileIndex, FilePriority};
use std::net::SocketAddr;

#[derive(Debug)]
pub enum TorrentInfoCommand {
    ShowFiles,
    UpdatePriority(FileIndex, FilePriority),
    ShowAddPeer,
    AddPeer(SocketAddr),
    TogglePaused,
}
