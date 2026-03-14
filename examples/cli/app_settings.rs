use fx_torrent::TorrentFlags;
use std::path::PathBuf;

const APP_DEFAULT_STORAGE: &str = "torrents";
const DEFAULT_TORRENT_FLAGS: fn() -> TorrentFlags =
    || TorrentFlags::default() | TorrentFlags::Paused | TorrentFlags::UploadMode;

#[derive(Debug, Clone)]
pub(crate) struct AppSettings {
    pub storage: PathBuf,
    pub dht_enabled: bool,
    pub dht_bootstrap_nodes_enabled: bool,
    pub dht_info_hash_indexing_enabled: bool,
    pub trackers_enabled: bool,
    pub tcp_peer_enabled: bool,
    pub utp_peer_enabled: bool,
    pub webseeds_enabled: bool,
    pub torrent_flags: TorrentFlags,
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            storage: PathBuf::from(APP_DEFAULT_STORAGE),
            dht_enabled: true,
            dht_bootstrap_nodes_enabled: true,
            dht_info_hash_indexing_enabled: false,
            trackers_enabled: true,
            tcp_peer_enabled: true,
            utp_peer_enabled: true,
            webseeds_enabled: true,
            torrent_flags: DEFAULT_TORRENT_FLAGS(),
        }
    }
}
