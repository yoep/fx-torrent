use std::path::{Path, PathBuf};
use std::time::Duration;

const DEFAULT_PEER_CLIENT_NAME: &str = "PopcornFX";
const DEFAULT_PEER_TIMEOUT: Duration = Duration::from_secs(6);
const DEFAULT_PEER_LOWER_LIMIT: usize = 10;
const DEFAULT_PEER_UPPER_LIMIT: usize = 200;
const DEFAULT_PEER_IN_FLIGHT: usize = 25;
const DEFAULT_PEER_UPLOAD_SLOTS: usize = 50;
const DEFAULT_MAX_IN_FLIGHT_PIECES: usize = 256;
const DEFAULT_CHECKING_MEMORY_USAGE: usize = 32 * 1024 * 1024; // 32MB

/// The configuration of a torrent session.
#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub enable_tcp_peer: bool,
    pub enable_utp_peer: bool,
    pub torrent: TorrentConfig,
}

impl SessionConfig {
    /// Create a new session configuration builder.
    pub fn builder() -> SessionConfigBuilder {
        SessionConfigBuilder::builder()
    }

    /// Returns the client name for the session.
    pub fn client_name(&self) -> &str {
        self.torrent.client_name()
    }

    /// Returns the filepath for the torrent data of the session.
    pub fn path(&self) -> &Path {
        self.torrent.path()
    }
}

#[derive(Debug, Default)]
pub struct SessionConfigBuilder {
    enable_tcp_peer: Option<bool>,
    enable_utp_peer: Option<bool>,
    torrent: TorrentConfigBuilder,
}

impl SessionConfigBuilder {
    /// Create a new session configuration builder.
    pub fn builder() -> Self {
        Self::default()
    }

    /// Set if TCP peer connections should be enabled for the session.
    pub fn enable_tcp_peer(&mut self, enable: bool) -> &mut Self {
        self.enable_tcp_peer = Some(enable);
        self
    }

    /// Set if UTP peer connections should be enabled for the session.
    pub fn enable_utp_peer(&mut self, enable: bool) -> &mut Self {
        self.enable_utp_peer = Some(enable);
        self
    }

    /// Set the client name for the session.
    pub fn client_name<S: AsRef<str>>(&mut self, name: S) -> &mut Self {
        self.torrent.client_name(name);
        self
    }

    /// Set the filepath for the torrent data of the session.
    pub fn path(&mut self, path: impl AsRef<Path>) -> &mut Self {
        self.torrent.path(path);
        self
    }

    /// Set the lower limit for the number of peers.
    pub fn peers_lower_limit(&mut self, limit: usize) -> &mut Self {
        self.torrent.peers_lower_limit(limit);
        self
    }

    /// Set the upper limit for the number of peers.
    pub fn peers_upper_limit(&mut self, limit: usize) -> &mut Self {
        self.torrent.peers_upper_limit(limit);
        self
    }

    /// Set the max number of peer connections in flight.
    pub fn peers_in_flight(&mut self, limit: usize) -> &mut Self {
        self.torrent.peers_in_flight(limit);
        self
    }

    /// Set the max number of peer upload slots.
    pub fn peers_upload_slots(&mut self, slots: usize) -> &mut Self {
        self.torrent.peers_upload_slots(slots);
        self
    }

    /// Set the timeout for peer connections.
    pub fn peer_connection_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.torrent.peer_connection_timeout(timeout);
        self
    }

    /// Set the maximum number of in flight pieces which can be requested in parallel from peers.
    pub fn max_in_flight_pieces(&mut self, limit: usize) -> &mut Self {
        self.torrent.max_in_flight_pieces(limit);
        self
    }

    /// Set the maximum memory usage in bytes for checking files.
    /// Higher number means more memory usage but faster validation.
    /// Defaults to 32MB.
    pub fn checking_mem_usage(&mut self, mem_usage: usize) -> &mut Self {
        self.torrent.checking_mem_usage(mem_usage);
        self
    }

    /// Build the session configuration.
    pub fn build(&mut self) -> SessionConfig {
        SessionConfig {
            enable_tcp_peer: self.enable_tcp_peer.take().unwrap_or(true),
            enable_utp_peer: self.enable_utp_peer.take().unwrap_or(true),
            torrent: self.torrent.build(),
        }
    }
}

/// The torrent configuration values.
#[derive(Debug, Clone)]
pub struct TorrentConfig {
    client_name: String,
    path: PathBuf,
    pub peers_lower_limit: usize,
    pub peers_upper_limit: usize,
    pub peers_in_flight: usize,
    pub peers_upload_slots: usize,
    pub peer_connection_timeout: Duration,
    /// The maximum number of in flight pieces which can be requested in parallel from peers.
    pub max_in_flight_pieces: usize,
    /// The maximum memory usage in bytes for checking files.
    /// Higher number means more memory usage but faster validation.
    pub checking_mem_usage: usize,
}

impl TorrentConfig {
    /// Create a new torrent configuration builder.
    pub fn builder() -> TorrentConfigBuilder {
        TorrentConfigBuilder::builder()
    }

    /// Returns the client name of the torrent.
    pub fn client_name(&self) -> &str {
        self.client_name.as_str()
    }

    /// Returns the path of the torrent data.
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// Set the filepath for the torrent data.
    pub fn set_path(&mut self, path: impl AsRef<Path>) {
        self.path = path.as_ref().to_path_buf();
    }
}

#[derive(Debug, Default)]
pub struct TorrentConfigBuilder {
    client_name: Option<String>,
    path: Option<PathBuf>,
    peers_lower_limit: Option<usize>,
    peers_upper_limit: Option<usize>,
    peers_in_flight: Option<usize>,
    peers_upload_slots: Option<usize>,
    peer_connection_timeout: Option<Duration>,
    max_in_flight_pieces: Option<usize>,
    checking_mem_usage: Option<usize>,
}

impl TorrentConfigBuilder {
    /// Create a new torrent configuration builder.
    pub fn builder() -> Self {
        Self::default()
    }

    /// Set the name of the client.
    pub fn client_name<S: AsRef<str>>(&mut self, name: S) -> &mut Self {
        self.client_name = Some(name.as_ref().to_string());
        self
    }

    /// Set the torrent data path.
    /// This is the path where the downloaded data will be stored.
    pub fn path<P: AsRef<Path>>(&mut self, path: P) -> &mut Self {
        self.path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Set the lower limit for the number of peers.
    pub fn peers_lower_limit(&mut self, limit: usize) -> &mut Self {
        self.peers_lower_limit = Some(limit);
        self
    }

    /// Set the upper limit for the number of peers.
    pub fn peers_upper_limit(&mut self, limit: usize) -> &mut Self {
        self.peers_upper_limit = Some(limit);
        self
    }

    /// Set the max number of peer connections in flight.
    pub fn peers_in_flight(&mut self, limit: usize) -> &mut Self {
        self.peers_in_flight = Some(limit);
        self
    }

    /// Set the max number of peer upload slots.
    pub fn peers_upload_slots(&mut self, slots: usize) -> &mut Self {
        self.peers_upload_slots = Some(slots);
        self
    }

    /// Set the timeout for peer connections.
    pub fn peer_connection_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.peer_connection_timeout = Some(timeout);
        self
    }

    /// Set the maximum number of in flight pieces which can be requested in parallel from peers.
    pub fn max_in_flight_pieces(&mut self, limit: usize) -> &mut Self {
        self.max_in_flight_pieces = Some(limit);
        self
    }

    /// Set the maximum memory usage in bytes for checking files.
    /// Higher number means more memory usage but faster validation.
    /// Defaults to 32MB.
    pub fn checking_mem_usage(&mut self, mem_usage: usize) -> &mut Self {
        self.checking_mem_usage = Some(mem_usage);
        self
    }

    /// Build the torrent configuration.
    pub fn build(&mut self) -> TorrentConfig {
        let client_name = self
            .client_name
            .take()
            .unwrap_or_else(|| DEFAULT_PEER_CLIENT_NAME.to_string());
        let path = self.path.take().unwrap_or_else(|| PathBuf::new());
        let peers_lower_limit = self
            .peers_lower_limit
            .take()
            .unwrap_or(DEFAULT_PEER_LOWER_LIMIT);
        let peers_upper_limit = self
            .peers_upper_limit
            .take()
            .unwrap_or(DEFAULT_PEER_UPPER_LIMIT);
        let peers_in_flight = self
            .peers_in_flight
            .take()
            .unwrap_or(DEFAULT_PEER_IN_FLIGHT);
        let peers_upload_slots = self
            .peers_upload_slots
            .take()
            .unwrap_or(DEFAULT_PEER_UPLOAD_SLOTS);
        let peer_connection_timeout = self
            .peer_connection_timeout
            .take()
            .unwrap_or(DEFAULT_PEER_TIMEOUT);
        let max_in_flight_pieces = self
            .max_in_flight_pieces
            .take()
            .unwrap_or(DEFAULT_MAX_IN_FLIGHT_PIECES);
        let checking_mem_usage = self
            .checking_mem_usage
            .take()
            .unwrap_or(DEFAULT_CHECKING_MEMORY_USAGE);

        TorrentConfig {
            client_name,
            path,
            peers_lower_limit,
            peers_upper_limit,
            peers_in_flight,
            peers_upload_slots,
            peer_connection_timeout,
            max_in_flight_pieces,
            checking_mem_usage,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod session {
        use super::*;

        #[test]
        fn test_default() {
            let result = SessionConfig::builder().build();

            assert_eq!(DEFAULT_PEER_CLIENT_NAME, result.client_name());
            assert_eq!(PathBuf::new(), result.path());
        }
    }

    mod torrent {
        use super::*;

        #[test]
        fn test_default() {
            let result = TorrentConfig::builder().build();

            assert_eq!(DEFAULT_PEER_CLIENT_NAME, result.client_name);
            assert_eq!(PathBuf::new(), result.path);
        }
    }
}
