/*!
# FX-Torrent

FX-Torrent is the most complete BitTorrent implementation fully written in Rust, which supports both Linux, MacOS, and Windows.
It supports most of the Bittorrent protocol specifications, such as multi-file torrents, validating existing files, resuming torrent files,
and is based on the `libtorrent` library for functionality and naming convention.

## Getting Started

The entry point for the `fx_torrent` crate is the [`FxTorrentSession`].
A session manages the lifecycle of multiple torrents.

### Basic Usage

```rust
use std::io;
use fx_torrent::{FxTorrentSession, Session, SessionConfig, TorrentFlags, TorrentMetadata};

// The fx-torrent crate makes use of async tokio runtimes
// this requires that new sessions and torrents need to be created within an async context
#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let session = FxTorrentSession::builder()
        .config(
            SessionConfig::builder()
                .base_path("/downloads")
                .client_name("MyClient")
                .build(),
        )
        .build()?;

    // 1. Add a torrent via Magnet URI
    let magnet_torrent = session
        .add_torrent_from_uri("magnet:?xt=urn:btih:...", TorrentFlags::default())
        .await;

    // 2. Add a torrent from a local .torrent file
    let file_torrent = session
        .add_torrent_from_uri("/path/to/file.torrent", TorrentFlags::default())
        .await;

    // 3. Add a torrent from raw metadata bytes
    let data: &[u8] = &[0; 1024]; // Replace with actual bencoded bytes
    let metadata = TorrentMetadata::try_from(data)?;
    let metadata_torrent = session
        .add_torrent_from_metadata(metadata, TorrentFlags::Paused)
        .await;

    Ok(())
}
```

### Working with Magnets and Metadata

#### Magnets

You can parse a magnet string or construct one manually using the builder pattern.

```rust
# use fx_torrent::Magnet;
# fn example() {
    // Parsing from a string
    match Magnet::from_str("magnet:?xt=urn:btih:2C6B6858D61DA9543D4231A71DB4B1C9264B0685&...") {
        Ok(magnet) => println!("{:?}", magnet),
        Err(e) => println!("{:?}", e),
    }

    // Using the builder
    match Magnet::builder()
        .exact_topic("xt=urn:btih:2C6B6858D61DA9543D4231A71DB4B1C9264B0685")
        .build() {
        Ok(magnet) => println!("{:?}", magnet),
        Err(e) => println!("{:?}", e),
    }
# }
```

#### Metadata

Metadata can be decoded from bencoded bytes (using `TorrentMetadata::try_from`) or constructed manually.

```rust
# use fx_torrent::TorrentMetadata;
# fn example() {
    // Parsing from bencoded bytes
    match TorrentMetadata::try_from(&[0; 1024]) {
        Ok(metadata) => println!("{:?}", metadata),
        Err(e) => println!("{:?}", e),
    }

    // Using the builder
    let metadata = TorrentMetadata::builder()
        .name("MyTorrent")
        .build();
    println!("{:?}", metadata);
# }
```

### Examples

For more examples, see the [examples](./examples).

## DHT

When using the `dht` feature, enabled by default, one of the following additional features should be enabled:
- `ed25519-dalek`
- `ring-compat`

These crypto providers are used within the DHT network to verify mutable items within the network.
When both features are missing, a [dht::Error::MissingCryptoProvider] error will be returned.

*/

pub use compact::*;
pub use config::*;
pub use dht_option::*;
pub use errors::*;
pub use file::*;
pub use info_hash::*;
pub use magnet::*;
pub use piece::*;
use piece_chunk_pool::*;
pub use session::*;
pub use session_cache::*;
pub use torrent::*;
pub use torrent_flags::*;
pub use torrent_health::*;
pub use torrent_metadata::*;
pub use torrent_metrics::*;
pub use torrent_peer::*;

use std::ops::Range;

#[cfg(test)]
#[macro_use]
mod test_macros;
#[macro_use]
mod channel;

mod bloom_filter;
mod compact;
mod config;
#[cfg(feature = "dht")]
pub mod dht;
mod dht_option;
mod errors;
mod file;
mod info_hash;
mod magnet;
mod merkle;
pub mod metrics;
pub mod operation;
pub mod peer;
mod peer_pool;
mod piece;
mod piece_chunk_pool;
mod session;
mod session_cache;
pub mod storage;
mod torrent;
mod torrent_data;
mod torrent_flags;
mod torrent_health;
mod torrent_metadata;
mod torrent_metrics;
mod torrent_peer;
pub mod tracker;

#[cfg(feature = "extension-donthave")]
use crate::peer::extension::donthave::DontHaveExtension;
#[cfg(feature = "extension-metadata")]
use crate::peer::extension::metadata::MetadataExtension;
#[cfg(feature = "extension-pex")]
use crate::peer::extension::pex::PexExtension;
use crate::peer::ProtocolExtensionFlags;

const DEFAULT_TORRENT_PROTOCOL_EXTENSIONS: fn() -> ProtocolExtensionFlags = || {
    ProtocolExtensionFlags::LTEP | ProtocolExtensionFlags::Fast | ProtocolExtensionFlags::SupportV2
};
const DEFAULT_TORRENT_EXTENSIONS: fn() -> ExtensionFactories = || {
    let mut extensions: ExtensionFactories = Vec::new();

    #[cfg(feature = "extension-metadata")]
    extensions.push(|| Box::new(MetadataExtension::new()));
    #[cfg(feature = "extension-pex")]
    extensions.push(|| Box::new(PexExtension::new()));
    #[cfg(feature = "extension-donthave")]
    extensions.push(|| Box::new(DontHaveExtension::new()));

    extensions
};

/// Formats the given number of bytes into a human-readable format with appropriate units.
///
/// This function converts a byte size into a more readable format using common storage units (B, KB, MB, GB, TB).
/// The result is rounded to two decimal places for clarity. It ensures that the byte count is represented with
/// the most appropriate unit based on the size of the input. The units scale based on powers of 1024.
///
/// # Arguments
/// - `bytes`: The size in bytes to be formatted.
///
/// # Returns
///
/// It returns the formatted byte size with the corresponding unit.
///
/// # Example
///
/// ```rust,no_run
/// use fx_torrent::torrent::format_bytes;
///
/// let formatted = format_bytes(1048576);
/// println!("{}", formatted); // "1.00 MB"
/// ```
///
/// # Notes
/// The function uses the binary system for scaling (i.e., 1024 bytes = 1 KB).
pub fn format_bytes(bytes: usize) -> String {
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;

    while value >= 1024.0 && unit < units.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    format!("{:.2} {}", value, units[unit])
}

/// Calculates the data transfer rate in bytes per second.
///
/// This function computes the data transfer rate based on the number of bytes transferred and the
/// elapsed time in microseconds. It returns the rate as bytes per second (B/s). If the elapsed time is less
/// than one second (1,000,000 microseconds), it simply returns the number of bytes as the rate.
///
/// # Arguments
/// - `bytes`: The number of bytes transferred.
/// - `elapsed_micro_secs`: The time elapsed in microseconds.
///
/// # Returns
/// A `u64` representing the data transfer rate in bytes per second (B/s).
///
/// # Example
///
/// ```rust,no_run
/// use fx_torrent::torrent::calculate_byte_rate;
///
/// let rate = calculate_byte_rate(1_000_000, 1_500_000);
/// println!("{}", rate); // "666666" (bytes per second);
///
/// let rate = calculate_byte_rate(1_000_000, 2_000_000);
/// println!("{}", rate); // "500000" (bytes per second);
/// ```
///
/// # Notes
/// The function assumes that the elapsed time is given in microseconds. If the elapsed time is very short,
/// it will default to the total byte count as the rate.
pub fn calculate_byte_rate(bytes: usize, elapsed_micro_secs: u128) -> u64 {
    if elapsed_micro_secs <= 1_000_000 {
        return bytes as u64;
    }

    ((bytes as u128 * 1_000_000) / elapsed_micro_secs) as u64
}

/// Get the overlapping range of two ranges.
/// It returns the overlapping range if there is one, else [None].
#[inline]
pub(crate) fn overlapping_range<T>(r1: Range<T>, r2: &Range<T>) -> Option<Range<T>>
where
    T: Ord + Copy,
{
    let start = r1.start.max(r2.start);
    let end = r1.end.min(r2.end);

    if start < end {
        Some(start..end)
    } else {
        None
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::peer::tests::new_tcp_peer_discovery;
    use crate::peer::{BitTorrentPeer, PeerDiscovery, PeerId, PeerStream};
    use log::trace;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::time::Duration;
    use std::{env, fs};
    use tokio::net::TcpStream;
    use tokio::sync::mpsc::unbounded_channel;

    #[macro_export]
    macro_rules! create_torrent_context {
        ($uri:expr, $temp_dir:expr, $options:expr) => {{
            create_torrent_context!(
                $uri,
                $temp_dir,
                $options,
                crate::TorrentConfig::builder().path($temp_dir).build()
            )
        }};
        ($uri:expr, $temp_dir:expr, $options:expr, $config:expr) => {{
            use crate::peer::{PeerDiscovery, TcpPeerDiscovery, UtpPeerDiscovery};

            let tcp_discovery = TcpPeerDiscovery::new()
                .await
                .expect("expected a new tcp peer discovery");
            let utp_discovery = UtpPeerDiscovery::new()
                .await
                .expect("expected a new utp peer discovery");
            let discoveries: Vec<Box<dyn PeerDiscovery>> =
                vec![Box::new(tcp_discovery), Box::new(utp_discovery)];

            create_torrent_context!($uri, $temp_dir, $options, $config, discoveries)
        }};
        ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $discoveries:expr) => {{
            create_torrent_context!(
                $uri,
                $temp_dir,
                $options,
                $config,
                $discoveries,
                Some(crate::dht::DhtTracker::builder().build().await.unwrap())
            )
        }};
        ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $discoveries:expr, $dht:expr) => {{
            use crate::storage::MemoryStorage;
            use std::sync::Arc;

            create_torrent_context!(
                $uri,
                $temp_dir,
                $options,
                $config,
                $discoveries,
                $dht,
                |_, _| Arc::new(MemoryStorage::new())
            )
        }};
        ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $discoveries:expr, $dht:expr, $storage:expr) => {{
            use crate::peer::PeerDiscovery;
            use crate::torrent_data::DataPool;
            use crate::tracker::TrackerClient;
            use crate::{
                DhtOption, TorrentConfig, TorrentContext, TorrentFlags, TorrentMetadata,
                DEFAULT_TORRENT_EXTENSIONS, DEFAULT_TORRENT_PROTOCOL_EXTENSIONS,
            };
            use std::time::Duration;

            let uri: &str = $uri;
            let options: TorrentFlags = $options;
            let config: TorrentConfig = $config;
            let discoveries: Vec<Box<dyn PeerDiscovery>> = $discoveries;
            let dht: DhtOption = DhtOption::from($dht);
            let metadata: TorrentMetadata = metadata!(uri);
            let info_hash = metadata.info_hash.clone();
            let tracker_manager = TrackerClient::new(Duration::from_secs(2));
            let config = TorrentConfig::builder()
                .path($temp_dir)
                .peer_connection_timeout(config.peer_connection_timeout)
                .max_in_flight_pieces(config.max_in_flight_pieces)
                .peers_upper_limit(config.peers_upper_limit)
                .peers_lower_limit(config.peers_lower_limit)
                .peers_in_flight(config.peers_in_flight)
                .build();
            let data_pool = DataPool::new();
            let (command_sender, receiver) = channel!(512);

            (
                TorrentContext::new(
                    metadata,
                    config,
                    discoveries.first().map(|e| e.port()),
                    DEFAULT_TORRENT_PROTOCOL_EXTENSIONS(),
                    DEFAULT_TORRENT_EXTENSIONS(),
                    options,
                    data_pool.clone(),
                    dht,
                    tracker_manager,
                    ($storage)(info_hash, data_pool),
                    command_sender,
                ),
                receiver,
            )
        }};
    }

    #[macro_export]
    macro_rules! create_torrent {
        ($uri:expr, $temp_dir:expr, $options:expr) => {{
            create_torrent!(
                $uri,
                $temp_dir,
                $options,
                crate::TorrentConfig::builder().path($temp_dir).build()
            )
        }};
        ($uri:expr, $temp_dir:expr, $options:expr, $config:expr) => {{
            create_torrent!(
                $uri,
                $temp_dir,
                $options,
                $config,
                crate::operation::DEFAULT_OPERATIONS()
            )
        }};
        ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $operations:expr) => {{
            use crate::peer::{PeerDiscovery, TcpPeerDiscovery, UtpPeerDiscovery};

            let tcp_discovery = TcpPeerDiscovery::new()
                .await
                .expect("expected a new tcp peer discovery");
            let utp_discovery = UtpPeerDiscovery::new_with_port(tcp_discovery.port())
                .await
                .expect("expected a new utp peer discovery");
            let discoveries: Vec<Box<dyn PeerDiscovery>> =
                vec![Box::new(tcp_discovery), Box::new(utp_discovery)];

            create_torrent!($uri, $temp_dir, $options, $config, $operations, discoveries)
        }};
        ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $operations:expr, $discoveries:expr) => {{
            create_torrent!(
                $uri,
                $temp_dir,
                $options,
                $config,
                $operations,
                $discoveries,
                |params| {
                    Box::new(crate::storage::DiskStorage::new(
                        params.info_hash,
                        params.path,
                        params.data_pool,
                    ))
                }
            )
        }};
        ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $operations:expr, $discoveries:expr, $storage:expr) => {{
            create_torrent!(
                $uri,
                $temp_dir,
                $options,
                $config,
                $operations,
                $discoveries,
                $storage,
                Some(crate::dht::DhtTracker::builder().build().await.unwrap())
            )
        }};
        ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $operations:expr, $discoveries:expr, $storage:expr, $dht:expr) => {{
            use crate::tracker::TrackerClient;
            use std::time::Duration;

            create_torrent!(
                $uri,
                $temp_dir,
                $options,
                $config,
                $operations,
                $discoveries,
                $storage,
                $dht,
                TrackerClient::new(Duration::from_secs(2))
            )
        }};
        ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $operations:expr, $discoveries:expr, $storage:expr, $dht:expr, $tracker_manager:expr) => {{
            use crate::dht::DhtTracker;
            use crate::operation::TorrentOperation;
            use crate::peer::PeerDiscovery;
            use crate::{DhtOption, Torrent, TorrentConfig, TorrentFlags};

            let uri: &str = $uri;
            let options: TorrentFlags = $options;
            let config: TorrentConfig = $config;
            let operations: Vec<Box<dyn TorrentOperation>> = $operations;
            let discoveries: Vec<Box<dyn PeerDiscovery>> = $discoveries;
            let dht: Option<DhtTracker> = $dht;
            let torrent_info = metadata!(uri);
            let tracker_manager = $tracker_manager;
            let config = TorrentConfig::builder()
                .path($temp_dir)
                .peer_connection_timeout(config.peer_connection_timeout)
                .max_in_flight_pieces(config.max_in_flight_pieces)
                .peers_upper_limit(config.peers_upper_limit)
                .peers_lower_limit(config.peers_lower_limit)
                .build();

            Torrent::request()
                .metadata(torrent_info)
                .peer_discoveries(discoveries)
                .options(options)
                .config(config)
                .operations(operations)
                .storage($storage)
                .tracker_manager(tracker_manager)
                .dht(DhtOption::from(dht))
                .build()
                .unwrap()
        }};
    }

    #[macro_export]
    macro_rules! create_peer_pair {
        ($torrent:expr) => {
            crate::tests::create_tcp_peer_pair(
                $torrent,
                $torrent,
                $torrent.inner.protocol_extensions().await.unwrap(),
            )
            .await
        };
        ($torrent:expr, $protocols:expr) => {
            crate::tests::create_tcp_peer_pair($torrent, $torrent, $protocols).await
        };
        ($incoming_torrent:expr, $outgoing_torrent:expr, $protocols:expr) => {
            crate::tests::create_tcp_peer_pair($incoming_torrent, $outgoing_torrent, $protocols)
                .await
        };
    }

    pub async fn create_tcp_peer_pair(
        incoming_torrent: &Torrent,
        outgoing_torrent: &Torrent,
        protocols: ProtocolExtensionFlags,
    ) -> (BitTorrentPeer, BitTorrentPeer) {
        let outgoing_context = &outgoing_torrent.inner;
        let (tx, mut rx) = unbounded_channel();

        let incoming_context = incoming_torrent.inner.clone();
        let incoming_data_pool = incoming_context.data_pool().await.unwrap();
        let extensions = incoming_context.extensions().await.unwrap();
        let listener = new_tcp_peer_discovery().await.unwrap();
        let listener_port = listener.port();
        tokio::spawn(async move {
            if let Some(peer) = listener.recv().await {
                if let PeerStream::Tcp(stream) = peer.stream {
                    tx.send(
                        BitTorrentPeer::new_inbound(
                            PeerId::new(),
                            peer.socket_addr,
                            PeerStream::Tcp(stream),
                            incoming_context,
                            incoming_data_pool,
                            protocols.clone(),
                            extensions,
                            Duration::from_secs(5),
                        )
                        .await,
                    )
                    .unwrap()
                }
            }
        });

        let outgoing_context = outgoing_context.clone();
        let outgoing_extensions = outgoing_context.extensions().await.unwrap();
        let outgoing_data_pool = outgoing_context.data_pool().await.unwrap();
        let addr = SocketAddr::new([127, 0, 0, 1].into(), listener_port);
        let stream = TcpStream::connect(addr).await.unwrap();
        let outgoing_peer = BitTorrentPeer::new_outbound(
            PeerId::new(),
            addr,
            PeerStream::Tcp(stream),
            outgoing_context,
            outgoing_data_pool,
            protocols,
            outgoing_extensions,
            Duration::from_secs(5),
        )
        .await
        .expect("expected the outgoing connection to succeed");

        let incoming_peer = timeout!(
            rx.recv(),
            Duration::from_secs(1),
            "expected an incoming peer"
        )
        .unwrap()
        .expect("expected an incoming peer");
        (incoming_peer, outgoing_peer)
    }

    /// Retrieve the path to the testing resource directory.
    ///
    /// It returns the [PathBuf] to the testing resources directory.
    pub fn test_resource_directory() -> PathBuf {
        let root_dir = &env::var("CARGO_MANIFEST_DIR").expect("$CARGO_MANIFEST_DIR");
        let mut source = PathBuf::from(root_dir);
        source.push("test");

        source
    }

    /// Retrieve the filepath of a testing resource file.
    /// These are files located within the "test" directory of the crate.
    ///
    /// It returns the created [PathBuf] for the given filename.
    pub fn test_resource_filepath(filename: &str) -> PathBuf {
        let mut source = test_resource_directory();
        source.push(filename);

        source
    }

    pub fn read_test_file_to_bytes(filename: &str) -> Vec<u8> {
        let source = test_resource_filepath(filename);

        fs::read(&source).unwrap()
    }

    pub fn copy_test_file(temp_dir: &str, filename: &str, output_filename: Option<&str>) -> String {
        let root_dir = &env::var("CARGO_MANIFEST_DIR").expect("$CARGO_MANIFEST_DIR");
        let source = PathBuf::from(root_dir).join("test").join(filename);
        let destination = PathBuf::from(temp_dir).join(output_filename.unwrap_or(filename));

        // make sure the parent dir exists
        fs::create_dir_all(destination.parent().unwrap()).unwrap();

        trace!("Copying test file {} to {:?}", filename, destination);
        fs::copy(&source, &destination).unwrap();

        destination.to_str().unwrap().to_string()
    }

    mod overlapping_range {
        use super::*;

        #[test]
        fn test_overlap_range() {
            let r1 = 0..10;
            let r2 = 5..15;
            let result = overlapping_range(r1, &r2);
            assert_eq!(Some(5..10), result);

            let r1 = 16..32;
            let r2 = 30..64;
            let result = overlapping_range(r1, &r2);
            assert_eq!(Some(30..32), result);

            let r1 = 128..256;
            let r2 = 512..1024;
            let result = overlapping_range(r1, &r2);
            assert_eq!(None, result);
        }
    }

    pub mod helpers {
        use super::*;
        use fx_callback::Callback;
        use tokio::{select, time};

        pub async fn wait_for_torrent_pieces(torrent: &Torrent) {
            let mut receiver = torrent.subscribe();
            if torrent.pieces().await.filter(|e| !e.is_empty()).is_some() {
                return;
            }

            select! {
                _ = time::sleep(Duration::from_secs(2)) => assert!(false, "expected the pieces of {} to have been created", torrent),
                _ = async {
                    while let Ok(event) = receiver.recv().await {
                        match &*event {
                            TorrentEvent::PiecesChanged(_) => break,
                            _ => {}
                        }
                    }
                } => {}
            }
        }

        pub async fn wait_for_torrent_state(
            torrent: &Torrent,
            expected_state: TorrentState,
            timeout: Duration,
        ) {
            let mut receiver = torrent.subscribe();
            let mut state = torrent.state().await;
            if state == expected_state {
                return;
            }

            select! {
                _ = time::sleep(timeout) => assert!(false, "expected state {}, but got {:?}", expected_state, state),
                _ = async {
                    while let Ok(event) = receiver.recv().await {
                        match &*event {
                            TorrentEvent::StateChanged(new_state) => {
                                state = *new_state;
                                if state == expected_state {
                                    return;
                                }
                            },
                            _ => {}
                        }
                    }
                } => {}
            }
        }
    }
}
