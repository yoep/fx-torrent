/*!
# FX-Torrent

FX-Torrent is the most complete BitTorrent implementation fully written in Rust, which supports both Linux, MacOS, and Windows.
It supports most of the Bittorrent protocol specifications, such as multi-file torrents, validating existing files, resuming torrent files,
and is based on the `libtorrent` library for functionality and naming convention.

## Getting Started

The entry point for the `fx_torrent` crate is the [`FxSession`].
A session manages the lifecycle of multiple torrents.

### Basic Usage

```rust
# use std::io;
use fx_torrent::prelude::*;
// The fx-torrent crate makes use of async tokio runtimes
// this requires that new sessions and torrents need to be created within a tokio runtime
#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let session = FxSession::builder()
        .config(
            SessionConfig::builder()
                .base_path("/downloads")
                .client_name("MyClient")
                .build(),
        )
        .default_extensions()
        .dht(DhtTracker::builder()
            .default_routing_nodes()
            .build()
            .await?)
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

For more advanced examples, see the [examples](https://github.com/yoep/fx-torrent/tree/master/examples) directory.

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

## DHT

When using the `dht` feature, enabled by default, one of the following additional features should be enabled:
- `ed25519-dalek`
- `ring-compat`

These crypto providers are used within the DHT network to verify mutable items within the network.
When both features are missing, a [dht::Error::MissingCryptoProvider] error will be returned.

## Extensions

The `fx_torrent` crate is designed to be highly extensible.
You can modify the core behavior of components by implementing and registering "extension" traits.
These allow for custom logic in peer communication and data persistence.

### Peer Extension

Peer extensions allow you to extend the BitTorrent protocol with custom messaging and handshake
capabilities, following the **BEP 10** specification.

To modify peer protocol behavior, implement the [peer::extension::Extension] trait.
Once implemented, these extensions can be attached to individual torrents or globally across a session.

_example peer extension_
```rust
# use fx_torrent::prelude::*;
# use fx_torrent::peer::PeerContext;
# use fx_torrent::peer::extension::Extension;
# use fx_torrent::peer::extension::Result;

#[derive(Debug)]
pub struct MyPeerExtension;
impl Extension for MyPeerExtension {
    fn name(&self) -> &str {
        "my-extension"
    }

    // Additional trait methods
}

# fn example() {
    // 1. Peer extension directly in a torrent
    let torrent = Torrent::request()
        .extension(|| MyPeerExtension.into())
        .build()
        .unwrap();

    // 2. Peer extension in a session
    let session = FxSession::builder()
        .extension(|| MyPeerExtension.into())
        .build()
        .unwrap();
# }
```

### Storage Extension

Storage extensions allow you to customize how data is read from and written to disk (or memory).
This is useful for implementing custom caching layers, encrypted storage, or cloud-backed persistence.

To create your own storage backend, implement the [storage::Extension] trait.

_example storage extension_
```rust
# use fx_torrent::prelude::*;
# use fx_torrent::storage::Extension;
# use fx_torrent::storage::StorageParams;

#[derive(Debug)]
pub struct MyStorageExtension;
impl MyStorageExtension {
    pub fn new(_params: StorageParams) -> Self {
        Self
    }
}
impl Extension for MyStorageExtension {
    async fn read(&self, buffer: &mut [u8], piece: &PieceIndex, offset: usize) -> Result<usize> {
        // Read piece data from storage
        Ok(0)
    }

    // Additional trait methods
}

# fn example() {
    // 1. Storage extension directly in a torrent
    let torrent = Torrent::request()
        .storage(|params| MyStorageExtension::new(params).into())
        .build()
        .unwrap();

    // 2. Storage extension in a session
    let session = FxSession::builder()
        .storage(|params| MyStorageExtension::new(params).into())
        .build().unwrap();
# }
```

### Operation Extension

Operation extensions are **tick-based** tasks invoked by the [TorrentContext].
These operations are executed sequentially in an order-dependent chain,
meaning the sequence in which you register them determines their execution priority.

_example operation extension_
```rust
# use fx_torrent::prelude::*;
# use fx_torrent::TorrentContext;
# use fx_torrent::operation::Extension;
# use fx_torrent::operation::TorrentOperationResult;
# use fx_torrent::peer::PeerDiscovery;
# use async_trait;

#[derive(Debug)]
pub struct MyOperation;
#[async_trait]
impl Extension for MyOperation {
    /// The `tick` method is called periodically by the torrent engine.
    async fn tick(&self, context: &mut TorrentContext, peer_discoveries: &[PeerDiscovery]) -> TorrentOperationResult {
        // Logic for your custom operation goes here
        TorrentOperationResult::Continue
    }

    // Additional trait methods
}

# fn example() {
    // 1. Operation extension directly in a torrent
    let torrent = Torrent::request()
        .operation(MyOperation.into())
        .build()
        .unwrap();

    // 2. Operation extension in a session
    let session = FxSession::builder()
        .operation(|| MyOperation.into())
        .build()
        .unwrap();
# }
```

### Piece Picker Extension

Piece picker extensions allow you to customize or completely override the core piece selection algorithm.
Piece selection tasks are executed either periodically via a background ticker or instantly on-demand when requested by a peer.

To implement a custom piece picker algorithm, implement the [piece_picker::Extension] trait.

_example piece picker extension_
```rust
# use async_trait;
# use fx_torrent::DataPool;
# use fx_torrent::FxSession;
# use fx_torrent::InnerTorrent;
# use fx_torrent::Torrent;
# use fx_torrent::peer::Peer;
# use fx_torrent::piece_picker::Extension;
# use fx_torrent::piece_picker::PickerOptions;
# use fx_torrent::storage::Storage;
# use std::sync::Arc;

#[derive(Debug)]
pub struct MyPiecePicker;
#[async_trait]
impl Extension for MyPiecePicker {
    async fn pick_pieces(&mut self, peer: &Peer) {
        // Your custom piece picking algorithm goes here
    }

    async fn tick<'a>(&'a mut self, peers: Vec<&'a Peer>) {
        // Tick-based piece picking logic goes here
    }

    // Additional trait methods
}

# fn example () {
    // 1. Piece picker extension directly in a torrent
    let torrent = Torrent::request()
        .piece_picker(|
            torrent: InnerTorrent,
            data_pool: DataPool,
            storage: Storage,
            options: PickerOptions| MyPiecePicker.into())
        .build()
        .unwrap();

    // 2. Piece picker extension in a session
    let session = FxSession::builder()
        .piece_picker(|
            torrent: InnerTorrent,
            data_pool: DataPool,
            storage: Storage,
            options: PickerOptions| MyPiecePicker.into())
        .build()
        .unwrap();
# }
```

#### Piece Picker Strategy Extension

The [piece_picker::FxPiecePicker] architecture allows sub-strategies to be sequentially stacked or overridden.
These strategies operate in an order-dependent chain,
meaning their registration order explicitly dictates execution priority during the piece picking lifecycle.

```rust
# use fx_torrent::DataPool;
# use fx_torrent::InnerTorrent;
# use fx_torrent::PieceIndex;
# use fx_torrent::Torrent;
# use fx_torrent::peer::Peer;
# use fx_torrent::piece_picker::FxPiecePicker;
# use fx_torrent::piece_picker::PickerOptions;
# use fx_torrent::piece_picker::PiecePicker;
# use fx_torrent::piece_picker::PiecePickerBlock;
# use fx_torrent::piece_picker::strategy::{Extension, PriorityStrategy};
# use fx_torrent::storage::Storage;
# use std::sync::Arc;

#[derive(Debug)]
pub struct MyStrategy;
impl Extension for MyStrategy {
    async fn pick_pieces<'a>(
        &self,
        peer: &Peer,
        blocks: &'a Vec<PiecePickerBlock>,
        target_queue_len: usize,
        suggested_pieces: &[PieceIndex],
        is_end_game: bool,
        options: PickerOptions,
    ) -> Vec<&'a PiecePickerBlock> {
        // Your custom piece picking logic goes here
        vec![]
    }
}

# fn example() {
    let torrent = Torrent::request()
        .piece_picker(|
            torrent: InnerTorrent,
            data_pool: DataPool,
            storage: Storage,
            options: PickerOptions| FxPiecePicker::new(
            torrent,
            data_pool,
            storage,
            vec![
                MyStrategy.into(),
                PriorityStrategy::new().into(),
            ],
            32 * 1024
        ))
        .build()
        .unwrap();
# }
```

*/

pub use compact::*;
pub use config::*;
pub use error::*;
pub use file::*;
pub use info_hash::*;
#[cfg(feature = "lsd")]
pub use lsd::*;
pub use magnet::*;
pub use piece::*;
pub use session::*;
pub use session_cache::*;
pub use torrent::*;
pub use torrent_flags::*;
pub use torrent_health::*;
pub use torrent_metadata::*;
pub use torrent_metrics::*;
pub use torrent_tracker::*;
pub use types::*;

use std::ops::Range;

#[cfg(test)]
#[macro_use]
mod test_macros;
#[macro_use]
mod channel;

pub mod bencode;
mod bloom_filter;
mod compact;
mod config;
#[cfg(feature = "dht")]
pub mod dht;
mod error;
mod file;
mod info_hash;
#[cfg(feature = "lsd")]
mod lsd;
mod magnet;
mod merkle;
pub mod metrics;
pub mod operation;
pub mod peer;
mod peer_pool;
mod piece;
pub mod piece_picker;
mod session;
mod session_cache;
pub mod storage;
mod torrent;
mod torrent_data;
mod torrent_flags;
mod torrent_health;
mod torrent_metadata;
mod torrent_metrics;
mod torrent_tracker;
pub mod tracker;
mod types;

/// A prelude for conveniently writing applications using this library.
pub mod prelude {
    #[cfg(feature = "dht")]
    pub use crate::dht::prelude::*;
    pub use crate::FxSession;
    pub use crate::InfoHash;
    #[cfg(feature = "lsd")]
    pub use crate::LocalServiceDiscovery;
    pub use crate::Metrics;
    pub use crate::PieceIndex;
    pub use crate::PiecePriority;
    pub use crate::SessionConfig;
    pub use crate::SessionEvent;
    pub use crate::Torrent;
    pub use crate::TorrentEvent;
    pub use crate::TorrentFlags;
    pub use crate::TorrentMetadata;
    pub use crate::TorrentState;

    pub use crate::format_bytes;
}

use crate::peer::ProtocolExtensionFlags;

const DEFAULT_TORRENT_PROTOCOL_EXTENSIONS: fn() -> ProtocolExtensionFlags = || {
    ProtocolExtensionFlags::LTEP | ProtocolExtensionFlags::Fast | ProtocolExtensionFlags::SupportV2
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
    use log::trace;
    use std::path::PathBuf;
    use std::time::Duration;
    use std::{env, fs};

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
