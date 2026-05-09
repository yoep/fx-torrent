# FX-Torrent

![Build](https://github.com/yoep/fx-torrent/workflows/Build/badge.svg)
[![Crates](https://img.shields.io/crates/v/fx-torrent)](https://crates.io/crates/fx-torrent)
[![License: Apache-2.0](https://img.shields.io/github/license/yoep/fx-torrent)](./LICENSE)
[![Documentation](https://docs.rs/fx-torrent/badge.svg)](https://docs.rs/fx-torrent/0.9.0/fx_torrent/)
[![codecov](https://codecov.io/gh/yoep/fx-torrent/graph/badge.svg?token=CDT6SG6YEL)](https://codecov.io/gh/yoep/fx-torrent)

FX-Torrent is the most complete BitTorrent implementation fully written in Rust, which supports both Linux, MacOS, and Windows.
It supports most of the Bittorrent protocol specifications, such as multi-file torrents, validating existing files, resuming torrent files,
and is based on the `libtorrent` library for functionality and naming convention.

- [Getting Started](#getting-started)
- [Features](#features)
- [CLI example](#cli-example)
- [DHT](#dht)
- [Extensions](#extensions)

## Getting Started

Create a new `FxTorrentSession` which manages one or more torrents.
A `Torrent` can be created from a magnet link, torrent file, or passing the raw `TorrentMetadata`.

_create a new session with torrent_
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
                .base_path("/torrent/location/directory")
                .client_name("MyClient")
                .build(),
        )
        .default_extensions()
        .build()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    // Create a torrent from a magnet link
    let magnet_torrent = session.add_torrent_from_uri("magnet:?XXX", TorrentFlags::default()).await;

    // Create a torrent from a torrent file
    let file_torrent = session.add_torrent_from_uri("/tmp/example.torrent", TorrentFlags::default()).await;

    // Create a torrent from metadata info
    let data: &[u8] = &[0; 1024];
    let metadata: TorrentMetadata = TorrentMetadata::try_from(data)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let metadata_torrent = session.add_torrent_from_metadata(metadata, TorrentFlags::Paused).await;

    Ok(())
}
```

For more examples, see the [examples](./examples) directory.

### CLI example

The CLI example makes use of most of the functionality provided by the library and 
can be used to download torrents from magnet links or torrent files.
The CLI also allows the introspection of the DHT network and Trackers.

The example is built on top of the [Ratatui](https://ratatui.rs/) as terminal UI library.

#### Tracing & Tokio Unstable

The CLI example enables the **tracing** feature by default to support the `tokio-console` subscriber. 
This requires the `tokio_unstable` configuration flag to be passed to the compiler.

```shell
RUSTFLAGS="--cfg tokio_unstable" cargo run --example cli
```

If you do not wish to use experimental tokio features, you must disable the `tracing` feature in the example.

## DHT

When using the `dht` feature, enabled by default, one of the following additional features should be enabled:
- `ed25519-dalek`
- `ring-compat`

These crypto providers are used within the DHT network to verify mutable items within the network.
When both features are missing, a `Error::MissingCryptoProvider` error will be returned.

## Extensions

The `fx_torrent` crate is designed to be highly extensible.
You can modify the core behavior of components by implementing and registering "extension" traits.
These allow for custom logic in peer communication and data persistence.

### Peer Extension

Peer extensions allow you to extend the BitTorrent protocol with custom messaging and handshake
capabilities, following the **BEP 10** specification.

To modify peer protocol behavior, implement the `peer::extension::Extension` trait.
Once implemented, these extensions can be attached to individual torrents or globally across a session.

_example peer extension_
```rust
#[derive(Debug)]
pub struct MyPeerExtension;
impl Extension for MyPeerExtension {
    fn name(&self) -> &str {
        "my-extension"
    }

    // Additional trait methods
}

fn example() {
    // 1. Peer extension directly in a torrent
    let torrent = Torrent::request()
        .extension(|| MyPeerExtension.into())
        .build()
        .unwrap();

    // 2. Peer extension in a session
    let session = FxTorrentSession::builder()
        .extension(|| MyPeerExtension.into())
        .build()
        .unwrap();
}
```

### Storage Extension

Storage extensions allow you to customize how data is read from and written to disk (or memory).
This is useful for implementing custom caching layers, encrypted storage, or cloud-backed persistence.

To create your own storage backend, implement the `storage::Extension` trait.

_example storage extension_
```rust
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

fn example() {
    // 1. Storage extension directly in a torrent
    let torrent = Torrent::request()
        .storage(|params| MyStorageExtension::new(params).into())
        .build()
        .unwrap();

    // 2. Storage extension in a session
    let session = FxTorrentSession::builder()
        .storage(|params| MyStorageExtension::new(params).into())
        .build().unwrap();
}
```

### Operation Extension

Operation extensions are **tick-based** tasks invoked by the `TorrentContext`.
These operations are executed sequentially in an order-dependent chain,
meaning the sequence in which you register them determines their execution priority.

_example operation extension_
```rust
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

fn example() {
    // 1. Operation extension directly in a torrent
    let torrent = Torrent::request()
        .operation(MyOperation.into())
        .build()
        .unwrap();

    // 2. Operation extension in a session
    let session = FxTorrentSession::builder()
        .operation(|| MyOperation.into())
        .build()
        .unwrap();
}
```

## Features

- [x] [BEP3](https://www.bittorrent.org/beps/bep_0003.html) - The BitTorrent Protocol Specification
- [x] [BEP4](https://www.bittorrent.org/beps/bep_0004.html) - Assigned Numbers
- [x] [BEP5](https://www.bittorrent.org/beps/bep_0005.html) - DHT Protocol
- [x] [BEP6](https://www.bittorrent.org/beps/bep_0006.html) - Fast Extension
- [x] [BEP7](https://www.bittorrent.org/beps/bep_0007.html) - IPv6 Tracker Extension
- [x] [BEP9](https://www.bittorrent.org/beps/bep_0009.html) - Extension for Peers to Send Metadata Files
- [x] [BEP10](https://www.bittorrent.org/beps/bep_0010.html) - Extension Protocol
- [x] [BEP11](https://www.bittorrent.org/beps/bep_0011.html) - Peer Exchange (PEX)
- [x] [BEP12](https://www.bittorrent.org/beps/bep_0012.html) - Multitracker Metadata Extension
- [x] [BEP14](https://www.bittorrent.org/beps/bep_0014.html) - Local Service Discovery
- [x] [BEP15](https://www.bittorrent.org/beps/bep_0015.html) - UDP Tracker Protocol for BitTorrent
- [x] [BEP19](https://www.bittorrent.org/beps/bep_0019.html) - WebSeed - HTTP/FTP Seeding (GetRight style)
- [x] [BEP20](https://www.bittorrent.org/beps/bep_0020.html) - Peer ID Conventions
- [x] [BEP21](https://www.bittorrent.org/beps/bep_0021.html) - Extension for partial seeds
- [x] [BEP24](https://www.bittorrent.org/beps/bep_0024.html) - Tracker Returns External IP
- [x] [BEP29](https://www.bittorrent.org/beps/bep_0029.html) - uTorrent transport protocol
- [x] [BEP32](https://www.bittorrent.org/beps/bep_0032.html) - BitTorrent DHT Extensions for IPv6
- [x] [BEP33](https://www.bittorrent.org/beps/bep_0033.html) - DHT scrape
- [x] [BEP40](https://www.bittorrent.org/beps/bep_0040.html) - Canonical Peer Priority
- [x] [BEP42](https://www.bittorrent.org/beps/bep_0042.html) - DHT Security extension
- [x] [BEP43](https://www.bittorrent.org/beps/bep_0043.html) - Read-only DHT Nodes
- [x] [BEP44](https://www.bittorrent.org/beps/bep_0044.html) - Storing arbitrary data in the DHT
- [x] [BEP47](https://www.bittorrent.org/beps/bep_0047.html) - Padding files and extended file attributes
- [x] [BEP48](https://www.bittorrent.org/beps/bep_0048.html) - Tracker Protocol Extension: Scrape
- [x] [BEP51](https://www.bittorrent.org/beps/bep_0051.html) - DHT Infohash Indexing
- [ ] [BEP52](https://www.bittorrent.org/beps/bep_0052.html) - The BitTorrent Protocol Specification v2 (WIP)
- [x] [BEP53](https://www.bittorrent.org/beps/bep_0053.html) - Magnets
- [x] [BEP54](https://www.bittorrent.org/beps/bep_0054.html) - The lt_donthave extension
- [x] [BEP55](https://www.bittorrent.org/beps/bep_0055.html) - Holepunch extension

## License

This project is licensed under the [Apache-2.0 license](./LICENSE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license,
shall be licensed as above, without any additional terms or conditions.
