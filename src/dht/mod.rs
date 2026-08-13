/*!
# DHT

The DHT module of fx-torrent.
This module can be used as a standalone DHT implementation and doesn't require any interaction with
other modules in this crate.

## Node Modes

The DHT module supports 2 node modes:

- `client`: This mode is a **read-only** node that can be used to query the DHT network (BEP43).
- `server`: This mode is used to act as a DHT node and provide services to other nodes in the network.

_example_
```rust,no_run
# use fx_torrent::dht::{DhtTracker, Mode};

# async fn example() {
    DhtTracker::builder()
        .mode(Mode::Client)
        .build()
        .await;
# }
```
*/

pub use config::*;
pub use ed25519::*;
pub use error::*;
use handler::*;
pub use metrics::*;
pub use node::*;
pub use node_id::*;
use server::*;
pub use storage_data::*;
pub use tracker::*;

#[cfg(test)]
#[macro_use]
mod test_macros;

mod compact;
mod config;
mod ed25519;
mod error;
mod handler;
mod krpc;
mod metrics;
mod node;
mod node_id;
mod observer;
mod routing_table;
mod server;
mod storage_data;
mod tracker;
mod traversal;
mod utils;

/// A prelude for conveniently using the DHT module in your application.
pub mod prelude {
    pub use crate::dht::DhtEvent;
    pub use crate::dht::DhtMetrics;
    pub use crate::dht::DhtTracker;
    pub use crate::dht::DhtTrackerBuilder;
    pub use crate::dht::Mode;
    pub use crate::dht::Node;
    pub use crate::dht::NodeState;
    pub use crate::dht::PeerEntry;
}

const DEFAULT_ROUTING_NODE_SERVERS: fn() -> Vec<&'static str> = || {
    vec![
        "router.utorrent.com:6881",
        "router.bittorrent.com:6881",
        "dht.transmissionbt.com:6881",
        "dht.aelitis.com:6881",     // Vuze
        "dht.libtorrent.org:25401", // @arvidn's
        "dht.anacrolix.link:42069",
        "router.bittorrent.cloud:42069",
    ]
};
