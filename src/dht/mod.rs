pub use config::*;
pub use ed25519::*;
pub use errors::*;
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
mod errors;
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
