use fx_torrent::dht::DhtTracker;
use fx_torrent::InfoHash;
use std::io;
use std::str::FromStr;
use std::time::Duration;
use tokio::time;

/// Create a standalone DHT tracker.
/// This tracker can be used to establish a DHT network and query info from the network.
#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let info_hash =
        InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
    let dht = DhtTracker::builder()
        .enable_indexing(true)
        .default_routing_nodes()
        .build()
        .await
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("failed to create DHT node tracker, {}", e),
            )
        })?;
    println!("{} started on port {}", dht, dht.port());

    // wait some time to establish connections with other nodes in the network
    println!("{} is establishing connections...", dht);
    time::sleep(Duration::from_secs(10)).await;

    // request the number of connected nodes within the network
    let num_of_nodes = dht.total_nodes().await;
    println!("{} is connected to {} nodes", dht, num_of_nodes);

    // scrape the available peers for the given info hash
    let peers = dht
        .get_peers(&info_hash, 5, Duration::from_secs(10))
        .await
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("failed to scrape peers for info hash {}, {}", info_hash, e),
            )
        })?;
    println!("{} found {} peers for {}", dht, peers.len(), info_hash);

    Ok(())
}
