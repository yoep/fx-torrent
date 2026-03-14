use fx_torrent::dht::{DhtTracker, PublicKey};
use fx_torrent::Sha1Hash;
use log::{info, LevelFilter};
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::Config;
use std::io;
use std::time::Duration;
use tokio::time;

const LOG_PATTERN: &str = "\x1B[37m{d(%Y-%m-%d %H:%M:%S%.3f)}\x1B[0m {h({l:>5.5})} \x1B[35m{I:>6.6}\x1B[0m \x1B[37m---\x1B[0m \x1B[37m[{T:>15.15}]\x1B[0m \x1B[36m{t:<40.40}\x1B[0m \x1B[37m:\x1B[0m {m}{n}";

/// Get an item from the DHT network.
#[tokio::main]
pub async fn main() -> Result<(), io::Error> {
    initialize_logger(LevelFilter::Info);
    let dht = DhtTracker::builder()
        .enable_indexing(false)
        .default_routing_nodes()
        .build()
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    info!("{} started on port {}", dht, dht.port());

    // wait some time to establish connections with other nodes in the network
    info!("{} is establishing connections...", dht);
    time::sleep(Duration::from_secs(20)).await;
    info!("{} connected to {} nodes", dht, dht.total_nodes().await);

    // get an immutable item from the DHT network
    get_item(&dht).await?;

    // get a mutable item from the DHT network
    get_mutable_item(&dht).await?;

    Ok(())
}

async fn get_item(dht: &DhtTracker) -> Result<(), io::Error> {
    let hash = TryInto::<Sha1Hash>::try_into(
        hex::decode(b"a4a9ad29e303e137ecb995c50a4e104b3e8f72e5").unwrap(),
    )
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid hash"))?;

    // try to get the item from the DHT network
    info!("{} is getting item from the DHT network...", dht);
    let item = dht
        .get::<String>(hash, Duration::from_secs(20), 5)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    info!("{} got item from the DHT network: {:?}", dht, item);
    Ok(())
}

async fn get_mutable_item(dht: &DhtTracker) -> Result<(), io::Error> {
    let public_key = TryInto::<PublicKey>::try_into(
        hex::decode(b"77ff84905a91936367c01360803104f92432fcd904a43511876df5cdf3e7e548").unwrap(),
    )
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid public key"))?;

    // try to get the item from the DHT network
    info!("{} is getting item from the DHT network...", dht);
    let item = dht
        .get_mutable::<String>(&public_key, None, None, Duration::from_secs(20), 5)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    info!("{} got item from the DHT network: {:?}", dht, item);
    Ok(())
}

fn initialize_logger(level: LevelFilter) {
    log4rs::init_config(
        Config::builder()
            .appender(
                Appender::builder().build(
                    "stdout",
                    Box::new(
                        ConsoleAppender::builder()
                            .encoder(Box::new(PatternEncoder::new(LOG_PATTERN)))
                            .build(),
                    ),
                ),
            )
            .build(Root::builder().appender("stdout").build(level))
            .unwrap(),
    )
    .unwrap();
}
