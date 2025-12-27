use fx_torrent::dht::DhtTracker;
use fx_torrent::peer::ProtocolExtensionFlags;
use fx_torrent::{FxTorrentSession, Session};
use std::io;
use std::time::Duration;

/// Create a new simple torrent session and fetch the magnet information.
#[tokio::main]
async fn main() -> io::Result<()> {
    let magnet = "magnet:?xt=urn:btih:2C6B6858D61DA9543D4231A71DB4B1C9264B0685&dn=Ubuntu%2022.04%20LTS&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
    let session = FxTorrentSession::builder()
        .client_name("My session name")
        .path("torrents")
        .protocol_extensions(
            ProtocolExtensionFlags::Fast
                | ProtocolExtensionFlags::LTEP
                | ProtocolExtensionFlags::Dht,
        )
        .dht(
            DhtTracker::builder()
                .default_routing_nodes()
                .build()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
        )
        .build()
        .expect("failed to create torrent session");

    match session.fetch_magnet(magnet, Duration::from_secs(20)).await {
        Ok(info) => println!("Retrieved {:?}", info),
        Err(e) => println!("Failed to retrieve magnet info, {}", e),
    };

    Ok(())
}
