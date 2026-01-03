use fx_torrent::storage::MemoryStorage;
use fx_torrent::tracker::TrackerClient;
use fx_torrent::{Torrent, TorrentFlags, TorrentMetadata};
use std::path::PathBuf;
use std::time::Duration;
use std::{env, fs, io};

/// Create a standalone torrent, which doesn't make use of a session.
#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let bytes = read_test_file_bytes("debian.torrent")?;
    let metadata = TorrentMetadata::try_from(bytes.as_slice()).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("failed to parse torrent metadata, {}", e),
        )
    })?;

    // see TorrentRequest docs for more options
    let torrent = Torrent::request()
        .metadata(metadata)
        .options(TorrentFlags::AutoManaged | TorrentFlags::Paused)
        .storage(|_| Box::new(MemoryStorage::new()))
        .tracker_manager(TrackerClient::new(Duration::from_secs(10)))
        .build()
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("failed to create torrent, {}", e),
            )
        })?;
    println!("Torrent {} has been created", torrent);
    println!("Torrent state: {}", torrent.state().await);

    // resume the torrent to start downloading the file(s)
    torrent.resume().await;
    println!("Torrent state: {}", torrent.state().await);

    Ok(())
}

fn read_test_file_bytes(filename: &str) -> Result<Vec<u8>, io::Error> {
    let root_dir = &env::var("CARGO_MANIFEST_DIR").expect("$CARGO_MANIFEST_DIR");
    let mut source = PathBuf::from(root_dir);
    source.push("test");
    source.push(filename);

    fs::read(&source)
}
