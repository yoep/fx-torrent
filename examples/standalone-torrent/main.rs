use fx_callback::Callback;
use fx_torrent::storage::MemoryStorage;
use fx_torrent::tracker::TrackerClient;
use fx_torrent::{Torrent, TorrentEvent, TorrentFlags, TorrentMetadata, TorrentState};
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
        .storage(|_| MemoryStorage::new().into())
        .tracker(TrackerClient::new(Duration::from_secs(10)).into())
        .build()
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("failed to create torrent, {}", e),
            )
        })?;
    println!("Torrent {} has been created", torrent);

    // wait for the torrent to be initialed
    wait_for_torrent_initialization(&torrent).await;

    // resume the torrent to start downloading the file(s)
    torrent.resume().await;
    println!("Torrent state: {}", torrent.state().await);

    // request the number of pieces within the torrent
    let num_of_pieces = torrent.total_pieces().await;
    println!("Torrent has {} pieces", num_of_pieces);

    Ok(())
}

/// Wait for the torrent to be initialized.
async fn wait_for_torrent_initialization(torrent: &Torrent) {
    let mut receiver = torrent.subscribe();
    let state = torrent.state().await;

    println!("Torrent state: {}", state);
    if state != TorrentState::Initializing {
        return;
    }

    while let Ok(event) = receiver.recv().await {
        if let TorrentEvent::StateChanged(new_state) = &*event {
            println!("Torrent state changed to {}", new_state);
            if new_state != &TorrentState::Initializing {
                break;
            }
        }
    }
}

fn read_test_file_bytes(filename: &str) -> Result<Vec<u8>, io::Error> {
    let root_dir = &env::var("CARGO_MANIFEST_DIR").expect("$CARGO_MANIFEST_DIR");
    let mut source = PathBuf::from(root_dir);
    source.push("test");
    source.push(filename);

    fs::read(&source)
}
