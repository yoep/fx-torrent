use crate::metrics::Metric;
use crate::peer::{
    ConnectionDirection, ConnectionProtocol, Error, Metrics, PeerClientInfo, PeerEvent,
    PeerId, PeerState, Result,
};
use crate::torrent::InnerTorrent;
use crate::{BitVec, FileAttributeFlags, PieceBlock, PieceIndex, TorrentFileInfo, TorrentMetadata};
use derive_more::Display;
use fx_callback::{Callback, MultiThreadedCallback, Subscription};
use itertools::Itertools;
use log::{debug, warn};
use percent_encoding::{percent_encode, AsciiSet, NON_ALPHANUMERIC};
use reqwest::redirect::Policy;
use reqwest::Client;
use std::cmp::min;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::Mutex;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use url::Url;

const URL_ENCODE_RESERVED: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'~')
    .remove(b'.');
const STATUS_INTERVAL: Duration = Duration::from_secs(1);

/// The HTTP peer, also known as webseed, implementation that exchanges data with a HTTP server.
#[derive(Debug, Display, Clone)]
#[display("{}", inner)]
pub struct HttpPeer {
    inner: Arc<HttpPeerContext>,
}

impl HttpPeer {
    /// Create a new HTTP/webseed peer instance.
    pub fn new(url: Url, torrent: InnerTorrent) -> Result<Self> {
        let client = Client::builder()
            .redirect(Policy::limited(3))
            .build()
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))?;
        let addr = url
            .socket_addrs(|| match url.scheme() {
                "http" => Some(80),
                "https" => Some(443),
                _ => None,
            })
            .unwrap_or(Vec::new())
            .pop()
            .unwrap_or(SocketAddr::from(([120, 0, 0, 1], 80)));
        let inner = Arc::new(HttpPeerContext {
            client,
            client_info: PeerClientInfo {
                id: PeerId::new(),
                addr,
                connection_type: ConnectionDirection::Outbound,
                connection_protocol: ConnectionProtocol::Http,
            },
            url,
            addr,
            state: Mutex::new(PeerState::Idle),
            metrics: Metrics::new(),
            torrent,
            callbacks: MultiThreadedCallback::new(),
            cancellation_token: Default::default(),
        });

        let main_inner = inner.clone();
        spawn!(
            "HttpPeerContext::run",
            async move { main_inner.run().await }
        );

        Ok(Self { inner })
    }

    /// Returns the unique peer identifier within the torrent network.
    pub fn id(&self) -> &PeerId {
        &self.inner.client_info.id
    }

    /// Returns the address of the remote peer.   
    pub fn addr(&self) -> &SocketAddr {
        &self.inner.addr
    }

    /// Returns the client information of the peer.  
    pub fn client_info(&self) -> &PeerClientInfo {
        &self.inner.client_info
    }

    /// Returns the metrics of the peer.
    pub fn metrics(&self) -> &Metrics {
        &self.inner.metrics
    }

    /// Returns the state of the peer.
    pub async fn state(&self) -> PeerState {
        *self.inner.state.lock().await
    }

    /// Returns the bitfield of the remote peer.
    pub async fn remote_piece_bitfield(&self) -> BitVec {
        let total_pieces = self.inner.torrent.total_pieces().await;
        BitVec::repeat(true, total_pieces)
    }

    /// Request one or more piece blocks from the remote peer.
    pub async fn request(&self, blocks: &[PieceBlock]) {
        let metadata = match self.inner.torrent.metadata().await {
            Ok(metadata) => metadata,
            Err(_) => {
                warn!("Peer {} failed to retrieve metadata", self);
                for block in blocks {
                    self.inner
                        .torrent
                        .piece_block_rejected(&self.inner.addr, block)
                        .await;
                }
                return;
            }
        };
        let requests = blocks
            .into_iter()
            .map(|block| (block.piece, block))
            .into_group_map();

        // TODO: move the actual requests to a separate task with download queue
        for (piece, blocks) in requests {
            if let Err(e) = self
                .inner
                .request_piece(&piece, blocks.clone(), &metadata)
                .await
            {
                debug!(
                    "Peer {} failed to request piece {} blocks, {}",
                    self, piece, e
                );
                for block in blocks {
                    self.inner
                        .torrent
                        .piece_block_rejected(&self.inner.addr, block)
                        .await;
                }
            }
        }
    }

    /// Close the peer connection.
    pub fn close(&self) {
        self.inner.cancellation_token.cancel();
    }
}

impl Callback<PeerEvent> for HttpPeer {
    fn subscribe(&self) -> Subscription<PeerEvent> {
        self.inner.callbacks.subscribe()
    }
}

impl Drop for HttpPeer {
    fn drop(&mut self) {
        self.inner.cancellation_token.cancel();
    }
}

#[derive(Debug, Display)]
#[display("{}", client_info)]
struct HttpPeerContext {
    client: Client,
    client_info: PeerClientInfo,
    url: Url,
    addr: SocketAddr,
    state: Mutex<PeerState>,
    metrics: Metrics,
    torrent: InnerTorrent,
    callbacks: MultiThreadedCallback<PeerEvent>,
    cancellation_token: CancellationToken,
}

impl HttpPeerContext {
    async fn run(&self) {
        let mut stats_interval = interval(STATUS_INTERVAL);

        loop {
            select! {
                _ = self.cancellation_token.cancelled() => break,
                _ = stats_interval.tick() => {
                    self.callbacks.invoke(PeerEvent::Stats(self.metrics.snapshot()));
                    self.metrics.tick(STATUS_INTERVAL);
                }
            }
        }

        debug!("Http peer {} main loop ended", self);
    }

    /// Try to request the given piece.
    /// It returns an error if the piece couldn't be requested from the webseed.
    async fn request_piece(
        &self,
        piece_index: &PieceIndex,
        blocks: Vec<&PieceBlock>,
        metadata: &TorrentMetadata,
    ) -> Result<()> {
        let file_index = self
            .torrent
            .file_index_for(piece_index)
            .await
            .ok_or(Error::InvalidPiece(*piece_index))?;
        let piece = match self.torrent.piece(piece_index).await {
            None => return Err(Error::InvalidPiece(*piece_index)),
            Some(piece) => piece,
        };
        let mut cursor = 0usize;
        let len = blocks.len();
        let mut buffer = vec![0u8; len];

        // update the state while downloading
        self.update_state(PeerState::Downloading).await;

        while cursor < len {
            let file = self
                .torrent
                .file(&file_index)
                .await
                .ok_or(Error::InvalidPiece(*piece_index))?;
            if file.attributes().contains(FileAttributeFlags::PaddingFile) {
                cursor += file.len();
                continue;
            }

            let url = self.create_request_url(metadata, &file.info)?;
            let request_len = min(piece.length, file.len());
            let range_start = piece.offset.saturating_sub(file.torrent_offset);
            let range_end = range_start.saturating_add(request_len);

            let response = self
                .client
                .get(url)
                .header("Range", format!("bytes={}-{}", range_start, range_end))
                .send()
                .await
                .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))?;
            self.metrics
                .bytes_in
                .inc_by(response.content_length().unwrap_or(0));

            if response.status().is_success() {
                let body = response
                    .bytes()
                    .await
                    .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))?;
                self.metrics.bytes_in_useful.inc_by(body.len() as u64);

                if body.len() > request_len {
                    return Err(Error::InvalidLength(request_len as u32, body.len() as u32));
                }

                // copy the data into the buffer
                buffer[cursor..cursor + body.len()].copy_from_slice(&body);
                cursor += body.len();
            } else {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "expected status 200, but got {:?} instead",
                        response.status()
                    ),
                )));
            }

            // loop over each part that needs to be completed and fetch it from the body
            for block in &blocks {
                let data_len = buffer.len();
                let block_start = block.begin;
                let block_end = block_start.saturating_add(block.length);
                if block_end > data_len {
                    return Err(Error::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "block end {} is out of bound for response data length {}",
                            block_end, data_len
                        ),
                    )));
                }

                let data = &buffer[block_start..block_end];
                let _ = self
                    .torrent
                    .piece_block_received(&self.addr, block, data)
                    .await;
            }
        }

        self.update_state(PeerState::Idle).await;
        Ok(())
    }

    async fn update_state(&self, new_state: PeerState) {
        let mut state = self.state.lock().await;
        *state = new_state;
    }

    fn create_request_url(
        &self,
        metadata: &TorrentMetadata,
        file: &TorrentFileInfo,
    ) -> Result<Url> {
        let path = Self::create_filepath(metadata, file)?;
        let mut encoded_path_segments = Vec::new();
        let mut url = self.url.clone();

        for segment in path.iter() {
            encoded_path_segments.push(
                percent_encode(segment.to_string_lossy().as_bytes(), URL_ENCODE_RESERVED)
                    .to_string(),
            )
        }

        // remove trailing slash from the base URL if it exists
        if url.path().ends_with('/') {
            let path = url.path().to_string();
            url.set_path(&path[..url.path().len() - 1]);
        }

        // update the path segments of the url
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| Error::Parsing("invalid base url".to_string()))?;

            for segment in encoded_path_segments {
                segments.push(&segment);
            }
        }

        Ok(url)
    }

    fn create_filepath(metadata: &TorrentMetadata, file: &TorrentFileInfo) -> Result<PathBuf> {
        if let Some(name) = metadata.info.as_ref().map(|e| e.name()) {
            let mut path = PathBuf::from(name);

            if file.path_segments().len() > 0 {
                path.push(file.path());
            }

            return Ok(path);
        }

        Err(Error::Io(io::Error::new(
            io::ErrorKind::Other,
            format!("unable to create filepath for {:?}", file),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::read_test_file_to_bytes;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_state() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let url = Url::parse("https://mirror.com/pub/").unwrap();
        let torrent = torrent!(
            "debian.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            vec![]
        );
        let peer = HttpPeer::new(url, torrent.inner.clone()).expect("expected an http peer");

        let result = peer.state().await;

        assert_eq!(PeerState::Idle, result);
    }

    #[tokio::test]
    async fn test_remote_piece_bitfield() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let url = Url::parse("https://mirror.com/pub/").unwrap();
        let torrent = torrent!(
            "debian.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            vec![]
        );
        let total_pieces = torrent.total_pieces().await;
        let peer = HttpPeer::new(url, torrent.inner.clone()).expect("expected an http peer");

        let result = peer.remote_piece_bitfield().await;

        assert_eq!(BitVec::repeat(true, total_pieces), result);
    }

    #[tokio::test]
    async fn test_create_request_url() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let url = Url::parse("https://mirror.com/pub/").unwrap();
        let expected_result =
            Url::parse("https://mirror.com/pub/debian-11.6.0-amd64-netinst.iso/README%25201.md")
                .unwrap();
        let torrent = torrent!(
            "debian.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            vec![]
        );
        let metadata = torrent.metadata().await.unwrap();
        let file = TorrentFileInfo {
            length: 0,
            path: Some(vec!["README 1.md".to_string()]),
            path_utf8: None,
            md5sum: None,
            attr: None,
            symlink_path: None,
            sha1: None,
        };
        let peer = HttpPeer::new(url, torrent.inner.clone()).expect("expected an http peer");

        let result = peer
            .inner
            .create_request_url(&metadata, &file)
            .expect("expected the request url to be created");

        assert_eq!(expected_result, result);
    }

    #[test]
    fn test_create_filepath() {
        let expected_result = PathBuf::from("debian-11.6.0-amd64-netinst.iso");
        let torrent = read_test_file_to_bytes("debian.torrent");
        let metadata = TorrentMetadata::try_from(torrent.as_slice()).unwrap();
        let files = metadata.info.as_ref().unwrap().files();
        let file = files.get(0).expect("expected a file to have been present");

        let result = HttpPeerContext::create_filepath(&metadata, file)
            .expect("expected a filepath to have been returned");
        assert_eq!(expected_result, result);

        let expected_result = PathBuf::from("debian-11.6.0-amd64-netinst.iso/README.md");
        let file = TorrentFileInfo {
            length: 0,
            path: Some(vec!["README.md".to_string()]),
            path_utf8: None,
            md5sum: None,
            attr: None,
            symlink_path: None,
            sha1: None,
        };
        let result = HttpPeerContext::create_filepath(&metadata, &file)
            .expect("expected a filepath to have been returned");
        assert_eq!(expected_result, result);
    }
}
