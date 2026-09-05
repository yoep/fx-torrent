use crate::channel::{ChannelReceiver, ChannelSender, Reply};
use crate::metrics::Metric;
use crate::peer::peer_context::PeerContext;
use crate::peer::{
    ConnectionDirection, ConnectionProtocol, Error, Metrics, PeerEvent, PeerId, PeerState, Result,
};
use crate::torrent::InnerTorrent;
use crate::torrent_data::DataPool;
use crate::{
    BitVec, FileAttributeFlags, PieceBlock, PieceIndex, TorrentEvent, TorrentFileInfo,
    TorrentMetadata,
};
use derive_more::Display;
use fx_callback::{Callback, MultiThreadedCallback, Subscription};
use itertools::Itertools;
use log::{debug, trace};
use percent_encoding::{percent_encode, AsciiSet, NON_ALPHANUMERIC};
use reqwest::redirect::Policy;
use reqwest::Client;
use std::cmp::{max, min};
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::select;
use tokio::task::JoinSet;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use url::Url;

const URL_ENCODE_RESERVED: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'~')
    .remove(b'.');
const STATUS_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_DESIRED_QUEUE_LEN: usize = 256;

/// The HTTP peer, also known as webseed, implementation that exchanges data with a HTTP server.
#[derive(Debug, Display, Clone)]
#[display("{}", context)]
pub struct HttpPeer {
    context: PeerContext,
    sender: ChannelSender<HttpPeerCommand>,
    callbacks: MultiThreadedCallback<PeerEvent>,
    cancellation_token: CancellationToken,
}

impl HttpPeer {
    /// Create a new HTTP/webseed peer instance.
    pub fn new(
        url: Url,
        torrent: InnerTorrent,
        data_pool: DataPool,
        metadata: TorrentMetadata,
    ) -> Result<Self> {
        let (sender, receiver) = channel!(512);
        let event_receiver = torrent.subscribe();
        let addr = Self::resolve_url(&url)?;
        let mut context = HttpPeerContext::new(url, addr, torrent, data_pool, metadata)?;
        let peer_context = context.context.clone();
        let callbacks = context.callbacks.clone();
        let cancellation_token = context.cancellation_token.clone();

        spawn!("HttpPeerContext::run", async move {
            context.run(receiver, event_receiver).await
        });

        Ok(Self {
            context: peer_context,
            sender,
            callbacks,
            cancellation_token,
        })
    }

    /// Returns the unique peer identifier within the torrent network.
    pub fn id(&self) -> &PeerId {
        self.context.id()
    }

    /// Returns the address of the remote peer.
    pub fn addr(&self) -> &SocketAddr {
        self.context.addr()
    }

    /// Returns the metrics of the peer.
    pub fn metrics(&self) -> &Metrics {
        self.context.metrics()
    }

    /// Returns the state of the peer.
    pub async fn state(&self) -> PeerState {
        self.context.state().await
    }

    /// Returns the bitfield of the remote peer.
    pub async fn remote_piece_bitfield(&self) -> BitVec {
        self.sender
            .send(|tx| HttpPeerCommand::GetRemotePieceBitfield { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the target number of requests which should be queued for the remote peer.
    pub async fn target_request_queue_len(&self) -> usize {
        self.sender
            .send(|tx| HttpPeerCommand::GetTargetQueueLen { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Request one or more piece blocks from the remote peer.
    pub async fn request(&self, blocks: &[PieceBlock]) {
        self.sender
            .fire_and_forget(HttpPeerCommand::Request {
                blocks: blocks.to_vec(),
            })
            .await
    }

    /// Close the peer connection.
    pub fn close(&self) {
        self.cancellation_token.cancel();
    }

    /// Resolve the given url to a usable socket address.
    fn resolve_url(url: &Url) -> Result<SocketAddr> {
        url.socket_addrs(|| match url.scheme() {
            "http" => Some(80),
            "https" => Some(443),
            _ => None,
        })
        .unwrap_or(Vec::new())
        .pop()
        .ok_or(Error::Io(io::Error::new(
            io::ErrorKind::HostUnreachable,
            "unable to resolve url",
        )))
    }
}

impl Callback<PeerEvent> for HttpPeer {
    fn subscribe(&self) -> Subscription<PeerEvent> {
        self.callbacks.subscribe()
    }
}

#[derive(Debug)]
enum HttpPeerCommand {
    GetRemotePieceBitfield { response: Reply<BitVec> },
    GetTargetQueueLen { response: Reply<usize> },
    Request { blocks: Vec<PieceBlock> },
}

#[derive(Debug, Display)]
#[display("{}", context)]
struct HttpPeerContext {
    client: Client,
    context: PeerContext,
    url: Url,
    torrent: InnerTorrent,
    data_pool: DataPool,
    metadata: TorrentMetadata,
    desired_queue_len: usize,
    min_desired_queue_len: usize,
    requested_queue_len: usize,
    pending_requests: JoinSet<RequestTask>,
    callbacks: MultiThreadedCallback<PeerEvent>,
    cancellation_token: CancellationToken,
}

impl HttpPeerContext {
    fn new(
        url: Url,
        addr: SocketAddr,
        torrent: InnerTorrent,
        data_pool: DataPool,
        metadata: TorrentMetadata,
    ) -> Result<Self> {
        let client = Client::builder()
            .redirect(Policy::limited(3))
            .build()
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))?;
        let desired_queue_len = metadata
            .info
            .as_ref()
            .map(|e| HttpPeerContext::calculate_desired_queue_len(e.piece_length as usize))
            .unwrap_or(DEFAULT_DESIRED_QUEUE_LEN);

        Ok(Self {
            client,
            context: PeerContext::builder()
                .id(PeerId::new())
                .addr(addr)
                .state(PeerState::Idle)
                .connection_type(ConnectionDirection::Outbound)
                .protocol(ConnectionProtocol::Http)
                .build(),
            url,
            torrent,
            data_pool,
            metadata,
            desired_queue_len,
            min_desired_queue_len: desired_queue_len,
            requested_queue_len: 0,
            pending_requests: Default::default(),
            callbacks: MultiThreadedCallback::new(),
            cancellation_token: Default::default(),
        })
    }

    async fn run(
        &mut self,
        mut command_receiver: ChannelReceiver<HttpPeerCommand>,
        mut event_receiver: Subscription<TorrentEvent>,
    ) {
        let mut stats_interval = interval(STATUS_INTERVAL);
        if self.metadata.info.is_some() {
            self.inform_piece_availability().await;
        }

        loop {
            select! {
                _ = self.cancellation_token.cancelled() => break,
                command = command_receiver.recv() => match command {
                    Some(command) => self.on_command(command).await,
                    None => break,
                },
                Ok(event) = event_receiver.recv() => self.on_torrent_event(&*event).await,
                Some(Ok(task)) = self.pending_requests.join_next() => self.on_request_completed(task).await,
                _ = stats_interval.tick() => {
                    self.callbacks.invoke(PeerEvent::Stats(self.context.metrics().snapshot()));
                    self.context.metrics().tick(STATUS_INTERVAL);
                }
            }
        }

        self.update_state(PeerState::Closed).await;
        debug!("Http peer {} main loop ended", self);
    }

    async fn on_command(&mut self, command: HttpPeerCommand) {
        match command {
            HttpPeerCommand::GetRemotePieceBitfield { response } => {
                response.send(self.remote_piece_bitfield().await)
            }
            HttpPeerCommand::GetTargetQueueLen { response } => response.send(
                self.desired_queue_len
                    .saturating_sub(self.requested_queue_len),
            ),
            HttpPeerCommand::Request { blocks } => self.on_request(blocks).await,
        }
    }

    async fn on_torrent_event(&mut self, event: &TorrentEvent) {
        match event {
            TorrentEvent::MetadataChanged(metadata) => {
                self.on_metadata_changed(metadata.clone()).await
            }
            _ => {}
        }
    }

    async fn on_request_completed(&mut self, task: RequestTask) {
        self.requested_queue_len = self.requested_queue_len.saturating_sub(task.blocks.len());
        if let Some(err) = task.err {
            debug!(
                "Peer {} failed to request piece {} blocks, {}",
                self, task.piece_index, err
            );
            for block in task.blocks {
                self.torrent
                    .piece_block_rejected(self.context.addr(), &block)
                    .await;
            }
        }

        // recalculate the desired queue length for this peer
        let download_rate = self.context.metrics().bytes_in_useful.rate();
        let target_queue_len = max(
            (Duration::from_secs(3).as_secs() * download_rate as u64) as usize
                / PieceBlock::MAX_LEN,
            self.min_desired_queue_len,
        );
        self.desired_queue_len = target_queue_len;

        // update the peer state if all pending requests have been completed
        if self.pending_requests.is_empty() {
            self.update_state(PeerState::Idle).await;
        }
    }

    async fn remote_piece_bitfield(&self) -> BitVec {
        let total_pieces = self.data_pool.num_of_pieces().await;
        BitVec::repeat(true, total_pieces)
    }

    async fn on_metadata_changed(&mut self, metadata: TorrentMetadata) {
        self.metadata = metadata;
        self.desired_queue_len = self
            .metadata
            .info
            .as_ref()
            .map(|e| Self::calculate_desired_queue_len(e.piece_length as usize))
            .unwrap_or(DEFAULT_DESIRED_QUEUE_LEN);
        self.min_desired_queue_len = self.desired_queue_len;
        self.inform_piece_availability().await;
    }

    async fn inform_piece_availability(&self) {
        let total_pieces = self.data_pool.num_of_pieces().await;
        self.torrent
            .piece_availabilities((0..total_pieces).collect_vec(), true)
            .await;
    }

    async fn on_request(&mut self, blocks: Vec<PieceBlock>) {
        self.requested_queue_len += blocks.len();
        let requests = blocks
            .into_iter()
            .map(|block| (block.piece, block))
            .into_group_map();

        self.update_state(PeerState::Downloading).await;
        for (piece, blocks) in requests {
            self.request_piece(piece, blocks.clone());
        }
    }

    /// Try to request the given piece.
    /// It returns an error if the piece couldn't be requested from the webseed.
    fn request_piece(&mut self, piece_index: PieceIndex, blocks: Vec<PieceBlock>) {
        let client = self.client.clone();
        let url = self.url.clone();
        let context = self.context.clone();
        let torrent = self.torrent.clone();
        let data_pool = self.data_pool.clone();
        let metadata = self.metadata.clone();
        self.pending_requests.spawn(async move {
            let err = Self::execute_request(
                client,
                url,
                piece_index,
                &blocks,
                context,
                torrent,
                data_pool,
                metadata,
            )
            .await
            .err();
            RequestTask {
                piece_index,
                blocks,
                err,
            }
        });
    }

    async fn update_state(&self, new_state: PeerState) {
        if self.context.state().await == new_state {
            return;
        }

        self.context.set_state(new_state).await;
        debug!("Peer {} state updated to {:?}", self, new_state);
        self.callbacks.invoke(PeerEvent::StateChanged(new_state));
    }

    fn create_multi_file_request_url(
        url: &Url,
        metadata: &TorrentMetadata,
        file: &TorrentFileInfo,
    ) -> Result<Url> {
        let mut url = url.clone();
        let path = Self::create_filepath(metadata, file)?;
        let mut encoded_path_segments = Vec::new();

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

    async fn execute_request(
        client: Client,
        url: Url,
        piece_index: usize,
        blocks: &[PieceBlock],
        context: PeerContext,
        torrent: InnerTorrent,
        data_pool: DataPool,
        metadata: TorrentMetadata,
    ) -> Result<()> {
        let piece = match data_pool.piece(&piece_index).await {
            None => return Err(Error::InvalidPiece(piece_index)),
            Some(piece) => piece,
        };
        let mut file_index = data_pool
            .file_index_for(&piece_index)
            .await
            .ok_or(Error::InvalidPiece(piece_index))?;

        let len = piece.len();
        let mut cursor = 0usize;
        let mut buffer = vec![0u8; len];

        while cursor < len {
            let file = data_pool
                .file(&file_index)
                .await
                .ok_or(Error::InvalidPiece(piece_index))?;
            let url = if data_pool.num_of_files().await > 1 {
                Self::create_multi_file_request_url(&url, &metadata, &file.info)?
            } else {
                url.clone()
            };

            if file.attributes().contains(FileAttributeFlags::PaddingFile) {
                cursor += file.len();
                file_index += 1;
                continue;
            }

            let request_len = min(piece.length, file.len());
            let range_start = piece.offset.saturating_sub(file.torrent_offset);
            let range_end = range_start.saturating_add(request_len) - 1;

            trace!("Peer {} requesting piece data {}", context, url);
            let response = client
                .get(url.clone())
                .header("Range", format!("bytes={}-{}", range_start, range_end))
                .send()
                .await
                .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))?;
            context
                .metrics()
                .bytes_in
                .inc_by(response.content_length().unwrap_or(0));

            if response.status().is_success() {
                let body = response
                    .bytes()
                    .await
                    .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))?;
                context.metrics().bytes_in_useful.inc_by(body.len() as u64);

                if body.len() > request_len {
                    return Err(Error::InvalidLength(request_len as u32, body.len() as u32));
                }

                // copy the data into the buffer
                buffer[cursor..cursor + body.len()].copy_from_slice(&body);
                cursor += body.len();
                file_index += 1;
            } else {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "expected status 200, but got {:?} instead",
                        response.status()
                    ),
                )));
            }

            // loop over each block that needs to be completed and fetch it from the body
            for block in blocks {
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
                let _ = torrent
                    .piece_block_received(context.addr(), block, data)
                    .await;
            }
        }

        Ok(())
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

    fn calculate_desired_queue_len(piece_len: usize) -> usize {
        (piece_len + PieceBlock::MAX_LEN - 1) / PieceBlock::MAX_LEN
    }
}

#[derive(Debug)]
struct RequestTask {
    piece_index: PieceIndex,
    blocks: Vec<PieceBlock>,
    err: Option<Error>,
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
        let data_pool = torrent.inner.data_pool().await.unwrap();
        let metadata = torrent.inner.metadata().await.unwrap();
        let peer = HttpPeer::new(url, torrent.inner.clone(), data_pool, metadata)
            .expect("expected an http peer");

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
        let data_pool = torrent.inner.data_pool().await.unwrap();
        let metadata = torrent.inner.metadata().await.unwrap();
        let total_pieces = torrent.total_pieces().await;
        let peer = HttpPeer::new(url, torrent.inner.clone(), data_pool, metadata)
            .expect("expected an http peer");

        let result = peer.remote_piece_bitfield().await;

        assert_eq!(BitVec::repeat(true, total_pieces), result);
    }

    #[tokio::test]
    async fn test_target_queue_len() {
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
        let data_pool = torrent.inner.data_pool().await.unwrap();
        let metadata = torrent.inner.metadata().await.unwrap();
        let piece_len = metadata
            .info
            .as_ref()
            .map(|e| e.piece_length as usize)
            .unwrap();
        let expected_result = (piece_len + PieceBlock::MAX_LEN - 1) / PieceBlock::MAX_LEN;
        let peer = HttpPeer::new(url, torrent.inner.clone(), data_pool, metadata)
            .expect("expected an http peer");

        let result = peer.target_request_queue_len().await;

        assert_eq!(expected_result, result);
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

        let data_pool = torrent.inner.data_pool().await.unwrap();
        let metadata = torrent.inner.metadata().await.unwrap();
        let addr = HttpPeer::resolve_url(&url).unwrap();
        let context = HttpPeerContext::new(
            url,
            addr,
            torrent.inner.clone(),
            data_pool,
            metadata.clone(),
        )
        .unwrap();

        let result = HttpPeerContext::create_multi_file_request_url(&context.url, &metadata, &file)
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
