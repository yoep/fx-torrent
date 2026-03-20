use crate::channel::{ChannelReceiver, ChannelSender, Reply};
use crate::config::TorrentConfig;
use crate::errors::Result;
use crate::file::File;
use crate::operation::{TorrentOperation, TorrentOperationResult, DEFAULT_OPERATIONS};
use crate::peer::extension::{Extension, Extensions};
use crate::peer::{
    BitTorrentPeer, CloseReason, Peer, PeerClientInfo, PeerDiscovery, PeerEntry, PeerHandle,
    PeerId, ProtocolExtensionFlags,
};
use crate::peer_pool::{PeerIdentifier, PeerPool};
use crate::storage::{Storage, StorageParams};
use crate::torrent_data::DataPool;
use crate::tracker::{
    AnnounceEvent, AnnouncementResult, TrackerClient, TrackerClientEvent, TrackerEntry,
};
use crate::{
    DhtOption, FileAttributeFlags, FileIndex, InfoHash, Metrics, Piece, PieceChunkPool, PieceIndex,
    PiecePart, PiecePriority, Sha1Hash, Sha256Hash, TorrentError, TorrentFlags, TorrentMetadata,
    TorrentMetadataInfo, TorrentPeer, DEFAULT_TORRENT_EXTENSIONS,
    DEFAULT_TORRENT_PROTOCOL_EXTENSIONS,
};
use bit_vec::BitVec;
use derive_more::Display;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use fx_callback::{Callback, MultiThreadedCallback, Subscriber, Subscription};
use fx_handle::Handle;
use log::{debug, error, info, trace, warn};
use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Display, Formatter};
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::{select, time};
use tokio_util::sync::{
    CancellationToken, WaitForCancellationFuture, WaitForCancellationFutureOwned,
};
#[cfg(feature = "tracing")]
use tracing::instrument;
use url::Url;

const PEER_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const OPERATIONS_INTERVAL: Duration = Duration::from_secs(1);

/// A unique handle identifier of a [Torrent].
pub type TorrentHandle = Handle;

/// The chain of torrent operations that are executed for each torrent.
pub type TorrentOperations = Vec<Box<dyn TorrentOperation>>;

/// A [Torrent] extension factory.
/// This factory will create a new instance of an [Extension] for each new torrent.
pub type ExtensionFactory = fn() -> Box<dyn Extension>;

/// A list of [Torrent] extension factories.
pub type ExtensionFactories = Vec<ExtensionFactory>;

/// Creates a new torrent [Storage] instance.
pub type StorageFactory = dyn FnOnce(StorageParams) -> Box<dyn Storage> + Send + Sync;

/// The states of the torrent
#[derive(Debug, Display, Copy, Clone, PartialEq)]
pub enum TorrentState {
    /// The torrent is being initialized
    #[display("initializing")]
    Initializing,
    /// The torrent is trying to retrieve the metadata from peers.
    #[display("retrieving metadata")]
    RetrievingMetadata,
    /// The torrent has not started its download yet, and is currently checking existing files.
    #[display("validating files")]
    CheckingFiles,
    /// The torrent is being downloaded. This is the state most torrents will be in most of the time.
    #[display("downloading")]
    Downloading,
    /// In this state the torrent has finished downloading but still doesn't have the entire torrent.
    #[display("finished")]
    Finished,
    /// In this state the torrent has finished downloading and is a pure seeder.
    #[display("seeding")]
    Seeding,
    /// The torrent is currently paused and no longer executing any operations.
    #[display("paused")]
    Paused,
    /// The torrent encountered an unrecoverable error.
    #[display("error")]
    Error,
    /// The torrent has been stopped and is no longer executing any operations.
    #[display("stopped")]
    Stopped,
}

impl TorrentState {
    /// Check if the current state is an initialization phase state.
    pub fn is_initializing_phase(&self) -> bool {
        self == &TorrentState::Initializing
            || self == &TorrentState::RetrievingMetadata
            || self == &TorrentState::CheckingFiles
    }
}

impl Default for TorrentState {
    fn default() -> Self {
        Self::Initializing
    }
}

/// Requests a new torrent creation based on the given data.
/// This is the **recommended** way to create new torrents.
///
/// # Examples
///
/// ```rust,no_run
/// use fx_torrent::torrent::{Torrent, TorrentFlags, TorrentMetadata, TorrentRequest, MagnetResult, ExtensionFactories, CompactResult};
/// use fx_torrent::torrent::storage::{DiskStorage};
/// use fx_torrent::torrent::peer::extension::Extensions;
/// use fx_torrent::torrent::peer::{PeerDiscovery, TcpPeerDiscovery};
///
/// fn create_new_torrent(
///     metadata: TorrentMetadata,
///     extensions: ExtensionFactories,
/// ) -> CompactResult<Torrent> {
///     // create a tcp peer discovery for dialing and accepting tpc connections
///     let peer_discovery = TcpPeerDiscovery::new();
///
///     Torrent::request()
///         .metadata(metadata)
///         .options(TorrentFlags::AutoManaged)
///         .extensions(extensions)
///         .storage(|params| {
///             Box::new(DiskStorage::new(params.info_hash, params.path, params.files))
///         })
///         .peer_discovery(Box::new(peer_discovery))
///         .build()
/// }
/// ```
#[derive(Default)]
pub struct TorrentRequest {
    /// The torrent metadata information
    metadata: Option<TorrentMetadata>,
    /// The torrent options
    options: Option<TorrentFlags>,
    /// The torrent configuration
    config: Option<TorrentConfig>,
    /// The discovery strategies for peer connections.
    peer_discoveries: Option<Vec<Box<dyn PeerDiscovery>>>,
    /// The protocol extensions that should be enabled
    protocol_extensions: Option<ProtocolExtensionFlags>,
    /// The factories for creating the peer extensions that should be enabled for this torrent
    extensions: Option<ExtensionFactories>,
    /// The storage strategy to use for the torrent data
    storage: Option<Box<StorageFactory>>,
    /// The operations used by the torrent for processing data
    operations: Option<Vec<Box<dyn TorrentOperation>>>,
    /// The DHT node server to use for discovering peers
    dht: Option<DhtOption>,
    /// The peer tracker manager for the torrent
    tracker_manager: Option<TrackerClient>,
}

impl TorrentRequest {
    /// Set the torrent metadata
    pub fn metadata(&mut self, metadata: TorrentMetadata) -> &mut Self {
        self.metadata = Some(metadata);
        self
    }

    /// Set the torrent options
    pub fn options(&mut self, options: TorrentFlags) -> &mut Self {
        self.options = Some(options);
        self
    }

    /// Set the torrent configuration
    pub fn config(&mut self, config: TorrentConfig) -> &mut Self {
        self.config = Some(config);
        self
    }

    /// Add the given peer dialer to the torrent.
    ///
    /// ## Remark
    ///
    /// The order in which the dialers are added are important for outgoing connections.
    pub fn peer_discovery(&mut self, dialer: Box<dyn PeerDiscovery>) -> &mut Self {
        self.peer_discoveries.get_or_insert(Vec::new()).push(dialer);
        self
    }

    /// Set the given peer dialers of the torrent.
    ///
    /// ## Remark
    ///
    /// The order of the dialers are important for outgoing connections.
    pub fn peer_discoveries(&mut self, dialers: Vec<Box<dyn PeerDiscovery>>) -> &mut Self {
        self.peer_discoveries = Some(dialers);
        self
    }

    /// Set the protocol extensions that should be enabled
    pub fn protocol_extensions(&mut self, extensions: ProtocolExtensionFlags) -> &mut Self {
        self.protocol_extensions = Some(extensions);
        self
    }

    /// Add the given extension factory that should be activated.
    pub fn extension(&mut self, extension: ExtensionFactory) -> &mut Self {
        self.extensions.get_or_insert(Vec::new()).push(extension);
        self
    }

    /// Set the extension factories that should be activated for this torrent
    pub fn extensions(&mut self, extensions: ExtensionFactories) -> &mut Self {
        self.extensions = Some(extensions);
        self
    }

    /// Set the underlying storage for storing the torrent file data.
    pub fn storage<F>(&mut self, storage: F) -> &mut Self
    where
        F: FnOnce(StorageParams) -> Box<dyn Storage> + Send + Sync + 'static,
    {
        self.storage = Some(Box::new(storage));
        self
    }

    /// Add the operation to the torrent for processing data.
    pub fn operation(&mut self, operation: Box<dyn TorrentOperation>) -> &mut Self {
        self.operations.get_or_insert(Vec::new()).push(operation);
        self
    }

    /// Set the operations used by the torrent for processing data
    pub fn operations(&mut self, operations: Vec<Box<dyn TorrentOperation>>) -> &mut Self {
        self.operations = Some(operations);
        self
    }

    /// Set the DHT node server to use for discovering peers.
    pub fn dht(&mut self, dht: DhtOption) -> &mut Self {
        self.dht = Some(dht);
        self
    }

    /// Set the tracker manager for discovering peers.
    pub fn tracker_manager(&mut self, tracker_manager: TrackerClient) -> &mut Self {
        self.tracker_manager.get_or_insert(tracker_manager);
        self
    }

    /// Build the torrent from the given data.
    /// This is the same as calling `Torrent::try_from(self)`.
    pub fn build(&mut self) -> Result<Torrent> {
        Torrent::try_from(self)
    }

    /// Get the list of default operations for the torrent.
    pub fn default_operations() -> Vec<Box<dyn TorrentOperation>> {
        DEFAULT_OPERATIONS()
    }
}

impl Debug for TorrentRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TorrentRequest")
            .field("metadata", &self.metadata)
            .field("options", &self.options)
            .field("config", &self.config)
            .field("peer_discoveries", &self.peer_discoveries)
            .field("protocol_extensions", &self.protocol_extensions)
            .field("operations", &self.operations)
            .field("dht", &self.dht)
            .field("tracker_manager", &self.tracker_manager)
            .finish()
    }
}

impl TryFrom<&mut TorrentRequest> for Torrent {
    type Error = TorrentError;

    fn try_from(request: &mut TorrentRequest) -> Result<Self> {
        let metadata = request.metadata.take().ok_or(TorrentError::InvalidRequest(
            "metadata is missing".to_string(),
        ))?;
        let peer_discoveries = request
            .peer_discoveries
            .take()
            .unwrap_or(Vec::with_capacity(0));
        let protocol_extensions = request
            .protocol_extensions
            .unwrap_or_else(DEFAULT_TORRENT_PROTOCOL_EXTENSIONS);
        let extensions = request
            .extensions
            .take()
            .unwrap_or_else(DEFAULT_TORRENT_EXTENSIONS);
        let options = request.options.unwrap_or(TorrentFlags::default());
        let config = request
            .config
            .take()
            .unwrap_or_else(|| TorrentConfig::builder().build());
        let data_pool = DataPool::new();
        let storage = request.storage.take().ok_or(TorrentError::InvalidRequest(
            "file storage is missing".to_string(),
        ))?;
        let storage_params = StorageParams {
            info_hash: metadata.info_hash.clone(),
            path: config.path().to_path_buf(),
            data_pool: data_pool.clone(),
        };
        let operations = request
            .operations
            .take()
            .unwrap_or_else(TorrentRequest::default_operations);
        let dht = request.dht.take().unwrap_or_else(|| DhtOption::default());
        let tracker_manager =
            request
                .tracker_manager
                .take()
                .ok_or(TorrentError::InvalidRequest(
                    "tracker_manager is missing".to_string(),
                ))?;

        Ok(Self::new(
            metadata,
            peer_discoveries.into_iter().map(Arc::from).collect(),
            protocol_extensions,
            extensions,
            options,
            config,
            data_pool,
            Arc::from(storage(storage_params)),
            operations,
            dht,
            tracker_manager,
        ))
    }
}

/// The result metrics from a tracker scrape.
#[derive(Debug, Clone, PartialEq)]
pub struct ScrapeMetrics {
    /// The number of active peers that have completed downloading.
    pub complete: u32,
    /// The number of active peers that have not completed downloading.
    pub incomplete: u32,
    /// The number of peers that have ever completed downloading.
    pub downloaded: u32,
}

#[derive(Debug, Display, Clone, PartialEq)]
pub enum TorrentEvent {
    /// Invoked when the status of the torrent has changed
    #[display("torrent state has changed to {}", _0)]
    StateChanged(TorrentState),
    /// Invoked when the torrent metadata has been changed
    #[display("torrent metadata has been changed")]
    MetadataChanged(TorrentMetadata),
    /// Invoked when a new peer connection has been established
    #[display("peer {} has been connected", _0)]
    PeerConnected(PeerClientInfo),
    /// Invoked when an existing peer connection has closed.
    #[display("peer {} has been disconnected", _0)]
    PeerDisconnected(PeerClientInfo),
    /// Invoked when the active trackers have been changed
    #[display("trackers have changed")]
    TrackersChanged,
    /// Invoked when the pieces have changed of the torrent
    #[display("torrent pieces have changed to {}", _0)]
    PiecesChanged(usize),
    /// Invoked when the priorities of the torrent pieces have changed
    #[display("torrent piece priorities have changed")]
    PiecePrioritiesChanged,
    /// Invoked when a piece has been completed.
    #[display("piece {} has been completed", _0)]
    PieceCompleted(PieceIndex),
    /// Invoked when the files have changed of the torrent
    #[display("torrent files have changed")]
    FilesChanged,
    /// Invoked when the options of the torrent have been changed
    #[display("torrent options have changed")]
    OptionsChanged,
    /// Invoked when the torrent metrics have been updated
    #[display("torrent stats changed {:?}", _0)]
    Stats(Metrics),
}

/// A torrent is an actual tracked torrent which is communicating with one or more trackers and peers.
///
/// Use [TorrentMetadata] if you only want to retrieve the metadata of a torrent.
#[derive(Debug, Display, Clone)]
#[display("{}", inner)]
pub struct Torrent {
    /// The unique peer id of this torrent
    /// This id is used as our client id when connecting to peers
    peer_id: PeerId,
    metrics: Metrics,
    pub(crate) inner: InnerTorrent,
    instance_counter: Arc<()>,
    cancellation_token: CancellationToken,
}

impl Torrent {
    /// Create a new request builder for creating a new torrent.
    /// See [TorrentRequest] for more information.
    pub fn request() -> TorrentRequest {
        TorrentRequest::default()
    }

    fn new(
        metadata: TorrentMetadata,
        peer_discoveries: Vec<Arc<dyn PeerDiscovery>>,
        protocol_extensions: ProtocolExtensionFlags,
        extensions: ExtensionFactories,
        options: TorrentFlags,
        config: TorrentConfig,
        data_pool: DataPool,
        storage: Arc<dyn Storage>,
        operations: Vec<Box<dyn TorrentOperation>>,
        dht: DhtOption,
        tracker_manager: TrackerClient,
    ) -> Self {
        let info_hash = metadata.info_hash.clone();
        let (command_sender, command_receiver) = channel!(1024);
        let location = config.path().to_path_buf();
        let mut context = TorrentContext::new(
            metadata,
            config,
            peer_discoveries.first().map(|e| e.port()),
            protocol_extensions,
            extensions,
            options,
            data_pool,
            dht,
            tracker_manager,
            storage,
            command_sender,
        );

        let torrent = Self {
            peer_id: context.peer_id,
            metrics: context.metrics.clone(),
            inner: InnerTorrent::new(
                context.handle,
                context.command_sender().clone(),
                context.callbacks.clone(),
            ),
            instance_counter: Arc::new(Default::default()),
            cancellation_token: context.cancellation_token.clone(),
        };

        tokio::spawn(async move {
            context
                .run(operations, peer_discoveries, command_receiver)
                .await;
        });

        info!(
            "Torrent {} (info hash {}) created with storage location {:?}",
            torrent, info_hash, location
        );
        torrent
    }

    /// Returns the unique handle of the torrent.
    pub fn handle(&self) -> TorrentHandle {
        self.inner.handle()
    }

    /// Returns `true` if the torrent is still valid, else `false` if it has been closed/stopped.
    pub fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    /// Returns the unique peer id of this torrent.
    /// This ID is used within the peer clients to identify with remote peers.
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// Returns the port number on which the torrent is listening for incoming connections,
    /// or [None] if the torrent is not listening for incoming peer connections.
    pub async fn peer_port(&self) -> Option<u16> {
        self.inner
            .sender
            .send(|tx| TorrentCommand::PeerPort { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Add a new peer address for this torrent.
    ///
    /// The address is added to the peer pool. A new connection may be established
    /// when additional peers are needed.
    ///
    /// # Errors
    ///
    /// When the torrent is invalid or stopped, it returns [TorrentError::InvalidHandle].
    pub async fn add_peer(&self, addr: SocketAddr) -> Result<()> {
        self.inner.add_peer(addr).await
    }

    /// Get the absolute filesystem path to a given file in the torrent.
    ///
    /// This combines the torrent's storage path with the file's [`torrent_path`]
    /// to produce a full path on the local filesystem.
    ///
    /// # Errors
    ///
    /// When the torrent is invalid or stopped, it returns [TorrentError::InvalidHandle].
    pub async fn absolute_file_path(&self, file: &File) -> Result<PathBuf> {
        Ok(self
            .inner
            .sender
            .send(|tx| TorrentCommand::GetFilePath {
                file: file.clone(),
                response: tx,
            })
            .await
            .await?)
    }

    /// Get the absolute path to the torrent location.
    /// This can either be a file or directory to the torrent depending on the type of the torrent.
    ///
    /// The path is only available when the `metadata` of the torrent is known.
    /// See [Torrent::is_metadata_known].
    ///
    /// # Returns
    ///
    /// It returns the location of the torrent if the metadata is known, else [None].
    pub async fn path(&self) -> Option<PathBuf> {
        self.inner
            .sender
            .send(|tx| TorrentCommand::GetPath { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the info hash of the torrent.
    ///
    /// # Errors
    ///
    /// When the torrent is invalid or stopped, it returns [TorrentError::InvalidHandle].
    pub async fn info_hash(&self) -> Result<InfoHash> {
        self.inner.info_hash().await
    }

    /// Returns the state of the torrent.
    pub async fn state(&self) -> TorrentState {
        self.inner.state().await
    }

    /// Returns the enabled protocol extensions of the torrent.
    ///
    /// # Errors
    ///
    /// When the torrent is invalid or stopped, it returns [TorrentError::InvalidHandle].
    pub async fn protocol_extensions(&self) -> Result<ProtocolExtensionFlags> {
        self.inner.protocol_extensions().await
    }

    /// Get the metric statics of the torrent.
    /// These are collected from each active peer connection within the torrent and are periodically scraped.
    ///
    /// # Returns
    ///
    /// It returns the statics of this torrent.
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Returns the metadata of the torrent, or [TorrentError::InvalidHandle] when the torrent is invalid.
    pub async fn metadata(&self) -> Result<TorrentMetadata> {
        self.inner.metadata().await
    }

    /// Returns `true` when the metadata of the torrent is known, else `false` when it's still being retrieved.
    pub async fn is_metadata_known(&self) -> bool {
        self.inner.is_metadata_known().await
    }

    /// Returns the active options of the torrent.
    pub async fn options(&self) -> Result<TorrentFlags> {
        self.inner.options().await
    }

    /// Add the given options to the torrent.
    ///
    /// It triggers the [TorrentEvent::OptionsChanged] event if the options changed.
    /// If the options are already present, this will be a no-op.
    pub async fn add_options(&self, options: TorrentFlags) {
        let _ = self
            .inner
            .sender
            .send(|tx| TorrentCommand::AddOptions {
                options,
                response: tx,
            })
            .await
            .await;
    }

    /// Remove the given options to the torrent.
    ///
    /// It triggers the [TorrentEvent::OptionsChanged] event if the options changed.
    /// If none of the given options are present, this will be a no-op.
    pub async fn remove_options(&self, options: TorrentFlags) {
        let _ = self
            .inner
            .sender
            .send(|tx| TorrentCommand::RemoveOptions {
                options,
                response: tx,
            })
            .await
            .await;
    }

    /// Return the total amount of pieces for this torrent.
    /// If the metadata is still being retrieved, the total pieces cannot yet be known and this will result in 0.
    pub async fn total_pieces(&self) -> usize {
        self.inner.total_pieces().await
    }

    /// Get the total number of completed pieces for this torrent.
    ///
    /// # Returns
    ///
    /// It returns the total amount of completed pieces of this torrent when known.
    pub async fn total_completed_pieces(&self) -> usize {
        self.inner
            .sender
            .send(|tx| TorrentCommand::NumOfCompletedPieces { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Retrieve the torrent pieces, if known.
    /// If the metadata is still being retrieved, the pieces cannot yet be created and will result in [None].
    ///
    /// # Returns
    ///
    /// Returns the current torrent pieces when known, else [None].
    pub async fn pieces(&self) -> Option<Vec<Piece>> {
        self.inner
            .sender
            .send(|tx| TorrentCommand::GetPieces { response: tx })
            .await
            .await
            .ok()
    }

    /// Returns the information about a specific piece within the torrent.
    /// If the pieces are not yet known, in case the metadata is still being retrieved, then it returns [None].
    ///
    /// If a piece index is requested out-of-bounds of the pieces, [None] will also be returned.
    pub async fn piece(&self, piece: &PieceIndex) -> Option<Piece> {
        self.inner.piece(piece).await
    }

    /// Get the priorities of the pieces.
    /// It might return an empty array if the metadata is still being retrieved.
    pub async fn piece_priorities(&self) -> BTreeMap<PieceIndex, PiecePriority> {
        self.inner
            .sender
            .send(|tx| TorrentCommand::GetPiecePriorities { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Set the priorities of the pieces.
    /// Use [Torrent::piece_priorities] to get the current priorities with its [PieceIndex].
    ///
    /// Providing all piece indexes of the torrent is not required.
    pub async fn prioritize_pieces(&self, priorities: Vec<(PieceIndex, PiecePriority)>) {
        let _ = self
            .inner
            .sender
            .send(|tx| TorrentCommand::PrioritizePieces {
                priorities,
                response: tx,
            })
            .await
            .await;
    }

    /// Get if the given piece index has completed downloading, validating, and written to the storage.
    ///
    /// # Returns
    ///
    /// Returns true if the piece has been downloaded, validated, and written to storage, else false.
    pub async fn has_piece(&self, piece: &PieceIndex) -> bool {
        self.inner
            .sender
            .send(|tx| TorrentCommand::HasPiece {
                piece: *piece,
                response: tx,
            })
            .await
            .await
            .unwrap_or_default()
    }

    /// Prioritize the given bytes within the torrent.
    /// This will match the bytes against the relevant pieces, and prioritize those pieces.
    pub async fn prioritize_bytes(&self, bytes: &std::ops::Range<usize>, priority: PiecePriority) {
        let _ = self
            .inner
            .sender
            .send(|tx| TorrentCommand::PrioritizeBytes {
                bytes: bytes.clone(),
                priority,
                response: tx,
            })
            .await
            .await;
    }

    /// Get if the given byte range has completed downloading, validating and written to the storage.
    ///
    /// # Returns
    ///
    /// Returns true if the bytes have been downloaded, validated and written to storage.
    pub async fn has_bytes(&self, range: &std::ops::Range<usize>) -> bool {
        self.inner
            .sender
            .send(|tx| TorrentCommand::HasBytes {
                bytes: range.clone(),
                response: tx,
            })
            .await
            .await
            .unwrap_or_default()
    }

    /// Get the total files of the torrent.
    /// If the metadata is still being retrieved, the files cannot yet be created and will result in [None].
    ///
    /// # Returns
    ///
    /// Returns the total files of the torrent when known, else [None].
    pub async fn total_files(&self) -> Option<usize> {
        self.inner
            .sender
            .send(|tx| TorrentCommand::NumOfFiles { response: tx })
            .await
            .await
            .ok()
    }

    /// Returns the files of the torrent, if the metadata is known (see [Torrent::is_metadata_known]).
    /// If the metadata is still being retrieved, the returned files array will be empty.
    pub async fn files(&self) -> Vec<File> {
        self.inner.files().await
    }

    /// Returns the [File] for the given torrent file index, if available.
    pub async fn file(&self, file: &FileIndex) -> Option<File> {
        self.inner.file(file).await
    }

    /// Set the priorities of the torrent files.
    /// Use [Torrent::files] to get the current files with their respective [FileIndex].
    ///
    /// Providing all file indexes of the torrent is not required.
    pub async fn prioritize_files(&self, priorities: Vec<(FileIndex, PiecePriority)>) {
        let _ = self
            .inner
            .sender
            .send(|tx| TorrentCommand::PrioritizeFiles {
                priorities,
                response: tx,
            })
            .await
            .await;
    }

    /// Returns the number of healthy peer connections in the torrent.
    pub async fn active_peer_connections(&self) -> usize {
        self.inner
            .sender
            .send(|tx| TorrentCommand::NumOfActivePeerConnections { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the number of active tracker connections in the torrent.
    pub async fn active_tracker_connections(&self) -> usize {
        self.inner
            .sender
            .send(|tx| TorrentCommand::NumOfActiveTrackerConnections { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Check if the torrent has completed downloading all wanted pieces.
    pub async fn is_completed(&self) -> bool {
        self.inner
            .sender
            .send(|tx| TorrentCommand::IsCompleted { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns `true` when the torrent is currently paused.
    pub async fn is_paused(&self) -> bool {
        self.inner
            .sender
            .send(|tx| TorrentCommand::IsPaused { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Announce this torrent to the known trackers.
    /// This will retrieve the announcement information from the trackers.
    ///
    /// # Returns
    ///
    /// Returns the announcement information, or [TorrentError::InvalidHandle] when the torrent is invalid.
    pub async fn announce(&self) -> Result<AnnouncementResult> {
        if !self.is_valid() {
            return Err(TorrentError::InvalidHandle(self.inner.handle));
        }

        // try to wait for at least 2 connections
        if self.active_tracker_connections().await == 0 {
            self.wait_for_trackers(2).await;
        }

        Ok(self
            .inner
            .sender
            .send(|tx| TorrentCommand::AnnounceAll { response: tx })
            .await
            .await?)
    }

    /// Scrape the trackers of the torrent to retrieve the metrics.
    pub async fn scrape(&self) -> Result<ScrapeMetrics> {
        if !self.is_valid() {
            return Err(TorrentError::InvalidHandle(self.inner.handle));
        }

        // try to wait for at least 2 connections
        if self.active_tracker_connections().await == 0 {
            self.wait_for_trackers(2).await;
        }

        self.inner
            .sender
            .send(|tx| TorrentCommand::ScrapeAll { response: tx })
            .await
            .await
    }

    /// Get a "weak" reference to a peer in this torrent identified by `handle`.
    ///
    /// This looks up the `handle` within the peer pool of the torrent.
    /// When found, it will create a weak reference to the [Peer].
    /// Before calling a method, make sure to check if the reference is still valid by calling [TorrentPeer::is_valid].
    ///
    /// # Arguments
    ///
    /// * `handle` — The [`PeerHandle`] reference to look up.
    ///
    /// # Returns
    ///
    /// It returns the torrent peer (weak reference) when found, else [None].
    pub async fn peer(&self, handle: &PeerHandle) -> Option<TorrentPeer> {
        self.inner
            .sender
            .send(|tx| TorrentCommand::GetPeer {
                handle: *handle,
                response: tx,
            })
            .await
            .await
            .ok()
            .flatten()
    }

    /// Resume the downloading of the torrent data.
    pub async fn resume(&self) {
        let _ = self
            .inner
            .sender
            .send(|tx| TorrentCommand::Resume { response: tx })
            .await
            .await;
    }

    /// Pause the current torrent.
    pub async fn pause(&self) {
        let _ = self
            .inner
            .sender
            .send(|tx| TorrentCommand::Pause { response: tx })
            .await
            .await;
    }

    /// Returns the torrent data for the given piece, if available.
    /// This doesn't verify if the bytes are valid and completed.
    pub async fn read_piece(&self, piece: &PieceIndex) -> Result<Vec<u8>> {
        self.inner.read_piece(piece).await
    }

    /// Returns the torrent data for the given piece byte range, if available.
    /// This doesn't verify if the bytes are valid and completed.
    pub async fn read_piece_bytes(
        &self,
        piece: &PieceIndex,
        range: std::ops::Range<usize>,
    ) -> Result<Vec<u8>> {
        self.inner.read_piece_bytes(piece, range).await
    }

    /// Try to read the bytes from the given torrent file.
    /// This reads all available bytes of the file stored within the [Storage].
    ///
    /// Returns the amount of bytes read and the byte buffer.
    ///
    /// ## Remarks
    ///
    /// This doesn't verify if the bytes are valid and completed.
    pub async fn read_file_to_end(&self, file: &FileIndex) -> Result<(usize, Vec<u8>)> {
        self.inner
            .sender
            .send(|tx| TorrentCommand::ReadFileToEnd {
                file: *file,
                response: tx,
            })
            .await
            .await
    }

    /// Wait for the given number of active trackers.
    async fn wait_for_trackers(&self, num_of_trackers: usize) {
        let notifier = Arc::new(Notify::new());
        let mut receiver = self.subscribe();
        let cancellation_token = CancellationToken::new();

        let inner_cancel = cancellation_token.clone();
        let inner_notifier = notifier.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = inner_cancel.cancelled() => break,
                    Some(event) = receiver.recv() => {
                        if let TorrentEvent::TrackersChanged = *event {
                            inner_notifier.notify_one();
                        }
                    }
                }
            }
        });

        loop {
            notifier.notified().await;
            if self.active_tracker_connections().await >= num_of_trackers {
                break;
            }
        }

        cancellation_token.cancel();
    }
}

impl Callback<TorrentEvent> for Torrent {
    fn subscribe(&self) -> Subscription<TorrentEvent> {
        self.inner.subscribe()
    }

    fn subscribe_with(&self, subscriber: Subscriber<TorrentEvent>) {
        self.inner.subscribe_with(subscriber);
    }
}

impl PartialEq for Torrent {
    fn eq(&self, other: &Self) -> bool {
        self.inner.handle == other.inner.handle && self.peer_id == other.peer_id
    }
}

impl Drop for Torrent {
    fn drop(&mut self) {
        if Arc::strong_count(&self.instance_counter) == 1 {
            self.cancellation_token.cancel();
        }
    }
}

#[derive(Debug, Display, Clone)]
#[display("{}", handle)]
pub struct InnerTorrent {
    handle: TorrentHandle,
    sender: ChannelSender<TorrentCommand>,
    callbacks: MultiThreadedCallback<TorrentEvent>,
}

impl InnerTorrent {
    pub fn new(
        handle: TorrentHandle,
        sender: ChannelSender<TorrentCommand>,
        callbacks: MultiThreadedCallback<TorrentEvent>,
    ) -> Self {
        Self {
            handle,
            sender,
            callbacks,
        }
    }

    /// Returns the unique handle of the torrent.
    pub fn handle(&self) -> TorrentHandle {
        self.handle
    }

    /// Returns `true` if the torrent is still valid, else `false` if it has been closed.
    pub fn is_valid(&self) -> bool {
        !self.sender.is_closed()
    }

    /// Returns the info hash of the torrent.
    pub async fn info_hash(&self) -> Result<InfoHash> {
        Ok(self
            .sender
            .send(|tx| TorrentCommand::InfoHash { response: tx })
            .await
            .await?)
    }

    /// Returns the state of the torrent.
    pub async fn state(&self) -> TorrentState {
        self.sender
            .send(|tx| TorrentCommand::State { response: tx })
            .await
            .await
            .unwrap_or(TorrentState::Error)
    }

    /// Returns the port on which the torrent is listening for incoming connections.
    pub async fn peer_port(&self) -> Option<u16> {
        self.sender
            .send(|tx| TorrentCommand::PeerPort { response: tx })
            .await
            .await
            .ok()
            .flatten()
    }

    /// Add the given peer address to the torrent pool for establishing a connection.
    pub async fn add_peer(&self, addr: SocketAddr) -> Result<()> {
        Ok(self
            .sender
            .send(|tx| TorrentCommand::AddPeer { addr, response: tx })
            .await
            .await?)
    }

    /// Add the given peer addresses to the torrent pool for establishing connections.
    pub async fn add_peers(&self, addrs: Vec<SocketAddr>) {
        self.sender
            .fire_and_forget(TorrentCommand::AddPeers { addrs })
            .await;
    }

    /// Decrease the priority of the given peer addresses in the torrent pool.
    pub async fn decrease_peer_priority(&self, addrs: Vec<SocketAddr>) {
        self.sender
            .fire_and_forget(TorrentCommand::DecreasePeerPriority { addrs })
            .await;
    }

    /// Returns the enabled protocol extensions of the torrent.
    /// It can return [Err] when the torrent has been closed (see [Torrent::is_valid]).
    pub async fn protocol_extensions(&self) -> Result<ProtocolExtensionFlags> {
        Ok(self
            .sender
            .send(|tx| TorrentCommand::ProtocolExtensions { response: tx })
            .await
            .await?)
    }

    /// Returns the metadata of the torrent, or [TorrentError::InvalidHandle] when the torrent is invalid.
    pub async fn metadata(&self) -> Result<TorrentMetadata> {
        Ok(self
            .sender
            .send(|tx| TorrentCommand::Metadata { response: tx })
            .await
            .await?)
    }

    /// Update the metadata of the torrent.
    pub async fn set_metadata(&self, metadata: TorrentMetadataInfo) {
        let _ = self
            .sender
            .send(|tx| TorrentCommand::UpdateMetadata {
                metadata,
                response: tx,
            })
            .await
            .await;
    }

    /// Returns the completed pieces bitfield of the torrent.
    pub async fn bitfield(&self) -> Result<BitVec> {
        Ok(self
            .sender
            .send(|tx| TorrentCommand::Bitfield { response: tx })
            .await
            .await?)
    }

    /// Returns `true` when the metadata of the torrent is known, else `false` when it's still being retrieved.
    pub async fn is_metadata_known(&self) -> bool {
        self.sender
            .send(|tx| TorrentCommand::IsMetadataKnown { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the active options of the torrent.
    pub async fn options(&self) -> Result<TorrentFlags> {
        Ok(self
            .sender
            .send(|tx| TorrentCommand::GetOptions { response: tx })
            .await
            .await?)
    }

    /// Returns the configuration used by the torrent.
    /// This is a snapshot and might be modified in the future.
    pub async fn config(&self) -> Result<TorrentConfig> {
        Ok(self
            .sender
            .send(|tx| TorrentCommand::GetConfig { response: tx })
            .await
            .await?)
    }

    /// Returns the information about a specific piece within the torrent.
    /// If the pieces are not yet known, in case the metadata is still being retrieved, then it returns [None].
    ///
    /// If a piece index is requested out-of-bounds of the pieces, [None] will also be returned.
    pub async fn piece(&self, piece: &PieceIndex) -> Option<Piece> {
        self.sender
            .send(|tx| TorrentCommand::GetPiece {
                piece: *piece,
                response: tx,
            })
            .await
            .await
            .ok()
            .flatten()
    }

    /// Return the total amount of pieces for this torrent.
    /// If the metadata is still being retrieved, the total pieces cannot yet be known and this will result in 0.
    pub async fn total_pieces(&self) -> usize {
        self.sender
            .send(|tx| TorrentCommand::NumOfPieces { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the wanted pieces of the torrent.
    pub async fn wanted_pieces(&self) -> Result<Vec<Piece>> {
        Ok(self
            .sender
            .send(|tx| TorrentCommand::WantedPieces { response: tx })
            .await
            .await?)
    }

    /// Returns the wanted pieces which are not being requested at the moment by the torrent.
    pub async fn wanted_request_pieces(&self) -> Result<Vec<Piece>> {
        Ok(self
            .sender
            .send(|tx| TorrentCommand::WantedRequestPieces { response: tx })
            .await
            .await?)
    }

    /// Update the availability of the given pieces for the torrent.
    pub async fn piece_availabilities(&self, pieces: Vec<PieceIndex>, available: bool) {
        self.sender
            .fire_and_forget(TorrentCommand::PieceAvailabilities { pieces, available })
            .await
    }

    /// Returns the files of the torrent, if the metadata is known (see [Torrent::is_metadata_known]).
    /// If the metadata is still being retrieved, the returned files array will be empty.
    pub async fn files(&self) -> Vec<File> {
        self.sender
            .send(|tx| TorrentCommand::GetFiles { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the torrent file for the given index, if available.
    pub async fn file(&self, file: &FileIndex) -> Option<File> {
        self.sender
            .send(|tx| TorrentCommand::GetFile {
                file: *file,
                response: tx,
            })
            .await
            .await
            .ok()
            .flatten()
    }

    /// Returns the [FileIndex] for the starting byte of the given piece.
    pub async fn file_index_for(&self, piece: &PieceIndex) -> Option<FileIndex> {
        self.sender
            .send(|tx| TorrentCommand::GetFileIndexFor {
                piece: *piece,
                response: tx,
            })
            .await
            .await
            .ok()
            .flatten()
    }

    /// Returns the extensions of the torrent.
    pub async fn extensions(&self) -> Result<Extensions> {
        Ok(self
            .sender
            .send(|tx| TorrentCommand::Extensions { response: tx })
            .await
            .await?)
    }

    /// Mark the given piece as completed.
    pub async fn piece_completed(&self, piece: &PieceIndex) -> Result<()> {
        Ok(self
            .sender
            .send(|tx| TorrentCommand::PieceCompleted {
                piece: *piece,
                response: tx,
            })
            .await
            .await?)
    }

    /// Notifies the torrent that a piece part has been completed.
    pub async fn piece_part_completed<T: Into<Vec<u8>>>(&self, part: &PiecePart, data: T) {
        self.sender
            .fire_and_forget(TorrentCommand::PiecePartCompleted {
                part: part.clone(),
                data: data.into(),
            })
            .await;
    }

    /// Inform the torrent that a pending data request has been rejected by a peer.
    pub async fn pending_request_rejected(
        &self,
        piece: &PieceIndex,
        piece_offset: usize,
        peer: &PeerClientInfo,
    ) {
        self.sender
            .fire_and_forget(TorrentCommand::PendingRequestRejected {
                piece: *piece,
                begin: piece_offset,
                peer: peer.clone(),
            })
            .await;
    }

    /// Returns a download permit for the given piece, if available, else [None].
    pub async fn request_download_permit(
        &self,
        piece: &PieceIndex,
    ) -> Option<OwnedSemaphorePermit> {
        self.sender
            .send(|tx| TorrentCommand::RequestDownloadPermit {
                piece: *piece,
                response: tx,
            })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the torrent data for the given piece, if available.
    /// This doesn't verify if the bytes are valid and completed.
    pub async fn read_piece(&self, piece: &PieceIndex) -> Result<Vec<u8>> {
        self.sender
            .send(|tx| TorrentCommand::ReadPiece {
                piece: *piece,
                response: tx,
            })
            .await
            .await
    }

    /// Returns the torrent data for the given piece byte range, if available.
    /// This doesn't verify if the bytes are valid and completed.
    pub async fn read_piece_bytes(
        &self,
        piece: &PieceIndex,
        range: std::ops::Range<usize>,
    ) -> Result<Vec<u8>> {
        self.sender
            .send(|tx| TorrentCommand::ReadPieceBytes {
                piece: *piece,
                bytes: range,
                response: tx,
            })
            .await
            .await
    }

    /// Calculate the hash for the given piece stored in the storage.
    pub async fn hash_v1(&self, piece: &PieceIndex) -> Result<Sha1Hash> {
        self.sender
            .send(|tx| TorrentCommand::HashV1Data {
                piece: *piece,
                response: tx,
            })
            .await
            .await
    }

    /// Calculate the hash for the given piece stored in the storage.
    pub async fn hash_v2(&self, piece: &PieceIndex) -> Result<Sha256Hash> {
        self.sender
            .send(|tx| TorrentCommand::HashV2Data {
                piece: *piece,
                response: tx,
            })
            .await
            .await
    }

    /// Returns `true` if download data for the torrent is allowed, else `false`.
    pub async fn is_download_allowed(&self) -> bool {
        self.sender
            .send(|tx| TorrentCommand::IsDownloadAllowed { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns `true` if uploading data for the torrent is allowed, else `false`.
    pub async fn is_upload_allowed(&self) -> bool {
        self.sender
            .send(|tx| TorrentCommand::IsUploadAllowed { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns `true` if the torrent is a partial seed, else `false`.
    ///
    /// Partial seed is when the torrent has some files completed but not all wanted.
    /// In this case, the torrent has completed its download process, and will never reach the full seed status.
    pub async fn is_partial_seed(&self) -> bool {
        self.sender
            .send(|tx| TorrentCommand::IsPartialSeed { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Notify the torrent that a peer's connection is closed.
    pub(crate) async fn peer_closed(&self, peer: PeerIdentifier, reason: CloseReason) {
        self.sender
            .fire_and_forget(TorrentCommand::PeerClosed { peer, reason })
            .await;
    }

    /// Returns the data pool of the torrent.
    #[cfg(test)]
    pub async fn data_pool(&self) -> Result<DataPool> {
        Ok(self
            .sender
            .send(|tx| TorrentCommand::DataPool { response: tx })
            .await
            .await?)
    }
}

impl Callback<TorrentEvent> for InnerTorrent {
    fn subscribe(&self) -> Subscription<TorrentEvent> {
        self.callbacks.subscribe()
    }

    fn subscribe_with(&self, subscriber: Subscriber<TorrentEvent>) {
        self.callbacks.subscribe_with(subscriber);
    }
}

/// The reason why a pending request was rejected
#[derive(Debug, Clone, PartialEq)]
pub enum RequestRejectedReason {
    /// Indicates that the received piece data was invalid
    InvalidDataResponse,
    /// Indicates that the remote peer rejected the request
    RejectedByRemotePeer,
}

/// The command sender to interact with the [TorrentContext] main task loop.
#[derive(Debug)]
pub enum TorrentCommand {
    PeerPort {
        response: Reply<Option<u16>>,
    },
    AddPeer {
        addr: SocketAddr,
        response: Reply<()>,
    },
    AddPeers {
        addrs: Vec<SocketAddr>,
    },
    GetPeer {
        handle: PeerHandle,
        response: Reply<Option<TorrentPeer>>,
    },
    PeerConnected {
        peer: Box<dyn Peer>,
    },
    DecreasePeerPriority {
        addrs: Vec<SocketAddr>,
    },
    PeerClosed {
        peer: PeerIdentifier,
        reason: CloseReason,
    },
    State {
        response: Reply<TorrentState>,
    },
    GetConfig {
        response: Reply<TorrentConfig>,
    },
    Metadata {
        response: Reply<TorrentMetadata>,
    },
    UpdateMetadata {
        metadata: TorrentMetadataInfo,
        response: Reply<()>,
    },
    IsMetadataKnown {
        response: Reply<bool>,
    },
    InfoHash {
        response: Reply<InfoHash>,
    },
    GetOptions {
        response: Reply<TorrentFlags>,
    },
    AddOptions {
        options: TorrentFlags,
        response: Reply<()>,
    },
    RemoveOptions {
        options: TorrentFlags,
        response: Reply<()>,
    },
    NumOfPieces {
        response: Reply<usize>,
    },
    NumOfFiles {
        response: Reply<usize>,
    },
    NumOfCompletedPieces {
        response: Reply<usize>,
    },
    NumOfActivePeerConnections {
        response: Reply<usize>,
    },
    NumOfActiveTrackerConnections {
        response: Reply<usize>,
    },
    GetPath {
        response: Reply<Option<PathBuf>>,
    },
    GetFilePath {
        file: File,
        response: Reply<PathBuf>,
    },
    GetPieces {
        response: Reply<Vec<Piece>>,
    },
    GetPiece {
        piece: PieceIndex,
        response: Reply<Option<Piece>>,
    },
    /// Returns all wanted pieces of the torrent which have not yet been completed.
    WantedPieces {
        response: Reply<Vec<Piece>>,
    },
    /// Returns all wanted pieces which are currently not being requested by a [Peer].
    WantedRequestPieces {
        response: Reply<Vec<Piece>>,
    },
    /// Update the availability of the given pieces for the torrent.
    PieceAvailabilities {
        pieces: Vec<PieceIndex>,
        available: bool,
    },
    GetFiles {
        response: Reply<Vec<File>>,
    },
    GetFile {
        file: FileIndex,
        response: Reply<Option<File>>,
    },
    /// Returns the [FileIndex] containing the first byte of the given [PieceIndex].
    GetFileIndexFor {
        piece: PieceIndex,
        response: Reply<Option<FileIndex>>,
    },
    HasPiece {
        piece: PieceIndex,
        response: Reply<bool>,
    },
    HasBytes {
        bytes: std::ops::Range<usize>,
        response: Reply<bool>,
    },
    GetPiecePriorities {
        response: Reply<BTreeMap<PieceIndex, PiecePriority>>,
    },
    PrioritizePieces {
        priorities: Vec<(PieceIndex, PiecePriority)>,
        response: Reply<()>,
    },
    PrioritizeFiles {
        priorities: Vec<(FileIndex, PiecePriority)>,
        response: Reply<()>,
    },
    PrioritizeBytes {
        bytes: std::ops::Range<usize>,
        priority: PiecePriority,
        response: Reply<()>,
    },
    IsCompleted {
        response: Reply<bool>,
    },
    IsPaused {
        response: Reply<bool>,
    },
    Pause {
        response: Reply<()>,
    },
    Resume {
        response: Reply<()>,
    },
    AnnounceAll {
        response: Reply<AnnouncementResult>,
    },
    ScrapeAll {
        response: Reply<Result<ScrapeMetrics>>,
    },
    ReadPiece {
        piece: PieceIndex,
        response: Reply<Result<Vec<u8>>>,
    },
    ReadPieceBytes {
        piece: PieceIndex,
        bytes: std::ops::Range<usize>,
        response: Reply<Result<Vec<u8>>>,
    },
    ReadFileToEnd {
        file: FileIndex,
        response: Reply<Result<(usize, Vec<u8>)>>,
    },
    HashV1Data {
        piece: PieceIndex,
        response: Reply<Result<Sha1Hash>>,
    },
    HashV2Data {
        piece: PieceIndex,
        response: Reply<Result<Sha256Hash>>,
    },
    ProtocolExtensions {
        response: Reply<ProtocolExtensionFlags>,
    },
    Extensions {
        response: Reply<Extensions>,
    },
    Bitfield {
        response: Reply<BitVec>,
    },
    PendingRequestRejected {
        piece: PieceIndex,
        begin: usize,
        peer: PeerClientInfo,
    },
    RequestDownloadPermit {
        piece: PieceIndex,
        response: Reply<Option<OwnedSemaphorePermit>>,
    },
    RequestUploadPermit {
        response: Reply<Option<OwnedSemaphorePermit>>,
    },
    PieceCompleted {
        piece: PieceIndex,
        response: Reply<()>,
    },
    PiecePartCompleted {
        part: PiecePart,
        data: Vec<u8>,
    },
    IsDownloadAllowed {
        response: Reply<bool>,
    },
    IsUploadAllowed {
        response: Reply<bool>,
    },
    IsPartialSeed {
        response: Reply<bool>,
    },
    #[cfg(test)]
    DataPool {
        response: Reply<DataPool>,
    },
}

/// The torrent context data.
/// This context can be shared by multiple [Torrent] instances, but only one [Torrent] instance can own the context.
#[derive(Debug)]
pub struct TorrentContext {
    /// The unique immutable handle of the torrent
    handle: TorrentHandle,
    /// The unique immutable peer id of the torrent
    peer_id: PeerId,
    /// The peer address port on which the torrent is listening for incoming peer connections
    peer_port: Option<u16>,
    /// The torrent metadata information of the torrent
    /// This might still be incomplete if the torrent was created from a magnet link
    metadata: TorrentMetadata,
    /// The manager of the trackers for the torrent
    tracker_manager: TrackerClient,
    /// The dht server of the torrent
    dht: DhtOption,

    /// The pool of peer connections
    peer_pool: PeerPool,

    /// The pieces of the torrent, these are only known if the metadata is available
    data_pool: DataPool,
    /// The pool which stores the received piece parts
    piece_chunk_pool: PieceChunkPool,
    /// The in-flight pending requests of pieces by peers
    pending_piece_requests: HashMap<PieceIndex, Instant>,

    /// The permit counter for requesting pieces from remote peers
    request_download_permits: Arc<Semaphore>,
    /// The permit counter for uploading pieces to remote peers
    request_upload_permits: Arc<Semaphore>,

    /// The storage interface of the torrent
    storage: Arc<dyn Storage>,

    /// The immutable enabled protocol extensions for this torrent
    protocol_extensions: ProtocolExtensionFlags,
    /// The immutable peer extension factories for this torrent.
    /// These factories create the extensions for each established peer connection.
    extensions: ExtensionFactories,

    /// The state of the torrent
    state: TorrentState,
    /// The torrent options that are set for this torrent
    options: TorrentFlags,
    /// The torrent configuration
    config: TorrentConfig,
    /// The metrics of the torrent
    metrics: Metrics,
    /// The main task loop command sender of the torrent
    command_sender: ChannelSender<TorrentCommand>,
    /// The callbacks for the torrent events
    callbacks: MultiThreadedCallback<TorrentEvent>,
    /// The main loop cancellation token
    cancellation_token: CancellationToken,
}

impl TorrentContext {
    pub(crate) fn new(
        metadata: TorrentMetadata,
        config: TorrentConfig,
        peer_port: Option<u16>,
        protocol_extensions: ProtocolExtensionFlags,
        extensions: ExtensionFactories,
        options: TorrentFlags,
        data_pool: DataPool,
        dht: DhtOption,
        tracker_manager: TrackerClient,
        storage: Arc<dyn Storage>,
        command_sender: ChannelSender<TorrentCommand>,
    ) -> Self {
        let handle = TorrentHandle::new();

        Self {
            handle,
            metadata,
            peer_id: PeerId::new(),
            peer_port,
            tracker_manager,
            dht,
            peer_pool: PeerPool::new(handle, config.peers_upper_limit),
            data_pool,
            piece_chunk_pool: PieceChunkPool::new(),
            pending_piece_requests: Default::default(),
            request_download_permits: Arc::new(Semaphore::new(config.max_in_flight_pieces)),
            request_upload_permits: Arc::new(Semaphore::new(config.peers_upload_slots)),
            protocol_extensions,
            extensions,
            storage,
            state: Default::default(),
            options,
            config,
            metrics: Metrics::new(),
            command_sender,
            callbacks: MultiThreadedCallback::new(),
            cancellation_token: CancellationToken::new(),
        }
    }

    /// Run the main task loop of the torrent context.
    /// This process is automatically terminated when the `command_receiver` has no more active sender channels.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    pub(crate) async fn run(
        &mut self,
        mut operations: Vec<Box<dyn TorrentOperation>>,
        peer_discoveries: Vec<Arc<dyn PeerDiscovery>>,
        mut command_receiver: ChannelReceiver<TorrentCommand>,
    ) {
        let mut tracker_event_receiver = self.tracker_manager.subscribe();
        let mut operations_tick = time::interval(OPERATIONS_INTERVAL);
        let mut cleanup_interval = time::interval(Duration::from_secs(30));

        // register the torrent within the tracker
        if !self.add_torrent_to_tracker().await {
            return;
        }

        let mut peer_connections: FuturesUnordered<BoxFuture<'_, (usize, Option<PeerEntry>)>> =
            FuturesUnordered::new();
        for (idx, discovery) in peer_discoveries.iter().enumerate() {
            peer_connections.push(Box::pin(async move {
                let entry = discovery.recv().await;
                (idx, entry)
            }));
        }

        loop {
            select! {
                _ = self.cancellation_token.cancelled() => break,
                command = command_receiver.recv() => match command {
                    Some(command) => self.on_command(command).await,
                    None => break,
                },
                Some(event) = tracker_event_receiver.recv() => self.handle_tracker_event((*event).clone()).await,
                Some((idx, entry)) = peer_connections.next() => {
                    if let Some(entry) = entry {
                        self.handle_incoming_peer_connection(entry).await;
                        let discovery = &peer_discoveries[idx];
                        peer_connections.push(Box::pin(async move {
                            let entry = discovery.recv().await;
                            (idx, entry)
                        }));
                    }
                },
                _ = operations_tick.tick() => {
                    self.execute_operations_chain(&mut operations, peer_discoveries.as_slice()).await;
                },
                _ = cleanup_interval.tick() => {
                    self.clean_peers().await;
                },
            }
        }

        // shutdown the peer pool
        self.peer_pool.shutdown().await;
        // inform the tracker the torrent is being stopped
        self.tracker_manager
            .announce_all(&self.metadata.info_hash, AnnounceEvent::Stopped)
            .await;
        self.tracker_manager
            .remove_torrent(&self.metadata.info_hash);
        self.data_pool.close().await;
        self.cancellation_token.cancel();
        self.update_state(TorrentState::Stopped);
        trace!("Torrent {} main loop ended", self);
    }

    /// Get the unique handle of the torrent.
    /// It returns an owned instance of the torrent handle.
    pub fn handle(&self) -> TorrentHandle {
        self.handle
    }

    /// Get the address on which the torrent is listening for new incoming connections.
    pub fn addr(&self) -> Option<SocketAddr> {
        self.peer_port
            .as_ref()
            .map(|port| SocketAddr::from((Ipv4Addr::UNSPECIFIED, *port)))
    }

    /// Returns a reference to the peer pool of the torrent.
    pub fn peer_pool(&self) -> &PeerPool {
        &self.peer_pool
    }

    /// Returns a mutable reference to the peer pool of the torrent.
    pub fn peer_pool_mut(&mut self) -> &mut PeerPool {
        &mut self.peer_pool
    }

    /// Get the peer id of the torrent.
    /// This is the unique peer ID that is used within the communication with remote peers for this torrent.
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// Returns the port number on which the torrent is listening.
    /// Or [None] if the torrent is not listening for incoming peer connections.
    pub fn peer_port(&self) -> Option<&u16> {
        self.peer_port.as_ref()
    }

    /// Returns `true` if the torrent is canceled.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }

    /// Returns a Future that gets fulfilled when the torrent is being cancelled/stopped.
    /// The future will complete immediately if the torrenbt is already cancelled when this method is called.
    pub fn cancelled(&self) -> WaitForCancellationFuture<'_> {
        self.cancellation_token.cancelled()
    }

    /// Returns a Future that gets fulfilled when the torrent is being cancelled/stopped.
    /// The future will complete immediately if the torrenbt is already cancelled when this method is called.
    pub fn cancelled_owned(&self) -> WaitForCancellationFutureOwned {
        self.cancellation_token.clone().cancelled_owned()
    }

    /// Get the enabled protocol extensions for the torrent.
    pub fn protocol_extensions(&self) -> ProtocolExtensionFlags {
        self.protocol_extensions
    }

    /// Returns the active peer extensions of the torrent.
    /// These extensions should be activated for each established peer connection of the torrent.
    pub fn extensions(&self) -> Vec<Box<dyn Extension>> {
        self.extensions.iter().map(|e| e()).collect()
    }

    /// Get the tracker manager for the torrent.
    pub fn tracker_manager(&self) -> &TrackerClient {
        &self.tracker_manager
    }

    /// Returns the absolute path to the torrent location, if the metadata is known.
    /// This can either be a file or directory to the torrent depending on the type of the torrent.
    pub fn path(&self) -> Option<PathBuf> {
        match &self.metadata.info {
            None => None,
            Some(info) => Some(self.config.path().join(info.name())),
        }
    }

    /// Get the state of the torrent.
    pub fn state(&self) -> &TorrentState {
        &self.state
    }

    /// Get the known torrent transfer stats.
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Get the options of the torrent.
    pub fn options(&self) -> &TorrentFlags {
        &self.options
    }

    /// Get an owned instance of the options of the torrent.
    pub fn options_owned(&self) -> TorrentFlags {
        self.options.clone()
    }

    /// Get the configuration of the torrent.
    pub fn config(&self) -> &TorrentConfig {
        &self.config
    }

    /// Get the currently active trackers of the torrent.
    pub async fn active_trackers(&self) -> Vec<Url> {
        self.tracker_manager.tracker_urls().await
    }

    /// Get an owned instance of the metadata from the torrent.
    /// It returns an owned instance of the metadata.
    pub fn metadata(&self) -> &TorrentMetadata {
        &self.metadata
    }

    /// Check if the metadata of the torrent is known.
    /// It returns false when the torrent is still retrieving the metadata, else true.
    pub fn is_metadata_known(&self) -> bool {
        self.metadata.info.is_some()
    }

    /// Get the total amount of actively connected peers.
    /// This only counts peers that have not been closed yet, so it can be smaller than the peer pool.
    pub async fn active_peer_connections(&self) -> usize {
        self.peer_pool.active_peer_connections().await
    }

    /// Get the total amount of active tracker connections.
    /// This only counts trackers which have at least made one successful announcement.
    pub async fn active_tracker_connections(&self) -> usize {
        self.tracker_manager.trackers_len().await
    }

    /// Get the DHT tracker of the torrent.
    pub fn dht(&self) -> &DhtOption {
        &self.dht
    }

    /// Returns a reference to the data pool of the torrent.
    pub fn data_pool(&self) -> &DataPool {
        &self.data_pool
    }

    /// Returns a reference to the underlying storage layer of the torrent.
    pub fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    /// Returns a reference to the command sender of the torrent.
    /// This allowed for delegating tasks to the main task of the torrent context.
    pub fn command_sender(&self) -> &ChannelSender<TorrentCommand> {
        &self.command_sender
    }

    /// Returns a reference to the event callbacks of the torrent.
    pub fn callbacks(&self) -> &MultiThreadedCallback<TorrentEvent> {
        &self.callbacks
    }

    /// Returns all wanted pieces which are currently not being requested by a [Peer].
    /// Pieces with the highest priority will be first.
    ///
    /// It returns all piece indexes for which the priority is not [PiecePriority::None], the piece has not been completed and
    /// no peer is requesting the data.
    ///
    /// ## Sorting
    ///
    /// The pieces are **sorted** by their priorities, meaning that pieces with [PiecePriority::High] will come before [PiecePriority::Normal].
    pub async fn wanted_request_pieces(&self) -> Vec<Piece> {
        let is_end_game = self.data_pool.is_end_game().await;
        self.data_pool
            .wanted_pieces()
            .await
            .into_iter()
            // don't allow duplicate piece requests which have not timed out
            // the exclusion on this is only during the end-game phase of the torrent
            .filter(|piece| {
                let should_request_piece = self
                    .pending_piece_requests
                    .get(&piece.index)
                    .filter(|e| e.elapsed() <= PEER_REQUEST_TIMEOUT)
                    .is_none();

                is_end_game || should_request_piece
            })
            .collect()
    }

    /// Get the total amount of wanted pieces by the torrent.
    pub async fn total_wanted_pieces(&self) -> usize {
        self.data_pool.wanted_pieces().await.len()
    }

    /// Returns `true` if all bytes are completed, validated, and written to the storage, else `false`.
    pub async fn has_bytes(&self, range: &std::ops::Range<usize>) -> bool {
        self.data_pool.has_bytes(range.clone()).await
    }

    /// Prioritize the given pieces within this torrent.
    pub async fn prioritize_pieces(&mut self, priorities: Vec<(PieceIndex, PiecePriority)>) {
        trace!("Torrent {} is prioritizing pieces {:?}", self, priorities);
        self.data_pool
            .set_piece_priorities(priorities.as_slice())
            .await;
        self.update_interested_pieces_stats().await;

        debug!("Torrent {} piece priorities have been changed", self);
        self.invoke_event(TorrentEvent::PiecePrioritiesChanged);

        // update the state of the torrent based on the new priorities
        // this can only be done after the init phase to not disrupt the init operations
        let is_not_init_state = !self.state.is_initializing_phase();
        if is_not_init_state {
            let new_state = self.determine_state().await;
            self.update_state(new_state);
        }
    }

    /// Prioritize the given bytes within the torrent.
    /// This will match the bytes against the relevant pieces, and prioritize those pieces.
    pub async fn prioritize_bytes(
        &mut self,
        bytes: &std::ops::Range<usize>,
        priority: PiecePriority,
    ) {
        let piece_priorities = self
            .find_relevant_pieces_for_bytes(bytes)
            .await
            .into_iter()
            .map(|piece| (piece.index, priority))
            .collect();
        self.prioritize_pieces(piece_priorities).await;
    }

    /// Check if the torrent has completed downloading all wanted pieces.
    pub async fn is_completed(&self) -> bool {
        self.data_pool.is_completed().await
    }

    /// Check if downloading piece data is allowed by the torrent.
    pub fn is_download_allowed(&self) -> bool {
        let is_download_mode = self.options.contains(TorrentFlags::DownloadMode);
        let is_not_paused = !self.options.contains(TorrentFlags::Paused);
        let is_not_init_state = !self.state.is_initializing_phase();

        is_download_mode && is_not_paused && is_not_init_state
    }

    /// Check if uploading piece data is allowed by the torrent.
    pub fn is_upload_allowed(&self) -> bool {
        let is_not_paused = !self.options.contains(TorrentFlags::Paused);
        let is_uploading_mode = self.options.contains(TorrentFlags::UploadMode)
            || self.options.contains(TorrentFlags::SeedMode);

        is_uploading_mode && is_not_paused
    }

    /// Check if the torrent is a partial seed.
    /// A partial seed is a torrent that is seeding only a selection of a multi file torrent.
    pub async fn is_partial_seed(&self) -> bool {
        // check if this a multi file torrent
        if self.total_files().await <= 1 {
            return false;
        }

        // check if all wanted pieces have been downloaded
        self.total_wanted_pieces().await == 0
    }

    /// Check if the torrent is currently paused.
    pub fn is_paused(&self) -> bool {
        self.options.contains(TorrentFlags::Paused)
    }

    /// Determines the number of additional peer connections needed for the torrent.
    ///
    /// This function calculates how many more peer connections are required based on the
    /// current torrent state, configuration limits, and active connections. It ensures
    /// the number of connections stays within defined thresholds.
    ///
    /// # Returns
    ///
    /// It returns a number of additionally wanted connection, ensuring the total
    /// stays within the configured peer connection limits.
    pub async fn remaining_peer_connections_needed(&self) -> usize {
        // if the torrent is trying to retrieve the metadata,
        // then allow at least the lower limit during paused state
        if self.options.contains(TorrentFlags::Paused) {
            return if self.state == TorrentState::RetrievingMetadata {
                self.config.peers_lower_limit
            } else {
                0
            };
        }

        // if the torrent is validating files, then don't open any new peer connections during the process
        // if the torrent is finished, then don't actively reach out to new peers
        if matches!(
            self.state,
            TorrentState::CheckingFiles | TorrentState::Finished | TorrentState::Seeding
        ) {
            return 0;
        }

        let currently_active_peers = self.active_peer_connections().await;

        let is_retrieving_data = self.options.contains(TorrentFlags::DownloadMode);
        let is_retrieving_metadata = self.options.contains(TorrentFlags::Metadata)
            && self.state == TorrentState::RetrievingMetadata;

        let peer_lower_bound = self.config.peers_lower_limit;
        let peer_upper_bound = self.config.peers_upper_limit;

        // if we're downloading or retrieving metadata, aim for the upper bound
        if is_retrieving_metadata || is_retrieving_data {
            return peer_upper_bound.saturating_sub(currently_active_peers);
        }

        // if we're not actively requesting any data, aim for the lower bound
        peer_lower_bound.saturating_sub(currently_active_peers)
    }

    /// Get all relevant pieces for the given torrent byte range.
    ///
    /// # Arguments
    ///
    /// * `torrent_bytes` - The torrent byte range to retrieve the relevant pieces of.
    ///
    /// # Returns
    ///
    /// It returns all pieces with at least 1 byte overlapping with the given range.
    pub async fn find_relevant_pieces_for_bytes(
        &self,
        torrent_bytes: &std::ops::Range<usize>,
    ) -> Vec<Piece> {
        self.data_pool
            .pieces()
            .await
            .into_iter()
            .filter(|e| e.contains(torrent_bytes))
            .collect()
    }

    /// Try to find the [PiecePart] for the given piece and begin index.
    pub async fn find_piece_part(&self, piece: PieceIndex, offset: usize) -> Option<PiecePart> {
        self.data_pool.find_piece_part(&piece, offset).await
    }

    /// Get the pieces for the given file.
    /// This will retrieve all overlapping pieces with the file.
    /// The last piece can be longer than the actual file if the piece overlaps with multiple files.
    ///
    /// # Returns
    ///
    /// Returns the cloned pieces for the given file.
    pub async fn file_pieces(&self, file: &File) -> Vec<Piece> {
        self.data_pool
            .pieces()
            .await
            .into_iter()
            .filter(|piece| file.contains(&piece.torrent_range()))
            .collect()
    }

    /// Get the list of non-padding files contained in the torrent.
    ///
    /// This method filters out any files marked with the [`FileAttributeFlags::PaddingFile`] attribute,
    /// so padding files will **not** be included in the returned list.
    ///
    /// ## Remarks
    ///
    /// If the torrent's metadata has not yet been fully retrieved, this method will return an empty vector.
    pub async fn files(&self) -> Vec<File> {
        self.data_pool
            .files()
            .await
            .into_iter()
            // filter out any padding files
            .filter_map(|file| {
                if !file.attributes().contains(FileAttributeFlags::PaddingFile) {
                    Some(file)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get the number of non-padding files currently known in the torrent.
    ///
    /// Files marked with the [`FileAttributeFlags::PaddingFile`] attribute are excluded from the count.
    ///
    /// ## Remarks
    ///
    /// If the torrent's metadata has not yet been fully retrieved, this method will return `0`.
    pub async fn total_files(&self) -> usize {
        self.data_pool.num_of_files().await
    }

    /// Prioritize the files of the torrent.
    /// This will update the underlying piece priorities of each file.
    ///
    /// Providing all file indexes of the torrent is not required.
    pub async fn prioritize_files(&self, priorities: Vec<(FileIndex, PiecePriority)>) {
        trace!("Torrent {} is prioritizing files {:?}", self, priorities);
        self.data_pool
            .set_file_priorities(priorities.as_slice())
            .await;
    }

    /// Get the absolute filesystem path to a given file in the torrent.
    ///
    /// This combines the torrent's storage path with the file's [`torrent_path`]
    /// to produce a full path on the local filesystem.
    pub fn absolute_file_path(&self, file: &File) -> PathBuf {
        self.config.path().join(file.torrent_path.as_path())
    }

    /// Returns sum of bytes of all files within the torrent.
    pub fn len(&self) -> Option<usize> {
        self.metadata.info.as_ref().map(|e| e.len())
    }

    /// Get the list of currently discovered peers.
    pub async fn discovered_peers(&self) -> Vec<SocketAddr> {
        self.tracker_manager
            .discovered_peers(&self.metadata.info_hash)
            .await
            .unwrap_or_else(Vec::new)
    }

    /// Try to add the given tracker to the tracker manager of this torrent.
    /// This creates the tracker in a background task.
    pub async fn add_tracker_async(&self, entry: TrackerEntry) {
        self.tracker_manager.add_tracker_async(entry).await;
    }

    /// Add the given peer to this torrent.
    /// Duplicate peers will be ignored and dropped.
    fn add_peer(&mut self, peer: Box<dyn Peer>) {
        trace!("Torrent {} is trying to add new peer {}", self, peer);
        let info = peer.client();
        match self.peer_pool.add_peer(peer) {
            Ok(_) => {
                debug!("Torrent {} added peer {}", self, info);
                self.metrics.peers.inc();
                self.invoke_event(TorrentEvent::PeerConnected(info));
            }
            Err(e) => {
                debug!("Torrent {} failed to add peer {}, {}", self, info, e);
            }
        }
    }

    /// Handle a closed torrent peer connection.
    async fn on_peer_closed(&mut self, id: PeerIdentifier, reason: CloseReason) {
        trace!(
            "Torrent {} peer connection closed {:?}, reason: {:?}",
            self,
            id,
            reason
        );
        let peer = match self.peer_pool.peer_closed(&id, reason).await {
            None => return,
            Some(peer) => peer,
        };

        let bitfield = peer.remote_piece_bitfield().await;

        // decrease the availability of the pieces that the peer had
        for (piece_index, _) in bitfield.iter().enumerate().filter(|(_, value)| *value) {
            self.data_pool.update_availability(&piece_index, -1).await;
        }

        self.metrics.peers.dec();
        self.invoke_event(TorrentEvent::PeerDisconnected(peer.client()));
    }

    /// Add the given metadata to the torrent.
    /// This method can be used by extensions to update the torrent metadata when the current
    /// connection is based on a magnet link.
    ///
    /// If the data was already known, this method does nothing.
    pub(crate) fn add_metadata(&mut self, metadata_info: TorrentMetadataInfo) {
        // verify if the metadata of the torrent is already known
        // if so, we ignore this update
        if self.metadata.info.is_some() {
            return;
        }

        // validate the received metadata against our info hash
        let info_hash = self.metadata.info_hash.clone();
        let is_metadata_invalid = metadata_info
            .info_hash()
            .map(|metadata_info_hash| metadata_info_hash != info_hash)
            .map_err(|e| {
                debug!(
                    "Failed to calculate the info hash from the received metadata of {}, {}",
                    self, e
                );
            })
            .unwrap_or(true);
        if is_metadata_invalid {
            debug!("Torrent {} received invalid metadata", self);
            return;
        }

        self.metadata.info = Some(metadata_info);
        debug!("Torrent {} updated metadata of {}", self, info_hash);
        self.invoke_event(TorrentEvent::MetadataChanged(self.metadata.clone()));
    }

    /// Announce the torrent to all trackers.
    /// It returns the announcement result collected from all active trackers.
    pub async fn announce_all(&self) -> AnnouncementResult {
        self.tracker_manager
            .announce_all(&self.metadata.info_hash, AnnounceEvent::Started)
            .await
    }

    /// Announce to all the trackers without waiting for the results.
    pub async fn make_announce_all(&self) {
        self.tracker_manager
            .make_announcement_to_all(&self.metadata.info_hash, AnnounceEvent::Started)
    }

    /// Get the scrape metrics result from scraping all trackers for this torrent.
    pub async fn scrape(&self) -> Result<ScrapeMetrics> {
        trace!("Torrent {} is scraping trackers", self);
        match self.tracker_manager.scrape(&self.metadata.info_hash).await {
            Ok(result) => {
                if let Some(metrics) = result.files.get(&self.metadata.info_hash) {
                    Ok(ScrapeMetrics {
                        complete: metrics.complete,
                        incomplete: metrics.incomplete,
                        downloaded: metrics.downloaded,
                    })
                } else {
                    Err(TorrentError::InvalidInfoHash(format!(
                        "info hash {} not found in scrape result",
                        self.metadata.info_hash
                    )))
                }
            }
            Err(e) => Err(TorrentError::Tracker(e)),
        }
    }

    /// Add the given options to the torrent.
    ///
    /// It triggers the [TorrentEvent::OptionsChanged] event if the options changed.
    /// If the options are already present, this will be a no-op.
    pub fn add_options(&mut self, options: TorrentFlags) {
        // check if all the given options are already present
        // of so, this is a no-op
        if self.options.contains(options) {
            return;
        }

        self.options |= options;
        self.invoke_event(TorrentEvent::OptionsChanged);
    }

    /// Remove the given options from the torrent.
    ///
    /// It triggers the [TorrentEvent::OptionsChanged] event if the options changed.
    /// If none of the given options are present, this will be a no-op.
    pub fn remove_options(&mut self, options: TorrentFlags) {
        // check if any of the given options is actually present
        // of not, this is a no-op
        if !self.options.intersects(options) {
            return;
        }

        self.options &= !options;
        self.invoke_event(TorrentEvent::OptionsChanged);
    }

    /// Update the state of this torrent.
    /// If the torrent is already in the given state, this will be a no-op.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    pub fn update_state(&mut self, state: TorrentState) {
        // check if we're already in the expected state
        // if so, ignore this update
        if self.state == state {
            return;
        }

        self.state = state;

        // inform the trackers about the new state
        match &state {
            TorrentState::Downloading => self
                .tracker_manager
                .make_announcement_to_all(&self.metadata.info_hash, AnnounceEvent::Started),
            TorrentState::Seeding | TorrentState::Finished => self
                .tracker_manager
                .make_announcement_to_all(&self.metadata.info_hash, AnnounceEvent::Completed),
            TorrentState::Paused => self
                .tracker_manager
                .make_announcement_to_all(&self.metadata.info_hash, AnnounceEvent::Paused),
            _ => {}
        }

        debug!("Torrent {} state updated to {:?}", self, state);
        self.invoke_event(TorrentEvent::StateChanged(state));
    }

    /// Update the availability of the given piece indexes.
    /// This will increase or decrease the availability of the torrent pieces.
    ///
    /// This method can be used ot both increase and decrease the availability information
    /// to correctly establish the rarity of pieces.
    ///
    /// # Arguments
    ///
    /// * `pieces` - The piece indexes that need to be updated.
    ///* `available` - Indicates if the pieces become available or unavailable.
    pub async fn update_piece_availabilities(&self, pieces: Vec<PieceIndex>, available: bool) {
        // check if the metadata is known and the pieces have been created
        if !self.is_metadata_known() || self.data_pool().num_of_pieces().await == 0 {
            trace!(
                "Torrent {} is unable to update piece availabilities, metadata or pieces are unknown",
                self
            );
            return;
        }

        for piece in pieces {
            let change = if available { 1 } else { -1 };
            self.data_pool.update_availability(&piece, change).await;
        }
    }

    /// Set the pieces of the torrent.
    pub(crate) async fn update_pieces(&self, pieces: Vec<Piece>) {
        let total_pieces = pieces.len();
        self.data_pool.set_pieces(pieces).await;

        {
            // update the piece availability based on the current peer connections
            let mut availability: BTreeMap<PieceIndex, u32> = BTreeMap::new();
            let mut peer_count = 0u32;

            {
                for peer in self
                    .peer_pool
                    .peers()
                    .into_iter()
                    .filter_map(|peer| peer.upgrade())
                {
                    peer_count += 1;
                    for (piece_index, _) in peer
                        .remote_piece_bitfield()
                        .await
                        .into_iter()
                        .enumerate()
                        .filter(|(_, value)| *value)
                    {
                        *availability.entry(piece_index).or_insert(0) += 1;
                    }
                }
            }

            let availability_len = availability.len();
            if availability_len > 0 {
                for (piece, availability) in availability {
                    self.data_pool
                        .update_availability(&piece, availability as i32)
                        .await;
                }
                debug!(
                    "Torrent {} updated {} piece availabilities from {} peers",
                    self, availability_len, peer_count
                );
            }
        }

        debug!("Torrent {} updated {} pieces", self, total_pieces);
        self.update_interested_pieces_stats().await;
        self.invoke_event(TorrentEvent::PiecesChanged(total_pieces));
    }

    /// Set the files of the torrent, replacing any existing files.
    pub(crate) async fn update_files(&self, files: Vec<File>) {
        let total_files = files.len();
        self.data_pool.set_files(files).await;
        debug!("Torrent {} updated {} file(s)", self, total_files);
        self.invoke_event(TorrentEvent::FilesChanged);
    }

    /// Set the given piece as completed.
    /// This can be called by file validation operations to indicate that a piece has been stored in the storage.
    ///
    /// ## Remark
    ///
    /// This function doesn't verify if the piece is actually valid.
    pub async fn piece_completed(&mut self, piece: PieceIndex) {
        self.pieces_completed(vec![piece]).await;
    }

    /// Set the given pieces as completed.
    /// This can be called by file validation operations to indicate that a piece has been stored in the storage.
    ///
    /// ## Remark
    ///
    /// This function doesn't verify if the pieces are actually valid.
    pub async fn pieces_completed(&mut self, pieces: Vec<PieceIndex>) {
        let mut total_wanted_completed_size = 0;
        let mut total_completed_pieces_size = 0;
        let mut total_wanted_completed_pieces = 0;
        let mut total_completed_pieces = 0;

        for piece in pieces.iter() {
            self.data_pool.set_completed(piece, true).await;
            if let Some(piece) = self.data_pool.piece(piece).await {
                total_completed_pieces_size += piece.length;
                total_completed_pieces += 1;

                if piece.priority != PiecePriority::None {
                    total_wanted_completed_size += piece.length;
                    total_wanted_completed_pieces += 1;
                }
            } else {
                warn!(
                    "Torrent {} received unknown completed piece {}",
                    self, piece
                );
            }

            // remove the pending request
            self.pending_piece_requests.remove(&piece);
        }

        self.metrics.completed_pieces.inc_by(total_completed_pieces);
        self.metrics
            .wanted_completed_pieces
            .inc_by(total_wanted_completed_pieces);
        self.metrics
            .completed_size
            .inc_by(total_completed_pieces_size as u64);
        self.metrics
            .wanted_completed_size
            .inc_by(total_wanted_completed_size as u64);

        // inform the subscribers about each completed piece
        for piece in pieces.iter() {
            debug!("Torrent {} piece {} has been completed", self, piece);
            self.invoke_event(TorrentEvent::PieceCompleted(*piece));
        }

        // check if the all wanted pieces have been completed
        let is_completed = self.is_completed().await;
        if is_completed {
            // offload the state change to the main loop
            self.update_state(TorrentState::Finished);
        }

        // notify the connected peers about the completed pieces
        self.notify_peers_have_pieces(pieces);
    }

    /// Update the stats info of all interested pieces by the torrent.
    async fn update_interested_pieces_stats(&self) {
        let mut wanted_pieces = 0;
        let mut wanted_completed_pieces = 0;
        let mut wanted_size = 0;
        let mut wanted_completed_size = 0;

        {
            for piece_index in self.data_pool.interested_pieces().await {
                if let Some(piece) = self.data_pool.piece(&piece_index).await {
                    wanted_pieces += 1;
                    wanted_size += piece.length;

                    if piece.is_completed() {
                        wanted_completed_pieces += 1;
                        wanted_completed_size += piece.length;
                    }
                }
            }
        }

        self.metrics.wanted_pieces.set(wanted_pieces);
        self.metrics
            .wanted_completed_pieces
            .set(wanted_completed_pieces);
        self.metrics.wanted_size.set(wanted_size as u64);
        self.metrics
            .wanted_completed_size
            .set(wanted_completed_size as u64);
    }

    /// Cancel all currently queued pending requests of the torrent.
    /// This will clear all pending requests from the buffer.
    pub async fn cancel_all_pending_requests(&self) {
        // TODO: cancel pending requests in the peer
    }

    /// Resume the torrent.
    /// This will put the torrent back into [TorrentFlags::DownloadMode], trying to download any missing pieces.
    pub(crate) async fn resume(&mut self) {
        self.add_options(TorrentFlags::DownloadMode | TorrentFlags::Metadata);
        self.remove_options(TorrentFlags::Paused);

        // announce to the trackers if we don't know any peers
        if self.peer_pool.num_connect_candidates() == 0 {
            self.tracker_manager
                .make_announcement_to_all(&self.metadata.info_hash, AnnounceEvent::Started);
        }

        let wanted_pieces = self.total_wanted_pieces().await;
        debug!(
            "Torrent {} is resuming with {} wanted remaining pieces",
            self, wanted_pieces
        );
    }

    /// Pause the torrent operations.
    pub(crate) fn pause(&mut self) {
        self.add_options(TorrentFlags::Paused);
        self.update_state(TorrentState::Paused);
    }

    /// Add the specified peer addresses to the peer pool of the torrent.
    ///
    /// These peers will be considered as potential connection targets in the future,
    /// particularly when the torrent requires additional connections.
    /// The provided addresses are queued for possible use; there is no immediate
    /// guarantee that connections will be attempted right away.
    pub fn add_peer_addresses(&mut self, peer_addrs: Vec<SocketAddr>) {
        self.peer_pool.add_peer_addresses(peer_addrs, self.addr());
    }

    /// Process the given command for the torrent context.
    /// It returns `true` when the main loop of the context needs to be stopped, else `false`.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    pub(crate) async fn on_command(&mut self, command: TorrentCommand) {
        match command {
            TorrentCommand::PeerPort { response } => {
                response.send(self.peer_port);
            }
            TorrentCommand::AddPeer { addr, response } => {
                response.send(self.add_peer_addresses(vec![addr]));
            }
            TorrentCommand::AddPeers { addrs } => {
                self.add_peer_addresses(addrs);
            }
            TorrentCommand::GetPeer { handle, response } => {
                response.send(self.peer_pool.get(&handle));
            }
            TorrentCommand::PeerConnected { peer } => self.add_peer(peer),
            TorrentCommand::DecreasePeerPriority { addrs } => {
                self.decrease_peer_addr_priority(addrs)
            }
            TorrentCommand::PeerClosed { peer, reason } => {
                self.on_peer_closed(peer, reason).await;
            }
            TorrentCommand::Metadata { response } => {
                response.send(self.metadata.clone());
            }
            TorrentCommand::UpdateMetadata { metadata, response } => {
                response.send(self.add_metadata(metadata))
            }
            TorrentCommand::IsMetadataKnown { response } => {
                response.send(self.is_metadata_known());
            }
            TorrentCommand::InfoHash { response } => {
                response.send(self.metadata.info_hash.clone());
            }
            TorrentCommand::GetOptions { response } => {
                response.send(self.options);
            }
            TorrentCommand::AddOptions { options, response } => {
                self.add_options(options);
                response.send(());
                self.options_changed().await;
            }
            TorrentCommand::RemoveOptions { options, response } => {
                self.remove_options(options);
                response.send(());
                self.options_changed().await;
            }
            TorrentCommand::State { response } => {
                response.send(*self.state());
            }
            TorrentCommand::GetConfig { response } => {
                response.send(self.config.clone());
            }
            TorrentCommand::NumOfPieces { response } => {
                response.send(self.data_pool().num_of_pieces().await);
            }
            TorrentCommand::NumOfFiles { response } => {
                response.send(self.data_pool().num_of_files().await);
            }
            TorrentCommand::NumOfCompletedPieces { response } => {
                response.send(self.data_pool().num_completed_pieces().await);
            }
            TorrentCommand::NumOfActivePeerConnections { response } => {
                response.send(self.peer_pool.active_peer_connections().await);
            }
            TorrentCommand::NumOfActiveTrackerConnections { response } => {
                response.send(self.active_tracker_connections().await);
            }
            TorrentCommand::GetPath { response } => {
                response.send(self.path().clone());
            }
            TorrentCommand::GetFilePath { file, response } => {
                response.send(self.absolute_file_path(&file));
            }
            TorrentCommand::GetPieces { response } => {
                response.send(self.data_pool.pieces().await);
            }
            TorrentCommand::GetPiece { piece, response } => {
                response.send(self.data_pool.piece(&piece).await);
            }
            TorrentCommand::WantedPieces { response } => {
                response.send(self.data_pool.wanted_pieces().await);
            }
            TorrentCommand::WantedRequestPieces { response } => {
                response.send(self.wanted_request_pieces().await)
            }
            TorrentCommand::PieceAvailabilities { pieces, available } => {
                self.update_piece_availabilities(pieces, available).await;
            }
            TorrentCommand::GetFiles { response } => {
                response.send(self.data_pool.files().await);
            }
            TorrentCommand::GetFile { file, response } => {
                response.send(self.data_pool.file(&file).await);
            }
            TorrentCommand::GetFileIndexFor { piece, response } => {
                response.send(self.data_pool.file_index_for(&piece).await)
            }
            TorrentCommand::HasPiece { piece, response } => {
                response.send(self.data_pool.is_piece_completed(&piece).await);
            }
            TorrentCommand::HasBytes { bytes, response } => {
                response.send(self.has_bytes(&bytes).await)
            }
            TorrentCommand::GetPiecePriorities { response } => {
                response.send(self.data_pool.piece_priorities().await)
            }
            TorrentCommand::PieceCompleted { piece, response } => {
                response.send(self.piece_completed(piece).await);
            }
            TorrentCommand::PrioritizePieces {
                priorities,
                response,
            } => {
                response.send(self.prioritize_pieces(priorities).await);
            }
            TorrentCommand::PrioritizeFiles {
                priorities,
                response,
            } => {
                response.send(self.prioritize_files(priorities).await);
            }
            TorrentCommand::PrioritizeBytes {
                bytes,
                priority,
                response,
            } => {
                response.send(self.prioritize_bytes(&bytes, priority).await);
            }
            TorrentCommand::IsCompleted { response } => {
                response.send(self.is_completed().await);
            }
            TorrentCommand::IsPaused { response } => {
                response.send(self.is_paused());
            }
            TorrentCommand::Pause { response } => {
                response.send(self.pause());
            }
            TorrentCommand::Resume { response } => {
                response.send(self.resume().await);
            }
            TorrentCommand::AnnounceAll { response } => {
                response.send(self.announce_all().await);
            }
            TorrentCommand::ScrapeAll { response } => {
                response.send(self.scrape().await);
            }
            TorrentCommand::ReadPiece { piece, response } => {
                response.send(self.read_piece(&piece).await);
            }
            TorrentCommand::ReadPieceBytes {
                piece,
                bytes,
                response,
            } => response.send(self.read_piece_bytes(&piece, bytes).await),
            TorrentCommand::ReadFileToEnd { file, response } => {
                response.send(self.read_file_to_end(&file).await);
            }
            TorrentCommand::HashV1Data { piece, response } => {
                response.send(self.storage().hash_v1(&piece).await.map_err(Into::into));
            }
            TorrentCommand::HashV2Data { piece, response } => {
                response.send(self.storage().hash_v2(&piece).await.map_err(Into::into));
            }
            TorrentCommand::ProtocolExtensions { response } => {
                response.send(self.protocol_extensions())
            }
            TorrentCommand::Extensions { response } => response.send(self.extensions()),
            TorrentCommand::Bitfield { response } => response.send(self.data_pool.bitfield().await),
            TorrentCommand::PendingRequestRejected { piece, begin, peer } => {
                self.pending_request_rejected(piece, begin, peer).await
            }
            TorrentCommand::RequestDownloadPermit { piece, response } => {
                response.send(self.request_download_permit(&piece).await);
            }
            TorrentCommand::RequestUploadPermit { response } => {
                response.send(self.request_upload_permit().await);
            }
            TorrentCommand::PiecePartCompleted { part, data } => {
                self.process_completed_piece_part(part, data).await
            }
            TorrentCommand::IsDownloadAllowed { response } => {
                response.send(self.is_download_allowed());
            }
            TorrentCommand::IsUploadAllowed { response } => {
                response.send(self.is_upload_allowed());
            }
            TorrentCommand::IsPartialSeed { response } => {
                response.send(self.is_partial_seed().await);
            }
            #[cfg(test)]
            TorrentCommand::DataPool { response } => response.send(self.data_pool.clone()),
        }
    }

    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn handle_tracker_event(&mut self, event: TrackerClientEvent) {
        match event {
            TrackerClientEvent::PeersDiscovered(info_hash, peers) => {
                if info_hash == self.metadata.info_hash {
                    self.add_peer_addresses(peers)
                }
            }
            TrackerClientEvent::TrackerAdded(handle) => {
                let is_paused = self.options.contains(TorrentFlags::Paused);
                let is_pieces_known = self.data_pool.num_of_pieces().await > 0;
                let is_completed = self.is_completed().await;
                let mut event = AnnounceEvent::Started;

                if is_paused {
                    event = AnnounceEvent::Paused;
                } else if is_pieces_known && is_completed {
                    event = AnnounceEvent::Completed;
                }

                self.tracker_manager
                    .make_announcement(handle, &self.metadata.info_hash, event);
                self.invoke_event(TorrentEvent::TrackersChanged);
            }
            _ => {}
        }
    }

    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn handle_incoming_peer_connection(&mut self, entry: PeerEntry) {
        trace!(
            "Torrent {} is trying to accept incoming {} peer connection",
            self,
            entry.socket_addr
        );
        let timeout = self.config.peer_connection_timeout;

        let handle = self.handle;
        let peer_id = self.peer_id;
        let data_pool = self.data_pool.clone();
        let protocol_extensions = self.protocol_extensions();
        let command_sender = self.command_sender.clone();
        let callbacks = self.callbacks.clone();
        tokio::spawn(async move {
            match BitTorrentPeer::new_inbound(
                peer_id,
                entry.socket_addr,
                entry.stream,
                InnerTorrent {
                    handle,
                    sender: command_sender.clone(),
                    callbacks,
                },
                data_pool.clone(),
                protocol_extensions,
                timeout,
            )
            .await
            {
                Ok(peer) => {
                    debug!(
                        "Torrent {} established connection with peer {}",
                        handle, peer
                    );
                    command_sender
                        .fire_and_forget(TorrentCommand::PeerConnected {
                            peer: Box::new(peer),
                        })
                        .await;
                }
                Err(e) => debug!(
                    "Torrent {} failed to accept incoming peer connection {}, {}",
                    handle, entry.socket_addr, e
                ),
            }
        });
    }

    fn decrease_peer_addr_priority(&mut self, peers: Vec<SocketAddr>) {
        for peer in peers {
            self.peer_pool.update_peer_rank(&peer, -1);
        }
    }

    async fn add_torrent_to_tracker(&mut self) -> bool {
        let info_hash = self.metadata.info_hash.clone();
        let peer_port = self.peer_port().cloned().unwrap_or(6881);

        if let Err(e) = self
            .tracker_manager
            .add_torrent(self.peer_id, peer_port, info_hash, self.metrics.clone())
            .await
        {
            error!(
                "Torrent {} failed to register with tracker manager, {}",
                self, e
            );
            self.update_state(TorrentState::Error);
            return false;
        }

        true
    }

    async fn pending_request_rejected(
        &mut self,
        piece: PieceIndex,
        begin: usize,
        peer: PeerClientInfo,
    ) {
        if let Some(part) = self.find_piece_part(piece, begin).await {
            debug!(
                "Torrent {} received rejected request for part {:?} from {:?}",
                self, part, peer
            );
            // release the pending request to be retried by another peer
            self.pending_piece_requests.remove(&piece);
        } else {
            warn!(
                "Unable to find rejected request part for piece {}, begin {} for {}",
                piece, begin, self
            )
        }
    }

    async fn process_completed_piece_part(&mut self, piece_part: PiecePart, data: Vec<u8>) {
        let piece = match self.data_pool.piece(&piece_part.piece).await {
            Some(piece) => piece,
            None => return,
        };

        // check if the piece has already been completed
        // this can happen "end game" as the same piece & parts are requested from multiple torrents
        if piece.is_completed() {
            debug!(
                "Torrent {} received piece {} part {} data which has already been completed",
                self, piece_part.piece, piece_part.part
            );
            return;
        }

        trace!(
            "Torrent {} writing piece {} part {} data (size {}) to chunk pool",
            self,
            piece_part.piece,
            piece_part.part,
            data.len()
        );
        match self
            .piece_chunk_pool
            .add_chunk(&piece_part, piece.len(), data)
            .await
        {
            Ok(_) => {
                // update the piece info
                self.data_pool
                    .set_part_completed(&piece.index, &piece_part.part)
                    .await;
                self.pending_piece_requests
                    .insert(piece.index, Instant::now());

                if self.data_pool.is_piece_completed(&piece.index).await {
                    self.process_completed_piece(piece.index).await;
                }
            }
            Err(e) => warn!("Failed to add chunk data for {}, {}", self, e),
        }
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    async fn process_completed_piece(&mut self, piece: PieceIndex) {
        if let Some(data) = self.piece_chunk_pool.get(piece).await {
            let data_size = data.len();
            trace!(
                "Torrent {} is validating piece {} data (size {})",
                self,
                piece,
                data_size
            );
            let is_valid = self.validate_piece_index_data(&piece, &data).await;

            if is_valid {
                debug!(
                    "Torrent {} validated piece {} data (size {}) with success",
                    self, piece, data_size
                );

                match self.storage.write(&data, &piece, 0).await {
                    Ok(len) => {
                        trace!("Torrent {} wrote piece {} ({} bytes)", self, piece, len);
                        self.piece_completed(piece).await
                    }
                    Err(e) => {
                        error!(
                            "Torrent {} failed to write piece {} data, {}",
                            self, piece, e
                        );
                        // reset the pending piece to be retried
                        self.pending_piece_requests.remove(&piece);
                        self.metrics.wasted.inc_by(data_size as u64);
                    }
                }
            } else {
                trace!(
                    "Torrent {} validated piece {} data (size {}) as failure",
                    self,
                    piece,
                    data_size
                );
                self.data_pool.set_completed(&piece, false).await;
                self.metrics.wasted.inc_by(data_size as u64);
            }
        } else {
            warn!(
                "Torrent {} received piece completion of {}, but no data is available in the chunk pool",
                self, piece
            );
        }
    }

    /// Process the new options of the torrent.
    async fn options_changed(&mut self) {
        // update the state of the torrent based on the new options
        // this can only be done after the init phase to not disrupt the init operations
        let is_not_init_state = !self.state.is_initializing_phase();
        if is_not_init_state {
            let state = self.determine_state().await;
            self.update_state(state);
        }
    }

    /// Try to determine the state the torrent currently has.
    /// It returns the expected state of the torrent without actually updating the state.
    pub async fn determine_state(&self) -> TorrentState {
        let is_paused = self.options.contains(TorrentFlags::Paused);
        let is_download_mode = self.options.contains(TorrentFlags::DownloadMode);

        if is_paused {
            return TorrentState::Paused;
        }

        let total_pieces = self.data_pool.num_of_pieces().await;
        if total_pieces == 0 {
            return TorrentState::Initializing;
        }

        if is_download_mode {
            return TorrentState::Downloading;
        }
        if self.is_upload_allowed() {
            return TorrentState::Seeding;
        }

        TorrentState::Finished
    }

    /// Validate if the given piece data is valid.
    /// It retrieves the known piece hash from the pieces map and checks if the hash matches the data.
    ///
    /// ## Remarks
    ///
    /// If an unknown [PieceIndex] is given, it will always be assumed as invalid as there is no way to validate the data.
    #[cfg_attr(feature = "tracing", instrument(skip(self, data)))]
    pub async fn validate_piece_index_data(&self, piece: &PieceIndex, data: &[u8]) -> bool {
        if let Some(piece) = self.data_pool.piece(piece).await {
            return Self::validate_piece_data(&piece, data);
        } else {
            warn!(
                "Unable to validate piece data, piece {} is unknown within {}",
                piece, self
            );
        }

        false
    }

    /// Get the piece part of the torrent based on the piece and the offset within the piece.
    /// It returns [None] if the piece part is unknown to this torrent.
    ///
    /// # Arguments
    ///
    /// * `piece` - The index of the piece.
    /// * `begin` - The offset within the piece.
    pub async fn piece_part(&self, piece: PieceIndex, begin: usize) -> Option<PiecePart> {
        self.find_piece_part(piece, begin).await
    }

    /// Get the total amount of completed pieces for the torrent.
    pub async fn total_completed_pieces(&self) -> usize {
        self.data_pool.num_completed_pieces().await
    }

    /// Get a request permit for the given piece to download piece data from a remote peer.
    /// A permit should be retrieved for each piece that is being requested from a peer.
    pub async fn request_download_permit(
        &mut self,
        piece: &PieceIndex,
    ) -> Option<OwnedSemaphorePermit> {
        if !self.is_download_allowed() {
            return None;
        }

        // check if the request is already in-flight and not timed-out
        let is_end_game = self.data_pool.is_end_game().await;
        let is_piece_download_allowed = self
            .pending_piece_requests
            .get(piece)
            .filter(|e| e.elapsed() <= PEER_REQUEST_TIMEOUT)
            .is_none();
        if !is_end_game && !is_piece_download_allowed {
            trace!(
                "Torrent {} is already requesting piece {} data",
                self,
                piece
            );
            return None;
        }

        if let Some(permit) = self
            .request_download_permits
            .clone()
            .try_acquire_owned()
            .ok()
        {
            self.pending_piece_requests.insert(*piece, Instant::now());
            return Some(permit);
        }

        None
    }

    /// Get a request permit to upload piece data to a remote peer.
    /// A permit is peer based and should only be requested when trying to unchoke the client peer.
    pub async fn request_upload_permit(&self) -> Option<OwnedSemaphorePermit> {
        if !self.is_upload_allowed() {
            return None;
        }

        self.request_upload_permits.clone().try_acquire_owned().ok()
    }

    /// Try to read the bytes from the given torrent file.
    /// This reads all available bytes of the file stored within the [Storage].
    ///
    /// ## Remarks
    ///
    /// This doesn't verify if the bytes are valid and completed.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    pub async fn read_file_to_end(&self, file: &FileIndex) -> Result<(usize, Vec<u8>)> {
        let file = self
            .data_pool
            .file(file)
            .await
            .ok_or(TorrentError::DataUnavailable)?;
        if let Some(piece) = self.data_pool.piece(&file.pieces.start).await {
            let len = file.len();
            let mut buffer = vec![0; len];
            let file_offset = file.torrent_offset.saturating_sub(piece.offset);

            let bytes_read = self
                .storage
                .read(&mut buffer, &piece.index, file_offset)
                .await?;

            return Ok((bytes_read, buffer[..bytes_read].to_vec()));
        }

        Err(TorrentError::DataUnavailable)
    }

    /// Try to read the given piece bytes.
    /// It will read the bytes from all relevant files which overlap with the given piece.
    ///
    /// ## Remarks
    ///
    /// This doesn't verify if the bytes are valid and completed.
    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub(crate) async fn read_piece(&self, piece: &PieceIndex) -> Result<Vec<u8>> {
        match self.data_pool.piece(piece).await {
            None => Err(TorrentError::DataUnavailable),
            Some(piece) => {
                let mut buffer = vec![0; piece.length];
                let bytes_read = self.storage.read(&mut buffer, &piece.index, 0).await?;
                if bytes_read != piece.len() {
                    return Err(TorrentError::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "wanted {} bytes, but got {} instead",
                            piece.len(),
                            bytes_read
                        ),
                    )));
                }

                Ok(buffer)
            }
        }
    }

    /// Try to read the given piece bytes range.
    ///
    /// ## Remarks
    ///
    /// This doesn't verify if the bytes are valid and completed.
    #[cfg_attr(feature = "tracing", instrument(skip(self, range)))]
    pub(crate) async fn read_piece_bytes(
        &self,
        piece: &PieceIndex,
        range: std::ops::Range<usize>,
    ) -> Result<Vec<u8>> {
        // TODO: improve the retrieval of bytes
        self.read_piece(piece).await.map(|e| e[range].to_vec())
    }

    /// Try to read the given bytes from the torrent.
    /// This reads all available bytes of one or more files from the torrent stored within the [Storage].
    /// The returned bytes will be padded with 0 if the available data is smaller than the requested range.
    ///
    /// # Arguments
    ///
    /// * `torrent_range` - The byte range within the torrent to read.
    ///
    /// # Returns
    ///
    /// It returns the bytes read from the torrent, padding the bytes with `0` if the data was not available.
    pub async fn read_bytes_with_padding(
        &self,
        torrent_range: std::ops::Range<usize>,
    ) -> Result<Vec<u8>> {
        self.internal_read_bytes(torrent_range, true).await
    }

    /// Try to read the given bytes from the torrent.
    /// This reads all bytes of one or more files from the torrent stored within the [Storage].
    ///
    /// # Arguments
    ///
    /// * `torrent_range` - The byte range within the torrent to read.
    ///
    /// # Returns
    ///
    /// It returns the bytes read from the torrent, returning a [TorrentError] if data was not available.
    pub async fn read_bytes(&self, torrent_range: std::ops::Range<usize>) -> Result<Vec<u8>> {
        self.internal_read_bytes(torrent_range, false).await
    }

    async fn internal_read_bytes(
        &self,
        torrent_range: std::ops::Range<usize>,
        with_padding: bool,
    ) -> Result<Vec<u8>> {
        // verify that the given range is not longer than the total torrent size
        let length = self.len().ok_or(TorrentError::InvalidMetadata(
            "metadata is unknown".to_string(),
        ))?;
        if torrent_range.is_empty() || torrent_range.end > length {
            return Err(TorrentError::InvalidRange(torrent_range));
        }

        let pieces = self.data_pool.pieces().await;
        let starting_piece = pieces
            .iter()
            .find(|piece| {
                torrent_range.start >= piece.offset
                    && torrent_range.start <= piece.offset + piece.length
            })
            .ok_or(TorrentError::DataUnavailable)?;
        let offset = torrent_range.start.saturating_sub(starting_piece.offset);
        let mut buffer = vec![0u8; torrent_range.len()];

        let bytes_read = self
            .storage
            .read(&mut buffer, &starting_piece.index, offset)
            .await?;
        if bytes_read < torrent_range.len() && !with_padding {
            return Err(TorrentError::DataUnavailable);
        }

        Ok(buffer)
    }

    /// Cleanup the peer resources which have been closed or are no longer valid.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn clean_peers(&mut self) {
        trace!("Torrent {} is executing peer cleanup cycle", self);
        for peer in self.peer_pool.clean().await {
            self.callbacks
                .invoke(TorrentEvent::PeerDisconnected(peer.client()));
        }
    }

    /// Notify the peers about the pieces that have become available.
    fn notify_peers_have_pieces(&self, pieces: Vec<PieceIndex>) {
        for peer in self.peer_pool.peers.values() {
            peer.notify_piece_availability(pieces.clone());
        }
    }

    /// Invoke the given torrent event for all registered callbacks.
    pub(crate) fn invoke_event(&self, event: TorrentEvent) {
        self.callbacks.invoke(event)
    }

    /// Validate the given piece data.
    /// The data will be validated against the underlying hash of the piece.
    ///
    /// # Important
    ///
    /// This is computationally expensive operation and should be executed on a thread pool.
    ///
    /// # Returns
    ///
    /// It returns `true` if the data is valid for the given piece, else `false`.
    pub fn validate_piece_data(piece: &Piece, data: &[u8]) -> bool {
        let hash = &piece.hash;

        if hash.has_v2() {
            let actual_hash = Sha256::digest(&data);
            hash.hash_v2()
                .map_or(false, |v2_hash| v2_hash == actual_hash.as_slice())
        } else {
            let actual_hash = Sha1::digest(&data);
            hash.hash_v1()
                .map_or(false, |v1_hash| v1_hash == actual_hash.as_slice())
        }
    }

    /// Execute the torrent operations chain.
    ///
    /// This will execute the operations in order as defined by the chain.
    /// If an operation returns [None], the execution chain will be interrupted.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn execute_operations_chain(
        &mut self,
        operations: &mut Vec<Box<dyn TorrentOperation>>,
        peer_discoveries: &[Arc<dyn PeerDiscovery>],
    ) {
        for operation in operations.iter_mut() {
            let execution_result = operation.execute(self, peer_discoveries).await;
            if execution_result == TorrentOperationResult::Stop {
                break;
            }
        }
    }
}

impl Callback<TorrentEvent> for TorrentContext {
    fn subscribe(&self) -> Subscription<TorrentEvent> {
        self.callbacks.subscribe()
    }

    fn subscribe_with(&self, subscriber: Subscriber<TorrentEvent>) {
        self.callbacks.subscribe_with(subscriber)
    }
}

impl Display for TorrentContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.handle)
    }
}

impl PartialEq for TorrentContext {
    fn eq(&self, other: &Self) -> bool {
        self.handle == other.handle
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_torrent;
    use crate::create_torrent_context;
    use crate::operation::{
        TorrentConnectPeersOperation, TorrentCreatePiecesAndFilesOperation,
        TorrentFileValidationOperation, TorrentStatsOperation,
    };
    use crate::peer::TcpPeerDiscovery;
    use crate::storage::MemoryStorage;
    use crate::tests::helpers::{wait_for_torrent_pieces, wait_for_torrent_state};
    use crate::tests::{copy_test_file, read_test_file_to_bytes};
    use crate::{InfoHash, Magnet};
    use std::net::Ipv4Addr;
    use std::ops::Sub;
    use std::str::FromStr;
    use tempfile::tempdir;
    use tokio::sync::mpsc::unbounded_channel;

    mod drop {
        use super::*;
        use tokio::sync::oneshot;
        use tokio::time::timeout;

        #[tokio::test]
        async fn test_drop_last_torrent() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let (tx, rx) = oneshot::channel();
            let torrent = create_torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![],
                vec![]
            );
            let command_sender = torrent.inner.sender.clone();

            let mut receiver = torrent.subscribe();
            tokio::spawn(async move {
                while let Some(event) = receiver.recv().await {
                    if let TorrentEvent::StateChanged(state) = &*event {
                        if state == &TorrentState::Stopped {
                            let _ = tx.send(());
                            break;
                        }
                    }
                }
            });

            // drop the last torrent reference
            drop(torrent);

            // wait for the torrent to reach the stopped state
            timeout(Duration::from_millis(250), rx)
                .await
                .expect("timeout waiting for TorrentState::Stopped")
                .unwrap();

            // verify that the context run task is stopped
            assert!(
                command_sender.is_closed(),
                "context run task should be stopped"
            );
        }
    }

    mod tracker {
        use super::*;
        use crate::tracker::TrackerServer;

        #[tokio::test]
        async fn test_announce() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let torrent = create_torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build()
            );

            let result = torrent.announce().await.unwrap();

            assert_ne!(
                0, result.total_seeders,
                "expected seeders to have been found"
            );
            assert_ne!(0, result.peers.len(), "expected peers to have been found");
        }

        #[tokio::test]
        async fn test_scrape() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let tracker_server = TrackerServer::new().await.unwrap();
            let tracker_manager = TrackerClient::new(Duration::from_secs(1));
            let torrent = create_torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![],
                vec![],
                |_| Box::new(MemoryStorage::new()),
                None,
                tracker_manager.clone()
            );

            // add the server to the tracker client
            match tracker_manager
                .add_tracker_entry(TrackerEntry {
                    tier: 0,
                    url: Url::parse(
                        format!("http://localhost:{}/announce", tracker_server.addr().port())
                            .as_str(),
                    )
                    .unwrap(),
                })
                .await
            {
                Ok(_) => {}
                Err(e) => assert!(
                    false,
                    "expected the tracker entry to have been added, {}",
                    e
                ),
            }

            // add a dummy peer
            let info_hash = torrent.info_hash().await.unwrap();
            tracker_server
                .add_peer(
                    info_hash.clone(),
                    (Ipv4Addr::LOCALHOST, 8000).into(),
                    PeerId::new(),
                    6881,
                    true,
                )
                .await;

            let result = torrent.scrape().await.unwrap();

            assert_ne!(
                0, result.downloaded,
                "expected scrape results to have been returned"
            )
        }
    }

    mod metadata {
        use super::*;
        use crate::operation::TorrentMetadataOperation;

        #[tokio::test]
        async fn test_metadata_available() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let filename = "debian-udp.torrent";
            let torrent_info_data = read_test_file_to_bytes(filename);
            let torrent_info = TorrentMetadata::try_from(torrent_info_data.as_slice()).unwrap();
            let torrent = create_torrent!(
                filename,
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![Box::new(TorrentCreatePiecesAndFilesOperation::new())]
            );

            let metadata = torrent.metadata().await.unwrap();

            assert_eq!(torrent_info, metadata);
            assert!(
                metadata.info.is_some(),
                "expected the torremt metadata info to have been known"
            );
        }

        #[tokio::test]
        async fn test_retrieve_metadata() {
            init_logger!();
            let filename = "debian.torrent";
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            copy_test_file(temp_path, filename, None);
            let info =
                TorrentMetadata::try_from(read_test_file_to_bytes(filename).as_slice()).unwrap();
            let magnet_uri = Magnet::try_from(&info).unwrap().to_string();
            let (tx, mut rx) = unbounded_channel();
            let source_torrent = create_torrent!(
                filename,
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![],
                vec![Box::new(TcpPeerDiscovery::new().await.unwrap())]
            );
            let torrent = create_torrent!(
                magnet_uri.as_str(),
                temp_path,
                TorrentFlags::Metadata,
                TorrentConfig::builder().build(),
                vec![
                    Box::new(TorrentStatsOperation::new()),
                    Box::new(TorrentConnectPeersOperation::new(false)),
                    Box::new(TorrentMetadataOperation::new(None))
                ],
                vec![Box::new(TcpPeerDiscovery::new().await.unwrap())],
                |_| { Box::new(MemoryStorage::new()) },
                None
            );

            // listen for the metadata changed event
            let torrent_handle = torrent.handle();
            let mut receiver = torrent.subscribe();
            tokio::spawn(async move {
                while let Some(event) = receiver.recv().await {
                    match &*event {
                        TorrentEvent::MetadataChanged(_) => {
                            let _ = tx.send(());
                            break;
                        }
                        TorrentEvent::Stats(stats) => {
                            debug!("Torrent {} stats: {}", torrent_handle, stats);
                        }
                        _ => {}
                    }
                }
            });

            // connect the torrent to the source torrent, which has the metadata
            let port = source_torrent
                .peer_port()
                .await
                .expect("expected the source torrent peer port");
            torrent
                .add_peer((Ipv4Addr::LOCALHOST, port).into())
                .await
                .unwrap();

            timeout!(
                rx.recv(),
                Duration::from_secs(10),
                "expected to receive a MetadataChanged event"
            )
            .unwrap();
            let result = torrent.metadata().await.unwrap();

            assert_ne!(
                None, result.info,
                "expected the metadata to have been present"
            );
        }

        #[tokio::test]
        async fn test_info_hash() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let info_hash = InfoHash::from_str("EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let torrent = create_torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![Box::new(TorrentCreatePiecesAndFilesOperation::new())]
            );

            let result = torrent.info_hash().await.unwrap();

            assert_eq!(info_hash, result, "expected the info hash to match");
        }
    }

    mod pieces {
        use super::*;

        #[tokio::test]
        async fn test_create_pieces() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let torrent = create_torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![Box::new(TorrentCreatePiecesAndFilesOperation::new())]
            );
            let (tx, mut rx) = unbounded_channel();

            let mut receiver = torrent.subscribe();
            tokio::spawn(async move {
                loop {
                    if let Some(event) = receiver.recv().await {
                        if let TorrentEvent::PiecesChanged(_) = *event {
                            tx.send(()).unwrap();
                        }
                    } else {
                        break;
                    }
                }
            });

            // wait for the pieces changed event
            timeout!(
                rx.recv(),
                Duration::from_millis(750),
                "expected the pieces to be created"
            )
            .unwrap();
            let pieces = torrent.pieces().await.unwrap();

            assert_ne!(0, pieces.len(), "expected the pieces to have been created");
        }

        #[tokio::test]
        async fn test_get_piece() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let torrent = create_torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build()
            );

            // wait for the pieces to have been create
            wait_for_torrent_pieces(&torrent).await;

            let result = torrent
                .piece(&0)
                .await
                .expect("expected a piece to have been returned");
            assert_eq!(0, result.index, "expected the piece index to match");
        }

        #[tokio::test]
        async fn test_piece_part() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let expected_piece_part = PiecePart {
                piece: 0,
                part: 1,
                begin: 16384,
                length: 16384,
            };
            let (mut context, _) = create_torrent_context!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build()
            );

            // create the torrent pieces
            let mut operation = TorrentCreatePiecesAndFilesOperation::new();
            let result = operation.execute(&mut context, vec![].as_slice()).await;
            assert_eq!(TorrentOperationResult::Continue, result);

            // request an invalid piece part
            let result = context.piece_part(0, 16000).await;
            assert_eq!(
                None, result,
                "expected no piece part to be returned for invalid begin"
            );

            // request a valid piece part
            let result = context.piece_part(0, 16384).await;
            assert_eq!(Some(expected_piece_part), result, "expected the piece part");
        }

        #[tokio::test]
        async fn test_total_wanted_pieces() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let expected_result = 75;
            let mut operation = TorrentCreatePiecesAndFilesOperation::new();
            let (mut context, _) = create_torrent_context!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![]
            );

            // create the torrent pieces
            operation.execute(&mut context, vec![].as_slice()).await;

            // only request the first piece
            let total_pieces = context.data_pool().num_of_pieces().await;
            let priorities = (0..total_pieces)
                .into_iter()
                .map(|i| {
                    if i < expected_result {
                        (i, PiecePriority::Normal)
                    } else {
                        (i, PiecePriority::None)
                    }
                })
                .collect();
            context.prioritize_pieces(priorities).await;

            // check the total wanted pieces
            let result = context.total_wanted_pieces().await;
            assert_eq!(expected_result, result);
        }
    }

    mod files {
        use super::*;

        #[tokio::test]
        async fn test_create_files() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let torrent = create_torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![Box::new(TorrentCreatePiecesAndFilesOperation::new())]
            );
            let (tx, mut rx) = unbounded_channel();

            // wait for the pieces changed event
            let mut receiver = torrent.subscribe();
            tokio::spawn(async move {
                while let Some(event) = receiver.recv().await {
                    if let TorrentEvent::FilesChanged = *event {
                        tx.send(()).unwrap();
                    }
                }
            });

            let _ = timeout!(
                rx.recv(),
                Duration::from_millis(750),
                "expected the files to be created"
            )
            .unwrap();
            let files = torrent.files().await;

            assert_eq!(1, files.len(), "expected the files to have been created");
        }

        #[tokio::test]
        async fn test_get_file() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let torrent = create_torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build()
            );

            // wait for the pieces to have been create
            wait_for_torrent_pieces(&torrent).await;

            let result = torrent
                .file(&0)
                .await
                .expect("expected a file to have been returned");
            assert_eq!(0, result.index, "expected the file index to match");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_torrent_resume_internal() {
        init_logger!(LevelFilter::Debug);
        let temp_dir_source = tempdir().unwrap();
        let temp_path_source = temp_dir_source.path().to_str().unwrap();
        let temp_dir_target = tempdir().unwrap();
        let temp_path_target = temp_dir_target.path().to_str().unwrap();
        let num_of_pieces = 30;
        copy_test_file(
            temp_path_source,
            "piece-1_30.iso",
            Some("debian-12.4.0-amd64-DVD-1.iso"),
        );
        let expected_file_data = read_test_file_to_bytes("piece-1_30.iso");
        let (tx_state, mut rx_state) = unbounded_channel();
        let source_torrent = create_torrent!(
            "debian-udp.torrent",
            temp_path_source,
            TorrentFlags::UploadMode | TorrentFlags::SeedMode,
            TorrentConfig::builder().build(),
            vec![
                Box::new(TorrentCreatePiecesAndFilesOperation::new()),
                Box::new(TorrentFileValidationOperation::new())
            ],
            vec![Box::new(TcpPeerDiscovery::new().await.unwrap())]
        );
        let target_torrent = create_torrent!(
            "debian-udp.torrent",
            temp_path_target,
            TorrentFlags::DownloadMode | TorrentFlags::Paused,
            TorrentConfig::builder().build(),
            vec![
                Box::new(TorrentStatsOperation::new()),
                Box::new(TorrentConnectPeersOperation::new(false)),
                Box::new(TorrentCreatePiecesAndFilesOperation::new()),
            ],
            vec![Box::new(TcpPeerDiscovery::new().await.unwrap())]
        );

        // initialize the source torrent
        wait_for_torrent_pieces(&source_torrent).await;
        wait_for_torrent_state(
            &source_torrent,
            TorrentState::Seeding,
            Duration::from_secs(10),
        )
        .await;

        // initialize the target torrent
        wait_for_torrent_pieces(&target_torrent).await;

        // only request the first X amount of pieces
        let total_pieces = target_torrent.total_pieces().await;
        target_torrent
            .prioritize_pieces(
                (num_of_pieces..total_pieces)
                    .into_iter()
                    .map(|piece| (piece, PiecePriority::None))
                    .collect(),
            )
            .await;

        // resume the target torrent to fetch data from the source torrent
        target_torrent.resume().await;

        // listen to the finished event
        let target_handle = target_torrent.handle();
        let mut receiver = target_torrent.subscribe();
        tokio::spawn(async move {
            while let Some(event) = receiver.recv().await {
                match &*event {
                    TorrentEvent::StateChanged(state) => {
                        if state == &TorrentState::Finished {
                            let _ = tx_state.send(());
                            break;
                        }
                    }
                    TorrentEvent::Stats(stats) => {
                        debug!("Torrent {} stats: {}", target_handle, stats);
                    }
                    _ => {}
                }
            }
        });

        // connect the target torrent to the source torrent
        // do not connect the source torrent to the target, as the source torrent is seeding and won't actively create new connections
        target_torrent
            .add_peer(SocketAddr::from((
                [127, 0, 0, 1],
                source_torrent.peer_port().await.unwrap(),
            )))
            .await
            .expect("expected the peer address to have been added");

        // wait for all pieces to be completed (finished state)
        timeout!(
            rx_state.recv(),
            Duration::from_secs(90),
            "expected the torrent to enter the FINISHED state"
        )
        .unwrap();

        // validate the pieces and received data
        let data_pool = target_torrent.inner.data_pool().await.unwrap();
        let pieces = target_torrent.pieces().await.unwrap();
        let pieces_bitfield = data_pool.bitfield().await;

        for piece in &pieces[0..num_of_pieces] {
            let piece_index = piece.index;
            assert_eq!(
                true,
                piece.is_completed(),
                "expected piece {} to have been completed",
                piece_index
            );
            assert_eq!(
                Some(true),
                pieces_bitfield.get(piece_index),
                "expected piece bitfield bit {} to be set",
                piece_index
            );
        }

        // read the torrent file
        match target_torrent.read_file_to_end(&0).await {
            Ok((bytes_read, bytes)) => {
                assert_eq!(
                    expected_file_data.len(),
                    bytes_read,
                    "expected the available data to have been read"
                );
                assert_eq!(
                    expected_file_data, bytes,
                    "expected the available data to have been read"
                );
            }
            Err(e) => assert!(false, "failed to read torrent data, {}", e),
        }
    }

    // FIXME: unstable in Github actions
    #[ignore]
    #[tokio::test]
    async fn test_torrent_is_completed() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        copy_test_file(
            temp_path,
            "piece-1_30.iso",
            Some("debian-12.4.0-amd64-DVD-1.iso"),
        );
        let torrent = create_torrent!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![
                Box::new(TorrentCreatePiecesAndFilesOperation::new()),
                Box::new(TorrentFileValidationOperation::new()),
            ]
        );
        let (tx, mut rx) = unbounded_channel();

        let mut receiver = torrent.subscribe();
        tokio::spawn(async move {
            while let Some(event) = receiver.recv().await {
                if let TorrentEvent::StateChanged(state) = &*event {
                    if state != &TorrentState::Initializing && state != &TorrentState::CheckingFiles
                    {
                        tx.send(()).unwrap();
                    }
                }
            }
        });

        // wait for the expected state
        timeout!(
            rx.recv(),
            Duration::from_secs(8),
            "expected the torrent to be initialized"
        )
        .unwrap();

        // prioritize the first 30 pieces
        let total_pieces = torrent.total_pieces().await;
        let priorities = (30..total_pieces)
            .into_iter()
            .map(|i| (i, PiecePriority::None))
            .collect();
        torrent.prioritize_pieces(priorities).await;

        let result = torrent.is_completed().await;
        assert_eq!(true, result, "expected the torrent to be completed");
    }

    #[tokio::test]
    async fn test_torrent_is_download_allowed() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (mut context, _) = create_torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build()
        );

        // create the pieces and files for the torrent
        let mut operation = TorrentCreatePiecesAndFilesOperation::new();
        operation.execute(&mut context, vec![].as_slice()).await;

        let mut receiver = context.subscribe();

        // reset the state to Initializing
        context.update_state(TorrentState::Initializing);
        let result = context.is_download_allowed();
        assert_eq!(false, result, "expected downloading to not be allowed");

        let result = async {
            context.add_options(TorrentFlags::DownloadMode);
            // wait for the state change event
            let _ = receiver.recv().await;
            context.is_download_allowed()
        }
        .await;
        assert_eq!(false, result, "expected downloading to not be allowed");

        let result = async {
            context.update_state(TorrentState::Finished);
            context.is_download_allowed()
        }
        .await;
        assert_eq!(true, result, "expected downloading to be allowed");

        let result = async {
            context.add_options(TorrentFlags::Paused);
            context.is_download_allowed()
        }
        .await;
        assert_eq!(false, result, "expected downloading to not be allowed");
    }

    #[tokio::test]
    async fn test_torrent_is_upload_allowed() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (mut context, _) = create_torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::UploadMode,
            TorrentConfig::builder().build()
        );

        // create pieces and files for the torrent
        let mut operation = TorrentCreatePiecesAndFilesOperation::new();
        operation.execute(&mut context, vec![].as_slice()).await;

        // validate the existing files
        let mut operation = TorrentFileValidationOperation::new();
        operation.execute(&mut context, vec![].as_slice()).await;

        let result = context.is_upload_allowed();
        assert_eq!(true, result, "expected uploading to be allowed");

        context.add_options(TorrentFlags::Paused);
        let result = context.is_upload_allowed();
        assert_eq!(false, result, "expected uploading to not be allowed");

        context.remove_options(TorrentFlags::Paused | TorrentFlags::UploadMode);
        let result = context.is_upload_allowed();
        assert_eq!(false, result, "expected uploading to not be allowed");
    }

    #[tokio::test]
    async fn test_torrent_is_end_game() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let pieces_len = 100;
        let piece_size = 128;
        let torrent = create_torrent!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            vec![]
        );
        let data_pool = torrent.inner.data_pool().await.unwrap();

        // set the pieces of the torrent
        data_pool
            .set_pieces(
                (0..pieces_len)
                    .into_iter()
                    .map(|index| Piece {
                        hash: Default::default(),
                        index,
                        offset: index * piece_size,
                        length: piece_size,
                        priority: Default::default(),
                        parts: vec![],
                        completed_parts: Default::default(),
                        availability: 0,
                    })
                    .collect::<Vec<_>>(),
            )
            .await;

        let total_pieces = torrent.total_pieces().await;
        assert_ne!(0, total_pieces, "expected the pieces to have been created");

        let result = data_pool.is_end_game().await;
        assert_eq!(
            false, result,
            "expected the torrent to not be in the end-game phase"
        );

        let completed_range_1 = (total_pieces as f64 * 0.90) as usize;
        for piece in (0..completed_range_1).into_iter().map(|e| e as PieceIndex) {
            let _ = torrent.inner.piece_completed(&piece).await;
        }

        let result = data_pool.is_end_game().await;
        assert_eq!(
            false, result,
            "expected the torrent to not be in the end-game phase"
        );

        let completed_range_2 = (total_pieces as f64 * 0.98) as usize;
        for piece in (completed_range_1..completed_range_2)
            .into_iter()
            .map(|e| e as PieceIndex)
        {
            let _ = torrent.inner.piece_completed(&piece).await;
        }

        let result = data_pool.is_end_game().await;
        assert_eq!(
            true, result,
            "expected the torrent to be in the end-game phase"
        );
    }

    #[tokio::test]
    async fn test_torrent_determine_state() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let pieces = vec![Piece::new(
            InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap(),
            0,
            0,
            1024,
        )];
        let (mut context, _) = create_torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build()
        );

        let result = context.determine_state().await;
        assert_eq!(TorrentState::Initializing, result);

        context.update_pieces(pieces).await;
        let result = context.determine_state().await;
        assert_eq!(TorrentState::Finished, result);

        context.add_options(TorrentFlags::UploadMode);
        let result = context.determine_state().await;
        assert_eq!(TorrentState::Seeding, result);

        context.remove_options(TorrentFlags::UploadMode);
        context.add_options(TorrentFlags::DownloadMode);
        context.update_state(TorrentState::Paused);
        let result = context.determine_state().await;
        assert_eq!(TorrentState::Downloading, result);
    }

    #[tokio::test]
    async fn test_torrent_wanted_pieces() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (mut context, _) = create_torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build()
        );

        // create the torrent pieces
        let mut operation = TorrentCreatePiecesAndFilesOperation::new();
        operation.execute(&mut context, vec![].as_slice()).await;

        let total_pieces = context.data_pool().num_of_pieces().await;
        assert_ne!(0, total_pieces, "expected the pieces to have been created");

        context
            .prioritize_pieces(
                (30..total_pieces)
                    .into_iter()
                    .map(|piece| (piece, PiecePriority::None))
                    .collect(),
            )
            .await;

        let expected_result: Vec<PieceIndex> = (0..30)
            .into_iter()
            .map(|piece| piece as PieceIndex)
            .collect();
        let result = context
            .data_pool
            .wanted_pieces()
            .await
            .into_iter()
            .map(|e| e.index)
            .collect::<Vec<_>>();
        assert_eq!(expected_result, result);

        context
            .pieces_completed((0..2).into_iter().map(|e| e as PieceIndex).collect())
            .await;
        let expected_result: Vec<PieceIndex> = (2..30)
            .into_iter()
            .map(|piece| piece as PieceIndex)
            .collect();
        let result = context
            .data_pool
            .wanted_pieces()
            .await
            .into_iter()
            .map(|e| e.index)
            .collect::<Vec<_>>();
        assert_eq!(expected_result, result);
    }

    #[tokio::test]
    async fn test_torrent_wanted_request_pieces() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (mut context, _) = create_torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::DownloadMode,
            TorrentConfig::builder().build()
        );

        // wait for pieces
        let mut operation = TorrentCreatePiecesAndFilesOperation::new();
        operation.execute(&mut context, vec![].as_slice()).await;

        let total_pieces = context.data_pool.num_of_pieces().await;
        assert_ne!(0, total_pieces, "expected the pieces to have been created");

        context
            .prioritize_pieces(
                (100..total_pieces)
                    .into_iter()
                    .map(|piece| (piece, PiecePriority::None))
                    .collect(),
            )
            .await;

        // acquire some locks
        let permits = async {
            // update the torrent state to a "download allowed" state
            context.update_state(TorrentState::Downloading);
            // start requesting permits
            let mut permits = Vec::new();
            for piece in (0..10).into_iter().map(|e| e as PieceIndex) {
                let permit = context
                    .request_download_permit(&piece)
                    .await
                    .expect(format!("expected to get a permit for {} piece", piece).as_str());
                permits.push(permit);
            }
            permits
        }
        .await;
        assert_eq!(10, permits.len(), "expected to acquire 10 permits");

        let expected_wanted_pieces: Vec<PieceIndex> =
            (10..100).into_iter().map(|e| e as PieceIndex).collect();
        let wanted_pieces = context
            .wanted_request_pieces()
            .await
            .into_iter()
            .map(|e| e.index)
            .collect::<Vec<_>>();
        assert_eq!(expected_wanted_pieces, wanted_pieces);

        // update a piece 0 to have timed out
        context
            .pending_piece_requests
            .insert(0, Instant::now().sub(Duration::from_secs(120)));
        let wanted_pieces = context.wanted_request_pieces().await;
        assert_eq!(
            Some(0),
            wanted_pieces.get(0).as_ref().map(|e| e.index),
            "expected piece 0 to be requested again after timeout"
        );
    }

    #[tokio::test]
    async fn test_torrent_update_state() {
        init_logger!();
        let expected_state = TorrentState::Paused;
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (tx, mut rx) = unbounded_channel();
        let (mut context, _) = create_torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![]
        );

        // subscribe to the events of the torrent
        let mut receiver = context.subscribe();
        tokio::spawn(async move {
            loop {
                if let Some(event) = receiver.recv().await {
                    if let TorrentEvent::StateChanged(state) = &*event {
                        tx.send(state.clone()).unwrap();
                        break;
                    }
                } else {
                    break;
                }
            }
        });

        context.update_state(expected_state);

        let result = timeout!(
            rx.recv(),
            Duration::from_millis(200),
            "expected a state change event"
        )
        .unwrap();
        assert_eq!(
            expected_state, result,
            "expected the state change event to match the new state"
        );

        let result = context.state();
        assert_eq!(
            &expected_state, result,
            "expected the state function to match the new state"
        );
    }

    mod prioritize {
        use super::*;
        use crate::tests::helpers::wait_for_torrent_pieces;
        use crate::FilePriority;

        #[tokio::test]
        async fn test_torrent_prioritize_pieces() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let torrent = create_torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![Box::new(TorrentCreatePiecesAndFilesOperation::new())]
            );

            // create the pieces
            wait_for_torrent_pieces(&torrent).await;

            // only request the first piece
            let mut priorities = torrent.piece_priorities().await;
            priorities.insert(8, PiecePriority::High);
            priorities.insert(9, PiecePriority::High);
            let priorities = priorities
                .into_iter()
                .map(|(i, priority)| {
                    if i < 10 {
                        (i, priority)
                    } else {
                        (i, PiecePriority::None)
                    }
                })
                .collect();

            torrent.prioritize_pieces(priorities).await;

            // check the new priorities of the pieces
            let result = torrent
                .pieces()
                .await
                .expect("expected the pieces to be present");
            for piece in 0..8 {
                let priority = PiecePriority::Normal;
                assert_eq!(
                    priority, result[piece].priority,
                    "expected piece {} to have priority {:?}",
                    piece, priority
                );
            }
            for piece in 9..10 {
                let priority = PiecePriority::High;
                assert_eq!(
                    priority, result[piece].priority,
                    "expected piece {} to have priority {:?}",
                    piece, priority
                );
            }
            for piece in 10..20 {
                let priority = PiecePriority::None;
                assert_eq!(
                    priority, result[piece].priority,
                    "expected piece {} to have priority {:?}",
                    piece, priority
                );
            }

            // check the wanted pieces
            let expected_wanted_pieces = vec![8, 9, 0, 1, 2, 3, 4, 5, 6, 7];
            let data_pool = torrent.inner.data_pool().await.unwrap();
            let result = data_pool
                .wanted_pieces()
                .await
                .into_iter()
                .map(|e| e.index)
                .collect::<Vec<_>>();
            assert_eq!(
                expected_wanted_pieces, result,
                "expected only piece 0 to be wanted"
            );

            // check the interested pieces
            let expected_interested_pieces = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            let result = data_pool.interested_pieces().await;
            assert_eq!(
                expected_interested_pieces, result,
                "expected only piece 0 to be interested"
            );
        }

        #[tokio::test]
        async fn test_prioritize_bytes() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let mut operation = TorrentCreatePiecesAndFilesOperation::new();
            let (mut context, _) = create_torrent_context!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![]
            );
            let piece_length = context
                .metadata()
                .info
                .as_ref()
                .map(|e| e.piece_length)
                .unwrap() as usize;
            let range = 0usize..(2 * piece_length);

            // create the torrent pieces
            operation.execute(&mut context, vec![].as_slice()).await;

            // prioritize the first 2 pieces through the bytes
            context.prioritize_bytes(&range, PiecePriority::High).await;

            let priorities = context.data_pool.piece_priorities().await;
            assert_eq!(Some(&PiecePriority::High), priorities.get(&0));
            assert_eq!(Some(&PiecePriority::High), priorities.get(&1));
            assert_eq!(Some(&PiecePriority::Normal), priorities.get(&2));
        }

        #[tokio::test]
        async fn test_prioritize_files() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let torrent = create_torrent!(
                "multifile.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![Box::new(TorrentCreatePiecesAndFilesOperation::new())]
            );

            // create the pieces and files of the torrent
            wait_for_torrent_pieces(&torrent).await;

            // prioritize only the 2nd file
            let file_priorities = torrent
                .files()
                .await
                .into_iter()
                .map(|file| {
                    if file.index == 1 {
                        (file.index, FilePriority::Normal)
                    } else {
                        (file.index, FilePriority::None)
                    }
                })
                .collect();
            torrent.prioritize_files(file_priorities).await;

            let priorities = torrent.piece_priorities().await;

            // verify that file 0 is ignored, except for the last piece
            for piece in 0usize..401usize {
                assert_eq!(
                    Some(&PiecePriority::None),
                    priorities.get(&piece),
                    "expected the first file (piece {}) to be ignored",
                    piece
                );
            }
            assert_eq!(Some(&PiecePriority::Normal), priorities.get(&401));
            // check that file 1 is wanted
            for piece in 402usize..725usize {
                assert_eq!(
                    Some(&PiecePriority::Normal),
                    priorities.get(&piece),
                    "expected the second file (piece {}) to be wanted",
                    piece
                );
            }
            // check that the remaining files are ignored
            for piece in 725usize..priorities.len() {
                assert_eq!(
                    Some(&PiecePriority::None),
                    priorities.get(&piece),
                    "expected the remaining files (piece {}) to be ignored",
                    piece
                );
            }
        }
    }
}
