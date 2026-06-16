use crate::channel::{ChannelReceiver, ChannelSender, Reply};
use crate::config::TorrentConfig;
#[cfg(feature = "dht")]
use crate::dht::DhtTracker;
use crate::file::File;
use crate::operation::{Operation, TorrentOperationResult};
use crate::peer::extension::PeerExtension;
use crate::peer::{
    BitTorrentPeer, CloseReason, ConnectionProtocol, Peer, PeerClientInfo, PeerDiscovery,
    PeerEntry, PeerHandle, PeerId, ProtocolExtensionFlags,
};
use crate::peer_pool::PeerPool;
use crate::piece_picker::strategy::{
    PriorityStrategy, RarestFirstStrategy, SequentialStrategy, SuggestedOnlyStrategy,
};
use crate::piece_picker::{FxPiecePicker, PickerOptions, PiecePicker};
use crate::storage::{Storage, StorageParams};
use crate::torrent_data::DataPool;
use crate::tracker::{AnnounceEvent, AnnouncementResult, TrackerClient};
#[cfg(feature = "lsd")]
use crate::LocalServiceDiscovery;
use crate::TorrentTracker;
use crate::{BitVec, Result};
use crate::{
    FileAttributeFlags, FileIndex, InfoHash, Metrics, Piece, PieceBlock, PieceIndex, PiecePriority,
    Sha1Hash, Sha256Hash, TorrentError, TorrentFlags, TorrentMetadata, TorrentMetadataInfo,
    DEFAULT_TORRENT_PROTOCOL_EXTENSIONS,
};
use crate::{FileStream, TorrentHandle};
use derive_more::Display;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use fx_callback::{Callback, MultiThreadedCallback, Subscription};
use itertools::Itertools;
use log::{debug, info, trace};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot::Sender;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;
use tokio::{select, time};
use tokio_util::sync::{
    CancellationToken, WaitForCancellationFuture, WaitForCancellationFutureOwned,
};
use url::Url;

const TICK_INTERVAL: Duration = Duration::from_secs(1);

/// A [Torrent] extension factory.
/// This factory will create a new instance of an [Extension] for each new torrent.
pub type ExtensionFactory = fn() -> PeerExtension;

/// Factory type for creating a new [Storage] instance.
pub type StorageFactory = dyn FnOnce(StorageParams) -> Storage + Send + Sync;

/// Factory type for creating a new [PiecePicker] instance.
pub type PiecePickerFactory =
    dyn Fn(InnerTorrent, DataPool, Storage, PickerOptions) -> PiecePicker + Send + Sync;

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
/// ```rust
/// # use std::time::Duration;
/// # use fx_torrent::{Torrent, TorrentFlags, TorrentMetadata, TorrentRequest, MagnetResult, ExtensionFactories, CompactResult};
/// # use fx_torrent::storage::{DiskStorage};
/// # use fx_torrent::peer::extension::Extensions;
/// # use fx_torrent::peer::{PeerDiscovery, TcpPeerDiscovery};
/// # use fx_torrent::tracker::TrackerClient;
///
/// # fn create_new_torrent(
/// #     metadata: TorrentMetadata,
/// #     extensions: ExtensionFactories,
/// # ) -> CompactResult<Torrent> {
///     // create a tcp peer discovery for dialing and accepting tpc connections
///     let peer_discovery = TcpPeerDiscovery::new();
///     // create a new tracker client for discovering peer addresses
///     let tracker = TrackerClient::new(Duration::from_secs(6));
///
///     Torrent::request()
///         .metadata(metadata)
///         .options(TorrentFlags::AutoManaged)
///         .extensions(extensions)
///         .storage(|params| {
///             Box::new(DiskStorage::new(params.info_hash, params.path, params.files))
///         })
///         .peer_discovery(Box::new(peer_discovery))
///         .tracker(tracker.into())
///         .build()
/// # }
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
    peer_discoveries: Vec<PeerDiscovery>,
    /// The protocol extensions that should be enabled
    protocol_extensions: Option<ProtocolExtensionFlags>,
    /// The factories for creating the peer extensions that should be enabled for this torrent
    extensions: Vec<ExtensionFactory>,
    /// The storage strategy to use for the torrent data
    storage: Option<Box<StorageFactory>>,
    /// The operations used by the torrent for processing data
    operations: Option<Vec<Operation>>,
    /// The trackers of the torrent to use.
    trackers: Vec<TorrentTracker>,
    /// The piece picker factory to use for the torrent.
    piece_picker: Option<Box<PiecePickerFactory>>,
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
    pub fn peer_discovery(&mut self, dialer: PeerDiscovery) -> &mut Self {
        self.peer_discoveries.push(dialer);
        self
    }

    /// Set the given peer dialers of the torrent.
    /// This overrides any existing configured peer dialers.
    ///
    /// ## Remark
    ///
    /// The order of the dialers are important for outgoing connections.
    pub fn peer_discoveries(&mut self, discoveries: Vec<PeerDiscovery>) -> &mut Self {
        self.peer_discoveries = discoveries;
        self
    }

    /// Set the protocol extensions that should be enabled
    pub fn protocol_extensions(&mut self, extensions: ProtocolExtensionFlags) -> &mut Self {
        self.protocol_extensions = Some(extensions);
        self
    }

    /// Add the given extension factory that should be activated.
    pub fn extension(&mut self, extension: ExtensionFactory) -> &mut Self {
        self.extensions.push(extension);
        self
    }

    /// Set the extension factories that should be activated for this torrent.
    /// This overrides any previously set extensions.
    pub fn extensions(&mut self, extensions: Vec<ExtensionFactory>) -> &mut Self {
        self.extensions = extensions;
        self
    }

    /// Set the underlying storage for storing the torrent file data.
    pub fn storage<F>(&mut self, storage: F) -> &mut Self
    where
        F: FnOnce(StorageParams) -> Storage + Send + Sync + 'static,
    {
        self.storage = Some(Box::new(storage));
        self
    }

    /// Add the operation to the torrent for processing data.
    pub fn operation(&mut self, operation: Operation) -> &mut Self {
        self.operations.get_or_insert(Vec::new()).push(operation);
        self
    }

    /// Set the operations used by the torrent for processing data
    pub fn operations(&mut self, operations: Vec<Operation>) -> &mut Self {
        self.operations = Some(operations);
        self
    }

    /// Add the given tracker to the torrent.
    pub fn tracker(&mut self, tracker: TorrentTracker) -> &mut Self {
        self.trackers.push(tracker);
        self
    }

    /// Set the trackers of the torrent to use.
    /// This will override any existing configured trackers.
    pub fn trackers(&mut self, trackers: Vec<TorrentTracker>) -> &mut Self {
        self.trackers = trackers;
        self
    }

    /// Set the piece picker factory to use for the torrent.
    pub fn piece_picker<F>(&mut self, picker: F) -> &mut Self
    where
        F: Fn(InnerTorrent, DataPool, Storage, PickerOptions) -> PiecePicker
            + Send
            + Sync
            + 'static,
    {
        self.piece_picker = Some(Box::new(picker));
        self
    }

    /// Build the torrent from the given data.
    /// This is the same as calling `Torrent::try_from(self)`.
    pub fn build(&mut self) -> Result<Torrent> {
        Torrent::try_from(self)
    }

    /// Get the list of default operations for the torrent.
    pub fn default_operations() -> Vec<Operation> {
        Operation::default_operations()
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
            .field("extensions", &self.extensions)
            .field("operations", &self.operations)
            .field("trackers", &self.trackers)
            .finish()
    }
}

impl TryFrom<&mut TorrentRequest> for Torrent {
    type Error = TorrentError;

    fn try_from(request: &mut TorrentRequest) -> Result<Self> {
        let metadata = request.metadata.take().ok_or(TorrentError::InvalidRequest(
            "metadata is missing".to_string(),
        ))?;
        let peer_discoveries = std::mem::take(&mut request.peer_discoveries);
        let protocol_extensions = request
            .protocol_extensions
            .unwrap_or_else(DEFAULT_TORRENT_PROTOCOL_EXTENSIONS);
        let extensions = std::mem::take(&mut request.extensions);
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
        let trackers = std::mem::take(&mut request.trackers);
        let piece_picker = std::mem::take(&mut request.piece_picker).unwrap_or_else(|| {
            Box::new(
                |torrent: InnerTorrent,
                 data_pool: DataPool,
                 storage: Storage,
                 options: PickerOptions| {
                    FxPiecePicker::new(
                        torrent,
                        // TODO: limit max number of in-flight pieces
                        data_pool,
                        storage,
                        vec![
                            RarestFirstStrategy::new().into(),
                            SuggestedOnlyStrategy::new().into(),
                            SequentialStrategy::new().into(),
                            PriorityStrategy::new().into(),
                        ],
                        32 * 1024 * 1024, // 32MB, TODO: make this configurable
                        options,
                    )
                    .into()
                },
            )
        });

        Ok(Self::new(
            metadata,
            peer_discoveries,
            protocol_extensions,
            extensions,
            options,
            config,
            data_pool,
            storage(storage_params),
            operations,
            trackers,
            piece_picker,
        ))
    }
}

/// The result metrics from a tracker scrape.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ScrapeMetrics {
    /// The number of active peers that have completed downloading.
    pub complete: u32,
    /// The number of active peers that have not completed downloading.
    pub incomplete: u32,
    /// The number of peers that have ever completed downloading.
    pub downloaded: u32,
}

impl FromIterator<ScrapeMetrics> for ScrapeMetrics {
    fn from_iter<T: IntoIterator<Item = ScrapeMetrics>>(iter: T) -> Self {
        let mut result = Self::default();
        for metrics in iter {
            result.complete += metrics.complete;
            result.incomplete += metrics.incomplete;
            result.downloaded += metrics.downloaded;
        }
        result
    }
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
        peer_discoveries: Vec<PeerDiscovery>,
        protocol_extensions: ProtocolExtensionFlags,
        extensions: Vec<ExtensionFactory>,
        options: TorrentFlags,
        config: TorrentConfig,
        data_pool: DataPool,
        storage: Storage,
        operations: Vec<Operation>,
        trackers: Vec<TorrentTracker>,
        piece_picker: Box<PiecePickerFactory>,
    ) -> Self {
        let handle = TorrentHandle::new();
        let callbacks = MultiThreadedCallback::new();
        let info_hash = metadata.info_hash.clone();
        let (command_sender, command_receiver) = channel!(1024);
        let location = config.path().to_path_buf();
        let piece_picker = piece_picker(
            InnerTorrent {
                handle,
                sender: command_sender.clone(),
                callbacks: callbacks.clone(),
            },
            data_pool.clone(),
            storage.clone(),
            PickerOptions::Priority,
        );
        let mut context = TorrentContext::new(
            handle,
            metadata,
            config,
            peer_discoveries.first().map(|e| e.addr().port()),
            protocol_extensions,
            extensions,
            options,
            data_pool,
            trackers,
            storage,
            piece_picker,
            command_sender,
            callbacks,
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
                .run(operations, command_receiver, peer_discoveries)
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
    pub async fn prioritize_bytes(&self, bytes: &Range<usize>, priority: PiecePriority) {
        self.inner.prioritize_bytes(bytes, priority).await;
    }

    /// Returns `true` if the given byte range is available in the torrent, else `false`.
    /// This means that the bytes have been downloaded, written, and validated.
    pub async fn has_bytes(&self, range: &Range<usize>) -> bool {
        self.inner.has_bytes(range).await
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

    /// Returns the [File] for the given torrent file index, if found.
    pub async fn file(&self, file: &FileIndex) -> Option<File> {
        self.inner.file(file).await
    }

    /// Returns the [File] for the given torrent file name, if found.
    pub async fn file_by_name(&self, name: &str) -> Option<File> {
        self.inner.file_by_name(name).await
    }

    /// Stream the given file from the torrent.
    /// It returns an error if the file stream couldn't be started.
    pub async fn stream(&self, file: &File) -> Result<FileStream> {
        self.inner.stream(&file.index).await
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

        Ok(self
            .inner
            .sender
            .send(|tx| TorrentCommand::ScrapeAll { response: tx })
            .await
            .await?)
    }

    /// Returns an existing peer from the peer pool by the given handle.
    /// The returned instance is a weak reference that can be dropped by the pool at any time.
    ///
    /// ## Remark
    ///
    /// Before calling a method,
    /// make sure to check if the reference is still valid by calling [Peer::is_valid].
    pub async fn peer(&self, handle: &PeerHandle) -> Option<Peer> {
        self.inner.peer(handle).await
    }

    /// Returns an existing peer from the peer pool by the given address.
    /// The returned instance is a weak reference that can be dropped by the pool at any time.
    ///
    /// ## Remark
    ///
    /// Before calling a method,
    /// make sure to check if the reference is still valid by calling [Peer::is_valid].
    pub async fn peer_by_addr(&self, addr: &SocketAddr) -> Option<Peer> {
        self.inner.peer_by_addr(addr).await
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
        range: Range<usize>,
    ) -> Result<Vec<u8>> {
        self.inner.read_piece_bytes(piece, range).await
    }

    /// Try to read the bytes from the given torrent file.
    /// This reads all available bytes of the file stored within the [StorageExtension].
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
                    Ok(event) = receiver.recv() => {
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

    /// Returns an existing peer from the pool by the given handle.
    /// The returned instance is a weak reference that can be dropped by the pool at any time.
    pub async fn peer(&self, handle: &PeerHandle) -> Option<Peer> {
        self.sender
            .send(|tx| TorrentCommand::GetPeer {
                handle: *handle,
                response: tx,
            })
            .await
            .await
            .ok()
            .flatten()
    }

    /// Returns an existing peer from the pool by the given address.
    /// The returned instance is a weak reference that can be dropped by the pool at any time.
    pub async fn peer_by_addr(&self, addr: &SocketAddr) -> Option<Peer> {
        self.sender
            .send(|tx| TorrentCommand::GetPeerByAddr {
                addr: *addr,
                response: tx,
            })
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

    /// Returns the total number of peer addresses known in the peer pool.
    pub async fn peer_addrs_len(&self) -> usize {
        self.sender
            .send(|tx| TorrentCommand::NumOfPeerAddrs { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Notify the torrent that a peer has connected.
    pub async fn peer_connected(&self, peer: Peer) {
        self.sender
            .fire_and_forget(TorrentCommand::PeerConnected { peer })
            .await
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

    /// Update the availability of the given pieces for the torrent.
    pub async fn piece_availabilities(&self, pieces: Vec<PieceIndex>, available: bool) {
        self.sender
            .fire_and_forget(TorrentCommand::PieceAvailabilities { pieces, available })
            .await
    }

    /// Prioritize the given bytes within the torrent.
    pub async fn prioritize_bytes(&self, bytes: &Range<usize>, priority: PiecePriority) {
        let _ = self
            .sender
            .send(|tx| TorrentCommand::PrioritizeBytes {
                bytes: bytes.clone(),
                priority,
                response: tx,
            })
            .await
            .await;
    }

    /// Returns `true` if the given byte range is available in the torrent, else `false`.
    pub async fn has_bytes(&self, range: &Range<usize>) -> bool {
        self.sender
            .send(|tx| TorrentCommand::HasBytes {
                bytes: range.clone(),
                response: tx,
            })
            .await
            .await
            .unwrap_or_default()
    }

    /// Read the given torrent byte range.
    pub async fn read_bytes(&self, range: &Range<usize>) -> Result<Vec<u8>> {
        self.sender
            .send(|tx| TorrentCommand::ReadBytes {
                range: range.clone(),
                response: tx,
            })
            .await
            .await
    }

    /// Returns the files of the torrent, if the metadata is known (see [Torrent::is_metadata_known]).
    /// If the metadata is still being retrieved, the returned files array will be empty.
    pub async fn files(&self) -> Vec<File> {
        self.sender
            .send(|tx| TorrentCommand::Files { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the torrent file for the given index, if available.
    pub async fn file(&self, file: &FileIndex) -> Option<File> {
        self.sender
            .send(|tx| TorrentCommand::File {
                file: *file,
                response: tx,
            })
            .await
            .await
            .ok()
            .flatten()
    }

    /// Returns the torrent file for the given index, if available.
    pub async fn file_by_name(&self, name: &str) -> Option<File> {
        self.sender
            .send(|tx| TorrentCommand::FileByName {
                name: name.to_string(),
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
            .send(|tx| TorrentCommand::FileIndexFor {
                piece: *piece,
                response: tx,
            })
            .await
            .await
            .ok()
            .flatten()
    }

    /// Stream the given file from the torrent.
    pub async fn stream(&self, file: &FileIndex) -> Result<FileStream> {
        self.sender
            .send(|tx| TorrentCommand::FileStream {
                file: *file,
                response: tx,
            })
            .await
            .await
    }

    /// Returns the extensions of the torrent.
    pub async fn extensions(&self, protocol: ConnectionProtocol) -> Result<Vec<PeerExtension>> {
        Ok(self
            .sender
            .send(|tx| TorrentCommand::Extensions {
                protocol,
                response: tx,
            })
            .await
            .await?)
    }

    /// Request the torrent piece picker to pick pieces for the given peer.
    pub async fn pick_pieces(&self, peer: &PeerHandle) {
        self.sender
            .fire_and_forget(TorrentCommand::PickPieces { peer: *peer })
            .await;
    }

    /// Inform the torrent that the given piece has been verified.
    pub async fn piece_verified(
        &self,
        piece: &PieceIndex,
        v1_hash: Option<Sha1Hash>,
        v2_hash: Option<Sha256Hash>,
    ) {
        self.sender
            .fire_and_forget(TorrentCommand::PieceVerified {
                piece: *piece,
                v1_hash,
                v2_hash,
            })
            .await;
    }

    /// Process a received piece block from a peer.
    pub async fn piece_block_received<T: Into<Vec<u8>>>(
        &self,
        peer: &PeerHandle,
        block: &PieceBlock,
        data: T,
    ) {
        self.sender
            .fire_and_forget(TorrentCommand::PieceBlockReceived {
                peer: *peer,
                block: *block,
                data: data.into(),
            })
            .await;
    }

    /// Inform the torrent that a piece block request has been rejected by the peer.
    pub async fn piece_block_rejected(&self, peer: &PeerHandle, block: &PieceBlock) {
        self.sender
            .fire_and_forget(TorrentCommand::PieceBlockRejected {
                peer: *peer,
                block: *block,
            })
            .await
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
        range: Range<usize>,
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

    /// Returns `true` if the torrent is in the end-game phase, else `false`.
    pub async fn is_end_game(&self) -> bool {
        self.sender
            .send(|tx| TorrentCommand::IsEndGame { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Notify the torrent that a peer's connection is closed.
    pub(crate) async fn peer_closed(&self, addr: SocketAddr, reason: CloseReason) {
        self.sender
            .fire_and_forget(TorrentCommand::PeerClosed { addr, reason })
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
    /// Returns an existing peer from the torrent by the given handle.
    GetPeer {
        handle: PeerHandle,
        response: Reply<Option<Peer>>,
    },
    /// Returns an existing peer from the torrent by the given peer address.
    GetPeerByAddr {
        addr: SocketAddr,
        response: Reply<Option<Peer>>,
    },
    /// Inform that a new peer connection has been established.
    PeerConnected {
        peer: Peer,
    },
    DecreasePeerPriority {
        addrs: Vec<SocketAddr>,
    },
    PeerClosed {
        addr: SocketAddr,
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
    /// Returns the total number of completed pieces in the torrent.
    NumOfCompletedPieces {
        response: Reply<usize>,
    },
    /// Returns the total number of known peer addresses in the peer pool.
    NumOfPeerAddrs {
        response: Reply<usize>,
    },
    /// Returns the total number of active peer connections in the peer pool.
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
    /// Returns all wanted pieces of the torrent that have not yet been completed.
    WantedPieces {
        response: Reply<Vec<Piece>>,
    },
    /// Update the availability of the given pieces for the torrent.
    PieceAvailabilities {
        pieces: Vec<PieceIndex>,
        available: bool,
    },
    Files {
        response: Reply<Vec<File>>,
    },
    File {
        file: FileIndex,
        response: Reply<Option<File>>,
    },
    FileByName {
        name: String,
        response: Reply<Option<File>>,
    },
    FileIndexFor {
        piece: PieceIndex,
        response: Reply<Option<FileIndex>>,
    },
    FileStream {
        file: FileIndex,
        response: Reply<Result<FileStream>>,
    },
    HasPiece {
        piece: PieceIndex,
        response: Reply<bool>,
    },
    HasBytes {
        bytes: Range<usize>,
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
        bytes: Range<usize>,
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
        response: Reply<ScrapeMetrics>,
    },
    ReadPiece {
        piece: PieceIndex,
        response: Reply<Result<Vec<u8>>>,
    },
    ReadPieceBytes {
        piece: PieceIndex,
        bytes: Range<usize>,
        response: Reply<Result<Vec<u8>>>,
    },
    ReadFileToEnd {
        file: FileIndex,
        response: Reply<Result<(usize, Vec<u8>)>>,
    },
    ReadBytes {
        range: Range<usize>,
        response: Reply<Result<Vec<u8>>>,
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
        protocol: ConnectionProtocol,
        response: Reply<Vec<PeerExtension>>,
    },
    Bitfield {
        response: Reply<BitVec>,
    },
    RequestUploadPermit {
        response: Reply<Option<OwnedSemaphorePermit>>,
    },
    PickPieces {
        peer: PeerHandle,
    },
    PieceVerified {
        piece: PieceIndex,
        v1_hash: Option<Sha1Hash>,
        v2_hash: Option<Sha256Hash>,
    },
    PieceBlockReceived {
        peer: PeerHandle,
        block: PieceBlock,
        data: Vec<u8>,
    },
    PieceBlockRejected {
        peer: PeerHandle,
        block: PieceBlock,
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
    IsEndGame {
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
    /// The trackers of the torrent
    trackers: Vec<TorrentTracker>,

    /// The pool of peer connections
    peer_pool: PeerPool,

    /// The pieces of the torrent, these are only known if the metadata is available
    data_pool: DataPool,
    /// The data storage of the torrent.
    storage: Storage,
    /// The piece picker of the torrent.
    piece_picker: PiecePicker,

    /// The permit counter for uploading pieces to remote peers
    request_upload_permits: Arc<Semaphore>,

    /// The immutable enabled protocol extensions for this torrent
    protocol_extensions: ProtocolExtensionFlags,
    /// The immutable peer extension factories for this torrent.
    /// These factories create the extensions for each established peer connection.
    extensions: Vec<ExtensionFactory>,

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
        handle: TorrentHandle,
        metadata: TorrentMetadata,
        config: TorrentConfig,
        peer_port: Option<u16>,
        protocol_extensions: ProtocolExtensionFlags,
        extensions: Vec<ExtensionFactory>,
        options: TorrentFlags,
        data_pool: DataPool,
        trackers: Vec<TorrentTracker>,
        storage: Storage,
        piece_picker: PiecePicker,
        command_sender: ChannelSender<TorrentCommand>,
        callbacks: MultiThreadedCallback<TorrentEvent>,
    ) -> Self {
        Self {
            handle,
            metadata,
            peer_id: PeerId::new(),
            peer_port,
            trackers,
            peer_pool: PeerPool::new(handle, config.peers_upper_limit),
            data_pool,
            storage,
            piece_picker,
            request_upload_permits: Arc::new(Semaphore::new(config.peers_upload_slots)),
            protocol_extensions,
            extensions,
            state: Default::default(),
            options,
            config,
            metrics: Metrics::new(),
            command_sender,
            callbacks,
            cancellation_token: CancellationToken::new(),
        }
    }

    /// Run the main task loop of the torrent context.
    /// This process is automatically terminated when the `command_receiver` has no more active sender channels.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(crate) async fn run(
        &mut self,
        mut operations: Vec<Operation>,
        mut command_receiver: ChannelReceiver<TorrentCommand>,
        peer_discoveries: Vec<PeerDiscovery>,
    ) {
        let mut operations_tick = time::interval(TICK_INTERVAL);

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
                Some((idx, entry)) = peer_connections.next() => {
                    if let Some(entry) = entry {
                        self.on_incoming_peer_connection(entry).await;
                        let discovery = &peer_discoveries[idx];
                        peer_connections.push(Box::pin(async move {
                            let entry = discovery.recv().await;
                            (idx, entry)
                        }));
                    }
                },
                _ = operations_tick.tick() => self.on_tick(&mut operations, peer_discoveries.as_slice()).await,
            }
        }

        // shutdown the peer pool
        self.peer_pool.shutdown().await;
        // inform the trackers that the torrent has been stopped
        for tracker in self.trackers.iter() {
            tracker
                .announce(
                    &self.metadata.info_hash,
                    self.peer_port().cloned().unwrap_or(6881),
                    AnnounceEvent::Stopped,
                )
                .await;
        }
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

    /// Returns the peer extensions, enabled within the torrent, that support the given protocol.
    /// These extensions should be activated for each established peer connection of the torrent.
    pub fn extensions(&self, protocol: ConnectionProtocol) -> Vec<PeerExtension> {
        self.extensions
            .iter()
            .map(|e| e())
            .filter(|e| e.supports(protocol))
            .collect()
    }

    /// Returns `true` if the specified extension is enabled for the torrent, else `false`.
    pub fn is_extension_enabled(&self, extension: &str) -> bool {
        self.extensions.iter().any(|e| e().name() == extension)
    }

    /// Returns the configured trackers for the torrent.
    pub fn trackers(&self) -> &[TorrentTracker] {
        self.trackers.as_slice()
    }

    /// Returns the DHT tracker for the torrent, if one is configured.
    #[cfg(feature = "dht")]
    pub fn dht(&self) -> Option<&DhtTracker> {
        self.trackers.iter().find_map(|tracker| {
            if let TorrentTracker::Dht(dht) = tracker {
                Some(dht)
            } else {
                None
            }
        })
    }

    /// Returns the local service discovery for the torrent, if one is configured.
    #[cfg(feature = "lsd")]
    pub fn lsd(&self) -> Option<&LocalServiceDiscovery> {
        self.trackers.iter().find_map(|tracker| {
            if let TorrentTracker::Lsd(lsd) = tracker {
                Some(lsd)
            } else {
                None
            }
        })
    }

    /// Returns the tracker client for the torrent, if one is configured.
    pub fn tracker(&self) -> Option<&TrackerClient> {
        self.trackers.iter().find_map(|tracker| {
            if let TorrentTracker::TrackerClient(client) = tracker {
                Some(client)
            } else {
                None
            }
        })
    }

    /// Returns the currently active trackers of the torrent.
    pub async fn active_trackers(&self) -> Vec<Url> {
        match self.tracker() {
            Some(client) => client.tracker_urls().await,
            None => vec![],
        }
    }

    /// Returns the total amount of active tracker connections.
    /// This only counts trackers which have at least made one successful announcement.
    pub async fn active_tracker_connections(&self) -> usize {
        match self.tracker() {
            Some(client) => client.trackers_len().await,
            None => 0,
        }
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
    pub fn active_peer_connections(&self) -> usize {
        self.peer_pool.active_peer_connections()
    }

    /// Returns a reference to the data pool of the torrent.
    pub fn data_pool(&self) -> &DataPool {
        &self.data_pool
    }

    /// Returns a reference to the underlying storage layer of the torrent.
    pub fn storage(&self) -> &Storage {
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

    /// Get the total amount of wanted pieces by the torrent.
    pub async fn total_wanted_pieces(&self) -> usize {
        self.data_pool.wanted_pieces().await.len()
    }

    /// Returns `true` if all bytes are completed, validated, and written to the storage, else `false`.
    pub async fn has_bytes(&self, range: &Range<usize>) -> bool {
        self.data_pool.has_bytes(range.clone()).await
    }

    /// Prioritize the given pieces within this torrent.
    pub async fn prioritize_pieces(&mut self, priorities: Vec<(PieceIndex, PiecePriority)>) {
        trace!("Torrent {} is prioritizing pieces {:?}", self, priorities);
        self.data_pool
            .set_piece_priorities(priorities.as_slice())
            .await;
        priorities
            .iter()
            .for_each(|(piece, priority)| self.piece_picker.set_priority(piece, *priority));
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
    pub async fn prioritize_bytes(&mut self, bytes: &Range<usize>, priority: PiecePriority) {
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

        let currently_active_peers = self.active_peer_connections();
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
    pub async fn find_relevant_pieces_for_bytes(&self, torrent_bytes: &Range<usize>) -> Vec<Piece> {
        self.data_pool
            .pieces()
            .await
            .into_iter()
            .filter(|e| e.contains(torrent_bytes))
            .collect()
    }

    /// Try to find the [PieceBlock] for the given piece and offset.
    /// The offset will be matched against the blocks within the piece.
    pub async fn find_piece_block(&self, piece: PieceIndex, offset: usize) -> Option<PieceBlock> {
        self.data_pool.find_piece_block(&piece, offset).await
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

    /// Stream the given torrent file.
    async fn stream(&self, file: &FileIndex) -> Result<FileStream> {
        let file = match self.data_pool.file(file).await {
            None => {
                return Err(TorrentError::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    "torrent file not found",
                )))
            }
            Some(file) => file,
        };
        let buffer_len = match self.metadata.info.as_ref() {
            None => {
                return Err(TorrentError::InvalidMetadata(
                    "metadata unknown".to_string(),
                ))
            }
            Some(info) => info.piece_length as usize,
        };

        Ok(FileStream::new(
            file,
            buffer_len,
            buffer_len,
            self.storage.clone(),
            self.data_pool.clone(),
            self.command_sender.clone(),
            self.subscribe(),
        ))
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

    /// Returns a list of peer address within the peer pool of the torrent.
    pub async fn discovered_peers(&self) -> Vec<SocketAddr> {
        self.peer_pool.peer_addrs().cloned().collect_vec()
    }

    /// Add the given peer to this torrent.
    /// Duplicate peers will be ignored and dropped.
    fn add_peer(&mut self, peer: Peer) {
        trace!("Torrent {} is trying to add new peer {}", self, peer);
        let info = peer.client_info().clone();
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
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn on_peer_closed(&mut self, addr: SocketAddr, reason: CloseReason) {
        trace!(
            "Torrent {} peer connection closed {:?}, reason: {:?}",
            self,
            addr,
            reason
        );
        let peer = match self.peer_pool.peer_closed(&addr, reason) {
            None => return,
            Some(peer) => peer,
        };

        match timeout(Duration::from_millis(200), peer.remote_piece_bitfield()).await {
            Ok(bitfield) => {
                // decrease the availability of the pieces that the peer had
                for (piece_index, _) in bitfield.iter().enumerate().filter(|(_, value)| **value) {
                    self.data_pool.update_availability(&piece_index, -1).await;
                }
            }
            Err(e) => {
                debug!("Torrent {} failed to collect peer bitfield, {}", self, e);
            }
        }

        self.metrics.peers.dec();
        self.invoke_event(TorrentEvent::PeerDisconnected(peer.client_info().clone()));
    }

    /// Update the given metadata to the torrent.
    /// This method can be used by extensions to update the torrent metadata when the current
    /// connection is based on a magnet link.
    ///
    /// If the data was already known, this method does nothing.
    pub(crate) fn set_metadata(&mut self, metadata_info: TorrentMetadataInfo) {
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

    /// Returns the announce event for the current torrent state.
    pub fn announce_event(&self) -> AnnounceEvent {
        match self.state {
            TorrentState::Paused => AnnounceEvent::Paused,
            TorrentState::Seeding | TorrentState::Finished => AnnounceEvent::Completed,
            TorrentState::Stopped => AnnounceEvent::Stopped,
            _ => AnnounceEvent::Started,
        }
    }

    /// Announce the torrent to all trackers.
    pub fn announce_all(&self, response: Option<Sender<AnnouncementResult>>) {
        let event = self.announce_event();
        let peer_port = self.peer_port().cloned().unwrap_or(6881);
        let info_hash = self.metadata.info_hash.clone();
        let trackers = self.trackers.clone();

        // move the operation to a separate task
        trace!("Torrent {} is announcing {}", self, event);
        tokio::spawn(async move {
            let futures = trackers
                .iter()
                .map(|tracker| tracker.announce(&info_hash, peer_port, event))
                .collect_vec();
            let announcement_result = futures::future::join_all(futures)
                .await
                .into_iter()
                .collect();

            if let Some(response) = response {
                let _ = response.send(announcement_result);
            }
        });
    }

    /// Get the scrape metrics result from scraping all trackers for this torrent.
    pub fn scrape(&self, response: Option<Sender<ScrapeMetrics>>) {
        let handle = self.handle;
        let info_hash = self.metadata.info_hash.clone();
        let trackers = self.trackers.clone();

        // move the operation to a separate task
        trace!("Torrent {} is scraping trackers", self);
        tokio::spawn(async move {
            let futures = trackers
                .iter()
                .map(|tracker| tracker.scrape(&info_hash))
                .collect_vec();
            let scrape_metrics = futures::future::join_all(futures).await.into_iter()
                .filter_map(|result| {
                    match result {
                        Ok(scrape_metrics) => Some(scrape_metrics),
                        Err(e) => {
                            debug!("Torrent {} failed to scrape tracker, {}", handle, e);
                            None
                        }
                    }
                })
                .filter_map(|mut result| {
                    match result.files.remove(&info_hash) {
                        None => {
                            debug!("Torrent {} failed to scrape tracker, info hash {} not found in scrape result", handle, info_hash);
                            None
                        }
                        Some(metrics) => Some(ScrapeMetrics {
                            complete: metrics.complete,
                            incomplete: metrics.incomplete,
                            downloaded: metrics.downloaded,
                        })
                    }
                })
                .collect();

            if let Some(response) = response {
                let _ = response.send(scrape_metrics);
            }
        });
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
        if self.options.contains(TorrentFlags::SequentialDownload) {
            self.piece_picker.add_options(PickerOptions::Sequential);
        }

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
        if !self.options.contains(TorrentFlags::SequentialDownload) {
            self.piece_picker.remove_options(PickerOptions::Sequential);
        }

        self.invoke_event(TorrentEvent::OptionsChanged);
    }

    /// Update the state of this torrent.
    /// If the torrent is already in the given state, this will be a no-op.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub fn update_state(&mut self, state: TorrentState) {
        // check if we're already in the expected state
        // if so, ignore this update
        if self.state == state {
            return;
        }

        self.state = state;
        // inform the trackers about the new state
        self.announce_all(None);

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
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(crate) async fn update_pieces(&mut self, pieces: Vec<Piece>) {
        let start_time = Instant::now();
        let total_pieces = pieces.len();
        self.piece_picker.set_pieces(pieces.as_slice());
        self.data_pool.set_pieces(pieces).await;

        // update the piece availability based on the current peer connections
        let mut availability: BTreeMap<PieceIndex, u32> = BTreeMap::new();
        let mut peer_count = 0u32;

        for peer in self.peer_pool.peers() {
            peer_count += 1;
            match timeout(Duration::from_millis(200), peer.remote_piece_bitfield()).await {
                Ok(bitfield) => {
                    for (piece_index, _) in
                        bitfield.into_iter().enumerate().filter(|(_, value)| *value)
                    {
                        *availability.entry(piece_index).or_insert(0) += 1;
                    }
                }
                Err(e) => {
                    debug!("Torrent {} failed to collect peer bitfield, {}", self, e);
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

        let elapsed = start_time.elapsed();
        debug!(
            "Torrent {} updated {} pieces in {:.3}ms",
            self,
            total_pieces,
            elapsed.as_secs_f64() * 1000.0
        );
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

    /// Pick pieces for the given peer.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn on_pick_pieces(&mut self, peer_handle: PeerHandle) {
        // early exit if downloading pieces is not allowed
        if !self.is_download_allowed() {
            return;
        }
        let peer = match self.peer_pool.get(&peer_handle) {
            None => return,
            Some(peer) => peer,
        };

        self.piece_picker.pick_pieces(peer).await;
    }

    /// Verify the given hash of the piece.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn on_piece_verified(
        &mut self,
        piece_index: &PieceIndex,
        v1_hash: Option<Sha1Hash>,
        v2_hash: Option<Sha256Hash>,
    ) {
        // early exit if the piece couldn't be hashed
        // this is most of the time due to missing data in the storage
        if v1_hash.is_none() && v2_hash.is_none() {
            self.piece_picker.set_failed(piece_index);
            return;
        }

        let piece = match self.data_pool.piece(piece_index).await {
            None => {
                debug!(
                    "Torrent {} failed to verify piece {}, piece not found",
                    self, piece_index
                );
                return;
            }
            Some(piece) => piece,
        };
        let expected_v1 = piece.hash.hash_v1();
        let expected_v2 = piece.hash.hash_v2();

        let validation_result = match (expected_v1, expected_v2) {
            (Some(_), Some(hash_v2)) | (None, Some(hash_v2)) => {
                v2_hash.map(|hash| hash_v2 == hash).unwrap_or(false)
            }
            (Some(hash_v1), None) => v1_hash.map(|hash| hash == hash_v1).unwrap_or(false),
            (None, None) => {
                debug!(
                    "Torrent {} is unable to verify piece {}, piece hash is missing or invalid",
                    self, piece_index
                );
                return;
            }
        };

        if validation_result {
            self.on_piece_completed(&piece).await;
        } else {
            self.piece_picker.set_failed(piece_index);
            self.metrics.wasted.inc_by(piece.length as u64);
        }
    }

    /// Process the completed piece.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn on_piece_completed(&mut self, piece: &Piece) {
        // mark the piece as completed
        self.piece_picker.set_completed(&piece.index);
        self.data_pool.set_completed(&[piece.index], true).await;

        // update the metrics
        if piece.priority != PiecePriority::None {
            self.metrics
                .wanted_completed_size
                .inc_by(piece.length as u64);
            self.metrics.wanted_completed_pieces.inc();
        }

        self.metrics.completed_pieces.inc();
        self.metrics.completed_size.inc_by(piece.length as u64);

        // inform the subscribers
        debug!("Torrent {} piece {} has been completed", self, piece.index);
        self.invoke_event(TorrentEvent::PieceCompleted(piece.index));

        // check if the all wanted pieces have been completed
        let is_completed = self.is_completed().await;
        if is_completed {
            // offload the state change to the main loop
            self.update_state(TorrentState::Finished);
        }
    }

    /// Update the stats info of all interested pieces by the torrent.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn update_interested_pieces_stats(&self) {
        let start_time = Instant::now();
        let interested_pieces = self.data_pool.interested_pieces().await;
        let len = interested_pieces.len() as u64;

        // calculate the wanted metrics
        self.metrics.wanted_pieces.set(len);
        let wanted_size = self.data_pool.interested_size().await as u64;
        self.metrics.wanted_size.set(wanted_size);

        // calculate the completed metrics
        let wanted_completed_pieces = self
            .data_pool
            .completed_pieces()
            .await
            .into_iter()
            .filter(|piece| interested_pieces.contains(piece))
            .count() as u64;
        self.metrics
            .wanted_completed_pieces
            .set(wanted_completed_pieces);
        let remaining_size = self
            .data_pool
            .wanted_pieces()
            .await
            .into_iter()
            .map(|e| e.length as u64)
            .sum();
        // calculate the wanted completed size,
        // by subtracting the remaining size from the wanted size
        self.metrics
            .wanted_completed_size
            .set(wanted_size.saturating_sub(remaining_size));

        let elapsed = start_time.elapsed();
        trace!(
            "Torrent {} updated interested pieces stats in {:.3}ms",
            self,
            elapsed.as_secs_f64() * 1000.0
        );
    }

    /// Resume the torrent.
    /// This will put the torrent back into [TorrentFlags::DownloadMode], trying to download any missing pieces.
    pub(crate) async fn resume(&mut self) {
        self.add_options(TorrentFlags::DownloadMode | TorrentFlags::Metadata);
        self.remove_options(TorrentFlags::Paused);

        // announce to the trackers if we don't know any peers
        if self.peer_pool.num_connect_candidates() == 0 {
            self.announce_all(None);
        }

        let wanted_pieces = self.total_wanted_pieces().await;
        debug!(
            "Torrent {} is resuming with {} wanted remaining pieces",
            self, wanted_pieces
        );
    }

    /// Pause the torrent operations.
    pub(crate) async fn pause(&mut self) {
        self.add_options(TorrentFlags::Paused);
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
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
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
                response.send(self.peer_pool.get(&handle).cloned());
            }
            TorrentCommand::GetPeerByAddr { addr, response } => {
                response.send(self.peer_pool.get_by_addr(&addr).cloned())
            }
            TorrentCommand::PeerConnected { peer } => self.add_peer(peer),
            TorrentCommand::DecreasePeerPriority { addrs } => {
                self.decrease_peer_addr_priority(addrs)
            }
            TorrentCommand::PeerClosed { addr, reason } => {
                self.on_peer_closed(addr, reason).await;
            }
            TorrentCommand::Metadata { response } => {
                response.send(self.metadata.clone());
            }
            TorrentCommand::UpdateMetadata { metadata, response } => {
                response.send(self.set_metadata(metadata))
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
            TorrentCommand::NumOfPeerAddrs { response } => response.send(self.peer_pool.len()),
            TorrentCommand::NumOfActivePeerConnections { response } => {
                response.send(self.peer_pool.active_peer_connections());
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
            TorrentCommand::PieceAvailabilities { pieces, available } => {
                self.update_piece_availabilities(pieces, available).await;
            }
            TorrentCommand::Files { response } => {
                response.send(self.data_pool.files().await);
            }
            TorrentCommand::File { file, response } => {
                response.send(self.data_pool.file(&file).await);
            }
            TorrentCommand::FileByName { name, response } => {
                response.send(self.data_pool.file_by_name(name).await);
            }
            TorrentCommand::FileIndexFor { piece, response } => {
                response.send(self.data_pool.file_index_for(&piece).await)
            }
            TorrentCommand::FileStream { file, response } => {
                response.send(self.stream(&file).await)
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
            TorrentCommand::PickPieces { peer } => {
                self.on_pick_pieces(peer).await;
            }
            TorrentCommand::PieceVerified {
                piece,
                v1_hash,
                v2_hash,
            } => {
                self.on_piece_verified(&piece, v1_hash, v2_hash).await;
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
                response.send(self.pause().await);
            }
            TorrentCommand::Resume { response } => {
                response.send(self.resume().await);
            }
            TorrentCommand::AnnounceAll { response } => {
                self.announce_all(Some(response.take()));
            }
            TorrentCommand::ScrapeAll { response } => {
                self.scrape(Some(response.take()));
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
            TorrentCommand::ReadBytes { range, response } => {
                response.send(self.read_bytes(range).await);
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
            TorrentCommand::Extensions { protocol, response } => {
                response.send(self.extensions(protocol))
            }
            TorrentCommand::Bitfield { response } => response.send(self.data_pool.bitfield().await),
            TorrentCommand::RequestUploadPermit { response } => {
                response.send(self.request_upload_permit().await);
            }
            TorrentCommand::PieceBlockReceived { peer, block, data } => {
                self.on_piece_block_received(peer, block, data).await
            }
            TorrentCommand::PieceBlockRejected { peer, block } => {
                self.on_piece_block_rejected(peer, block).await
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
            TorrentCommand::IsEndGame { response } => {
                response.send(self.piece_picker.is_end_game());
            }
            #[cfg(test)]
            TorrentCommand::DataPool { response } => response.send(self.data_pool.clone()),
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn on_incoming_peer_connection(&mut self, entry: PeerEntry) {
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
                        .fire_and_forget(TorrentCommand::PeerConnected { peer: peer.into() })
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

    /// Process the received data for a piece block.
    async fn on_piece_block_received(
        &mut self,
        peer: PeerHandle,
        block: PieceBlock,
        data: Vec<u8>,
    ) {
        let peer = match self.peer_pool.get(&peer) {
            None => {
                debug!("Torrent {} received block from unknown peer {}", self, peer);
                return;
            }
            Some(peer) => peer,
        };

        self.piece_picker.block_received(peer, block, data).await;
    }

    /// Process a rejected piece block request.
    async fn on_piece_block_rejected(&mut self, peer: PeerHandle, block: PieceBlock) {
        let peer = match self.peer_pool.get(&peer) {
            None => {
                debug!(
                    "Torrent {} received block reject from unknown peer {}",
                    self, peer
                );
                return;
            }
            Some(peer) => peer,
        };

        self.piece_picker.block_rejected(peer, block).await;
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

    /// Get the piece part of the torrent based on the piece and the offset within the piece.
    /// It returns [None] if the piece part is unknown to this torrent.
    ///
    /// # Arguments
    ///
    /// * `piece` - The index of the piece.
    /// * `begin` - The offset within the piece.
    pub async fn piece_block(&self, piece: PieceIndex, begin: usize) -> Option<PieceBlock> {
        self.find_piece_block(piece, begin).await
    }

    /// Get the total amount of completed pieces for the torrent.
    pub async fn total_completed_pieces(&self) -> usize {
        self.data_pool.num_completed_pieces().await
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
    /// This reads all available bytes of the file stored within the [StorageExtension].
    ///
    /// ## Remarks
    ///
    /// This doesn't verify if the bytes are valid and completed.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
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
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub(crate) async fn read_piece(&self, piece: &PieceIndex) -> Result<Vec<u8>> {
        match self.data_pool.piece(piece).await {
            None => Err(TorrentError::DataUnavailable),
            Some(piece) => {
                self.internal_read_bytes(piece.offset..piece.offset + piece.length, false)
                    .await
            }
        }
    }

    /// Try to read the given piece bytes range.
    ///
    /// ## Remarks
    ///
    /// This doesn't verify if the bytes are valid and completed.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, range)))]
    pub(crate) async fn read_piece_bytes(
        &self,
        piece: &PieceIndex,
        range: Range<usize>,
    ) -> Result<Vec<u8>> {
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
    pub async fn read_bytes_with_padding(&self, torrent_range: Range<usize>) -> Result<Vec<u8>> {
        self.internal_read_bytes(torrent_range, true).await
    }

    /// Try to read the given bytes from the torrent.
    /// This reads all bytes of one or more files from the torrent stored within the [Storage].
    ///
    /// # Arguments
    ///
    /// * `byte_range` - The torrent byte range to be read.
    ///
    /// # Returns
    ///
    /// It returns the bytes read from the torrent, returning a [TorrentError] if data was not available.
    pub async fn read_bytes(&self, byte_range: Range<usize>) -> Result<Vec<u8>> {
        self.internal_read_bytes(byte_range, false).await
    }

    async fn internal_read_bytes(
        &self,
        byte_range: Range<usize>,
        with_padding: bool,
    ) -> Result<Vec<u8>> {
        // early exit if an empty bytes range is given
        if byte_range.is_empty() {
            return Err(TorrentError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "range cannot be empty",
            )));
        }

        let metadata = match self.metadata.info.as_ref() {
            None => return Err(TorrentError::DataUnavailable),
            Some(info) => info,
        };

        // early exit if the given byte range is out-of-bounds
        let total_len = metadata.len();
        if byte_range.end > total_len {
            return Err(TorrentError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("range {:?} is out-of-bounds", byte_range),
            )));
        }

        let mut buffer = vec![0u8; byte_range.len()];
        let start_piece_index = byte_range.start / metadata.piece_length as usize;
        let bytes_read = self
            .storage
            .read(
                &mut buffer,
                &start_piece_index,
                byte_range.start % metadata.piece_length as usize,
            )
            .await?;

        if bytes_read < byte_range.len() && !with_padding {
            return Err(TorrentError::DataUnavailable);
        }

        Ok(buffer)
    }

    /// Invoke the given torrent event for all registered callbacks.
    pub(crate) fn invoke_event(&self, event: TorrentEvent) {
        self.callbacks.invoke(event)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn on_tick(
        &mut self,
        operations: &mut Vec<Operation>,
        peer_discoveries: &[PeerDiscovery],
    ) {
        self.execute_operations_chain(operations, peer_discoveries)
            .await;
        self.piece_picker_tick().await;
        self.peer_pool.tick().await;
    }

    /// Execute the torrent operations chain.
    ///
    /// This will execute the operations in order as defined by the chain.
    /// If an operation returns [None], the execution chain will be interrupted.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn execute_operations_chain(
        &mut self,
        operations: &mut Vec<Operation>,
        peer_discoveries: &[PeerDiscovery],
    ) {
        for operation in operations.iter_mut() {
            let start_time = Instant::now();
            let execution_result = operation.execute(self, peer_discoveries).await;
            let elapsed_time = start_time.elapsed();
            trace!(
                "Torrent {} \"{}\" took {:.3}ms",
                self,
                operation.name(),
                elapsed_time.as_secs_f64() * 1000.0
            );
            if execution_result == TorrentOperationResult::Stop {
                break;
            }
        }
    }

    /// Execute a tick operation within the piece picker.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn piece_picker_tick(&mut self) {
        if !self.is_download_allowed() {
            return;
        }

        self.piece_picker.tick(self.peer_pool.peers()).await;
    }
}

impl Callback<TorrentEvent> for TorrentContext {
    fn subscribe(&self) -> Subscription<TorrentEvent> {
        self.callbacks.subscribe()
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
    use crate::operation::{
        ConnectPeersOperation, CreatePiecesAndFilesOperation, FileValidationOperation,
        StatsOperation,
    };
    use crate::peer::TcpPeerDiscovery;
    use crate::storage::{DiskStorage, MemoryStorage};
    use crate::tests::helpers::{wait_for_torrent_pieces, wait_for_torrent_state};
    use crate::tests::{copy_test_file, read_test_file_to_bytes};
    use crate::{InfoHash, Magnet};
    use std::net::Ipv4Addr;
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
            let torrent = torrent!(
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
                while let Ok(event) = receiver.recv().await {
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
        use crate::tracker::{TrackerEntry, TrackerServer};

        #[tokio::test]
        async fn test_announce() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let torrent = torrent!(
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

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_scrape() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let tracker_server = TrackerServer::new().await.unwrap();
            let tracker_manager = TrackerClient::new(Duration::from_secs(2));
            let torrent = torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![],
                vec![],
                |_| MemoryStorage::new().into(),
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
        use crate::operation::MetadataOperation;

        #[tokio::test]
        async fn test_metadata_available() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let filename = "debian-udp.torrent";
            let torrent_info_data = read_test_file_to_bytes(filename);
            let torrent_info = TorrentMetadata::try_from(torrent_info_data.as_slice()).unwrap();
            let torrent = torrent!(
                filename,
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![CreatePiecesAndFilesOperation::new().into()]
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
            let source_torrent = torrent!(
                filename,
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![],
                vec![TcpPeerDiscovery::new().await.unwrap().into()]
            );
            let torrent = torrent!(
                magnet_uri.as_str(),
                temp_path,
                TorrentFlags::Metadata,
                TorrentConfig::builder().build(),
                vec![
                    StatsOperation::new().into(),
                    ConnectPeersOperation::new(false).into(),
                    MetadataOperation::new(None).into(),
                ],
                vec![TcpPeerDiscovery::new().await.unwrap().into()],
                |_| { MemoryStorage::new().into() },
                None
            );

            // listen for the metadata changed event
            let torrent_handle = torrent.handle();
            let mut receiver = torrent.subscribe();
            tokio::spawn(async move {
                while let Ok(event) = receiver.recv().await {
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
                Duration::from_secs(10),
                rx.recv(),
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
            let torrent = torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![CreatePiecesAndFilesOperation::new().into()]
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
            let torrent = torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![CreatePiecesAndFilesOperation::new().into()]
            );
            let (tx, mut rx) = unbounded_channel();

            let mut receiver = torrent.subscribe();
            tokio::spawn(async move {
                while let Ok(event) = receiver.recv().await {
                    if let TorrentEvent::PiecesChanged(_) = *event {
                        tx.send(()).unwrap();
                    }
                }
            });

            // wait for the pieces changed event
            timeout!(
                Duration::from_millis(750),
                rx.recv(),
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
            let torrent = torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                Operation::default_operations(),
                vec![],
                |_| MemoryStorage::new().into(),
                None
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
            let expected_piece_part = PieceBlock {
                piece: 0,
                block: 1,
                begin: 16384,
                length: 16384,
            };
            let (mut context, _) = torrent_context!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build()
            );

            // create the torrent pieces
            let mut operation = CreatePiecesAndFilesOperation::new();
            let result = operation.execute(&mut context).await;
            assert_eq!(TorrentOperationResult::Continue, result);

            // request an invalid piece part
            let result = context.piece_block(0, 16000).await;
            assert_eq!(
                None, result,
                "expected no piece part to be returned for invalid begin"
            );

            // request a valid piece part
            let result = context.piece_block(0, 16384).await;
            assert_eq!(Some(expected_piece_part), result, "expected the piece part");
        }

        #[tokio::test]
        async fn test_total_wanted_pieces() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let expected_result = 75;
            let mut operation = CreatePiecesAndFilesOperation::new();
            let (mut context, _) = torrent_context!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![]
            );

            // create the torrent pieces
            operation.execute(&mut context).await;

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
            let torrent = torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![CreatePiecesAndFilesOperation::new().into()]
            );
            let (tx, mut rx) = unbounded_channel();

            // wait for the pieces changed event
            let mut receiver = torrent.subscribe();
            tokio::spawn(async move {
                while let Ok(event) = receiver.recv().await {
                    if let TorrentEvent::FilesChanged = *event {
                        tx.send(()).unwrap();
                    }
                }
            });

            let _ = timeout!(
                Duration::from_millis(750),
                rx.recv(),
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
            let torrent = torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                Operation::default_operations(),
                vec![],
                |_| { MemoryStorage::new().into() },
                None
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
        init_logger!(LevelFilter::Trace);
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
        let source_torrent = torrent!(
            "debian-udp.torrent",
            temp_path_source,
            TorrentFlags::UploadMode | TorrentFlags::SeedMode,
            TorrentConfig::builder().build(),
            vec![
                CreatePiecesAndFilesOperation::new().into(),
                FileValidationOperation::new().into(),
            ],
            vec![TcpPeerDiscovery::new().await.unwrap().into()],
            |params| DiskStorage::new(params.info_hash, params.path, params.data_pool).into(),
            None
        );
        let target_torrent = torrent!(
            "debian-udp.torrent",
            temp_path_target,
            TorrentFlags::DownloadMode | TorrentFlags::Paused,
            TorrentConfig::builder().build(),
            vec![
                StatsOperation::new().into(),
                ConnectPeersOperation::new(false).into(),
                CreatePiecesAndFilesOperation::new().into(),
            ],
            vec![TcpPeerDiscovery::new().await.unwrap().into()],
            |params| DiskStorage::new(params.info_hash, params.path, params.data_pool).into(),
            None
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
            while let Ok(event) = receiver.recv().await {
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
            Duration::from_secs(90),
            rx_state.recv(),
            "expected the torrent to enter the FINISHED state"
        )
        .unwrap();

        // validate the pieces and received data
        let data_pool = target_torrent.inner.data_pool().await.unwrap();
        let pieces_bitfield = data_pool.bitfield().await;

        for piece_index in 0..num_of_pieces {
            assert_eq!(
                Some(true),
                pieces_bitfield.get(piece_index).map(|e| *e),
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

    #[tokio::test]
    async fn test_torrent_is_completed() {
        init_logger!(LevelFilter::Info);
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        copy_test_file(
            temp_path,
            "piece-1_30.iso",
            Some("debian-12.4.0-amd64-DVD-1.iso"),
        );
        let torrent = torrent!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![CreatePiecesAndFilesOperation::new().into()],
            vec![]
        );

        // wait for the pieces to be created
        wait_for_torrent_pieces(&torrent).await;

        // mark all pieces as completed
        let pieces = torrent.pieces().await.unwrap();
        for piece in 0..pieces.len() {
            torrent
                .inner
                .piece_verified(
                    &piece,
                    pieces.get(piece).and_then(|e| e.hash.hash_v1()),
                    None,
                )
                .await;
        }

        let result = torrent.is_completed().await;
        assert_eq!(true, result, "expected the torrent to be completed");
    }

    #[tokio::test]
    async fn test_torrent_is_download_allowed() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (mut context, _) = torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build()
        );

        // create the pieces and files for the torrent
        let mut operation = CreatePiecesAndFilesOperation::new();
        operation.execute(&mut context).await;

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
        let (mut context, _) = torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::UploadMode,
            TorrentConfig::builder().build()
        );

        // create pieces and files for the torrent
        let mut operation = CreatePiecesAndFilesOperation::new();
        operation.execute(&mut context).await;

        // validate the existing files
        let mut operation = FileValidationOperation::new();
        operation.execute(&mut context).await;

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
        init_logger!(LevelFilter::Info);
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let torrent = torrent!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![CreatePiecesAndFilesOperation::new().into()],
            vec![],
            |params| DiskStorage::new(params.info_hash, params.path, params.data_pool).into(),
            None
        );
        let data_pool = torrent.inner.data_pool().await.unwrap();

        // wait for the torrent pieces to be created
        wait_for_torrent_pieces(&torrent).await;
        // retrieve all created pieces from the torrent
        let pieces = data_pool.pieces().await;

        let completed_range_1 = (pieces.len() as f64 * 0.90) as usize;
        for piece in (0..completed_range_1).into_iter().map(|e| e as PieceIndex) {
            let _ = torrent
                .inner
                .piece_verified(
                    &piece,
                    pieces.get(piece).as_ref().and_then(|e| e.hash.hash_v1()),
                    None,
                )
                .await;
        }

        let result = torrent.inner.is_end_game().await;
        assert_eq!(
            false, result,
            "expected the torrent to not be in the end-game phase"
        );

        let completed_range_2 = (pieces.len() as f64 * 0.98) as usize;
        for piece in (completed_range_1..completed_range_2)
            .into_iter()
            .map(|e| e as PieceIndex)
        {
            let _ = torrent
                .inner
                .piece_verified(
                    &piece,
                    pieces.get(piece).as_ref().and_then(|e| e.hash.hash_v1()),
                    None,
                )
                .await;
        }

        // wait for all pieces to be processed and check the bitfield result
        let bitfield = torrent.inner.bitfield().await.unwrap();
        assert_eq!(
            (pieces.len() as f64 * 0.98) as usize,
            bitfield.count_ones(),
            "expected 98% of pieces to be completed"
        );

        let result = torrent.inner.is_end_game().await;
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
        let (mut context, _) = torrent_context!(
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
        let (mut context, _) = torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build()
        );

        // create the torrent pieces
        let mut operation = CreatePiecesAndFilesOperation::new();
        operation.execute(&mut context).await;

        // retrieve the pieces from the data pool
        let pieces = context.data_pool().pieces().await;
        let total_pieces = pieces.len();
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

        for piece in 0..2 {
            context
                .on_piece_completed(pieces.get(piece).as_ref().unwrap())
                .await;
        }

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
    async fn test_torrent_update_state() {
        init_logger!();
        let expected_state = TorrentState::Paused;
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (tx, mut rx) = unbounded_channel();
        let (mut context, _) = torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![]
        );

        // subscribe to the events of the torrent
        let mut receiver = context.subscribe();
        tokio::spawn(async move {
            while let Ok(event) = receiver.recv().await {
                if let TorrentEvent::StateChanged(state) = &*event {
                    tx.send(state.clone()).unwrap();
                    break;
                }
            }
        });

        context.update_state(expected_state);

        let result = timeout!(
            Duration::from_millis(200),
            rx.recv(),
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
            let torrent = torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![CreatePiecesAndFilesOperation::new().into()]
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
            let mut operation = CreatePiecesAndFilesOperation::new();
            let (mut context, _) = torrent_context!(
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
            operation.execute(&mut context).await;

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
            let torrent = torrent!(
                "multifile.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![CreatePiecesAndFilesOperation::new().into()]
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
