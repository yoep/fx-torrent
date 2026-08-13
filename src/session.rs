use crate::config::SessionConfig;
#[cfg(feature = "dht")]
use crate::dht::DhtTracker;
#[cfg(feature = "lsd")]
use crate::lsd::LocalServiceDiscovery;
#[cfg(feature = "lsd")]
use crate::operation::LsdPeersOperation;
use crate::operation::{
    ConnectPeersOperation, CreatePiecesAndFilesOperation, FileValidationOperation,
    MetadataOperation, Operation, StatsOperation, TorrentOperationFactory, TrackerPeersOperation,
    TrackersOperation,
};
#[cfg(feature = "dht")]
use crate::operation::{DhtNodesOperation, DhtPeersOperation};
#[cfg(feature = "extension-donthave")]
use crate::peer::extension::DontHaveExtension;
use crate::peer::extension::HolepunchExtension;
#[cfg(feature = "extension-metadata")]
use crate::peer::extension::MetadataExtension;
#[cfg(feature = "extension-pex")]
use crate::peer::extension::PexExtension;
use crate::peer::{PeerDiscovery, ProtocolExtensionFlags, TcpPeerDiscovery, UtpPeerDiscovery};
use crate::session_cache::{FxSessionCache, SessionCache};
use crate::storage::{DiskStorage, MemoryStorage, Storage, StorageParams};
use crate::torrent::Torrent;
use crate::tracker::TrackerClient;
use crate::Result;
use crate::SessionHandle;
use crate::TorrentTracker;
use crate::{
    ExtensionFactory, InfoHash, Magnet, NoSessionCache, TorrentConfig, TorrentError, TorrentEvent,
    TorrentFlags, TorrentHandle, TorrentHealth, TorrentMetadata,
    DEFAULT_TORRENT_PROTOCOL_EXTENSIONS,
};
use async_trait::async_trait;
use derive_more::Display;
use fx_callback::{Callback, MultiThreadedCallback, Subscription};
use log::{debug, trace};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io;
use std::io::Read;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{Mutex, RwLock};
use tokio::{select, time};
use tokio_util::sync::CancellationToken;

const DEFAULT_TRACKER_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_CACHE_LIMIT: usize = 10;

/// The [StorageExtension] factory used to create underlying storage for torrents.
pub type SessionStorageFactory = dyn Fn(StorageParams) -> Storage + Send + Sync;

#[deprecated(since = "0.10.0", note = "Use [FxSession] instead")]
#[doc(hidden)]
pub type FxTorrentSession = FxSession;
#[deprecated(since = "0.10.0", note = "Use [FxSessionBuilder] instead")]
#[doc(hidden)]
pub type FxTorrentSessionBuilder = FxSessionBuilder;

/// The events of a torrent session.
#[derive(Debug, Display, Clone, PartialEq)]
pub enum SessionEvent {
    /// Indicates that a new torrent was added to the session.
    #[display("torrent {} has been added", _0)]
    TorrentAdded(TorrentHandle),
    /// Indicates that a torrent has been removed from the session.
    #[display("torrent {} has been removed", _0)]
    TorrentRemoved(TorrentHandle),
}

/// A torrent session which isolates torrents from each-other.
/// A [Session] can process and manage torrents from multiple sources.
///
/// The session is always the owner of a [Torrent], meaning that it's able to drop a torrent at any time.
///
/// # Deprecated
///
/// Use [FxSession] struct instead of the [Session] trait.
/// This trait will be removed near future.
#[async_trait]
#[doc(hidden)]
#[deprecated(since = "0.10.0", note = "Use [FxTorrentSession] instead")]
pub trait Session: Debug + Callback<SessionEvent> + Send + Sync {
    /// Retrieve the unique session identifier for this session.
    /// This handle can be used to identify a session.
    ///
    /// # Returns
    ///
    /// Returns the unique session handle for this session.
    fn handle(&self) -> SessionHandle;

    /// Returns the DHT tracker instance of the session, if one is present.
    #[cfg(feature = "dht")]
    fn dht(&self) -> Option<&DhtTracker>;

    /// Returns the local service discovery instance of the session, if one is present.
    #[cfg(feature = "lsd")]
    fn local_service_discovery(&self) -> Option<&LocalServiceDiscovery>;

    /// Returns the tracker client of the session.
    fn tracker(&self) -> Option<&TrackerClient>;

    /// Get the location path to the storage of the torrents for this session.
    async fn base_path(&self) -> PathBuf;

    /// Set a new location path for the storage of the torrents within this session.
    /// This will only be applicable to new torrents, existing torrents will still use the old location.
    async fn set_base_path(&self, location: PathBuf);

    /// Get the torrent based on the given handle.
    /// It returns a weak reference to the torrent, which can be invalidated at any moment.
    /// To check if a torrent is still valid, use the [Torrent::is_valid] method.
    ///
    /// # Arguments
    ///
    /// * `handle` - The handle of the torrent to retrieve.
    ///
    /// # Returns
    ///
    /// Returns the torrent if found, else `None`.
    async fn find_torrent_by_handle(&self, handle: &TorrentHandle) -> Option<Torrent>;

    /// Get the torrent based on the given info hash.
    ///
    /// # Arguments
    ///
    /// * `info_hash` - The info hash of the torrent to retrieve.
    ///
    /// # Returns
    ///
    /// Returns a weak reference to the torrent if found, else `None`.
    async fn find_torrent_by_info_hash(&self, info_hash: &InfoHash) -> Option<Torrent>;

    /// Get the calculated torrent health based on the given torrent metadata.
    ///
    /// # Arguments
    ///
    /// * `torrent_info` - The metadata information of the torrent to check.
    ///
    /// # Returns
    ///
    /// Returns a result containing the torrent health on success or an error on failure.
    async fn torrent_health_from_info(
        &self,
        torrent_info: &TorrentMetadata,
    ) -> Result<TorrentHealth>;

    /// Get the torrent health information for the given uri.
    /// The uri can either be a magnet uri or a filepath to a torrent file.
    ///
    /// If the uri points to a valid resolvable torrent information, than the seeders and leechers will be requested from the trackers.
    ///
    /// # Arguments
    ///
    /// * `uri` - The uri of the torrent to check.
    ///
    /// # Returns
    ///
    /// Returns a result containing the torrent health on success or an error on failure.
    async fn torrent_health_from_uri(&self, uri: &str) -> Result<TorrentHealth>;

    /// Resolve the given uri into torrent information.
    /// The uri can either be a magnet uri or a filepath to a torrent file.
    ///
    /// This doesn't create any underlying [Torrent] neither does it retrieve the metadata if it's incomplete.
    /// It's just a simple conversion of a `.torrent` file or magnet uri into [TorrentMetadata].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use fx_torrent::FxSession;
    ///
    /// # fn example(session: FxSession) {
    ///     let magnet_uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
    ///     let info = session.resolve(magnet_uri);
    ///     
    ///     let filepath = "/my/path/example.torrent";
    ///     let info = session.resolve(magnet_uri);
    /// # }
    /// ```
    ///
    /// # Arguments
    ///
    /// * `uri` - The uri to resolve.
    ///
    /// # Returns
    ///
    /// Returns the resolved torrent information on success.
    fn resolve(&self, uri: &str) -> Result<TorrentMetadata>;

    /// Get the torrent information for the given magnet URI.
    ///
    /// # Arguments
    ///
    /// * `magnet_uri` - The magnet URI of the torrent to fetch.
    /// * `timeout` - The timeout to use when fetching the torrent information.
    ///
    /// # Returns
    ///
    /// Returns a result containing the torrent information on success or an error on failure.
    async fn fetch_magnet(&self, magnet_uri: &str, timeout: Duration) -> Result<TorrentMetadata>;

    /// Add a new torrent to this session for the given uri.
    /// The uri can either be a path to a torrent file or a magnet link.
    ///
    /// # Arguments
    ///
    /// * `uri` - The uri of the torrent to add.
    /// * `options` - The torrent options to use when adding the torrent.
    ///
    /// # Returns
    ///
    /// Returns the created torrent handle if successful.
    async fn add_torrent_from_uri(&self, uri: &str, options: TorrentFlags) -> Result<Torrent>;

    /// Add a new torrent to this session for the given metadata information.
    ///
    /// # Arguments
    ///
    /// * `torrent_info` - The metadata information of the torrent to add.
    /// * `options` - The torrent options to use when adding the torrent.
    ///
    /// # Returns
    ///
    /// Returns the created torrent handle if successful.
    async fn add_torrent_from_info(
        &self,
        torrent_info: TorrentMetadata,
        options: TorrentFlags,
    ) -> Result<Torrent>;

    /// Remove a torrent from this session.
    /// The handle will be ignored if it does not exist in this session.
    ///
    /// # Arguments
    ///
    /// * `handle` - The handle of the torrent to remove.
    async fn remove_torrent(&self, handle: &TorrentHandle);

    /// Get the total number of active connections within this session.
    ///
    /// # Returns
    ///
    /// It returns the total connections in-use.
    async fn total_connections(&self) -> usize;
}

/// The torrent session manager, which managed multiple torrents with shared resources.
///
/// # Example Usage
///
/// ```rust,no_run
/// # use fx_torrent::prelude::*;
///
/// # async fn example() {
///     let session = FxSession::builder()
///       .config(SessionConfig::builder()
///               .client_name("MyClient")
///               .path("/downloads")
///               .build())
///       .default_extensions()
///       .dht(DhtTracker::builder()
///             .default_routing_nodes()
///             .build()
///             .await
///             .unwrap())
///       .build();
/// # }
/// ```
#[derive(Debug, Display, Clone)]
#[display("{}", inner)]
pub struct FxSession {
    inner: Arc<InnerSession>,
}

impl FxSession {
    /// Create a new torrent session builder.
    /// The builder always requires a `base_path` to be set, all other fields are optional and will use defaults if not set.
    ///
    /// This allows for easy setup of a torrent session, while still allow some flexibility in customization at runtime.
    ///
    /// # Panics
    ///
    /// The `build` function of the builder will panic if the `base path` or `client name` is not set.
    /// Everything else is optional and uses default settings if not set.
    pub fn builder() -> FxSessionBuilder {
        FxSessionBuilder::new()
    }

    /// Create a new torrent session instance.
    /// This session can be used to manage one or more torrents at the same time.
    ///
    /// # Returns
    ///
    /// Returns the session when initialized successfully or an error on failure.
    pub fn new(
        config: SessionConfig,
        protocol_extensions: ProtocolExtensionFlags,
        extensions: Vec<ExtensionFactory>,
        operations: Vec<TorrentOperationFactory>,
        storage: Arc<SessionStorageFactory>,
        session_cache: Box<dyn SessionCache>,
        trackers: Vec<TorrentTracker>,
    ) -> Self {
        let handle = SessionHandle::new();

        trace!("Creating new torrent session {}", handle);
        let (command_sender, command_receiver) = unbounded_channel();
        let inner = Arc::new(InnerSession {
            handle,
            config: RwLock::new(config),
            trackers,
            torrents: Default::default(),
            protocol_extensions,
            extension_factories: extensions,
            torrent_operations: operations,
            storage_factory: storage,
            session_cache: Mutex::new(session_cache),
            callbacks: MultiThreadedCallback::new(),
            command_sender,
            cancellation_token: Default::default(),
        });

        let main_inner = inner.clone();
        spawn!("InnerSession::run", async move {
            main_inner.run(command_receiver).await;
        });

        debug!("Created new torrent session {}", inner.handle);
        Self { inner }
    }

    /// Returns the unique session handle.
    fn handle(&self) -> SessionHandle {
        self.inner.handle
    }

    /// Returns the DHT tracker reference used by the session, if one is present.
    #[cfg(feature = "dht")]
    pub fn dht(&self) -> Option<&DhtTracker> {
        self.inner.trackers.iter().find_map(|tracker| {
            if let TorrentTracker::Dht(dht) = tracker {
                Some(dht)
            } else {
                None
            }
        })
    }

    /// Returns the local service discovery tracker reference used by the session, if one is present.
    #[cfg(feature = "lsd")]
    pub fn local_service_discovery(&self) -> Option<&LocalServiceDiscovery> {
        self.inner.trackers.iter().find_map(|tracker| {
            if let TorrentTracker::Lsd(lsd) = tracker {
                Some(lsd)
            } else {
                None
            }
        })
    }

    /// Returns the tracker client reference used by the session, if one is present.
    pub fn tracker(&self) -> Option<&TrackerClient> {
        self.inner.trackers.iter().find_map(|tracker| {
            if let TorrentTracker::TrackerClient(tracker) = tracker {
                Some(tracker)
            } else {
                None
            }
        })
    }

    /// Returns the path to the storage location of the torrents for this session.
    pub async fn base_path(&self) -> PathBuf {
        self.inner.config.read().await.path().to_path_buf()
    }

    /// Set a new location path for the storage of the torrents within this session.
    /// This will only be applicable to new torrents, existing torrents will still use the old location.
    pub async fn set_base_path(&self, location: PathBuf) {
        self.inner.config.write().await.torrent.set_path(location);
    }

    /// Returns the torrent based on the given handle.
    /// It returns a weak reference to the torrent, which can be invalidated at any moment.
    /// To check if a torrent is still valid, use the [Torrent::is_valid] method.
    ///
    /// # Arguments
    ///
    /// * `handle` - The handle of the torrent to retrieve.
    pub async fn find_torrent_by_handle(&self, handle: &TorrentHandle) -> Option<Torrent> {
        self.inner.find_torrent_by_handle(handle).await
    }

    /// Returns the torrent based on the given info hash.
    ///
    /// # Arguments
    ///
    /// * `info_hash` - The info hash of the torrent to retrieve.
    pub async fn find_torrent_by_info_hash(&self, info_hash: &InfoHash) -> Option<Torrent> {
        self.inner.find_torrent_by_info_hash(info_hash).await
    }

    /// Returns the calculated torrent health based on the given torrent metadata.
    ///
    /// # Arguments
    ///
    /// * `torrent_info` - The metadata information of the torrent to check.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub async fn torrent_health_from_info(
        &self,
        torrent_info: &TorrentMetadata,
    ) -> Result<TorrentHealth> {
        trace!("Retrieving torrent health for {:?}", torrent_info);
        // try to retrieve the existing torrent based on its info hash
        // otherwise, we'll create a new torrent
        let torrent = match self
            .inner
            .find_torrent_by_info_hash(&torrent_info.info_hash)
            .await
        {
            Some(e) => e,
            None => Torrent::request()
                .metadata(torrent_info.clone())
                .options(TorrentFlags::none())
                .config(
                    TorrentConfig::builder()
                        .client_name(self.inner.config.read().await.client_name())
                        .peers_lower_limit(0)
                        .peers_upper_limit(0)
                        .peer_connection_timeout(Duration::from_secs(0))
                        .build(),
                )
                .protocol_extensions(self.inner.protocol_extensions)
                .extensions(self.inner.extensions())
                .operations(vec![TrackersOperation::new().into()])
                .storage(|_| MemoryStorage::new().into())
                .trackers(self.inner.trackers.clone())
                .build()?,
        };

        let metrics = torrent.scrape().await?;

        debug!(
            "Converting announcement to torrent health for {:?}",
            metrics
        );
        Ok(TorrentHealth::from(metrics.complete, metrics.incomplete))
    }

    /// Returns the torrent health information for the given uri.
    /// The uri can either be a magnet uri or a filepath to a torrent file.
    ///
    /// If the uri points to a valid resolvable torrent information, than the seeders and leechers will be requested from the trackers.
    ///
    /// # Arguments
    ///
    /// * `uri` - The uri of the torrent to check.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub async fn torrent_health_from_uri(&self, uri: &str) -> Result<TorrentHealth> {
        trace!(
            "Session {} is retrieving torrent health for {:?}",
            self,
            uri
        );
        let torrent_info = self.resolve(uri)?;
        self.torrent_health_from_info(&torrent_info).await
    }

    /// Resolve the given uri into torrent metadata information.
    /// The uri can either be a magnet uri or a filepath to a torrent file.
    ///
    /// This doesn't create any underlying [Torrent] neither does it retrieve the metadata if it's incomplete.
    /// It's just a simple conversion of a `.torrent` file or magnet uri into [TorrentMetadata].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use fx_torrent::FxSession;
    ///
    /// # fn example(session: FxSession) {
    ///     let magnet_uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
    ///     let info = session.resolve(magnet_uri);
    ///
    ///     let filepath = "/my/path/example.torrent";
    ///     let info = session.resolve(magnet_uri);
    /// # }
    /// ```
    ///
    /// # Arguments
    ///
    /// * `uri` - The uri to resolve.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn resolve(&self, uri: &str) -> Result<TorrentMetadata> {
        if Magnet::has_magnet_scheme(uri) {
            trace!("Session {} is resolving torrent magnet uri {}", self, uri);
            Magnet::from_str(uri)
                .map_err(Into::<TorrentError>::into)
                .and_then(|e| TorrentMetadata::try_from(e))
        } else {
            trace!("Session {} is resolving torrent path uri {}", self, uri);
            PathBuf::from_str(uri)
                .map_err(|e| TorrentError::Io(io::Error::new(io::ErrorKind::InvalidInput, e)))
                .and_then(|filepath| {
                    std::fs::OpenOptions::new()
                        .create(false)
                        .read(true)
                        .open(filepath)
                        .map_err(|e| TorrentError::Io(e))
                })
                .and_then(|mut file| {
                    let mut buffer = vec![];
                    if let Err(e) = file.read_to_end(&mut buffer) {
                        return Err(TorrentError::Io(e));
                    }

                    Ok(buffer)
                })
                .and_then(|bytes| TorrentMetadata::try_from(bytes.as_slice()))
        }
    }

    /// Returns the torrent metadata information for the given magnet URI.
    ///
    /// # Arguments
    ///
    /// * `magnet_uri` - The magnet URI of the torrent to fetch.
    /// * `timeout` - The timeout to use when fetching the torrent information.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub async fn fetch_magnet(
        &self,
        magnet_uri: &str,
        timeout: Duration,
    ) -> Result<TorrentMetadata> {
        trace!("Session {} is trying to fetch magnet {}", self, magnet_uri);
        let torrent_info = self.resolve(magnet_uri)?;

        {
            // check if we've already cached the metadata in the past
            let session_cache = self.inner.session_cache.lock().await;
            if let Some(metadata) = session_cache.find_metadata(&torrent_info.info_hash) {
                if metadata.info.is_some() {
                    return Ok(metadata.clone());
                }
            }
        }

        let torrent = self
            .find_or_add_torrent(torrent_info, TorrentFlags::Metadata, false)
            .await?;

        // check if the torrent metadata needs to be fetched, or is already known
        if torrent.metadata().await?.info.is_none() {
            // make sure the torrent tries to download the metadata
            torrent.add_options(TorrentFlags::Metadata).await;

            trace!("Trying to fetch metadata for {}", magnet_uri);
            select! {
                _ = time::sleep(timeout) => Err(TorrentError::Timeout),
                _ = Self::wait_for_metadata(&torrent) => Ok(()),
            }?;
        }

        // store the metadata within the session cache
        let metadata = torrent.metadata().await?;
        self.inner.add_torrent_metadata(&metadata).await;

        Ok(metadata)
    }

    /// Add a new torrent to this session for the given uri.
    /// The uri can either be a path to a torrent file or a magnet link.
    ///
    /// # Arguments
    ///
    /// * `uri` - The uri of the torrent to add.
    /// * `options` - The torrent options to use when adding the torrent.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub async fn add_torrent_from_uri(&self, uri: &str, options: TorrentFlags) -> Result<Torrent> {
        let torrent_info = self.resolve(uri)?;
        self.add_torrent_from_info(torrent_info, options).await
    }

    /// Add a new torrent to this session for the given metadata information.
    ///
    /// # Arguments
    ///
    /// * `torrent_info` - The metadata information of the torrent to add.
    /// * `options` - The torrent options to use when adding the torrent.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, torrent_info)))]
    pub async fn add_torrent_from_info(
        &self,
        torrent_info: TorrentMetadata,
        options: TorrentFlags,
    ) -> Result<Torrent> {
        self.find_or_add_torrent(torrent_info, options, true).await
    }

    /// Remove a torrent from this session.
    /// The handle will be ignored if it does not exist in this session.
    ///
    /// # Arguments
    ///
    /// * `handle` - The handle of the torrent to remove.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub async fn remove_torrent(&self, handle: &TorrentHandle) {
        self.inner.remove_torrent(handle).await
    }

    /// Returns the total number of active connections within this session.
    pub async fn total_connections(&self) -> usize {
        let torrents = self.inner.torrents.read().await;
        let mut total_connections = 0;

        for torrent in torrents.values() {
            total_connections += torrent.active_peer_connections().await;
        }

        total_connections
    }

    /// Try to find an existing torrent within the session based on the info hash,
    /// or create a new torrent from the given torrent information.
    ///
    /// # Arguments
    ///
    /// * `torrent_info` - The metadata information of the torrent to add.
    /// * `options` - The torrent options to use when adding the torrent.
    /// * `send_callback_event` - Whether to send a callback event when the torrent is added.
    ///
    /// # Returns
    ///
    /// Returns a torrent (weak reference) on success.
    async fn find_or_add_torrent(
        &self,
        torrent_info: TorrentMetadata,
        options: TorrentFlags,
        send_callback_event: bool,
    ) -> Result<Torrent> {
        trace!(
            "Trying to add {:?} to session {}",
            torrent_info,
            self.inner.handle
        );
        // check if the info hash is already known
        if let Some(torrent) = self
            .inner
            .find_torrent_by_info_hash(&torrent_info.info_hash)
            .await
        {
            debug!(
                "Torrent info hash {} already exists in session {}",
                torrent_info.info_hash, self.inner.handle
            );
            return Ok(torrent);
        }

        let torrent_info = if torrent_info.info.is_some() {
            torrent_info
        } else {
            let session_cache = self.inner.session_cache.lock().await;
            session_cache
                .find_metadata(&torrent_info.info_hash)
                .cloned()
                .unwrap_or(torrent_info)
        };

        let info_hash = torrent_info.info_hash.clone();
        let config = self.inner.config.read().await.torrent.clone();

        trace!(
            "Session {} is creating new torrent for info hash {}",
            self,
            info_hash
        );
        let peer_discoveries = self.inner.create_discoveries().await?;
        let storage = self.inner.storage_factory.clone();
        let torrent = Torrent::request()
            .metadata(torrent_info)
            .options(options)
            .config(config)
            .peer_discoveries(peer_discoveries)
            .protocol_extensions(self.inner.protocol_extensions)
            .extensions(self.inner.extensions())
            .operations(self.inner.torrent_operations())
            .storage(move |params| storage(params))
            .trackers(self.inner.trackers.clone())
            .build()?;
        let result_torrent = torrent.clone();

        self.inner
            .add_torrent(info_hash, torrent, send_callback_event)
            .await;

        Ok(result_torrent)
    }

    async fn wait_for_metadata(torrent: &Torrent) {
        let mut receiver = torrent.subscribe();
        while let Ok(event) = receiver.recv().await {
            if let TorrentEvent::MetadataChanged(_) = &*event {
                break;
            }
        }
    }
}

#[async_trait]
impl Session for FxSession {
    fn handle(&self) -> SessionHandle {
        self.handle()
    }

    #[cfg(feature = "dht")]
    fn dht(&self) -> Option<&DhtTracker> {
        self.dht()
    }

    #[cfg(feature = "lsd")]
    fn local_service_discovery(&self) -> Option<&LocalServiceDiscovery> {
        self.local_service_discovery()
    }

    fn tracker(&self) -> Option<&TrackerClient> {
        self.tracker()
    }

    async fn base_path(&self) -> PathBuf {
        self.base_path().await
    }

    async fn set_base_path(&self, location: PathBuf) {
        self.set_base_path(location).await;
    }

    async fn find_torrent_by_handle(&self, handle: &TorrentHandle) -> Option<Torrent> {
        self.find_torrent_by_handle(handle).await
    }

    async fn find_torrent_by_info_hash(&self, info_hash: &InfoHash) -> Option<Torrent> {
        self.find_torrent_by_info_hash(info_hash).await
    }

    async fn torrent_health_from_info(
        &self,
        torrent_info: &TorrentMetadata,
    ) -> Result<TorrentHealth> {
        self.torrent_health_from_info(torrent_info).await
    }

    async fn torrent_health_from_uri(&self, uri: &str) -> Result<TorrentHealth> {
        self.torrent_health_from_uri(uri).await
    }

    fn resolve(&self, uri: &str) -> Result<TorrentMetadata> {
        self.resolve(uri)
    }

    async fn fetch_magnet(&self, magnet_uri: &str, timeout: Duration) -> Result<TorrentMetadata> {
        self.fetch_magnet(magnet_uri, timeout).await
    }

    async fn add_torrent_from_uri(&self, uri: &str, options: TorrentFlags) -> Result<Torrent> {
        self.add_torrent_from_uri(uri, options).await
    }

    async fn add_torrent_from_info(
        &self,
        torrent_info: TorrentMetadata,
        options: TorrentFlags,
    ) -> Result<Torrent> {
        self.add_torrent_from_info(torrent_info, options).await
    }

    async fn remove_torrent(&self, handle: &TorrentHandle) {
        self.remove_torrent(handle).await;
    }

    async fn total_connections(&self) -> usize {
        self.total_connections().await
    }
}

impl Callback<SessionEvent> for FxSession {
    fn subscribe(&self) -> Subscription<SessionEvent> {
        self.inner.callbacks.subscribe()
    }
}

impl Drop for FxSession {
    fn drop(&mut self) {
        // check if we're the last 2 references to the session
        // if so, terminate the main loop of the session
        if Arc::strong_count(&self.inner) == 2 {
            self.inner.trackers.iter().for_each(|e| e.close());
            self.inner.cancellation_token.cancel();
        }
    }
}

/// The torrent session builder for configuring a new [FxSession].
///
/// # Required fields
///
/// The following fields are required to be configured.
///
/// - `path` - The location where torrent file data will be stored.
/// - `client_name` - The client name which is communicated between torrent peers.
///
/// All other fields make use of defaults when not set.
///
/// # Example
///
/// ```rust,no_run
/// # use fx_torrent::prelude::*;
///
/// FxSession::builder()
///     .config(SessionConfig::builder()
///         .client_name("MyClientName")
///         .path("/tmp/fx-torrent")
///         .build())
///     .build();
/// ```
#[derive(Default)]
pub struct FxSessionBuilder {
    config: Option<SessionConfig>,
    protocol_extensions: Option<ProtocolExtensionFlags>,
    extension_factories: Vec<ExtensionFactory>,
    operation_factories: Option<Vec<TorrentOperationFactory>>,
    storage: Option<Arc<SessionStorageFactory>>,
    session_cache: Option<Box<dyn SessionCache>>,
    #[cfg(feature = "dht")]
    dht: Option<DhtTracker>,
    #[cfg(feature = "lsd")]
    lsd: Option<LocalServiceDiscovery>,
}

impl FxSessionBuilder {
    /// Create a new builder instance to construct a [FxSession].
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the configuration of the session.
    pub fn config(&mut self, config: SessionConfig) -> &mut Self {
        self.config = Some(config);
        self
    }

    /// Set the protocol extensions for the session.
    pub fn protocol_extensions(
        &mut self,
        protocol_extensions: ProtocolExtensionFlags,
    ) -> &mut Self {
        self.protocol_extensions = Some(protocol_extensions);
        self
    }

    /// Enable the default peer extensions to use within the session.
    pub fn default_extensions(&mut self) -> &mut Self {
        #[cfg(feature = "extension-metadata")]
        self.extension_factories
            .push(|| MetadataExtension::new().into());
        #[cfg(feature = "extension-pex")]
        self.extension_factories
            .push(|| PexExtension::new(Duration::from_secs(90)).into());
        #[cfg(feature = "extension-donthave")]
        self.extension_factories
            .push(|| DontHaveExtension::new().into());
        self.extension_factories
            .push(|| HolepunchExtension::new().into());
        self
    }

    /// Add an extension to the session.
    pub fn extension(&mut self, extension: ExtensionFactory) -> &mut Self {
        self.extension_factories.push(extension);
        self
    }

    /// Set the extensions for the session.
    /// This overrides any previously configured extensions.
    pub fn extensions(&mut self, extensions: Vec<ExtensionFactory>) -> &mut Self {
        self.extension_factories = extensions;
        self
    }

    /// Add an operation to the session.
    pub fn operation(&mut self, operation: TorrentOperationFactory) -> &mut Self {
        self.operation_factories
            .get_or_insert(Vec::new())
            .push(operation);
        self
    }

    /// Set the torrent operation factories for the session.
    /// These are the operations which are executed on the main loop of the torrent.
    pub fn operations(&mut self, torrent_operations: Vec<TorrentOperationFactory>) -> &mut Self {
        self.operation_factories = Some(torrent_operations);
        self
    }

    /// Set the storage factory for the session.
    pub fn storage<F>(&mut self, storage: F) -> &mut Self
    where
        F: Fn(StorageParams) -> Storage + Send + Sync + 'static,
    {
        self.storage = Some(Arc::new(storage));
        self
    }

    /// Set the torrent session cache to be used within the session.
    pub fn session_cache<S: SessionCache + 'static>(&mut self, cache: S) -> &mut Self {
        self.session_cache = Some(Box::new(cache));
        self
    }

    /// Disable the torrent session cache used within the session.
    pub fn disable_session_cache(&mut self) -> &mut Self {
        self.session_cache = Some(Box::new(NoSessionCache::new()));
        self
    }

    /// Set the DHT tracker for the session.
    #[cfg(feature = "dht")]
    pub fn dht(&mut self, dht: DhtTracker) -> &mut Self {
        self.dht = Some(dht);
        self
    }

    /// Set the DHT tracker for the session.
    /// This overrides any previously configured DHT tracker.
    #[cfg(feature = "dht")]
    pub fn dht_option(&mut self, dht: Option<DhtTracker>) -> &mut Self {
        self.dht = dht;
        self
    }

    /// Set the local service discovery for the session.
    #[cfg(feature = "lsd")]
    pub fn local_service_discovery(&mut self, lsd: LocalServiceDiscovery) -> &mut Self {
        self.lsd = Some(lsd);
        self
    }

    /// Create a new torrent session from this builder.
    /// The only required field within this builder is the base path for the torrent storage.
    ///
    /// # Returns
    ///
    /// It returns an error when one of the required is not set.
    pub fn build(&mut self) -> Result<FxSession> {
        let config = self
            .config
            .take()
            .unwrap_or_else(|| SessionConfig::builder().build());
        let protocol_extensions = self
            .protocol_extensions
            .unwrap_or_else(DEFAULT_TORRENT_PROTOCOL_EXTENSIONS);
        let extensions = std::mem::take(&mut self.extension_factories);
        let torrent_operations = self.operation_factories.take().unwrap_or_else(|| {
            // FIXME: this is currently a duplicate list, consolidate with the torrent request operations
            vec![
                TorrentOperationFactory::new(|| StatsOperation::new().into()),
                TorrentOperationFactory::new(|| TrackersOperation::new().into()),
                #[cfg(feature = "dht")]
                TorrentOperationFactory::new(|| DhtNodesOperation::new().into()),
                #[cfg(feature = "dht")]
                TorrentOperationFactory::new(|| DhtPeersOperation::new().into()),
                #[cfg(feature = "lsd")]
                TorrentOperationFactory::new(|| LsdPeersOperation::new().into()),
                TorrentOperationFactory::new(|| TrackerPeersOperation::new().into()),
                TorrentOperationFactory::new(|| ConnectPeersOperation::new(true).into()),
                TorrentOperationFactory::new(|| MetadataOperation::new(None).into()),
                TorrentOperationFactory::new(|| CreatePiecesAndFilesOperation::new().into()),
                TorrentOperationFactory::new(|| FileValidationOperation::new().into()),
            ]
        });
        let storage = self.storage.take().unwrap_or_else(|| {
            Arc::new(|params| {
                DiskStorage::new(params.info_hash, params.path, params.data_pool).into()
            })
        });
        let session_cache = self
            .session_cache
            .take()
            .unwrap_or_else(|| Box::new(FxSessionCache::new(DEFAULT_CACHE_LIMIT)));
        let mut trackers = vec![TrackerClient::new(DEFAULT_TRACKER_TIMEOUT).into()];

        #[cfg(feature = "dht")]
        {
            if let Some(dht) = self.dht.take() {
                trackers.push(dht.into());
            }
        }
        #[cfg(feature = "lsd")]
        {
            if let Some(lsd) = self.lsd.take() {
                trackers.push(lsd.into());
            }
        }

        Ok(FxSession::new(
            config,
            protocol_extensions,
            extensions,
            torrent_operations,
            storage,
            session_cache,
            trackers,
        ))
    }
}

impl Debug for FxSessionBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("FxSessionBuilder");
        d.field("config", &self.config)
            .field("protocol_extensions", &self.protocol_extensions)
            .field("extensions", &self.extension_factories)
            .field("operation_factories", &self.operation_factories)
            .field("session_cache", &self.session_cache);
        #[cfg(feature = "dht")]
        d.field("dht", &self.dht);
        d.finish()
    }
}

#[derive(Debug)]
enum SessionCommand {
    /// Store the metadata within the session
    StoreMetadata(TorrentMetadata),
}

// TODO: add options which support configuring timeouts etc
#[derive(Display)]
#[display("{}", handle)]
struct InnerSession {
    /// The unique session identifier
    handle: SessionHandle,
    /// The config settings of the session
    config: RwLock<SessionConfig>,
    /// The trackers of the session
    trackers: Vec<TorrentTracker>,
    /// The currently active torrents within the session
    torrents: RwLock<HashMap<InfoHash, Torrent>>,
    /// The enabled protocol extensions of the session
    protocol_extensions: ProtocolExtensionFlags,
    /// The factories which initializes extensions for a new torrent
    extension_factories: Vec<ExtensionFactory>,
    /// The factories which initialize operations for a new torrent
    torrent_operations: Vec<TorrentOperationFactory>,
    /// The factory which initialize a storage for a new torrent
    storage_factory: Arc<SessionStorageFactory>,
    /// The torrent cache of the session
    session_cache: Mutex<Box<dyn SessionCache>>,
    /// The event callbacks of the session
    callbacks: MultiThreadedCallback<SessionEvent>,
    command_sender: UnboundedSender<SessionCommand>,
    cancellation_token: CancellationToken,
}

impl InnerSession {
    /// Run the main loop of the session.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn run(&self, mut command_receiver: UnboundedReceiver<SessionCommand>) {
        loop {
            select! {
                _ = self.cancellation_token.cancelled() => break,
                Some(command) = command_receiver.recv() => self.handle_command(command).await,
            }
        }

        debug!("Session {} main loop ended", self);
    }

    async fn handle_command(&self, command: SessionCommand) {
        match command {
            SessionCommand::StoreMetadata(metadata) => self.add_torrent_metadata(&metadata).await,
        }
    }

    /// Get the enabled peer extensions of the session.
    fn extensions(&self) -> Vec<ExtensionFactory> {
        self.extension_factories.clone()
    }

    /// Get the torrent processing operation.
    fn torrent_operations(&self) -> Vec<Operation> {
        self.torrent_operations.iter().map(|e| e.create()).collect()
    }

    async fn find_torrent_by_handle(&self, handle: &TorrentHandle) -> Option<Torrent> {
        self.torrents
            .read()
            .await
            .iter()
            .find(|(_, e)| e.handle() == *handle)
            .map(|(_, e)| e.clone())
    }

    /// Try to find the torrent by the given info hash.
    /// It returns a weak reference to the torrent if it is found, otherwise None.
    async fn find_torrent_by_info_hash(&self, info_hash: &InfoHash) -> Option<Torrent> {
        (*self.torrents.read().await)
            .get(info_hash)
            .map(|e| e.clone())
    }

    /// Add or replace the torrent in the session based on the info hash.
    ///
    /// ## Caution
    ///
    /// This might replace an existing torrent with the same info hash.
    /// The original strong reference torrent will be dropped in this scenario, invalidating the original torrent.
    ///
    /// # Arguments
    ///
    /// * `info_hash` - The info hash of the torrent to add.
    /// * `torrent` - The torrent to add.
    /// * `send_callback_event` - Whether to send a callback event when the torrent is added.
    async fn add_torrent(&self, info_hash: InfoHash, torrent: Torrent, send_callback_event: bool) {
        let handle = torrent.handle();

        let command_sender = self.command_sender.clone();
        let mut receiver = torrent.subscribe();
        tokio::spawn(async move {
            while let Ok(event) = receiver.recv().await {
                if let TorrentEvent::MetadataChanged(metadata) = &*event {
                    let _ = command_sender.send(SessionCommand::StoreMetadata(metadata.clone()));
                    break;
                }
            }
        });

        {
            let mut mutex = self.torrents.write().await;
            debug!(
                "Adding torrent {} with options {:?}",
                handle,
                torrent.options().await
            );
            mutex.insert(info_hash, torrent);
        }

        if send_callback_event {
            self.callbacks.invoke(SessionEvent::TorrentAdded(handle));
        }
    }

    async fn add_torrent_metadata(&self, metadata: &TorrentMetadata) {
        let mut session_cache = self.session_cache.lock().await;
        session_cache.store_metadata(metadata);
    }

    async fn remove_torrent(&self, handle: &TorrentHandle) {
        trace!("Session {} is trying to remove torrent {}", self, handle);
        let torrent_info_hash: Option<InfoHash>;

        {
            let mut mutex = self.torrents.write().await;
            torrent_info_hash = mutex
                .iter()
                .find(|(_, torrent)| torrent.handle() == *handle)
                .map(|(info_hash, _)| info_hash)
                .cloned();

            if let Some(info_hash) = &torrent_info_hash {
                mutex.remove(&info_hash);
                debug!("Session {} removed torrent {}", self, handle);
            }
        }

        if let Some(_) = torrent_info_hash {
            self.callbacks.invoke(SessionEvent::TorrentRemoved(*handle));
        } else {
            trace!("Session {} has no torrent {}", self, handle);
        }
    }

    async fn create_discoveries(&self) -> Result<Vec<PeerDiscovery>> {
        let mut port = 0;
        let config = self.config.read().await;
        let mut discoveries: Vec<PeerDiscovery> = Vec::new();

        if config.enable_utp_peer {
            let utp_discovery = UtpPeerDiscovery::with_port(port)
                .await
                .map_err(|e| TorrentError::Peer(e))?;
            port = utp_discovery.addr().port();
            discoveries.push(utp_discovery.into());
        }
        if config.enable_tcp_peer {
            let tcp_discovery = TcpPeerDiscovery::with_port(port)
                .await
                .map_err(|e| TorrentError::Peer(e))?;
            port = tcp_discovery.addr().port();
            discoveries.push(tcp_discovery.into());
        }

        debug!("Session {} listening on port {}", self, port);
        Ok(discoveries)
    }
}

impl Debug for InnerSession {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InnerSession")
            .field("handle", &self.handle)
            .field("config", &self.config)
            .field("trackers", &self.trackers)
            .field("torrents", &self.torrents)
            .field("protocol_extensions", &self.protocol_extensions)
            .field("extension_factories", &self.extension_factories)
            .field("torrent_operations", &self.torrent_operations)
            .field("session_cache", &self.session_cache)
            .finish()
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::tests::{read_test_file_to_bytes, test_resource_filepath};
    use crate::TorrentHealthState;
    use log::info;
    use std::net::Ipv4Addr;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::sync::mpsc::unbounded_channel;

    #[tokio::test]
    async fn test_session_find_torrent() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let data = read_test_file_to_bytes("debian.torrent");
        let info = TorrentMetadata::try_from(data.as_slice()).unwrap();
        let info_hash = info.info_hash.clone();
        let session = create_session(temp_path).await;

        let _ = session
            .add_torrent_from_info(info, TorrentFlags::default())
            .await
            .expect("expected the torrent to have been added");
        let result = session.find_torrent_by_info_hash(&info_hash).await;

        assert_ne!(None, result);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_session_fetch_magnet() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
        let source_torrent = torrent!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::Metadata,
            TorrentConfig::builder().build(),
            vec![]
        );
        let source_port = source_torrent.peer_port().await.unwrap();
        let session = FxSession::builder()
            .config(
                SessionConfig::builder()
                    .client_name("fetch magnet test")
                    .path(temp_path)
                    .enable_utp_peer(false)
                    .build(),
            )
            .default_extensions()
            .operations(vec![
                TorrentOperationFactory::new(|| ConnectPeersOperation::new(false).into()),
                TorrentOperationFactory::new(|| MetadataOperation::new(None).into()),
                TorrentOperationFactory::new(|| CreatePiecesAndFilesOperation::new().into()),
            ])
            .build()
            .unwrap();

        // initially, add the torrent without any flags
        let target_torrent = session
            .add_torrent_from_uri(uri, TorrentFlags::Metadata)
            .await
            .unwrap();

        // add the source torrent to the peer pool of the target
        target_torrent
            .add_peer((Ipv4Addr::LOCALHOST, source_port).into())
            .await
            .unwrap();

        // now fetch the magnet torrent from the same uri
        // this will reuse the same target torrent and attach the TorrentFlags::Metadata
        let result = session
            .fetch_magnet(uri, Duration::from_secs(40))
            .await
            .unwrap();
        assert_ne!(
            None, result.info,
            "expected the metadata to have been present"
        );
    }

    #[tokio::test]
    async fn test_session_torrent_health_from_file() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let data = read_test_file_to_bytes("debian-udp.torrent");
        let info = TorrentMetadata::try_from(data.as_slice()).unwrap();
        let session = create_session(temp_path).await;

        let result = session
            .torrent_health_from_info(&info)
            .await
            .expect("expected a torrent health");

        info!("Got torrent health result {:?}", result);
        assert_ne!(TorrentHealthState::Unknown, result.state);
        assert_ne!(0, result.seeds, "expected seeders to have been found");
    }

    #[tokio::test]
    async fn test_session_torrent_health_from_magnet() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
        let magnet = Magnet::from_str(uri).unwrap();
        let info = TorrentMetadata::try_from(magnet).unwrap();
        let session = create_session(temp_path).await;

        let result = session
            .torrent_health_from_info(&info)
            .await
            .expect("expected a torrent health");

        info!("Got torrent health result {:?}", result);
        assert_ne!(TorrentHealthState::Unknown, result.state);
        assert_ne!(0, result.seeds, "expected seeders to have been found");
    }

    #[tokio::test]
    async fn test_session_resolve() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let session = create_session(temp_path).await;

        let filepath = test_resource_filepath("debian.torrent");
        let result = session
            .resolve(filepath.to_str().unwrap())
            .expect("expected the torrent info to have been resolved");
        let expected_info_hash =
            InfoHash::from_str("6D4795DEE70AEB88E03E5336CA7C9FCF0A1E206D").unwrap();
        assert_eq!(expected_info_hash, result.info_hash);

        let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
        let result = session
            .resolve(uri)
            .expect("expected the torrent info to have been resolved");
        let expected_info_hash =
            InfoHash::from_str("EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
        assert_eq!(expected_info_hash, result.info_hash);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_session_add_torrent() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let data = read_test_file_to_bytes("debian.torrent");
        let info = TorrentMetadata::try_from(data.as_slice()).unwrap();
        let (tx, mut rx) = unbounded_channel();
        let session = create_session(temp_path).await;

        let mut receiver = session.subscribe();
        tokio::spawn(async move {
            while let Ok(event) = receiver.recv().await {
                let _ = tx.send((*event).clone());
            }
        });

        let torrent = session
            .add_torrent_from_info(info, TorrentFlags::none())
            .await
            .expect("expected a torrent handle");

        let event = select! {
            _ = tokio::time::sleep(Duration::from_millis(500)) => {
                assert!(false, "receive event timed out");
                return;
            },
            event = rx.recv() => event.unwrap(),
        };
        assert_eq!(event, SessionEvent::TorrentAdded(torrent.handle()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_session_remove_torrent() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let data = read_test_file_to_bytes("debian.torrent");
        let info = TorrentMetadata::try_from(data.as_slice()).unwrap();
        let (tx, mut rx) = unbounded_channel();
        let session = create_session(temp_path).await;

        let mut receiver = session.subscribe();
        tokio::spawn(async move {
            while let Ok(event) = receiver.recv().await {
                let _ = tx.send((*event).clone());
            }
        });
        let torrent = session
            .add_torrent_from_info(info, TorrentFlags::none())
            .await
            .expect("expected a torrent handle");
        let handle = torrent.handle();

        let event = timeout!(
            Duration::from_millis(250),
            rx.recv(),
            "expected to receive a session event"
        )
        .unwrap();
        assert_eq!(event, SessionEvent::TorrentAdded(handle));

        session.remove_torrent(&handle).await;

        let event = timeout!(
            Duration::from_millis(250),
            rx.recv(),
            "expected to receive a session event"
        )
        .unwrap();
        assert_eq!(event, SessionEvent::TorrentRemoved(handle));
    }

    mod dht {
        use super::*;

        #[tokio::test]
        async fn test_dht_tracker() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let dht = DhtTracker::builder().build().await.unwrap();
            let session = FxSession::builder()
                .config(
                    SessionConfig::builder()
                        .client_name("test")
                        .path(temp_path)
                        .build(),
                )
                .dht(dht)
                .default_extensions()
                .build()
                .unwrap();

            // retrieve the dht tracker from the session
            let result = session.dht();
            assert!(result.is_some(), "expected a dht tracker to be present");
        }

        #[tokio::test]
        async fn test_dht_tracker_option() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let dht = DhtTracker::builder().build().await.unwrap();
            let session = FxSession::builder()
                .config(
                    SessionConfig::builder()
                        .client_name("test")
                        .path(temp_path)
                        .build(),
                )
                .dht_option(Some(dht))
                .default_extensions()
                .build()
                .unwrap();

            // retrieve the dht tracker from the session
            let result = session.dht();
            assert!(result.is_some(), "expected a dht tracker to be present");
        }
    }

    mod lsd {
        use super::*;

        #[tokio::test]
        async fn test_lsd_tracker() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let session = FxSession::builder()
                .config(
                    SessionConfig::builder()
                        .client_name("test")
                        .path(temp_path)
                        .build(),
                )
                .local_service_discovery(
                    LocalServiceDiscovery::new(Ipv4Addr::LOCALHOST.into())
                        .await
                        .unwrap(),
                )
                .default_extensions()
                .build()
                .unwrap();

            // retrieve the dht tracker from the session
            let result = session.local_service_discovery();
            assert!(result.is_some(), "expected a lsd tracker to be present");
        }
    }

    mod tracker_client {
        use super::*;

        #[tokio::test]
        async fn test_tracker_client() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let session = FxSession::builder()
                .config(
                    SessionConfig::builder()
                        .client_name("test")
                        .path(temp_path)
                        .build(),
                )
                .default_extensions()
                .build()
                .unwrap();

            // retrieve the dht tracker from the session
            let result = session.tracker();
            assert!(result.is_some(), "expected a tracker client to be present");
        }
    }

    async fn create_session(temp_path: &str) -> FxSession {
        FxSession::builder()
            .config(
                SessionConfig::builder()
                    .client_name("test")
                    .path(temp_path)
                    .build(),
            )
            .default_extensions()
            .build()
            .expect("expected a session to have been created")
    }
}
