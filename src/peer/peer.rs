use crate::channel::{ChannelReceiver, ChannelSender, Reply, Response};
use crate::merkle::LEAF_BLOCK_SIZE;
use crate::metrics::Metric;
use crate::peer::extension::{ExtensionNumber, ExtensionRegistry, PeerExtension};
use crate::peer::peer_connection::PeerConnection;
use crate::peer::protocol::{CloseReason, UtpStream};
use crate::peer::protocol::{ExtendedHandshake, Handshake, HashRequest, Message, Piece, Request};
use crate::peer::{
    extension, ChokeState, Error, InterestState, Metrics, PeerEvent, PeerHandle, PeerId,
    PeerPriority, Result,
};
use crate::torrent::InnerTorrent;
use crate::torrent_data::DataPool;
use crate::{
    BitVec, CompactIp, PieceBlock, PieceIndex, TorrentError, TorrentEvent, TorrentMetadata,
    TorrentMetadataInfo,
};
use bitmask_enum::bitmask;
use byteorder::BigEndian;
use byteorder::ByteOrder;
use derive_more::Display;
use fx_callback::{Callback, MultiThreadedCallback, Subscription};
use itertools::Itertools;
use log::{debug, error, trace, warn};
use std::cmp::max;
use std::collections::{HashSet, VecDeque};
use std::fmt::{Debug, Display, Formatter};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use std::{io, result};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio::{select, time};
use tokio_util::sync::CancellationToken;

const KEEP_ALIVE_SECONDS: u64 = 90;
const PEER_TICK_INTERVAL: Duration = Duration::from_secs(1);
const MIN_TARGET_QUEUE_LEN: usize = 2;
const MAX_TARGET_QUEUE_LEN: usize = 500;
const REQUEST_QUEUE_TIME: Duration = Duration::from_secs(3);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// The response of a remote peer connection.
#[derive(Debug, PartialEq)]
pub(crate) enum PeerResponse {
    /// The remote peer sent a handshake.
    Handshake(Handshake),
    /// The remote peer sent a message.
    Message(Message),
    /// The remote peer connection encountered an error.
    Error(Error),
    /// The remote peer has closed the connection.
    Closed,
}

/// The underlying stream implementation of a peer connection.
/// This stream is used to connect with, or receive from, a remote peer.
#[derive(Debug)]
pub enum PeerStream {
    /// The peer is a TCP stream
    Tcp(TcpStream),
    /// The peer is a UTP stream
    Utp(UtpStream),
}

impl From<TcpStream> for PeerStream {
    fn from(stream: TcpStream) -> Self {
        Self::Tcp(stream)
    }
}

impl From<UtpStream> for PeerStream {
    fn from(stream: UtpStream) -> Self {
        Self::Utp(stream)
    }
}

/// The underlying network protocol used by the peer to communicate with the remote peer.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ConnectionProtocol {
    Tcp,
    Utp,
    Http,
    Other,
}

impl Display for ConnectionProtocol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// The connection direction type of the peer.
/// This indicates if the initial established connection with the remote peer was an inbound or outbound connection.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConnectionDirection {
    Inbound = 0,
    Outbound = 1,
}

/// The state that a peer is in
#[derive(Debug, Display, Copy, Clone, PartialEq)]
pub enum PeerState {
    /// The peer is currently exchanging the handshake
    #[display("performing peer handshake")]
    Handshake,
    /// The peer is currently trying to retrieve the metadata
    #[display("retrieving metadata")]
    RetrievingMetadata,
    /// The peer is currently paused
    #[display("paused")]
    Paused,
    /// The peer is currently idle
    #[display("idle")]
    Idle,
    #[display("downloading")]
    Downloading,
    #[display("uploading")]
    Uploading,
    #[display("error")]
    Error,
    #[display("closed")]
    Closed,
}

/// The extension flags of the protocol.
/// See BEP4 (<https://www.bittorrent.org/beps/bep_0004.html>) for more info.
///
/// _The known collisions mentioned in BEP4, are ignored within these flags._
#[bitmask(u16)]
#[bitmask_config(vec_debug, flags_iter)]
pub enum ProtocolExtensionFlags {
    /// Azureus Messaging Protocol
    Azureus,
    /// Libtorrent Extension Protocol, aka Extensions
    LTEP,
    /// Extension Negotiation Protocol
    ENP,
    /// BitTorrent DHT
    Dht,
    /// XBT Peer Exchange
    XbtPeerExchange,
    /// suggest, haveall, havenone, reject request, and allow fast extensions
    Fast,
    /// NAT Traversal
    Nat,
    /// hybrid torrent legacy to v2 upgrade
    SupportV2,
}

impl Display for ProtocolExtensionFlags {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut extensions = Vec::new();

        if self.contains(Self::Azureus) {
            extensions.push("Azureus");
        }
        if self.contains(Self::LTEP) {
            extensions.push("LTEP");
        }
        if self.contains(Self::ENP) {
            extensions.push("ENP");
        }
        if self.contains(Self::Dht) {
            extensions.push("DHT");
        }
        if self.contains(Self::XbtPeerExchange) {
            extensions.push("XBT");
        }
        if self.contains(Self::Fast) {
            extensions.push("Fast");
        }
        if self.contains(Self::Nat) {
            extensions.push("Nat");
        }
        if self.contains(Self::SupportV2) {
            extensions.push("SupportV2");
        }

        write!(f, "{}", extensions.join(" | "))
    }
}

/// The remote peer information
#[derive(Debug, Clone, PartialEq)]
pub struct RemotePeer {
    pub peer_id: PeerId,
    pub protocol_extensions: ProtocolExtensionFlags,
    pub extensions: ExtensionRegistry,
    pub client_name: Option<String>,
    /// Indicates if the extended handshake has been received from the remote peer.
    pub extended_handshake: bool,
    /// Indicates that the connection has been upgraded to v2
    pub is_v2: bool,
}

#[derive(Debug, Default, Clone)]
pub struct PeerStats {
    /// The bytes that have been transferred to the peer.
    pub upload: usize,
    /// The bytes that contain actual piece data transferred to the peer.
    pub upload_useful: usize,
    /// The bytes that have been transferred from the peer.
    pub download: usize,
    /// The bytes that contain actual piece data transferred from the peer.
    pub download_useful: usize,
}

/// The client information of a connected peer.
#[derive(Debug, Display, Clone, PartialEq)]
#[display("{}[{}:{}]", id, connection_protocol, addr)]
pub struct PeerClientInfo {
    /// The unique handle of the peer
    pub handle: PeerHandle,
    /// The unique peer id communicated with the remote peer
    pub id: PeerId,
    /// The remote peer address the client is connected to
    pub addr: SocketAddr,
    /// The connection direction of the peer client
    pub connection_type: ConnectionDirection,
    /// The connection protocol of the peer client used for communicating with the remote peer.
    pub connection_protocol: ConnectionProtocol,
}

impl PeerClientInfo {
    /// Get the canonical peer priority (BEP-40) of this peer compared against.
    pub fn peer_priority(&self, other: &Self) -> Option<u32> {
        PeerPriority::from((self, other)).take()
    }
}

impl From<(&PeerClientInfo, &PeerClientInfo)> for PeerPriority {
    fn from(value: (&PeerClientInfo, &PeerClientInfo)) -> Self {
        Self::from((&value.0.addr, &value.1.addr))
    }
}

/// The BitTorrent peer protocol implementation.
/// This [TorrentPeer] exchanges torrent data with remote peers through the specified BEP3 BitTorrent protocol.
///
/// It communicates with remote peers over TCP or uTP, see [PeerConn] for more info.
#[derive(Debug, Display, Clone)]
#[display("{}", client)]
pub struct BitTorrentPeer {
    client: PeerClientInfo,
    metrics: Metrics,
    sender: ChannelSender<PeerCommand>,
    callbacks: MultiThreadedCallback<PeerEvent>,
    cancellation_token: CancellationToken,
}

impl BitTorrentPeer {
    /// Create a new outgoing BitTorrent peer connection for the given network stream.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::net::SocketAddr;
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use tokio::net::TcpStream;
    /// use tokio::runtime::Runtime;
    /// use fx_torrent::torrent::peer::{BitTorrentPeer, PeerId, PeerStream, ProtocolExtensionFlags, Result};
    /// use fx_torrent::torrent::peer::extension::Extension;
    /// use fx_torrent::torrent::TorrentContext;
    ///
    /// async fn create_new_peer(torrent: Arc<TorrentContext>) -> Result<BitTorrentPeer> {
    ///     let peer_id = PeerId::new();
    ///     let addr = SocketAddr::from(([127,0,0,1], 6881));
    ///     let stream = PeerStream::Tcp(TcpStream::connect(addr).await?);
    ///     let protocol_extensions = ProtocolExtensionFlags::LTEP | ProtocolExtensionFlags::Fast;
    ///     let extensions : Vec<Box<dyn Extension>> = vec![];
    ///
    ///     BitTorrentPeer::new_outbound(
    ///         peer_id,
    ///         addr,
    ///         stream,
    ///         torrent,
    ///         protocol_extensions,
    ///         extensions,
    ///         Duration::from_secs(10),
    ///     ).await
    /// }
    /// ```
    pub(crate) async fn new_outbound(
        peer_id: PeerId,
        addr: SocketAddr,
        stream: PeerStream,
        torrent: InnerTorrent,
        data_pool: DataPool,
        protocol_extensions: ProtocolExtensionFlags,
        extensions: Vec<PeerExtension>,
        timeout: Duration,
    ) -> Result<Self> {
        trace!(
            "Torrent {} is trying to create outgoing peer connection to {}",
            torrent,
            addr
        );
        let metrics = Metrics::new();
        let connection = match stream {
            PeerStream::Tcp(stream) => {
                PeerConnection::new_tcp(peer_id, addr, stream, metrics.clone())
            }
            PeerStream::Utp(stream) => {
                PeerConnection::new_utp(peer_id, addr, stream, metrics.clone())
            }
        };

        Self::process_connection_stream(
            peer_id,
            addr,
            connection,
            ConnectionDirection::Outbound,
            torrent,
            data_pool,
            protocol_extensions,
            extensions,
            metrics,
            timeout,
        )
        .await
    }

    /// Try to accept a new incoming BitTorrent peer connection for the given network stream.
    pub(crate) async fn new_inbound(
        peer_id: PeerId,
        addr: SocketAddr,
        stream: PeerStream,
        torrent: InnerTorrent,
        data_pool: DataPool,
        protocol_extensions: ProtocolExtensionFlags,
        extensions: Vec<PeerExtension>,
        timeout: Duration,
    ) -> Result<Self> {
        let metrics = Metrics::new();
        let connection = match stream {
            PeerStream::Tcp(stream) => {
                PeerConnection::new_tcp(peer_id, addr, stream, metrics.clone())
            }
            PeerStream::Utp(stream) => {
                PeerConnection::new_utp(peer_id, addr, stream, metrics.clone())
            }
        };

        trace!(
            "Torrent {} is trying to receive incoming peer connection from {}",
            torrent,
            addr
        );
        select! {
            _ = time::sleep(timeout) => {
                Err(Error::Io(io::Error::new(io::ErrorKind::TimedOut, format!("connection from {} timed out", addr))))
            },
            result = Self::process_connection_stream(
                peer_id,
                addr,
                connection,
                ConnectionDirection::Inbound,
                torrent,
                data_pool,
                protocol_extensions,
                extensions,
                metrics,
                timeout,
            ) => result
        }
    }

    /// Returns the unique handle of the peer.
    pub fn handle(&self) -> &PeerHandle {
        &self.client.handle
    }

    /// Returns the address of the remote peer.
    pub fn addr(&self) -> &SocketAddr {
        &self.client.addr
    }

    /// Returns the metrics of the peer.
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Get the connection type of the peer.
    ///
    /// # Returns
    ///
    /// Returns the connection type of the peer.
    pub fn connection_type(&self) -> ConnectionDirection {
        self.client.connection_type
    }

    /// Retrieve the remote peer id.
    /// This is only available after the handshake with the peer has been completed.
    ///
    /// # Returns
    ///
    /// Returns the remote peer id when the handshake has been completed, else `None`.
    pub async fn remote_id(&self) -> Option<PeerId> {
        self.sender
            .send(|tx| PeerCommand::GetRemoteId { response: tx })
            .await
            .await
            .ok()
            .flatten()
    }

    /// Returns the client information of the peer.
    pub fn client_info(&self) -> &PeerClientInfo {
        &self.client
    }

    /// Get the remote peer information.
    /// This is only available after the handshake with the peer has been completed.
    ///
    /// # Returns
    ///
    /// Returns the remote peer information when the handshake has been completed, else `None`.
    pub async fn remote_peer(&self) -> Option<RemotePeer> {
        self.sender
            .send(|tx| PeerCommand::GetRemotePeer { response: tx })
            .await
            .await
            .ok()
            .flatten()
    }

    /// Get the known supported extensions of the remote peer.
    /// This might still be `None` when the handshake with the peer has not been completed yet.
    ///
    /// # Returns
    ///
    /// Returns the supported extensions of the remote peer.
    pub async fn remote_supported_extensions(&self) -> ProtocolExtensionFlags {
        self.sender
            .send(|tx| PeerCommand::GetRemoteSupportedExtensions { response: tx })
            .await
            .await
            .ok()
            .flatten()
            .unwrap_or(ProtocolExtensionFlags::none())
    }

    /// Set the choke state of the client peer.
    pub async fn set_choke_state(&self, state: ChokeState) {
        self.sender
            .fire_and_forget(PeerCommand::SetChokeState { state })
            .await
    }

    /// Returns the choke state of the client peer,
    /// indicating if data can be sent to the remote peer or not.
    pub async fn choke_state(&self) -> ChokeState {
        self.sender
            .send(|tx| PeerCommand::GetChokeState { response: tx })
            .await
            .await
            .ok()
            .unwrap_or(ChokeState::Choked)
    }

    /// Get the remote peer choke state.
    pub async fn remote_choke_state(&self) -> ChokeState {
        self.sender
            .send(|tx| PeerCommand::GetRemoteChokeState { response: tx })
            .await
            .await
            .ok()
            .unwrap_or(ChokeState::Choked)
    }

    /// Get the interested state of the remote peer.
    pub async fn remote_interest_state(&self) -> InterestState {
        self.sender
            .send(|tx| PeerCommand::GetRemoteInterestState { response: tx })
            .await
            .await
            .ok()
            .unwrap_or(InterestState::NotInterested)
    }

    /// Verify if the remote peer has the given piece.
    ///
    /// # Arguments
    ///
    /// * `piece` - The piece index that should be checked.
    ///
    /// # Returns
    ///
    /// Returns true when the remote peer has the piece available, else false.
    pub async fn remote_has_piece(&self, piece: &PieceIndex) -> bool {
        self.sender
            .send(|tx| PeerCommand::GetRemoteHasPiece {
                piece: *piece,
                response: tx,
            })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the bitfield of the pieces that have been completed by the client.
    pub async fn client_piece_bitfield(&self) -> BitVec {
        self.sender
            .send(|tx| PeerCommand::GetClientBitfield { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the bitfield of the pieces that the remote peer has.
    pub async fn remote_piece_bitfield(&self) -> BitVec {
        self.sender
            .send(|tx| PeerCommand::GetRemoteBitfield { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the bitfield of the fast pieces for the remote peer.
    ///
    /// This bitfield indicates the pieces which are allowed to be downloaded,
    /// even when [Peer::remote_choke_state] returns [ChokeState::Choked].
    pub async fn remote_fast_bitfield(&self) -> BitVec {
        self.sender
            .send(|tx| PeerCommand::GetRemoteFastBitfield { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns `true` if the remote peer has pieces that are wanted by the torrent
    /// and are not yet being requested.
    pub async fn has_wanted_pieces(&self) -> bool {
        self.sender
            .send(|tx| PeerCommand::HasWantedPieces { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Check if the remote peer is a seed.
    /// This means that the remote peer has all pieces available and is seeding the torrent.
    pub async fn is_seed(&self) -> bool {
        self.sender
            .send(|tx| PeerCommand::IsSeed { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the active state of the peer.
    pub async fn state(&self) -> PeerState {
        self.sender
            .send(|tx| PeerCommand::GetState { response: tx })
            .await
            .await
            .unwrap_or(PeerState::Closed)
    }

    /// Verify if the peer supports the given extension name with the remote peer.
    /// There is a plausibility for a "false-negative" when the extended handshake has not yet been executed.
    ///
    /// # Arguments
    ///
    /// * `extension_name` - The name of the extension to check for
    ///
    /// # Returns
    ///
    /// Returns true when the extension is supported, else false
    pub async fn supports_extension<S: Into<String>>(&self, extension_name: S) -> bool {
        // both the remote peer and this peer should support the given extension name
        self.sender
            .send(|tx| PeerCommand::IsExtensionSupported {
                name: extension_name.into(),
                response: tx,
            })
            .await
            .await
            .unwrap_or_default()
    }

    /// Return the remote [ExtensionNumber] for the given extension name, if found.
    /// Returns [None] when the extension is not supported by the remote peer.
    ///
    /// Related [BitTorrentPeer::supports_extension].
    pub async fn remote_extension_number<S: Into<String>>(
        &self,
        extension_name: S,
    ) -> Option<ExtensionNumber> {
        self.sender
            .send(|tx| PeerCommand::FindExtensionNumber {
                name: extension_name.into(),
                response: tx,
            })
            .await
            .await
            .ok()
            .flatten()
    }

    /// Returns the total number of pending requests to the remote peer.
    pub async fn client_pending_requests_len(&self) -> usize {
        self.sender
            .send(|tx| PeerCommand::GetClientPendingRequestsLen { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the total number of pending requests from the remote peer.
    pub async fn remote_pending_requests_len(&self) -> usize {
        self.sender
            .send(|tx| PeerCommand::GetRemotePendingRequestsLen { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the suggested pieces by the remote peer for downloading.
    pub async fn suggested_pieces(&self) -> Vec<PieceIndex> {
        self.sender
            .send(|tx| PeerCommand::SuggestedPieces { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Request one or more piece blocks from the remote peer.
    pub async fn request(&self, blocks: &[PieceBlock]) -> Result<()> {
        self.sender
            .send(|tx| PeerCommand::Request {
                blocks: blocks.to_vec(),
                response: tx,
            })
            .await
            .await
    }

    /// Returns the target number of requests which should be queued for the remote peer.
    pub async fn target_request_queue_len(&self) -> usize {
        self.sender
            .send(|tx| PeerCommand::TargetRequestQueueLen { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Try to HolePunch the given target peer address.
    /// The response is completed once a `connect` message has been received from the relaying peer.
    pub(crate) async fn holepunch(
        &self,
        addr: SocketAddr,
    ) -> Response<SocketAddr, extension::Error> {
        self.sender
            .send(|tx| PeerCommand::SendHolePunch {
                target: addr,
                response: tx,
            })
            .await
    }

    /// Send the given message to the remote peer.
    pub(crate) async fn send(&self, message: Message) -> Result<()> {
        self.sender
            .send(|tx| PeerCommand::SendMessage {
                message,
                response: tx,
            })
            .await
            .await
    }

    /// Close the peer connection.
    pub(crate) async fn close(&self) {
        let _ = self
            .sender
            .send(|tx| PeerCommand::Close { response: tx })
            .await
            .await;
    }

    async fn process_connection_stream(
        peer_id: PeerId,
        addr: SocketAddr,
        connection: PeerConnection,
        connection_type: ConnectionDirection,
        torrent: InnerTorrent,
        data_pool: DataPool,
        protocol_extensions: ProtocolExtensionFlags,
        extensions: Vec<PeerExtension>,
        metrics: Metrics,
        timeout: Duration,
    ) -> Result<Self> {
        let (command_sender, command_receiver) = channel!(128);
        metrics.client_choked.set(true);
        metrics.remote_choked.set(true);

        let torrent_event_receiver = torrent.subscribe();
        let mut context = PeerContext::new(
            peer_id,
            addr,
            connection,
            connection_type,
            torrent,
            data_pool,
            protocol_extensions,
            extensions.as_slice(),
            metrics,
            timeout,
        )
        .await?;
        let peer = Self {
            client: context.client.clone(),
            metrics: context.metrics.clone(),
            sender: command_sender,
            callbacks: context.callbacks.clone(),
            cancellation_token: context.cancellation_token.clone(),
        };

        // exchange the handshake with the remote peer
        context.exchange_handshake().await?;

        // run the peer context in a separate task
        spawn!("PeerContext::run", async move {
            context
                .run(command_receiver, torrent_event_receiver, extensions)
                .await
        });

        Ok(peer)
    }
}

impl Callback<PeerEvent> for BitTorrentPeer {
    fn subscribe(&self) -> Subscription<PeerEvent> {
        self.callbacks.subscribe()
    }
}

impl PartialEq for BitTorrentPeer {
    fn eq(&self, other: &Self) -> bool {
        self.client == other.client
    }
}

/// The internal peer commands executed on the main loop of the peer.
#[derive(Debug)]
enum PeerCommand {
    /// Returns the remote peer id, if known.
    GetRemoteId {
        response: Reply<Option<PeerId>>,
    },
    /// Returns the remote peer information, if known.
    GetRemotePeer {
        response: Reply<Option<RemotePeer>>,
    },
    /// Returns the supported extensions of the remote peer, if known.
    GetRemoteSupportedExtensions {
        response: Reply<Option<ProtocolExtensionFlags>>,
    },
    SetChokeState {
        state: ChokeState,
    },
    /// Returns the choke state of the client peer.
    GetChokeState {
        response: Reply<ChokeState>,
    },
    /// Returns the remote peer choke state, if known.
    GetRemoteChokeState {
        response: Reply<ChokeState>,
    },
    /// Returns the remote peer interested state, if known.
    GetRemoteInterestState {
        response: Reply<InterestState>,
    },
    /// Returns `true` if the remote peer has the given piece, else `false`.
    GetRemoteHasPiece {
        piece: PieceIndex,
        response: Reply<bool>,
    },
    /// Returns `true` if the remote peer has pieces which are wanted by the torrent, else `false`.
    HasWantedPieces {
        response: Reply<bool>,
    },
    /// Returns the bitfield (pieces) of the remote peer, if known.
    GetRemoteBitfield {
        response: Reply<BitVec>,
    },
    /// Returns the bitfield (pieces) of the fast pieces for the remote peer, if known.
    GetRemoteFastBitfield {
        response: Reply<BitVec>,
    },
    /// Returns `true` if the remote peer is a seed, else `false`.
    IsSeed {
        response: Reply<bool>,
    },
    /// Returns the state of the peer.
    GetState {
        response: Reply<PeerState>,
    },
    /// Returns the bitfield of the client pieces that have been completed.
    GetClientBitfield {
        response: Reply<BitVec>,
    },
    /// Returns `true` if the peer and remote peer support the given extension, else `false`.
    IsExtensionSupported {
        name: String,
        response: Reply<bool>,
    },
    /// Returns the extension number of the given extension name if found, else `None`.
    FindExtensionNumber {
        name: String,
        response: Reply<Option<ExtensionNumber>>,
    },
    /// Returns the total number of pending requests to the remote peer.
    GetClientPendingRequestsLen {
        response: Reply<usize>,
    },
    /// Returns the total number of pending requests from the remote peer.
    GetRemotePendingRequestsLen {
        response: Reply<usize>,
    },
    /// Returns the suggested pieces of the remote peer.
    SuggestedPieces {
        response: Reply<Vec<PieceIndex>>,
    },
    /// Request one or more piece blocks from the remote peer.
    Request {
        blocks: Vec<PieceBlock>,
        response: Reply<Result<()>>,
    },
    TargetRequestQueueLen {
        response: Reply<usize>,
    },
    /// Send the given message to the remote peer.
    SendMessage {
        message: Message,
        response: Reply<Result<()>>,
    },
    /// Send a holepunch request to the remote peer.
    SendHolePunch {
        target: SocketAddr,
        response: Reply<extension::Result<SocketAddr>>,
    },
    /// Close the peer connection.
    Close {
        response: Reply<()>,
    },
}

#[derive(Debug, Display)]
#[display("{}", client)]
pub struct PeerContext {
    /// The client information of the peer
    client: PeerClientInfo,
    /// The remote peer information, known after the initial handshake.
    remote: Option<RemotePeer>,
    torrent: InnerTorrent,
    data_pool: DataPool,
    /// The state of the client peer connection with the remote peer
    state: PeerState,
    /// The peer client supported/enabled protocol extensions
    protocol_extensions: ProtocolExtensionFlags,
    /// The metrics of the peer
    metrics: Metrics,

    /// The client choke state
    client_choke_state: ChokeState,
    /// The choke state of the remote peer
    remote_choke_state: ChokeState,

    /// The client interest state for the pieces of the remote peer
    client_interest_state: InterestState,
    /// The interest state of the remote peer for our available pieces
    remote_interest_state: InterestState,

    /// The extensions which are support by the application
    /// These are immutable once the peer has been created
    extension_registry: ExtensionRegistry,

    /// The torrent pieces
    client_pieces: BitVec,
    /// The pieces of the remote peer
    remote_pieces: BitVec,
    /// The allowed fast pieces of the remote peer
    remote_fast_pieces: HashSet<PieceIndex>,
    /// The pieces which have been suggested by the remote peer for downloading
    remote_suggested_pieces: HashSet<PieceIndex>,

    /// The number of requests we should queue up for the remote peer
    target_queue_len: usize,
    /// The queue of requests which should be sent to the remote peer
    download_queue: VecDeque<PieceBlock>,
    /// The client peer pending requests to the remote torrent.
    /// These have been requested, but not yet received.
    pending_requests: Vec<PendingRequest>,
    /// The remote pending requests for this client.
    /// These are the requests the remote peer is interested in
    remote_pending_requests: Vec<Request>,

    /// The underlying peer connection
    connection: PeerConnection,

    /// The callbacks which are triggered by this peer when an event is raised
    callbacks: MultiThreadedCallback<PeerEvent>,
    /// The timeout duration of the connection.
    timeout: Duration,
    /// The cancellation token to cancel any async task within this peer on closure
    cancellation_token: CancellationToken,
}

impl PeerContext {
    /// Create a new peer context instance.
    pub(crate) async fn new(
        peer_id: PeerId,
        addr: SocketAddr,
        connection: PeerConnection,
        connection_type: ConnectionDirection,
        torrent: InnerTorrent,
        data_pool: DataPool,
        protocol_extensions: ProtocolExtensionFlags,
        extensions: &[PeerExtension],
        metrics: Metrics,
        timeout: Duration,
    ) -> Result<Self> {
        let peer_handle = PeerHandle::new();
        let total_pieces = data_pool.num_of_pieces().await;
        let extension_registry = Self::create_extension_registry(extensions);

        Ok(Self {
            client: PeerClientInfo {
                handle: peer_handle,
                id: peer_id,
                addr,
                connection_type,
                connection_protocol: connection.protocol(),
            },
            // the remote information is unknown until the handshake has been completed
            remote: None,
            torrent,
            data_pool,
            state: PeerState::Handshake,
            protocol_extensions,
            metrics,
            // connections should always start in the choked state
            client_choke_state: ChokeState::Choked,
            remote_choke_state: ChokeState::Choked,
            // connections should always start in the not interested state
            client_interest_state: InterestState::NotInterested,
            remote_interest_state: InterestState::NotInterested,
            extension_registry,
            client_pieces: BitVec::repeat(false, total_pieces),
            remote_pieces: BitVec::repeat(false, total_pieces),
            remote_fast_pieces: Default::default(),
            remote_suggested_pieces: Default::default(),
            target_queue_len: 4,
            download_queue: VecDeque::new(),
            pending_requests: vec![],
            remote_pending_requests: Vec::new(),
            connection,
            callbacks: MultiThreadedCallback::new(),
            cancellation_token: CancellationToken::new(),
            timeout,
        })
    }

    /// Run the main loop of peer.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn run(
        &mut self,
        mut command_receiver: ChannelReceiver<PeerCommand>,
        mut torrent_event_receiver: Subscription<TorrentEvent>,
        mut extensions: Vec<PeerExtension>,
    ) {
        // Try to send the initial message to the remote peer
        if let Err(e) = self.send_initial_messages().await {
            debug!("Peer {} failed to send initial messages, {}", self, e);
            self.update_state(PeerState::Error);
            return;
        }

        let mut interval = time::interval(PEER_TICK_INTERVAL);

        loop {
            select! {
                _ = self.cancellation_token.cancelled() => break,
                _ = time::sleep(Duration::from_secs(KEEP_ALIVE_SECONDS)) => self.send_keep_alive().await,
                Some(event) = self.connection.recv() => self.on_reader_event(event, extensions.as_mut_slice()).await,
                Some(event) = command_receiver.recv() => self.on_command(event, extensions.as_mut_slice()).await,
                Ok(event) = torrent_event_receiver.recv() => self.on_torrent_event(&*event).await,
                _ = interval.tick() => self.on_tick(extensions.as_mut_slice(), PEER_TICK_INTERVAL).await,
            }
        }

        self.update_state(PeerState::Closed);
        trace!("Peer {} main loop ended", self);
    }

    /// Try to exchange the handshake with the remote peer.
    /// Returns an error if the handshake failed to by exchanged with the remote peer.
    async fn exchange_handshake(&mut self) -> Result<()> {
        if self.client.connection_type == ConnectionDirection::Outbound {
            // as this is an outgoing connection, we're the once who initiate the handshake
            self.send_handshake().await?;
        }

        // retrieve the incoming handshake from the reader
        // as the handshake is always 68 bytes long, we request a buffer of 68 bytes from the reader
        trace!("Peer {} is awaiting the remote handshake", self);
        let handshake = self.try_receive_handshake().await?;
        if let Err(reason) = self.validate_handshake(handshake).await {
            self.close(reason).await;
            return Err(Error::Handshake(self.client.addr, format!("{:?}", reason)));
        };

        if self.client.connection_type == ConnectionDirection::Inbound {
            // as this is an incoming connection, we need to send our own handshake after receiving the peer handshake
            self.send_handshake().await?;
        }

        Ok(())
    }

    /// Returns the address of the remote peer.
    pub fn addr(&self) -> &SocketAddr {
        &self.client.addr
    }

    /// Returns the protocol used by the connection to the peer.
    pub fn connection_protocol(&self) -> ConnectionProtocol {
        self.connection.protocol()
    }

    /// Returns a reference the torrent this peer belongs to.
    pub(crate) fn torrent(&self) -> &InnerTorrent {
        &self.torrent
    }

    /// Returns the current state of the peer.
    pub fn state(&self) -> &PeerState {
        &self.state
    }

    /// Returns the client choke state of the peer.
    pub fn choke_state(&self) -> &ChokeState {
        &self.client_choke_state
    }

    /// Returns the remote peer id, if known.
    pub fn remote_id(&self) -> Option<PeerId> {
        self.remote.as_ref().map(|e| e.peer_id.clone())
    }

    /// Returns the remote peer information, if known.
    ///
    /// The data of the remote peer is only known after the handshake has been exchanged
    /// and validated.
    pub fn remote_peer(&self) -> Option<&RemotePeer> {
        self.remote.as_ref()
    }

    /// Get the supported extension registry of the remote peer.
    ///
    /// # Returns
    ///
    /// Returns the extension registry of the remote peer if known, else `None`.
    pub fn remote_extension_registry(&self) -> Option<ExtensionRegistry> {
        self.remote.as_ref().map(|e| e.extensions.clone())
    }

    /// Get the supported protocol extensions of the remote peer.
    /// This might still be `None` when the handshake with the peer has not been completed yet.
    pub fn remote_protocol_extensions(&self) -> Option<ProtocolExtensionFlags> {
        self.remote.as_ref().map(|e| e.protocol_extensions.clone())
    }

    /// Returns the available pieces of the remote peer as a bit vector.
    /// It might return an empty bit vector when the handshake has not been completed yet.
    pub fn remote_piece_bitfield(&self) -> &BitVec {
        &self.remote_pieces
    }

    /// Returns the bitfield of pieces which are allowed to be downloaded, even when choked.
    pub fn remote_fast_bitfield(&self) -> BitVec {
        let mut bitfield = BitVec::repeat(false, self.remote_pieces.len());

        // early exit, if the fast protocol is not supported
        if !self.is_protocol_enabled(ProtocolExtensionFlags::Fast) {
            return bitfield;
        }

        for piece in &self.remote_fast_pieces {
            bitfield.set(*piece, true);
        }

        bitfield
    }

    /// Check if the remote has all pieces available.
    /// The remote has all pieces if either an `HaveAll` message or completed `Bitfield` has been received by the remote.
    ///
    /// It returns true when the remote has all pieces and the metadata is known, else false.
    pub async fn remote_has_all_pieces(&self) -> bool {
        let torrent_total_pieces = self.data_pool.num_of_pieces().await;
        // the received bitfield can be greater than the actual total pieces due to byte alignment
        self.remote_pieces.len() > 0
            && self.remote_pieces.len() >= torrent_total_pieces
            && self.remote_pieces.all()
    }

    /// Check if a specific protocol extension is supported by the remote peer.
    /// If the client or the remote peer don't support the given extension, `false` is returned.
    pub fn is_protocol_enabled(&self, extension: ProtocolExtensionFlags) -> bool {
        self.protocol_extensions.contains(extension)
            && self
                .remote
                .as_ref()
                .map(|e| e.protocol_extensions.contains(extension))
                .unwrap_or(false)
    }

    /// Check if the client peer is currently interested in pieces from the remote peer.
    pub fn is_client_interested(&self) -> bool {
        self.client_interest_state == InterestState::Interested
    }

    /// Check if fast requests are allowed for the given piece.
    /// It returns true when fast requests are allowed for the given piece, else false.
    fn is_fast_allowed(&self, piece: &PieceIndex) -> bool {
        self.remote_fast_pieces.contains(piece)
    }

    /// Get the known metadata from the torrent.
    /// This info is requested from the torrent that created this peer.
    pub async fn metadata(&self) -> Result<TorrentMetadata> {
        Ok(self.torrent.metadata().await?)
    }

    /// Returns the completed pieces bitfield of the torrent.
    pub async fn torrent_bitfield(&self) -> BitVec {
        self.data_pool.bitfield().await
    }

    /// Check if the remote peer supports v2.
    pub fn is_v2_supported(&self) -> bool {
        if let Some(remote) = self.remote.as_ref() {
            return remote.is_v2.clone();
        }

        false
    }

    /// Update the underlying torrent metadata.
    /// This method can be used by extensions to update the torrent metadata when the current
    /// connection is based on a magnet link.
    pub async fn set_torrent_metadata(&self, metadata: TorrentMetadataInfo) {
        self.torrent.set_metadata(metadata).await;
    }

    /// Get the client peer extensions registry.
    /// This is the registry of our own client.
    ///
    /// # Returns
    ///
    /// Returns a reference to the client extension registry.
    pub fn client_extension_registry(&self) -> &ExtensionRegistry {
        &self.extension_registry
    }

    /// Check if the remote peer has wanted piece data that are not yet being requested.
    /// If it least one piece is available by the remote peer and wanted by the torrent, it returns `true`.
    pub async fn has_wanted_piece(&self) -> bool {
        let remote_has_all_pieces = self.remote_has_all_pieces().await;
        let wanted_pieces = self.data_pool.wanted_pieces().await;

        wanted_pieces.into_iter().any(|piece| {
            // check if the remote peer has this piece
            let has_piece = remote_has_all_pieces
                || self
                    .remote_pieces
                    .get(piece.index)
                    .map(|e| *e)
                    .unwrap_or_default();
            if !has_piece {
                return false;
            }

            // check if at least one wanted piece is not in the download queue and pending requests
            let in_queue = self.download_queue.iter().any(|e| &e.piece == &piece.index);
            let in_pending = self
                .pending_requests
                .iter()
                .any(|e| &e.request.index == &piece.index);
            !in_queue && !in_pending
        })
    }

    /// Process an incoming peer reader event.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn on_reader_event(&mut self, event: PeerResponse, extensions: &mut [PeerExtension]) {
        match event {
            PeerResponse::Closed => self.close(CloseReason::None).await,
            PeerResponse::Message(message) => {
                if let Message::ExtendedPayload(extension_number, payload) = message {
                    trace!(
                        "Trying to find extension for extended payload number {}",
                        extension_number
                    );
                    if let Some(extension) = self
                        .find_extension_by_number(extension_number, extensions)
                        .await
                    {
                        trace!(
                            "Peer {} is processing extension {} message payload",
                            self,
                            extension.name(),
                        );
                        if let Err(e) = extension.on_message(payload.as_ref(), self).await {
                            error!(
                                "Peer {} failed to process extension {} message, {}",
                                self,
                                extension.name(),
                                e
                            );
                        }
                    } else {
                        warn!(
                            "Peer {} received unsupported extension message for extension number {}",
                            self, extension_number
                        );
                    }
                } else {
                    self.on_message_received(message).await
                }
            }
            PeerResponse::Error(e) => {
                debug!("Peer {} encountered an error, {}", self, e);
                self.update_state(PeerState::Error);
            }
            _ => {}
        }
    }

    async fn on_message_received(&mut self, message: Message) {
        debug!("Peer {} received remote message {:?}", self, message);
        match message {
            Message::KeepAlive => {
                trace!("Peer {} received keep alive", self);
            }
            Message::Choke => {
                self.update_remote_peer_choke_state(ChokeState::Choked)
                    .await
            }
            Message::Unchoke => {
                self.update_remote_peer_choke_state(ChokeState::UnChoked)
                    .await
            }
            Message::Interested => {
                self.update_remote_peer_interest_state(InterestState::Interested)
                    .await
            }
            Message::NotInterested => {
                self.update_remote_peer_interest_state(InterestState::NotInterested)
                    .await
            }
            Message::Have(piece) => self.set_remote_has_piece(piece as PieceIndex, true).await,
            Message::HaveAll => self.update_remote_fast_have(true).await,
            Message::HaveNone => self.update_remote_fast_have(false).await,
            Message::Bitfield(pieces) => self.update_remote_pieces(pieces).await,
            Message::Request(request) => self.add_remote_pending_request(request).await,
            Message::RejectRequest(request) => self.handle_rejected_client_request(request).await,
            Message::Cancel(request) => self.cancel_remote_pending_request(request).await,
            Message::Suggest(piece) => self.on_piece_suggested(piece as PieceIndex).await,
            Message::AllowedFast(piece) => self.remote_fast_piece(piece as PieceIndex).await,
            Message::Piece(piece) => self.on_piece_data_received(piece).await,
            Message::ExtendedHandshake(handshake) => {
                self.update_extended_handshake(handshake).await
            }
            Message::HashRequest(request) => self.handle_hash_request(request).await,
            _ => warn!("Message not yet implemented for {:?}", message),
        }
    }

    /// Process a pending request requested by the remote peer.
    /// This tries to retrieve the requested data from the torrent.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn on_remote_pending_request(&mut self, request: Request) {
        // check if the client is choked, if so, we reject the request
        // this can happen if the client choke's while the request was still queued in the command channel
        if self.client_choke_state == ChokeState::Choked {
            self.send_reject_request(request).await;
            return;
        }

        // check if the piece request is valid
        // if not, reject the request
        if !self.validate_piece_request(&request).await {
            debug!("Peer {} received invalid piece request {:?}", self, request);
            self.send_reject_request(request).await;
            return;
        }

        if self.state != PeerState::Uploading {
            self.update_state(PeerState::Uploading);
        }

        let request_end = request.begin + request.length;
        match self
            .torrent
            .read_piece_bytes(&request.index, request.begin..request_end)
            .await
        {
            Ok(data) => {
                let data_len = data.len();
                match self
                    .send(Message::Piece(Piece {
                        index: request.index,
                        begin: request.begin,
                        data,
                    }))
                    .await
                {
                    Ok(_) => {
                        debug!(
                            "Peer {} sent piece {} data block (offset {}, size {}) to remote peer",
                            self, request.index, request.begin, data_len
                        );
                        self.metrics.bytes_out_useful.inc_by(data_len as u64);
                    }
                    Err(e) => warn!(
                        "Peer {} failed to sent piece {} data part (size {}) to remote peer, {}",
                        self, request.index, data_len, e
                    ),
                }
            }
            Err(e) => {
                warn!(
                    "Peer {} failed read piece {} data, {}",
                    self, request.index, e
                );
                self.send_reject_request(request).await;
            }
        }
    }

    /// Handle an event that has been triggered by the [Torrent].
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn on_torrent_event(&mut self, event: &TorrentEvent) {
        match event {
            TorrentEvent::PiecesChanged(_) => {
                trace!("Peer {} updating client piece bitfield", self);
                // retrieve the torrent pieces bitfield and store it as the client bitfield
                let piece_bitfield = self.data_pool.bitfield().await;
                let bitfield_len = piece_bitfield.len();
                self.client_pieces = piece_bitfield;

                // extend the remote pieces bitfield if needed
                if self.remote_pieces.len() < bitfield_len {
                    self.remote_pieces.resize(bitfield_len, false);
                }

                self.determine_client_interest_state().await;
            }
            TorrentEvent::PiecePrioritiesChanged => {
                self.determine_client_interest_state().await;
            }
            TorrentEvent::OptionsChanged => {
                self.determine_client_interest_state().await;
            }
            TorrentEvent::PieceCompleted(piece) => {
                self.update_client_piece_availability(piece).await;
            }
            _ => {}
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn on_tick(&mut self, extensions: &mut [PeerExtension], interval: Duration) {
        let start = Instant::now();

        self.on_stats_update(interval);
        self.on_extensions_tick(extensions, interval).await;
        self.send_queued_requests().await;
        self.process_remote_pending_requests().await;
        self.cleanup_pending_requests().await;

        let elapsed = start.elapsed();
        trace!(
            "Peer {} tick took {:.3}ms",
            self,
            elapsed.as_secs_f64() * 1000.0
        );
    }

    async fn process_remote_pending_requests(&mut self) {
        let len = self.remote_pending_requests.len().min(4);
        for pending_request in self.remote_pending_requests.drain(..len).collect_vec() {
            self.on_remote_pending_request(pending_request).await;
        }
    }

    /// Clean up timed-out block requests sent to the remote peer.
    async fn cleanup_pending_requests(&mut self) {
        let mut timed_out_blocks = vec![];
        self.pending_requests.retain_mut(|pending| {
            if pending.sent.elapsed() > REQUEST_TIMEOUT {
                timed_out_blocks.push(pending.block);
                return false;
            }

            true
        });

        for block in timed_out_blocks {
            self.torrent
                .piece_block_rejected(&self.client.handle, &block)
                .await;
        }
    }

    /// Process a request which has been rejected by the remote peer.
    /// This can be the case when we've request piece data that is no longer available, or the remote peer cannot serve it at the moment.
    async fn handle_rejected_client_request(&mut self, request: Request) {
        debug!("Peer {} remote rejected request {:?}", self, request);
        self.remove_client_pending_request(&request).await;

        if let Some(block) = self
            .data_pool
            .find_piece_block(&request.index, request.begin)
            .await
        {
            self.torrent
                .piece_block_rejected(&self.client.handle, &block)
                .await;
        }
    }

    /// Handle a received piece data message
    async fn on_piece_data_received(&mut self, piece: Piece) {
        let request = if let Some(request) = self
            .remove_client_pending_request(&Request::from(&piece))
            .await
        {
            request
        } else {
            debug!(
                "Received piece {} data from peer {} for an unwanted (not queued) request",
                piece.index, self
            );
            return;
        };

        self.metrics.bytes_in_useful.inc_by(piece.data.len() as u64);
        trace!("Received piece data for {:?} from {}", request, self);
        if let Some(block) = self
            .data_pool
            .find_piece_block(&piece.index, piece.begin)
            .await
        {
            let data_size = piece.data.len();
            if block.length == data_size {
                self.torrent
                    .piece_block_received(&self.client.handle, &block, piece.data)
                    .await;
            } else {
                debug!(
                "Peer {} received invalid piece part {:?} data, received data length {}, expected length {}",
                    self,
                    block,
                    piece.data.len(),
                    data_size
                );

                self.torrent
                    .piece_block_rejected(&self.client.handle, &block)
                    .await;
            }
        } else {
            debug!(
                "Received piece {} data from peer {} for a part that is unknown to the torrent",
                piece.index, self
            );
        }

        // recalculate the target requests queue length of the peer
        self.update_target_request_queue_len();

        // check if the download queue is empty and the remote client is still unchoked
        // if so, request additional pieces to be picked for this peer connection
        if self.remote_choke_state == ChokeState::UnChoked && self.download_queue.is_empty() {
            self.torrent.pick_pieces(&self.client.handle).await;
        }
    }

    /// Handle a received hash request from the remote peer.
    async fn handle_hash_request(&self, _request: HashRequest) {
        // check if the torrent hash is a v2
        let metadata = match timeout(Duration::from_millis(500), self.torrent.metadata())
            .await
            .map_err(|_| TorrentError::Timeout)
            .flatten()
        {
            Ok(metadata) => metadata,
            Err(e) => {
                trace!("Peer {} failed to retrieve metadata, {}", self, e);
                return;
            }
        };
        let metadata_version = metadata.metadata_version().unwrap_or(0);
        if metadata_version != 2 {
            warn!(
                "Peer {} is unable to handle hash request for torrent with metadata version {}",
                self, metadata_version
            );
            return;
        }
    }

    /// Handle an internal peer command event.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn on_command(&mut self, command: PeerCommand, extensions: &mut [PeerExtension]) {
        match command {
            PeerCommand::GetRemoteId { response } => response.send(self.remote_id()),
            PeerCommand::GetRemotePeer { response } => response.send(self.remote_peer().cloned()),
            PeerCommand::GetRemoteSupportedExtensions { response } => {
                response.send(self.remote.as_ref().map(|e| e.protocol_extensions))
            }
            PeerCommand::SetChokeState { state } => self.update_client_choke_state(state).await,
            PeerCommand::GetChokeState { response } => response.send(self.client_choke_state),
            PeerCommand::GetRemoteChokeState { response } => response.send(self.remote_choke_state),
            PeerCommand::GetRemoteInterestState { response } => {
                response.send(self.remote_interest_state)
            }
            PeerCommand::GetRemoteHasPiece { piece, response } => response.send(
                self.remote_pieces
                    .get(piece)
                    .map(|bit| *bit)
                    .unwrap_or_default(),
            ),
            PeerCommand::HasWantedPieces { response } => {
                response.send(self.has_wanted_piece().await)
            }
            PeerCommand::GetRemoteBitfield { response } => {
                response.send(self.remote_piece_bitfield().clone())
            }
            PeerCommand::GetRemoteFastBitfield { response } => {
                response.send(self.remote_fast_bitfield())
            }
            PeerCommand::IsSeed { response } => response.send(self.remote_has_all_pieces().await),
            PeerCommand::GetState { response } => response.send(self.state),
            PeerCommand::GetClientBitfield { response } => {
                response.send(self.client_pieces.clone())
            }
            PeerCommand::IsExtensionSupported { name, response } => {
                response.send(self.is_extension_supported(name.as_str()))
            }
            PeerCommand::FindExtensionNumber { name, response } => {
                response.send(self.find_remote_extension_number(name.as_str()))
            }
            PeerCommand::GetClientPendingRequestsLen { response } => {
                response.send(self.download_queue.len() + self.pending_requests.len())
            }
            PeerCommand::GetRemotePendingRequestsLen { response } => {
                response.send(self.remote_pending_requests.len())
            }
            PeerCommand::SuggestedPieces { response } => {
                response.send(self.remote_suggested_pieces.iter().cloned().collect())
            }
            PeerCommand::Request { blocks, response } => {
                response.send(self.on_request(blocks).await)
            }
            PeerCommand::TargetRequestQueueLen { response } => {
                response.send(self.target_request_queue_len())
            }
            PeerCommand::SendMessage { message, response } => {
                response.send(self.send(message).await)
            }
            PeerCommand::SendHolePunch { target, response } => {
                self.send_holepunch(target, response, extensions).await
            }
            PeerCommand::Close { response } => {
                response.send(self.close(CloseReason::None).await);
            }
        }
    }

    /// Returns `true` if the extension is supported by both the client- and remote peer.
    pub fn is_extension_supported(&self, name: &str) -> bool {
        self.extension_registry.contains_key(name)
            && self
                .remote
                .as_ref()
                .map_or(false, |e| e.extensions.contains_key(name))
    }

    /// Check if v2 hashes should be requested for the current torrent.
    async fn should_request_hashes(&self) -> bool {
        let metadata = match timeout(Duration::from_millis(500), self.torrent.metadata())
            .await
            .map_err(|_| TorrentError::Timeout)
            .flatten()
        {
            Ok(metadata) => metadata,
            Err(e) => {
                trace!("Peer {} failed to retrieve metadata, {}", self, e);
                return false;
            }
        };
        if let Some(metadata_version) = metadata.metadata_version() {
            return metadata_version == 2 && self.is_v2_supported();
        }

        false
    }

    /// Request any missing hashes from the remote peer.
    async fn request_missing_hashes(&self) {
        let metadata = match timeout(Duration::from_millis(500), self.torrent.metadata())
            .await
            .map_err(|_| TorrentError::Timeout)
            .flatten()
        {
            Ok(metadata) => metadata,
            Err(e) => {
                trace!("Peer {} failed to retrieve metadata, {}", self, e);
                return;
            }
        };
        if let Some(info) = metadata.info {
            trace!("Peer {} is requesting missing v2 hashes", self);
            let piece_length = info.piece_length as usize;
            let _base_layer = (piece_length + LEAF_BLOCK_SIZE - 1) / LEAF_BLOCK_SIZE;
        } else {
            warn!(
                "Peer {} is unable to request missing hashes, torrent metadata info is unknown",
                self
            );
        }
    }

    /// Determine if our peer client is interested in pieces from the remote peer.
    async fn determine_client_interest_state(&mut self) {
        let state: InterestState;
        // FIXME: torrent should inform the peer about the state
        let is_download_allowed = true;

        // check if downloading is allowed by the torrent
        if is_download_allowed {
            let has_wanted_pieces = self.has_wanted_piece().await;
            if has_wanted_pieces {
                state = InterestState::Interested;
            } else {
                state = InterestState::NotInterested;
            }
        } else {
            state = InterestState::NotInterested;
        }

        self.update_client_interest_state(state).await;
    }

    /// Try to receive/read the incoming handshake from the remote peer.
    async fn try_receive_handshake(&mut self) -> Result<Handshake> {
        select! {
            _ = time::sleep(self.timeout) => Err(Error::Handshake(
                *self.addr(),
                format!(
                    "handshake has timed out after {}.{:03} seconds",
                    self.timeout.as_secs(), self.timeout.subsec_millis()
                )
            )),
            result = self.connection.recv() => {
                if let Some(message) = result {
                    match message {
                        PeerResponse::Handshake(handshake) => Ok(handshake),
                        PeerResponse::Error(e) => Err(e),
                        PeerResponse::Closed => Err(Error::Closed),
                        _ => Err(Error::Handshake(*self.addr(), "invalid handshake received".to_string())),
                    }
                } else {
                    Err(Error::Closed)
                }
            },
        }
    }

    async fn validate_handshake(
        &mut self,
        handshake: Handshake,
    ) -> result::Result<(), CloseReason> {
        let info_hash = match self.torrent.info_hash().await {
            Ok(e) => e,
            Err(_) => {
                return Err(CloseReason::TorrentRemoved);
            }
        };
        let mut v2_enabled = false;
        let mut is_valid = false;
        trace!("Peer {} received handshake {:?}", self, handshake);

        // check if v2 support is enabled
        if self
            .protocol_extensions
            .contains(ProtocolExtensionFlags::SupportV2)
            && handshake
                .supported_extensions
                .contains(ProtocolExtensionFlags::SupportV2)
        {
            // use the v2 info hash for validation
            if let Some(v2_hash) = info_hash.v2_as_short() {
                trace!("Peer {} is validating v2 handshake {:?}", self, v2_hash);
                if v2_hash == handshake.info_hash.short_info_hash_bytes() {
                    debug!("Peer {} has successfully upgraded to v2", self);
                    v2_enabled = true;
                    is_valid = true;
                } else {
                    debug!(
                        "Peer {} failed to upgrade to v2, invalid v2 handshake, falling back to v1 handshake validation",
                        self
                    );
                }
            } else {
                warn!(
                    "Peer {} is unable to upgrade to v2, metadata v2 hash is missing",
                    self
                )
            }
        }

        // check if the v2 handshake didn't succeed and we're using v1 handshake validation
        if !is_valid && info_hash != handshake.info_hash {
            self.update_state(PeerState::Error);
            return Err(CloseReason::InvalidInfoHash);
        }

        // store the remote peer information
        trace!(
            "Peer {} is updating remote peer information with {:?}",
            self,
            handshake
        );
        {
            self.remote = Some(RemotePeer {
                peer_id: handshake.peer_id,
                protocol_extensions: handshake.supported_extensions,
                extensions: ExtensionRegistry::default(),
                client_name: None,
                extended_handshake: false,
                is_v2: v2_enabled,
            });
        }

        debug!(
            "Peer {} handshake has been validated, {:?}",
            self, handshake
        );
        Ok(())
    }

    /// Updates the choke state of the client peer.
    pub async fn update_client_choke_state(&mut self, state: ChokeState) {
        // check if we're already in the expected state
        if self.client_choke_state == state {
            return;
        }

        self.client_choke_state = state;
        self.metrics.client_choked.set(state == ChokeState::Choked);

        let send_result: Result<()>;
        if state == ChokeState::Choked {
            send_result = self.send(Message::Choke).await;
            self.reject_remote_pending_requests().await;
        } else {
            send_result = self.send(Message::Unchoke).await;
        }

        if let Err(e) = send_result {
            debug!(
                "Peer {} failed to sent {:?} state update, {}",
                self, state, e
            );
            self.update_state(PeerState::Error);
            return;
        }

        self.callbacks
            .invoke(PeerEvent::ClientChokeStateChanged(self.client_choke_state));
        debug!("Peer {} client entered {} state", self, state);
    }

    /// Updates the choke state of the remote peer.
    async fn update_remote_peer_choke_state(&mut self, state: ChokeState) {
        if self.remote_choke_state == state {
            return;
        }

        // update the choke state of the remote peer
        self.remote_choke_state = state;
        self.metrics.remote_choked.set(state == ChokeState::Choked);

        if state == ChokeState::Choked {
            // if the remote is choked and the fast protocol is disabled,
            // then all pending requests are implicitly rejected
            if !self.is_protocol_enabled(ProtocolExtensionFlags::Fast) {
                self.reject_pending_requests().await;
            }

            // reject the whole downloading queue for this peer
            self.reject_download_queue().await;
        } else {
            self.send_queued_requests().await;
            self.torrent.pick_pieces(&self.client.handle).await;
        }

        self.callbacks
            .invoke(PeerEvent::RemoteChokeStateChanged(self.client_choke_state));
        trace!("Peer {} remote entered {} state", self, state);
    }

    /// Updates the interest state of the client peer.
    /// This will notify the remote peer about the new interest state of our client if it changed.
    pub async fn update_client_interest_state(&mut self, state: InterestState) {
        // check if we're already in the expected state
        if self.client_interest_state == state {
            return;
        }

        self.client_interest_state = state;
        self.metrics
            .client_interested
            .set(state == InterestState::Interested);

        let send_result: Result<()>;
        if state == InterestState::NotInterested {
            send_result = self.send(Message::NotInterested).await;
        } else {
            send_result = self.send(Message::Interested).await;
        }

        if let Err(e) = send_result {
            debug!(
                "Failed to send state {} to remote peer {}, {}",
                state, self, e
            );
        } else {
            debug!("Peer {} client entered {} state", self, state);
        }
    }

    /// Updates the interest state of the remote peer.
    async fn update_remote_peer_interest_state(&mut self, state: InterestState) {
        if self.remote_interest_state == state {
            return;
        }

        self.remote_interest_state = state;
        self.metrics
            .remote_interested
            .set(state == InterestState::Interested);
    }

    /// Updates the state of the peer.
    pub fn update_state(&mut self, new_state: PeerState) {
        if self.state == new_state {
            return;
        }

        self.state = new_state;
        debug!("Peer {} state updated to {:?}", self, new_state);
        self.invoke_event(PeerEvent::StateChanged(new_state));
    }

    /// Set the client peer as having the given pieces.
    /// This updates the peer client bitfield availability and informs the remote peer about the newly available pieces.
    async fn update_client_piece_availability(&mut self, piece: &PieceIndex) {
        // we might not have the bitfield stored if it was unknown when this peer was created
        // if that's the case, copy the whole bitfield from the torrent instead
        if self.client_pieces.len() <= *piece {
            self.client_pieces = self.data_pool.bitfield().await;
        } else {
            self.client_pieces.set(piece.clone(), true);
        }

        match self.send(Message::Have(*piece as u32)).await {
            Ok(_) => trace!(
                "Peer {} notified remote about {} piece availability",
                self,
                piece
            ),
            Err(e) => warn!(
                "Peer {} failed to notify remote peer about {} piece availability, {}",
                self, piece, e
            ),
        }
    }

    /// Update the remote piece availabilities with given piece.
    ///
    /// The range of the piece will be checked against the known pieces of the torrent, if known.
    /// If the piece is out-of-range, the update will be ignored.
    pub(crate) async fn set_remote_has_piece(&mut self, piece: PieceIndex, has_piece: bool) {
        let total_pieces = self.data_pool.num_of_pieces().await;
        let is_metadata_known =
            match timeout(Duration::from_millis(500), self.torrent.is_metadata_known()).await {
                Ok(metadata) => metadata,
                Err(_) => false,
            };

        // ensure the BitVec is large enough to accommodate the piece index
        if piece >= self.remote_pieces.len() {
            let is_piece_bounds_known = is_metadata_known && total_pieces != 0;
            // check if the given piece index is out of bounds
            if is_piece_bounds_known && total_pieces < piece {
                warn!(
                    "Peer {} received remote has piece index {} out of bounds ({})",
                    self,
                    piece,
                    self.remote_pieces.len()
                );
                return;
            }

            // increase the size of the BitVec if metadata is still being retrieved
            self.remote_pieces.resize(piece + 1, false);
        }

        self.remote_pieces.set(piece, has_piece);

        self.metrics
            .available_pieces
            .set(self.remote_pieces.count_ones() as u64);

        if has_piece {
            if !self.is_client_interested() {
                self.determine_client_interest_state().await;
            }
            self.torrent.piece_availabilities(vec![piece], true).await;
            if self.remote_has_all_pieces().await {
                self.invoke_event(PeerEvent::SeedStateChanged(true));
            }
        } else {
            self.torrent.piece_availabilities(vec![piece], false).await;
            self.invoke_event(PeerEvent::SeedStateChanged(false));
        }
    }

    /// Update the remote piece availability based on the supplied [BitVec].
    async fn update_remote_pieces(&mut self, pieces: BitVec) {
        self.remote_pieces = pieces.clone();
        debug!(
            "Peer {} updated {}/{} remote available pieces",
            self,
            pieces.count_ones(),
            pieces.len()
        );

        // notify subscribers about each available piece
        let piece_indexes: Vec<_> = pieces
            .into_iter()
            .enumerate()
            .filter(|(_, v)| *v)
            .map(|(piece, _)| piece as PieceIndex)
            .collect();

        if !piece_indexes.is_empty() {
            self.torrent.piece_availabilities(piece_indexes, true).await;
            if !self.is_client_interested() {
                self.determine_client_interest_state().await;
            }
        }

        if self.remote_has_all_pieces().await {
            self.invoke_event(PeerEvent::SeedStateChanged(true));
        }
    }

    async fn update_remote_fast_have(&mut self, have_all: bool) {
        // if the fast protocol is disabled, we should close the connection
        if !self
            .protocol_extensions
            .contains(ProtocolExtensionFlags::Fast)
        {
            warn!(
                "Fast protocol is disabled, closing connection with peer {}",
                self
            );
            self.close(CloseReason::InvalidAllowFastMessage).await;
            return;
        }

        let bitfield_len = self.data_pool.num_of_pieces().await;
        self.update_remote_pieces(BitVec::repeat(have_all, bitfield_len))
            .await;
        self.metrics.available_pieces.set(bitfield_len as u64);
        self.determine_client_interest_state().await;
    }

    /// Add a pending request which is being requested by the remote peer.
    /// This request can however still be rejected on several conditions.
    async fn add_remote_pending_request(&mut self, request: Request) {
        let mut reject_request = false;
        // check if the request is a duplicate
        if self.remote_pending_requests.contains(&request) {
            warn!("Peer {} requested duplicate request {:?}", self, request);
            if self.is_protocol_enabled(ProtocolExtensionFlags::Fast) {
                self.close(CloseReason::InvalidAllowFastMessage).await;
            }

            return;
        }
        // check if the client peer is choked, if so, reject the request
        if self.client_choke_state == ChokeState::Choked {
            debug!(
                "Peer {} received request for piece {} data while being choked",
                self, request.index
            );
            reject_request = true;
        }
        // check if the request chunk is larger than the allowed chunk size, if so, reject the request
        if request.length > PieceBlock::MAX_LEN {
            debug!(
                "Peer {} requested too large piece {} part, max length {}, requested length {}",
                self,
                request.index,
                PieceBlock::MAX_LEN,
                request.length
            );
            reject_request = true;
        }

        if reject_request {
            self.send_reject_request(request).await;
            return;
        }

        self.on_remote_pending_request(request).await;
    }

    /// Try to cancel a remote pending request.
    /// This will remove the pending request from the queue if found.
    async fn cancel_remote_pending_request(&mut self, request: Request) {
        if let Some(position) = self
            .remote_pending_requests
            .iter()
            .position(|e| e == &request)
        {
            let request = self.remote_pending_requests.remove(position);
            debug!("Cancelled remote pending {:?} for {}", request, self);
        } else {
            debug!(
                "Unable to cancel remote pending {:?} for {}, pending request not found",
                request, self
            );
        }
    }

    /// Reject any remaining pending requests of the remote peer.
    /// This should be called when our client peer enters the [ChokeState::Choked].
    async fn reject_remote_pending_requests(&mut self) {
        if self.is_protocol_enabled(ProtocolExtensionFlags::Fast) {
            // reject any remaining pending requests as specified in BEP6 when entering the choked state
            // this should prevent race conditions in which case we're still sending some piece data while
            // the client is entering the choked state
            for request in std::mem::take(&mut self.remote_pending_requests) {
                if self.connection.is_closed() {
                    break;
                }

                self.send_reject_request(request).await;
            }
        } else {
            // clear any remaining pending requests as specified in BEP3 when entering the choked state
            self.remote_pending_requests.clear();
        }
    }

    /// Add the given piece to be executed as fast request.
    async fn remote_fast_piece(&mut self, piece: PieceIndex) {
        // When the fast extension is disabled, if a peer receives an Allowed Fast message then the peer MUST close the connection.
        if !self.is_protocol_enabled(ProtocolExtensionFlags::Fast) {
            self.close(CloseReason::InvalidAllowFastMessage).await;
            return;
        }

        // resize the remote bitfield, if needed
        if self.remote_pieces.len() < piece {
            self.remote_pieces.resize(piece, false);
        }

        self.remote_fast_pieces.insert(piece);
    }

    /// Process an incoming piece suggestion from the remote peer.
    /// This will request the given piece if the fast protocol is enabled, downloading is allowed and the piece is wanted by the torrent.
    async fn on_piece_suggested(&mut self, piece: PieceIndex) {
        // When the fast extension is disabled, if a peer receives a Suggest Piece message, the peer MUST close the connection.
        if !self.is_protocol_enabled(ProtocolExtensionFlags::Fast) {
            self.close(CloseReason::InvalidAllowFastMessage).await;
            return;
        }

        self.remote_suggested_pieces.insert(piece);
    }

    async fn update_extended_handshake(&mut self, handshake: ExtendedHandshake) {
        if let Some(remote) = self.remote.as_mut() {
            remote.extensions = handshake.m;
            remote.client_name = handshake.client;
            remote.extended_handshake = true;
            let remote_info = format!("{:?}", remote);

            debug!(
                "Peer {} updated extended handshake information, {}",
                self, remote_info
            );
        } else {
            warn!(
                "Peer {} received extended handshake before the initial handshake was completed",
                self
            );
            self.close(CloseReason::None).await;
        }
    }

    /// Process piece block requests, which need to be queued for downloading from the remote peer.
    async fn on_request(&mut self, blocks: Vec<PieceBlock>) -> Result<()> {
        // early exit if the peer is being closed
        if self.cancellation_token.is_cancelled() {
            return Err(Error::Closed);
        }

        let mut requests_added = 0usize;
        for block in blocks {
            // filter out any duplicate piece blocks
            if self.download_queue.contains(&block) {
                trace!("Peer {} has already queued piece block {:?}", self, block);
                continue;
            }
            if self.pending_requests.iter().any(|pending| {
                pending.request.index == block.piece && pending.request.begin == block.begin
            }) {
                trace!(
                    "Peer {} is already downloading piece block {:?}",
                    self,
                    block
                );
                continue;
            }

            self.download_queue.push_back(block);
            requests_added += 1;
        }

        trace!(
            "Peer {} is adding {} request(s) to the download queue",
            self,
            requests_added
        );
        Ok(())
    }

    fn target_request_queue_len(&self) -> usize {
        self.target_queue_len
            .saturating_sub(self.download_queue.len())
            .saturating_sub(self.pending_requests.len())
    }

    fn update_target_request_queue_len(&mut self) {
        let download_rate = self.metrics.bytes_in_useful.rate();
        let target_queue_len = max(
            (REQUEST_QUEUE_TIME.as_secs() * download_rate as u64) as usize / PieceBlock::MAX_LEN,
            MIN_TARGET_QUEUE_LEN,
        );

        self.target_queue_len = target_queue_len.min(MAX_TARGET_QUEUE_LEN);
    }

    /// Reject the current in-flight block requests to the remote peer.
    async fn reject_pending_requests(&mut self) {
        for pending in std::mem::take(&mut self.pending_requests) {
            self.torrent
                .piece_block_rejected(&self.client.handle, &pending.block)
                .await
        }
    }

    /// Reject the queued piece blocks within the download queue.
    async fn reject_download_queue(&mut self) {
        for block in std::mem::take(&mut self.download_queue) {
            self.torrent
                .piece_block_rejected(&self.client.handle, &block)
                .await
        }
    }

    /// Try to sent one or more queued requests to the remote peer.
    async fn send_queued_requests(&mut self) {
        while !self.download_queue.is_empty() && self.pending_requests.len() < self.target_queue_len
        {
            let block = match self.download_queue.pop_front() {
                None => return,
                Some(request) => request,
            };
            // if the remote peer is choked, but the fast protocol allows this piece
            // we continue sending the request as it's still valid
            let is_remote_choked = self.remote_choke_state == ChokeState::Choked;
            let is_fast_allowed = self.is_fast_allowed(&block.piece);
            if is_remote_choked && !is_fast_allowed {
                continue;
            }

            // if the block is already in the pending requests,
            // drop it and continue with the next one
            if self.pending_requests.iter().any(|pending| {
                pending.request.index == block.piece && pending.request.begin == block.begin
            }) {
                trace!(
                    "Peer {} is already downloading piece block {:?}",
                    self,
                    block
                );
                continue;
            }

            if let Err(e) = self.send_pending_request(block.clone().into()).await {
                trace!(
                    "Peer {} failed to sent request ({}:{}), {}",
                    self,
                    block.piece,
                    block.begin,
                    e
                );
                self.torrent
                    .piece_block_rejected(&self.client.handle, &block)
                    .await;
                break;
            }
        }
    }

    /// Try to send the handshake information of our client peer to the remote peer.
    async fn send_handshake(&mut self) -> Result<()> {
        self.update_state(PeerState::Handshake);
        let info_hash = match self.torrent.info_hash().await {
            Ok(e) => e,
            Err(_) => {
                return Err(Error::Closed);
            }
        };

        let handshake = Handshake::new(info_hash, self.client.id, self.protocol_extensions);
        debug!("Peer {} is sending handshake {:?}", self, handshake);
        match self
            .send_raw_bytes(TryInto::<Vec<u8>>::try_into(handshake)?)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                debug!("Peer {} failed to send handshake, {}", self, e);
                self.update_state(PeerState::Error);
                Err(e)
            }
        }
    }

    async fn send_extended_handshake(&self) -> Result<()> {
        let extension_registry = self.extension_registry.clone();
        let is_partial_seed = self.torrent.is_partial_seed().await;
        let peer_port = self.torrent.peer_port().await;
        let config = match self.torrent.config().await {
            Ok(e) => e,
            Err(_) => return Err(Error::Closed),
        };
        let message = Message::ExtendedHandshake(ExtendedHandshake {
            m: extension_registry,
            upload_only: is_partial_seed,
            client: Some(config.client_name())
                .filter(|e| !e.is_empty())
                .map(|e| e.to_string()),
            regg: None,
            encryption: false,
            metadata_size: None,
            port: peer_port.map(|e| e as u32),
            your_ip: Some(CompactIp::from(&self.client.addr)),
            ipv4: None,
            ipv6: None,
        });

        debug!("Peer {} is sending extended handshake {:?}", self, message);
        self.send(message).await
    }

    async fn send_initial_messages(&mut self) -> Result<()> {
        let mut is_fast_have_sent = false;

        // the extended handshake should be sent immediately after the standard bittorrent handshake to any peer that supports the extension protocol
        if self.is_protocol_enabled(ProtocolExtensionFlags::LTEP) {
            trace!("Peer {} exchanging extended handshake", self);
            self.update_state(PeerState::Handshake);
            if let Err(e) = self.send_extended_handshake().await {
                warn!("Peer {} failed to send extended handshake, {}", self, e);
                // remove the LTEP extension flag from the remote peer
                // as the extended handshake has failed to complete
                if let Some(remote) = self.remote.as_mut() {
                    remote.protocol_extensions &= !ProtocolExtensionFlags::LTEP;
                }
            }
        }

        // check if the fast protocol is enabled
        // if so, we send the initial fast messages to the remote peer
        let bitfield = self.torrent_bitfield().await;
        let is_bitfield_known = bitfield.len() > 0;
        let is_fast_enabled = self.is_protocol_enabled(ProtocolExtensionFlags::Fast);
        if is_fast_enabled && is_bitfield_known {
            let mut message: Option<Message> = None;
            let is_metadata_known = self.metadata().await?.info.is_some();

            if is_metadata_known && bitfield.all() {
                message = Some(Message::HaveAll);
            } else if !is_metadata_known || bitfield.not_any() {
                message = Some(Message::HaveNone);
            }

            if let Some(message) = message {
                let message_type = format!("{:?}", message);
                match self.send(message).await {
                    Ok(_) => {
                        is_fast_have_sent = true;
                        debug!("Peer {} sent message {}", self, message_type);
                    }
                    Err(e) => {
                        warn!(
                            "Peer {} failed to send message {}, {}",
                            self, message_type, e
                        );
                        self.update_state(PeerState::Error);
                    }
                }
            }
        }

        // we try to send the bitfield with completed pieces if none of the initial fast messages have been sent
        // this is only done if at least one piece is completed
        if !is_fast_have_sent && is_bitfield_known && bitfield.any() {
            let message = Message::Bitfield(bitfield.clone());
            let message_type = format!("{:?}", message);
            match self.send(message).await {
                Ok(_) => debug!("Peer {} sent message {}", self, message_type),
                Err(e) => {
                    warn!("Peer {} failed to send bitfield message, {}", self, e);
                    self.update_state(PeerState::Error);
                }
            }
        }

        // store the bitfield of the torrent as initial state
        self.client_pieces = bitfield;

        // request missing hashes if needed
        if self.should_request_hashes().await {
            self.request_missing_hashes().await;
        }

        self.update_state(PeerState::Idle);
        Ok(())
    }

    /// Try to send a HolePunch request to the remote peer.
    /// This is done through the HolePunch extension, if available.
    async fn send_holepunch(
        &self,
        target: SocketAddr,
        response: Reply<extension::Result<SocketAddr>>,
        extensions: &mut [PeerExtension],
    ) {
        let extension = match extensions.iter_mut().find_map(|e| match e {
            PeerExtension::Holepunch(e) => Some(e),
            _ => None,
        }) {
            None => {
                response.send(Err(extension::Error::Unsupported));
                return;
            }
            Some(e) => e,
        };

        extension.send_rendezvous(target, response, self).await;
    }

    /// Send the reject request to the remote peer.
    /// This is only executed if the fast protocol is enabled.
    async fn send_reject_request(&mut self, request: Request) {
        self.metrics.rejects.inc();

        // if the fast protocol is disabled, then we don't send a reject
        if !self.is_protocol_enabled(ProtocolExtensionFlags::Fast) {
            return;
        }

        let piece = request.index;
        match self.send(Message::RejectRequest(request)).await {
            Ok(_) => trace!("Peer {} rejected remote request {}", self, piece),
            Err(e) => {
                warn!(
                    "Peer {} failed to reject remote request {}, {}",
                    self, piece, e
                );
                if let Error::Io(e) = e {
                    if matches!(
                        e.kind(),
                        io::ErrorKind::Interrupted
                            | io::ErrorKind::ConnectionReset
                            | io::ErrorKind::ConnectionAborted
                            | io::ErrorKind::ConnectionRefused
                    ) {
                        self.close(CloseReason::TimedOutRequest).await
                    }
                }
            }
        }
    }

    /// Try to send the given message to the remote peer.
    pub async fn send(&self, message: Message) -> Result<()> {
        trace!("Peer {} trying to send message {:?}", self, message);
        let message_bytes = TryInto::<Vec<u8>>::try_into(message)?;
        self.send_bytes(message_bytes).await
    }

    /// Send the given message to the remote peer.
    /// This method will prefix the message bytes with the BigEndian length bytes of the given message.
    pub async fn send_bytes<T: AsRef<[u8]>>(&self, message: T) -> Result<()> {
        let msg_length = message.as_ref().len();
        let mut buffer = vec![0u8; 4];

        // write the length of the given message as BigEndian in the first 4 bytes
        BigEndian::write_u32(&mut buffer[..4], msg_length as u32);
        // append the given message bytes to the buffer
        buffer.extend_from_slice(message.as_ref());

        self.send_raw_bytes(buffer).await
    }

    /// Send the given message bytes AS-IS to the remote peer.
    /// The given bytes should be a valid BitTorrent protocol message.
    async fn send_raw_bytes<T: AsRef<[u8]>>(&self, bytes: T) -> Result<()> {
        let msg_length = bytes.as_ref().len();

        timeout(self.timeout, self.connection.write(bytes.as_ref())).await??;

        self.metrics.bytes_out.inc_by(msg_length as u64);
        Ok(())
    }

    /// Request piece data which is available from the remote peer.
    async fn send_pending_request(&mut self, block: PieceBlock) -> Result<()> {
        if self.state != PeerState::Downloading {
            self.update_state(PeerState::Downloading);
        }

        self.send(Message::Request(block.clone().into())).await?;

        // add the request to the pending requests queue
        self.pending_requests.push(PendingRequest {
            request: block.clone().into(),
            block,
            sent: Instant::now(),
        });
        Ok(())
    }

    /// Send the keep alive message to the remote peer.
    pub async fn send_keep_alive(&self) {
        let message = Message::KeepAlive;

        match TryInto::<Vec<u8>>::try_into(message) {
            Ok(bytes) => {
                if let Err(e) = self.send_bytes(bytes).await {
                    warn!("Failed to send keep alive to peer {}, {}", self, e);
                }
            }
            Err(e) => warn!("Failed to parse keep alive message, {}", e),
        }
    }

    /// Try to remove the given request from the pending requests.
    /// This function should be called when piece data has been received or rejected for the given request.
    async fn remove_client_pending_request(&mut self, request: &Request) -> Option<Request> {
        self.pending_requests
            .iter()
            .position(|e| &e.request == request)
            .map(|pos| self.pending_requests.remove(pos).request)
    }

    /// Verify that the given piece request is valid to be processed.
    /// This will check that the requested range is within the piece range and that the piece is completed.
    async fn validate_piece_request(&self, request: &Request) -> bool {
        let is_piece_completed = self.data_pool.is_piece_completed(&request.index).await;

        if is_piece_completed {
            if let Some(piece) = self.data_pool.piece(&request.index).await {
                let piece_len = piece.length;
                let request_end = request.begin + request.length;

                return request.begin < piece_len && request_end <= piece_len;
            } else {
                warn!(
                    "Peer {} failed to retrieve piece data of {}",
                    self, request.index
                );
            }
        } else {
            debug!(
                "Peer {} received piece request for incomplete piece {}",
                self, request.index
            );
        }

        false
    }

    /// Find the supported extension from our own client extensions through the extensions number.
    /// This should be used when we've received an extended message from the remote peer.
    pub async fn find_extension_by_number<'a>(
        &self,
        extension_number: ExtensionNumber,
        extensions: &'a mut [PeerExtension],
    ) -> Option<&'a mut PeerExtension> {
        // search for the given extension, by extensions number, in our own supported extensions
        let extension_registry = self.client_extension_registry();
        if let Some(extension_name) = extension_registry
            .iter()
            .find(|(_, number)| extension_number == **number)
            .map(|(name, _)| name.clone())
        {
            return extensions.iter_mut().find(|e| e.name() == extension_name);
        } else {
            let extensions = self.remote_extension_registry();
            debug!(
                "Extension number {} is not support by {}, supported remote {:?}",
                extension_number, self, extensions
            )
        }

        None
    }

    /// Returns the extension number of the remote peer for the given extension name.
    pub fn find_remote_extension_number(&self, extension_name: &str) -> Option<ExtensionNumber> {
        self.remote
            .as_ref()
            .and_then(|remote| remote.extensions.get(extension_name).copied())
    }

    /// Invoke the extension tick function for all enabled extensions.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn on_extensions_tick(&mut self, extensions: &mut [PeerExtension], interval: Duration) {
        for extension in extensions {
            let start = Instant::now();
            extension.tick(self).await;
            let elapsed = start.elapsed();
            if elapsed > interval {
                debug!(
                    "Peer {} detected long extension {} tick, tick took {:.3}ms",
                    self,
                    extension.name(),
                    elapsed.as_secs_f64() * 1000.0
                );
            }
        }
    }

    /// Update the statistics of the peer.
    fn on_stats_update(&self, interval: Duration) {
        let event_stats = self.metrics.snapshot();
        self.metrics.tick(interval);
        self.callbacks.invoke(PeerEvent::Stats(event_stats));
    }

    /// Invoke an event on the peer instance.
    /// This will trigger the event for all enabled extensions.
    pub fn invoke_event(&self, event: PeerEvent) {
        self.callbacks.invoke(event);
    }

    /// Close the connection of the peer.
    /// This cancels the main loop of the peer and notifies the parent torrent of the closure.
    pub(crate) async fn close(&mut self, reason: CloseReason) {
        debug!("Peer {} is closing, {:?}", self, reason);
        // cancel the main loop of the peer to stop any ongoing operation
        self.cancellation_token.cancel();
        // reject any pending requests and queued block downloads
        self.reject_pending_requests().await;
        self.reject_download_queue().await;
        // close underlying connection
        let _ = self.connection.close().await;
        // notify any subscribers
        self.update_state(PeerState::Closed);

        // notify the torrent that this peer is being closed
        if self.torrent.is_valid() {
            self.torrent.peer_closed(self.client.addr, reason).await;
        }

        self.invoke_event(PeerEvent::Closed(reason));
    }

    /// Return the extension registry for the given extensions.
    fn create_extension_registry(extensions: &[PeerExtension]) -> ExtensionRegistry {
        let mut extension_index = 0u8;

        extensions
            .iter()
            .map(|e| {
                extension_index += 1;
                (e.name().to_string(), extension_index)
            })
            .collect()
    }
}

impl Callback<PeerEvent> for PeerContext {
    fn subscribe(&self) -> Subscription<PeerEvent> {
        self.callbacks.subscribe()
    }
}

impl Drop for PeerContext {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
        trace!("Peer {} is being dropped", self)
    }
}

#[derive(Debug)]
struct PendingRequest {
    /// The piece block belonging to this request.
    block: PieceBlock,
    /// The outgoing request that is pending a response.
    request: Request,
    /// The time the request was sent to the remote peer.
    sent: Instant,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::{CreatePiecesAndFilesOperation, FileValidationOperation};
    use crate::peer::protocol::UtpPacketCapture;
    use crate::peer::tests::create_utp_peer_pair;
    use crate::storage::{DiskStorage, MemoryStorage};
    use crate::tests::copy_test_file;
    use crate::tests::helpers::wait_for_torrent_pieces;
    use crate::DEFAULT_TORRENT_PROTOCOL_EXTENSIONS;
    use std::cmp::Ordering;
    use tempfile::tempdir;

    mod new {
        use super::*;

        #[tokio::test]
        async fn test_new_tcp() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let torrent = torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![],
                vec![]
            );
            let (outgoing, incoming) = tcp_peer_pair!(&torrent, vec![]);

            let result = incoming.state().await;
            assert_ne!(PeerState::Error, result);
            assert_ne!(PeerState::Closed, result);

            incoming.close().await;
            let result = incoming.state().await;
            assert_eq!(PeerState::Closed, result);
            assert_timeout!(
                Duration::from_secs(1),
                PeerState::Closed == outgoing.state().await,
                "expected the outgoing connection to be closed"
            );
        }

        #[tokio::test]
        async fn test_new_utp() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let torrent = torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![],
                vec![]
            );
            let incoming_capture = UtpPacketCapture::new();
            let outgoing_capture = UtpPacketCapture::new();
            let (incoming_socket, outgoing_socket) = create_utp_socket_pair!(
                vec![incoming_capture.clone().into()],
                vec![outgoing_capture.clone().into()]
            );
            let (outgoing, incoming) = create_utp_peer_pair(
                &incoming_socket,
                &outgoing_socket,
                &torrent,
                &torrent,
                DEFAULT_TORRENT_PROTOCOL_EXTENSIONS(),
            )
            .await;

            let result = incoming.state().await;
            assert_ne!(PeerState::Error, result);
            assert_ne!(PeerState::Closed, result);

            // close the incoming peer connection
            incoming.close().await;
            let result = incoming.state().await;
            assert_eq!(
                PeerState::Closed,
                result,
                "expected the incoming connection to be closed"
            );

            // wait for the outgoing peer connection to reach the closed state
            assert_timeout!(
                Duration::from_secs(1),
                PeerState::Closed == outgoing.state().await,
                "expected the outgoing connection to be closed"
            );
        }
    }

    mod handshake {
        use super::*;
        use std::net::Ipv4Addr;
        use tokio::net::TcpListener;
        use tokio::sync::oneshot;

        #[tokio::test]
        async fn test_handshake() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let (tx, rx) = oneshot::channel();
            let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
                .await
                .unwrap();
            let listener_addr = (Ipv4Addr::LOCALHOST, listener.local_addr().unwrap().port()).into();
            let torrent = torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![],
                vec![]
            );

            // create a new outbound peer
            // this peer is responsible for starting the handshake
            let inner = torrent.inner.clone();
            let data_pool = torrent.inner.data_pool().await.unwrap();

            // subscribe to the peer and listen for the handshake completed event
            tokio::spawn(async move {
                // the peer is only returned after the handshake has been completed
                let peer = BitTorrentPeer::new_outbound(
                    PeerId::new(),
                    listener_addr,
                    PeerStream::Tcp(TcpStream::connect(listener_addr).await.unwrap()),
                    inner,
                    data_pool,
                    ProtocolExtensionFlags::none(),
                    vec![],
                    Duration::from_secs(5),
                )
                .await
                .expect("expected a new outbound peer");

                let _ = tx.send(peer);
            });

            // try to accept the incoming peer connection
            let (stream, addr) = timeout!(Duration::from_millis(250), listener.accept())
                .expect("expected to receive an incoming connection");
            let peer_id = PeerId::new();
            let connection = PeerConnection::new_tcp(peer_id, addr, stream, Metrics::new());

            // the outgoing connection should always send a handshake as first message
            // so wait for the incoming handshake from the peer
            let info_hash = torrent.info_hash().await.unwrap();
            let message = timeout!(
                Duration::from_millis(250),
                connection.recv(),
                "expected an incoming handshake"
            )
            .unwrap();
            let handshake = match message {
                PeerResponse::Handshake(handshake) => handshake,
                _ => {
                    assert!(false, "expected a handshake message");
                    return;
                }
            };
            assert_eq!(
                ProtocolExtensionFlags::none(),
                handshake.supported_extensions,
                "expected the handshake to have no extensions"
            );
            assert_eq!(
                info_hash, handshake.info_hash,
                "expected the handshake to have the correct info hash"
            );

            // send a response to the given handshake
            let reply_handshake = TryInto::<Vec<u8>>::try_into(Handshake {
                supported_extensions: ProtocolExtensionFlags::none(),
                info_hash,
                peer_id,
            })
            .unwrap();
            let result = connection.write(reply_handshake.as_slice()).await;
            assert!(
                result.is_ok(),
                "expected the handshake to be sent successfully, but got {:?}",
                result
            );

            // wait for the remote peer to receive and validate the handshake
            let peer = timeout!(
                Duration::from_millis(250),
                rx,
                "expected the handshake to be validated"
            )
            .unwrap();

            // verify that the peer is no longer in the handshake state
            let state = peer.state().await;
            assert_ne!(
                PeerState::Handshake,
                state,
                "expected the peer handshake to have been completed"
            );
        }
    }

    #[tokio::test]
    async fn test_peer_has_wanted_pieces() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        copy_test_file(
            &temp_path,
            "piece-1_30.iso",
            Some("debian-12.4.0-amd64-DVD-1.iso"),
        );
        let source = torrent!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![
                CreatePiecesAndFilesOperation::new().into(),
                FileValidationOperation::new().into(),
            ],
            vec![],
            |params| DiskStorage::new(params.info_hash, params.path, params.data_pool,).into(),
            None
        );
        let target = torrent!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![CreatePiecesAndFilesOperation::new().into()],
            vec![],
            |_| MemoryStorage::new().into(),
            None
        );

        // create the pieces for the torrent
        wait_for_torrent_pieces(&source).await;
        wait_for_torrent_pieces(&target).await;

        // wait for the source torrent to validate the existing pieces
        // this should automatically inform the target peer that it has wanted pieces
        assert_timeout!(
            Duration::from_secs(1),
            source.has_piece(&29).await,
            "expected the pieces to have been validated"
        );

        // pause the source to prevent the target from starting to download the available pieces
        source.pause().await;

        // create the peer pair
        let (_source_peer, target_peer) = tcp_peer_pair!(
            &source,
            &target,
            vec![],
            vec![],
            ProtocolExtensionFlags::LTEP
        );

        // check if the target peer has wanted pieces from the source
        // as the bitfield is sent after the handshake, it might not have been received yet
        assert_timeout!(
            Duration::from_millis(500),
            target_peer.has_wanted_pieces().await == true,
            "expected the remote to have wanted pieces"
        );
    }

    #[tokio::test]
    async fn test_peer_torrent_pieces_changed() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let torrent = torrent!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![CreatePiecesAndFilesOperation::new().into()],
            vec![]
        );
        let (outgoing, _incoming) = tcp_peer_pair!(&torrent, vec![]);

        // create the pieces for the torrent
        wait_for_torrent_pieces(&torrent).await;

        // check if both the client & remote piece bitfield have been updated
        let torrent_bitfield = torrent.inner.data_pool().await.unwrap().bitfield().await;
        assert_timeout!(
            Duration::from_secs(1),
            torrent_bitfield == outgoing.client_piece_bitfield().await,
            "expected the peer client bitfield to match the torrent bitfield"
        );
        assert_timeout!(
            Duration::from_millis(500),
            outgoing.remote_piece_bitfield().await.len() == torrent_bitfield.len(),
            "expected the remote bitfield to match the torrent bitfield length"
        );
    }

    #[test]
    fn test_interest_state_ordering() {
        let result = InterestState::NotInterested.cmp(&InterestState::NotInterested);
        assert_eq!(Ordering::Equal, result);

        let result = InterestState::Interested.cmp(&InterestState::NotInterested);
        assert_eq!(Ordering::Greater, result);

        let result = InterestState::NotInterested.cmp(&InterestState::Interested);
        assert_eq!(Ordering::Less, result);

        let result = InterestState::Interested.cmp(&InterestState::Interested);
        assert_eq!(Ordering::Equal, result);
    }

    mod target_request_queue_len {
        use super::*;

        #[tokio::test]
        async fn test_download_rate() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let torrent = torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![CreatePiecesAndFilesOperation::new().into()],
                vec![],
                |_| MemoryStorage::new().into(),
                None
            );
            let (mut peer, _target) = peer_context_pair!(&torrent.inner, &[]);

            // update the target queue len when the download rate is 0
            peer.update_target_request_queue_len();
            assert_eq!(
                MIN_TARGET_QUEUE_LEN, peer.target_queue_len,
                "expected the target_queue_len to be MIN_TARGET_QUEUE_LEN"
            );

            peer.metrics.bytes_in_useful.inc_by(640_000);
            peer.metrics.tick(Duration::from_secs(1));
            peer.metrics.bytes_in_useful.inc_by(1_280_000);
            peer.metrics.tick(Duration::from_secs(1));

            // update the target queue len when a download rate is known
            peer.update_target_request_queue_len();
            assert_eq!(
                65, peer.target_queue_len,
                "expected the target_queue_len to be calculated from the download rate"
            );
        }

        #[tokio::test]
        async fn test_max_target_queue_len() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let torrent = torrent!(
                "debian-udp.torrent",
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![CreatePiecesAndFilesOperation::new().into()],
                vec![],
                |_| MemoryStorage::new().into(),
                None
            );
            let (mut peer, _target) = peer_context_pair!(&torrent.inner, &[]);

            // set the download rate to a ridiculously high value
            peer.metrics.bytes_in_useful.inc_by(100_000_000);
            peer.metrics.tick(Duration::from_secs(1));

            peer.update_target_request_queue_len();
            assert_eq!(
                MAX_TARGET_QUEUE_LEN, peer.target_queue_len,
                "expected the target_queue_len to be MAX_TARGET_QUEUE_LEN"
            );
        }
    }

    mod peer_client_info {
        use super::*;

        #[test]
        fn test_peer_priority() {
            let peer1 = create_info_from_addr(([230, 12, 123, 1], 1234).into());
            let peer2 = create_info_from_addr(([230, 12, 123, 3], 300).into());

            assert_eq!(Some(2579844473), peer1.peer_priority(&peer2));
        }

        fn create_info_from_addr(addr: SocketAddr) -> PeerClientInfo {
            PeerClientInfo {
                handle: Default::default(),
                id: PeerId::new(),
                addr,
                connection_type: ConnectionDirection::Inbound,
                connection_protocol: ConnectionProtocol::Tcp,
            }
        }
    }
}
