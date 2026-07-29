use std::sync::Once;

/// Create a [TorrentContext] instance for the given uri and options.
macro_rules! torrent_context {
    ($uri:expr, $temp_dir:expr, $options:expr) => {{
        torrent_context!(
            $uri,
            $temp_dir,
            $options,
            crate::TorrentConfig::builder().path($temp_dir).build()
        )
    }};
    ($uri:expr, $temp_dir:expr, $options:expr, $config:expr) => {{
        use crate::peer::{TcpPeerDiscovery, UtpPeerDiscovery};

        let tcp_discovery = TcpPeerDiscovery::new()
            .await
            .expect("expected a new tcp peer discovery");
        let utp_discovery = UtpPeerDiscovery::new()
            .await
            .expect("expected a new utp peer discovery");

        torrent_context!(
            $uri,
            $temp_dir,
            $options,
            $config,
            vec![tcp_discovery.into(), utp_discovery.into(),]
        )
    }};
    ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $discoveries:expr) => {{
        torrent_context!(
            $uri,
            $temp_dir,
            $options,
            $config,
            $discoveries,
            Some(crate::dht::DhtTracker::builder().build().await.unwrap())
        )
    }};
    ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $discoveries:expr, $extensions:expr) => {{
        use crate::peer::extension::DontHaveExtension;
        use crate::peer::extension::HolepunchExtension;
        use crate::peer::extension::MetadataExtension;
        use crate::peer::extension::PexExtension;

        torrent_context!(
            $uri,
            $temp_dir,
            $options,
            $config,
            $discoveries,
            vec![
                || MetadataExtension::new().into(),
                || PexExtension::new(std::time::Duration::from_secs(90)).into(),
                || DontHaveExtension::new().into(),
                || HolepunchExtension::new().into(),
            ],
            Some(crate::dht::DhtTracker::builder().build().await.unwrap())
        )
    }};
    ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $discoveries:expr, $extensions:expr, $dht:expr) => {{
        torrent_context!(
            $uri,
            $temp_dir,
            $options,
            $config,
            $discoveries,
            $extensions,
            $dht,
            None,
            |_, _| crate::storage::MemoryStorage::new().into()
        )
    }};
    ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $discoveries:expr, $extensions:expr, $dht:expr, $lsd:expr) => {{
        torrent_context!(
            $uri,
            $temp_dir,
            $options,
            $config,
            $discoveries,
            $extensions,
            $dht,
            $lsd,
            |_, _| crate::storage::MemoryStorage::new().into()
        )
    }};
    ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $discoveries:expr, $extensions:expr, $dht:expr, $lsd:expr, $storage:expr) => {{
        use crate::dht::DhtTracker;
        use crate::peer::PeerDiscovery;
        use crate::piece_picker::FxPiecePicker;
        use crate::piece_picker::PickerOptions;
        use crate::storage::Storage;
        use crate::torrent_data::DataPool;
        use crate::tracker::TrackerClient;
        use crate::ExtensionFactory;
        use crate::LocalServiceDiscovery;
        use crate::TorrentHandle;
        use crate::{
            TorrentConfig, TorrentContext, TorrentFlags, TorrentMetadata,
            DEFAULT_TORRENT_PROTOCOL_EXTENSIONS,
        };
        use fx_callback::MultiThreadedCallback;
        use std::time::Duration;

        let uri: &str = $uri;
        let options: TorrentFlags = $options;
        let config: TorrentConfig = $config;
        let discoveries: Vec<PeerDiscovery> = $discoveries;
        let extensions: Vec<ExtensionFactory> = $extensions;
        let dht: Option<DhtTracker> = $dht;
        let lsd: Option<LocalServiceDiscovery> = $lsd;
        let metadata: TorrentMetadata = metadata!(uri);
        let info_hash = metadata.info_hash.clone();
        let config = TorrentConfig::builder()
            .path($temp_dir)
            .peer_connection_timeout(config.peer_connection_timeout)
            .max_in_flight_pieces(config.max_in_flight_pieces)
            .peers_upper_limit(config.peers_upper_limit)
            .peers_lower_limit(config.peers_lower_limit)
            .peers_in_flight(config.peers_in_flight)
            .build();
        let data_pool = DataPool::new();
        let (command_sender, receiver) = channel!(512);
        let mut trackers = vec![TrackerClient::new(Duration::from_secs(2)).into()];

        if let Some(dht) = dht {
            trackers.push(dht.into());
        }
        if let Some(lsd) = lsd {
            trackers.push(lsd.into());
        }

        let handle = TorrentHandle::new();
        let max_outstanding_pieces = config.max_in_flight_pieces;
        let peer_port = discoveries.first().map(|e| e.addr().port());
        let callbacks = MultiThreadedCallback::new();
        let storage: Storage = ($storage)(info_hash, data_pool.clone());
        (
            TorrentContext::new(
                handle,
                metadata,
                config,
                peer_port,
                DEFAULT_TORRENT_PROTOCOL_EXTENSIONS(),
                extensions,
                options,
                data_pool.clone(),
                trackers,
                storage.clone(),
                FxPiecePicker::new(
                    handle,
                    data_pool,
                    storage,
                    vec![],
                    16 * 1024 * 1024,
                    max_outstanding_pieces,
                    PickerOptions::Priority,
                )
                .into(),
                command_sender,
                callbacks,
            ),
            receiver,
        )
    }};
}

/// Create a new [Torrent] instance.
macro_rules! torrent {
    ($uri:expr, $temp_dir:expr, $options:expr) => {{
        torrent!(
            $uri,
            $temp_dir,
            $options,
            crate::TorrentConfig::builder().path($temp_dir).build()
        )
    }};
    ($uri:expr, $temp_dir:expr, $options:expr, $config:expr) => {{
        torrent!(
            $uri,
            $temp_dir,
            $options,
            $config,
            crate::operation::Operation::default_operations()
        )
    }};
    ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $operations:expr) => {{
        use crate::peer::{TcpPeerDiscovery, UtpPeerDiscovery};

        let tcp_discovery = TcpPeerDiscovery::new()
            .await
            .expect("expected a new tcp peer discovery");
        let peer_port = tcp_discovery.addr().port();
        let utp_discovery = UtpPeerDiscovery::with_port(peer_port)
            .await
            .expect("expected a new utp peer discovery");

        torrent!(
            $uri,
            $temp_dir,
            $options,
            $config,
            $operations,
            vec![tcp_discovery.into(), utp_discovery.into(),]
        )
    }};
    ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $operations:expr, $discoveries:expr) => {{
        torrent!(
            $uri,
            $temp_dir,
            $options,
            $config,
            $operations,
            $discoveries,
            |params| {
                crate::storage::DiskStorage::new(params.info_hash, params.path, params.data_pool)
                    .into()
            }
        )
    }};
    ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $operations:expr, $discoveries:expr, $storage:expr) => {{
        torrent!(
            $uri,
            $temp_dir,
            $options,
            $config,
            $operations,
            $discoveries,
            $storage,
            Some(crate::dht::DhtTracker::builder().build().await.unwrap())
        )
    }};
    ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $operations:expr, $discoveries:expr, $storage:expr, $dht:expr) => {{
        torrent!(
            $uri,
            $temp_dir,
            $options,
            $config,
            $operations,
            $discoveries,
            $storage,
            $dht,
            crate::tracker::TrackerClient::new(std::time::Duration::from_secs(2))
        )
    }};
    ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $operations:expr, $discoveries:expr, $storage:expr, $dht:expr, $tracker_manager:expr) => {{
        use crate::peer::extension::DontHaveExtension;
        use crate::peer::extension::HolepunchExtension;
        use crate::peer::extension::MetadataExtension;
        use crate::peer::extension::PexExtension;

        torrent!(
            $uri,
            $temp_dir,
            $options,
            $config,
            $operations,
            $discoveries,
            $storage,
            $dht,
            $tracker_manager,
            vec![
                || DontHaveExtension::new().into(),
                || HolepunchExtension::new().into(),
                || MetadataExtension::new().into(),
                || PexExtension::new(std::time::Duration::from_secs(5)).into(),
            ]
        )
    }};
    ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $operations:expr, $discoveries:expr, $storage:expr, $dht:expr, $tracker_manager:expr, $extensions:expr) => {{
        use crate::dht::DhtTracker;
        use crate::operation::Operation;
        use crate::peer::PeerDiscovery;
        use crate::ExtensionFactory;
        use crate::{Torrent, TorrentConfig, TorrentFlags};

        let uri: &str = $uri;
        let options: TorrentFlags = $options;
        let config: TorrentConfig = $config;
        let operations: Vec<Operation> = $operations;
        let discoveries: Vec<PeerDiscovery> = $discoveries;
        let extensions: Vec<ExtensionFactory> = $extensions;
        let dht: Option<DhtTracker> = $dht;
        let torrent_info = metadata!(uri);
        let tracker_manager = $tracker_manager;
        let config = TorrentConfig::builder()
            .path($temp_dir)
            .peer_connection_timeout(config.peer_connection_timeout)
            .max_in_flight_pieces(config.max_in_flight_pieces)
            .peers_upper_limit(config.peers_upper_limit)
            .peers_lower_limit(config.peers_lower_limit)
            .build();
        let mut trackers = vec![tracker_manager.into()];

        if let Some(dht) = dht {
            trackers.push(dht.into());
        }

        Torrent::request()
            .metadata(torrent_info)
            .peer_discoveries(discoveries)
            .options(options)
            .config(config)
            .operations(operations)
            .storage($storage)
            .trackers(trackers)
            .extensions(extensions)
            .build()
            .unwrap()
    }};
}

/// Create a new pair of TCP peers.
macro_rules! tcp_peer_pair {
    ($torrent:expr, $extensions:expr) => {{
        tcp_peer_pair!($torrent, $extensions, crate::peer::ProtocolExtensionFlags::none())
    }};
    ($torrent:expr, $extensions:expr, $protocol_extensions:expr) => {{
        tcp_peer_pair!($torrent, $torrent, $extensions, $extensions, $protocol_extensions)
    }};
    ($incoming_torrent:expr, $outgoing_torrent:expr, $incoming_extensions:expr, $outgoing_extensions:expr, $protocol_extensions:expr) => {{
        use crate::Torrent;
        use crate::peer::BitTorrentPeer;
        use crate::peer::PeerId;
        use crate::peer::ProtocolExtensionFlags;
        use crate::peer::extension::PeerExtension;
        use std::net::Ipv4Addr;
        use std::time::Duration;
        use tokio::net::{TcpListener, TcpStream};
        use tokio::sync::oneshot;

        let incoming_torrent: &Torrent = $incoming_torrent;
        let outgoing_torrent: &Torrent = $outgoing_torrent;
        let incoming_extensions: Vec<PeerExtension> = $incoming_extensions;
        let outgoing_extensions: Vec<PeerExtension> = $outgoing_extensions;
        let protocol_extensions: ProtocolExtensionFlags = $protocol_extensions;

        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let outgoing_addr = listener.local_addr().unwrap();
        let outgoing_stream = TcpStream::connect(outgoing_addr).await.unwrap();
        let (incoming_stream, incoming_addr) = listener.accept().await.unwrap();

        // offload the incoming peer to a separate task
        // this is required, as the `new_inbound` wait for the handshake to be completed before returning
        let incoming_peer = BitTorrentPeer::new_inbound(
            PeerId::new(),
            incoming_addr,
            incoming_torrent.inner.peer_port().await,
            incoming_torrent.inner.config().await.unwrap().client_name().to_string(),
            incoming_stream.into(),
            incoming_torrent.inner.clone(),
            incoming_torrent.metadata().await.unwrap(),
            incoming_torrent.inner.data_pool().await.unwrap(),
            incoming_torrent.inner.storage().await.unwrap(),
            protocol_extensions,
            incoming_extensions,
            Duration::from_secs(2),
        );
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let result = incoming_peer.await.expect("expected the incoming peer to have been created");
            let _ = tx.send(result);
        });

        let outgoing_peer = BitTorrentPeer::new_outbound(
            PeerId::new(),
            outgoing_addr,
            outgoing_torrent.inner.peer_port().await,
            outgoing_torrent.inner.config().await.unwrap().client_name().to_string(),
            outgoing_stream.into(),
            outgoing_torrent.inner.clone(),
            outgoing_torrent.metadata().await.unwrap(),
            outgoing_torrent.inner.data_pool().await.unwrap(),
            outgoing_torrent.inner.storage().await.unwrap(),
            protocol_extensions,
            outgoing_extensions,
            Duration::from_secs(2),
        ).await.expect("expected the outgoing peer to have been created");

        let incoming_peer = rx.await.expect("expected the incoming peer to have been received");
        (incoming_peer, outgoing_peer)
    }}
}

/// Create a new pair of uTP peers.
macro_rules! utp_peer_pair {
    ($torrent:expr, $extensions:expr) => {{
        use crate::peer::ProtocolExtensionFlags;

        utp_peer_pair!($torrent, $torrent, $extensions, $extensions, ProtocolExtensionFlags::none())
    }};
    ($incoming_torrent:expr, $outgoing_torrent:expr, $incoming_extensions:expr, $outgoing_extensions:expr, $protocol_extensions:expr) => {{
        use core::net::{SocketAddr, Ipv4Addr};

        let incoming_socket = crate::peer::protocol::UtpSocket::bind(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            vec![],
        ).await.unwrap();
        let outgoing_socket = crate::peer::protocol::UtpSocket::bind(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            vec![],
        ).await.unwrap();

        let pair = utp_peer_pair!(
            $incoming_torrent,
            $outgoing_torrent,
            $incoming_extensions,
            $outgoing_extensions,
            $protocol_extensions,
            &incoming_socket,
            &outgoing_socket
        );

        (pair.0, pair.1, incoming_socket, outgoing_socket)
    }};
    ($incoming_torrent:expr, $outgoing_torrent:expr, $incoming_extensions:expr, $outgoing_extensions:expr, $protocol_extensions:expr, $in_socket:expr, $out_socket:expr) => {{
        use crate::Torrent;
        use crate::peer::extension::PeerExtension;
        use crate::peer::protocol::UtpSocket;
        use crate::peer::{BitTorrentPeer, PeerId, ProtocolExtensionFlags};
        use std::time::Duration;
        use tokio::sync::oneshot;

        let incoming_torrent: &Torrent = $incoming_torrent;
        let outgoing_torrent: &Torrent = $outgoing_torrent;
        let incoming_extensions: Vec<PeerExtension> = $incoming_extensions;
        let outgoing_extensions: Vec<PeerExtension> = $outgoing_extensions;
        let protocol_extensions: ProtocolExtensionFlags = $protocol_extensions;

        let incoming_socket: &UtpSocket = $in_socket;
        let outgoing_socket: &UtpSocket = $out_socket;

        let outgoing_stream = outgoing_socket
            .connect(incoming_socket.addr())
            .await
            .expect("expected an outgoing uTP stream to be established");
        let incoming_stream = incoming_socket
            .recv()
            .await
            .expect("expected an incoming uTP stream to be established");

        // offload the incoming peer to a separate task
        // this is required, as the `new_inbound` wait for the handshake to be completed before returning
        let incoming_peer = BitTorrentPeer::new_inbound(
            PeerId::new(),
            incoming_stream.addr(),
            incoming_torrent.inner.peer_port().await,
            incoming_torrent.inner.config().await.unwrap().client_name().to_string(),
            incoming_stream.into(),
            incoming_torrent.inner.clone(),
            incoming_torrent.inner.metadata().await.unwrap(),
            incoming_torrent.inner.data_pool().await.unwrap(),
            incoming_torrent.inner.storage().await.unwrap(),
            protocol_extensions,
            incoming_extensions,
            Duration::from_secs(2),
        );
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let result = incoming_peer.await.expect("expected the incoming peer to have been created");
            let _ = tx.send(result);
        });

        let outgoing_config = outgoing_torrent.inner.config().await.unwrap();
        let outgoing_peer = BitTorrentPeer::new_outbound(
            PeerId::new(),
            outgoing_stream.addr(),
            outgoing_torrent.inner.peer_port().await,
            outgoing_config.client_name().to_string(),
            outgoing_stream.into(),
            outgoing_torrent.inner.clone(),
            outgoing_torrent.inner.metadata().await.unwrap(),
            outgoing_torrent.inner.data_pool().await.unwrap(),
            outgoing_torrent.inner.storage().await.unwrap(),
            protocol_extensions,
            outgoing_extensions,
            Duration::from_secs(2),
        ).await.expect("expected the outgoing peer to have been created");

        let incoming_peer = rx.await.expect("expected the incoming peer to have been created");
        (incoming_peer, outgoing_peer)
    }};
}

pub(crate) static INIT: Once = Once::new();

/// Initializes the logger with the specified log level.
macro_rules! init_logger {
    () => {{
        init_logger!(log::LevelFilter::Trace)
    }};
    ($level:expr) => {{
        use log4rs::config::runtime::{Appender, Config, Logger, Root};
        use log4rs::append::console::ConsoleAppender;
        use log4rs::encode::pattern::PatternEncoder;
        use log::LevelFilter;

        let level: LevelFilter = $level;

        crate::test_macros::INIT.call_once(|| {
            log4rs::init_config(Config::builder()
                .appender(Appender::builder().build("stdout", Box::new(ConsoleAppender::builder()
                    .encoder(Box::new(PatternEncoder::new("\x1B[37m{d(%Y-%m-%d %H:%M:%S%.3f)}\x1B[0m {h({l:>5.5})} \x1B[35m{I:>6.6}\x1B[0m \x1B[37m---\x1B[0m \x1B[37m[{T:>15.15}]\x1B[0m \x1B[36m{t:<60.60}\x1B[0m \x1B[37m:\x1B[0m {m}{n}")))
                    .build())))
                .logger(Logger::builder().build("axum", LevelFilter::Info))
                .logger(Logger::builder().build("fx_callback", LevelFilter::Info))
                .logger(Logger::builder().build("httpmock", LevelFilter::Debug))
                .logger(Logger::builder().build("hyper_util", LevelFilter::Info))
                .logger(Logger::builder().build("mio", LevelFilter::Info))
                .logger(Logger::builder().build("reqwest", LevelFilter::Info))
                .build(Root::builder().appender("stdout").build(level))
                .unwrap())
                .unwrap();
        })
    }};
}

/// Create the torrent metadata from the given uri.
/// The uri can either point to a `.torrent` file or a magnet link.
macro_rules! metadata {
    ($uri:expr) => {{
        use crate::tests::read_test_file_to_bytes;
        use crate::{Magnet, TorrentMetadata};
        use std::str::FromStr;

        let uri: &str = $uri;

        if uri.starts_with("magnet:") {
            let magnet = Magnet::from_str(uri).unwrap();
            TorrentMetadata::try_from(magnet).unwrap()
        } else {
            let torrent_info_data = read_test_file_to_bytes(uri);
            TorrentMetadata::try_from(torrent_info_data.as_slice()).unwrap()
        }
    }};
}

/// Mark the given piece as completed within the torrent.
macro_rules! mark_piece_completed {
    ($command_sender:expr, $piece:expr, $peer_addr:expr, $test_file:expr) => {{
        use crate::channel::ChannelSender;
        use crate::tests::read_test_file_to_bytes;
        use crate::torrent_data::DataPool;
        use crate::Piece;
        use crate::PieceIndex;
        use crate::TorrentCommand;
        use std::net::SocketAddr;

        let command_sender: &ChannelSender<TorrentCommand> = $command_sender;
        let piece: PieceIndex = $piece;
        let peer_addr: &SocketAddr = $peer_addr;
        let test_file: &str = $test_file;

        let test_data = read_test_file_to_bytes(test_file);
        let data_pool: DataPool = command_sender
            .send(|tx| TorrentCommand::DataPool { response: tx })
            .await
            .await
            .unwrap();
        let piece: Piece = data_pool.piece(&piece).await.unwrap();

        for block in &piece.blocks {
            let start = piece.offset + block.begin;
            let end = start + block.length;

            command_sender
                .fire_and_forget(TorrentCommand::PieceBlockReceived {
                    peer_addr: *peer_addr,
                    block: block.clone(),
                    data: test_data[start..end].to_vec(),
                })
                .await;
        }
    }};
}

/// Asserts that a condition is true within a specified timeout.
macro_rules! assert_timeout {
    ($timeout:expr, $condition:expr) => {{
        assert_timeout!($timeout, $condition, "")
    }};
    ($timeout:expr, $condition:expr, $message:expr) => {{
        use std::time::Duration;
        use tokio::select;
        use tokio::time;

        let result = select! {
            _ = time::sleep($timeout) => false,
            result = async {
                loop {
                    if $condition {
                        return true;
                    }

                    time::sleep(Duration::from_millis(10)).await;
                }
            } => result,
        };

        if !result {
            assert!(
                false,
                concat!("Timeout assertion failed after {:?}: ", $message),
                $timeout
            );
        }
    }};
}

/// A macro wrapper for [`tokio::time::timeout`] that awaits a future with a timeout duration.
macro_rules! timeout {
    ($duration:expr, $future:expr) => {{
        timeout!($duration, $future, "operation timed-out")
    }};
    ($duration:expr, $future:expr, $message:expr) => {{
        use std::io;
        use std::time::Duration;
        use tokio::time::timeout;

        let future = $future;
        let duration: Duration = $duration;

        timeout(duration, future)
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("after {}.{:03}s", duration.as_secs(), duration.as_millis()),
                )
            })
            .expect($message)
    }};
}
