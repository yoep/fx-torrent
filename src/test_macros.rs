use std::sync::Once;

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

/// Create a [TorrentContext] instance for the given uri and options.
#[macro_export]
macro_rules! create_torrent_context {
    ($uri:expr, $temp_dir:expr, $options:expr) => {{
        create_torrent_context!(
            $uri,
            $temp_dir,
            $options,
            crate::TorrentConfig::builder().path($temp_dir).build()
        )
    }};
    ($uri:expr, $temp_dir:expr, $options:expr, $config:expr) => {{
        use crate::peer::{PeerDiscovery, TcpPeerDiscovery, UtpPeerDiscovery};

        let tcp_discovery = TcpPeerDiscovery::new()
            .await
            .expect("expected a new tcp peer discovery");
        let utp_discovery = UtpPeerDiscovery::new()
            .await
            .expect("expected a new utp peer discovery");
        let discoveries: Vec<Box<dyn PeerDiscovery>> =
            vec![Box::new(tcp_discovery), Box::new(utp_discovery)];

        create_torrent_context!($uri, $temp_dir, $options, $config, discoveries)
    }};
    ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $discoveries:expr) => {{
        create_torrent_context!(
            $uri,
            $temp_dir,
            $options,
            $config,
            $discoveries,
            Some(crate::dht::DhtTracker::builder().build().await.unwrap())
        )
    }};
    ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $discoveries:expr, $dht:expr) => {{
        use std::sync::Arc;

        create_torrent_context!(
            $uri,
            $temp_dir,
            $options,
            $config,
            $discoveries,
            $dht,
            None,
            |_, _| Arc::new(crate::storage::MemoryStorage::new())
        )
    }};
    ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $discoveries:expr, $dht:expr, $lsd:expr) => {{
        use std::sync::Arc;

        create_torrent_context!(
            $uri,
            $temp_dir,
            $options,
            $config,
            $discoveries,
            $dht,
            $lsd,
            |_, _| Arc::new(crate::storage::MemoryStorage::new())
        )
    }};
    ($uri:expr, $temp_dir:expr, $options:expr, $config:expr, $discoveries:expr, $dht:expr, $lsd:expr, $storage:expr) => {{
        use crate::dht::DhtTracker;
        use crate::peer::PeerDiscovery;
        use crate::torrent_data::DataPool;
        use crate::tracker::TrackerClient;
        use crate::LocalServiceDiscovery;
        use crate::{
            TorrentConfig, TorrentContext, TorrentFlags, TorrentMetadata,
            DEFAULT_TORRENT_EXTENSIONS, DEFAULT_TORRENT_PROTOCOL_EXTENSIONS,
        };
        use std::time::Duration;

        let uri: &str = $uri;
        let options: TorrentFlags = $options;
        let config: TorrentConfig = $config;
        let discoveries: Vec<Box<dyn PeerDiscovery>> = $discoveries;
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

        (
            TorrentContext::new(
                metadata,
                config,
                discoveries.first().map(|e| e.port()),
                DEFAULT_TORRENT_PROTOCOL_EXTENSIONS(),
                DEFAULT_TORRENT_EXTENSIONS(),
                options,
                data_pool.clone(),
                trackers,
                ($storage)(info_hash, data_pool),
                command_sender,
            ),
            receiver,
        )
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
    ($future:expr, $duration:expr) => {{
        timeout!($future, $duration, "operation timed-out")
    }};
    ($future:expr, $duration:expr, $message:expr) => {{
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
