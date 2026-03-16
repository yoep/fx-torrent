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
            .expect("operation timed-out")
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
