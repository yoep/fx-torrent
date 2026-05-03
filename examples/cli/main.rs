mod app;
mod app_logger;
mod app_settings;
mod dht_info;
mod menu;
mod torrent;
mod tracker_info;
mod widgets;

use crate::app::App;
use crate::app_logger::AppLogger;
#[cfg(not(feature = "tracing"))]
use log::LevelFilter;
use std::io;
use tokio::select;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
    let app_logger = AppLogger::new();
    #[cfg(feature = "tracing")]
    tracing::init_tracing(app_logger.clone());
    let mut app = App::new(app_logger.clone()).await?;
    let terminal = ratatui::init();

    #[cfg(not(feature = "tracing"))]
    init_logger(app_logger)?;

    let result = select! {
        _ = tokio::signal::ctrl_c() => Ok(()),
        result = app.run(terminal) => result,
    };

    ratatui::restore();
    result
}

#[cfg(not(feature = "tracing"))]
fn init_logger(app_logger: AppLogger) -> io::Result<()> {
    log::set_boxed_logger(Box::new(app_logger))
        .map(|()| log::set_max_level(LevelFilter::Trace))
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

#[cfg(feature = "tracing")]
mod tracing {
    use super::*;

    use console_subscriber::ConsoleLayer;
    use std::time::Duration;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    pub fn init_tracing(app_logger: AppLogger) {
        let console_layer = ConsoleLayer::builder()
            .with_default_env()
            .retention(Duration::from_secs(60))
            .spawn();

        tracing_subscriber::registry()
            .with(console_layer)
            .with(app_logger)
            .init();
    }
}
