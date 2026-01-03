mod app;
mod app_logger;
mod dht_info;
mod menu;
mod torrent_info;
mod tracker_info;
mod widget;

use crate::app::App;
use crate::app_logger::AppLogger;
#[cfg(not(feature = "tracing"))]
use log::LevelFilter;
use std::io;
use tokio::select;
#[cfg(feature = "tracing")]
use tracing_chrome::{ChromeLayerBuilder, FlushGuard};
#[cfg(feature = "tracing")]
use tracing_subscriber::layer::SubscriberExt;
#[cfg(feature = "tracing")]
use tracing_subscriber::util::SubscriberInitExt;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
    let app_logger = AppLogger::new();
    #[cfg(feature = "tracing")]
    let _tracing_guard = init_tracing(app_logger.clone());
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

#[cfg(feature = "tracing")]
fn init_tracing(app_logger: AppLogger) -> FlushGuard {
    let (chrome_layer, guard) = ChromeLayerBuilder::new().build();
    tracing_subscriber::registry()
        .with(chrome_layer)
        .with(app_logger)
        .init();
    guard
}

#[cfg(not(feature = "tracing"))]
fn init_logger(app_logger: AppLogger) -> io::Result<()> {
    log::set_boxed_logger(Box::new(app_logger))
        .map(|()| log::set_max_level(LevelFilter::Trace))
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}
