use chrono::Local;
use log::{Level, Log, Metadata, Record};
use std::collections::VecDeque;
use std::io;
use std::io::Write;
use std::sync::{Arc, Mutex};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

const LOG_FILE_PATH: &str = "fx-torrent.log";

/// A log entry of the application.
#[derive(Debug)]
pub struct LogEntry {
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct AppLogger {
    inner: Arc<InnerAppLogger>,
}

impl AppLogger {
    pub fn new() -> Self {
        let (log_sender, log_receiver) = unbounded_channel();

        let inner = Arc::new(InnerAppLogger {
            loggers: Mutex::new(
                vec![
                    Logger {
                        name: "DHT".to_string(),
                        target: "fx_torrent::dht".to_string(),
                        level: Level::Info,
                    },
                    Logger {
                        name: "DNS".to_string(),
                        target: "fx_torrent::dns".to_string(),
                        level: Level::Info,
                    },
                    Logger {
                        name: "Operations".to_string(),
                        target: "fx_torrent::operation".to_string(),
                        level: Level::Info,
                    },
                    Logger {
                        name: "Peers".to_string(),
                        target: "fx_torrent::peer".to_string(),
                        level: Level::Info,
                    },
                    Logger {
                        name: "Peer protocol".to_string(),
                        target: "fx_torrent::peer::protocol".to_string(),
                        level: Level::Info,
                    },
                    Logger {
                        name: "Session".to_string(),
                        target: "fx_torrent::session".to_string(),
                        level: Level::Info,
                    },
                    Logger {
                        name: "Torrent".to_string(),
                        target: "fx_torrent::torrent".to_string(),
                        level: Level::Info,
                    },
                    Logger {
                        name: "Trackers".to_string(),
                        target: "fx_torrent::tracker".to_string(),
                        level: Level::Info,
                    },
                ]
                .into_iter()
                .collect(),
            ),
            logs: Mutex::new(VecDeque::new()),
            log_sender,
        });

        tokio::spawn(async move {
            let mut logfile_writer = AppLogfileWriter::new();
            logfile_writer.start(log_receiver).await;
        });

        Self { inner }
    }

    /// Try to get the next log entry from the logger.
    pub fn next(&self) -> Option<LogEntry> {
        self.inner.logs.lock().ok().and_then(|mut e| e.pop_front())
    }

    /// Get the current configured loggers with their log level
    pub fn loggers(&self) -> Vec<Logger> {
        self.inner
            .loggers
            .lock()
            .ok()
            .map(|e| e.clone())
            .unwrap_or_default()
    }

    /// Update the log level for the given target.
    pub fn update<S: AsRef<str>>(&self, target: S, level: &Level) {
        if let Ok(mut loggers) = self.inner.loggers.lock() {
            if let Some(logger) = loggers.iter_mut().find(|e| &e.target == target.as_ref()) {
                logger.level = *level;
            }
        }
    }
}

impl Log for AppLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        self.inner.enabled(metadata)
    }

    fn log(&self, record: &Record) {
        if !Log::enabled(&self, record.metadata()) {
            return;
        }

        self.inner.send_entry(record);
    }

    fn flush(&self) {
        // no-op
    }
}

#[derive(Debug)]
struct InnerAppLogger {
    loggers: Mutex<Vec<Logger>>,
    logs: Mutex<VecDeque<LogEntry>>,
    log_sender: UnboundedSender<LogEntry>,
}

impl InnerAppLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        let target = metadata.target();
        let level = self
            .loggers
            .lock()
            .ok()
            .map(|e| {
                let mut level = Level::Info;
                let mut last_overlap_size = 0u32;

                for conf in e.iter() {
                    let overlap = Self::overlap_size(conf.target.as_str(), target);

                    if overlap > last_overlap_size {
                        level = conf.level.clone();
                        last_overlap_size = overlap;
                    }
                }

                level
            })
            .unwrap_or(Level::Info);

        metadata.level().to_level_filter() <= level.to_level_filter()
    }

    fn send_entry(&self, record: &Record) {
        let time = Local::now();
        let target = {
            let target = record.target();
            let target = format!("{}{}", target, " ".repeat(40));
            target[0..40].to_string()
        };
        let text = format!(
            "{} {} --- {} : {}",
            time.format("%Y-%m-%d %H:%M:%S%.f"),
            record.level(),
            target,
            record.args()
        );

        if let Ok(mut logs) = self.logs.lock() {
            logs.push_back(LogEntry { text: text.clone() });
        }

        let _ = self.log_sender.send(LogEntry { text });
    }

    fn overlap_size(logger: &str, target: &str) -> u32 {
        let mut logger_chars = logger.chars();
        let mut target_chars = target.chars();

        let mut count = 0;
        loop {
            match (logger_chars.next(), target_chars.next()) {
                (Some(a), Some(b)) if a == b => {
                    count += 1;
                }
                _ => return count,
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Logger {
    /// The display name of the logger
    pub name: String,
    /// The log target of the logger
    pub target: String,
    /// The configured level of the logger
    pub level: Level,
}

#[derive(Debug)]
struct AppLogfileWriter {}

impl AppLogfileWriter {
    fn new() -> Self {
        Self {}
    }

    async fn start(&mut self, mut log_receiver: UnboundedReceiver<LogEntry>) {
        if let Ok(mut file) = Self::create_logfile().await {
            while let Some(log) = log_receiver.recv().await {
                let mut buf = Vec::new();
                let _ = writeln!(buf, "{}", log.text);
                let _ = file.write_all(&buf).await;
            }
        }
    }

    async fn create_logfile() -> Result<File, io::Error> {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(LOG_FILE_PATH)
            .await
    }
}

#[cfg(feature = "tracing")]
mod log_tracing {
    use super::*;
    use std::fmt::Debug;
    use tracing::field::{Field, Visit};
    use tracing::{Event, Subscriber};
    use tracing_subscriber::layer::Context;
    use tracing_subscriber::registry::LookupSpan;
    use tracing_subscriber::Layer;

    impl<S> Layer<S> for AppLogger
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            let metadata = event.metadata();
            let level = metadata.level();
            let target = metadata.target();
            if target != "log" {
                return;
            }

            let mut visitor = EventVisitor {
                message: None,
                target: None,
                module_path: None,
            };
            event.record(&mut visitor);

            let target = visitor.target.unwrap_or_default();
            let metadata = Metadata::builder()
                .level(match *level {
                    tracing::Level::ERROR => Level::Error,
                    tracing::Level::WARN => Level::Warn,
                    tracing::Level::INFO => Level::Info,
                    tracing::Level::DEBUG => Level::Debug,
                    tracing::Level::TRACE => Level::Trace,
                })
                .target(target.as_str())
                .build();

            if self.inner.enabled(&metadata) {
                self.inner.send_entry(
                    &Record::builder()
                        .metadata(metadata)
                        .module_path(visitor.module_path.as_deref())
                        .args(format_args!("{}", visitor.message.unwrap_or_default()))
                        .build(),
                )
            }
        }
    }

    #[derive(Debug)]
    struct EventVisitor {
        message: Option<String>,
        target: Option<String>,
        module_path: Option<String>,
    }

    impl Visit for EventVisitor {
        fn record_str(&mut self, field: &Field, value: &str) {
            match field.name() {
                "message" => {
                    self.message = Some(value.to_string());
                }
                "log.target" => {
                    self.target = Some(value.to_string());
                }
                "log.module_path" => {
                    self.module_path = Some(value.to_string());
                }
                _ => {}
            }
        }

        fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
            match field.name() {
                "message" => {
                    self.message = Some(format!("{:?}", value));
                }
                "log.target" => {
                    self.target = Some(format!("{:?}", value));
                }
                "log.module_path" => {
                    self.module_path = Some(format!("{:?}", value));
                }
                _ => {}
            }
        }
    }
}
