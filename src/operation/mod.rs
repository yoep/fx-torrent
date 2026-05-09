#[cfg(feature = "dht")]
pub use connect_dht_nodes::*;
pub use connect_peers::*;
pub use connect_trackers::*;
pub use create_pieces_and_files::*;
pub use op::*;
#[cfg(feature = "dht")]
pub use retrieve_dht_peers::*;
#[cfg(feature = "lsd")]
pub use retrieve_lsd_peers::*;
pub use retrieve_metadata::*;
pub use retrieve_tracker_peers::*;
pub use stats::*;
pub use validate_files::*;

#[cfg(feature = "dht")]
mod connect_dht_nodes;
mod connect_peers;
mod connect_trackers;
mod create_pieces_and_files;
mod op;
#[cfg(feature = "dht")]
mod retrieve_dht_peers;
#[cfg(feature = "lsd")]
mod retrieve_lsd_peers;
mod retrieve_metadata;
mod retrieve_tracker_peers;
mod stats;
mod validate_files;

use std::fmt::Debug;

/// The [Torrent] operation factory.
/// Creates a new instance of a [Extension] for each new torrent.
pub struct TorrentOperationFactory {
    make: Box<dyn Fn() -> Operation + Send + Sync>,
}

impl TorrentOperationFactory {
    /// Create a new [Extension] factory for the given closure.
    pub fn new<F>(make: F) -> Self
    where
        F: Fn() -> Operation + Send + Sync + 'static,
    {
        Self {
            make: Box::new(make),
        }
    }

    /// Create a new [Extension] instance using the factory.
    pub fn create(&self) -> Operation {
        (self.make)()
    }
}

impl<F> From<F> for TorrentOperationFactory
where
    F: Fn() -> Operation + Send + Sync + 'static,
{
    fn from(value: F) -> Self {
        Self::new(value)
    }
}

impl Debug for TorrentOperationFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TorrentOperationFactory").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factory_new() {
        let factory = TorrentOperationFactory::new(|| StatsOperation::new().into());

        let instance = factory.create();
        let result = instance.name();
        assert_eq!(result, "stats operation");
    }

    #[test]
    fn test_into_factory() {
        let factory: TorrentOperationFactory =
            (|| -> Operation { StatsOperation::new().into() }).into();

        let instance = factory.create();
        let result = instance.name();
        assert_eq!(result, "stats operation");
    }
}
