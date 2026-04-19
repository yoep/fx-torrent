#[cfg(feature = "dht")]
pub use connect_dht_nodes::*;
pub use connect_peers::*;
pub use connect_trackers::*;
pub use create_pieces_and_files::*;
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
#[cfg(feature = "dht")]
mod retrieve_dht_peers;
#[cfg(feature = "lsd")]
mod retrieve_lsd_peers;
mod retrieve_metadata;
mod retrieve_tracker_peers;
mod stats;
mod validate_files;

use crate::peer::PeerDiscovery;
use crate::torrent::TorrentContext;
use async_trait::async_trait;
use std::fmt::Debug;

/// The default list of operations which are executed in a chain during the lifetime of the torrent.
/// The operations are executed in the order they are defined in this constant.
pub(crate) const DEFAULT_OPERATIONS: fn() -> Vec<Box<dyn TorrentOperation>> = || {
    vec![
        Box::new(TorrentStatsOperation::new()),
        Box::new(TorrentTrackersOperation::new()),
        #[cfg(feature = "dht")]
        Box::new(TorrentDhtNodesOperation::new()),
        #[cfg(feature = "dht")]
        Box::new(TorrentDhtPeersOperation::new()),
        #[cfg(feature = "lsd")]
        Box::new(TorrentLsdPeersOperation::new()),
        Box::new(TorrentTrackerPeersOperation::new()),
        Box::new(TorrentConnectPeersOperation::new(true)),
        Box::new(TorrentMetadataOperation::new(None)),
        Box::new(TorrentCreatePiecesAndFilesOperation::new()),
        Box::new(TorrentFileValidationOperation::new()),
    ]
};

/// A torrent operation which is executed in a chain during the lifetime of the torrent.
/// It provides a specific operation to be executed on the torrent in a sequential order.
///
/// The operation is always specific to one torrent, but should be allowed to create a new instance of the operation.
/// This allows the operation to store data which is specific to the torrent.
#[async_trait]
pub trait TorrentOperation: Debug + Send {
    /// Get the unique name of the operation.
    fn name(&self) -> &str;

    /// Execute the operation for the given torrent.
    /// The [TorrentContext] reference exposes additional internal data of the torrent which is otherwise not exposed on the [Torrent].
    async fn execute(
        &mut self,
        context: &mut TorrentContext,
        peer_discoveries: &[PeerDiscovery],
    ) -> TorrentOperationResult;
}

/// The result of executing a torrent operation.
#[derive(Debug, PartialEq)]
pub enum TorrentOperationResult {
    /// Continue the operations chain
    Continue,
    /// Stop the operations chain
    Stop,
}

/// The [Torrent] operation factory.
/// Creates a new instance of a [TorrentOperation] for each new torrent.
pub struct TorrentOperationFactory {
    make: Box<dyn Fn() -> Box<dyn TorrentOperation> + Send + Sync>,
}

impl TorrentOperationFactory {
    /// Create a new [TorrentOperation] factory for the given closure.
    pub fn new<F>(make: F) -> Self
    where
        F: Fn() -> Box<dyn TorrentOperation> + Send + Sync + 'static,
    {
        Self {
            make: Box::new(make),
        }
    }

    /// Create a new [TorrentOperation] instance using the factory.
    pub fn create(&self) -> Box<dyn TorrentOperation> {
        (self.make)()
    }
}

impl<F> From<F> for TorrentOperationFactory
where
    F: Fn() -> Box<dyn TorrentOperation> + Send + Sync + 'static,
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
        let factory = TorrentOperationFactory::new(|| Box::new(TorrentStatsOperation::new()));

        let instance = factory.create();
        let result = instance.name();
        assert_eq!(result, "torrent stats operation");
    }

    #[test]
    fn test_into_factory() {
        let factory: TorrentOperationFactory =
            (|| -> Box<dyn TorrentOperation> { Box::new(TorrentStatsOperation::new()) }).into();

        let instance = factory.create();
        let result = instance.name();
        assert_eq!(result, "torrent stats operation");
    }
}
