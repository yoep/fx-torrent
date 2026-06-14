#[cfg(feature = "lsd")]
use crate::operation::LsdPeersOperation;
use crate::operation::{
    ConnectPeersOperation, CreatePiecesAndFilesOperation, FileValidationOperation,
    MetadataOperation, StatsOperation, TrackerPeersOperation, TrackersOperation,
};
#[cfg(feature = "dht")]
use crate::operation::{DhtNodesOperation, DhtPeersOperation};
use crate::peer::PeerDiscovery;
use crate::TorrentContext;
use async_trait::async_trait;
use std::fmt::Debug;

/// The torrent operation, executed within the tick event loop of the torrent context.
#[derive(Debug)]
pub enum Operation {
    Stats(StatsOperation),
    Trackers(TrackersOperation),
    #[cfg(feature = "dht")]
    DhtNodes(DhtNodesOperation),
    #[cfg(feature = "dht")]
    DhtPeers(DhtPeersOperation),
    #[cfg(feature = "lsd")]
    LsdPeers(LsdPeersOperation),
    TrackerPeers(TrackerPeersOperation),
    ConnectPeers(ConnectPeersOperation),
    Metadata(MetadataOperation),
    PiecesAndFiles(CreatePiecesAndFilesOperation),
    Validation(FileValidationOperation),
    Other(Box<dyn Extension>),
}

impl Operation {
    /// Returns the name of the torrent operation.
    pub fn name(&self) -> &str {
        match self {
            Operation::Stats(_) => "stats operation",
            Operation::Trackers(_) => "connect trackers operation",
            #[cfg(feature = "dht")]
            Operation::DhtNodes(_) => "connect torrent DHT nodes operation",
            #[cfg(feature = "dht")]
            Operation::DhtPeers(_) => "retrieve DHT peers operation",
            #[cfg(feature = "lsd")]
            Operation::LsdPeers(_) => "retrieve lsd peers operation",
            Operation::TrackerPeers(_) => "retrieve tracker peers operation",
            Operation::ConnectPeers(_) => "create peer connections operation",
            Operation::Metadata(_) => "retrieve metadata operation",
            Operation::PiecesAndFiles(_) => "create pieces operation",
            Operation::Validation(_) => "file validation operation",
            Operation::Other(op) => op.name(),
        }
    }

    /// Execute the operation for the given torrent.
    /// The [TorrentContext] reference exposes additional internal data of the torrent which is otherwise not exposed on the [Torrent].
    pub async fn execute(
        &mut self,
        context: &mut TorrentContext,
        peer_discoveries: &[PeerDiscovery],
    ) -> TorrentOperationResult {
        match self {
            Operation::Stats(op) => op.execute(context).await,
            Operation::Trackers(op) => op.execute(context).await,
            #[cfg(feature = "dht")]
            Operation::DhtNodes(op) => op.execute(context).await,
            #[cfg(feature = "dht")]
            Operation::DhtPeers(op) => op.execute(context).await,
            #[cfg(feature = "lsd")]
            Operation::LsdPeers(op) => op.execute(context).await,
            Operation::TrackerPeers(op) => op.execute(context).await,
            Operation::ConnectPeers(op) => op.execute(context, peer_discoveries).await,
            Operation::Metadata(op) => op.execute(context).await,
            Operation::PiecesAndFiles(op) => op.execute(context).await,
            Operation::Validation(op) => op.execute(context).await,
            Operation::Other(op) => op.execute(context, peer_discoveries).await,
        }
    }

    /// Returns the list of default operations for the torrent.
    pub fn default_operations() -> Vec<Operation> {
        vec![
            StatsOperation::new().into(),
            TrackersOperation::new().into(),
            #[cfg(feature = "dht")]
            DhtNodesOperation::new().into(),
            #[cfg(feature = "dht")]
            DhtPeersOperation::new().into(),
            #[cfg(feature = "lsd")]
            LsdPeersOperation::new().into(),
            TrackerPeersOperation::new().into(),
            ConnectPeersOperation::new(true).into(),
            MetadataOperation::new(None).into(),
            CreatePiecesAndFilesOperation::new().into(),
            FileValidationOperation::new().into(),
        ]
    }
}

impl From<StatsOperation> for Operation {
    fn from(value: StatsOperation) -> Self {
        Self::Stats(value)
    }
}

impl From<TrackersOperation> for Operation {
    fn from(value: TrackersOperation) -> Self {
        Self::Trackers(value)
    }
}

#[cfg(feature = "dht")]
impl From<DhtNodesOperation> for Operation {
    fn from(value: DhtNodesOperation) -> Self {
        Self::DhtNodes(value)
    }
}

#[cfg(feature = "dht")]
impl From<DhtPeersOperation> for Operation {
    fn from(value: DhtPeersOperation) -> Self {
        Self::DhtPeers(value)
    }
}

#[cfg(feature = "lsd")]
impl From<LsdPeersOperation> for Operation {
    fn from(value: LsdPeersOperation) -> Self {
        Self::LsdPeers(value)
    }
}

impl From<TrackerPeersOperation> for Operation {
    fn from(value: TrackerPeersOperation) -> Self {
        Self::TrackerPeers(value)
    }
}

impl From<ConnectPeersOperation> for Operation {
    fn from(value: ConnectPeersOperation) -> Self {
        Self::ConnectPeers(value)
    }
}

impl From<MetadataOperation> for Operation {
    fn from(value: MetadataOperation) -> Self {
        Self::Metadata(value)
    }
}

impl From<CreatePiecesAndFilesOperation> for Operation {
    fn from(value: CreatePiecesAndFilesOperation) -> Self {
        Self::PiecesAndFiles(value)
    }
}

impl From<FileValidationOperation> for Operation {
    fn from(value: FileValidationOperation) -> Self {
        Self::Validation(value)
    }
}

impl<E> From<E> for Operation
where
    E: Extension + Send + Sync + 'static,
{
    fn from(value: E) -> Self {
        Self::Other(Box::new(value))
    }
}

/// The operation extension which is executed in a chain during the lifetime of the torrent.
/// It provides a specific operation to be executed on the torrent in a sequential order.
///
/// The operation is always specific to one torrent, but should be allowed to create a new instance of the operation.
/// This allows the operation to store data which is specific to the torrent.
#[async_trait]
pub trait Extension: Debug + Send + Sync {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestOperation;
    #[async_trait]
    impl Extension for TestOperation {
        fn name(&self) -> &str {
            "test operation"
        }

        async fn execute(
            &mut self,
            _: &mut TorrentContext,
            _: &[PeerDiscovery],
        ) -> TorrentOperationResult {
            TorrentOperationResult::Continue
        }
    }

    #[test]
    fn test_operation_from_extension() {
        let result: Operation = TestOperation.into();
        assert_eq!(result.name(), "test operation");
    }
}
