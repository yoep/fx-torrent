use crate::storage::{DiskStorage, MemoryStorage, Metrics, Result};
use crate::torrent_data::DataPool;
use crate::{InfoHash, PieceIndex, Sha1Hash, Sha256Hash};
use async_trait::async_trait;
use std::fmt::Debug;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The storage type for storing and reading torrent data.
#[derive(Debug, Clone)]
pub enum Storage {
    Disk(Arc<DiskStorage>),
    Memory(Arc<MemoryStorage>),
    Other(Arc<dyn Extension>),
}

impl Storage {
    /// Read the torrent data from the given piece into the buffer.
    /// The [Extension] keeps reading piece(s) data until the buffer is filled,
    /// or the no more data is available.
    ///
    /// # Arguments
    ///
    /// * `buffer` - The buffer to write the bytes into.
    /// * `piece` - The piece index to read.
    /// * `offset` - The offset from the piece to start reading from.
    ///
    /// # Returns
    ///
    /// Returns the number of bytes read from the storage.
    pub async fn read(
        &self,
        buffer: &mut [u8],
        piece: &PieceIndex,
        offset: usize,
    ) -> Result<usize> {
        match self {
            Storage::Disk(storage) => storage.read(buffer, piece, offset).await,
            Storage::Memory(storage) => storage.read(buffer, piece, offset).await,
            Storage::Other(storage) => storage.read(buffer, piece, offset).await,
        }
    }

    /// Write the given data slice of the piece to the storage.
    ///
    /// # Arguments
    ///
    /// * `data` - The bytes to write to the storage.
    /// * `piece` - The piece index to write to.
    /// * `offset` - The offset within the piece to start writing to.
    ///
    /// # Returns
    ///
    /// Returns the number of bytes written to the storage.
    pub async fn write(&self, data: &[u8], piece: &PieceIndex, offset: usize) -> Result<usize> {
        match self {
            Storage::Disk(storage) => storage.write(data, piece, offset).await,
            Storage::Memory(storage) => storage.write(data, piece, offset).await,
            Storage::Other(storage) => storage.write(data, piece, offset).await,
        }
    }

    /// Calculate the hash for the given piece stored in the storage.
    pub async fn hash_v1(&self, piece: &PieceIndex) -> Result<Sha1Hash> {
        match self {
            Storage::Disk(storage) => storage.hash_v1(piece).await,
            Storage::Memory(storage) => storage.hash_v1(piece).await,
            Storage::Other(storage) => storage.hash_v1(piece).await,
        }
    }

    /// Calculate the hash for the given piece stored in the storage.
    pub async fn hash_v2(&self, piece: &PieceIndex) -> Result<Sha256Hash> {
        match self {
            Storage::Disk(storage) => storage.hash_v2(piece).await,
            Storage::Memory(storage) => storage.hash_v2(piece).await,
            Storage::Other(storage) => storage.hash_v2(piece).await,
        }
    }

    /// Move the storage to the new location path.
    pub async fn move_storage(&self, new_path: &Path) -> Result<()> {
        match self {
            Storage::Disk(storage) => storage.move_storage(new_path).await,
            Storage::Memory(_) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "unsupported operation",
            )),
            Storage::Other(storage) => storage.move_storage(new_path).await,
        }
    }

    /// Returns the storage metrics.
    pub fn metrics(&self) -> &Metrics {
        match self {
            Storage::Disk(storage) => storage.metrics(),
            Storage::Memory(storage) => storage.metrics(),
            Storage::Other(storage) => storage.metrics(),
        }
    }
}

impl From<DiskStorage> for Storage {
    fn from(storage: DiskStorage) -> Self {
        Storage::Disk(Arc::new(storage))
    }
}

impl From<MemoryStorage> for Storage {
    fn from(storage: MemoryStorage) -> Self {
        Storage::Memory(Arc::new(storage))
    }
}

impl<E> From<E> for Storage
where
    E: Extension + 'static,
{
    fn from(storage: E) -> Self {
        Storage::Other(Arc::new(storage))
    }
}

#[async_trait]
pub trait Extension: Debug + Send + Sync {
    /// Read the torrent data from the given piece into the buffer.
    /// The [Extension] keeps reading piece(s) data until the buffer is filled,
    /// or the no more data is available.
    ///
    /// # Arguments
    ///
    /// * `buffer` - The buffer to write the bytes into.
    /// * `piece` - The piece index to read.
    /// * `offset` - The offset from the piece to start reading from.
    ///
    /// # Returns
    ///
    /// Returns the number of bytes read from the storage.
    async fn read(&self, buffer: &mut [u8], piece: &PieceIndex, offset: usize) -> Result<usize>;

    /// Write the piece data to the storage for the given bytes.
    /// The given bytes should be verified against the hash before calling this fn.
    ///
    /// # Arguments
    ///
    /// * `data` - The bytes to write to the storage.
    /// * `piece` - The piece index to write to.
    /// * `offset` - The offset within the piece to start writing to.
    ///
    /// # Returns
    ///
    /// Returns the number of bytes written to the storage.
    async fn write(&self, data: &[u8], piece: &PieceIndex, offset: usize) -> Result<usize>;

    /// Calculate the hash for the given piece stored in the storage.
    async fn hash_v1(&self, piece: &PieceIndex) -> Result<Sha1Hash>;

    /// Calculate the hash for the given piece stored in the storage.
    async fn hash_v2(&self, piece: &PieceIndex) -> Result<Sha256Hash>;

    /// Move the storage to the new location path.
    async fn move_storage(&self, new_path: &Path) -> Result<()>;

    /// Returns the storage metrics.
    fn metrics(&self) -> &Metrics;
}

/// The storage parameters for initializing a new [Extension] instance.
#[derive(Debug, Clone)]
pub struct StorageParams {
    pub info_hash: InfoHash,
    pub path: PathBuf,
    pub data_pool: DataPool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockall::mock;
    use rand::{rng, Rng};

    mock! {
        #[derive(Debug)]
        pub Extension {}
        #[async_trait]
        impl Extension for Extension {
            async fn read(&self, buffer: &mut [u8], piece: &PieceIndex, offset: usize) -> Result<usize>;
            async fn write(&self, data: &[u8], piece: &PieceIndex, offset: usize) -> Result<usize>;
            async fn hash_v1(&self, piece: &PieceIndex) -> Result<Sha1Hash>;
            async fn hash_v2(&self, piece: &PieceIndex) -> Result<Sha256Hash>;
            async fn move_storage(&self, new_path: &Path) -> Result<()>;
            fn metrics(&self) -> &Metrics;
        }
    }

    #[tokio::test]
    async fn test_extension_read() {
        let mut extension = MockExtension::new();
        extension.expect_read().times(1).returning(|buffer, _, _| {
            rng().fill_bytes(buffer);
            Ok(buffer.len())
        });
        let storage: Storage = extension.into();

        let mut buffer = vec![0u8; 256];
        let result = storage.read(&mut buffer, &0, 0).await;
        match result {
            Ok(result) => {
                assert_eq!(256, result, "expected 256 bytes read");
            }
            _ => assert!(false, "expected Ok, but got {:?}", result),
        }
    }

    #[test]
    fn test_extension_metrics() {
        let metrics = Metrics::default();
        metrics.bytes_read.inc_by(20);
        let mut extension = MockExtension::new();
        extension
            .expect_metrics()
            .times(1)
            .return_const(metrics.clone());
        let storage: Storage = extension.into();

        let metrics = storage.metrics();

        assert_eq!(20, metrics.bytes_read.get());
    }
}
