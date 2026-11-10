use crate::storage::{unavailable, DiskStorage, Error, Metrics, PartFile, Result};
use crate::torrent_data::DataPool;
use crate::{
    FileAttributeFlags, FileIndex, FilePriority, InfoHash, PieceIndex, Sha1Hash, Sha256Hash,
};
use sha1::digest::Output;
use sha1::{Digest, Sha1};
use sha2::Sha256;
use std::cmp::min;
use std::io;
use std::path::{Path, PathBuf};
use tokio::sync::OnceCell;
#[cfg(feature = "tracing")]
use tracing::instrument;

/// A memory-mapped storage, which stores the data either in a [DiskStorage] or [PartFile].
#[derive(Debug)]
pub struct MMapStorage {
    info_hash: InfoHash,
    storage: DiskStorage,
    part_file: OnceCell<PartFile>,
    data_pool: DataPool,
    path: PathBuf,
    metrics: Metrics,
}

impl MMapStorage {
    /// Create a new memory mapped storage for the given path and info hash.
    pub fn new(path: impl AsRef<Path>, info_hash: InfoHash, data_pool: DataPool) -> Self {
        Self {
            info_hash,
            storage: DiskStorage::new(path.as_ref(), data_pool.clone()),
            part_file: OnceCell::new(),
            data_pool,
            path: path.as_ref().to_path_buf(),
            metrics: Default::default(),
        }
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self, buffer)))]
    pub async fn read(
        &self,
        buffer: &mut [u8],
        piece: &PieceIndex,
        offset: usize,
    ) -> Result<usize> {
        let mut cursor = 0;
        let buffer_len = buffer.len();
        let (mut file_index, mut torrent_offset) =
            self.readwrite(piece, offset, buffer_len).await?;

        while cursor < buffer_len {
            let file = self
                .data_pool
                .file(&file_index)
                .await
                .ok_or(unavailable())?;
            let bytes_remaining = buffer_len.saturating_sub(cursor);

            // check if the file is a padding file
            // if so, we skip the bytes as they all yield zero
            if file.attributes().contains(FileAttributeFlags::PaddingFile) {
                cursor += file.len();
                torrent_offset += file.len();
                file_index += 1;
                continue;
            }

            // check if we need to read from the parts file
            if file.priority == FilePriority::None {
                let parts_len = min(bytes_remaining, file.len());
                let mut parts_buffer = vec![0u8; parts_len];

                let bytes_read = self
                    .part_file()
                    .await?
                    .read(
                        &mut parts_buffer,
                        &file.pieces.start,
                        torrent_offset.saturating_sub(file.torrent_offset),
                    )
                    .await?;
                buffer[cursor..cursor + bytes_read].copy_from_slice(&parts_buffer[..bytes_read]);
                cursor += bytes_read;
                torrent_offset += bytes_read;
                file_index += 1;
                self.metrics.bytes_read.inc_by(bytes_read as u64);
                continue;
            }

            // read the current file from the disk storage
            // start reading at the current torrent offset, not at the start of the file
            let file_offset = torrent_offset.saturating_sub(file.torrent_offset);
            let read_len = bytes_remaining.min(file.len().saturating_sub(file_offset));
            let cur_piece = self
                .data_pool
                .find_piece_at_offset(torrent_offset)
                .await
                .ok_or(unavailable())?;
            // read directly into the buffer
            let bytes_read = self
                .storage
                .read(
                    &mut buffer[cursor..cursor + read_len],
                    &cur_piece.index,
                    torrent_offset.saturating_sub(cur_piece.offset),
                )
                .await?;
            // check if we've reached EOF
            if bytes_read == 0 {
                break;
            }

            cursor += bytes_read;
            self.metrics.bytes_read.inc_by(bytes_read as u64);

            // check if all bytes from the file where available
            // if not, don't read the next file
            if bytes_read < read_len {
                break;
            }

            file_index += 1;
            torrent_offset += bytes_read;
        }

        Ok(cursor)
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self, data)))]
    pub async fn write(&self, data: &[u8], piece: &PieceIndex, offset: usize) -> Result<usize> {
        let mut cursor = 0;
        let data_len = data.len();
        let (mut file_index, mut torrent_offset) = self.readwrite(piece, offset, data_len).await?;

        while cursor < data_len {
            let file = self
                .data_pool
                .file(&file_index)
                .await
                .ok_or(unavailable())?;
            let bytes_remaining = data_len - cursor;

            // check if the file is a padding file
            // if so, we skip the bytes as they all yield zero
            if file.attributes().contains(FileAttributeFlags::PaddingFile) {
                cursor += file.len();
                torrent_offset += file.len();
                file_index += 1;
                continue;
            }

            // check if we need to write to the parts file
            if file.priority == FilePriority::None {
                let cur_piece = match self.data_pool.find_piece_at_offset(torrent_offset).await {
                    Some(piece) => piece,
                    None => return Err(unavailable()),
                };
                let piece_offset = torrent_offset.saturating_sub(cur_piece.offset);
                let file_start_offset = torrent_offset.saturating_sub(file.torrent_offset);
                let parts_len = min(
                    bytes_remaining,
                    file.len().saturating_sub(file_start_offset),
                );
                let bytes_written = self
                    .part_file()
                    .await?
                    .write(
                        &data[cursor..cursor + parts_len],
                        &cur_piece.index,
                        piece_offset,
                    )
                    .await?;

                cursor += bytes_written;
                torrent_offset += bytes_written;
                file_index += 1;
                self.metrics.bytes_written.inc_by(bytes_written as u64);
                continue;
            }

            // start writing at the current torrent offset, not at the start of the file
            let file_offset = torrent_offset.saturating_sub(file.torrent_offset);
            let write_len = bytes_remaining.min(file.len().saturating_sub(file_offset));
            let cur_piece = self
                .data_pool
                .find_piece_at_offset(torrent_offset)
                .await
                .ok_or(unavailable())?;
            let bytes_written = self
                .storage
                .write(
                    &data[cursor..cursor + write_len],
                    &cur_piece.index,
                    torrent_offset.saturating_sub(cur_piece.offset),
                )
                .await?;

            // verify if all bytes could be written to the storage
            if bytes_written < write_len {
                break;
            }

            cursor += bytes_written;
            torrent_offset += bytes_written;
            file_index += 1;
            self.metrics.bytes_written.inc_by(bytes_written as u64);
        }

        Ok(cursor)
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn hash_v1(&self, piece: &PieceIndex) -> Result<Sha1Hash> {
        let hash = self.hash::<Sha1>(piece).await?;
        Sha1Hash::try_from(hash).map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn hash_v2(&self, piece: &PieceIndex) -> Result<Sha256Hash> {
        let hash = self.hash::<Sha256>(piece).await?;
        Sha256Hash::try_from(hash).map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn move_storage(&self, new_path: &Path) -> Result<()> {
        self.storage.move_storage(new_path).await
    }

    /// Returns the metrics reference of the memory-mapped storage.
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Calculate the digest of the given piece.
    ///
    /// The piece data is read through [MMapStorage::read], which resolves each file of the piece
    /// from either the disk storage or the parts file based on the file priority.
    /// The digest itself is computed on the blocking thread pool.
    async fn hash<D: Digest + Send + 'static>(&self, piece: &PieceIndex) -> Result<Output<D>> {
        let len = self
            .data_pool
            .piece(piece)
            .await
            .map(|piece| piece.len())
            .ok_or(unavailable())?;
        let mut buffer = vec![0u8; len];

        let bytes_read = self.read(&mut buffer, piece, 0).await?;
        if bytes_read != len {
            return Err(unavailable());
        }

        tokio::task::spawn_blocking(move || D::digest(buffer.as_slice()))
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    /// Returns the offset in bytes of the piece within the torrent.
    async fn piece_start(&self, piece: &PieceIndex) -> Result<usize> {
        self.data_pool
            .piece(&piece)
            .await
            .map(|piece| piece.offset)
            .ok_or(Error::new(io::ErrorKind::Other, "missing piece data"))
    }

    /// Returns the parts file for the memory-mapped storage.
    async fn part_file(&self) -> Result<&PartFile> {
        self.part_file
            .get_or_try_init(|| async {
                let piece_len = self
                    .data_pool
                    .piece(&0)
                    .await
                    .map(|piece| piece.len())
                    .ok_or(Error::new(
                        io::ErrorKind::Other,
                        "unable to create parts file, metadata is unknown",
                    ))?;
                let filename = format!(
                    ".{}.parts",
                    hex::encode(self.info_hash.short_info_hash_bytes())
                );
                let path = self.path.join(filename);

                if path.exists() {
                    PartFile::open(path.as_path(), piece_len).await
                } else {
                    PartFile::new(path.as_path(), piece_len, 10).await
                }
            })
            .await
    }

    /// The read/write file index and torrent offset to start from for the given piece and offset.
    async fn readwrite(
        &self,
        piece: &PieceIndex,
        offset: usize,
        buffer_len: usize,
    ) -> Result<(FileIndex, usize)> {
        let torrent_piece_start = self
            .data_pool
            .piece(piece)
            .await
            .map(|piece| piece.offset.saturating_add(offset))
            .ok_or(unavailable())?;

        // check if the requested range is still within the torrent range
        let end = torrent_piece_start
            .checked_add(buffer_len)
            .ok_or(io::Error::new(
                io::ErrorKind::Other,
                "buffer length overflow",
            ))?;
        let torrent_len = self.torrent_len().await;
        if end > torrent_len {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "out-of-bounds"));
        }

        let file_index = self
            .data_pool
            .file_index_for(&piece)
            .await
            .ok_or(unavailable())?;

        Ok((file_index, torrent_piece_start))
    }

    /// Returns the amount of bytes within the torrent.
    async fn torrent_len(&self) -> usize {
        let last_piece_index = self.data_pool.num_of_pieces().await.saturating_sub(1);
        self.data_pool
            .piece(&last_piece_index)
            .await
            .map(|piece| piece.offset.saturating_add(piece.len()))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{copy_test_file, read_test_file_to_bytes};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_read() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        copy_test_file(
            temp_path,
            "piece-1_30.iso",
            Some("debian-12.4.0-amd64-DVD-1.iso"),
        );
        let expected_data = read_test_file_to_bytes("piece-1_30.iso");
        let (mut context, _) = torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().path(temp_path).build(),
            vec![],
            vec![],
            None
        );
        let info_hash = context.metadata().info_hash.clone();
        let storage = MMapStorage::new(temp_path, info_hash, context.data_pool().clone());

        // wait for the files of the torrent
        create_pieces_and_files!(&mut context);

        // retrieve the piece length from the data pool
        let piece_len = context.data_pool().piece(&0).await.unwrap().len();

        // try to read the piece 2 and 3
        let mut buffer = vec![0u8; piece_len * 2];
        storage.read(&mut buffer, &2, 0).await.unwrap();
        assert_eq!(
            &expected_data[piece_len * 2..piece_len * 4],
            buffer.as_slice()
        );

        // verify that the read metrics have been updated
        assert_eq!(buffer.len(), storage.metrics().bytes_read.get() as usize);

        // try to read piece 4 and 5 with an offset
        let offset = 16usize;
        let mut buffer = vec![0u8; (piece_len * 2) - offset];
        storage.read(&mut buffer, &4, offset).await.unwrap();
        assert_eq!(
            &expected_data[piece_len * 4 + offset..piece_len * 6],
            buffer.as_slice()
        );
    }

    #[tokio::test]
    async fn test_write() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let data = read_test_file_to_bytes("piece-1_30.iso");
        let (mut context, _) = torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().path(temp_path).build(),
            vec![],
            vec![],
            None
        );
        let info_hash = context.metadata().info_hash.clone();
        let storage = MMapStorage::new(temp_path, info_hash, context.data_pool().clone());

        // wait for the files of the torrent
        create_pieces_and_files!(&mut context);

        let piece_len = context.data_pool().piece(&0).await.unwrap().len();

        // write the pieces 2 and 3
        let piece_data = &data[piece_len * 2..piece_len * 4];
        let written = storage.write(piece_data, &2, 0).await.unwrap();
        assert_eq!(piece_data.len(), written);

        // verify that the write metrics have been updated
        assert_eq!(
            piece_data.len(),
            storage.metrics().bytes_written.get() as usize
        );

        let mut buffer = vec![0u8; piece_len * 2];
        let read = storage.read(&mut buffer, &2, 0).await.unwrap();
        assert_eq!(buffer.len(), read);
        assert_eq!(piece_data, buffer.as_slice());

        // write the pieces 4 and 5 with an offset
        let offset = 16usize;
        let piece_data = &data[piece_len * 4 + offset..piece_len * 6];
        let written = storage.write(piece_data, &4, offset).await.unwrap();
        assert_eq!(piece_data.len(), written);

        let mut buffer = vec![0u8; piece_data.len()];
        let read = storage.read(&mut buffer, &4, offset).await.unwrap();
        assert_eq!(buffer.len(), read);
        assert_eq!(piece_data, buffer.as_slice());

        // the previously written pieces should be untouched
        let mut buffer = vec![0u8; piece_len * 2];
        storage.read(&mut buffer, &2, 0).await.unwrap();
        assert_eq!(&data[piece_len * 2..piece_len * 4], buffer.as_slice());
    }

    #[tokio::test]
    async fn test_hash_v1() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let piece: PieceIndex = 0;
        let data = read_test_file_to_bytes("piece-1.iso");
        let (mut context, _) = torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().path(temp_path).build(),
            vec![],
            vec![],
            None
        );
        let info_hash = context.metadata().info_hash.clone();
        let storage = MMapStorage::new(temp_path, info_hash, context.data_pool().clone());

        // wait for the files of the torrent
        create_pieces_and_files!(&mut context);

        let written = storage
            .write(&data, &piece, 0)
            .await
            .expect("expected the data to have been written");
        assert_eq!(data.len(), written);

        let expected_hash = context
            .data_pool()
            .piece(&piece)
            .await
            .expect("expected the piece to have been found")
            .hash
            .hash_v1()
            .expect("expected the v1 hash to be present within the piece");
        let result = storage
            .hash_v1(&piece)
            .await
            .expect("expected the hash to have been calculated");

        assert_eq!(expected_hash, result);
    }
}
