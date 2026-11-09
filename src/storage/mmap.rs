use crate::storage::{unavailable, DiskStorage, Error, Metrics, PartFile, Result};
use crate::torrent_data::DataPool;
use crate::{
    FileAttributeFlags, FileIndex, FilePriority, InfoHash, PieceIndex, Sha1Hash, Sha256Hash,
};
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
            let read_len = bytes_remaining.min(file.len());
            let piece = file.pieces.start;
            let piece_start = self.piece_start(&piece).await?;
            // ready directly into the buffer
            let bytes_read = self
                .storage
                .read(
                    &mut buffer[cursor..cursor + read_len],
                    &file.pieces.start,
                    file.torrent_offset.saturating_sub(piece_start),
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

            let write_len = bytes_remaining.min(file.len());
            let piece = file.pieces.start;
            let piece_start = self.piece_start(&piece).await?;
            let bytes_written = self
                .storage
                .write(
                    &data[cursor..cursor + write_len],
                    &piece,
                    file.torrent_offset.saturating_sub(piece_start),
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
        todo!()
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn hash_v2(&self, piece: &PieceIndex) -> Result<Sha256Hash> {
        todo!()
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn move_storage(&self, new_path: &Path) -> Result<()> {
        todo!()
    }

    /// Returns the metrics reference of the memory-mapped storage.
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
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
