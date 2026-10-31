use crate::storage::parts_file::PartsFile;
use crate::storage::{unavailable, Metrics, Result};
use crate::torrent_data::DataPool;
use crate::{
    FileAttributeFlags, FileIndex, FilePriority, InfoHash, PieceIndex, Sha1Hash, Sha256Hash,
};
use sha1::digest::Output;
use sha1::{Digest, Sha1};
use sha2::Sha256;
use std::cmp::min;
use std::io;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use tokio::fs::{create_dir_all, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::RwLock;
#[cfg(feature = "tracing")]
use tracing::instrument;

/// The buffer size used to stream piece data from the disk while hashing.
const HASH_BUFFER_SIZE: usize = 128 * 1024;

/// File system storage for the torrent piece data.
#[derive(Debug)]
pub struct DiskStorage {
    path: RwLock<PathBuf>,
    data_pool: DataPool,
    part_file: PartsFile,
    metrics: Metrics,
}

impl DiskStorage {
    pub fn new<P: AsRef<Path>>(info_hash: InfoHash, path: P, data_pool: DataPool) -> Self {
        let part_filename = format!(".{}.parts", hex::encode(info_hash.short_info_hash_bytes()));

        Self {
            path: RwLock::new(path.as_ref().to_path_buf()),
            part_file: PartsFile::new(part_filename, path, data_pool.clone()),
            data_pool,
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
                    .part_file
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

            // try to open the torrent file
            let mut fs_file = self.open(&file.torrent_path, false).await?;
            let start_offset = torrent_offset.saturating_sub(file.torrent_offset);
            fs_file.seek(SeekFrom::Start(start_offset as u64)).await?;

            let mut total_bytes_read = 0;
            while total_bytes_read < bytes_remaining {
                let bytes_read = fs_file
                    .read(&mut buffer[cursor + total_bytes_read..cursor + bytes_remaining])
                    .await?;

                // check if we've reached EOF
                if bytes_read == 0 {
                    break;
                }

                total_bytes_read += bytes_read;
                self.metrics.bytes_read.inc_by(bytes_read as u64);
            }

            cursor += total_bytes_read;

            // check if all bytes from the file where available
            // if not, don't read the next file
            if start_offset + total_bytes_read < file.len() {
                break;
            }

            file_index += 1;
            torrent_offset += total_bytes_read;
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
                    .part_file
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

            // try to open the torrent file
            let mut fs_file = self.open(&file.torrent_path, true).await?;
            let start_offset = torrent_offset.saturating_sub(file.torrent_offset);
            let file_len = min(bytes_remaining, file.len().saturating_sub(start_offset));
            fs_file.seek(SeekFrom::Start(start_offset as u64)).await?;
            fs_file.write_all(&data[cursor..cursor + file_len]).await?;
            fs_file.flush().await?;

            cursor += file_len;
            torrent_offset += file_len;
            file_index += 1;
            self.metrics.bytes_written.inc_by(file_len as u64);
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

    /// Move the storage to a new location.
    pub async fn move_storage(&self, _new_path: &Path) -> Result<()> {
        todo!()
    }

    /// Returns the storage metrics.
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Calculate the digest of the given piece.
    ///
    /// The piece data is streamed from the disk and hashed on the blocking thread pool,
    /// preventing the async runtime from being blocked by the file I/O and hash computation.
    async fn hash<D: Digest + Send + 'static>(&self, piece: &PieceIndex) -> Result<Output<D>> {
        let segments = match self.piece_segments(piece).await? {
            Some(segments) => segments,
            None => {
                // the piece is (partially) stored within the parts file
                let len = self.piece_len(piece).await;
                let mut buffer = vec![0u8; len];
                let bytes_read = self.read(&mut buffer, piece, 0).await?;
                if bytes_read != len {
                    return Err(unavailable());
                }

                return tokio::task::spawn_blocking(move || D::digest(buffer.as_slice()))
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()));
            }
        };

        let (hash, bytes_read) =
            tokio::task::spawn_blocking(move || Self::hash_segments::<D>(segments))
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))??;
        self.metrics.bytes_read.inc_by(bytes_read as u64);

        Ok(hash)
    }

    /// Resolve the file segments which make up the data of the given piece.
    /// It returns `None` when the piece data is (partially) stored within the parts file.
    async fn piece_segments(&self, piece: &PieceIndex) -> Result<Option<Vec<PieceSegment>>> {
        let len = self.piece_len(piece).await;
        let (mut file_index, mut torrent_offset) = self.readwrite(piece, 0, len).await?;
        let mut segments = Vec::new();
        let mut remaining = len;

        while remaining > 0 {
            let file = self
                .data_pool
                .file(&file_index)
                .await
                .ok_or(unavailable())?;
            let start_offset = torrent_offset.saturating_sub(file.torrent_offset);
            let segment_len = min(remaining, file.len().saturating_sub(start_offset));

            if segment_len > 0 {
                if file.attributes().contains(FileAttributeFlags::PaddingFile) {
                    segments.push(PieceSegment::Padding(segment_len));
                } else if file.priority == FilePriority::None {
                    return Ok(None);
                } else {
                    let path = self.absolute_filepath(&file.torrent_path).await;
                    self.assert_valid_filepath(&path).await?;
                    segments.push(PieceSegment::File {
                        path,
                        offset: start_offset as u64,
                        len: segment_len,
                    });
                }
            }

            remaining -= segment_len;
            torrent_offset += segment_len;
            file_index += 1;
        }

        Ok(Some(segments))
    }

    /// Calculate the digest of the given piece segments.
    /// This is a blocking operation and should be executed on the blocking thread pool.
    ///
    /// It returns the digest and the number of bytes read from the disk.
    fn hash_segments<D: Digest>(segments: Vec<PieceSegment>) -> Result<(Output<D>, usize)> {
        let mut hasher = D::new();
        let mut buffer = vec![0u8; HASH_BUFFER_SIZE];
        let mut total_bytes_read = 0;

        for segment in segments {
            match segment {
                PieceSegment::Padding(len) => {
                    buffer.fill(0);
                    let mut remaining = len;
                    while remaining > 0 {
                        let chunk_len = min(remaining, buffer.len());
                        hasher.update(&buffer[..chunk_len]);
                        remaining -= chunk_len;
                    }
                }
                PieceSegment::File { path, offset, len } => {
                    let mut file = std::fs::File::open(path)?;
                    file.seek(SeekFrom::Start(offset))?;

                    let mut remaining = len;
                    while remaining > 0 {
                        let chunk_len = min(remaining, buffer.len());
                        file.read_exact(&mut buffer[..chunk_len])
                            .map_err(|e| match e.kind() {
                                io::ErrorKind::UnexpectedEof => unavailable(),
                                _ => e,
                            })?;
                        hasher.update(&buffer[..chunk_len]);
                        remaining -= chunk_len;
                        total_bytes_read += chunk_len;
                    }
                }
            }
        }

        Ok((hasher.finalize(), total_bytes_read))
    }

    /// Get the absolute filepath for the given filepath within the storage.
    async fn absolute_filepath<P: AsRef<Path>>(&self, filepath: P) -> PathBuf {
        self.path.read().await.join(filepath.as_ref())
    }

    /// Get the amount of bytes for the given piece.
    async fn piece_len(&self, piece: &PieceIndex) -> usize {
        self.data_pool
            .piece(piece)
            .await
            .map(|e| e.len())
            .unwrap_or_default()
    }

    /// Get the amount of bytes within the torrent.
    async fn torrent_len(&self) -> usize {
        let last_piece_index = self.data_pool.num_of_pieces().await.saturating_sub(1);
        self.data_pool
            .piece(&last_piece_index)
            .await
            .map(|piece| piece.offset.saturating_add(piece.len()))
            .unwrap_or_default()
    }

    /// Check if the given filepath is valid within the storage.
    /// If the target filepath leaves the storage path, it returns false.
    async fn is_valid_filepath<P: AsRef<Path>>(&self, filepath: P) -> bool {
        let base = Self::canonicalize_unchecked(&*self.path.read().await);
        let target = Self::canonicalize_unchecked(filepath.as_ref());

        target.starts_with(&base)
    }

    /// Assert if the given filepath is valid within the storage.
    /// This prevents file paths from traversing upwards/leaving the storage path.
    async fn assert_valid_filepath<P: AsRef<Path>>(&self, filepath: P) -> Result<()> {
        if !self.is_valid_filepath(&filepath).await {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{:?}", filepath.as_ref()),
            ));
        }

        Ok(())
    }

    /// Try to open the given torrent file from the disk storage.
    async fn open<P: AsRef<Path>>(&self, filepath: P, write: bool) -> Result<File> {
        let absolute_path = self.absolute_filepath(filepath).await;
        self.assert_valid_filepath(&absolute_path).await?;

        if write {
            let base_dir = self.path.read().await.clone();
            let parent = absolute_path.parent().unwrap_or(base_dir.as_path());
            create_dir_all(parent).await?;
        }

        OpenOptions::new()
            .read(true)
            .write(write)
            .create(write)
            .open(absolute_path)
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

    /// Get the canonicalized path for the given path.
    /// Resolves "." and ".." without touching the filesystem (symlinks not resolved).
    fn canonicalize_unchecked(path: &Path) -> PathBuf {
        let components = path.components();
        let mut result = PathBuf::new();

        // Traverse the path components and resolve ".." and "." appropriately
        for component in components {
            match component {
                // Ignore "." (current directory)
                std::path::Component::CurDir => {}
                // Remove the last component for ".." (parent directory)
                std::path::Component::ParentDir => {
                    result.pop();
                }
                // Add other components as normal
                std::path::Component::Normal(part) => {
                    result.push(part);
                }
                std::path::Component::RootDir | std::path::Component::Prefix(_) => {
                    result.push(component.as_os_str());
                }
            }
        }

        result
    }
}

/// A contiguous segment of piece data.
#[derive(Debug)]
enum PieceSegment {
    /// A segment within a torrent file on the disk.
    File {
        path: PathBuf,
        offset: u64,
        len: usize,
    },
    /// A segment of zero bytes within a padding file.
    Padding(usize),
}

impl Drop for DiskStorage {
    fn drop(&mut self) {
        // TODO: cleanup parts file
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::{CreatePiecesAndFilesOperation, TorrentOperationResult};
    use crate::tests::read_test_file_to_bytes;
    use crate::torrent::TorrentContext;
    use crate::{Piece, TorrentFileInfo};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_read() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let data = read_test_file_to_bytes("piece-1_30.iso");
        let (mut context, _) = torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().path(temp_path).build(),
            vec![]
        );
        let data_pool = context.data_pool().clone();
        let storage = DiskStorage::new(context.metadata().info_hash.clone(), temp_path, data_pool);

        // create pieces & files
        create_pieces_and_files(&mut context).await;

        // write the piece data
        {
            let result = storage.write(&data, &0, 0).await;
            match result {
                Ok(result) => assert_eq!(data.len(), result, "expected all data to be written"),
                Err(_) => assert!(false, "expected Ok, but got {:?}", result),
            }
        }

        // read the starting piece with offset
        {
            let piece: PieceIndex = 0;
            let offset = 32;
            let piece_len = context.data_pool().piece(&piece).await.unwrap().length;
            let mut buffer = vec![0u8; piece_len];
            let result = storage.read(&mut buffer, &piece, offset).await;
            match result {
                Ok(result) => assert_eq!(piece_len, result, "expected the buffer to be filled"),
                Err(_) => assert!(false, "expected Ok, but got {:?}", result),
            }
            assert_eq!(
                &data[offset..offset + piece_len],
                &buffer[..],
                "expected the buffer to match the original data"
            );
        }

        // read non-starting piece without offset
        {
            let piece: PieceIndex = 7;
            let piece_len = context.data_pool().piece(&piece).await.unwrap().length;
            let mut buffer = vec![0u8; piece_len];
            let result = storage.read(&mut buffer, &piece, 0).await;
            match result {
                Ok(result) => assert_eq!(piece_len, result, "expected the full piece to be read"),
                Err(_) => assert!(false, "expected Ok, but got {:?}", result),
            }
        }
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
            vec![]
        );
        let data_pool = context.data_pool().clone();
        let storage = DiskStorage::new(context.metadata().info_hash.clone(), temp_path, data_pool);

        // create pieces & files
        create_pieces_and_files(&mut context).await;

        // write the piece data
        {
            let result = storage.write(&data, &piece, 0).await;
            match result {
                Ok(result) => assert_eq!(data.len(), result, "expected all data to be written"),
                Err(_) => assert!(false, "expected Ok, but got {:?}", result),
            }
        }

        // get the hash result from the piece
        {
            let piece_hash = context
                .data_pool()
                .piece(&piece)
                .await
                .expect("expected the piece to have been found")
                .hash;
            let expected_hash = piece_hash
                .hash_v1()
                .expect("expected the v1 hash to be present within the piece");

            let result = storage
                .hash_v1(&piece)
                .await
                .expect("expected the hash to have been calculated");
            assert_eq!(
                expected_hash, result,
                "expected the hash to equal the piece hash"
            );
        }
    }

    /// Test that a piece spanning two files is correctly written to both files.
    ///
    /// Layout:
    ///   Piece 0: bytes   0-63  (entirely in file_a)
    ///   Piece 1: bytes  64-127 (36 bytes in file_a, 28 bytes in file_b) ← cross-file
    ///   Piece 2: bytes 128-191 (entirely in file_b)
    ///
    ///   file_a: torrent bytes   0-99  (length 100)
    ///   file_b: torrent bytes 100-191 (length 92)
    #[test]
    fn test_hash_segments() {
        let temp_dir = tempdir().unwrap();
        let file_a = temp_dir.path().join("a.bin");
        let file_b = temp_dir.path().join("b.bin");
        std::fs::write(&file_a, b"loremipsum").unwrap();
        std::fs::write(&file_b, b"dolorsitamet").unwrap();
        let segments = vec![
            PieceSegment::File {
                path: file_a,
                offset: 5,
                len: 5,
            },
            PieceSegment::Padding(4),
            PieceSegment::File {
                path: file_b,
                offset: 0,
                len: 5,
            },
        ];
        let expected_hash = Sha1::digest(b"ipsum\0\0\0\0dolor");

        let (result, bytes_read) = DiskStorage::hash_segments::<Sha1>(segments)
            .expect("expected the segments to have been hashed");

        assert_eq!(expected_hash, result);
        assert_eq!(10, bytes_read, "expected the padding bytes to not be read");
    }

    #[test]
    fn test_hash_segments_incomplete_file() {
        let temp_dir = tempdir().unwrap();
        let filepath = temp_dir.path().join("incomplete.bin");
        std::fs::write(&filepath, b"lorem").unwrap();
        let segments = vec![PieceSegment::File {
            path: filepath,
            offset: 0,
            len: 10,
        }];

        let result = DiskStorage::hash_segments::<Sha1>(segments);

        let err = result.expect_err("expected an error to be returned");
        assert_eq!(io::ErrorKind::NotFound, err.kind());
    }

    #[tokio::test]
    async fn test_hash_v1_missing_file() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (mut context, _) = torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().path(temp_path).build(),
            vec![]
        );
        let data_pool = context.data_pool().clone();
        let storage = DiskStorage::new(context.metadata().info_hash.clone(), temp_path, data_pool);

        // create pieces & files
        create_pieces_and_files(&mut context).await;

        let result = storage.hash_v1(&0).await;

        let err = result.expect_err("expected an error to be returned");
        assert_eq!(io::ErrorKind::NotFound, err.kind());
    }

    #[tokio::test]
    async fn test_write_piece_spanning_two_files() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path();
        let piece_size: usize = 64;
        let file_a_len: usize = 100;
        let file_b_len: usize = 92;

        // Build pieces
        let pieces = vec![
            Piece::new(InfoHash::default(), 0, 0, piece_size),
            Piece::new(InfoHash::default(), 1, piece_size, piece_size),
            Piece::new(InfoHash::default(), 2, piece_size * 2, piece_size),
        ];

        // Build files: file_a covers torrent bytes 0-99, file_b covers 100-191
        let make_file_info = |length: usize| TorrentFileInfo {
            length: length as u64,
            path: None,
            path_utf8: None,
            md5sum: None,
            attr: None,
            symlink_path: None,
            sha1: None,
        };
        let files = vec![
            crate::File {
                index: 0,
                torrent_path: PathBuf::from("file_a.bin"),
                torrent_offset: 0,
                info: make_file_info(file_a_len),
                priority: FilePriority::Normal,
                pieces: 0..2,
            },
            crate::File {
                index: 1,
                torrent_path: PathBuf::from("file_b.bin"),
                torrent_offset: file_a_len,
                info: make_file_info(file_b_len),
                priority: FilePriority::Normal,
                pieces: 1..3,
            },
        ];

        let data_pool = DataPool::new();
        data_pool.set_pieces(pieces).await;
        data_pool.set_files(files).await;

        let storage = DiskStorage::new(InfoHash::default(), temp_path, data_pool);

        // Generate deterministic data for piece 1
        let data: Vec<u8> = (0u8..piece_size as u8).collect();

        let result = storage.write(&data, &1, 0).await;
        assert!(
            result.is_ok(),
            "expected write to succeed, got {:?}",
            result
        );
        assert_eq!(
            piece_size,
            result.unwrap(),
            "expected all bytes to be written"
        );

        // Verify file_a.bin received bytes data[0..36] at offset 64
        let bytes_in_a = file_a_len - piece_size; // 36
        let file_a_path = temp_path.join("file_a.bin");
        let file_a_content = tokio::fs::read(&file_a_path)
            .await
            .expect("expected file_a.bin to exist");
        assert_eq!(
            &data[..bytes_in_a],
            &file_a_content[piece_size..],
            "expected file_a.bin tail to match piece data head"
        );

        // Verify file_b.bin received bytes data[36..64] at offset 0
        let bytes_in_b = piece_size - bytes_in_a; // 28
        let file_b_path = temp_path.join("file_b.bin");
        let file_b_content = tokio::fs::read(&file_b_path)
            .await
            .expect("expected file_b.bin to exist");
        assert_eq!(
            &data[bytes_in_a..piece_size],
            &file_b_content[..bytes_in_b],
            "expected file_b.bin head to match piece data tail"
        );
    }

    async fn create_pieces_and_files(context: &mut TorrentContext) {
        let mut operation = CreatePiecesAndFilesOperation::new();
        let result = operation.execute(context).await;
        assert_eq!(TorrentOperationResult::Continue, result);
    }
}
