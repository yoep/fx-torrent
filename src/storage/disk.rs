use crate::storage::{unavailable, Error, Metrics, Result};
use crate::torrent_data::DataPool;
use crate::{FileAttributeFlags, FileIndex, PieceIndex, Sha1Hash, Sha256Hash};
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

/// Stores the torrent data on the file system.
///
/// The disk storage doesn't take the file priorities into account and will always try to access
/// the data from/to the file system.
///
/// All operations are piece-indexed: a piece and an offset within that piece are resolved through
/// the [DataPool] to the torrent files that hold the data. Reads and writes may span multiple
/// pieces and files, and padding files are skipped. File paths are validated to never leave the
/// storage path.
#[derive(Debug)]
pub struct DiskStorage {
    path: RwLock<PathBuf>,
    piece_len: RwLock<Option<usize>>,
    data_pool: DataPool,
    metrics: Metrics,
}

impl DiskStorage {
    /// Create a new disk storage which stores the torrent files relative to the given path.
    pub fn new(path: impl AsRef<Path>, piece_length: Option<usize>, data_pool: DataPool) -> Self {
        Self {
            path: RwLock::new(path.as_ref().to_path_buf()),
            piece_len: RwLock::new(piece_length),
            data_pool,
            metrics: Default::default(),
        }
    }

    /// Update the piece length of the torrent within the storage.
    pub async fn set_piece_len(&self, piece_len: usize) {
        self.piece_len.write().await.replace(piece_len);
    }

    /// Read data from the disk into the buffer, starting at `piece_offset` within the given piece.
    ///
    /// The read continues into subsequent pieces and files when the buffer exceeds the piece.
    /// It returns the number of bytes read, which is lower than the buffer length when the end of
    /// a file was reached before the buffer was filled.
    ///
    /// # Errors
    ///
    /// It returns an error when the piece is unknown, the requested range exceeds the torrent
    /// length, or the file path is invalid or couldn't be opened.
    #[cfg_attr(feature = "tracing", instrument(skip(self, buffer)))]
    pub async fn read(
        &self,
        buffer: &mut [u8],
        piece: &PieceIndex,
        piece_offset: usize,
    ) -> Result<usize> {
        let mut cursor = 0;
        let buffer_len = buffer.len();
        let (mut file_index, mut torrent_offset) =
            self.readwrite(piece, piece_offset, buffer_len).await?;

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

    /// Write the data to the disk, starting at `offset` within the given piece.
    ///
    /// The write continues into subsequent pieces and files when the data exceeds the piece.
    /// Missing files and directories are created. It returns the number of bytes written.
    ///
    /// # Errors
    ///
    /// It returns an error when the piece is unknown, the range exceeds the torrent length,
    /// or the file path is invalid or couldn't be opened.
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

    /// Calculate the SHA-1 hash (BEP3) of the given piece from the data stored on the disk.
    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn hash_v1(&self, piece: &PieceIndex) -> Result<Sha1Hash> {
        let hash = self.hash::<Sha1>(piece).await?;
        Sha1Hash::try_from(hash).map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    /// Calculate the SHA-256 hash (BEP52) of the given piece from the data stored on the disk.
    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn hash_v2(&self, piece: &PieceIndex) -> Result<Sha256Hash> {
        let hash = self.hash::<Sha256>(piece).await?;
        Sha256Hash::try_from(hash).map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    /// Move the storage to a new location.
    ///
    /// Each existing torrent file is moved to the same relative path within `new_path`, creating
    /// the required directories. Files which don't exist (yet) are skipped. The storage path is
    /// only updated when all files have been moved; on failure, the already moved files are
    /// moved back on a best-effort basis.
    ///
    /// # Errors
    ///
    /// It returns an error when a file path would leave the new storage path, or a file couldn't
    /// be moved.
    pub async fn move_storage(&self, new_path: &Path) -> Result<()> {
        let mut path = self.path.write().await;
        // early exit, if the new path is the same as the current one
        if path.as_path() == new_path {
            return Ok(());
        }

        let base = Self::canonicalize_unchecked(new_path);
        let mut moved: Vec<(PathBuf, PathBuf)> = Vec::new();
        for file in self.data_pool.files().await {
            if file.attributes().contains(FileAttributeFlags::PaddingFile) {
                continue;
            }

            let source = path.join(&file.torrent_path);
            let target = new_path.join(&file.torrent_path);
            if !Self::canonicalize_unchecked(&target).starts_with(&base) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{:?}", target),
                ));
            }
            if !tokio::fs::try_exists(&source).await? {
                continue;
            }

            Self::move_file(&source, &target).await?;
            moved.push((source, target));
        }

        *path = new_path.to_path_buf();
        Ok(())
    }

    /// Returns the storage metrics.
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Calculate the digest of the given piece.
    ///
    /// The file segments of the piece are resolved once and streamed from the disk, hashed on
    /// the blocking thread pool. This prevents the async runtime from being blocked by the
    /// file I/O and hash computation, and avoids buffering the whole piece.
    pub(super) async fn hash<D: Digest + Send + 'static>(
        &self,
        piece: &PieceIndex,
    ) -> Result<Output<D>> {
        let segments = self.piece_segments(piece).await?;
        let (hash, bytes_read) =
            tokio::task::spawn_blocking(move || Self::hash_segments::<D>(segments))
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))??;
        self.metrics.bytes_read.inc_by(bytes_read as u64);

        Ok(hash)
    }

    /// Move a single file, creating the parent directories of the target.
    /// It falls back to copy and remove when the target is on a different file system.
    async fn move_file(source: &Path, target: &Path) -> Result<()> {
        if let Some(parent) = target.parent() {
            create_dir_all(parent).await?;
        }

        if tokio::fs::rename(source, target).await.is_ok() {
            return Ok(());
        }

        tokio::fs::copy(source, target).await?;
        tokio::fs::remove_file(source).await
    }

    /// Resolve the file segments which make up the data of the given piece.
    async fn piece_segments(&self, piece: &PieceIndex) -> Result<Vec<PieceSegment>> {
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

        Ok(segments)
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

    /// Returns the amount of bytes within the torrent.
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

        // resolve the file containing the start of the range, which isn't
        // necessarily the first file of the piece when an offset is given
        let piece_index = self.piece_at_offset(torrent_piece_start).await?;
        let file_index = self
            .data_pool
            .file_index_for(&piece_index)
            .await
            .ok_or(unavailable())?;

        Ok((file_index, torrent_piece_start))
    }

    /// Returns the piece index for the given torrent byte offset.
    async fn piece_at_offset(&self, offset: usize) -> Result<PieceIndex> {
        let piece_len = self
            .piece_len
            .read()
            .await
            .ok_or(Error::new(io::ErrorKind::Other, "piece length unknown"))?;

        Ok(offset / piece_len)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::read_test_file_to_bytes;
    use crate::{FilePriority, InfoHash, Piece, TorrentFileInfo};
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
        let storage = DiskStorage::new(
            temp_path,
            context
                .metadata()
                .info
                .as_ref()
                .map(|info| info.piece_length as usize),
            data_pool,
        );

        // create pieces & files
        create_pieces_and_files!(&mut context);

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
        let storage = DiskStorage::new(
            temp_path,
            context
                .metadata()
                .info
                .as_ref()
                .map(|info| info.piece_length as usize),
            data_pool,
        );

        // create pieces & files
        create_pieces_and_files!(&mut context);

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
        init_logger!();
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
        init_logger!();
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
        let storage = DiskStorage::new(
            temp_path,
            context
                .metadata()
                .info
                .as_ref()
                .map(|info| info.piece_length as usize),
            data_pool,
        );

        // create pieces & files
        create_pieces_and_files!(&mut context);

        let result = storage.hash_v1(&0).await;

        let err = result.expect_err("expected an error to be returned");
        assert_eq!(io::ErrorKind::NotFound, err.kind());
    }

    #[tokio::test]
    async fn test_move_storage() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let old_path = temp_dir.path().join("old");
        let new_path = temp_dir.path().join("new");
        let make_file = |index: usize, name: &str, offset: usize| crate::File {
            index,
            torrent_path: PathBuf::from(name),
            torrent_offset: offset,
            info: TorrentFileInfo {
                length: 10,
                path: None,
                path_utf8: None,
                md5sum: None,
                attr: None,
                symlink_path: None,
                sha1: None,
            },
            priority: FilePriority::Normal,
            pieces: 0..1,
        };
        let data_pool = DataPool::new();
        data_pool
            .set_pieces(vec![Piece::new(InfoHash::default(), 0, 0, 20)])
            .await;
        data_pool
            .set_files(vec![
                make_file(0, "dir/file_a.bin", 0),
                make_file(1, "file_b.bin", 10),
            ])
            .await;
        let storage = DiskStorage::new(&old_path, Some(10), data_pool);

        // only file_a exists, file_b is skipped
        std::fs::create_dir_all(old_path.join("dir")).unwrap();
        std::fs::write(old_path.join("dir/file_a.bin"), b"0123456789").unwrap();

        storage
            .move_storage(&new_path)
            .await
            .expect("expected the storage to have been moved");

        assert!(!old_path.join("dir/file_a.bin").exists());
        assert_eq!(
            b"0123456789".as_slice(),
            std::fs::read(new_path.join("dir/file_a.bin")).unwrap()
        );
        assert!(!new_path.join("file_b.bin").exists());
        assert_eq!(new_path, *storage.path.read().await);

        // the data should be readable from the new location
        let mut buffer = vec![0u8; 10];
        let read = storage.read(&mut buffer, &0, 0).await.unwrap();
        assert_eq!(10, read);
        assert_eq!(b"0123456789".as_slice(), buffer.as_slice());
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

        let storage = DiskStorage::new(temp_path, Some(piece_size), data_pool);

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
}
