use crate::channel::{ChannelSender, Reply};
use crate::storage::Storage;
use crate::torrent_data::DataPool;
use crate::{File, PiecePriority, Result, TorrentCommand, TorrentError, TorrentEvent};
use futures::future::BoxFuture;
use futures::task::AtomicWaker;
use futures::{ready, FutureExt, Stream};
use fx_callback::Subscription;
use log::trace;
use std::cmp::min;
use std::fmt::Debug;
use std::io;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::select;
use tokio_util::sync::CancellationToken;

/// The internal stream buffer type.
type Buffer = Range<usize>;

/// Streams the contents of a single torrent file.
///
/// # Example Usage
///
/// ```rust
/// # use futures::StreamExt;
/// # use fx_torrent::Torrent;
///
/// # async fn example() {
///     let torrent = Torrent::request()
///         .build()
///         .unwrap();
///     let file = torrent.file(&0).await.unwrap();
///
///     let mut stream = torrent.stream(&file).await.unwrap();
///     while let Some(bytes) = stream.next().await {
///         // use the bytes here
///     }
/// # }
/// ```
#[derive(Debug)]
pub struct FileStream {
    file: File,
    storage: Storage,
    data_pool: DataPool,
    command_sender: ChannelSender<TorrentCommand>,
    cursor: usize,
    stream_buffer_len: usize,
    piece_len: usize,
    waker: Arc<AtomicWaker>,
    state: StreamState,
    cancellation_token: CancellationToken,
}

impl FileStream {
    pub(crate) fn new(
        file: File,
        piece_len: usize,
        stream_buffer_len: usize,
        storage: Storage,
        data_pool: DataPool,
        command_sender: ChannelSender<TorrentCommand>,
        receiver: Subscription<TorrentEvent>,
    ) -> Self {
        let waker = Arc::new(AtomicWaker::new());
        let cancellation_token = CancellationToken::new();

        let event_waker = waker.clone();
        let event_cancellation_token = cancellation_token.clone();
        tokio::spawn(async move {
            Self::run_event_loop(receiver, event_waker, event_cancellation_token).await;
        });

        Self {
            file,
            storage,
            data_pool,
            command_sender,
            cursor: 0,
            stream_buffer_len,
            piece_len,
            waker,
            state: StreamState::Idle,
            cancellation_token,
        }
    }

    /// Returns the total number of bytes in the stream.
    pub fn len(&self) -> usize {
        self.file.len()
    }

    /// Reset the stream progress back to the start.
    pub fn reset(&mut self) {
        self.cursor = 0;
        self.state = StreamState::Idle;
    }

    /// Seek the given offset within the stream.
    pub fn seek(&mut self, offset: usize) -> Result<()> {
        if offset > self.file.len() {
            return Err(TorrentError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset exceeds file length",
            )));
        }

        self.cursor = offset;
        self.state = StreamState::Idle;
        Ok(())
    }

    /// Returns the current byte range of the stream.
    pub fn range(&self) -> Range<usize> {
        self.cursor..self.file.len()
    }

    /// Returns the [Content-Range](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Range)
    /// information in bytes for the stream.
    pub fn content_range(&self) -> String {
        format!(
            "bytes {}-{}/{}",
            self.cursor,
            self.file.len() - 1,
            self.file.len()
        )
    }

    fn next_buffer(&self) -> Buffer {
        let buffer_end_byte = min(self.cursor + self.stream_buffer_len, self.file.len());
        self.file.torrent_offset + self.cursor..self.file.torrent_offset + buffer_end_byte
    }

    fn prioritize_buffer(&self, bytes: Buffer) {
        let command_sender = self.command_sender.clone();
        let waker = self.waker.clone();

        // execute the prioritization on a separate task
        tokio::spawn(async move {
            command_sender
                .fire_and_forget(TorrentCommand::PrioritizeBytes {
                    bytes,
                    priority: PiecePriority::Now,
                    response: Reply::empty(),
                })
                .await;

            waker.wake();
        });
    }

    async fn run_event_loop(
        mut receiver: Subscription<TorrentEvent>,
        waker: Arc<AtomicWaker>,
        cancellation_token: CancellationToken,
    ) {
        loop {
            select! {
                _ = cancellation_token.cancelled() => break,
                event = receiver.recv() => {
                    match event {
                        Err(_) => break,
                        Ok(event) => {
                            match &*event {
                                TorrentEvent::PieceCompleted(_) | TorrentEvent::StateChanged(_) => {
                                    waker.wake()
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
    }
}

impl Stream for FileStream {
    type Item = Result<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // early exit if the end of the file has been reached
        if self.cursor >= self.file.len() {
            return Poll::Ready(None);
        }

        self.waker.register(cx.waker());
        loop {
            match &mut self.state {
                StreamState::Idle => {
                    let buffer = self.next_buffer();
                    let data_pool = self.data_pool.clone();

                    let future = {
                        let buffer = buffer.clone();
                        async move { data_pool.has_bytes(buffer).await }
                    }
                    .boxed();
                    trace!("Streaming buffer {:?}", buffer);
                    self.state = StreamState::Checking {
                        buffer,
                        future,
                        is_prioritized: false,
                    };
                }
                StreamState::Checking {
                    buffer,
                    future,
                    is_prioritized,
                } => {
                    let buffer = buffer.clone();
                    let is_available = ready!(future.as_mut().poll(cx));

                    if !is_available {
                        if !*is_prioritized {
                            let buffer = buffer.clone();
                            self.prioritize_buffer(buffer);
                        }

                        self.state = StreamState::Waiting { buffer };
                        return Poll::Pending;
                    }

                    let storage = self.storage.clone();
                    let starting_piece = self.cursor / self.piece_len;
                    let starting_offset = self.cursor % self.piece_len;
                    let stream_buffer_len = self.stream_buffer_len;
                    let future = async move {
                        let mut buffer = vec![0u8; stream_buffer_len];

                        match storage
                            .read(&mut buffer, &starting_piece, starting_offset)
                            .await
                        {
                            Err(e) => Err(TorrentError::Io(e)),
                            Ok(bytes_read) => {
                                if bytes_read != buffer.len() {
                                    Err(TorrentError::Io(io::Error::new(
                                        io::ErrorKind::UnexpectedEof,
                                        format!(
                                            "wanted {} bytes, but got {:?} instead",
                                            stream_buffer_len, bytes_read
                                        ),
                                    )))
                                } else {
                                    Ok(buffer)
                                }
                            }
                        }
                    }
                    .boxed();
                    self.state = StreamState::Reading { future };
                }
                StreamState::Waiting { buffer } => {
                    let buffer = buffer.clone();
                    let data_pool = self.data_pool.clone();

                    let future = {
                        let buffer = buffer.clone();
                        async move { data_pool.has_bytes(buffer).await }.boxed()
                    };
                    self.state = StreamState::Checking {
                        buffer,
                        future,
                        is_prioritized: true,
                    };
                }
                StreamState::Reading { future } => {
                    let result = ready!(future.as_mut().poll(cx));
                    if let Ok(bytes) = &result {
                        self.cursor += bytes.len();
                    }

                    self.state = StreamState::Idle;
                    return Poll::Ready(Some(result));
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.stream_buffer_len == 0 || self.len() == 0 {
            return (0, Some(0));
        }

        let upper_bound = self.len().div_ceil(self.stream_buffer_len);
        (0, Some(upper_bound))
    }
}

impl Drop for FileStream {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
    }
}

enum StreamState {
    Idle,
    Checking {
        buffer: Buffer,
        future: BoxFuture<'static, bool>,
        is_prioritized: bool,
    },
    Waiting {
        buffer: Buffer,
    },
    Reading {
        future: BoxFuture<'static, Result<Vec<u8>>>,
    },
}

impl Debug for StreamState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamState::Idle => f.write_str("StreamState::Idle"),
            StreamState::Checking { .. } => f.write_str("StreamState::Checking"),
            StreamState::Waiting { .. } => f.write_str("StreamState::Waiting"),
            StreamState::Reading { .. } => f.write_str("StreamState::Reading"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::operation::CreatePiecesAndFilesOperation;
    use crate::storage::DiskStorage;
    use crate::tests::copy_test_file;
    use crate::tests::helpers::wait_for_torrent_pieces;
    use futures::{Stream, StreamExt};
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_stream_next() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        copy_test_file(
            temp_path,
            "piece-1_30.iso",
            Some("debian-12.4.0-amd64-DVD-1.iso"),
        );
        let torrent = torrent!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![CreatePiecesAndFilesOperation::new().into(),],
            vec![],
            |params| DiskStorage::new(params.info_hash, params.path, params.data_pool).into(),
            None
        );
        let data_pool = torrent.inner.data_pool().await.unwrap();
        let (peer, _peer2) = tcp_peer_pair!(&torrent, vec![]);
        torrent.inner.peer_connected(peer.clone().into()).await;

        // wait for the pieces to be created
        wait_for_torrent_pieces(&torrent).await;

        // create the stream
        let file = torrent.file(&0).await.unwrap();
        let mut stream = torrent.stream(&file).await.unwrap();

        // set piece 0 as completed
        mark_piece_completed!(&stream.command_sender, 0, peer.addr(), "piece-1_30.iso");
        assert_timeout!(
            Duration::from_secs(2),  // TODO: improve test performance
            data_pool.is_piece_completed(&0).await
        );

        // get the next buffer, which should complete instantly
        let result =
            timeout!(Duration::from_millis(500), stream.next()).expect("expected buffer data");
        match result {
            Ok(_) => {
                assert_eq!(
                    stream.stream_buffer_len, stream.cursor,
                    "expected the stream cursor to be updated"
                );
            }
            _ => assert!(false, "expected Ok(), but got {:?}", result),
        }

        // try to get the next buffer, which should be blocked
        let command_sender = stream.command_sender.clone();
        let mut future = stream.next();
        let result = timeout(Duration::from_millis(250), &mut future).await;
        match result {
            Err(_) => {}
            _ => assert!(false, "expected Err(Elapsed), but got {:?}", result),
        }

        // complete the piece
        mark_piece_completed!(&command_sender, 1, peer.addr(), "piece-1_30.iso");
        assert_timeout!(
            Duration::from_secs(2),  // TODO: improve test performance
            data_pool.is_piece_completed(&1).await
        );

        // try to complete the previous future instantly
        let result = timeout!(Duration::from_millis(500), future).unwrap();
        match result {
            Ok(_) => {}
            _ => assert!(false, "expected Ok(), but got {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_len() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        copy_test_file(
            temp_path,
            "piece-1_30.iso",
            Some("debian-12.4.0-amd64-DVD-1.iso"),
        );
        let torrent = torrent!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![CreatePiecesAndFilesOperation::new().into(),],
            vec![],
            |params| DiskStorage::new(params.info_hash, params.path, params.data_pool).into(),
            None
        );

        // wait for the pieces to be created
        wait_for_torrent_pieces(&torrent).await;

        // create the stream
        let file = torrent.file(&0).await.unwrap();
        let stream = torrent.stream(&file).await.unwrap();

        let result = stream.len();
        assert_eq!(file.len(), result, "expected the file length to match");
    }

    #[tokio::test]
    async fn test_reset() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        copy_test_file(
            temp_path,
            "piece-1_30.iso",
            Some("debian-12.4.0-amd64-DVD-1.iso"),
        );
        let torrent = torrent!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![CreatePiecesAndFilesOperation::new().into(),],
            vec![],
            |params| DiskStorage::new(params.info_hash, params.path, params.data_pool).into(),
            None
        );
        let data_pool = torrent.inner.data_pool().await.unwrap();
        let (peer, _peer2) = tcp_peer_pair!(&torrent, vec![]);
        torrent.inner.peer_connected(peer.clone().into()).await;

        // wait for the pieces to be created
        wait_for_torrent_pieces(&torrent).await;

        // create the stream
        let file = torrent.file(&0).await.unwrap();
        let mut stream = torrent.stream(&file).await.unwrap();

        // set the next piece as completed
        mark_piece_completed!(&stream.command_sender, 0, peer.addr(), "piece-1_30.iso");
        assert_timeout!(
            Duration::from_secs(2), // TODO: improve test performance
            data_pool.is_piece_completed(&0).await
        );

        // read the next stream buffer
        let result = timeout!(Duration::from_millis(750), stream.next());
        match result {
            Some(Ok(_)) => {
                assert_eq!(
                    stream.stream_buffer_len, stream.cursor,
                    "expected the stream cursor to be updated"
                );
            }
            _ => assert!(false, "expected Some(Ok()), but got {:?}", result),
        }

        // reset the stream back to the start
        stream.reset();
        assert_eq!(0, stream.cursor, "expected the cursor to have been reset");
    }

    #[tokio::test]
    async fn test_seek() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        copy_test_file(
            temp_path,
            "piece-1_30.iso",
            Some("debian-12.4.0-amd64-DVD-1.iso"),
        );
        let torrent = torrent!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![CreatePiecesAndFilesOperation::new().into(),],
            vec![],
            |params| DiskStorage::new(params.info_hash, params.path, params.data_pool).into(),
            None
        );
        let data_pool = torrent.inner.data_pool().await.unwrap();
        let (peer, _peer2) = tcp_peer_pair!(&torrent, vec![]);
        torrent.inner.peer_connected(peer.clone().into()).await;

        // wait for the pieces to be created
        wait_for_torrent_pieces(&torrent).await;

        // create the stream
        let file = torrent.file(&0).await.unwrap();
        let mut stream = torrent.stream(&file).await.unwrap();

        // set the next piece as completed
        mark_piece_completed!(&stream.command_sender, 0, peer.addr(), "piece-1_30.iso");
        assert_timeout!(
            Duration::from_secs(2), // TODO: improve test performance
            data_pool.is_piece_completed(&0).await
        );

        // read the next stream buffer
        let result = timeout!(Duration::from_millis(750), stream.next());
        match result {
            Some(Ok(_)) => {
                assert_eq!(
                    stream.stream_buffer_len, stream.cursor,
                    "expected the stream cursor to be updated"
                );
            }
            _ => assert!(false, "expected Some(Ok()), but got {:?}", result),
        }

        // seek an offset within the stream
        stream.seek(stream.stream_buffer_len * 3).unwrap();
        assert_eq!(
            stream.stream_buffer_len * 3,
            stream.cursor,
            "expected the cursor to have been reset"
        );
    }

    #[tokio::test]
    async fn test_range() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        copy_test_file(
            temp_path,
            "piece-1_30.iso",
            Some("debian-12.4.0-amd64-DVD-1.iso"),
        );
        let torrent = torrent!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![CreatePiecesAndFilesOperation::new().into(),],
            vec![],
            |params| DiskStorage::new(params.info_hash, params.path, params.data_pool).into(),
            None
        );

        // wait for the pieces to be created
        wait_for_torrent_pieces(&torrent).await;

        // create the stream
        let file = torrent.file(&0).await.unwrap();
        let mut stream = torrent.stream(&file).await.unwrap();

        // seek an offset within the stream
        let offset = stream.stream_buffer_len * 5;
        stream.seek(offset).unwrap();
        let result = stream.range();
        assert_eq!(
            offset..file.len(),
            result,
            "expected the stream range to match"
        );

        // reset the stream
        stream.reset();
        let result = stream.range();
        assert_eq!(0..file.len(), result, "expected the stream range to match");
    }

    #[tokio::test]
    async fn test_content_range() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        copy_test_file(
            temp_path,
            "piece-1_30.iso",
            Some("debian-12.4.0-amd64-DVD-1.iso"),
        );
        let torrent = torrent!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![CreatePiecesAndFilesOperation::new().into(),],
            vec![],
            |params| DiskStorage::new(params.info_hash, params.path, params.data_pool).into(),
            None
        );

        // wait for the pieces to be created
        wait_for_torrent_pieces(&torrent).await;

        // create the stream
        let file = torrent.file(&0).await.unwrap();
        let mut stream = torrent.stream(&file).await.unwrap();

        // seek an offset within the stream
        let offset = stream.stream_buffer_len * 9;
        stream.seek(offset).unwrap();
        let result = stream.content_range();
        assert_eq!(
            format!("bytes {}-{}/{}", offset, file.len() - 1, file.len()),
            result,
            "expected the stream range to match"
        );

        // reset the stream
        stream.reset();
        let result = stream.content_range();
        assert_eq!(
            format!("bytes 0-{}/{}", file.len() - 1, file.len()),
            result,
            "expected the stream range to match"
        );
    }

    #[tokio::test]
    async fn test_size_hint() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        copy_test_file(
            temp_path,
            "piece-1_30.iso",
            Some("debian-12.4.0-amd64-DVD-1.iso"),
        );
        let torrent = torrent!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![CreatePiecesAndFilesOperation::new().into(),],
            vec![],
            |params| DiskStorage::new(params.info_hash, params.path, params.data_pool).into(),
            None
        );

        // wait for the pieces to be created
        wait_for_torrent_pieces(&torrent).await;

        // create the stream
        let file = torrent.file(&0).await.unwrap();
        let stream = torrent.stream(&file).await.unwrap();

        let result = stream.size_hint();
        assert_eq!((0, Some(15237)), result, "expected the size hint to match");
    }
}
