use crate::channel::{ChannelSender, Reply};
use crate::storage::Storage;
use crate::torrent_data::DataPool;
use crate::{File, PiecePriority, Result, TorrentCommand, TorrentError, TorrentEvent};
use futures::future::BoxFuture;
use futures::task::AtomicWaker;
use futures::{ready, FutureExt, Stream};
use fx_callback::Subscription;
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

    fn next_buffer(&self) -> Buffer {
        let buffer_end_byte = min(self.cursor + self.stream_buffer_len, self.file.len());
        self.file.torrent_offset + self.cursor..self.file.torrent_offset + buffer_end_byte
    }

    fn wait_for(&mut self, bytes: &Buffer, cx: &mut Context) -> Poll<Option<Result<Vec<u8>>>> {
        self.waker.register(cx.waker());

        // prioritize the given buffer range within the torrent
        let command_sender = self.command_sender.clone();
        let bytes = bytes.clone();
        tokio::spawn(async move {
            command_sender
                .fire_and_forget(TorrentCommand::PrioritizeBytes {
                    bytes,
                    priority: PiecePriority::Now,
                    response: Reply::empty(),
                })
                .await;
        });

        Poll::Pending
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
                            if let TorrentEvent::PieceCompleted(_) = &*event {
                                waker.wake()
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

        loop {
            match &mut self.state {
                StreamState::Idle => {
                    let buffer = self.next_buffer();
                    let data_pool = self.data_pool.clone();

                    let future = async move { data_pool.has_bytes(buffer).await }.boxed();
                    self.state = StreamState::Checking(future);
                }
                StreamState::Checking(future) => {
                    let is_available = ready!(future.as_mut().poll(cx));
                    let buffer = self.next_buffer();

                    if !is_available {
                        self.state = StreamState::Idle;
                        return self.as_mut().wait_for(&buffer, cx);
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
                    self.state = StreamState::Reading(future);
                }
                StreamState::Reading(future) => {
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
}

impl Drop for FileStream {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
    }
}

enum StreamState {
    Idle,
    Checking(BoxFuture<'static, bool>),
    Reading(BoxFuture<'static, Result<Vec<u8>>>),
}

impl Debug for StreamState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamState::Idle => f.write_str("StreamState::Idle"),
            StreamState::Checking(_) => f.write_str("StreamState::Checking"),
            StreamState::Reading(_) => f.write_str("StreamState::Reading"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::operation::{CreatePiecesAndFilesOperation, FileValidationOperation};
    use crate::storage::DiskStorage;
    use crate::tests::copy_test_file;
    use crate::tests::helpers::wait_for_torrent_pieces;
    use futures::StreamExt;
    use tempfile::tempdir;

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
            vec![
                CreatePiecesAndFilesOperation::new().into(),
                FileValidationOperation::new().into(),
            ],
            vec![],
            |params| DiskStorage::new(params.info_hash, params.path, params.data_pool).into(),
            None
        );

        // wait for the file to be validated
        wait_for_torrent_pieces(&torrent).await;

        // create the stream
        let file = torrent.file(&0).await.unwrap();
        let mut stream = torrent.stream(&file).await.unwrap();

        // try to stream the first 30 pieces (which should be available after validation)
        for piece in 0..30 {
            let result = timeout!(
                Duration::from_millis(500),
                stream.next(),
                format!("time-out streaming piece {}", piece).as_str()
            );
            match result {
                Some(Ok(_)) => {
                    assert_eq!(
                        piece * stream.stream_buffer_len,
                        stream.cursor,
                        "expected the stream cursor to be updated"
                    );
                }
                _ => assert!(false, "expected Some(Ok()), but got {:?}", result),
            }
        }
    }

    #[tokio::test]
    async fn test_stream_reset() {
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
            vec![
                CreatePiecesAndFilesOperation::new().into(),
                FileValidationOperation::new().into(),
            ],
            vec![],
            |params| DiskStorage::new(params.info_hash, params.path, params.data_pool).into(),
            None
        );

        // wait for the file to be validated
        wait_for_torrent_pieces(&torrent).await;

        // create the stream
        let file = torrent.file(&0).await.unwrap();
        let mut stream = torrent.stream(&file).await.unwrap();

        // read the next stream buffer
        let result = timeout!(Duration::from_secs(1), stream.next());
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
    async fn test_stream_seek() {
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
            vec![
                CreatePiecesAndFilesOperation::new().into(),
                FileValidationOperation::new().into(),
            ],
            vec![],
            |params| DiskStorage::new(params.info_hash, params.path, params.data_pool).into(),
            None
        );

        // wait for the file to be validated
        wait_for_torrent_pieces(&torrent).await;

        // create the stream
        let file = torrent.file(&0).await.unwrap();
        let mut stream = torrent.stream(&file).await.unwrap();

        // read the next stream buffer
        let result = timeout!(Duration::from_secs(1), stream.next());
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
}
