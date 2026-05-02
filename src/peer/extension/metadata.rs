use crate::peer::extension::{Error, Result};
use crate::peer::protocol::Message;
use crate::peer::{extension, PeerContext, ProtocolExtensionFlags};
use crate::{PieceIndex, TorrentMetadataInfo};
use log::{debug, error, trace, warn};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{Debug, Formatter};
use std::io::Cursor;
use std::{io, result};
use tokio_util::bytes::Buf;

// The expected metadata piece size is 16 KiB, see BEP9
const METADATA_PIECE_SIZE: usize = 1024 * 16;

/// The BEP9 extension protocol message for the metadata extension.
#[derive(Serialize, Deserialize, PartialEq)]
struct MetadataExtensionMessage {
    /// Indicates which part of the metadata this message refers to
    pub piece: PieceIndex,
    /// The size of the additional bytes after the message
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_size: Option<usize>,
    #[serde(
        serialize_with = "serialize_metadata_type",
        deserialize_with = "deserialize_metadata_type"
    )]
    pub msg_type: MetadataMessageType,
    /// The remaining data within the metadata payload message
    #[serde(skip)]
    pub data: Vec<u8>,
}

impl Debug for MetadataExtensionMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataExtensionMessage")
            .field("piece", &self.piece)
            .field("total_size", &self.total_size)
            .field("msg_type", &self.msg_type)
            .field("data", &format!("[size {}]", self.data.len()))
            .finish()
    }
}

/// The metadata action type of the message.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MetadataMessageType {
    Request = 0,
    Data = 1,
    Reject = 2,
}

pub struct MetadataExtension {
    /// The number of expected pieces
    total_pieces: Option<usize>,
    /// The received metadata pieces
    metadata_buffer: Option<Vec<u8>>,
    initialized: bool,
}

impl MetadataExtension {
    pub const NAME: &'static str = "ut_metadata";

    /// Create a new extension instance.
    pub fn new() -> Self {
        Self {
            total_pieces: None,
            metadata_buffer: None,
            initialized: false,
        }
    }

    /// Process an incoming extension message payload which has been received from the remote peer.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn on_message(&mut self, payload: &[u8], peer: &PeerContext) -> Result<()> {
        let message: MetadataExtensionMessage = Self::deserialize(payload)?;
        trace!("Received metadata message {:?}", message);

        match message.msg_type {
            MetadataMessageType::Request => self.send_metadata(message.piece, peer).await?,
            MetadataMessageType::Data => self.process_metadata(message, peer).await?,
            MetadataMessageType::Reject => debug!(
                "Peer {} rejected the metadata request of piece {}",
                peer, message.piece
            ),
        }

        Ok(())
    }

    /// Invoked once per tick (typically once per second), providing a tick interval for the extension
    /// to process data.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn tick(&mut self, peer: &PeerContext) {
        // early exit if the metadata extension is already initialized
        if self.initialized {
            return;
        }

        let remote = match peer.remote_peer() {
            None => return,
            Some(e) => e,
        };
        if !remote
            .protocol_extensions
            .contains(ProtocolExtensionFlags::LTEP)
            || !remote.extended_handshake
        {
            return;
        }

        self.initialize(peer).await;
        self.initialized = true;
    }

    async fn send_metadata<'a>(&'a self, piece: PieceIndex, peer: &'a PeerContext) -> Result<()> {
        // retrieve the current known metadata
        let metadata = peer
            .metadata()
            .await
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))?
            .info;
        let extension_number = match peer.find_remote_extension_number(Self::NAME) {
            None => return Err(Error::Unsupported),
            Some(e) => e,
        };

        if let Some(metadata) = metadata {
            Self::send_metadata_piece(&metadata, piece, peer).await?;
        } else {
            debug!(
                "Unable to provide torrent metadata to peer {}, metadata is unknown at this moment",
                peer
            );

            // send a reject to the peer as we're unable to provide the metadata
            let message = MetadataExtensionMessage {
                piece: 0,
                total_size: None,
                msg_type: MetadataMessageType::Reject,
                data: vec![],
            };
            let payload = serde_bencode::to_bytes(&message)
                .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))?;

            trace!(
                "Peer {} sending torrent metadata reject, {:?}",
                peer,
                message
            );
            peer.send(Message::ExtendedPayload(extension_number, payload))
                .await
                .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))?;
        }

        Ok(())
    }

    async fn process_metadata<'a>(
        &mut self,
        message: MetadataExtensionMessage,
        peer: &PeerContext,
    ) -> Result<()> {
        let mut total_pieces = self.total_pieces.as_ref().map(|e| e.clone());
        let current_piece = message.piece;

        // check if the total pieces that should be requested is already known
        if let None = total_pieces {
            let metadata_total_size = message.total_size.ok_or(extension::Error::Operation(
                "expected the total size of the metadata to be known".to_string(),
            ))?;
            // always make sure we round up so we get the last piece
            let calculated_total_pieces =
                (metadata_total_size + METADATA_PIECE_SIZE - 1) / METADATA_PIECE_SIZE;

            self.total_pieces = Some(calculated_total_pieces);
            total_pieces = Some(calculated_total_pieces);
            debug!(
                "Peer {} requires a total of {} metadata requests",
                peer, calculated_total_pieces
            );
        }

        {
            // append the data to the metadata buffer
            if let Some(metadata_buffer) = self.metadata_buffer.as_mut() {
                metadata_buffer.extend_from_slice(&message.data);
            } else {
                self.metadata_buffer = Some(message.data);
            }
        }

        if let Some(total_pieces) = total_pieces {
            if total_pieces - 1 == message.piece as usize {
                // try to deserialize the metadata
                let metadata: TorrentMetadataInfo =
                    serde_bencode::from_bytes(self.metadata_buffer.as_ref().unwrap())?;
                debug!("Peer {} completed metadata requests, {:?}", peer, metadata);

                // update the metadata of the underlying torrent through the peer
                peer.set_torrent_metadata(metadata).await;
                // make sure the metadata_buffer is released before trying to clear it
                self.clear_buffer().await;
            } else if self.should_request_metadata(&peer).await {
                trace!(
                    "Requesting next metadata piece {} out of {}",
                    current_piece + 1,
                    total_pieces
                );
                self.request_metadata(current_piece + 1, peer).await?;
            }
        } else {
            warn!("The metadata total pieces should be known at this point");
        }

        Ok(())
    }

    async fn request_metadata<'a>(
        &'a self,
        piece_index: PieceIndex,
        peer: &'a PeerContext,
    ) -> Result<()> {
        let extension_number = match peer.find_remote_extension_number(Self::NAME) {
            None => {
                return Err(Error::Operation(
                    "failed to find metadata extension".to_string(),
                ))
            }
            Some(e) => e,
        };
        let message = MetadataExtensionMessage {
            piece: piece_index,
            total_size: None,
            msg_type: MetadataMessageType::Request,
            data: vec![],
        };
        let payload = serde_bencode::to_bytes(&message)?;

        trace!(
            "Sending metadata request {}",
            String::from_utf8_lossy(payload.as_ref())
        );
        peer.send(Message::ExtendedPayload(extension_number, payload))
            .await
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))
    }

    /// Check if the metadata should be requested for the torrent.
    async fn should_request_metadata<'a>(&'a self, peer: &'a PeerContext) -> bool {
        match peer.metadata().await {
            Ok(metadata) => metadata.info.is_none(),
            Err(e) => {
                warn!(
                    "Peer {} failed to retrieve the torrent metadata, {}",
                    peer, e
                );
                false
            }
        }
    }

    async fn initialize(&self, peer: &PeerContext) {
        if peer.find_remote_extension_number(Self::NAME).is_some()
            && self.should_request_metadata(peer).await
        {
            if let Err(e) = self.request_metadata(0, peer).await {
                error!(
                    "Peer {} failed to retrieve the torrent metadata, {}",
                    peer, e
                );
            }
        }
    }

    async fn send_metadata_piece(
        metadata: &TorrentMetadataInfo,
        piece: PieceIndex,
        peer: &PeerContext,
    ) -> Result<()> {
        // serialize the metadata
        let metadata_bytes = serde_bencode::to_bytes(&metadata)?;
        let metadata_size = metadata_bytes.len();
        let message = MetadataExtensionMessage {
            piece,
            total_size: Some(metadata_size),
            msg_type: MetadataMessageType::Data,
            data: vec![],
        };
        let extension_number = match peer.find_remote_extension_number(Self::NAME) {
            None => return Err(Error::Unsupported),
            Some(e) => e,
        };
        let mut payload = serde_bencode::to_bytes(&message)?;

        // calculate the payload size that should be sent
        let start_index = piece * METADATA_PIECE_SIZE;
        let mut end_index = start_index + METADATA_PIECE_SIZE;

        // check if the last piece is smaller than the METADATA_PIECE_SIZE
        // if so, we need to adjust the end index
        if end_index > metadata_size {
            end_index = metadata_size;
        }

        // append the metadata_bytes slice from the start to end index to the payload
        payload.extend_from_slice(&metadata_bytes[start_index as usize..end_index as usize]);

        // send the payload to the peer
        trace!("Sending torrent metadata to peer {}, {:?}", peer, message);
        peer.send(Message::ExtendedPayload(extension_number, payload))
            .await
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))?;
        Ok(())
    }

    async fn clear_buffer(&mut self) {
        std::mem::swap(&mut self.metadata_buffer, &mut None);
    }

    /// A custom deserializer for the metadata extension message.
    /// This is only used for the [MetadataMessageType::Data] as it contains additional bytes within
    /// the payload which represent the bencoded metadata.
    fn deserialize(payload: &[u8]) -> Result<MetadataExtensionMessage> {
        let mut cursor = Cursor::new(payload);
        let mut deserializer = serde_bencode::de::Deserializer::new(&mut cursor);

        let mut message: MetadataExtensionMessage = Deserialize::deserialize(&mut deserializer)?;
        message.data = cursor.chunk().to_vec();

        Ok(message)
    }
}

impl Debug for MetadataExtension {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataExtension")
            .field("total_pieces", &self.total_pieces)
            .finish()
    }
}

fn serialize_metadata_type<S>(
    message_type: &MetadataMessageType,
    serializer: S,
) -> result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_u8(message_type.clone() as u8)
}

fn deserialize_metadata_type<'de, D>(
    deserializer: D,
) -> result::Result<MetadataMessageType, D::Error>
where
    D: Deserializer<'de>,
{
    let value = u8::deserialize(deserializer)?;
    match value {
        0 => Ok(MetadataMessageType::Request),
        1 => Ok(MetadataMessageType::Data),
        2 => Ok(MetadataMessageType::Reject),
        _ => Err(de::Error::custom(format!(
            "Invalid message type {} specified",
            value
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::PeerState;
    use crate::storage::MemoryStorage;
    use crate::TorrentEvent;
    use fx_callback::Callback;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::sync::mpsc::channel;
    use tokio::{select, time};

    mod extension_message {
        use super::*;

        #[test]
        fn test_serialize() {
            let extension = MetadataExtensionMessage {
                piece: 0,
                total_size: None,
                msg_type: MetadataMessageType::Request,
                data: vec![],
            };
            let expected_result = "d8:msg_typei0e5:piecei0ee";

            let result = serde_bencode::to_string(&extension).unwrap();

            assert_eq!(expected_result, result.as_str());
        }

        #[test]
        fn test_deserialize() {
            let message = "d5:piecei5e8:msg_typei1e10:total_sizei12000ee";
            let expected_result = MetadataExtensionMessage {
                piece: 5,
                total_size: Some(12000),
                msg_type: MetadataMessageType::Data,
                data: vec![],
            };

            let result = serde_bencode::from_bytes(message.as_bytes()).unwrap();

            assert_eq!(expected_result, result);
        }
    }

    #[tokio::test]
    async fn test_request_metadata() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
        let source_torrent = torrent!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            vec![],
            |_| Box::new(MemoryStorage::new()),
            None
        );
        let target_torrent = torrent!(
            uri,
            temp_path,
            TorrentFlags::Metadata,
            TorrentConfig::builder().build(),
            vec![],
            vec![],
            |_| Box::new(MemoryStorage::new()),
            None
        );

        // subscribe to the target torrent events
        let (tx, mut rx) = channel(1);
        let mut receiver = target_torrent.subscribe();
        tokio::spawn(async move {
            while let Ok(event) = receiver.recv().await {
                if let TorrentEvent::MetadataChanged(_) = *event {
                    tx.send(()).await.unwrap();
                }
            }
        });

        // create a new peer pair connection between the 2 torrents
        let (source, target) = tcp_peer_pair!(
            &source_torrent,
            &target_torrent,
            ProtocolExtensionFlags::LTEP
        );

        // wait for the peer handshake to complete
        assert_timeout!(
            Duration::from_secs(1),
            PeerState::Handshake != target.state().await,
            "expected the peer handshake to have been completed"
        );
        let result = source.state().await;
        assert_ne!(
            PeerState::Error,
            result,
            "expected the source peer to be connected"
        );
        let result = target.state().await;
        assert_ne!(
            PeerState::Error,
            result,
            "expected the target peer to be connected"
        );

        select! {
            _ = time::sleep(Duration::from_secs(5)) => assert!(false, "expected the metadata to have been retrieved"),
            result = rx.recv() => assert!(result.is_some(), "expected some metadata to have been retrieved"),
        }
    }
}
