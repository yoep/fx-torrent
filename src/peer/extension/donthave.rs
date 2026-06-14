use crate::peer::extension::Result;
use crate::peer::PeerContext;
use crate::PieceIndex;
use log::{debug, trace};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct DontHaveMessage {
    /// The piece index that is no longer available
    piece: u32,
}

/// Discard a piece as no longer available.
#[derive(Debug)]
pub struct DontHaveExtension;

impl DontHaveExtension {
    /// The extension unique name.
    pub const NAME: &'static str = "lt_donthave";

    /// Create a new extension instance.
    pub fn new() -> Self {
        Self {}
    }

    /// Handle the given extension message payload which has been received from the remote peer.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn on_message(&self, payload: &[u8], peer: &mut PeerContext) -> Result<()> {
        trace!("Peer {} is parsing donthave message", peer);
        let piece = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let message = DontHaveMessage { piece };
        debug!("Peer {} parsed \"don't have\" message {:?}", peer, message);

        peer.set_remote_has_piece(message.piece as PieceIndex, false)
            .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::operation::CreatePiecesAndFilesOperation;
    use crate::storage::MemoryStorage;
    use crate::tests::helpers::wait_for_torrent_pieces;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_on_message() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let torrent = torrent!(
            "ubuntu-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![CreatePiecesAndFilesOperation::new().into()],
            vec![],
            |_| MemoryStorage::new().into(),
            None
        );
        let (mut incoming, _outgoing) = peer_context_pair!(&torrent.inner);
        let extension = DontHaveExtension::new();

        // wait for the pieces to be created
        wait_for_torrent_pieces(&torrent).await;

        // set the remote peer to have piece 1
        incoming.set_remote_has_piece(1, true).await;
        assert_eq!(
            true,
            incoming
                .remote_piece_bitfield()
                .get(1)
                .map(|bit| *bit)
                .unwrap_or_default(),
            "expected the remote peer to have piece 1"
        );

        // inform the peer that the remote no longer has piece 1
        let bytes = 1u32.to_be_bytes();
        extension
            .on_message(bytes.as_slice(), &mut incoming)
            .await
            .expect("expected the message to be processed");

        assert_eq!(
            false,
            incoming
                .remote_piece_bitfield()
                .get(1)
                .map(|bit| *bit)
                .unwrap_or_default(),
            "expected the remote peer to not have piece 1"
        );
    }
}
