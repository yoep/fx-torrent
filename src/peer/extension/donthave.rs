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
