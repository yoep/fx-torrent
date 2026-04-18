use crate::peer::{Error, Result};

/// The reasons why a peer is/might be disconnected.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u16)]
pub enum CloseReason {
    /// No reason specified. Generic close.
    None = 0,
    /// We're already connected to this peer ID.
    DuplicatePeerId = 1,
    /// This torrent has been removed, paused or stopped from this client.
    TorrentRemoved = 2,
    /// Client failed to allocate necessary memory for this peer connection.
    NoMemory = 3,
    /// The source port of this peer is blocked.
    PortBlocked = 4,
    /// The source IP has been blocked.
    Blocked = 5,
    /// Both ends of the connection are upload-only.
    UploadToUpload = 6,
    /// Other end is upload only and does not have any pieces we're interested in.
    NotInterestedUploadOnly = 7,
    /// Peer connection timed out (generic timeout).
    Timeout = 8,
    /// Peers have not been interested in each other for a very long time.
    TimedOutInterest = 9,
    /// The peer has not sent any message in a long time.
    TimedOutActivity = 10,
    /// The peer did not complete the handshake in too long.
    TimedOutHandshake = 11,
    /// Peer sent an interested message, but no request after being unchoked.
    TimedOutRequest = 12,
    /// The encryption mode is blocked.
    ProtocolBlocked = 13,
    /// Peer was disconnected in the hopes of finding a better peer.
    PeerChurn = 14,
    /// We have too many peers connected.
    TooManyConnections = 15,
    /// We have too many file-descriptors open.
    TooManyFiles = 16,

    // --- Protocol Errors (256 and up) ---
    /// The encryption handshake failed.
    EncryptionError = 256,
    /// The info hash sent was not what we expected.
    InvalidInfoHash = 257,
    SelfConnection = 258,
    /// Metadata matched info-hash but failed to parse.
    InvalidMetadata = 259,
    /// The advertised metadata size is too large.
    MetadataTooBig = 260,
    /// Invalid bittorrent messages.
    MessageTooBig = 261,
    InvalidMessageId = 262,
    InvalidMessage = 263,
    InvalidPieceMessage = 264,
    InvalidHaveMessage = 265,
    InvalidBitfieldMessage = 266,
    InvalidChokeMessage = 267,
    InvalidUnchokeMessage = 268,
    InvalidInterestedMessage = 269,
    InvalidNotInterestedMessage = 270,
    InvalidRequestMessage = 271,
    InvalidRejectMessage = 272,
    InvalidAllowFastMessage = 273,
    InvalidExtendedMessage = 274,
    InvalidCancelMessage = 275,
    InvalidDhtPortMessage = 276,
    InvalidSuggestMessage = 277,
    InvalidHaveAllMessage = 278,
    InvalidDontHaveMessage = 279,
    InvalidHaveNoneMessage = 280,
    InvalidPexMessage = 281,
    InvalidMetadataRequestMessage = 282,
    InvalidMetadataMessage = 283,
    InvalidMetadataOffset = 284,
    /// The peer sent a request while being choked.
    RequestWhenChoked = 285,
    /// The peer sent corrupt data.
    CorruptPieces = 286,
    PexMessageTooBig = 287,
    PexTooFrequent = 288,
}

impl TryFrom<u16> for CloseReason {
    type Error = Error;

    fn try_from(value: u16) -> Result<Self> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::DuplicatePeerId),
            2 => Ok(Self::TorrentRemoved),
            3 => Ok(Self::NoMemory),
            4 => Ok(Self::PortBlocked),
            5 => Ok(Self::Blocked),
            6 => Ok(Self::UploadToUpload),
            7 => Ok(Self::NotInterestedUploadOnly),
            8 => Ok(Self::Timeout),
            9 => Ok(Self::TimedOutInterest),
            10 => Ok(Self::TimedOutActivity),
            11 => Ok(Self::TimedOutHandshake),
            12 => Ok(Self::TimedOutRequest),
            13 => Ok(Self::ProtocolBlocked),
            14 => Ok(Self::PeerChurn),
            15 => Ok(Self::TooManyConnections),
            16 => Ok(Self::TooManyFiles),

            // --- Protocol Errors (256 and up) ---
            256 => Ok(Self::EncryptionError),
            257 => Ok(Self::InvalidInfoHash),
            258 => Ok(Self::SelfConnection),
            259 => Ok(Self::InvalidMetadata),
            260 => Ok(Self::MetadataTooBig),
            261 => Ok(Self::MessageTooBig),
            262 => Ok(Self::InvalidMessageId),
            263 => Ok(Self::InvalidMessage),
            264 => Ok(Self::InvalidPieceMessage),
            265 => Ok(Self::InvalidHaveMessage),
            266 => Ok(Self::InvalidBitfieldMessage),
            267 => Ok(Self::InvalidChokeMessage),
            268 => Ok(Self::InvalidUnchokeMessage),
            269 => Ok(Self::InvalidInterestedMessage),
            270 => Ok(Self::InvalidNotInterestedMessage),
            271 => Ok(Self::InvalidRequestMessage),
            272 => Ok(Self::InvalidRejectMessage),
            273 => Ok(Self::InvalidAllowFastMessage),
            274 => Ok(Self::InvalidExtendedMessage),
            275 => Ok(Self::InvalidCancelMessage),
            276 => Ok(Self::InvalidDhtPortMessage),
            277 => Ok(Self::InvalidSuggestMessage),
            278 => Ok(Self::InvalidHaveAllMessage),
            279 => Ok(Self::InvalidDontHaveMessage),
            280 => Ok(Self::InvalidHaveNoneMessage),
            281 => Ok(Self::InvalidPexMessage),
            282 => Ok(Self::InvalidMetadataRequestMessage),
            283 => Ok(Self::InvalidMetadataMessage),
            284 => Ok(Self::InvalidMetadataOffset),
            285 => Ok(Self::RequestWhenChoked),
            286 => Ok(Self::CorruptPieces),
            287 => Ok(Self::PexMessageTooBig),
            288 => Ok(Self::PexTooFrequent),

            _ => Err(Error::Parsing(format!(
                "invalid close reason variant {}",
                value
            ))),
        }
    }
}

impl Default for CloseReason {
    fn default() -> Self {
        Self::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_from() {
        assert_eq!(CloseReason::try_from(0), Ok(CloseReason::None));
        assert_eq!(CloseReason::try_from(1), Ok(CloseReason::DuplicatePeerId));
        assert_eq!(CloseReason::try_from(2), Ok(CloseReason::TorrentRemoved));
        assert_eq!(CloseReason::try_from(3), Ok(CloseReason::NoMemory));
        assert_eq!(CloseReason::try_from(4), Ok(CloseReason::PortBlocked));
        assert_eq!(CloseReason::try_from(5), Ok(CloseReason::Blocked));
        assert_eq!(CloseReason::try_from(6), Ok(CloseReason::UploadToUpload));
        assert_eq!(
            CloseReason::try_from(7),
            Ok(CloseReason::NotInterestedUploadOnly)
        );
        assert_eq!(CloseReason::try_from(8), Ok(CloseReason::Timeout));
        assert_eq!(CloseReason::try_from(9), Ok(CloseReason::TimedOutInterest));
    }

    #[test]
    fn test_try_from_invalid_variant() {
        assert_eq!(
            CloseReason::try_from(999),
            Err(Error::Parsing(
                "invalid close reason variant 999".to_string()
            ))
        );
    }

    #[test]
    fn test_default() {
        assert_eq!(CloseReason::default(), CloseReason::None);
    }
}
