use crate::peer::extension::Result;
use crate::peer::{ConnectionProtocol, PeerContext};
use serde::{Deserialize, Serialize};

/// The BEP55 holepunch extension message.
#[derive(Debug, Serialize, Deserialize)]
struct HolepunchMessage {
    /// Type of the holepunch message
    #[serde(rename = "msg_type")]
    message_type: MessageType,
    /// IP address family type
    addr_type: AddrType,
    #[serde(with = "serde_bytes")]
    addr: Vec<u8>,
    port: u16,
    #[serde(default, rename = "err_code", skip_serializing_if = "Option::is_none")]
    err_code: Option<ErrorCode>,
}

#[repr(u8)]
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(into = "u8", try_from = "u8")]
enum MessageType {
    Rendezvous = 0,
    Connect = 1,
    Error = 2,
}

impl From<MessageType> for u8 {
    fn from(value: MessageType) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for MessageType {
    type Error = String;

    fn try_from(value: u8) -> std::result::Result<Self, String> {
        match value {
            0 => Ok(MessageType::Rendezvous),
            1 => Ok(MessageType::Connect),
            2 => Ok(MessageType::Error),
            _ => Err(format!("Invalid MessageType {}", value)),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(into = "u8", try_from = "u8")]
enum AddrType {
    Ipv4 = 0,
    Ipv6 = 1,
}

impl From<AddrType> for u8 {
    fn from(value: AddrType) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for AddrType {
    type Error = String;

    fn try_from(value: u8) -> std::result::Result<Self, String> {
        match value {
            0 => Ok(AddrType::Ipv4),
            1 => Ok(AddrType::Ipv6),
            _ => Err(format!("Invalid AddrType {}", value)),
        }
    }
}

#[repr(u32)]
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(into = "u32", try_from = "u32")]
enum ErrorCode {
    NoSuchPeer = 1,
    NotConnected = 2,
    NoSupport = 3,
    NoSelf = 4,
}

impl From<ErrorCode> for u32 {
    fn from(value: ErrorCode) -> Self {
        value as u32
    }
}

impl TryFrom<u32> for ErrorCode {
    type Error = String;

    fn try_from(value: u32) -> std::result::Result<Self, String> {
        match value {
            1 => Ok(ErrorCode::NoSuchPeer),
            2 => Ok(ErrorCode::NotConnected),
            3 => Ok(ErrorCode::NoSupport),
            4 => Ok(ErrorCode::NoSelf),
            _ => Err(format!("Invalid ErrorCode {}", value)),
        }
    }
}

/// The holepunch extension as defined in BEP55
#[derive(Debug)]
pub struct HolepunchExtension {}

impl HolepunchExtension {
    pub const NAME: &'static str = "ut_holepunch";

    /// Create a new extension instance.
    pub fn new() -> Self {
        Self {}
    }

    /// Handle the given extension message payload which has been received from the remote peer.
    pub async fn handle<'a>(&'a self, payload: &'a [u8], _peer: &'a PeerContext) -> Result<()> {
        let _msg = serde_bencode::from_bytes::<HolepunchMessage>(payload)?;
        // TODO
        Ok(())
    }

    /// Returns `true` if the peer is using the uTP connection protocol.
    fn is_utp_connection(&self, peer: &PeerContext) -> bool {
        peer.connection_protocol() == ConnectionProtocol::Utp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_on_handle() {
        let extension = HolepunchExtension::new();
    }
}
