use crate::dht::compact::{CompactIPv4Nodes, CompactIPv6Nodes};
use crate::dht::{Error, NodeId, Result};
use crate::{CompactIpAddr, InfoHash};
use bitmask_enum::bitmask;
use serde::de::SeqAccess;
use serde::ser::SerializeSeq;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_bencode::value;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::result;
use std::str::FromStr;

const MESSAGE_PING: &str = "ping";
const MESSAGE_FIND_NODE: &str = "find_node";
const MESSAGE_GET_PEERS: &str = "get_peers";
const MESSAGE_ANNOUNCE: &str = "announce_peer";
const MESSAGE_SAMPLE_INFO_HASHES: &str = "sample_infohashes";

/// The unique transaction ID of a message.
pub type TransactionId = [u8; 2];

/// The query request message.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "q")]
pub enum QueryMessage {
    #[serde(rename = "ping")]
    Ping {
        #[serde(rename = "a")]
        request: PingMessage,
    },
    #[serde(rename = "find_node")]
    FindNode {
        #[serde(rename = "a")]
        request: FindNodeRequest,
    },
    #[serde(rename = "get_peers")]
    GetPeers {
        #[serde(rename = "a")]
        request: GetPeersRequest,
    },
    #[serde(rename = "announce_peer")]
    AnnouncePeer {
        #[serde(rename = "a")]
        request: AnnouncePeerRequest,
    },
    #[serde(rename = "sample_infohashes")]
    SampleInfoHashes {
        #[serde(rename = "a")]
        request: SampleInfoHashesRequest,
    },
}

impl QueryMessage {
    /// Returns the node ID of the sender.
    pub fn id(&self) -> &NodeId {
        match self {
            QueryMessage::Ping { request } => &request.id,
            QueryMessage::FindNode { request } => &request.id,
            QueryMessage::GetPeers { request } => &request.id,
            QueryMessage::AnnouncePeer { request } => &request.id,
            QueryMessage::SampleInfoHashes { request } => &request.id,
        }
    }

    /// Returns the name/type of the query message.
    pub fn name(&self) -> &str {
        match self {
            QueryMessage::Ping { .. } => MESSAGE_PING,
            QueryMessage::FindNode { .. } => MESSAGE_FIND_NODE,
            QueryMessage::GetPeers { .. } => MESSAGE_GET_PEERS,
            QueryMessage::AnnouncePeer { .. } => MESSAGE_ANNOUNCE,
            QueryMessage::SampleInfoHashes { .. } => MESSAGE_SAMPLE_INFO_HASHES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ResponseMessage {
    GetPeers {
        #[serde(rename = "r")]
        response: GetPeersResponse,
    },
    FindNode {
        #[serde(rename = "r")]
        response: FindNodeResponse,
    },
    SampleInfoHashes {
        #[serde(rename = "r")]
        response: SampleInfoHashesResponse,
    },
    Announce {
        #[serde(rename = "r")]
        response: AnnouncePeerResponse,
    },
    Ping {
        #[serde(rename = "r")]
        response: PingMessage,
    },
}

impl ResponseMessage {
    /// Returns the node ID of the sender.
    pub fn id(&self) -> &NodeId {
        match self {
            ResponseMessage::GetPeers { response } => &response.id,
            ResponseMessage::FindNode { response } => &response.id,
            ResponseMessage::Ping { response } => &response.id,
            ResponseMessage::Announce { response } => &response.id,
            ResponseMessage::SampleInfoHashes { response } => &response.id,
        }
    }

    /// Returns the name/type of the response message.
    pub fn name(&self) -> &str {
        match self {
            ResponseMessage::GetPeers { .. } => MESSAGE_GET_PEERS,
            ResponseMessage::FindNode { .. } => MESSAGE_FIND_NODE,
            ResponseMessage::Ping { .. } => MESSAGE_PING,
            ResponseMessage::Announce { .. } => MESSAGE_ANNOUNCE,
            ResponseMessage::SampleInfoHashes { .. } => MESSAGE_SAMPLE_INFO_HASHES,
        }
    }
}

/// The request- & response message of a ping query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PingMessage {
    pub id: NodeId,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct FindNodeRequest {
    pub id: NodeId,
    pub target: NodeId,
    #[serde(default, skip_serializing_if = "WantFamily::is_none")]
    pub want: WantFamily,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FindNodeResponse {
    /// The id of the node that was queried.
    pub id: NodeId,
    #[serde(default, skip_serializing_if = "CompactIPv4Nodes::is_empty")]
    pub nodes: CompactIPv4Nodes,
    #[serde(default, skip_serializing_if = "CompactIPv6Nodes::is_empty")]
    pub nodes6: CompactIPv6Nodes,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "serde_bytes")]
    pub token: Option<Vec<u8>>,
}

#[bitmask(u8)]
pub enum WantFamily {
    Ipv4,
    Ipv6,
}

impl WantFamily {
    /// Returns the underlying want value.
    pub fn values(&self) -> Vec<&str> {
        let mut result = vec![];
        if self.contains(WantFamily::Ipv4) {
            result.push("n4");
        }
        if self.contains(WantFamily::Ipv6) {
            result.push("n6");
        }
        result
    }

    /// Returns the number of wanted values.
    pub fn len(&self) -> usize {
        let mut len = 0;
        if self.contains(WantFamily::Ipv4) {
            len += 1;
        }
        if self.contains(WantFamily::Ipv6) {
            len += 1;
        }
        len
    }
}

impl FromStr for WantFamily {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "n4" => Ok(WantFamily::Ipv4),
            "n6" => Ok(WantFamily::Ipv6),
            _ => Err(Error::Parse(
                format!("invalid want value {}", s).to_string(),
            )),
        }
    }
}

impl Default for WantFamily {
    fn default() -> Self {
        WantFamily::none()
    }
}

impl Serialize for WantFamily {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for value in self.values() {
            seq.serialize_element(value)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for WantFamily {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct WantVisitor;
        impl<'de> de::Visitor<'de> for WantVisitor {
            type Value = WantFamily;

            fn expecting(&self, f: &mut Formatter) -> std::fmt::Result {
                write!(f, "expected a sequence of Want values")
            }

            fn visit_seq<A>(self, mut seq: A) -> result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut result = WantFamily::none();
                while let Some(value) = seq.next_element::<String>().map_err(|e| {
                    de::Error::custom(format!("failed to deserialize Want value: {}", e))
                })? {
                    result |= WantFamily::from_str(value.as_str()).map_err(de::Error::custom)?;
                }
                Ok(result)
            }
        }

        deserializer.deserialize_any(WantVisitor)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct GetPeersRequest {
    pub id: NodeId,
    pub info_hash: InfoHash,
    #[serde(default, skip_serializing_if = "WantFamily::is_none")]
    pub want: WantFamily,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetPeersResponse {
    pub id: NodeId,
    #[serde(with = "serde_bytes")]
    pub token: Vec<u8>,
    #[serde(default, with = "serde_bytes", skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "CompactIPv4Nodes::is_empty")]
    pub nodes: CompactIPv4Nodes,
    #[serde(default, skip_serializing_if = "CompactIPv6Nodes::is_empty")]
    pub nodes6: CompactIPv6Nodes,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct AnnouncePeerRequest {
    pub id: NodeId,
    #[serde(with = "serde_implied_port")]
    pub implied_port: bool,
    pub info_hash: InfoHash,
    pub port: u16,
    pub token: String,
    /// The name of the torrent, if provided
    #[serde(default, rename = "n", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnnouncePeerResponse {
    pub id: NodeId,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct SampleInfoHashesRequest {
    pub id: NodeId,
    pub target: NodeId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SampleInfoHashesResponse {
    pub id: NodeId,
    pub interval: u32,
    #[serde(default, skip_serializing_if = "CompactIPv4Nodes::is_empty")]
    pub nodes: CompactIPv4Nodes,
    #[serde(default, skip_serializing_if = "CompactIPv6Nodes::is_empty")]
    pub nodes6: CompactIPv6Nodes,
    /// The number of info hashes in storage
    pub num: u32,
    /// The subset of stored info hashes as 20 byte string
    #[serde(with = "serde_info_hash")]
    pub samples: Vec<InfoHash>,
}

/// The error message.
#[derive(Debug, PartialEq)]
pub enum ErrorMessage {
    Generic(String),
    Server(String),
    Protocol(String),
    Method(String),
}

impl ErrorMessage {
    /// Get the error code of the error message.
    /// See BEP5 for more info about codes.
    pub fn code(&self) -> u16 {
        match self {
            ErrorMessage::Generic(_) => 201,
            ErrorMessage::Server(_) => 202,
            ErrorMessage::Protocol(_) => 203,
            ErrorMessage::Method(_) => 204,
        }
    }

    /// Get the error description of the error message.
    pub fn description(&self) -> &str {
        match self {
            ErrorMessage::Generic(msg) => msg.as_str(),
            ErrorMessage::Server(msg) => msg.as_str(),
            ErrorMessage::Protocol(msg) => msg.as_str(),
            ErrorMessage::Method(msg) => msg.as_str(),
        }
    }
}

impl Serialize for ErrorMessage {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = (self.code(), self.description());

        HashMap::from([("e".to_string(), value)]).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ErrorMessage {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let map = HashMap::<String, (u16, String)>::deserialize(deserializer)?;

        if let Some((_, (code, msg))) = map.into_iter().next() {
            match code {
                201 => Ok(ErrorMessage::Generic(msg)),
                202 => Ok(ErrorMessage::Server(msg)),
                203 => Ok(ErrorMessage::Protocol(msg)),
                404 => Ok(ErrorMessage::Method(msg)),
                _ => Err(de::Error::custom(format!("Unknown error code {}", code))),
            }
        } else {
            Err(de::Error::custom("expected an error key pair"))
        }
    }
}

/// The response payload of a message.
/// This is either a raw bencode value or a response message.
#[derive(Debug, PartialEq)]
pub enum ResponsePayload {
    Raw {
        id: Option<NodeId>,
        value: value::Value,
    },
    Message(ResponseMessage),
}

impl ResponsePayload {
    /// Returns the node ID of the sender, if known/available.
    pub fn id(&self) -> Option<&NodeId> {
        match self {
            ResponsePayload::Raw { id, .. } => id.as_ref(),
            ResponsePayload::Message(msg) => Some(msg.id()),
        }
    }

    /// Parse the response payload into a response message based on the query name.
    pub fn parse(&self, query_name: &str) -> Result<ResponseMessage> {
        let bytes = match self {
            ResponsePayload::Message(e) => return Ok(e.clone()),
            ResponsePayload::Raw { value, .. } => {
                // extract "r" from the raw value as we're trying to parse a specific message
                if let value::Value::Dict(e) = value {
                    match e.get("r".as_bytes()) {
                        Some(v) => serde_bencode::to_bytes(v)?, // this is overhead, but I've no other idea on how to use the Value
                        _ => {
                            return Err(Error::InvalidMessage(
                                "expected the raw value to container 'r'".to_string(),
                            ))
                        }
                    }
                } else {
                    return Err(Error::InvalidMessage(
                        "expected the raw value to be a dict".to_string(),
                    ));
                }
            }
        };

        Self::try_parse(query_name, bytes)
    }

    fn try_parse(query_name: &str, bytes: Vec<u8>) -> Result<ResponseMessage> {
        match query_name {
            MESSAGE_PING => Ok(ResponseMessage::Ping {
                response: serde_bencode::from_bytes(bytes.as_slice())
                    .map_err(|e| Self::parse_error(query_name, e))?,
            }),
            MESSAGE_FIND_NODE => Ok(ResponseMessage::FindNode {
                response: serde_bencode::from_bytes(bytes.as_slice())
                    .map_err(|e| Self::parse_error(query_name, e))?,
            }),
            MESSAGE_GET_PEERS => Ok(ResponseMessage::GetPeers {
                response: serde_bencode::from_bytes(bytes.as_slice())
                    .map_err(|e| Self::parse_error(query_name, e))?,
            }),
            MESSAGE_ANNOUNCE => Ok(ResponseMessage::Announce {
                response: serde_bencode::from_bytes(bytes.as_slice())
                    .map_err(|e| Self::parse_error(query_name, e))?,
            }),
            MESSAGE_SAMPLE_INFO_HASHES => Ok(ResponseMessage::SampleInfoHashes {
                response: serde_bencode::from_bytes(bytes.as_slice())
                    .map_err(|e| Self::parse_error(query_name, e))?,
            }),
            _ => Err(Error::InvalidMessage(format!(
                "unable to parse response payload, unknown query \"{}\"",
                query_name
            ))),
        }
    }

    fn parse_error(query_name: &str, err: serde_bencode::error::Error) -> Error {
        Error::Parse(format!(
            "failed to parse response \"{}\", {}",
            query_name, err
        ))
    }
}

impl Serialize for ResponsePayload {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ResponsePayload::Raw { value, .. } => value.serialize(serializer),
            ResponsePayload::Message(msg) => msg.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for ResponsePayload {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // always decode the raw value when received over the network
        // this is later deserialized into a ResponseMessage once the original request is known
        let value = value::Value::deserialize(deserializer)?;
        let id = match &value {
            value::Value::Dict(dict) => dict.get("r".as_bytes()).and_then(|e| match e {
                value::Value::Dict(r) => r.get("id".as_bytes()).and_then(|id| match id {
                    value::Value::Bytes(bytes) => NodeId::try_from(bytes.as_slice()).ok(),
                    _ => None,
                }),
                _ => None,
            }),
            _ => None,
        };

        Ok(ResponsePayload::Raw { id, value })
    }
}

/// The payload data of a message.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "y")]
pub enum MessagePayload {
    #[serde(rename = "q")]
    Query(QueryMessage),
    #[serde(rename = "r")]
    Response(ResponsePayload),
    #[serde(rename = "e")]
    Error(ErrorMessage),
}

/// The version info of the DHT node.
#[derive(Debug, PartialEq)]
pub struct Version {
    raw: Vec<u8>,
}

impl Serialize for Version {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match std::str::from_utf8(&self.raw) {
            Ok(e) => serializer.serialize_str(e),
            Err(_) => serializer.serialize_bytes(&self.raw),
        }
    }
}

impl<'de> Deserialize<'de> for Version {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VersionVisitor;
        impl<'de> de::Visitor<'de> for VersionVisitor {
            type Value = Version;

            fn expecting(&self, f: &mut Formatter) -> std::fmt::Result {
                write!(f, "expected a version string or bytes")
            }

            fn visit_str<E>(self, v: &str) -> result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Self::Value::from(v))
            }

            fn visit_bytes<E>(self, v: &[u8]) -> result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Self::Value { raw: v.to_vec() })
            }
        }

        deserializer.deserialize_any(VersionVisitor)
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", std::str::from_utf8(&self.raw).unwrap_or("UNKNOWN"))
    }
}

impl Default for Version {
    fn default() -> Self {
        Self { raw: vec![] }
    }
}

impl From<&str> for Version {
    fn from(s: &str) -> Self {
        Self {
            raw: s.as_bytes().to_vec(),
        }
    }
}

/// The KRPC message communication between nodes.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Message {
    #[serde(rename = "t", with = "serde_bytes")]
    pub transaction_id_bytes: TransactionId,
    #[serde(default, rename = "v", skip_serializing_if = "Option::is_none")]
    pub version: Option<Version>,
    #[serde(flatten)]
    pub payload: MessagePayload,
    /// The node's external IP.
    /// See BEP42 for more info.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip: Option<CompactIpAddr>,
    /// The node's external port
    #[serde(default, skip_serializing_if = "Option::is_none", with = "serde_bytes")]
    pub port: Option<[u8; 2]>, // this field is present in libtorrent, but not documented in a BEP
    #[serde(default, rename = "ro", skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,
}

impl Message {
    /// Returns a new builder instance to create a message.
    pub fn builder() -> MessageBuilder {
        MessageBuilder::new()
    }

    /// Returns the node ID of the sender, if available.
    pub fn id(&self) -> Option<&NodeId> {
        match &self.payload {
            MessagePayload::Query(q) => Some(q.id()),
            MessagePayload::Response(response_payload) => response_payload.id(),
            MessagePayload::Error(_) => None,
        }
    }

    /// Returns the [u16] representation of the transaction ID.
    pub fn transaction_id(&self) -> u16 {
        u16::from_be_bytes(self.transaction_id_bytes)
    }
}

#[derive(Debug, Default)]
pub(crate) struct MessageBuilder {
    transaction_id: Option<Vec<u8>>,
    version: Option<Version>,
    payload: Option<MessagePayload>,
    ip: Option<CompactIpAddr>,
    port: Option<[u8; 2]>,
    read_only: Option<bool>,
}

impl MessageBuilder {
    /// Create a new instance of the message builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the transaction of the message.
    pub fn transaction_id(&mut self, id: u16) -> &mut Self {
        self.transaction_id = Some(id.to_be_bytes().to_vec());
        self
    }

    /// Set the underlying transaction ID bytes.
    /// This is useful for testing purposes.
    #[cfg(test)]
    fn transaction_id_bytes(&mut self, id: &[u8]) -> &mut Self {
        self.transaction_id = Some(id.to_vec());
        self
    }

    /// Set the version of the message.
    pub fn version(&mut self, version: Version) -> &mut Self {
        self.version = Some(version);
        self
    }

    /// Set the payload data of the message.
    pub fn payload(&mut self, payload: MessagePayload) -> &mut Self {
        self.payload = Some(payload);
        self
    }

    /// Set the node's external compact IP address.
    pub fn ip(&mut self, ip: CompactIpAddr) -> &mut Self {
        self.ip = Some(ip);
        self
    }

    /// Set the node's external port.
    pub fn port(&mut self, port: u16) -> &mut Self {
        self.port = Some(port.to_be_bytes());
        self
    }

    /// Set the read-only flag of the message.
    pub fn read_only(&mut self, read_only: bool) -> &mut Self {
        self.read_only = Some(read_only);
        self
    }

    /// Finalize the builder and try to create a new message.
    ///
    /// The transaction ID and message type are required fields.
    /// When one of the required fields was not provided, it will return an error.
    pub fn build(&mut self) -> Result<Message> {
        let transaction_id_value = self
            .transaction_id
            .take()
            .ok_or(Error::InvalidMessage("missing transaction id".to_string()))?;

        Ok(Message {
            transaction_id_bytes: transaction_id_value
                .as_slice()
                .try_into()
                .map_err(|_| Error::InvalidTransactionId)?,
            version: self.version.take(),
            payload: self
                .payload
                .take()
                .ok_or(Error::InvalidMessage("missing payload".to_string()))?,
            ip: self.ip.take(),
            port: self.port.take(),
            read_only: self.read_only.take().unwrap_or(false),
        })
    }
}

mod serde_info_hash {
    use super::*;
    use itertools::Itertools;
    use serde::de::Visitor;

    pub fn serialize<S>(value: &Vec<InfoHash>, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = value
            .iter()
            .map(|e| e.short_info_hash_bytes().to_vec())
            .concat();
        serializer.serialize_bytes(&bytes)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> result::Result<Vec<InfoHash>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct InfoHashVisitor;
        impl<'de> Visitor<'de> for InfoHashVisitor {
            type Value = Vec<InfoHash>;

            fn expecting(&self, f: &mut Formatter) -> std::fmt::Result {
                write!(f, "expected a sequence of info hash bytes")
            }

            fn visit_bytes<E>(self, bytes: &[u8]) -> result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                let n = bytes.len() / 20;
                let mut info_hashes = Vec::with_capacity(n);
                for chunk in bytes.chunks(20) {
                    info_hashes.push(InfoHash::try_from_bytes(chunk).map_err(de::Error::custom)?);
                }
                Ok(info_hashes)
            }

            fn visit_seq<A>(self, mut seq: A) -> result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut info_hashes = Vec::new();
                while let Some(e) = seq.next_element::<InfoHash>()? {
                    info_hashes.push(e);
                }
                Ok(info_hashes)
            }
        }

        D::deserialize_any(deserializer, InfoHashVisitor {})
    }
}

mod serde_implied_port {
    use super::*;
    use serde::de::Visitor;

    pub fn serialize<S>(value: &bool, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(*value as u64)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> result::Result<bool, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BoolVisitor;
        impl<'de> Visitor<'de> for BoolVisitor {
            type Value = bool;

            fn expecting(&self, f: &mut Formatter) -> std::fmt::Result {
                write!(f, "expected an integer representing a boolean value")
            }

            fn visit_i64<E>(self, v: i64) -> result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                match v {
                    0 => Ok(false),
                    1 => Ok(true),
                    _ => Err(de::Error::custom("invalid boolean value")),
                }
            }

            fn visit_u64<E>(self, v: u64) -> result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                match v {
                    0 => Ok(false),
                    1 => Ok(true),
                    _ => Err(de::Error::custom("invalid boolean value")),
                }
            }
        }

        deserializer.deserialize_any(BoolVisitor {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod ping {
        use super::*;

        #[test]
        fn test_request() {
            let payload = "d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe";
            let node_id = NodeId::try_from("abcdefghij0123456789".as_bytes()).unwrap();
            let expected_result = Message::builder()
                .transaction_id_bytes("aa".as_bytes())
                .payload(MessagePayload::Query(QueryMessage::Ping {
                    request: PingMessage { id: node_id },
                }))
                .build()
                .unwrap();

            // deserialize the payload
            let result =
                serde_bencode::from_str::<Message>(payload).expect("expected a valid message");
            assert_eq!(expected_result, result);

            // serialize the payload and compare it with the original payload
            let result = serde_bencode::to_string(&result).unwrap();
            assert_eq!(payload, result.as_str());
        }

        #[test]
        fn test_response() {
            let payload = "d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re";
            let node_id = NodeId::try_from("mnopqrstuvwxyz123456".as_bytes()).unwrap();
            let expected_result = Message::builder()
                .transaction_id_bytes("aa".as_bytes())
                .payload(MessagePayload::Response(ResponsePayload::Message(
                    ResponseMessage::Ping {
                        response: PingMessage { id: node_id },
                    },
                )))
                .build()
                .unwrap();

            // deserialize the payload
            let result = deserialize_response(
                serde_bencode::from_str::<Message>(payload).unwrap(),
                MESSAGE_PING,
            );
            assert_eq!(Some(&node_id), result.id(), "expected the node id to match");
            assert_eq!(expected_result, result);

            // serialize the payload and compare it with the original payload
            let result = serde_bencode::to_string(&result).unwrap();
            assert_eq!(payload, result.as_str());
        }
    }

    mod find_node {
        use super::*;
        use crate::dht::compact::CompactIPv4Node;
        use crate::CompactIpv4Addr;
        use std::net::Ipv4Addr;

        #[test]
        fn test_request() {
            let payload = "d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe";
            let id = NodeId::try_from("abcdefghij0123456789".as_bytes()).unwrap();
            let expected_result = Message::builder()
                .transaction_id_bytes("aa".as_bytes())
                .payload(MessagePayload::Query(QueryMessage::FindNode {
                    request: FindNodeRequest {
                        id,
                        target: NodeId::try_from("mnopqrstuvwxyz123456".as_bytes()).unwrap(),
                        want: Default::default(),
                    },
                }))
                .build()
                .unwrap();

            // deserialize the payload
            let result =
                serde_bencode::from_str::<Message>(payload).expect("expected a valid message");
            assert_eq!(expected_result, result);

            // serialize the payload and compare it with the original payload
            let result = serde_bencode::to_string(&result).unwrap();
            assert_eq!(payload, result.as_str());
        }

        #[test]
        fn test_response() {
            // as compact address cannot be printed as UTF8 strings,
            // we're going to use hex representation instead
            let payload_hex = "64313a7264323a696432303a303132333435363738396162636465666768696a353a6e6f64657332363a71776572747930313233343536373839617a65727f0000011ae1353a746f6b656e31323a746f6b656e6578616d706c6565313a74323a6161313a79313a7265";
            let id = NodeId::try_from("0123456789abcdefghij".as_bytes()).unwrap();
            let token = "tokenexample".as_bytes();
            let expected_result = Message::builder()
                .transaction_id_bytes("aa".as_bytes())
                .payload(MessagePayload::Response(ResponsePayload::Message(
                    ResponseMessage::FindNode {
                        response: FindNodeResponse {
                            id,
                            nodes: vec![CompactIPv4Node {
                                id: NodeId::try_from("qwerty0123456789azer".as_bytes()).unwrap(),
                                addr: CompactIpv4Addr {
                                    ip: Ipv4Addr::LOCALHOST,
                                    port: 6881,
                                },
                            }]
                            .into(),
                            nodes6: Default::default(),
                            token: Some(token.to_vec()),
                        },
                    },
                )))
                .build()
                .unwrap();

            // deserialize the payload
            let bytes = hex::decode(payload_hex).unwrap();
            let result = deserialize_response(
                serde_bencode::from_bytes::<Message>(bytes.as_slice()).unwrap(),
                MESSAGE_FIND_NODE,
            );
            assert_eq!(expected_result, result);

            // serialize the payload and compare it with the original payload
            let result = serde_bencode::to_bytes(&result).unwrap();
            assert_eq!(payload_hex, hex::encode(result.as_slice()));
        }
    }

    mod announce_peer {
        use super::*;

        #[test]
        fn test_request() {
            let payload = "d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe";
            let expected_result = Message::builder()
                .transaction_id_bytes("aa".as_bytes())
                .payload(MessagePayload::Query(QueryMessage::AnnouncePeer {
                    request: AnnouncePeerRequest {
                        id: NodeId::try_from("abcdefghij0123456789".as_bytes()).unwrap(),
                        implied_port: true,
                        info_hash: InfoHash::from_str("mnopqrstuvwxyz123456").unwrap(),
                        port: 6881,
                        token: "aoeusnth".to_string(),
                        name: None,
                        seed: None,
                    },
                }))
                .build()
                .unwrap();

            // deserialize the payload
            let result =
                serde_bencode::from_str::<Message>(payload).expect("expected a valid message");
            assert_eq!(expected_result, result);

            // serialize the payload and compare it with the original payload
            let result = serde_bencode::to_string(&result).unwrap();
            assert_eq!(payload, result.as_str());
        }

        #[test]
        fn test_response() {
            let payload = "d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re";
            let expected_result = Message::builder()
                .transaction_id_bytes("aa".as_bytes())
                .payload(MessagePayload::Response(ResponsePayload::Message(
                    ResponseMessage::Announce {
                        response: AnnouncePeerResponse {
                            id: NodeId::try_from("mnopqrstuvwxyz123456".as_bytes()).unwrap(),
                        },
                    },
                )))
                .build()
                .unwrap();

            // deserialize the payload
            let result = deserialize_response(
                serde_bencode::from_str::<Message>(payload).unwrap(),
                MESSAGE_ANNOUNCE,
            );
            assert_eq!(expected_result, result);

            // serialize the payload and compare it with the original payload
            let result = serde_bencode::to_string(&result).unwrap();
            assert_eq!(payload, result.as_str());
        }
    }

    mod sample_infohashes {
        use super::*;

        #[test]
        fn test_request() {
            let payload = "d1:ad2:id20:abcdefghij01234567896:target20:qwerty0123456789azere1:q17:sample_infohashes1:t2:aa1:y1:qe";
            let id = NodeId::try_from("abcdefghij0123456789".as_bytes()).unwrap();
            let target = NodeId::try_from("qwerty0123456789azer".as_bytes()).unwrap();
            let expected_result = Message::builder()
                .transaction_id_bytes("aa".as_bytes())
                .payload(MessagePayload::Query(QueryMessage::SampleInfoHashes {
                    request: SampleInfoHashesRequest { id, target },
                }))
                .build()
                .unwrap();

            // deserialize the payload
            let result =
                serde_bencode::from_str::<Message>(payload).expect("expected a valid message");
            assert_eq!(expected_result, result);

            // serialize the payload and compare it with the original payload
            let result = serde_bencode::to_string(&result).unwrap();
            assert_eq!(payload, result.as_str());
        }

        #[test]
        fn test_response() {
            let payload_hex = "64313a7264323a696432303a6162636465666768696a30313233343536373839383a696e74657276616c6931323065333a6e756d693265373a73616d706c657332303aeadaf0efea39406914414d359e0ea16416409bd765313a74323a6161313a79313a7265";
            let id = NodeId::try_from("abcdefghij0123456789".as_bytes()).unwrap();
            let expected_result = Message::builder()
                .transaction_id_bytes("aa".as_bytes())
                .payload(MessagePayload::Response(ResponsePayload::Message(
                    ResponseMessage::SampleInfoHashes {
                        response: SampleInfoHashesResponse {
                            id,
                            interval: 120,
                            nodes: Default::default(),
                            nodes6: Default::default(),
                            num: 2,
                            samples: vec![InfoHash::from_str(
                                "urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7",
                            )
                            .unwrap()],
                        },
                    },
                )))
                .build()
                .unwrap();

            // deserialize the payload
            let bytes = hex::decode(payload_hex).unwrap();
            let result = deserialize_response(
                serde_bencode::from_bytes::<Message>(bytes.as_slice()).unwrap(),
                MESSAGE_SAMPLE_INFO_HASHES,
            );
            assert_eq!(expected_result, result);

            // serialize the payload and compare it with the original payload
            let result = serde_bencode::to_bytes(&result).unwrap();
            assert_eq!(payload_hex, hex::encode(result.as_slice()));
        }
    }

    mod error {
        use super::*;

        #[test]
        fn test_error_serialize() {
            let message = Message::builder()
                .transaction_id_bytes("aa".as_bytes())
                .payload(MessagePayload::Error(ErrorMessage::Generic(
                    "A Generic Error Occurred".to_string(),
                )))
                .build()
                .unwrap();
            let expected_result = "d1:eli201e24:A Generic Error Occurrede1:t2:aa1:y1:ee";

            let result = serde_bencode::to_string(&message).unwrap();

            assert_eq!(expected_result, result.as_str());
        }

        #[test]
        fn test_error_deserialize() {
            let payload = "d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee";
            let expected_result = Message::builder()
                .transaction_id_bytes("aa".as_bytes())
                .payload(MessagePayload::Error(ErrorMessage::Generic(
                    "A Generic Error Ocurred".to_string(),
                )))
                .build()
                .unwrap();

            let result =
                serde_bencode::from_str::<Message>(payload).expect("expected a valid message");

            assert_eq!(expected_result, result);
        }
    }

    mod want {
        use super::*;

        #[test]
        fn test_deserialize() {
            let want = WantFamily::Ipv4;
            let bytes = serde_bencode::to_bytes(&want).unwrap();
            let result = serde_bencode::from_bytes::<WantFamily>(bytes.as_slice()).unwrap();
            assert_eq!(want, result);

            let want = WantFamily::Ipv6;
            let bytes = serde_bencode::to_bytes(&want).unwrap();
            let result = serde_bencode::from_bytes::<WantFamily>(bytes.as_slice()).unwrap();
            assert_eq!(want, result);

            let want = WantFamily::Ipv4 | WantFamily::Ipv6;
            let bytes = serde_bencode::to_bytes(&want).unwrap();
            let result = serde_bencode::from_bytes::<WantFamily>(bytes.as_slice()).unwrap();
            assert_eq!(want, result);
        }
    }

    /// Deserialize the [ResponsePayload::Raw] into [ResponsePayload::Message].
    fn deserialize_response(mut message: Message, query: &str) -> Message {
        match &message.payload {
            MessagePayload::Response(payload) => {
                message.payload = MessagePayload::Response(ResponsePayload::Message(
                    payload.parse(query).unwrap(),
                ));
            }
            _ => assert!(
                false,
                "expected MessagePayload::Response, but got {:?}",
                message.payload
            ),
        }

        message
    }
}
