use crate::dht::compact::{CompactIPv4Nodes, CompactIPv6Nodes};
use crate::dht::{Error, NodeId, Result};
use crate::{CompactIpAddr, InfoHash};
use bitmask_enum::bitmask;
use serde::de::{DeserializeOwned, IgnoredAny, MapAccess, SeqAccess};
use serde::ser::SerializeSeq;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::result;
use std::str::FromStr;

const MESSAGE_PING: &str = "ping";
const MESSAGE_FIND_NODE: &str = "find_node";
const MESSAGE_GET_PEERS: &str = "get_peers";
const MESSAGE_ANNOUNCE: &str = "announce_peer";
const MESSAGE_SAMPLE_INFO_HASHES: &str = "sample_infohashes";

/// The unique transaction ID of a message.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TransactionId {
    #[serde(with = "serde_bytes")]
    id: Vec<u8>,
}

impl TransactionId {
    /// Returns the UTF-8 representation of the transaction ID.
    /// This might return [None] if the sender is not using a text string.
    pub fn as_str(&self) -> Option<&str> {
        str::from_utf8(&self.id).ok()
    }

    /// Returns the BigEndian representation of the transaction ID.
    pub fn as_u32(&self) -> u32 {
        let mut bytes = [0u8; 4];
        let len = self.id.len().min(4);
        bytes[..len].copy_from_slice(&self.id[..len]);
        u32::from_be_bytes(bytes)
    }
}

impl From<&[u8]> for TransactionId {
    fn from(bytes: &[u8]) -> Self {
        Self { id: bytes.to_vec() }
    }
}

impl From<Vec<u8>> for TransactionId {
    fn from(bytes: Vec<u8>) -> Self {
        Self { id: bytes }
    }
}

impl Display for TransactionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(s) = self.as_str() {
            f.write_str(s)
        } else {
            write!(f, "{}", self.as_u32())
        }
    }
}

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
    /// BEP33 - The responding node should try to fill the values list with non-seed items on a best-effort basis.
    #[serde(
        default,
        skip_serializing_if = "std::ops::Not::not",
        with = "serde_int_bool"
    )]
    pub no_seed: bool,
    /// BEP33 - The responding node has database entries for that info hash,
    /// then it must add two fields to the "r" dictionary in the response.
    #[serde(
        default,
        skip_serializing_if = "std::ops::Not::not",
        with = "serde_int_bool"
    )]
    pub scrape: bool,
    #[serde(default, skip_serializing_if = "WantFamily::is_none")]
    pub want: WantFamily,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetPeersResponse {
    pub id: NodeId,
    /// The name of the torrent.
    #[serde(default, rename = "n", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The token for announcing a peer.
    /// If a node does not return a token, it indicates that it currently cannot accept announces for this info hash.
    #[serde(default, with = "serde_bytes", skip_serializing_if = "Option::is_none")]
    pub token: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<CompactIpAddr>,
    #[serde(default, skip_serializing_if = "CompactIPv4Nodes::is_empty")]
    pub nodes: CompactIPv4Nodes,
    #[serde(default, skip_serializing_if = "CompactIPv6Nodes::is_empty")]
    pub nodes6: CompactIPv6Nodes,
    /// BEP33 - Bloom Filter representing all stored peers for the info hash.
    #[serde(default, rename = "BFpe", skip_serializing_if = "Option::is_none")]
    pub downloaders: Option<String>,
    /// BEP33 - Bloom Filter representing all stored seeds for the info hash.
    #[serde(default, rename = "BFsd", skip_serializing_if = "Option::is_none")]
    pub seeds: Option<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct AnnouncePeerRequest {
    pub id: NodeId,
    /// Indicates if the `port` field should be ignored and the source port of the packet should be used instead.
    #[serde(with = "serde_int_bool")]
    pub implied_port: bool,
    pub info_hash: InfoHash,
    pub port: u16,
    #[serde(with = "serde_bytes")]
    pub token: Vec<u8>,
    /// The name of the torrent, if provided
    #[serde(default, rename = "n", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// BEP33 - The requesting node is seeding the torrent it announces
    #[serde(
        default,
        skip_serializing_if = "std::ops::Not::not",
        with = "serde_int_bool"
    )]
    pub seed: bool,
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

impl TryFrom<(u16, String)> for ErrorMessage {
    type Error = Error;

    fn try_from(value: (u16, String)) -> Result<Self> {
        match value.0 {
            201 => Ok(ErrorMessage::Generic(value.1)),
            202 => Ok(ErrorMessage::Server(value.1)),
            203 => Ok(ErrorMessage::Protocol(value.1)),
            204 => Ok(ErrorMessage::Method(value.1)),
            _ => Err(Error::InvalidMessage(format!(
                "unknown error code {}",
                value.0
            ))),
        }
    }
}

impl Serialize for ErrorMessage {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (self.code(), self.description()).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ErrorMessage {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ErrorMessageVisitor;
        impl<'de> de::Visitor<'de> for ErrorMessageVisitor {
            type Value = ErrorMessage;

            fn expecting(&self, f: &mut Formatter) -> std::fmt::Result {
                write!(f, "expected a sequence of error code and error message")
            }

            fn visit_seq<A>(self, mut seq: A) -> result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let code: u16 = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let msg: String = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;

                Self::Value::try_from((code, msg)).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_any(ErrorMessageVisitor)
    }
}

/// The response payload of a message.
/// This is either a raw bencode value or a response message.
#[derive(Debug, PartialEq)]
pub enum ResponsePayload {
    Raw {
        id: Option<NodeId>,
        value: serde_bencode::value::Value,
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
        match self {
            ResponsePayload::Message(e) => Ok(e.clone()),
            ResponsePayload::Raw { value, .. } => Self::try_parse(query_name, value),
        }
    }

    fn try_parse(query_name: &str, value: &serde_bencode::value::Value) -> Result<ResponseMessage> {
        match query_name {
            MESSAGE_PING => Ok(ResponseMessage::Ping {
                response: decode_bencode_value(value)
                    .map_err(|e| Self::parse_error(query_name, e))?,
            }),
            MESSAGE_FIND_NODE => Ok(ResponseMessage::FindNode {
                response: decode_bencode_value(value)
                    .map_err(|e| Self::parse_error(query_name, e))?,
            }),
            MESSAGE_GET_PEERS => Ok(ResponseMessage::GetPeers {
                response: decode_bencode_value(value)
                    .map_err(|e| Self::parse_error(query_name, e))?,
            }),
            MESSAGE_ANNOUNCE => Ok(ResponseMessage::Announce {
                response: decode_bencode_value(value)
                    .map_err(|e| Self::parse_error(query_name, e))?,
            }),
            MESSAGE_SAMPLE_INFO_HASHES => {
                match decode_bencode_value::<SampleInfoHashesResponse, serde_bencode::Error>(value)
                {
                    Ok(e) => Ok(ResponseMessage::SampleInfoHashes { response: e }),
                    Err(_) => {
                        // not all nodes support this query and will handle it as a `find_node`
                        // so we'll retry again for `find_node`
                        Self::try_parse(MESSAGE_FIND_NODE, value)
                    }
                }
            }
            _ => Err(Error::InvalidMessage(format!(
                "unable to parse response payload, unknown query \"{}\"",
                query_name
            ))),
        }
    }

    fn parse_error(query_name: &str, err: serde_bencode::Error) -> Error {
        Error::Parse(format!(
            "failed to parse response \"{}\", {}",
            query_name, err
        ))
    }

    fn from_value(value: serde_bencode::value::Value) -> Self {
        let id = match &value {
            serde_bencode::value::Value::Dict(dict) => {
                dict.get("id".as_bytes()).and_then(|v| match v {
                    serde_bencode::value::Value::Bytes(bytes) => {
                        NodeId::try_from(bytes.as_slice()).ok()
                    }
                    _ => None,
                })
            }
            _ => None,
        };

        Self::Raw { id, value }
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
        serde_bencode::value::Value::deserialize(deserializer).map(Self::from_value)
    }
}

/// The payload data of a message.
#[derive(Debug, PartialEq, Serialize)]
#[serde(tag = "y")]
pub enum MessagePayload {
    #[serde(rename = "q")]
    Query(QueryMessage),
    #[serde(rename = "r")]
    Response(ResponsePayload),
    #[serde(rename = "e")]
    Error {
        #[serde(rename = "e")]
        error: ErrorMessage,
    },
}

impl MessagePayload {
    pub fn error(error: ErrorMessage) -> Self {
        Self::Error { error }
    }
}

impl<'de> Deserialize<'de> for MessagePayload {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MessagePayloadVisitor;
        impl<'de> de::Visitor<'de> for MessagePayloadVisitor {
            type Value = MessagePayload;

            fn expecting(&self, f: &mut Formatter) -> std::fmt::Result {
                write!(f, "expected a dict containing the message payload")
            }

            fn visit_map<A>(self, mut map: A) -> result::Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut message_type: Option<Cow<'de, str>> = None;
                let mut query_type: Option<Cow<'de, str>> = None;
                let mut query_args: Option<serde_bencode::value::Value> = None;
                let mut response: Option<ResponsePayload> = None;
                let mut error: Option<ErrorMessage> = None;

                while let Some(key) = map.next_key::<Cow<'de, str>>()? {
                    match key.as_ref() {
                        "y" => {
                            message_type = Some(map.next_value()?);
                        }
                        "q" => {
                            query_type = Some(map.next_value()?);
                        }
                        "a" => {
                            query_args = Some(map.next_value()?);
                        }
                        "r" => {
                            response = Some(map.next_value()?);
                        }
                        "e" => {
                            error = Some(map.next_value()?);
                        }
                        _ => {
                            let _ = map.next_value::<IgnoredAny>()?;
                        }
                    }
                }

                let message_type = message_type.ok_or_else(|| de::Error::missing_field("y"))?;
                match message_type.as_ref() {
                    "q" => {
                        let query_type = query_type.ok_or_else(|| de::Error::missing_field("q"))?;
                        let query_args = query_args.ok_or_else(|| de::Error::missing_field("a"))?;

                        let query = match query_type.as_ref() {
                            "ping" => {
                                let request = decode_bencode_value(&query_args)?;
                                QueryMessage::Ping { request }
                            }
                            "find_node" => {
                                let request = decode_bencode_value(&query_args)?;
                                QueryMessage::FindNode { request }
                            }
                            "get_peers" => {
                                let request = decode_bencode_value(&query_args)?;
                                QueryMessage::GetPeers { request }
                            }
                            "announce_peer" => {
                                let request = decode_bencode_value(&query_args)?;
                                QueryMessage::AnnouncePeer { request }
                            }
                            // BEP51: "sample_infohashes"
                            "sample_infohashes" => {
                                let request = decode_bencode_value(&query_args)?;
                                QueryMessage::SampleInfoHashes { request }
                            }
                            _ => {
                                return Err(de::Error::unknown_variant(
                                    query_type.as_ref(),
                                    &[
                                        "ping",
                                        "find_node",
                                        "get_peers",
                                        "announce",
                                        "sample_info_hashes",
                                    ],
                                ))
                            }
                        };

                        Ok(MessagePayload::Query(query))
                    }
                    "r" => {
                        let response = response.ok_or_else(|| de::Error::missing_field("r"))?;
                        Ok(MessagePayload::Response(response))
                    }
                    "e" => {
                        let error = error.ok_or_else(|| de::Error::missing_field("e"))?;
                        Ok(MessagePayload::error(error))
                    }
                    _ => Err(de::Error::unknown_variant(
                        message_type.as_ref(),
                        &["q", "r", "e"],
                    )),
                }
            }
        }

        deserializer.deserialize_any(MessagePayloadVisitor)
    }
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
    #[serde(rename = "t")]
    pub transaction_id: TransactionId,
    #[serde(default, rename = "v", skip_serializing_if = "Option::is_none")]
    pub version: Option<Version>,
    #[serde(flatten)]
    pub payload: MessagePayload,
    /// The node's external IP.
    /// See BEP42 for more info.
    #[serde(default, rename = "ip", skip_serializing_if = "Option::is_none")]
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
            MessagePayload::Error { .. } => None,
        }
    }

    /// Returns the UTF-8 representation of the transaction ID.
    /// This might return [None] if the sender is not using a text string.
    pub fn transaction_id_as_str(&self) -> Option<&str> {
        self.transaction_id.as_str()
    }

    /// Returns the BigEndian representation of the transaction ID.
    pub fn transaction_id_as_u32(&self) -> u32 {
        self.transaction_id.as_u32()
    }
}

#[derive(Debug, Default)]
pub(crate) struct MessageBuilder {
    transaction_id: Option<TransactionId>,
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
    pub fn transaction_id(&mut self, id: TransactionId) -> &mut Self {
        self.transaction_id = Some(id);
        self
    }

    /// Set the underlying transaction ID bytes.
    /// This is useful for testing purposes.
    #[cfg(test)]
    fn transaction_id_bytes<T: AsRef<[u8]>>(&mut self, id: T) -> &mut Self {
        self.transaction_id = Some(id.as_ref().into());
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
        let transaction_id_bytes = self
            .transaction_id
            .take()
            .ok_or(Error::InvalidMessage("missing transaction id".to_string()))?;

        Ok(Message {
            transaction_id: transaction_id_bytes,
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

/// Decode the given bencode Value into a concrete type.
fn decode_bencode_value<T, E>(value: &serde_bencode::value::Value) -> result::Result<T, E>
where
    T: DeserializeOwned,
    E: de::Error,
{
    let bytes = serde_bencode::to_bytes(value).map_err(E::custom)?;
    serde_bencode::from_bytes(&bytes).map_err(E::custom)
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

/// Serialize a boolean as an integer (0 or 1).
mod serde_int_bool {
    use super::*;
    use serde::de::Visitor;

    pub fn serialize<S>(value: &bool, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(i64::from(*value))
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

            fn visit_bool<E>(self, v: bool) -> result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(v)
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

    mod transaction_id {
        use super::*;

        #[test]
        fn test_as_str() {
            let id = format!("{:02x}", 1);

            let transaction_id = TransactionId::from(id.as_bytes());

            assert_eq!(transaction_id.as_str(), Some(id.as_str()));
        }

        #[test]
        fn test_as_u32() {
            let id = format!("{:02x}", 1).as_bytes().to_vec();
            let mut bytes = [0u8; 4];
            bytes[..id.len()].copy_from_slice(&id[..id.len()]);
            let expected_result = u32::from_be_bytes(bytes);

            let transaction_id = TransactionId::from(id.clone());

            assert_eq!(transaction_id.as_u32(), expected_result);
        }
    }

    mod ping {
        use super::*;
        use serde::de::Error;

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
            let result = serde_bencode::from_bytes::<Message>(payload.as_bytes())
                .expect("expected a valid message");
            assert_eq!(expected_result, result);

            // serialize the payload and compare it with the original payload
            let result = serde_bencode::to_bytes(&result)
                .and_then(|e| String::from_utf8(e).map_err(|e| serde_bencode::Error::custom(e)))
                .unwrap();
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
                serde_bencode::from_bytes::<Message>(payload.as_bytes()).unwrap(),
                MESSAGE_PING,
            );
            assert_eq!(Some(&node_id), result.id(), "expected the node id to match");
            assert_eq!(expected_result, result);

            // serialize the payload and compare it with the original payload
            let result = serde_bencode::to_bytes(&result)
                .and_then(|e| String::from_utf8(e).map_err(|e| serde_bencode::Error::custom(e)))
                .unwrap();
            assert_eq!(payload, result.as_str());
        }
    }

    mod find_node {
        use super::*;
        use crate::dht::compact::CompactIPv4Node;
        use crate::CompactIpv4Addr;
        use serde::de::Error;
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
            let result = serde_bencode::from_bytes::<Message>(payload.as_bytes())
                .expect("expected a valid message");
            assert_eq!(expected_result, result);

            // serialize the payload and compare it with the original payload
            let result = serde_bencode::to_bytes(&result)
                .and_then(|e| String::from_utf8(e).map_err(|e| serde_bencode::Error::custom(e)))
                .unwrap();
            assert_eq!(payload, result.as_str());
        }

        #[test]
        fn test_response() {
            // as compact address cannot be printed as UTF8 strings,
            // we're going to use hex representation instead
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
            let bytes = serde_bencode::to_bytes(&expected_result).unwrap();
            let result = deserialize_response(
                serde_bencode::from_bytes::<Message>(bytes.as_slice()).unwrap(),
                MESSAGE_FIND_NODE,
            );
            assert_eq!(expected_result, result);

            // serialize the payload and compare it with the original payload
            let result = serde_bencode::to_bytes(&result).unwrap();
            assert_eq!(bytes, result);
        }
    }

    mod announce_peer {
        use super::*;
        use serde::de::Error;

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
                        token: "aoeusnth".as_bytes().to_vec(),
                        name: None,
                        seed: false,
                    },
                }))
                .build()
                .unwrap();

            // deserialize the payload
            let result = serde_bencode::from_bytes::<Message>(payload.as_bytes())
                .expect("expected a valid message");
            assert_eq!(expected_result, result);

            // serialize the payload and compare it with the original payload
            let result = serde_bencode::to_bytes(&result)
                .and_then(|e| String::from_utf8(e).map_err(|e| serde_bencode::Error::custom(e)))
                .unwrap();
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
            let result = serde_bencode::to_bytes(&result)
                .and_then(|e| String::from_utf8(e).map_err(|e| serde_bencode::Error::custom(e)))
                .unwrap();
            assert_eq!(payload, result.as_str());
        }
    }

    mod sample_infohashes {
        use super::*;
        use serde::de::Error;

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
            let result = serde_bencode::from_bytes::<Message>(payload.as_bytes())
                .expect("expected a valid message");
            assert_eq!(expected_result, result);

            // serialize the payload and compare it with the original payload
            let result = serde_bencode::to_bytes(&result)
                .and_then(|e| String::from_utf8(e).map_err(|e| serde_bencode::Error::custom(e)))
                .unwrap();
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
                .payload(MessagePayload::error(ErrorMessage::Generic(
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
            let payload = "d1:eli201e24:A Generic Error Occurrede1:t2:aa1:y1:ee";
            let expected_result = Message::builder()
                .transaction_id_bytes("aa".as_bytes())
                .payload(MessagePayload::error(ErrorMessage::Generic(
                    "A Generic Error Occurred".to_string(),
                )))
                .build()
                .unwrap();

            let result =
                serde_bencode::from_str::<Message>(payload).expect("expected a valid message");

            assert_eq!(expected_result, result);
        }

        #[test]
        fn test_error_message_deserialize() {
            let payload = "d1:eli201e24:A Generic Error Occurrede1:t2:aa1:y1:ee";
            let result = serde_bencode::from_str::<Message>(payload).unwrap();
            let expected_result = Message::builder()
                .transaction_id_bytes("aa".as_bytes())
                .payload(MessagePayload::error(ErrorMessage::Generic(
                    "A Generic Error Occurred".to_string(),
                )))
                .build()
                .unwrap();
            assert_eq!(expected_result, result);

            let payload = "d1:eli202e14:A Server Errore1:t2:aa1:y1:ee";
            let result = serde_bencode::from_str::<Message>(payload).unwrap();
            let expected_result = Message::builder()
                .transaction_id_bytes("aa".as_bytes())
                .payload(MessagePayload::error(ErrorMessage::Server(
                    "A Server Error".to_string(),
                )))
                .build()
                .unwrap();
            assert_eq!(expected_result, result);

            let payload = "d1:eli203e16:A Protocol Errore1:t2:bb1:y1:ee";
            let result = serde_bencode::from_str::<Message>(payload).unwrap();
            let expected_result = Message::builder()
                .transaction_id_bytes("bb".as_bytes())
                .payload(MessagePayload::error(ErrorMessage::Protocol(
                    "A Protocol Error".to_string(),
                )))
                .build()
                .unwrap();
            assert_eq!(expected_result, result);

            let payload = "d1:eli204e14:Method Unknowne1:t2:bb1:y1:ee";
            let result = serde_bencode::from_str::<Message>(payload).unwrap();
            let expected_result = Message::builder()
                .transaction_id_bytes("bb".as_bytes())
                .payload(MessagePayload::error(ErrorMessage::Method(
                    "Method Unknown".to_string(),
                )))
                .build()
                .unwrap();
            assert_eq!(expected_result, result);
        }

        #[test]
        fn test_code() {
            let error = ErrorMessage::Generic("A Generic Error Occurred".to_string());
            assert_eq!(201, error.code());

            let error = ErrorMessage::Server("A Server Error".to_string());
            assert_eq!(202, error.code());

            let error = ErrorMessage::Protocol("A Protocol Error".to_string());
            assert_eq!(203, error.code());

            let error = ErrorMessage::Method("Method Unknown".to_string());
            assert_eq!(204, error.code());
        }

        #[test]
        fn test_description() {
            let error = ErrorMessage::Generic("A Generic Error Occurred".to_string());
            assert_eq!("A Generic Error Occurred", error.description());

            let error = ErrorMessage::Server("A Server Error".to_string());
            assert_eq!("A Server Error", error.description());

            let error = ErrorMessage::Protocol("A Protocol Error".to_string());
            assert_eq!("A Protocol Error", error.description());

            let error = ErrorMessage::Method("Method Unknown".to_string());
            assert_eq!("Method Unknown", error.description());
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
