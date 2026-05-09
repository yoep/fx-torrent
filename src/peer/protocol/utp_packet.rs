use crate::peer::protocol::{CloseReason, ConnectionId};
use crate::peer::{Error, Result};
use bit_vec::BitVec;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use log::debug;
use std::fmt::{Debug, Formatter};
use std::io;
use std::io::{Cursor, Read, Write};

/// The uTP packet header len in bytes.
pub const UTP_HEADER_SIZE: usize = 20;

/// An uTP packet to be sent or received by uTP sockets & connections.
/// See BEP29 for more information.
#[derive(Clone, PartialEq)]
pub struct Packet {
    /// The packet type
    pub state_type: StateType,
    /// The uTP packet extension
    pub extension: Extension,
    /// Unique connection identifier of the stream to which the packet belongs
    pub connection_id: ConnectionId,
    /// The timestamp of when this packet was sent
    pub timestamp_microseconds: u32,
    /// The difference between the local time and the timestamp in the last received packet
    pub timestamp_difference_microseconds: u32,
    /// The number of bytes in-flight that have not been acked yet
    pub window_size: u32,
    /// The packet sequence number
    pub sequence_number: u16,
    /// The sequence number of the last received packet
    pub acknowledge_number: u16,
    /// The payload of the packet.
    pub payload: Vec<u8>,
}

impl Packet {
    /// Convert the packet into the uTP protocol wire bytes.
    pub fn as_bytes(&self) -> Result<Vec<u8>> {
        let mut buffer = vec![0u8; 1];

        // write the type & version into the first byte
        buffer[0] = (self.state_type as u8) << 4 | 1;
        // write the extension bytes
        buffer.write_u8(self.extension.as_u8())?;
        // write the connection number
        buffer.write_u16::<BigEndian>(self.connection_id)?;
        // write the current timestamp
        buffer.write_u32::<BigEndian>(self.timestamp_microseconds)?;
        // write the timestamp difference
        buffer.write_u32::<BigEndian>(self.timestamp_difference_microseconds)?;
        // write the current in-flight window size
        buffer.write_u32::<BigEndian>(self.window_size)?;
        // write the sequence number
        buffer.write_u16::<BigEndian>(self.sequence_number)?;
        // write the acknowledgment number
        buffer.write_u16::<BigEndian>(self.acknowledge_number)?;
        // write the extension bytes right after the header
        buffer.write_all(&self.extension.as_bytes()?)?;
        // append the payload
        buffer.write_all(self.payload.as_slice())?;

        Ok(buffer)
    }
}

impl TryFrom<&[u8]> for Packet {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(value);

        // start by reading the version from the first byte
        let byte = cursor.read_u8()?;
        let version = byte & 0x0f;

        // if the version doesn't match v1, we reject the packet
        if version != 1 {
            return Err(Error::UnsupportedVersion(version as u32));
        }

        let state_type_value = byte >> 4;
        let state_type = StateType::try_from(state_type_value)?;
        // read the extension from the second byte
        let extension_number = cursor.read_u8()?;
        let connection_id = cursor.read_u16::<BigEndian>()?;
        let timestamp_microseconds = cursor.read_u32::<BigEndian>()?;
        let timestamp_difference_microseconds = cursor.read_u32::<BigEndian>()?;
        let window_size = cursor.read_u32::<BigEndian>()?;
        let sequence_number = cursor.read_u16::<BigEndian>()?;
        let acknowledge_number = cursor.read_u16::<BigEndian>()?;
        // read the extensions, if the extension_number is not 0
        let extension = Extension::read(extension_number, &mut cursor)?;
        // read the remaining bytes as payload
        let mut payload = Vec::new();
        cursor.read_to_end(&mut payload)?;

        Ok(Self {
            state_type,
            extension,
            connection_id,
            timestamp_microseconds,
            timestamp_difference_microseconds,
            window_size,
            sequence_number,
            acknowledge_number,
            payload,
        })
    }
}

impl Debug for Packet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Packet")
            .field("state_type", &self.state_type)
            .field("extension", &self.extension)
            .field("connection_id", &self.connection_id)
            .field("timestamp_microseconds", &self.timestamp_microseconds)
            .field(
                "timestamp_difference_microseconds",
                &self.timestamp_difference_microseconds,
            )
            .field("window_size", &self.window_size)
            .field("sequence_number", &self.sequence_number)
            .field("acknowledge_number", &self.acknowledge_number)
            .field("payload", &self.payload.len())
            .finish()
    }
}

/// The state type of UTP packets.
/// See BEP29 for more info about the states of packets.
#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum StateType {
    /// Regular data packet type
    Data = 0,
    /// Finalize the connection
    Fin = 1,
    /// State packet
    State = 2,
    /// Terminate the connection forcefully
    Reset = 3,
    /// Initiate a connection
    Syn = 4,
}

impl TryFrom<u8> for StateType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(StateType::Data),
            1 => Ok(StateType::Fin),
            2 => Ok(StateType::State),
            3 => Ok(StateType::Reset),
            4 => Ok(StateType::Syn),
            _ => Err(Error::UnsupportedMessage(value)),
        }
    }
}

/// The extensions of an uTP packet.
#[derive(Debug, Clone, PartialEq)]
pub enum Extension {
    None,
    SelectiveAck { bitmask: BitVec },
    CloseReason { reason: CloseReason },
}

impl Extension {
    /// Returns the extension number of the extension.
    /// See BEP29 for more info about extensions.
    pub fn as_u8(&self) -> u8 {
        match self {
            Extension::None => 0,
            Extension::SelectiveAck { .. } => 1,
            Extension::CloseReason { .. } => 3,
        }
    }

    /// Returns the extension as bytes.
    pub fn as_bytes(&self) -> Result<Vec<u8>> {
        let extension_payload = match self {
            Extension::None => return Ok(vec![]),
            Extension::SelectiveAck { bitmask } => bitmask.to_bytes(),
            Extension::CloseReason { reason } => {
                let mut bytes = vec![0u8; 4];
                let reason_bytes: [u8; 2] = (*reason as u16).to_be_bytes();
                // write the close reason in the last 2 bytes
                bytes[2] = reason_bytes[0];
                bytes[3] = reason_bytes[1];
                bytes
            }
        };

        let len = extension_payload.len();
        let mut bytes = vec![0u8; len + 2];

        // as we only support 1 extension (not a list)
        // write the next extension as the termination byte 0
        bytes[0] = 0;

        if !extension_payload.is_empty() {
            // write the extension payload length
            bytes[1] = len as u8;
            // write the extension payload
            bytes[2..2 + len].copy_from_slice(&extension_payload);
        }

        Ok(bytes)
    }

    /// Try to read an extension from the given cursor being parsed by a [Packet].
    fn read(mut extension_nr: u8, cursor: &mut Cursor<&[u8]>) -> Result<Self> {
        let mut extension = Extension::None;

        // loop until the end of the extension list is reached
        while extension_nr != 0 {
            let next_extension = cursor.read_u8().map_err(|_| {
                Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "missing extension byte",
                ))
            })?;
            let extension_len = cursor.read_u8().map_err(|_| {
                Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "missing extension length byte",
                ))
            })?;

            // try to read the bytes from the cursor
            let mut bytes = vec![0u8; extension_len as usize];
            cursor.read_exact(&mut bytes).map_err(|e| {
                Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("failed to read {} extension bytes, {}", extension_len, e),
                ))
            })?;

            match extension_nr {
                1 => {
                    extension = Extension::SelectiveAck {
                        bitmask: BitVec::from_bytes(&bytes),
                    }
                }
                3 => {
                    // check if the len is 4 bytes
                    if extension_len != 4 {
                        return Err(Error::Io(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!("expected 4 bytes, but got {}", extension_len),
                        )));
                    }
                    // skip the 2 reserved bytes
                    bytes = bytes.split_off(2);

                    let bytes: [u8; 2] = bytes.try_into().map_err(|e: Vec<u8>| {
                        Error::Parsing(format!(
                            "failed to parse close reason, got {} bytes",
                            e.len()
                        ))
                    })?;
                    let reason = u16::from_be_bytes(bytes);

                    extension = Extension::CloseReason {
                        reason: CloseReason::try_from(reason)?,
                    }
                }
                _ => {
                    // log but ignore the unknown extension number
                    debug!("Utp extension {} is currently not supported", extension_nr);
                }
            }

            extension_nr = next_extension
        }

        Ok(extension)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod packet {
        use super::*;

        #[test]
        fn test_deserialize() {
            let packet = Packet {
                state_type: StateType::Syn,
                extension: Extension::None,
                connection_id: 12345,
                timestamp_microseconds: 1234567890,
                timestamp_difference_microseconds: 13,
                window_size: 1024,
                sequence_number: 888,
                acknowledge_number: 12,
                payload: vec![],
            };

            let bytes = packet
                .as_bytes()
                .expect("expected the packet to have been serialized");
            let result = Packet::try_from(bytes.as_slice())
                .expect("expected the packet to have been deserialized");

            assert_eq!(packet, result);
        }

        #[test]
        fn test_deserialize_payload() {
            let payload = "Lorem ipsum dolor esta";
            let packet = Packet {
                state_type: StateType::Data,
                extension: Extension::None,
                connection_id: 1313,
                timestamp_microseconds: 1234567890,
                timestamp_difference_microseconds: 666,
                window_size: 1024,
                sequence_number: 11,
                acknowledge_number: 2,
                payload: payload.as_bytes().to_vec(),
            };

            let bytes = packet
                .as_bytes()
                .expect("expected the packet to have been serialized");
            let result = Packet::try_from(bytes.as_slice())
                .expect("expected the packet to have been deserialized");
            assert_eq!(packet, result);

            let result =
                String::from_utf8(result.payload).expect("expected the payload to be valid utf-8");
            assert_eq!(payload, result);
        }

        #[test]
        fn test_deserialize_extension() {
            let packet = Packet {
                state_type: StateType::Data,
                extension: Extension::SelectiveAck {
                    bitmask: BitVec::from_bytes(&[0b10100011]),
                },
                connection_id: 1,
                timestamp_microseconds: 2,
                timestamp_difference_microseconds: 3,
                window_size: 1024,
                sequence_number: 11,
                acknowledge_number: 2,
                payload: vec![],
            };

            let bytes = packet
                .as_bytes()
                .expect("expected the packet to have been serialized");
            let result = Packet::try_from(bytes.as_slice())
                .expect("expected the packet to have been deserialized");

            assert_eq!(packet, result);
        }
    }

    mod extension {
        use super::*;

        #[test]
        fn test_deserialize() {
            let extension = Extension::SelectiveAck {
                bitmask: BitVec::from_bytes(&[0b10100000]),
            };

            let bytes = extension
                .as_bytes()
                .expect("expected the extension to have been serialized");
            let mut cursor = Cursor::new(bytes.as_slice());
            let result =
                Extension::read(1, &mut cursor).expect("expected the extension to be valid");

            assert_eq!(extension, result);
        }

        #[test]
        fn test_extension_close_reason() {
            let extension = Extension::CloseReason {
                reason: CloseReason::NoMemory,
            };

            let bytes = extension
                .as_bytes()
                .expect("expected the extension to have been serialized");
            let mut cursor = Cursor::new(bytes.as_slice());
            let result =
                Extension::read(3, &mut cursor).expect("expected the extension to be valid");

            assert_eq!(extension, result);
        }
    }
}
