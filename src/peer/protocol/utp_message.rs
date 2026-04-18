use crate::peer::protocol::{CloseReason, ConnectionId, SequenceNumber};
use crate::peer::protocol::{Extension, Packet, StateType};
use crate::peer::Error;
use std::fmt::{Debug, Formatter};

/// A parsed uTP message.
#[derive(Clone, PartialEq)]
pub enum UtpMessage {
    /// Connect to the utp peer with the connection id
    Connect { connection: ConnectionId },
    /// The latest known state of an uTP peer with `sequence_number` & `acknowledge_number`.
    State(ConnectionId, SequenceNumber, SequenceNumber),
    /// Message containing data information
    Data {
        connection: ConnectionId,
        payload: Vec<u8>,
    },
    /// Terminate the connection forcefully.
    Terminate {
        connection: ConnectionId,
        reason: CloseReason,
    },
    /// Close the connection
    Close {
        connection: ConnectionId,
        reason: CloseReason,
    },
}

impl UtpMessage {
    /// Convert this message into an uTP packet.
    pub fn into_packet(
        self,
        sequence_number: SequenceNumber,
        acknowledge_number: SequenceNumber,
        timestamp_microseconds: u32,
        timestamp_difference_microseconds: u32,
        window_size: u32,
    ) -> Packet {
        match self {
            UtpMessage::Connect { connection } => Packet {
                state_type: StateType::Syn,
                extension: Extension::None,
                connection_id: connection,
                timestamp_microseconds,
                timestamp_difference_microseconds,
                window_size,
                sequence_number,
                acknowledge_number,
                payload: Vec::with_capacity(0),
            },
            UtpMessage::State(connection_id, seq_number, ack_number) => Packet {
                state_type: StateType::State,
                extension: Extension::None,
                connection_id,
                timestamp_microseconds,
                timestamp_difference_microseconds,
                window_size,
                sequence_number: seq_number,
                acknowledge_number: ack_number,
                payload: Vec::with_capacity(0),
            },
            UtpMessage::Data {
                connection,
                payload,
            } => Packet {
                state_type: StateType::Data,
                extension: Extension::None,
                connection_id: connection,
                timestamp_microseconds,
                timestamp_difference_microseconds,
                window_size,
                sequence_number,
                acknowledge_number,
                payload,
            },
            UtpMessage::Terminate { connection, reason } => Packet {
                state_type: StateType::Reset,
                extension: Extension::CloseReason { reason },
                connection_id: connection,
                timestamp_microseconds,
                timestamp_difference_microseconds,
                window_size,
                sequence_number,
                acknowledge_number,
                payload: vec![],
            },
            UtpMessage::Close { connection, reason } => Packet {
                state_type: StateType::Fin,
                extension: Extension::CloseReason { reason },
                connection_id: connection,
                timestamp_microseconds,
                timestamp_difference_microseconds,
                window_size,
                sequence_number,
                acknowledge_number,
                payload: vec![],
            },
        }
    }
}

impl TryFrom<&Packet> for UtpMessage {
    type Error = Error;

    fn try_from(value: &Packet) -> crate::peer::Result<Self> {
        match value.state_type {
            StateType::Syn => Ok(UtpMessage::Connect {
                connection: value.connection_id,
            }),
            StateType::State => Ok(UtpMessage::State(
                value.connection_id,
                value.sequence_number,
                value.acknowledge_number,
            )),
            StateType::Data => Ok(UtpMessage::Data {
                connection: value.connection_id,
                payload: value.payload.clone(),
            }),
            StateType::Fin => Ok(UtpMessage::Close {
                connection: value.connection_id,
                reason: CloseReason::None,
            }),
            StateType::Reset => Ok(UtpMessage::Terminate {
                connection: value.connection_id,
                reason: CloseReason::None,
            }),
        }
    }
}

impl From<&UtpMessage> for StateType {
    fn from(value: &UtpMessage) -> Self {
        match value {
            UtpMessage::Connect { .. } => StateType::Syn,
            UtpMessage::State(_, _, _) => StateType::State,
            UtpMessage::Data { .. } => StateType::Data,
            UtpMessage::Terminate { .. } => StateType::Reset,
            UtpMessage::Close { .. } => StateType::Fin,
        }
    }
}

impl Debug for UtpMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UtpMessage::Connect { connection } => {
                write!(f, "Connect{{ connection: {} }}", connection)
            }
            UtpMessage::State(id, seq, ack) => write!(f, "State({}, {}, {})", id, seq, ack),
            UtpMessage::Data {
                connection,
                payload,
            } => write!(
                f,
                "Data{{ connection: {}, payload: {} }}",
                connection,
                payload.len()
            ),
            UtpMessage::Terminate { connection, reason } => write!(
                f,
                "Terminate{{ connection: {}, reason: {:?} }}",
                connection, reason
            ),
            UtpMessage::Close { connection, reason } => write!(
                f,
                "Close{{ connection: {}, reason: {:?} }}",
                connection, reason
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_type_from_message() {
        let connection_id = 0;

        let message = UtpMessage::Connect {
            connection: connection_id,
        };
        let result = StateType::from(&message);
        assert_eq!(StateType::Syn, result);

        let message = UtpMessage::State(connection_id, 0, 0);
        let result = StateType::from(&message);
        assert_eq!(StateType::State, result);

        let message = UtpMessage::Data {
            connection: connection_id,
            payload: vec![],
        };
        let result = StateType::from(&message);
        assert_eq!(StateType::Data, result);

        let message = UtpMessage::Terminate {
            connection: connection_id,
            reason: CloseReason::None,
        };
        let result = StateType::from(&message);
        assert_eq!(StateType::Reset, result);

        let message = UtpMessage::Close {
            connection: connection_id,
            reason: CloseReason::None,
        };
        let result = StateType::from(&message);
        assert_eq!(StateType::Fin, result);
    }
}
