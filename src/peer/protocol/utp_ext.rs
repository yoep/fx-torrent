use crate::peer::protocol::utp_packet::{Extension, Packet};
use crate::peer::protocol::utp_stream::UtpStreamContext;
use log::warn;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

/// The selective acks extension for the uTP socket connection.
/// This allows non-sequentially ack packets.
#[derive(Debug)]
pub struct UtpSelectiveAck;

impl UtpSelectiveAck {
    pub async fn incoming(&self, packet: &mut Packet, stream: &UtpStreamContext) {
        match packet.extension {
            Extension::SelectiveAck { .. } => {
                // TODO
                warn!(
                    "Utp stream {} selective acks extensions not yet implemented",
                    stream
                );
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone)]
pub struct UtpPacketCapture {
    inner: Arc<InnerUtpPacketCapture>,
}

impl UtpPacketCapture {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(InnerUtpPacketCapture {
                incoming_packets: Default::default(),
                outgoing_packets: Default::default(),
            }),
        }
    }

    pub async fn incoming_packets(&self) -> MutexGuard<'_, Vec<Packet>> {
        self.inner.incoming_packets.lock().await
    }

    pub async fn outgoing_packets(&self) -> MutexGuard<'_, Vec<Packet>> {
        self.inner.outgoing_packets.lock().await
    }

    /// Store an incoming uTP packet on the socket.
    pub async fn incoming(&self, packet: &mut Packet) {
        self.inner
            .incoming_packets
            .lock()
            .await
            .push(packet.clone());
    }

    /// Store an outgoing uTP packet from the socket.
    pub async fn outgoing(&self, packet: &mut Packet) {
        self.inner
            .outgoing_packets
            .lock()
            .await
            .push(packet.clone());
    }
}

#[derive(Debug)]
struct InnerUtpPacketCapture {
    incoming_packets: Mutex<Vec<Packet>>,
    outgoing_packets: Mutex<Vec<Packet>>,
}
