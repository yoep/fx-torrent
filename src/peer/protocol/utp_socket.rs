use crate::peer::protocol::{Packet, StateType};
use crate::peer::protocol::{UtpStream, UtpStreamContext, MAX_PACKET_SIZE, UTP_HEADER_SIZE};
use crate::peer::{Error, Result};
use async_trait::async_trait;
use derive_more::Display;
use fx_handle::Handle;
use log::{debug, trace, warn};
use rand::{rng, RngExt};
use std::collections::HashMap;
use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{Mutex, RwLock};
use tokio::{select, time};
use tokio_util::sync::CancellationToken;

/// The UTP socket identifier.
pub type UtpHandle = Handle;
/// The packet connection identifier.
pub type ConnectionId = u16;
/// The packet sequence number.
pub type SequenceNumber = u16;
/// The uTP socket extensions.
pub type UtpSocketExtensions = Vec<Box<dyn UtpSocketExtension>>;

/// The uTorrent transport protocol socket extension.
/// An extension can be used to add additional functionality to the uTP socket packets.
#[async_trait]
pub trait UtpSocketExtension: Debug + Send + Sync {
    /// Handle an incoming uTP packet for the given stream.
    ///
    /// # Arguments
    ///
    /// * `packet` - The packet to handle.
    /// * `stream` - The stream to handle the packet for.
    async fn incoming(&self, packet: &mut Packet, stream: &UtpStreamContext);

    /// Handle an outgoing uTp packet for the given stream.
    /// This is invoked before it's being serialized and sent to the remote peer.
    ///
    /// # Arguments
    ///
    /// * `packet` - The packet to handle.
    /// * `stream` - The stream to handle the packet for.
    async fn outgoing(&self, packet: &mut Packet, stream: &UtpStreamContext);
}

/// A connection identifier of an utp stream.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub(crate) struct UtpConnId {
    pub recv_id: u16,
    pub send_id: u16,
}

impl UtpConnId {
    pub fn new() -> Self {
        let mut thread_ng = rng();
        let mut connection_id_recv: u16;
        let connection_id_send: u16;

        loop {
            connection_id_recv = thread_ng.random();
            if connection_id_recv < u16::MAX {
                connection_id_send = connection_id_recv.saturating_add(1);
                break;
            }
        }

        Self {
            recv_id: connection_id_recv,
            send_id: connection_id_send,
        }
    }
}

/// The uTorrent transport protocol socket which can receive and send packets to multiple peers simultaneously.
#[derive(Debug, Display)]
#[display("{}", inner)]
pub struct UtpSocket {
    inner: Arc<UtpSocketContext>,
}

impl UtpSocket {
    /// Create a new UTP socket on the given address.
    ///
    /// The address should either be a [std::net::Ipv4Addr] or [std::net::Ipv6Addr] local machine address, and never an external address.
    /// If the socket port is no longer available, the UTP socket cannot be created and an error will be returned.
    ///
    /// # Arguments
    ///
    /// * `addr` - The socket address to bind the uTP socket to.
    /// * `timeout` - The connection timeout for the uTP socket.
    /// * `extensions` - The uTP socket extensions to use.
    pub async fn new(
        addr: SocketAddr,
        timeout: Duration,
        extensions: UtpSocketExtensions,
    ) -> Result<Self> {
        let socket = UdpSocket::bind(addr).await?;
        let addr = socket.local_addr()?; // get the bound socket address in case port was 0
        let (incoming_stream_sender, incoming_stream_receiver) = unbounded_channel();
        let id = UtpSocketId {
            handle: Default::default(),
            addr,
        };
        let cancellation_token = CancellationToken::new();
        let inner = Arc::new(UtpSocketContext {
            id,
            addr,
            socket,
            connections: Default::default(),
            incoming_stream_sender,
            incoming_stream_receiver: Mutex::new(incoming_stream_receiver),
            extensions: Arc::new(extensions),
            timeout,
            cancellation_token,
        });

        // start the main loop of the socket a new thread
        let inner_main_loop = inner.clone();
        tokio::spawn(async move { inner_main_loop.run(&inner_main_loop).await });

        Ok(Self { inner })
    }

    /// Get the local socket address of the uTP connection.
    /// It returns the socket address on which the uTP socket is bound.
    pub fn addr(&self) -> SocketAddr {
        self.inner.addr
    }

    /// Try to connect to the given utp target address.
    /// This will establish a new stream with the given address that can be used to send and receive data.
    ///
    /// It returns an error if the connection for the utp socket couldn't be established.
    pub async fn connect(&self, addr: SocketAddr) -> Result<UtpStream> {
        let mut connections = self.inner.connections.write().await;
        let connection_id: UtpConnId;

        // generate a unique connection id
        loop {
            let id = UtpConnId::new();
            if !connections.contains_key(&id) {
                connection_id = id;
                break;
            }
        }

        let (message_sender, message_receiver) = unbounded_channel();
        let stream = UtpStream::new_outgoing(
            connection_id,
            addr,
            self.inner.clone(),
            message_receiver,
            self.inner.extensions(),
        )
        .await?;

        // store the connection
        debug!("Utp socket {} connected with {}", self, addr);
        connections.insert(connection_id, message_sender);

        Ok(stream)
    }

    /// Receive the next incoming uTP stream of the socket.
    /// These streams can only be received by one caller at a time.
    ///
    /// It returns the next uTP stream if available, else [None].
    pub async fn recv(&self) -> Option<UtpStream> {
        let mut receiver = self.inner.incoming_stream_receiver.lock().await;
        select! {
            _ = self.inner.cancellation_token.cancelled() => None,
            stream = receiver.recv() => stream,
        }
    }

    #[cfg(test)]
    pub(crate) fn context(&self) -> &Arc<UtpSocketContext> {
        &self.inner
    }
}

impl Drop for UtpSocket {
    fn drop(&mut self) {
        debug!("Utp socket {} is being dropped", self);
        self.inner.cancellation_token.cancel();
    }
}

#[derive(Debug, Display)]
#[display("{}", id)]
pub(crate) struct UtpSocketContext {
    /// The unique id of the socket
    id: UtpSocketId,
    /// The underlying socket address of the utp socket
    addr: SocketAddr,
    /// The underlying UDP socket being used by the uTP socket.
    socket: UdpSocket,
    /// The established connections for the utp socket
    connections: RwLock<HashMap<UtpConnId, UnboundedSender<Packet>>>,
    /// The sender of new incoming utp streams.
    incoming_stream_sender: UnboundedSender<UtpStream>,
    /// The receiver of new incoming utp streams.
    incoming_stream_receiver: Mutex<UnboundedReceiver<UtpStream>>,
    /// The uTP extensions of the socket.
    extensions: Arc<UtpSocketExtensions>,
    /// The connection timeout
    timeout: Duration,
    /// The termination cancellation token
    cancellation_token: CancellationToken,
}

impl UtpSocketContext {
    /// Run the main loop of the utp socket.
    async fn run(&self, context: &Arc<UtpSocketContext>) {
        let mut buffer = [0u8; MAX_PACKET_SIZE];

        loop {
            select! {
                _ = self.cancellation_token.cancelled() => break,
                result = self.socket.recv_from(&mut buffer) => {
                    match result {
                        Ok((0, _)) => break, // socket closed
                        Ok((bytes_read, addr)) => {
                            self.on_packet_received(&buffer[..bytes_read], addr, context).await;
                        },
                        Err(e) => {
                            warn!("Utp socket {} reader failed to receive packet, {}", self, e);
                            break;
                        }
                    }
                }
            }
        }
        debug!("Utp socket {} main loop ended", self);
    }

    /// Returns the unique id of the socket.
    pub fn id(&self) -> &UtpSocketId {
        &self.id
    }

    /// Try to send the given packet over the uTP socket to the given remote peer address.
    ///
    /// # Arguments
    ///
    /// * `packet` - The uTP packet to send.
    /// * `addr` - The remote uTP peer address.
    pub async fn send(&self, packet: Packet, addr: SocketAddr) -> Result<()> {
        // convert the packet into the payload bytes
        let bytes: Vec<u8> = packet.as_bytes()?;
        let bytes_len = bytes.len();

        // verify the packet size
        if bytes_len > MAX_PACKET_SIZE {
            return Err(Error::TooLarge(MAX_PACKET_SIZE));
        }

        let start_time = Instant::now();
        select! {
            _ = time::sleep(self.timeout) => Err(Error::Io(io::Error::new(io::ErrorKind::TimedOut, format!("connection timed out after {}s", self.timeout.as_secs())))),
            result = self.socket.send_to(&bytes, addr) => {
                match result {
                    Ok(_) => {
                        let elapsed = start_time.elapsed();
                        trace!("Utp socket {} ({}) sent {} bytes in {}.{:03}ms", self, addr, bytes_len, elapsed.as_millis(), elapsed.subsec_micros() % 1000);
                        Ok(())
                    },
                    Err(e) => Err(Error::Io(e)),
                }
            }
        }
    }

    /// Close the given uTP stream connection.
    /// The key will be used to identify the stream and remove the packet sender of the stream.
    pub async fn close_connection(&self, key: UtpConnId) {
        let mut connections = self.connections.write().await;
        connections.remove(&key);
    }

    /// Handle a received packet payload from the socket.
    async fn on_packet_received(
        &self,
        packet_bytes: &[u8],
        addr: SocketAddr,
        context: &Arc<UtpSocketContext>,
    ) {
        if packet_bytes.len() < UTP_HEADER_SIZE {
            trace!(
                "Utp socket {} incoming packet from {} too small",
                self,
                addr
            );
            return;
        }

        let packet_size = packet_bytes.len();
        match Packet::try_from(packet_bytes) {
            Ok(packet) => {
                trace!(
                    "Utp socket {} ({}) received packet (len {}) {:?}",
                    self,
                    addr,
                    packet_size,
                    packet
                );
                // check if the packet is a new incoming connection
                if packet.state_type == StateType::Syn {
                    self.handle_incoming_connection(packet, addr, context).await;
                } else {
                    self.handle_received_packet(packet).await;
                }
            }
            Err(e) => warn!(
                "Utp socket {} failed to parse packet (len {}), {}",
                self, packet_size, e
            ),
        }
    }

    /// Handle a received packet from a remote peer.
    /// The packet will be sent to the relevant uTP connection if found, else it will be dropped.
    async fn handle_received_packet(&self, packet: Packet) {
        let mut key_to_remove: Option<UtpConnId> = None;

        {
            // try to find the relevant connection of the received packet
            let connections = self.connections.read().await;
            let connection_id = packet.connection_id;
            if let Some((key, connection_message_sender)) = connections
                .iter()
                .find(|(key, _)| key.recv_id == connection_id)
            {
                if let Err(e) = connection_message_sender.send(packet) {
                    trace!(
                        "Utp socket {} connection {} has been closed, {}",
                        self,
                        connection_id,
                        e
                    );
                    key_to_remove = Some(key.clone());
                }
            } else {
                debug!(
                    "Utp socket {} received packet with unknown connection id {}",
                    self, packet.connection_id
                );
            }
        }

        if let Some(key) = key_to_remove {
            debug!("Utp socket {} dropping connection {:?}", self, key);
            self.connections.write().await.remove(&key);
        }
    }

    /// Try to handle a new incoming uTP connection.
    async fn handle_incoming_connection(
        &self,
        packet: Packet,
        addr: SocketAddr,
        context: &Arc<UtpSocketContext>,
    ) {
        let key = UtpConnId {
            recv_id: packet.connection_id + 1,
            send_id: packet.connection_id,
        };
        let ack_number: u16 = packet.sequence_number;
        let (message_sender, message_receiver) = unbounded_channel();

        match UtpStream::new_incoming(
            key,
            addr,
            context.clone(),
            ack_number,
            message_receiver,
            self.extensions(),
        )
        .await
        {
            Ok(stream) => {
                debug!("Utp socket {} established connection with {}", self, addr);
                {
                    let mut connections = self.connections.write().await;
                    connections.insert(key, message_sender);
                }

                let _ = self.incoming_stream_sender.send(stream);
            }
            Err(e) => debug!(
                "Utp socket {} failed to accept incoming connection from {}, {}",
                self, addr, e
            ),
        }
    }

    /// Get the extension instances of the uTP socket.
    /// These will be applied within an uTP stream for every received- and sent packet.
    fn extensions(&self) -> Arc<Vec<Box<dyn UtpSocketExtension>>> {
        self.extensions.clone()
    }
}

/// The unique identifier of an uTP socket.
#[derive(Debug, Display, Copy, Clone)]
#[display("{}:{}", handle, addr.port())]
pub struct UtpSocketId {
    /// The unique socket handle
    handle: UtpHandle,
    /// The socket address on which the utp socket is listening
    addr: SocketAddr,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::protocol::Extension;
    use crate::peer::protocol::UtpStreamState;
    use std::net::Ipv4Addr;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_utp_conn_id_new() {
        let key = UtpConnId::new();

        let expected_result = key.recv_id + 1;
        let result = key.send_id;

        assert_eq!(
            expected_result, result,
            "expected the send id to be 1 higher than the receive id"
        );
    }

    #[test]
    fn test_state_type_from_integer() {
        let expected_result = StateType::Data;
        let result = StateType::try_from(0);
        assert_eq!(Ok(expected_result), result);

        let expected_result = StateType::Fin;
        let result = StateType::try_from(1);
        assert_eq!(Ok(expected_result), result);

        let expected_result = StateType::State;
        let result = StateType::try_from(2);
        assert_eq!(Ok(expected_result), result);

        let expected_result = StateType::Reset;
        let result = StateType::try_from(3);
        assert_eq!(Ok(expected_result), result);

        let expected_result = StateType::Syn;
        let result = StateType::try_from(4);
        assert_eq!(Ok(expected_result), result);
    }

    #[test]
    fn test_packet_try_from() {
        init_logger!();
        let timestamp_microseconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u32;
        let packet = Packet {
            state_type: StateType::Syn,
            extension: Extension::None,
            connection_id: 12345,
            timestamp_microseconds,
            timestamp_difference_microseconds: 0,
            window_size: 0,
            sequence_number: 0,
            acknowledge_number: 0,
            payload: Vec::with_capacity(0),
        };

        let bytes: Vec<u8> = packet
            .as_bytes()
            .expect("expected the packet to have been serialized");
        assert_eq!(
            20,
            bytes.len(),
            "expected the header to have a length of 20 bytes"
        );

        let result = Packet::try_from(bytes.as_slice())
            .expect("expected the packet to have been deserialized");

        assert_eq!(packet, result);
    }

    #[tokio::test]
    async fn test_udp_socket_new() {
        init_logger!();
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));

        // bind to an ephemeral port on localhost
        let socket = UtpSocket::new(addr, Duration::from_secs(1), vec![])
            .await
            .expect("expected an uTP socket");

        // check the bound port on the socket
        let port = socket.addr().port();
        assert_ne!(
            0, port,
            "expected the actual bound port to have been returned"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_utp_socket_connect() {
        init_logger!();
        let expected_result = UtpStreamState::Connected;
        let (incoming, outgoing) = create_utp_socket_pair!();

        let target_addr = (Ipv4Addr::LOCALHOST, incoming.addr().port()).into();
        let outgoing_stream = outgoing
            .connect(target_addr)
            .await
            .expect("expected an utp stream");

        assert_timeout!(
            Duration::from_secs(1),
            expected_result == outgoing_stream.state().await,
            "expected the outgoing stream to be connected"
        );

        let incoming_stream = incoming.recv().await.expect("expected an uTP stream");
        assert_timeout!(
            Duration::from_secs(1),
            expected_result == incoming_stream.state().await,
            "expected the incoming stream to be connected"
        );
    }

    #[tokio::test]
    async fn test_utp_socket_close_connection() {
        init_logger!();
        let id = UtpConnId::new();
        let (tx, mut rx) = unbounded_channel();
        let socket = create_utp_socket!();
        let context = socket.context();

        {
            let mut connections = context.connections.write().await;
            connections.insert(id, tx);
        }

        // close the connection based on it's id
        context.close_connection(id).await;
        let result = context.connections.read().await;
        assert_eq!(
            0,
            result.len(),
            "expected the connection to have been removed from the socket"
        );

        // check if the receiver is correctly closed
        let result = rx.recv().await;
        assert_eq!(None, result);
    }
}
