use crate::channel::{ChannelReceiver, ChannelSender, Reply};
use crate::peer::protocol::{
    ConnectionId, Extension, Packet, SequenceNumber, StateType, UtpConnId, UtpSocketContext,
    UtpSocketExtension, UtpSocketExtensions, UtpSocketId, MAX_PACKET_PAYLOAD_SIZE,
};
use crate::peer::{Error, Result};
use async_trait::async_trait;
use derive_more::Display;
use futures::task::AtomicWaker;
use futures::Future;
use itertools::Itertools;
use log::{debug, trace, warn};
use rand::random;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::select;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{Mutex, MutexGuard, TryLockError};
use tokio::time::interval;
use tokio_util::bytes::{Buf, Bytes, BytesMut};
use tokio_util::sync::CancellationToken;

/// The maximum amount out-of-order packets which can stored in memory.
const MAX_UNACKED_PACKETS: usize = 128;
/// The maximum amount of bytes allowed within the read buffer.
const MAX_READ_BUFFER: usize = 1024 * 1024; // 1MB
const RETRY_INTERVAL: Duration = Duration::from_millis(500);

/// The state of an uTP stream connection.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum UtpStreamState {
    /// The stream is being initialized and no state is known at the moment
    Initializing,
    /// The stream has sent the SYN packet
    SynSent,
    /// The stream has received the SYN packet
    SynRecv,
    /// The stream has successfully connected with the remote uTP socket
    Connected,
    /// The stream has been closed
    Closed,
}

/// A parsed uTP message.
#[derive(Clone, PartialEq)]
enum Message {
    /// Connect to the utp peer with the connection id
    Connect(ConnectionId),
    /// The latest known state of an uTP peer with `sequence_number` & `acknowledge_number`.
    State(ConnectionId, SequenceNumber, SequenceNumber),
    /// Message containing data information
    Data(ConnectionId, Vec<u8>),
    /// Terminate the connection forcefully.
    Terminate(ConnectionId),
    /// Close the connection
    Close(ConnectionId),
}

impl Message {
    /// Convert this message into an uTP packet.
    pub fn into_packet(
        self,
        sequence_number: SequenceNumber,
        acknowledge_number: SequenceNumber,
        base_time: Instant,
        last_packet_timestamp: u32,
        window_size: u32,
    ) -> Packet {
        let timestamp_microseconds = base_time.elapsed().as_micros() as u32;
        let timestamp_difference_microseconds =
            timestamp_microseconds.wrapping_sub(last_packet_timestamp);
        match self {
            Message::Connect(connection_id) => Packet {
                state_type: StateType::Syn,
                extension: Extension::None,
                connection_id,
                timestamp_microseconds,
                timestamp_difference_microseconds,
                window_size,
                sequence_number,
                acknowledge_number,
                payload: Vec::with_capacity(0),
            },
            Message::State(connection_id, seq_number, ack_number) => Packet {
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
            Message::Data(connection_id, payload) => Packet {
                state_type: StateType::Data,
                extension: Extension::None,
                connection_id,
                timestamp_microseconds,
                timestamp_difference_microseconds,
                window_size,
                sequence_number,
                acknowledge_number,
                payload,
            },
            Message::Terminate(connection_id) => Packet {
                state_type: StateType::Reset,
                extension: Extension::None,
                connection_id,
                timestamp_microseconds,
                timestamp_difference_microseconds,
                window_size,
                sequence_number,
                acknowledge_number,
                payload: Vec::with_capacity(0),
            },
            Message::Close(connection_id) => Packet {
                state_type: StateType::Fin,
                extension: Extension::None,
                connection_id,
                timestamp_microseconds,
                timestamp_difference_microseconds,
                window_size,
                sequence_number,
                acknowledge_number,
                payload: Vec::with_capacity(0),
            },
        }
    }
}

impl TryFrom<&Packet> for Message {
    type Error = Error;

    fn try_from(value: &Packet) -> Result<Self> {
        match value.state_type {
            StateType::Syn => Ok(Message::Connect(value.connection_id)),
            StateType::State => Ok(Message::State(
                value.connection_id,
                value.sequence_number,
                value.acknowledge_number,
            )),
            StateType::Data => Ok(Message::Data(value.connection_id, value.payload.clone())),
            StateType::Fin => Ok(Message::Close(value.connection_id)),
            StateType::Reset => Ok(Message::Terminate(value.connection_id)),
        }
    }
}

impl From<&Message> for StateType {
    fn from(value: &Message) -> Self {
        match value {
            Message::Connect(_) => StateType::Syn,
            Message::State(_, _, _) => StateType::State,
            Message::Data(_, _) => StateType::Data,
            Message::Terminate(_) => StateType::Reset,
            Message::Close(_) => StateType::Fin,
        }
    }
}

impl Debug for Message {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::Connect(id) => write!(f, "Connect({})", id),
            Message::State(id, seq, ack) => write!(f, "State({}, {}, {})", id, seq, ack),
            Message::Data(id, data) => write!(f, "Data({}, len {})", id, data.len()),
            Message::Terminate(id) => write!(f, "Terminate({})", id),
            Message::Close(id) => write!(f, "Close({})", id),
        }
    }
}

/// A uTorrent transport protocol connection stream.
/// This stream allows to read and write to a specific uTP connection.
#[derive(Display)]
#[display("{} ({})", id, addr)]
pub struct UtpStream {
    id: UtpSocketId,
    addr: SocketAddr,
    read_buffer: ReadBuffer,
    write_fut: Option<Pin<Box<dyn Future<Output = std::result::Result<usize, io::Error>> + Send>>>,
    flush_fut: Option<Pin<Box<dyn Future<Output = Result<()>> + Send>>>,
    sender: ChannelSender<StreamCommand>,
    cancellation_token: CancellationToken,
}

impl UtpStream {
    /// Try to create a new outgoing uTP stream for the given address.
    /// This will initiate the SYN process with the remote socket address.
    pub(crate) async fn new_outgoing(
        key: UtpConnId,
        addr: SocketAddr,
        socket: Arc<UtpSocketContext>,
        message_receiver: UnboundedReceiver<Packet>,
        extensions: Arc<UtpSocketExtensions>,
    ) -> Result<Self> {
        let seq_number = 1;
        let id = *socket.id();
        let (sender, receiver) = channel!(16);
        let mut inner = Self::new(
            key,
            addr,
            socket,
            ConnectionType::Outgoing,
            UtpStreamState::Initializing,
            seq_number,
            0,
            extensions,
        );
        let read_buffer = inner.read_buffer.clone();
        let cancellation_token = inner.cancellation_token.clone();

        tokio::spawn(async move {
            inner.run(message_receiver, receiver).await;
        });

        Ok(Self {
            id,
            addr,
            read_buffer,
            write_fut: None,
            flush_fut: None,
            sender,
            cancellation_token,
        })
    }

    /// Try to accept a new incoming uTP stream for the given address.
    /// This will finish the SYN process with the remote socket address.
    pub(crate) async fn new_incoming(
        key: UtpConnId,
        addr: SocketAddr,
        socket: Arc<UtpSocketContext>,
        ack_number: u16,
        message_receiver: UnboundedReceiver<Packet>,
        extensions: Arc<UtpSocketExtensions>,
    ) -> Result<Self> {
        let id = *socket.id();
        let (sender, receiver) = channel!(16);
        let mut inner = Self::new(
            key,
            addr,
            socket,
            ConnectionType::Incoming,
            UtpStreamState::SynRecv,
            random(),
            ack_number,
            extensions,
        );
        let read_buffer = inner.read_buffer.clone();
        let cancellation_token = inner.cancellation_token.clone();

        tokio::spawn(async move {
            inner.run(message_receiver, receiver).await;
        });

        Ok(Self {
            id,
            addr,
            read_buffer,
            write_fut: None,
            flush_fut: None,
            sender,
            cancellation_token,
        })
    }

    /// Returns the remote socket address of the uTP stream.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Returns the id of the remote peer.
    pub async fn recv_id(&self) -> u16 {
        self.sender
            .send(|tx| StreamCommand::GetRecvId { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the current state if the uTP stream connection.
    pub async fn state(&self) -> UtpStreamState {
        self.sender
            .send(|tx| StreamCommand::GetState { response: tx })
            .await
            .await
            .unwrap_or(UtpStreamState::Closed)
    }

    /// Returns the current sequence number of the client stream.
    pub async fn seq_number(&self) -> SequenceNumber {
        self.sender
            .send(|tx| StreamCommand::GetSeqNumber { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the last acknowledged number we've sent to the remote peer.
    pub async fn ack_number(&self) -> SequenceNumber {
        self.sender
            .send(|tx| StreamCommand::GetAckNumber { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the last sequence number that has been acknowledged by the remote peer.
    pub async fn last_ack_number(&self) -> SequenceNumber {
        self.sender
            .send(|tx| StreamCommand::GetLastAckNumber { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Returns the total number of pending outgoing packets.
    pub async fn pending_outgoing_len(&self) -> usize {
        self.sender
            .send(|tx| StreamCommand::PendingOutgoingLen { response: tx })
            .await
            .await
            .unwrap_or_default()
    }

    /// Check if the uTP stream is closed.
    /// In this state, the stream is no longer able to send or receive any packets.
    pub async fn is_closed(&self) -> bool {
        self.cancellation_token.is_cancelled() || self.state().await == UtpStreamState::Closed
    }

    /// Close the uTP stream.
    pub fn close(&self) {
        self.cancellation_token.cancel()
    }

    fn new(
        key: UtpConnId,
        addr: SocketAddr,
        socket: Arc<UtpSocketContext>,
        connection_type: ConnectionType,
        state: UtpStreamState,
        seq_number: u16,
        ack_number: u16,
        extensions: Arc<UtpSocketExtensions>,
    ) -> UtpStreamContext {
        UtpStreamContext {
            key,
            addr,
            socket,
            connection_type,
            base_time: Instant::now(),
            state,
            seq_number,
            ack_number,
            last_ack_number: seq_number - 1,
            pending_incoming_packets: Default::default(),
            pending_outgoing_packets: Default::default(),
            last_packet_timestamp: Default::default(),
            read_buffer: Default::default(),
            write_buffer: BytesMut::with_capacity(MAX_READ_BUFFER),
            pending_flush: None,
            remote_window_size: MAX_READ_BUFFER as u32,
            extensions,
            cancellation_token: Default::default(),
        }
    }
}

impl AsyncRead for UtpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut read_buf = match self.read_buffer.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        };
        if !read_buf.has_remaining() {
            // EOF the read buffer when the stream is being closed
            if self.cancellation_token.is_cancelled() {
                return Poll::Ready(Ok(()));
            }

            self.read_buffer.register(cx.waker());
            return Poll::Pending;
        }

        let to_copy = std::cmp::min(read_buf.remaining(), buf.remaining());
        buf.put_slice(&read_buf.split_to(to_copy));
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for UtpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, io::Error>> {
        let sender = self.sender.clone();
        let mut fut = self.write_fut.take().unwrap_or_else(|| {
            let data = Bytes::copy_from_slice(buf);
            Box::pin(async move {
                sender
                    .send(|tx| StreamCommand::WriteData { data, response: tx })
                    .await
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
            })
        });

        match fut.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result),
            Poll::Pending => {
                self.write_fut = Some(fut);
                Poll::Pending
            }
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), io::Error>> {
        let sender = self.sender.clone();
        let mut fut = self.flush_fut.take().unwrap_or_else(|| {
            Box::pin(async move {
                sender
                    .send(|tx| StreamCommand::Flush { response: tx })
                    .await
                    .await
            })
        });

        match fut.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result.map_err(|err| match err {
                Error::Io(e) => e,
                _ => io::Error::new(io::ErrorKind::Other, err),
            })),
            Poll::Pending => {
                self.flush_fut = Some(fut);
                Poll::Pending
            }
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), io::Error>> {
        self.close();
        Poll::Ready(Ok(()))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }
}

impl Debug for UtpStream {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UtpStream")
            .field("id", &self.id)
            .field("addr", &self.addr)
            .field("read_buffer", &self.read_buffer)
            .field("sender", &self.sender)
            .field("cancellation_token", &self.cancellation_token)
            .finish()
    }
}

impl Drop for UtpStream {
    fn drop(&mut self) {
        trace!("Utp stream {} is being dropped", self);
        self.cancellation_token.cancel();
    }
}

#[derive(Debug)]
enum StreamCommand {
    /// Returns the id of the remote peer.
    GetRecvId { response: Reply<u16> },
    /// Returns the current state of the stream.
    GetState { response: Reply<UtpStreamState> },
    /// Returns the current sequence number of the stream.
    GetSeqNumber { response: Reply<SequenceNumber> },
    /// Returns the current ack number of the stream.
    GetAckNumber { response: Reply<SequenceNumber> },
    /// Returns the last acknowledged number by the remote peer.
    GetLastAckNumber { response: Reply<SequenceNumber> },
    /// Returns the total number of pending outgoing packets.
    PendingOutgoingLen { response: Reply<usize> },
    /// Try to write the given data to the remote peer.
    WriteData {
        data: Bytes,
        response: Reply<Result<usize>>,
    },
    /// Flush the current write buffer to the remote peer.
    Flush { response: Reply<Result<()>> },
}

#[derive(Debug, Display)]
#[display("{} ({})", socket, addr)]
pub struct UtpStreamContext {
    /// The unique key of the utp stream
    key: UtpConnId,
    /// The remote connected address
    addr: SocketAddr,
    /// The uTP socket writer channel
    socket: Arc<UtpSocketContext>,
    /// The direction type of the connection.
    connection_type: ConnectionType,
    /// The state of the stream connection
    state: UtpStreamState,
    /// The next sequence number that will be used when sending packets. (outgoing)
    seq_number: SequenceNumber,
    /// The last packet sequence number that has been acknowledged to the remote peer. (outgoing)
    ack_number: SequenceNumber,
    /// Our last packet sequence number that was acknowledged by the remote peer. (incoming)
    last_ack_number: SequenceNumber,
    /// The pending incoming packets which have been received out of order from the remote peer.
    pending_incoming_packets: HashMap<SequenceNumber, Message>,
    /// The pending packets which have not been acked by the remote peer.
    pending_outgoing_packets: Vec<PendingPacket>,
    /// The time of the stream creation.
    /// This value is used to calculate the delay.
    base_time: Instant,
    /// The timestamp of the last received packet from the remote peer.
    last_packet_timestamp: u32,
    /// The uTP stream incoming data buffer of the remote peer.
    read_buffer: ReadBuffer,
    /// The uTP stream outgoing data buffer to the remote peer.
    write_buffer: BytesMut,
    /// A pending flush waiting for the stream to become Connected.
    pending_flush: Option<Reply<Result<()>>>,
    /// The currently allowed window size of the remote peer.
    remote_window_size: u32,
    /// The immutable extensions of the uTP stream.
    extensions: Arc<UtpSocketExtensions>,
    /// The cancellation token of the stream
    cancellation_token: CancellationToken,
}

impl UtpStreamContext {
    /// Run the main loop of the utp stream for processing messages.
    async fn run(
        &mut self,
        mut message_receiver: UnboundedReceiver<Packet>,
        mut command_receiver: ChannelReceiver<StreamCommand>,
    ) {
        if let Err(e) = self.initialize().await {
            self.update_state(UtpStreamState::Closed);
            debug!("Utp stream {} failed to initialize, {}", self, e);
            return;
        }

        let mut resend_interval = interval(RETRY_INTERVAL);
        loop {
            select! {
                _ = self.cancellation_token.cancelled() => break,
                packet = message_receiver.recv() => {
                    if let Some(packet) = packet {
                        self.on_received_packet(packet).await;
                    } else {
                        debug!("Utp stream {} socket has been closed", self);
                        break;
                    }
                }
                Some(command) = command_receiver.recv() => self.on_command(command).await,
                // _ = resend_interval.tick() => self.resend_timeout_packets().await,
            }

            self.do_flush().await;
        }

        let _ = self.close().await;
        debug!("Utp stream {} main loop ended", self);
    }

    /// Initialize the connection based on the connection type.
    async fn initialize(&mut self) -> Result<()> {
        match self.connection_type {
            ConnectionType::Incoming => {
                self.send_state().await?;
                self.update_state(UtpStreamState::Connected);
                Ok(())
            }
            ConnectionType::Outgoing => self.send_syn().await,
        }
    }

    /// Get the extensions of the uTP stream.
    fn extensions(&self) -> &[Box<dyn UtpSocketExtension>] {
        &self.extensions
    }

    /// Check if writing of the given payload to the remote peer is allowed.
    /// It checks if the stream is in a valid state, and that the remote peer window size allows the writing of the given data.
    ///
    /// # Returns
    ///
    /// It returns true when writing to the remote peer is allowed, else false.
    fn is_writing_allowed(&self, data_len: usize) -> bool {
        let is_remote_writing_allowed = self.remote_window_size >= data_len as u32;
        self.state == UtpStreamState::Connected && is_remote_writing_allowed
    }

    /// Try to parse the given received packet of the remote peer.
    async fn on_received_packet(&mut self, mut packet: Packet) {
        // check if the packet is valid for this stream
        if !self.assert_packet(&packet) {
            return;
        }

        // process the extensions for the given packet
        self.process_incoming_extensions(&mut packet).await;
        // calculate the latency of the uTP stream connection from the packet
        self.update_timestamp_difference(&packet).await;
        // update the remote window size info
        self.update_remote_window_size(&packet).await;

        match Message::try_from(&packet) {
            Ok(message) => self.on_message_received(message, packet).await,
            Err(e) => debug!("Utp stream {} failed to parse packet, {}", self, e),
        }
    }

    /// Try to process the received remote peer message.
    async fn on_message_received(&mut self, message: Message, packet: Packet) {
        // process the last ack number of the remote peer
        self.on_remote_acknowledgment(packet.acknowledge_number)
            .await;
        // process the syn acknowledgment of the remote peer if applicable
        self.on_syn_ack(packet.sequence_number, packet.state_type)
            .await;

        // check if we've already seen the packet, this can happen due to a resend delay
        let remote_sequence_number = packet.sequence_number;
        let current_ack_number = self.ack_number;
        if !is_less_than(current_ack_number, remote_sequence_number) {
            // check if the message is not a state packet, as state packets will always be guaranteed to be duplicates
            if packet.state_type != StateType::State {
                trace!(
                    "Utp stream {} has already seen packet {}",
                    self,
                    remote_sequence_number
                );
            }
            return;
        }

        // calculate the difference between the received sequence and our last inbound ack number
        let to_be_ack_number_for_remote = remote_sequence_number.wrapping_sub(1);
        let sequence_diff = current_ack_number.saturating_sub(to_be_ack_number_for_remote);
        // store the out-of-order ahead packet in the buffer is allowed
        if sequence_diff <= MAX_UNACKED_PACKETS as u16 {
            // buffer the incoming out-of-order packet
            self.pending_incoming_packets
                .insert(remote_sequence_number, message);
        }

        // if the packet is out-of-order,
        // do not try to process it
        if sequence_diff > 0 {
            trace!(
                "Utp stream {} received out-of-order packet ({}), skipping",
                self,
                remote_sequence_number
            );
            return;
        }

        let mut send_state_message = false;
        loop {
            let next_seq_number = self.ack_number.wrapping_add(1);
            match self.pending_incoming_packets.remove(&next_seq_number) {
                None => break, // we don't have the next sequence packet available, stop processing messages
                Some(message) => {
                    // process the incoming message in-order
                    let state_type = StateType::from(&message);
                    self.on_incoming_message(message, next_seq_number).await;
                    // update the processed ack number if the message is everything but a state message
                    if state_type != StateType::State {
                        self.ack_number = next_seq_number;
                        send_state_message = true;
                    }
                }
            }
        }

        if send_state_message {
            // confirm the processed packets if we don't have any outgoing data
            if self.write_buffer.len() == 0 {
                if let Err(e) = self.send_acknowledgment(self.ack_number).await {
                    debug!("Utp stream {}, failed to inform remote peer, {}", self, e);
                }
            }
        }
    }

    /// Process the received command.
    async fn on_command(&mut self, command: StreamCommand) {
        match command {
            StreamCommand::GetRecvId { response } => response.send(self.key.recv_id),
            StreamCommand::GetState { response } => {
                response.send(self.state);
            }
            StreamCommand::GetSeqNumber { response } => response.send(self.seq_number),
            StreamCommand::GetAckNumber { response } => response.send(self.ack_number),
            StreamCommand::GetLastAckNumber { response } => response.send(self.last_ack_number),
            StreamCommand::PendingOutgoingLen { response } => {
                response.send(self.pending_outgoing_packets.len())
            }
            StreamCommand::WriteData { data, response } => {
                response.send(self.write(data).await);
            }
            StreamCommand::Flush { response } => {
                if let Some(queued) = self.pending_flush.replace(response) {
                    queued.send(self.flush().await);
                }
            }
        }
    }

    /// Process the stream extensions for the given packet of the remote peer.
    async fn process_incoming_extensions(&self, packet: &mut Packet) {
        for extension in self.extensions().iter() {
            extension.incoming(packet, &self).await;
        }
    }

    /// Handle the last acknowledgement number of a remote peer.
    /// This will process any outgoing pending packets up to the given `ack_number`.
    async fn on_remote_acknowledgment(&mut self, remote_ack_number: SequenceNumber) {
        // try to find the pending packet belonging to the ack number
        if remote_ack_number > self.seq_number {
            debug!(
                "Utp stream {} received invalid ack number {}, current sequence number {}",
                self, remote_ack_number, self.seq_number
            );
            return;
        }
        // check if there is anything to be acked or if we've already caught up
        let ack_range = Self::calculate_ack_range(remote_ack_number, &mut self.last_ack_number);
        if ack_range.is_empty() {
            return;
        }

        // as the ack number might be the highest sequence number,
        // we need to acknowledge all pending messages up to the given ack number
        trace!(
            "Utp stream {} is processing remote ack number {} (ack range {:?})",
            self,
            remote_ack_number,
            ack_range
        );
        for ack_number in ack_range {
            if let Some(packet_index) = self
                .pending_outgoing_packets
                .iter()
                .position(|e| e.packet.sequence_number == ack_number)
            {
                // if the packet is found, remove it from the pending state
                self.pending_outgoing_packets.remove(packet_index);
                self.last_ack_number = ack_number;
            } else {
                trace!(
                    "Utp stream {} couldn't find pending packet for ack number {}",
                    self,
                    ack_number
                );
            }
        }
    }

    /// Handle the `SYN_ACK` sequence number sent by the remote peer if applicable.
    async fn on_syn_ack(&mut self, seq_number: SequenceNumber, packet_type: StateType) {
        // Only process SYN_ACKs if we are currently awaiting a connection confirmation
        let is_state_syn_send = self.state == UtpStreamState::SynSent;
        if !is_state_syn_send || packet_type != StateType::State {
            return;
        }

        // The remote peer's seq_number will be used for their first data/state packet.
        // To avoid "pre-acknowledging" data we haven't seen, we set our last-received
        // index to one less than their starting sequence.
        let ack_number = seq_number.wrapping_sub(1);
        self.ack_number = ack_number;
        self.update_state(UtpStreamState::Connected);

        debug!(
            "Utp stream {} connection established ({:?}), initial ack number set to {}",
            self, self.key, ack_number
        );
    }

    /// Handle a [StateType::Fin] packet from the remote peer.
    /// This will finalize the connection gracefully.
    async fn on_close_message(&mut self) {
        self.cancellation_token.cancel();
        self.update_state(UtpStreamState::Closed);
        self.read_buffer.wake();
    }

    /// Handle a received data payload from the remote peer.
    async fn on_received_payload(&self, bytes: Vec<u8>) {
        {
            let mut buffer = self.read_buffer.lock().await;
            buffer.extend_from_slice(bytes.as_slice());
        }
        trace!("Utp stream {} received {} data bytes", self, bytes.len());
        self.read_buffer.wake();
    }

    /// Process an in-order incoming uTP message.
    async fn on_incoming_message(&mut self, message: Message, seq_number: SequenceNumber) {
        trace!(
            "Utp stream {} is processing incoming message {}, {:?}",
            self,
            seq_number,
            message
        );
        match message {
            Message::Data(_, payload) => {
                self.on_received_payload(payload).await;
            }
            Message::Terminate(_) => {
                debug!("Utp stream {} received termination message", self);
                self.on_close_message().await;
            }
            Message::Close(_) => {
                debug!("Utp stream {} received close message", self);
                self.on_close_message().await;
            }
            _ => {}
        }
    }

    /// Verify if the received packet matches the expected stream connection id.
    fn assert_packet(&self, packet: &Packet) -> bool {
        let connection_id = packet.connection_id;
        if connection_id != self.key.recv_id {
            debug!(
                "Utp stream {} received invalid message id {}",
                self, connection_id
            );
            return false;
        }

        true
    }

    /// Try to write the given data to the write buffer for the remote peer.
    async fn write(&mut self, data: Bytes) -> Result<usize> {
        let available = self.write_buffer.capacity() - self.write_buffer.len();
        if available < data.len() {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::StorageFull,
                "write buffer is full",
            )));
        }

        self.write_buffer.extend_from_slice(&data);
        Ok(data.len())
    }

    /// Flush the current write buffer to the remote peer.
    async fn flush(&mut self) -> Result<()> {
        let len = self.write_buffer.remaining();
        if !self.is_writing_allowed(len) {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::ResourceBusy,
                "write buffer is full",
            )));
        }

        trace!("Utp stream {} is flushing {} bytes", self, len);
        let bytes = self.write_buffer.split_to(len);
        self.send_data(&bytes).await
    }

    /// Try to execute a queued flush, if one is available.
    async fn do_flush(&mut self) {
        let flush = match self.pending_flush.take() {
            Some(e) => e,
            None => return,
        };

        match self.state {
            UtpStreamState::Connected => {
                flush.send(self.flush().await);
            }
            UtpStreamState::Closed => {
                flush.send(Err(Error::Io(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "stream is closed",
                ))));
            }
            _ => {
                self.pending_flush = Some(flush);
                return;
            }
        }
    }

    /// Send the initial syn message to the remote peer.
    async fn send_syn(&mut self) -> Result<()> {
        let syn_message = Message::Connect(self.key.recv_id);

        self.send_message(syn_message, self.seq_number, self.ack_number)
            .await?;
        self.update_state(UtpStreamState::SynSent);
        Ok(())
    }

    /// Send the current uTP state info to the remote peer.
    async fn send_state(&mut self) -> Result<()> {
        self.send_acknowledgment(self.ack_number).await
    }

    /// Send an acknowledgment for a received remote peer packet.
    async fn send_acknowledgment(&mut self, ack_number: SequenceNumber) -> Result<()> {
        let message = Message::State(self.key.send_id, self.seq_number, ack_number);
        self.send_message(message, self.seq_number, ack_number)
            .await?;
        Ok(())
    }

    /// Send the given data to the remote peer.
    /// It will send one or more packets depending on the given payload size.
    async fn send_data(&mut self, bytes: &[u8]) -> Result<()> {
        // send the data in chunks to not exceed the maximum uTP packet size
        for chunk in bytes.chunks(MAX_PACKET_PAYLOAD_SIZE) {
            let message = Message::Data(self.key.send_id, chunk.to_vec());
            self.send_message(message, self.seq_number, self.ack_number)
                .await?;
        }

        Ok(())
    }

    /// Send the close state to the remote peer.
    async fn send_close(&mut self) -> Result<()> {
        self.send_message(
            Message::Close(self.key.send_id),
            self.seq_number,
            self.ack_number,
        )
        .await
    }

    /// Send the given message to the remote peer.
    async fn send_message(
        &mut self,
        message: Message,
        seq_number: SequenceNumber,
        ack_number: SequenceNumber,
    ) -> Result<()> {
        trace!("Utp stream {} is sending {:?}", self, message);
        let addr = self.addr;
        let window_size = self.window_size().await;
        let mut packet = message.into_packet(
            seq_number,
            ack_number,
            self.base_time,
            self.last_packet_timestamp,
            window_size,
        );

        // process the extensions
        for extension in self.extensions().iter() {
            extension.outgoing(&mut packet, &self).await;
        }

        let pending_packet = PendingPacket::new(packet.clone());
        let start_time = Instant::now();
        self.socket.send(packet, addr).await?;
        let elapsed = start_time.elapsed();
        debug!(
            "Utp stream {} sent {:?} in {}.{:03}ms",
            self,
            pending_packet.packet,
            elapsed.as_millis(),
            elapsed.subsec_micros() % 1000
        );

        // increase the next sequence number if we're not sending a state packet
        if pending_packet.packet.state_type != StateType::State {
            self.seq_number = self.seq_number.wrapping_add(1);
        }

        // store the pending packet if it's not a state packet (unless it's the initial outgoing Syn state confirmation)
        // this is done as state packets don't have a unique seq number that is confirmed by the remote peer
        if pending_packet.packet.state_type != StateType::State
            || self.state == UtpStreamState::SynRecv
        {
            self.pending_outgoing_packets.push(pending_packet);
        }
        Ok(())
    }

    /// Resend all packets which have not yet been acked and have timed out.
    async fn resend_timeout_packets(&mut self) {
        if self.last_packet_timestamp == 0 {
            return;
        }

        let now = self.base_time.elapsed().as_micros() as u32;
        let window_size = self.window_size().await;

        let mut timed_out_packets = self
            .pending_outgoing_packets
            .extract_if(.., |e| {
                now.saturating_sub(e.packet.timestamp_microseconds)
                    > self.last_packet_timestamp.min(5000)
            })
            .collect_vec();
        for pending_packet in timed_out_packets.iter_mut() {
            // update the packet with the latest info
            pending_packet.packet.timestamp_microseconds = now;
            pending_packet.packet.window_size = window_size;
            pending_packet.packet.acknowledge_number = self.ack_number;
            pending_packet.packet.timestamp_difference_microseconds = self.last_packet_timestamp;

            trace!(
                "Utp stream {} is resending packet {:?}",
                self,
                pending_packet
            );
            match self
                .socket
                .send(pending_packet.packet.clone(), self.addr)
                .await
            {
                Ok(_) => {
                    pending_packet.increase_resend();
                }
                Err(e) => {
                    debug!(
                        "Utp stream {} failed to resend packet {:?}, {}",
                        self, pending_packet, e
                    );
                    pending_packet.increase_failures();
                }
            }
        }
        self.pending_outgoing_packets.extend(timed_out_packets);
    }

    /// Get the current window size of all in-flight stream messages that have not yet been acked.
    async fn window_size(&self) -> u32 {
        let pending_inbound_packets_size: usize = self
            .pending_incoming_packets
            .iter()
            .map(|(_, message)| {
                if let Message::Data(_, data) = message {
                    return data.len();
                }

                0
            })
            .sum();

        let read_buffer_len = self.read_buffer.lock().await.len();
        let remaining_window_size = MAX_READ_BUFFER
            .saturating_sub(read_buffer_len)
            .saturating_sub(pending_inbound_packets_size);
        remaining_window_size as u32
    }

    /// Update the stream state.
    /// The update is ignored if the stream is already in the given state.
    fn update_state(&mut self, state: UtpStreamState) {
        if self.state == state {
            return;
        }

        self.state = state;
        debug!("Utp stream {} state changed to {:?}", self, state);
    }

    /// Update the timestamp difference information of the stream connection.
    async fn update_timestamp_difference(&mut self, packet: &Packet) {
        self.last_packet_timestamp = packet.timestamp_microseconds;
    }

    /// Update the currently allowed window size of the remote peer.
    /// This might wake any pending writes if the window size was modified.
    async fn update_remote_window_size(&mut self, packet: &Packet) {
        self.remote_window_size = packet.window_size;
    }

    /// Try to gracefully close the connection with the remote peer.
    async fn close(&mut self) -> Result<()> {
        if self.state == UtpStreamState::Closed {
            return Ok(());
        }

        let result = self.send_close().await;
        // update the state to close before cancelling the context
        // as the main loop might otherwise execute the close twice
        self.update_state(UtpStreamState::Closed);
        self.cancellation_token.cancel();
        self.socket.close_connection(self.key).await;
        self.read_buffer.wake();
        result
    }

    /// Calculate the range of outgoing packets that need to be acknowledged.
    /// It might return an empty range if the outgoing packets have already been acknowledged before.
    fn calculate_ack_range(
        remote_ack_number: SequenceNumber,
        last_ack_number: &SequenceNumber,
    ) -> std::ops::Range<SequenceNumber> {
        let start_index = *last_ack_number + 1;
        let end_index = remote_ack_number + 1;

        // check if the ack range has already been processed
        // this can happen if a packet has been resend or received out-of-order
        if end_index < start_index {
            return 0..0;
        }

        start_index..end_index
    }
}

#[derive(Debug)]
enum ConnectionType {
    Incoming,
    Outgoing,
}

#[derive(Debug, Clone)]
struct ReadBuffer {
    inner: Arc<InnerReadBuffer>,
}

impl ReadBuffer {
    /// Locks the underlying buffer.
    async fn lock(&self) -> MutexGuard<'_, BytesMut> {
        self.inner.buffer.lock().await
    }

    /// Attempts to acquire the lock,
    /// and returns TryLockError if the lock is currently held somewhere else.
    fn try_lock(&self) -> std::result::Result<MutexGuard<'_, BytesMut>, TryLockError> {
        self.inner.buffer.try_lock()
    }

    /// Registers the waker to be notified on calls to wake.
    fn register(&self, waker: &Waker) {
        self.inner.waker.register(waker)
    }

    /// Calls wake on the last Waker passed to register.
    fn wake(&self) {
        self.inner.waker.wake();
    }
}

impl Default for ReadBuffer {
    fn default() -> Self {
        Self {
            inner: Arc::new(InnerReadBuffer {
                buffer: Mutex::new(BytesMut::with_capacity(MAX_READ_BUFFER)),
                waker: AtomicWaker::new(),
            }),
        }
    }
}

#[derive(Debug)]
struct InnerReadBuffer {
    buffer: Mutex<BytesMut>,
    waker: AtomicWaker,
}

/// The selective acks extension for the uTP socket connection.
/// This allows non-sequentially ack packets.
#[derive(Debug)]
pub struct UtpSelectiveAckExtension;

#[async_trait]
impl UtpSocketExtension for UtpSelectiveAckExtension {
    async fn incoming(&self, packet: &mut Packet, stream: &UtpStreamContext) {
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

    async fn outgoing(&self, _packet: &mut Packet, _stream: &UtpStreamContext) {
        // TODO
    }
}

#[derive(Debug)]
struct PendingPacket {
    packet: Packet,
    total_resends: u32,
    total_failures: u32,
}

impl PendingPacket {
    fn new(packet: Packet) -> Self {
        Self {
            packet,
            total_resends: 0,
            total_failures: 0,
        }
    }

    /// Get the data size of the packet.
    fn packet_size(&self) -> usize {
        self.packet.payload.len()
    }

    /// Increase the resend counter of the pending packet.
    fn increase_resend(&mut self) {
        self.total_resends += 1;
    }

    /// Increase the failures counter of the pending packet.
    /// This indicates that the packet resend failed.
    fn increase_failures(&mut self) {
        self.total_failures += 1;
    }
}

/// Determines if the value `a` is considered less than the value `b` using a wrap-around comparison.
///
/// This function is particularly useful in contexts where values wrap around a fixed range, such as
/// sequence numbers in a circular buffer or modular arithmetic.
///
/// # Returns
///
/// It returns `true` if `a` is considered less than `b` according to the wrap-around logic; otherwise, returns `false`.
fn is_less_than(a: u16, b: u16) -> bool {
    if b < 0x8000 {
        a < b || a >= b.wrapping_sub(0x8000)
    } else {
        a < b && a >= b.wrapping_sub(0x8000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::protocol::tests::UtpPacketCaptureExtension;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::mpsc::unbounded_channel;

    #[test]
    fn test_state_type_from_message() {
        let connection_id = 0;

        let message = Message::Connect(connection_id);
        let result = StateType::from(&message);
        assert_eq!(StateType::Syn, result);

        let message = Message::State(connection_id, 0, 0);
        let result = StateType::from(&message);
        assert_eq!(StateType::State, result);

        let message = Message::Data(connection_id, Vec::with_capacity(0));
        let result = StateType::from(&message);
        assert_eq!(StateType::Data, result);

        let message = Message::Terminate(connection_id);
        let result = StateType::from(&message);
        assert_eq!(StateType::Reset, result);

        let message = Message::Close(connection_id);
        let result = StateType::from(&message);
        assert_eq!(StateType::Fin, result);
    }

    #[tokio::test]
    async fn test_utp_stream_new_incoming() {
        init_logger!();
        let initial_sequence_number = 1u16;
        let (_sender, receiver) = unbounded_channel();
        let socket = create_utp_socket!();
        let context = socket.context();
        let capture = UtpPacketCaptureExtension::new();

        let stream = UtpStream::new_incoming(
            UtpConnId::new(),
            SocketAddr::from(socket.addr()),
            context.clone(),
            initial_sequence_number,
            receiver,
            Arc::new(vec![Box::new(capture.clone())]),
        )
        .await
        .expect("expected an uTP stream to have been created");
        assert_timeout!(
            Duration::from_millis(500),
            stream.state().await == UtpStreamState::Connected,
            "expected the stream to accept the connection"
        );

        // check the initial sequence number
        let outgoing_packet = capture
            .outgoing_packets()
            .await
            .get(0)
            .cloned()
            .expect("expected an outgoing packet to have been sent");
        let seq_number_result = stream.seq_number().await;
        assert_ne!(
            1u16, seq_number_result,
            "expected our own seq_number to be random picked"
        );
        assert_eq!(
            outgoing_packet.sequence_number, seq_number_result,
            "expected the random seq_number to have been sent in the syn ack to the remote peer"
        );

        // check the initial remote ack number
        let ack_number_result = stream.ack_number().await;
        assert_eq!(
            1u16, ack_number_result,
            "expected the initial remote ack_number to match"
        );
        assert_eq!(
            outgoing_packet.acknowledge_number, ack_number_result,
            "expected the initial seq_number to have been acked to the remote peer"
        );

        // check the initial last_ack_number which should be one less than the initial state seq_number
        let expected_last_ack = seq_number_result - 1;
        let last_ack_result = stream.last_ack_number().await;
        assert_eq!(
            expected_last_ack, last_ack_result,
            "expected the remote last acknowledged number to match"
        );
    }

    #[tokio::test]
    async fn test_utp_stream_handle_received_packet_ack_syn_sent() {
        init_logger!();
        let sequence_number = 64;
        let now = Instant::now();
        let (sender, receiver) = unbounded_channel();
        let socket = create_utp_socket!();
        let context = socket.context();
        let capture = UtpPacketCaptureExtension::new();

        let stream = UtpStream::new_outgoing(
            UtpConnId::new(),
            SocketAddr::from(socket.addr()),
            context.clone(),
            receiver,
            Arc::new(vec![Box::new(capture.clone())]),
        )
        .await
        .expect("expected an uTP stream to have been created");
        assert_timeout!(
            Duration::from_millis(500),
            stream.state().await == UtpStreamState::SynSent,
            "expected the stream to initiate the connection"
        );

        // sent the syn ack packet to the stream
        let recv_id = stream.recv_id().await;
        let packet = Packet {
            state_type: StateType::State,
            extension: Extension::None,
            connection_id: recv_id,
            timestamp_microseconds: now.elapsed().as_micros() as u32,
            timestamp_difference_microseconds: 1500,
            window_size: MAX_READ_BUFFER as u32,
            sequence_number,
            acknowledge_number: 1,
            payload: vec![],
        };
        sender.send(packet).expect("expected the packet to be sent");
        assert_timeout!(
            Duration::from_millis(500),
            stream.state().await == UtpStreamState::Connected,
            "expected the stream to be in the connected state"
        );

        // check the current ack number
        let incoming_packet = capture
            .incoming_packets()
            .await
            .get(0)
            .cloned()
            .expect("expected to have received an incoming syn ack packet");
        let result = stream.ack_number().await;
        let expected_initial_ack_number = sequence_number - 1;
        assert_eq!(
            expected_initial_ack_number, result,
            "expected the ack number of the remote peer to have been set to the incoming sequence number minus one"
        );
        assert_eq!(
            sequence_number, incoming_packet.sequence_number,
            "expected the seq_number of the incoming syn ack packet to match"
        );

        // check the pending outgoing packets
        let result = stream.pending_outgoing_len().await;
        assert_eq!(
            0, result,
            "expected the syn packet to have been confirmed, got {} instead",
            result
        );
    }

    #[tokio::test]
    async fn test_utp_stream_handle_received_message_state_update() {
        init_logger!();
        let expected_sequence_number = 13;
        let (sender, receiver) = unbounded_channel();
        let socket = create_utp_socket!();
        let context = socket.context();

        let stream = UtpStream::new_incoming(
            UtpConnId::new(),
            SocketAddr::from(socket.addr()),
            context.clone(),
            expected_sequence_number,
            receiver,
            Arc::new(vec![Box::new(UtpPacketCaptureExtension::new())]),
        )
        .await
        .expect("expected an uTP stream to have been created");

        let recv_id = stream.recv_id().await;
        let packet = Packet {
            state_type: StateType::State,
            extension: Extension::None,
            connection_id: recv_id,
            timestamp_microseconds: 0,
            timestamp_difference_microseconds: 0,
            window_size: 0,
            sequence_number: 64,
            acknowledge_number: 1,
            payload: vec![],
        };
        sender.send(packet).expect("expected the packet to be sent");

        let ack_number = stream.ack_number().await;
        assert_eq!(
            expected_sequence_number, ack_number,
            "expected the ack number to not have been updated"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_utp_stream_connection_pairing() {
        init_logger!();
        let expected_outgoing_syn_sequence_number = 1u16;
        let incoming_capture = UtpPacketCaptureExtension::new();
        let outgoing_capture = UtpPacketCaptureExtension::new();
        let (incoming, outgoing) = create_utp_socket_pair!(
            vec![Box::new(incoming_capture.clone())],
            vec![Box::new(outgoing_capture.clone())]
        );
        let (incoming_stream, outgoing_stream) = create_utp_stream_pair!(&incoming, &outgoing);

        assert_timeout!(
            Duration::from_millis(500),
            UtpStreamState::Connected == incoming_stream.state().await,
            "expected the incoming stream to be connected"
        );
        assert_timeout!(
            Duration::from_millis(500),
            UtpStreamState::Connected == outgoing_stream.state().await,
            "expected the outgoing stream to be connected"
        );

        // check the outgoing_stream packets
        let outgoing_packet = outgoing_capture
            .outgoing_packets()
            .await
            .get(0)
            .cloned()
            .expect("expected to have sent a packet");
        let incoming_packet = outgoing_capture
            .incoming_packets()
            .await
            .get(0)
            .cloned()
            .expect("expected to have received a packet");
        assert_eq!(
            StateType::Syn,
            outgoing_packet.state_type,
            "expected the initial outgoing packet to be a syn"
        );
        assert_eq!(
            expected_outgoing_syn_sequence_number, outgoing_packet.sequence_number,
            "expected the initial outgoing seq_number to be 1"
        );
        assert_eq!(
            StateType::State,
            incoming_packet.state_type,
            "expected the initial incoming packet to be state ack for the syn"
        );
        assert_eq!(
            expected_outgoing_syn_sequence_number, incoming_packet.acknowledge_number,
            "expected the initial ack to confirm the syn packet"
        );

        // check the incoming_stream packets
        let outgoing_packet = incoming_capture
            .outgoing_packets()
            .await
            .get(0)
            .cloned()
            .expect("expected to have sent a packet");
        assert_eq!(
            StateType::State,
            outgoing_packet.state_type,
            "expected the initial outgoing packet to be a confirmation of the syn packet"
        );
        assert_eq!(
            expected_outgoing_syn_sequence_number, outgoing_packet.acknowledge_number,
            "expected the initial seq_number of the syn packet to be acknowledged"
        );
        assert_eq!(
            incoming_packet, outgoing_packet,
            "expected the outgoing packet to have been the same as the receiving end"
        );
    }

    #[tokio::test]
    async fn test_utp_stream_outgoing_write_incoming_read() {
        init_logger!();
        let expected_result = "Nullam varius felis in massa eleifend consectetur.";
        let incoming_capture = UtpPacketCaptureExtension::new();
        let outgoing_capture = UtpPacketCaptureExtension::new();
        let (incoming, outgoing) = create_utp_socket_pair!(
            vec![Box::new(incoming_capture.clone())],
            vec![Box::new(outgoing_capture.clone())]
        );
        let (mut incoming_stream, mut outgoing_stream) =
            create_utp_stream_pair!(&incoming, &outgoing);
        let (tx, mut rx) = unbounded_channel();

        // wait for the connection to be established
        assert_timeout!(
            Duration::from_millis(500),
            UtpStreamState::Connected == outgoing_stream.state().await,
            "expected the stream to be connected"
        );

        tokio::spawn(async move {
            let mut buffer = vec![0u8; expected_result.as_bytes().len()];
            let result_buffer_len = incoming_stream
                .read_exact(&mut buffer)
                .await
                .expect("expected a message to have been received");
            tx.send((result_buffer_len, buffer)).unwrap();
        });

        let bytes = expected_result.as_bytes();
        let bytes_len = bytes.len();
        outgoing_stream.write(bytes).await.unwrap();
        outgoing_stream.flush().await.unwrap();

        // check the outgoing packets of the outgoing_stream
        let outgoing_packets = outgoing_capture.outgoing_packets().await.clone();
        let syn_packet = outgoing_packets
            .get(0)
            .expect("expected an outgoing syn packet");
        assert_eq!(
            StateType::Syn,
            syn_packet.state_type,
            "expected the initial outgoing message to be a syn"
        );
        assert_eq!(
            1u16, syn_packet.sequence_number,
            "expected the syn packet to have seq_number 1"
        );
        let data_packet = outgoing_packets
            .get(1)
            .expect("expected an outgoing data packet");
        assert_eq!(
            StateType::Data,
            data_packet.state_type,
            "expected the 2nd outgoing packet to be a data packet"
        );
        assert_eq!(
            2u16, data_packet.sequence_number,
            "expected the seq_number to have been increased"
        );

        // check the read result of the receiving stream
        let (result_buffer_len, buffer) = timeout!(rx.recv(), Duration::from_millis(500)).unwrap();
        let result = String::from_utf8(buffer).unwrap();
        assert_eq!(
            bytes_len, result_buffer_len,
            "expected the read bytes to be the same as the written bytes"
        );
        assert_eq!(expected_result, result);

        // check the outgoing packets of the incoming_stream
        let outgoing_packets = incoming_capture.outgoing_packets().await.clone();
        let syn_ack_packet = outgoing_packets
            .get(0)
            .expect("expected initial syn ack packet");
        assert_eq!(
            StateType::State,
            syn_ack_packet.state_type,
            "expected the initial outgoing packet to be a syn ack state packet"
        );
        assert_eq!(
            syn_packet.sequence_number, syn_ack_packet.acknowledge_number,
            "expected the syn seq_number to be acked"
        );
        let data_ack_packet = outgoing_packets.get(1).expect("expected a data ack packet");
        assert_eq!(
            StateType::State,
            data_ack_packet.state_type,
            "expected the data packet to be acked"
        );
        assert_eq!(
            data_packet.sequence_number, data_ack_packet.acknowledge_number,
            "expected the data seq_number to be acked"
        );
    }

    #[tokio::test]
    async fn test_utp_stream_outgoing_read_incoming_write() {
        init_logger!();
        let expected_result = "Lorem ipsum dolor sit amet, consectetur adipiscing elit.";
        let (incoming, outgoing) = create_utp_socket_pair!();
        let (mut incoming_stream, mut outgoing_stream) =
            create_utp_stream_pair!(&incoming, &outgoing);
        let (tx, mut rx) = unbounded_channel();

        assert_timeout!(
            Duration::from_millis(500),
            UtpStreamState::Connected == outgoing_stream.state().await,
            "expected the stream to be connected"
        );

        tokio::spawn(async move {
            let mut buffer = vec![0u8; expected_result.as_bytes().len()];
            let result_buffer_len = outgoing_stream
                .read_exact(&mut buffer)
                .await
                .expect("expected a message to have been received");
            tx.send((result_buffer_len, buffer)).unwrap();
        });

        let bytes = expected_result.as_bytes();
        let bytes_len = bytes.len();
        incoming_stream.write(bytes).await.unwrap();
        incoming_stream.flush().await.unwrap();

        // wait for the data to be received in the outgoing stream
        let (result_buffer_len, buffer) = timeout!(rx.recv(), Duration::from_millis(500))
            .expect("expected the data to have been received");
        let result = String::from_utf8(buffer).unwrap();
        assert_eq!(
            bytes_len, result_buffer_len,
            "expected the read bytes to be the same as the written bytes"
        );
        assert_eq!(expected_result, result);
    }

    #[tokio::test]
    async fn test_calculate_ack_range() {
        let mut last_ack = 0;

        let result = UtpStreamContext::calculate_ack_range(1, &last_ack);
        assert_eq!(1..2, result);
        assert_eq!(1, result.len(), "expected a total of 1 packet to be acked");

        last_ack = 10;
        let result = UtpStreamContext::calculate_ack_range(8, &last_ack);
        assert_eq!(0..0, result, "expected an empty range to be acked");

        last_ack = 9;
        let result = UtpStreamContext::calculate_ack_range(15, &last_ack);
        assert_eq!(10..16, result);
        assert_eq!(6, result.len(), "expected a total of 6 packets to be acked");
    }

    mod close {
        use super::*;

        #[tokio::test]
        async fn test_close() {
            init_logger!();
            let (incoming, outgoing) = create_utp_socket_pair!();
            let (incoming_stream, outgoing_stream) = create_utp_stream_pair!(&incoming, &outgoing);

            // close the outgoing stream
            outgoing_stream.close();

            // check if the incoming stream has also been closed
            assert_timeout!(
                Duration::from_secs(1),
                UtpStreamState::Closed == incoming_stream.state().await,
                "expected the stream to be closed"
            );
        }

        #[tokio::test]
        async fn test_shutdown() {
            init_logger!();
            let (incoming, outgoing) = create_utp_socket_pair!();
            let (incoming_stream, mut outgoing_stream) =
                create_utp_stream_pair!(&incoming, &outgoing);

            // close the stream through the shutdown fn
            outgoing_stream
                .shutdown()
                .await
                .expect("expected the stream to close");

            // check if the incoming stream has also been closed
            assert_timeout!(
                Duration::from_millis(500),
                UtpStreamState::Closed == incoming_stream.state().await,
                "expected the stream to be closed"
            );
        }
    }

    #[test]
    fn test_is_less_than() {
        let a = 1000;
        let b = 2000;
        assert_eq!(true, is_less_than(a, b));

        let a = 60000;
        let b = 1000;
        assert_eq!(true, is_less_than(a, b));

        let a = 30000;
        let b = 20000;
        assert_eq!(false, is_less_than(a, b));
    }
}
