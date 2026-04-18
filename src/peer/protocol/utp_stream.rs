use crate::channel::{ChannelReceiver, ChannelSender, Reply};
use crate::peer::protocol::{
    CloseReason, Extension, Packet, SequenceNumber, StateType, UtpConnId, UtpMessage,
    UtpSocketContext, UtpSocketExtension, UtpSocketExtensions, UtpSocketId,
    MAX_PACKET_PAYLOAD_SIZE,
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
/// The bucket size of recorded delays in the LEDBAT algorithm.
const LED_BAT_BUCKET_SIZE: usize = 16;
/// The minimum Round-Trip-Time value to consider for the LEDBAT algorithm.
const LED_BAT_MIN_RTT: Duration = Duration::from_millis(100);

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
            state,
            seq_number,
            ack_number,
            last_ack_number: seq_number - 1,
            pending_incoming_packets: Default::default(),
            pending_outgoing_packets: Default::default(),
            read_buffer: Default::default(),
            write_buffer: BytesMut::with_capacity(MAX_READ_BUFFER),
            pending_flush: None,
            led_bat: Default::default(),
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
    pending_incoming_packets: HashMap<SequenceNumber, UtpMessage>,
    /// The pending packets which have not been acked by the remote peer.
    pending_outgoing_packets: Vec<PendingPacket>,
    /// The LedBat algorithm state.
    led_bat: LedBat,
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

        let mut resend_interval = interval(Duration::from_secs(1));
        loop {
            select! {
                _ = self.cancellation_token.cancelled() => break,
                packet = message_receiver.recv() => match packet {
                    Some(packet) => self.on_received_packet(packet).await,
                    None => {
                        debug!("Utp stream {} socket has been closed", self);
                        break;
                    }
                },
                Some(command) = command_receiver.recv() => self.on_command(command).await,
                _ = resend_interval.tick() => self.resend_timeout_packets().await,
            }

            self.do_flush().await;
        }

        let _ = self.close(CloseReason::None).await;
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

    /// Check if the stream may send at least one more byte to the remote peer.
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

        match UtpMessage::try_from(&packet) {
            Ok(message) => self.on_message_received(message, packet).await,
            Err(e) => debug!("Utp stream {} failed to parse packet, {}", self, e),
        }
    }

    /// Try to process the received remote peer message.
    async fn on_message_received(&mut self, message: UtpMessage, packet: Packet) {
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
    async fn on_incoming_message(&mut self, message: UtpMessage, seq_number: SequenceNumber) {
        trace!(
            "Utp stream {} is processing incoming message {}, {:?}",
            self,
            seq_number,
            message
        );
        match message {
            UtpMessage::Data { payload, .. } => {
                self.on_received_payload(payload).await;
            }
            UtpMessage::Terminate { reason, .. } => {
                debug!(
                    "Utp stream {} received termination message, {:?}",
                    self, reason
                );
                self.on_close_message().await;
            }
            UtpMessage::Close { reason, .. } => {
                debug!("Utp stream {} received close message, {:?}", self, reason);
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

    /// Flush as many bytes from the write buffer as the current send window permits.
    ///
    /// The number of bytes sent is bounded by
    /// `min(write_buffer, effective_window − bytes_in_flight)`, so a single call
    /// may only transmit a fraction of the write buffer when the window is small.
    /// The caller should retry once the window grows (i.e., on receiving ACKs).
    async fn flush(&mut self) -> Result<()> {
        let len = self.write_buffer.remaining();
        if !self.is_writing_allowed(len) {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::ResourceBusy,
                "congestion window is full",
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
        let syn_message = UtpMessage::Connect {
            connection: self.key.recv_id,
        };

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
        let message = UtpMessage::State(self.key.send_id, self.seq_number, ack_number);
        self.send_message(message, self.seq_number, ack_number)
            .await?;
        Ok(())
    }

    /// Send the given data to the remote peer.
    /// It will send one or more packets depending on the given payload size.
    async fn send_data(&mut self, bytes: &[u8]) -> Result<()> {
        // send the data in chunks to not exceed the maximum uTP packet size
        for chunk in bytes.chunks(MAX_PACKET_PAYLOAD_SIZE) {
            let message = UtpMessage::Data {
                connection: self.key.send_id,
                payload: chunk.to_vec(),
            };
            self.send_message(message, self.seq_number, self.ack_number)
                .await?;
        }

        Ok(())
    }

    /// Send the close state to the remote peer.
    async fn send_close(&mut self, reason: CloseReason) -> Result<()> {
        self.send_message(
            UtpMessage::Close {
                connection: self.key.send_id,
                reason,
            },
            self.seq_number,
            self.ack_number,
        )
        .await
    }

    /// Send the given message to the remote peer.
    async fn send_message(
        &mut self,
        message: UtpMessage,
        seq_number: SequenceNumber,
        ack_number: SequenceNumber,
    ) -> Result<()> {
        trace!("Utp stream {} is sending {:?}", self, message);
        let addr = self.addr;
        let window_size = self.window_size().await;
        let timestamp_microseconds = self.led_bat.base_time.elapsed().as_micros() as u32;
        let timestamp_microseconds_delay = self.led_bat.client_last_recorded_delay;
        let mut packet = message.into_packet(
            seq_number,
            ack_number,
            timestamp_microseconds,
            timestamp_microseconds_delay,
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
        if self.led_bat.is_empty() || self.cancellation_token.is_cancelled() {
            return;
        }
        if self.led_bat.last_received_packet.elapsed() > Duration::from_mins(1) {
            let _ = self.close(CloseReason::Timeout).await;
            return;
        }

        let window_size = self.window_size().await;

        let now = self.led_bat.base_time.elapsed().as_micros() as u32;
        let timeout_after = self.led_bat.packet_timeout();
        let mut timed_out_packets = self
            .pending_outgoing_packets
            .extract_if(.., |e| {
                now.saturating_sub(e.packet.timestamp_microseconds)
                    > timeout_after.as_micros() as u32
            })
            .collect_vec();

        for pending_packet in timed_out_packets.iter_mut() {
            // update the packet with the latest info
            pending_packet.packet.timestamp_microseconds = now;
            pending_packet.packet.window_size = window_size;
            pending_packet.packet.acknowledge_number = self.ack_number;
            pending_packet.packet.timestamp_difference_microseconds = self.led_bat.base_delay();

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
                if let UtpMessage::Data { payload, .. } = message {
                    return payload.len();
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
    ///
    /// The `timestamp_difference_microseconds` field in the received packet is the
    /// forward-path delay as measured by the remote peer (remote_received_time −
    /// our_sent_timestamp). We feed this into LEDBAT as our one-way delay sample.
    async fn update_timestamp_difference(&mut self, packet: &Packet) {
        if packet.timestamp_difference_microseconds == 0 {
            return;
        }

        let now = self.led_bat.base_time.elapsed().as_micros() as u32;
        self.led_bat.record(
            packet.timestamp_microseconds.wrapping_sub(now),
            packet.timestamp_difference_microseconds,
        );
    }

    /// Update the currently allowed window size of the remote peer.
    /// This might wake any pending writes if the window size was modified.
    async fn update_remote_window_size(&mut self, packet: &Packet) {
        self.remote_window_size = packet.window_size;
    }

    /// Try to gracefully close the connection with the remote peer.
    async fn close(&mut self, reason: CloseReason) -> Result<()> {
        if self.state == UtpStreamState::Closed {
            return Ok(());
        }

        let result = self.send_close(reason).await;
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

#[derive(Debug)]
struct LedBat {
    /// The start base time of the LEDBAT algorithm.
    base_time: Instant,
    /// The current bucket cursor.
    cursor: usize,
    /// The observed one-way delays in microseconds.
    delay_buckets: [u32; LED_BAT_BUCKET_SIZE],
    /// The last delay that has been recorded by our client.
    client_last_recorded_delay: u32,
    /// The last time a packet has been received from the remote peer.
    last_received_packet: Instant,
}

impl LedBat {
    /// Returns `true` if the LedBat delay sliding window is empty (no delays have been recorded yet).
    fn is_empty(&self) -> bool {
        self.delay_buckets.iter().all(|e| *e == 0)
    }

    /// Returns the base delay for the LedBat algorithm.
    fn base_delay(&self) -> u32 {
        let mut lowest_delay = u32::MAX;
        let mut lowest_diff = u32::MAX;

        for delay in self.delay_buckets.iter().filter(|e| **e != 0) {
            lowest_diff = lowest_diff.min(delay.abs_diff(lowest_delay));
            lowest_delay = lowest_delay.min(*delay);
        }

        lowest_diff
    }

    /// Returns the duration a packet would have before being timed-out based
    /// on the current LedBat state.
    fn packet_timeout(&self) -> Duration {
        // check if we're currently awaiting the initial SYN packet
        // in this case, we haven't recorded any delays yet
        if self.is_empty() {
            return Duration::from_secs(3);
        }

        Duration::from_micros(self.base_delay() as u64).min(LED_BAT_MIN_RTT) * 2
    }

    /// Record a new delay measurement in the LedBat algorithm.
    /// The `client_delay` is our own-measured delay,
    /// while `remote_delay` is the delay measured by the remote peer.
    fn record(&mut self, client_delay: u32, remote_delay: u32) {
        self.client_last_recorded_delay = client_delay;
        self.delay_buckets[self.cursor] = remote_delay;
        self.cursor = (self.cursor + 1) % LED_BAT_BUCKET_SIZE;
        self.last_received_packet = Instant::now();
    }
}

impl Default for LedBat {
    fn default() -> Self {
        Self {
            base_time: Instant::now(),
            cursor: 0,
            delay_buckets: [0u32; LED_BAT_BUCKET_SIZE],
            client_last_recorded_delay: 0,
            last_received_packet: Instant::now(),
        }
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
