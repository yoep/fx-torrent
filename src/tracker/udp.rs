use crate::tracker::{
    AnnounceEntryResponse, AnnounceEvent, Announcement, ConnectionMetrics, ScrapeResult,
    TrackerClientConnection, TrackerHandle,
};
use crate::tracker::{Result, TrackerError};
use crate::InfoHash;
use async_trait::async_trait;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use derive_more::Display;
use itertools::Itertools;
use log::{debug, trace};
use std::fmt::{Debug, Display, Formatter};
use std::io;
use std::io::{Cursor, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::select;
use tokio::sync::Mutex;
use tokio_util::bytes::Buf;
use tokio_util::sync::CancellationToken;

#[cfg(feature = "tracker-server")]
pub use server::*;

const PROTOCOL_ID: u64 = 0x41727101980; // see BEP15
const PACKET_SIZE: usize = 16 * 1024;

/// The UDP connection of a tracker.
#[derive(Debug, Display)]
#[display("Tracker {} UDP connection", handle)]
pub struct UdpConnection {
    handle: TrackerHandle,
    session: UdpConnectionSession,
    timeout: Duration,
    metrics: ConnectionMetrics,
    cancellation_token: CancellationToken,
}

impl UdpConnection {
    /// Create a new udp tracker client connection.
    /// It returns an error when the connection could not be established.
    pub async fn new(
        handle: TrackerHandle,
        addrs: &[SocketAddr],
        timeout: Duration,
    ) -> Result<Self> {
        trace!("Creating new tracker udp connection for {:?}", addrs);
        let socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))).await?;
        trace!(
            "Udp tracker client {} started on {}",
            handle,
            socket.local_addr()?.port()
        );

        // try to open a connection to an available address known for the tracker
        Self::establish_connection(&handle, addrs, &socket, timeout).await?;

        // initialize the instance
        let mut instance = Self {
            handle,
            session: UdpConnectionSession {
                connection_id: PROTOCOL_ID, // the magical connection id constant, see BEP15
                transaction_id: Self::generate_transaction_id(),
                socket,
            },
            timeout,
            metrics: Default::default(),
            cancellation_token: Default::default(),
        };
        // try to establish a connection with the tracker
        instance.send(RequestPayload::Connect).await?;
        // await the response from the tracker
        let response = instance.read().await?;
        match response.payload {
            ResponsePayload::Connection(connection_id) => {
                debug!(
                    "Udp tracker client {} received connect({}) response",
                    instance.handle, connection_id
                );
                // update the active connection session
                instance.session.connection_id = connection_id;
            }
            _ => {
                return Err(TrackerError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "expected Response::Connection, but got {:?} instead",
                        response
                    ),
                )));
            }
        }

        Ok(instance)
    }

    /// Try to send the given request message to the tracker.
    /// This method can only be used if a [RequestPayload::Connect] has already been established.
    ///
    /// # Returns
    ///
    /// It returns an error if the request message couldn't be sent.
    async fn send(&self, request: RequestPayload) -> Result<()> {
        trace!(
            "Udp tracker client {} is trying to send message {:?}",
            self,
            request
        );
        let bytes: Vec<u8> = RequestMessage {
            connection_id: self.session.connection_id,
            action: request.action(),
            transaction_id: self.session.transaction_id,
            payload: request,
        }
        .try_into()?;

        self.metrics.bytes_out.inc_by(bytes.len() as u64);
        select! {
            _ = self.cancellation_token.cancelled() => Err(TrackerError::Connection("connection is being closed".to_string())),
            response = self.session.socket.send(bytes.as_ref()) => {
                let _ = response?;
                Ok(())
            },
        }
    }

    async fn read(&self) -> Result<ResponseMessage> {
        trace!(
            "Udp tracker client {} is reading from {}",
            self,
            self.session.socket.peer_addr()?
        );
        let mut buffer = vec![0; PACKET_SIZE];
        let buffer_size = tokio::time::timeout(self.timeout, self.session.socket.recv(&mut buffer))
            .await?
            .map_err(|e| {
                self.metrics.timeouts.inc();
                TrackerError::from(e)
            })?;

        // make sure we shrink the buffer to the expected size before returning
        self.metrics.bytes_in.inc_by(buffer_size as u64);
        let message = ResponseMessage::try_from(&buffer[..buffer_size])?;

        // verify if the transaction id matches
        if message.transaction_id != self.session.transaction_id {
            return Err(TrackerError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid transaction id",
            )));
        }

        debug!(
            "Udp tracker client {} received message {:?}",
            self.handle, message
        );
        Ok(message)
    }

    async fn do_announce(&self, announce: Announcement) -> Result<AnnounceEntryResponse> {
        let info_hash = announce.info_hash.short_info_hash_bytes();
        let event = announce.event;
        let request = AnnounceRequest {
            info_hash,
            peer_id: announce.peer_id.value(),
            downloaded: announce.bytes_completed,
            left: announce.bytes_remaining,
            uploaded: 0,
            event,
            ip_address: 0,
            key: 0,
            num_want: 200,
            listen_port: announce.peer_port,
        };

        trace!(
            "Udp tracker {} is sending announce request {:?}",
            self,
            request
        );
        self.send(RequestPayload::Announce(request)).await?;
        let response = self.read().await?;
        match response.payload {
            ResponsePayload::Announce(response) => {
                debug!(
                    "Udp tracker {} received announce response {:?}",
                    self, response
                );
                Ok(AnnounceEntryResponse {
                    interval_seconds: response.interval as u64,
                    leechers: response.leechers as u64,
                    seeders: response.seeders as u64,
                    peers: response.peers,
                })
            }
            ResponsePayload::Error(e) => Err(TrackerError::AnnounceError(e)),
            _ => Err(TrackerError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "expected Response::Announce, but got {:?} instead",
                    response
                ),
            ))),
        }
    }

    fn generate_transaction_id() -> u32 {
        // don't use 0, because that has special meaning (uninitialized)
        rand::random::<u32>() + 1
    }

    async fn establish_connection(
        handle: &TrackerHandle,
        addrs: &[SocketAddr],
        socket: &UdpSocket,
        timeout: Duration,
    ) -> Result<()> {
        let addrs = AddressManager::new(addrs);

        while let Some(addr) = addrs.next_addr().await {
            trace!(
                "Udp tracker client {} is trying to connect to {:?}",
                handle,
                addr
            );
            let connection = select! {
                _ = tokio::time::sleep(timeout) => Err(TrackerError::Io(io::Error::new(io::ErrorKind::TimedOut, "connection timed out"))),
                conn = socket.connect(addr) => conn.map_err(TrackerError::from),
            };

            match connection {
                Ok(_) => {
                    debug!(
                        "Udp tracker client {} connected to tracker address {}",
                        handle, addr
                    );
                    return Ok(());
                }
                Err(e) => {
                    trace!(
                        "Udp tracker client {} connection failed for {}, {}",
                        handle,
                        addr,
                        e
                    );
                }
            }
        }

        Err(TrackerError::Io(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            format!("failed to connect to {:?}", addrs),
        )))
    }
}

#[async_trait]
impl TrackerClientConnection for UdpConnection {
    async fn announce(&self, announce: Announcement) -> Result<AnnounceEntryResponse> {
        self.do_announce(announce).await
    }

    async fn scrape(&self, hashes: &[InfoHash]) -> Result<ScrapeResult> {
        self.send(RequestPayload::Scrape(ScrapeRequest {
            hashes: hashes.to_vec(),
        }))
        .await?;
        let response = self.read().await?;
        match response.payload {
            ResponsePayload::Scrape(response) => {
                trace!(
                    "Udp tracker {} is parsing scrape response {:?}",
                    self,
                    response
                );
                let mut result = ScrapeResult::default();
                for (index, response) in response.metrics.into_iter().enumerate() {
                    if let Some(hash) = hashes.get(index) {
                        result.files.insert(
                            hash.clone(),
                            crate::tracker::tracker::ScrapeFileMetrics {
                                complete: response.seeders,
                                incomplete: response.leechers,
                                downloaded: response.completed,
                            },
                        );
                    } else {
                        return Err(TrackerError::Parse(format!(
                            "Udp tracker {} scrape response exceeded {}/{} expected hashes",
                            self,
                            index,
                            hashes.len()
                        )));
                    }
                }
                Ok(result)
            }
            ResponsePayload::Error(e) => Err(TrackerError::AnnounceError(e)),
            _ => Err(TrackerError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("expected Response::Scrape, but got {:?} instead", response),
            ))),
        }
    }

    fn metrics(&self) -> &ConnectionMetrics {
        &self.metrics
    }

    fn close(&self) {
        trace!("Closing udp connection");
        self.cancellation_token.cancel();
    }
}

#[cfg(feature = "tracker-server")]
mod server {
    use super::*;
    use crate::peer::PeerId;
    use crate::tracker::{BaseServer, ServerRequest, TrackerListener};
    use rand::{rng, RngExt};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::oneshot;
    use url::Url;

    #[derive(Debug)]
    pub struct UdpServer {
        inner: Arc<InnerServer>,
    }

    impl UdpServer {
        /// Create a new UDP tracker server instance for the given port.
        pub async fn with_port(port: u16) -> Result<Self> {
            let listener = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, port)).await?;
            let addr = listener.local_addr()?;
            let ip = if addr.ip().is_unspecified() {
                Ipv4Addr::LOCALHOST.into()
            } else {
                addr.ip()
            };
            let url = Url::parse(&format!("udp://{}:{}/announce", ip, addr.port()))?;
            let inner = Arc::new(InnerServer {
                base: BaseServer::new(TrackerHandle::new(), Duration::from_secs(15)),
                addr,
                url,
                socket: listener,
                connections: Default::default(),
            });

            let main_inner = inner.clone();
            tokio::spawn(async move { main_inner.run().await });

            Ok(Self { inner })
        }
    }

    #[async_trait]
    impl TrackerListener for UdpServer {
        async fn accept(&self) -> Option<ServerRequest> {
            self.inner.base.accept().await
        }

        fn addr(&self) -> &SocketAddr {
            &self.inner.addr
        }

        fn url(&self) -> &Url {
            &self.inner.url
        }

        fn metrics(&self) -> &ConnectionMetrics {
            &self.inner.base.metrics
        }

        fn close(&self) {
            self.inner.base.close();
        }
    }

    #[derive(Debug, Display)]
    #[display("{}", base.handle)]
    struct InnerServer {
        base: BaseServer,
        addr: SocketAddr,
        url: Url,
        socket: UdpSocket,
        connections: Mutex<HashMap<SocketAddr, u64>>,
    }

    impl InnerServer {
        /// Run the main loop of the UDP server.
        async fn run(&self) {
            loop {
                let mut buffer = vec![0u8; PACKET_SIZE];
                select! {
                    _ = self.base.cancellation_token.cancelled() => break,
                    Ok((len, addr)) = self.socket.recv_from(&mut buffer) => self.on_packet(&buffer[..len], addr).await,
                }
            }

            debug!("Udp tracker server {} main loop ended", self);
        }

        async fn on_packet(&self, packet: &[u8], addr: SocketAddr) {
            trace!(
                "Udp tracker server {} received packet from {:?}",
                self,
                addr
            );
            self.base.metrics.bytes_in.inc_by(packet.len() as u64);
            let request = match RequestMessage::try_from(packet) {
                Ok(request) => request,
                Err(e) => {
                    debug!("Udp tracker server {} failed to parse packet, {}", self, e);
                    return;
                }
            };

            match request.payload {
                RequestPayload::Connect => self.on_connect(&request, &addr).await,
                RequestPayload::Announce(announce) => {
                    self.on_announce(
                        request.connection_id,
                        request.transaction_id,
                        announce,
                        &addr,
                    )
                    .await
                }
                RequestPayload::Scrape(scrape) => {
                    self.on_scrape(request.connection_id, request.transaction_id, scrape, &addr)
                        .await
                }
            }
        }

        async fn on_connect(&self, request: &RequestMessage, addr: &SocketAddr) {
            let mut connections = self.connections.lock().await;
            let connection_id = Self::generate_connection_id(&connections);
            let connection_id = connections.entry(*addr).or_insert(connection_id);

            trace!(
                "Udp tracker server {} assigned connection ID {} to {}",
                self,
                connection_id,
                addr
            );
            if let Err(e) = self
                .send(
                    ResponseMessage {
                        action: Action::Connect,
                        transaction_id: request.transaction_id,
                        payload: ResponsePayload::Connection(*connection_id),
                    },
                    addr,
                )
                .await
            {
                debug!(
                    "Udp tracker server {} failed to send connection response, {}",
                    self, e
                );
            }
        }

        async fn on_announce(
            &self,
            connection_id: u64,
            transaction_id: u32,
            announce: AnnounceRequest,
            addr: &SocketAddr,
        ) {
            let send_result = match self.do_announce(&connection_id, announce, addr).await {
                Ok(response) => {
                    self.send(
                        ResponseMessage {
                            transaction_id,
                            action: Action::Announce,
                            payload: response,
                        },
                        addr,
                    )
                    .await
                }
                Err(e) => {
                    debug!(
                        "Udp tracker server {} received invalid announce message, {}",
                        self, e
                    );
                    self.send(
                        ResponseMessage {
                            transaction_id,
                            action: Action::Error,
                            payload: ResponsePayload::Error(e.to_string()),
                        },
                        addr,
                    )
                    .await
                }
            };

            if let Err(e) = send_result {
                debug!("Udp tracker server {} failed to send response, {}", self, e);
            }
        }

        async fn do_announce(
            &self,
            connection_id: &u64,
            announce: AnnounceRequest,
            addr: &SocketAddr,
        ) -> Result<ResponsePayload> {
            self.assert_connection(connection_id, addr).await?;
            let info_hash = InfoHash::try_from_bytes(&announce.info_hash)
                .map_err(|e| TrackerError::Parse(e.to_string()))?;
            let peer_id = PeerId::try_from(announce.peer_id.as_slice())
                .map_err(|e| TrackerError::Parse(e.to_string()))?;

            let (tx, rx) = oneshot::channel();
            self.base
                .request(ServerRequest::Announcement {
                    addr: *addr,
                    request: Announcement {
                        info_hash,
                        peer_id,
                        peer_port: announce.listen_port,
                        event: announce.event,
                        bytes_completed: announce.downloaded,
                        bytes_remaining: announce.left,
                    },
                    response: tx,
                })
                .await;

            let response = self.base.await_response(rx).await?;
            Ok(ResponsePayload::Announce(AnnounceResponse {
                interval: response.interval_seconds as u32,
                leechers: response.leechers as u32,
                seeders: response.seeders as u32,
                peers: response.peers,
            }))
        }

        async fn on_scrape(
            &self,
            connection_id: u64,
            transaction_id: u32,
            scrape: ScrapeRequest,
            addr: &SocketAddr,
        ) {
            let send_result = match self.do_scrape(&connection_id, scrape, addr).await {
                Ok(response) => {
                    self.send(
                        ResponseMessage {
                            transaction_id,
                            action: Action::Scrape,
                            payload: response,
                        },
                        addr,
                    )
                    .await
                }
                Err(e) => {
                    self.send(
                        ResponseMessage {
                            transaction_id,
                            action: Action::Error,
                            payload: ResponsePayload::Error(e.to_string()),
                        },
                        addr,
                    )
                    .await
                }
            };

            if let Err(e) = send_result {
                debug!("Udp tracker server {} failed to send response, {}", self, e);
            }
        }

        async fn do_scrape(
            &self,
            connection_id: &u64,
            scrape: ScrapeRequest,
            addr: &SocketAddr,
        ) -> Result<ResponsePayload> {
            self.assert_connection(connection_id, addr).await?;

            let (tx, rx) = oneshot::channel();
            self.base
                .request(ServerRequest::Scrape {
                    request: scrape.hashes,
                    response: tx,
                })
                .await;

            let response = self.base.await_response(rx).await?;
            Ok(ResponsePayload::Scrape(ScrapeResponse {
                metrics: response
                    .files
                    .into_values()
                    .map(|e| ScrapeFileMetrics {
                        seeders: e.downloaded,
                        completed: e.complete,
                        leechers: e.incomplete,
                    })
                    .collect(),
            }))
        }

        async fn assert_connection(&self, connection_id: &u64, addr: &SocketAddr) -> Result<()> {
            let connections = self.connections.lock().await;
            let known_connection_id = match connections.get(addr) {
                None => {
                    return Err(TrackerError::Connection(
                        "connection not established".to_string(),
                    ))
                }
                Some(id) => id,
            };

            if connection_id != known_connection_id {
                return Err(TrackerError::Connection(
                    "connection ID mismatch".to_string(),
                ));
            }

            Ok(())
        }

        async fn send(&self, response: ResponseMessage, addr: &SocketAddr) -> Result<()> {
            let bytes: Vec<u8> = response.try_into()?;
            let len = self.socket.send_to(bytes.as_slice(), addr).await?;
            trace!("Udp tracker server {} sent {} bytes to {}", self, len, addr);
            Ok(())
        }

        fn generate_connection_id(connections: &HashMap<SocketAddr, u64>) -> u64 {
            let mut rng = rng();
            loop {
                let connection_id = rng.random::<u64>() + 1;
                if !connections.values().any(|&id| id == connection_id)
                    && connection_id != PROTOCOL_ID
                {
                    return connection_id;
                }
            }
        }
    }
}

#[derive(Debug)]
struct AddressManager {
    addr_cursor: Mutex<usize>,
    addrs: Vec<SocketAddr>,
}

impl AddressManager {
    pub fn new(addrs: &[SocketAddr]) -> Self {
        Self {
            addr_cursor: Default::default(),
            addrs: addrs.to_vec(),
        }
    }

    /// Get the next available address from the address manager.
    /// It returns [None] if there are no more addresses left.
    pub async fn next_addr(&self) -> Option<&SocketAddr> {
        let mut cursor = self.addr_cursor.lock().await;

        if self.addrs.is_empty() || *cursor >= self.addrs.len() {
            return None;
        }

        let addr = self.addrs.get(*cursor);
        *cursor += 1;
        addr
    }
}

impl Display for AddressManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.addrs)
    }
}

/// Contains the session information about an active udp connection.
#[derive(Debug)]
struct UdpConnectionSession {
    transaction_id: u32,
    connection_id: u64,
    socket: UdpSocket,
}

#[derive(Debug)]
struct RequestMessage {
    connection_id: u64,
    transaction_id: u32,
    action: Action,
    payload: RequestPayload,
}

impl TryFrom<RequestMessage> for Vec<u8> {
    type Error = TrackerError;

    fn try_from(message: RequestMessage) -> Result<Self> {
        let mut buffer: Vec<u8> = Vec::with_capacity(64);

        // write the headers
        buffer.write_u64::<BigEndian>(message.connection_id)?;
        buffer.write_u32::<BigEndian>(message.action as u32)?;
        buffer.write_u32::<BigEndian>(message.transaction_id)?;

        // write the payload
        let payload: Vec<u8> = message.payload.try_into()?;
        buffer.write_all(&payload)?;

        Ok(buffer)
    }
}

impl TryFrom<&[u8]> for RequestMessage {
    type Error = TrackerError;

    fn try_from(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 16 {
            return Err(TrackerError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "Packet too short",
            )));
        }

        let mut cursor = Cursor::new(bytes);
        let connection_id = cursor.read_u64::<BigEndian>()?;
        let action = cursor.read_u32::<BigEndian>()?.try_into()?;
        let transaction_id = cursor.read_u32::<BigEndian>()?;
        let payload = Payload::try_from(cursor, &action)?;

        Ok(Self {
            connection_id,
            action,
            transaction_id,
            payload,
        })
    }
}

#[derive(Debug)]
struct ResponseMessage {
    transaction_id: u32,
    action: Action,
    payload: ResponsePayload,
}

impl TryFrom<ResponseMessage> for Vec<u8> {
    type Error = TrackerError;

    fn try_from(message: ResponseMessage) -> Result<Self> {
        let mut buffer: Vec<u8> = Vec::with_capacity(64);

        // write the headers
        buffer.write_u32::<BigEndian>(message.action as u32)?;
        buffer.write_u32::<BigEndian>(message.transaction_id)?;

        // write the payload
        let payload: Vec<u8> = message.payload.try_into()?;
        buffer.write_all(&payload)?;

        Ok(buffer)
    }
}

impl TryFrom<&[u8]> for ResponseMessage {
    type Error = TrackerError;

    fn try_from(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 8 {
            return Err(TrackerError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "Packet too short",
            )));
        }

        let mut cursor = Cursor::new(bytes);
        let action = cursor.read_u32::<BigEndian>()?.try_into()?;
        let transaction_id = cursor.read_u32::<BigEndian>()?;
        let payload = Payload::try_from(cursor, &action)?;

        Ok(Self {
            action,
            transaction_id,
            payload,
        })
    }
}

/// The message payload of a UDP tracker message.
trait Payload: Debug + TryInto<Vec<u8>, Error = TrackerError> {
    /// Converse the given cursor into a payload based on the message action.
    fn try_from(cursor: Cursor<&[u8]>, action: &Action) -> Result<Self>;
}

#[repr(u32)]
#[derive(Debug, Display, Clone)]
enum Action {
    #[display("connect")]
    Connect = 0,
    #[display("announce")]
    Announce = 1,
    #[display("scrape")]
    Scrape = 2,
    #[display("error")]
    Error = 3,
}

impl TryFrom<u32> for Action {
    type Error = TrackerError;

    fn try_from(value: u32) -> Result<Self> {
        match value {
            0 => Ok(Action::Connect),
            1 => Ok(Action::Announce),
            2 => Ok(Action::Scrape),
            3 => Ok(Action::Error),
            _ => Err(TrackerError::from(io::Error::from(
                io::ErrorKind::InvalidData,
            ))),
        }
    }
}

/// The UDP request message to send to a tracker.
#[derive(Debug)]
enum RequestPayload {
    Connect,
    Announce(AnnounceRequest),
    Scrape(ScrapeRequest),
}

impl RequestPayload {
    /// Returns the action of the request.
    fn action(&self) -> Action {
        match self {
            RequestPayload::Connect => Action::Connect,
            RequestPayload::Announce(_) => Action::Announce,
            RequestPayload::Scrape(_) => Action::Scrape,
        }
    }
}

impl TryFrom<RequestPayload> for Vec<u8> {
    type Error = TrackerError;

    fn try_from(message: RequestPayload) -> Result<Self> {
        match message {
            RequestPayload::Connect => Ok(vec![]),
            RequestPayload::Announce(announce) => announce.try_into(),
            RequestPayload::Scrape(request) => request.try_into(),
        }
    }
}

impl Payload for RequestPayload {
    fn try_from(cursor: Cursor<&[u8]>, action: &Action) -> Result<Self> {
        match action {
            Action::Connect => Ok(RequestPayload::Connect),
            Action::Announce => AnnounceRequest::try_from(cursor).map(RequestPayload::Announce),
            Action::Scrape => ScrapeRequest::try_from(cursor).map(RequestPayload::Scrape),
            _ => Err(TrackerError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid action for request message",
            ))),
        }
    }
}

/// The UDP response message received from a tracker.
#[derive(Debug)]
enum ResponsePayload {
    Connection(u64),
    Announce(AnnounceResponse),
    Scrape(ScrapeResponse),
    Error(String),
}

impl TryFrom<ResponsePayload> for Vec<u8> {
    type Error = TrackerError;

    fn try_from(value: ResponsePayload) -> Result<Self> {
        match value {
            ResponsePayload::Connection(connection_id) => {
                let mut buffer = Vec::new();
                buffer.write_u64::<BigEndian>(connection_id)?;
                Ok(buffer)
            }
            ResponsePayload::Announce(announce) => announce.try_into(),
            ResponsePayload::Scrape(scrape) => scrape.try_into(),
            ResponsePayload::Error(message) => Ok(message.as_bytes().to_vec()),
        }
    }
}

impl Payload for ResponsePayload {
    fn try_from(mut cursor: Cursor<&[u8]>, action: &Action) -> Result<Self> {
        match action {
            Action::Connect => {
                let connection_id = cursor.read_u64::<BigEndian>()?;
                Ok(ResponsePayload::Connection(connection_id))
            }
            Action::Announce => AnnounceResponse::try_from(cursor).map(ResponsePayload::Announce),
            Action::Scrape => ScrapeResponse::try_from(cursor).map(ResponsePayload::Scrape),
            Action::Error => {
                let mut message = String::new();
                cursor.read_to_string(&mut message)?;
                Ok(ResponsePayload::Error(message))
            }
        }
    }
}

#[derive(Debug)]
struct AnnounceRequest {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub downloaded: u64,
    pub uploaded: u64,
    pub left: u64,
    pub event: AnnounceEvent,
    pub ip_address: u32,
    pub key: u32,
    pub num_want: u32,
    pub listen_port: u16,
}

impl TryFrom<AnnounceRequest> for Vec<u8> {
    type Error = TrackerError;

    fn try_from(announce: AnnounceRequest) -> Result<Self> {
        let mut buffer = Vec::new();
        buffer.write_all(announce.info_hash.as_ref())?;
        buffer.write_all(announce.peer_id.as_ref())?;
        buffer.write_u64::<BigEndian>(announce.downloaded)?;
        buffer.write_u64::<BigEndian>(announce.left)?;
        buffer.write_u64::<BigEndian>(announce.uploaded)?;
        buffer.write_u32::<BigEndian>(announce.event as u32)?;
        buffer.write_u32::<BigEndian>(announce.ip_address)?;
        buffer.write_u32::<BigEndian>(announce.key)?;
        buffer.write_u32::<BigEndian>(announce.num_want)?;
        buffer.write_u16::<BigEndian>(announce.listen_port)?;
        Ok(buffer)
    }
}

impl TryFrom<Cursor<&[u8]>> for AnnounceRequest {
    type Error = TrackerError;

    fn try_from(mut cursor: Cursor<&[u8]>) -> Result<Self> {
        let mut info_hash = [0u8; 20];
        cursor.read_exact(&mut info_hash)?;
        let mut peer_id = [0u8; 20];
        cursor.read_exact(&mut peer_id)?;
        let downloaded = cursor.read_u64::<BigEndian>()?;
        let left = cursor.read_u64::<BigEndian>()?;
        let uploaded = cursor.read_u64::<BigEndian>()?;
        let event_raw = cursor.read_u32::<BigEndian>()?;
        let event = (event_raw as u8).try_into()?;
        let ip_address = cursor.read_u32::<BigEndian>()?;
        let key = cursor.read_u32::<BigEndian>()?;
        let num_want = cursor.read_u32::<BigEndian>()?;
        let listen_port = cursor.read_u16::<BigEndian>()?;

        Ok(Self {
            info_hash,
            peer_id,
            downloaded,
            uploaded,
            left,
            event,
            ip_address,
            key,
            num_want,
            listen_port,
        })
    }
}

#[derive(Debug)]
struct AnnounceResponse {
    /// The interval in seconds between successive announcements
    pub interval: u32,
    /// The number of peers with incomplete downloads
    pub leechers: u32,
    /// The number of peers with complete downloads
    pub seeders: u32,
    /// The discovered peers address for the tracker
    pub peers: Vec<SocketAddr>,
}

impl TryFrom<AnnounceResponse> for Vec<u8> {
    type Error = TrackerError;

    fn try_from(value: AnnounceResponse) -> Result<Self> {
        let mut buffer = Vec::new();
        buffer.write_u32::<BigEndian>(value.interval)?;
        buffer.write_u32::<BigEndian>(value.leechers)?;
        buffer.write_u32::<BigEndian>(value.seeders)?;

        for peer in value.peers {
            match peer.ip() {
                IpAddr::V4(ip) => buffer.write_u32::<BigEndian>(ip.to_bits())?,
                IpAddr::V6(ip) => buffer.write_u128::<BigEndian>(ip.to_bits())?,
            }
            buffer.write_u16::<BigEndian>(peer.port())?;
        }

        Ok(buffer)
    }
}

impl TryFrom<Cursor<&[u8]>> for AnnounceResponse {
    type Error = TrackerError;

    fn try_from(mut cursor: Cursor<&[u8]>) -> Result<Self> {
        let interval = cursor.read_u32::<BigEndian>()?;
        let leechers = cursor.read_u32::<BigEndian>()?;
        let seeders = cursor.read_u32::<BigEndian>()?;

        let mut addrs = Vec::new();

        // we currently only support ipv4
        while let Ok(ip) = cursor.read_u32::<BigEndian>() {
            let port = cursor.read_u16::<BigEndian>()?;
            addrs.push(SocketAddrV4::new(Ipv4Addr::from(ip), port).into());
        }

        Ok(Self {
            interval,
            leechers,
            seeders,
            peers: addrs,
        })
    }
}

#[derive(Debug)]
struct ScrapeRequest {
    hashes: Vec<InfoHash>,
}

impl TryFrom<ScrapeRequest> for Vec<u8> {
    type Error = TrackerError;

    fn try_from(value: ScrapeRequest) -> Result<Self> {
        Ok(value
            .hashes
            .into_iter()
            .map(|e| e.short_info_hash_bytes())
            .map(|e| e.to_vec())
            .concat())
    }
}

impl TryFrom<Cursor<&[u8]>> for ScrapeRequest {
    type Error = TrackerError;

    fn try_from(mut cursor: Cursor<&[u8]>) -> Result<Self> {
        let mut hashes = Vec::new();

        while cursor.has_remaining() {
            let mut info_hash = [0u8; 20];
            cursor.read_exact(&mut info_hash)?;
            hashes.push(
                InfoHash::try_from_bytes(&info_hash)
                    .map_err(|e| TrackerError::Parse(e.to_string()))?,
            );
        }

        Ok(Self { hashes })
    }
}

#[derive(Debug, Default)]
struct ScrapeResponse {
    metrics: Vec<ScrapeFileMetrics>,
}

impl TryFrom<ScrapeResponse> for Vec<u8> {
    type Error = TrackerError;

    fn try_from(value: ScrapeResponse) -> Result<Self> {
        let mut buffer = Vec::new();
        for metric in value.metrics {
            buffer.write_u32::<BigEndian>(metric.seeders)?;
            buffer.write_u32::<BigEndian>(metric.completed)?;
            buffer.write_u32::<BigEndian>(metric.leechers)?;
        }
        Ok(buffer)
    }
}

impl TryFrom<Cursor<&[u8]>> for ScrapeResponse {
    type Error = TrackerError;

    fn try_from(mut cursor: Cursor<&[u8]>) -> Result<Self> {
        let mut scrape_response = ScrapeResponse::default();

        while cursor.has_remaining() {
            let seeders = cursor.read_u32::<BigEndian>()?;
            let completed = cursor.read_u32::<BigEndian>()?;
            let leechers = cursor.read_u32::<BigEndian>()?;

            scrape_response.metrics.push(ScrapeFileMetrics {
                seeders,
                completed,
                leechers,
            });
        }

        Ok(scrape_response)
    }
}

#[derive(Debug)]
struct ScrapeFileMetrics {
    seeders: u32,
    completed: u32,
    leechers: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::PeerId;
    use crate::torrent_metrics;
    use std::str::FromStr;

    mod client {
        use super::*;

        #[test]
        fn test_generate_transaction_id() {
            let result = UdpConnection::generate_transaction_id();

            assert_ne!(
                0, result,
                "expected the transaction id to be greater than 0"
            );
        }

        #[tokio::test]
        async fn test_udp_connect() {
            init_logger!();
            let (client, _server) = udp_connection_pair!();

            assert_ne!(
                PROTOCOL_ID, client.session.connection_id,
                "expected the connection ID to have been updated"
            );
        }
    }

    mod server {
        use super::*;
        use crate::tracker::TrackerListener;
        use rand::{rng, Rng};

        #[tokio::test]
        async fn test_invalid_connection_id() {
            init_logger!();
            let info_hash =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let (mut client, _server) = udp_connection_pair!();

            // modify the client connection id
            client.session.connection_id = rng().next_u64();

            // make an announcement to the server
            let result = client
                .announce(Announcement {
                    info_hash,
                    peer_id: PeerId::new(),
                    peer_port: 6881,
                    event: AnnounceEvent::Started,
                    bytes_completed: 0,
                    bytes_remaining: 0,
                })
                .await;
            match result {
                Ok(_) => assert!(false, "expected the request to be rejected"),
                Err(e) => {
                    assert!(
                        e.to_string().contains("connection ID mismatch"),
                        "expected request to be rejected due to invalid connection id"
                    );
                }
            }
        }

        #[tokio::test]
        async fn test_close() {
            init_logger!();
            let info_hash =
                InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
            let (client, server) = udp_connection_pair!();

            // close the server
            server.close();

            // try to scrape the server
            let result = client.scrape(&[info_hash]).await;
            match result {
                Ok(_) => assert!(false, "expected the request to be rejected"),
                Err(TrackerError::Io(e)) => {
                    assert_eq!(io::ErrorKind::TimedOut, e.kind());
                }
                Err(_) => assert!(
                    false,
                    "expected Err(TrackerError::Io), but got {:?}",
                    result
                ),
            }
        }
    }

    #[tokio::test]
    async fn test_udp_tracker_announce() {
        init_logger!();
        let info_hash =
            InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
        let (handle, client, server) = udp_tracker_pair!();

        // add a new peer to the server
        server
            .add_peer(
                info_hash.clone(),
                SocketAddr::from((Ipv4Addr::LOCALHOST, 9900)),
                PeerId::new(),
                9900,
                false,
            )
            .await;

        // register the torrent in the client
        client
            .add_torrent(
                PeerId::new(),
                6881,
                info_hash.clone(),
                torrent_metrics::Metrics::default(),
            )
            .await
            .expect("expected the torrent to be added");

        // announce the torrent
        let result = client
            .announce(handle, &info_hash, AnnounceEvent::Started)
            .await
            .expect("expected the announce to succeed");
        assert_eq!(
            1, result.total_leechers,
            "expected the announce to have 1 leecher"
        );
        assert_eq!(
            0, result.total_seeders,
            "expected the announce to have 1 seeder"
        );
        assert_eq!(
            1,
            result.peers.len(),
            "expected the announce to have 1 peer"
        );
    }

    #[tokio::test]
    async fn test_udp_tracker_scrape() {
        init_logger!();
        let info_hash =
            InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
        let (_, client, server) = udp_tracker_pair!();

        // add peers to the server
        server
            .add_peer(
                info_hash.clone(),
                SocketAddr::from((Ipv4Addr::LOCALHOST, 9968)),
                PeerId::new(),
                9968,
                true,
            )
            .await;
        server
            .add_peer(
                info_hash.clone(),
                SocketAddr::from(([127, 0, 0, 2], 11990)),
                PeerId::new(),
                11990,
                false,
            )
            .await;

        // register the torrent in the client
        client
            .add_torrent(
                PeerId::new(),
                6881,
                info_hash.clone(),
                torrent_metrics::Metrics::default(),
            )
            .await
            .expect("expected the torrent to be added");

        // scrape the info hash
        let result = client
            .scrape(&info_hash)
            .await
            .expect("expected the scrape to succeed");
        assert!(
            result.files.get(&info_hash).is_some(),
            "expected info hash {} to have been present in the scrape result",
            info_hash
        );
    }

    #[tokio::test]
    async fn test_udp_tracker_scrape_no_result() {
        init_logger!();
        let info_hash =
            InfoHash::from_str("urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7").unwrap();
        let (_, client, _server) = udp_tracker_pair!();

        // register the torrent in the client
        client
            .add_torrent(
                PeerId::new(),
                6881,
                info_hash.clone(),
                torrent_metrics::Metrics::default(),
            )
            .await
            .expect("expected the torrent to be added");

        // scrape the info hash
        let result = client
            .scrape(&info_hash)
            .await
            .expect("expected the scrape to succeed");
        assert!(
            result.files.get(&info_hash).is_some(),
            "expected info hash {} to have been present in the scrape result",
            info_hash
        );
    }

    #[tokio::test]
    async fn test_address_manager_next_addr() {
        let addrs = vec![SocketAddr::from(([127, 0, 0, 1], 6881))];
        let manager = AddressManager::new(&addrs);

        let result = manager.next_addr().await;
        assert_ne!(None, result, "expected an address to be returned");

        let result = manager.next_addr().await;
        assert_eq!(None, result, "expected no address to be returned");
    }
}
