use crate::widgets::print_string_len;
use fx_callback::{Callback, Subscription};
use fx_torrent::peer::{Peer, PeerClientInfo, PeerEvent, PeerHandle, PeerState};
use fx_torrent::{format_bytes, TorrentPeer};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::prelude::{Color, Line, Style, Widget};
use ratatui::widgets::{Block, List, ListItem};
use std::collections::HashMap;
use std::time::{Duration, Instant};

const REMOVE_CLOSED_PEER_AFTER: Duration = Duration::from_secs(3);

#[derive(Debug)]
pub struct PeersWidget {
    peers: HashMap<PeerHandle, TorrentPeerData>,
}

impl PeersWidget {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }

    pub async fn add_peer(&mut self, peer: TorrentPeer) {
        let events_receiver = peer.subscribe();
        let state = peer.state().await;
        let is_seed = peer.is_seed().await;
        let metrics = peer.metrics();

        self.peers.insert(
            peer.handle(),
            TorrentPeerData {
                client: peer.client(),
                available_pieces: metrics.available_pieces.get(),
                client_interested: metrics.client_interested.get(),
                remote_interested: metrics.remote_interested.get(),
                client_choked: metrics.client_choked.get(),
                remote_choked: metrics.remote_choked.get(),
                bytes_in: metrics.bytes_in.rate(),
                bytes_in_total: metrics.bytes_in.total(),
                bytes_out: metrics.bytes_out.rate(),
                bytes_out_total: metrics.bytes_out.total(),
                peer,
                state,
                is_seed,
                events_receiver,
                closed_since: None,
            },
        );
    }

    pub fn remove_peer(&mut self, handle: &PeerHandle) {
        if let Some(peer) = self.peers.get_mut(handle) {
            peer.closed_since = Some(Instant::now());
        }
    }

    async fn handle_peer_events(&mut self) {
        for (_, peer_data) in &mut self.peers {
            while let Ok(event) = peer_data.events_receiver.try_recv() {
                match &*event {
                    PeerEvent::StateChanged(state) => {
                        peer_data.state = *state;
                    }
                    PeerEvent::Stats(metrics) => {
                        peer_data.available_pieces = metrics.available_pieces.get();
                        peer_data.client_interested = metrics.client_interested.get();
                        peer_data.remote_interested = metrics.remote_interested.get();
                        peer_data.client_choked = metrics.client_choked.get();
                        peer_data.remote_choked = metrics.remote_choked.get();
                        peer_data.bytes_in = metrics.bytes_in.rate();
                        peer_data.bytes_in_total = metrics.bytes_in.total();
                        peer_data.bytes_out = metrics.bytes_out.rate();
                        peer_data.bytes_out_total = metrics.bytes_out.total();
                        peer_data.is_seed = peer_data.peer.is_seed().await;
                    }
                    _ => {}
                }
            }
        }
    }

    fn handle_closed_peers(&mut self) {
        self.peers.retain(|_, peer_data| {
            peer_data
                .closed_since
                .as_ref()
                .unwrap_or(&Instant::now())
                .elapsed()
                <= REMOVE_CLOSED_PEER_AFTER
        });
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn tick(&mut self) {
        self.handle_peer_events().await;
        self.handle_closed_peers();
    }
}

impl Widget for &PeersWidget {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let items = self
            .peers
            .iter()
            .enumerate()
            .map(|(index, (_, peer))| {
                let color = if index % 2 == 0 {
                    Color::Rgb(80, 80, 50)
                } else {
                    Color::Rgb(80, 80, 80)
                };
                let seed_text = if peer.is_seed { " :: seed :: " } else { " :: " };
                let client_interest = if peer.client_interested { "I" } else { "" };
                let remote_interest = if peer.remote_interested { "i" } else { "" };
                let client_choked = if peer.client_choked { "C" } else { "" };
                let remote_choked = if peer.remote_choked { "c" } else { "" };

                ListItem::new(vec![
                    Line::from(vec![
                        print_string_len(peer.client.addr.to_string(), 21).into(),
                        " :: ".into(),
                        peer.client.connection_protocol.to_string().into(),
                        seed_text.into(),
                        peer_state_as_str(&peer.state).into(),
                    ])
                    .style(Style::new().bold()),
                    Line::from(vec![
                        format!(
                            "down: {}/s ({})",
                            format_bytes(peer.bytes_in as usize),
                            format_bytes(peer.bytes_in_total as usize)
                        )
                        .into(),
                        " - ".into(),
                        format!(
                            "up: {}/s ({})",
                            format_bytes(peer.bytes_out as usize),
                            format_bytes(peer.bytes_out_total as usize)
                        )
                        .into(),
                        " - ".into(),
                        format!("pieces: {}", peer.available_pieces).into(),
                        " - ".into(),
                        format!(
                            "{}{}{}{}",
                            client_interest, remote_interest, client_choked, remote_choked
                        )
                        .into(),
                    ]),
                ])
                .style(Style::new().bg(color))
            })
            .collect::<Vec<ListItem>>();

        let peers_list = List::new(items).block(Block::bordered().title(" Peers "));

        Widget::render(peers_list, area, buf);
    }
}

#[derive(Debug)]
struct TorrentPeerData {
    peer: TorrentPeer,
    client: PeerClientInfo,
    state: PeerState,
    is_seed: bool,
    available_pieces: u64,
    client_interested: bool,
    remote_interested: bool,
    client_choked: bool,
    remote_choked: bool,
    bytes_in: u32,
    bytes_in_total: u64,
    bytes_out: u32,
    bytes_out_total: u64,
    events_receiver: Subscription<PeerEvent>,
    closed_since: Option<Instant>,
}

fn peer_state_as_str(state: &PeerState) -> &'static str {
    match state {
        PeerState::Handshake => "Handshake",
        PeerState::RetrievingMetadata => "Retrieving metadata",
        PeerState::Paused => "Paused",
        PeerState::Idle => "Idle",
        PeerState::Downloading => "Downloading",
        PeerState::Uploading => "Uploading",
        PeerState::Error => "Error",
        PeerState::Closed => "Closed",
    }
}
