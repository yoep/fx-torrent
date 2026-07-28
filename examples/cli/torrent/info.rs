use crate::app::{FXKeyEvent, FXWidget, PERFORMANCE_HISTORY};
use crate::torrent::data::TorrentData;
use crate::torrent::widgets::{AddPeerWidget, ContentWidget, PeerAction, ProgressWidget};
use crate::widgets::print_optional_string;
use async_trait::async_trait;
use crossterm::event::KeyCode;
use fx_callback::{Callback, Subscription};
use fx_torrent::prelude::*;
use log::{debug, error, info, warn};
use ratatui::layout::Constraint::{Fill, Length, Percentage};
use ratatui::layout::{Layout, Rect};
use ratatui::prelude::{Color, Line, Span, Style};
use ratatui::style::Stylize;
use ratatui::widgets::{Block, Paragraph, Sparkline, Widget};
use ratatui::Frame;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::timeout;

#[derive(Debug)]
pub struct TorrentInfoWidget {
    name: String,
    torrent: Torrent,
    add_peer: AddPeerWidget,
    content_widget: ContentWidget,
    progress_widget: ProgressWidget,
    event_receiver: Subscription<TorrentEvent>,
    data: TorrentData,
    state: State,
}

impl TorrentInfoWidget {
    /// Create a new info widget for the given [Torrent].
    pub async fn new(name: &str, torrent: Torrent) -> Self {
        let event_receiver = torrent.subscribe();
        let files = torrent.files().await;
        let data = if let Some(metadata) = torrent.metadata().await.ok() {
            TorrentData {
                info_hash: Some(metadata.info_hash.clone()),
                path: torrent.path().await,
                state: None,
                total_pieces: torrent.total_pieces().await as u64,
                completed_pieces: 0,
                wanted_size: metadata
                    .info
                    .as_ref()
                    .map(|e| e.len() as u64)
                    .unwrap_or_default(),
                wanted_completed_size: 0,
                total_files: files.len(),
                peers: 0,
                wasted: 0,
                down: vec![],
                up: vec![],
            }
        } else {
            Default::default()
        };
        let mut content_widget = ContentWidget::new(torrent.clone());
        if !files.is_empty() {
            content_widget.on_files_changed(files);
        }

        Self {
            name: name.to_string(),
            add_peer: AddPeerWidget::new(),
            content_widget,
            progress_widget: ProgressWidget::new(torrent.clone()),
            torrent,
            event_receiver,
            data,
            state: State::Content,
        }
    }

    async fn on_event(&mut self, event: &TorrentEvent) {
        let data = &mut self.data;

        match event {
            TorrentEvent::StateChanged(state) => {
                data.state = Some(*state);
            }
            TorrentEvent::MetadataChanged(metadata) => {
                data.info_hash = Some(metadata.info_hash.clone());
                data.path = self.torrent.path().await;

                if let Some(name) = metadata.name().map(|e| e.to_string()) {
                    self.name = name;
                }
                if let Some(info) = metadata.info.as_ref() {
                    data.wanted_size = info.len() as u64;
                }
            }
            TorrentEvent::PeerConnected(peer) => {
                data.peers = self.torrent.active_peer_connections().await;

                match timeout(Duration::from_secs(1), self.torrent.peer(&peer.addr)).await {
                    Ok(Some(peer)) => {
                        self.content_widget.add_peer(peer).await;
                    }
                    Ok(None) => {
                        warn!("Torrent {} failed to find peer {}", self.torrent, peer);
                    }
                    Err(_) => {
                        debug!(
                            "Torrent {} timed out waiting for peer {}",
                            self.torrent, peer
                        );
                    }
                }
            }
            TorrentEvent::PeerDisconnected(peer) => {
                data.peers = self.torrent.active_peer_connections().await;
                self.content_widget.remove_peer(&peer.addr).await;
            }
            TorrentEvent::PiecesChanged(total_pieces) => {
                data.total_pieces = *total_pieces as u64;
            }
            TorrentEvent::PieceCompleted(piece) => {
                data.completed_pieces = self.torrent.total_completed_pieces().await as u64;
                self.content_widget.on_piece_completed(piece);
            }
            TorrentEvent::PiecePrioritiesChanged => {
                let files = self.torrent.files().await;
                self.content_widget.on_priorities_changed(
                    files
                        .into_iter()
                        .map(|e| (e.index, e.priority))
                        .collect::<Vec<_>>()
                        .as_slice(),
                )
            }
            TorrentEvent::FilesChanged => {
                data.total_files = self.torrent.total_files().await.unwrap_or(0);
                self.content_widget
                    .on_files_changed(self.torrent.files().await);
            }
            TorrentEvent::Stats(stats) => {
                data.wanted_completed_size = stats.wanted_completed_size.get();
                data.wasted = stats.wasted.total();
                data.down.push(stats.download.rate() as u64);
                data.up.push(stats.upload.rate() as u64);

                if data.down.len() > PERFORMANCE_HISTORY {
                    data.down.remove(0);
                }
                if data.up.len() > PERFORMANCE_HISTORY {
                    data.up.remove(0);
                }
                info!("Torrent {} stats {}", self.torrent, stats);
            }
            _ => {}
        }
    }

    async fn add_torrent_peer(&self, addr: SocketAddr) {
        if let Err(e) = self.torrent.add_peer(addr).await {
            error!("Failed to add peer {}, {}", addr, e);
        }
    }

    fn toggle_pause_state(&self) {
        let torrent = self.torrent.clone();
        tokio::spawn(async move {
            if torrent.is_paused().await {
                torrent.resume().await;
            } else {
                torrent.pause().await;
            }
        });
    }

    /// Process any pending events from the torrent.
    async fn process_torrent_events(&mut self) {
        while let Ok(event) = self.event_receiver.try_recv() {
            self.on_event(&event).await;
        }
    }

    /// Tick the state widget which is currently active within the UI.
    async fn tick_active_state_widget(&mut self) {
        match self.state {
            State::AddPeer => match self.add_peer.on_action() {
                Some(PeerAction::Ok(addr)) => {
                    self.add_torrent_peer(addr).await;
                    self.state = State::Content;
                }
                Some(PeerAction::Cancel) => {
                    self.state = State::Content;
                }
                _ => {}
            },
            State::Content => self.content_widget.tick().await,
        }
    }
}

#[async_trait]
impl FXWidget for TorrentInfoWidget {
    fn name(&self) -> &str {
        &self.name
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn tick(&mut self) {
        self.process_torrent_events().await;
        self.progress_widget.tick().await;
        self.tick_active_state_widget().await;
    }

    fn on_key_event(&mut self, mut event: FXKeyEvent) {
        if self.state != State::AddPeer {
            match event.key_code() {
                KeyCode::Char('a') => {
                    event.consume();
                    self.state = State::AddPeer;
                    return;
                }
                KeyCode::Char('p') => {
                    event.consume();
                    self.toggle_pause_state();
                    return;
                }
                _ => {}
            }
        }

        match self.state {
            State::AddPeer => self.add_peer.on_key_event(event),
            State::Content => self.content_widget.on_key_event(event),
        }
    }

    fn on_paste_event(&mut self, text: String) {
        match self.state {
            State::AddPeer => self.add_peer.on_paste_event(text),
            State::Content => self.content_widget.on_paste_event(text),
        }
    }

    fn render(&mut self, frame: &mut Frame, area: Rect) {
        let main = Layout::vertical([Length(12), Length(4), Fill(1)]);
        let [header_area, progress_area, content_area] = main.areas(area);
        let header = Layout::horizontal([Percentage(50), Percentage(50)]);
        let [metadata_area, performance_area] = header.areas(header_area);
        let performance = Layout::vertical([Percentage(50), Percentage(50)]);
        let [down_performance, up_performance] = performance.areas(performance_area);

        let data = &self.data;

        // render the metadata
        Paragraph::new(vec![
            Line::from(vec![Span::from("Name: ").bold(), self.name.as_str().into()]),
            Line::from(vec![
                Span::from("State: ").bold(),
                print_optional_string(data.state.as_ref()).into(),
            ]),
            Line::from(vec![
                Span::from("Path: ").bold(),
                print_optional_string(self.data.path.as_ref().and_then(|e| e.to_str())).into(),
            ]),
            Line::from(vec![
                Span::from("Info hash: ").bold(),
                print_optional_string(self.data.info_hash.as_ref()).into(),
            ]),
            Line::from(vec![
                Span::from("Size: ").bold(),
                format!(
                    "{}/{}",
                    format_bytes(self.data.wanted_completed_size as usize),
                    format_bytes(self.data.wanted_size as usize)
                )
                .into(),
            ]),
            Line::from(vec![
                Span::from("Pieces: ").bold(),
                format!("{}/{}", self.data.completed_pieces, self.data.total_pieces).into(),
            ]),
            Line::from(vec![
                Span::from("Wasted: ").bold(),
                format_bytes(self.data.wasted as usize).into(),
            ]),
            Line::from(vec![
                Span::from("Files: ").bold(),
                self.data.total_files.to_string().into(),
            ]),
            Line::from(vec![
                Span::from("Connected peers: ").bold(),
                self.data.peers.to_string().into(),
            ]),
        ])
        .block(
            Block::bordered()
                .title(" Metadata ")
                .title_bottom(" Press P to pause/resume, A to add peer "),
        )
        .render(metadata_area, frame.buffer_mut());

        // render the performance
        Sparkline::default()
            .block(Block::bordered().title(format!(
                " Down: {}/s ",
                format_bytes(self.data.down.last().map(|e| *e as usize).unwrap_or(0))
            )))
            .data(&self.data.down)
            .style(Style::default().fg(Color::Yellow))
            .render(down_performance, frame.buffer_mut());

        Sparkline::default()
            .block(Block::bordered().title(format!(
                " Up: {}/s ",
                format_bytes(self.data.up.last().map(|e| *e as usize).unwrap_or(0))
            )))
            .data(&self.data.up)
            .style(Style::default().fg(Color::Yellow))
            .render(up_performance, frame.buffer_mut());

        // render the progress
        self.progress_widget.render(frame, progress_area);

        // render the contents of the torrent
        match self.state {
            State::AddPeer => self.add_peer.render(frame, content_area),
            State::Content => self.content_widget.render(frame, content_area),
        }
    }
}

#[derive(Debug, PartialEq)]
enum State {
    AddPeer,
    Content,
}
