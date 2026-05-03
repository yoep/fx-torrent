use crate::app::{FXKeyEvent, FXWidget};
use crate::torrent::widgets::file_selection::FileSelectionWidget;
use crate::torrent::widgets::{FilePriorityWidget, FilesWidget, PeersWidget, PriorityAction};
use async_trait::async_trait;
use fx_torrent::peer::{Peer, PeerHandle};
use fx_torrent::{File, FileIndex, FilePriority, PieceIndex, Torrent};
use ratatui::buffer::Buffer;
use ratatui::layout::Constraint::Percentage;
use ratatui::layout::{Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph, Widget};
use ratatui::Frame;

#[derive(Debug)]
pub struct ContentWidget {
    torrent: Torrent,
    loading: LoadingWidget,
    file_picker: FileSelectionWidget,
    details: DetailsWidget,
    peers_widget: PeersWidget,
    state: State,
}

impl ContentWidget {
    pub fn new(torrent: Torrent) -> Self {
        Self {
            torrent: torrent.clone(),
            loading: LoadingWidget::new(),
            file_picker: FileSelectionWidget::new(),
            details: DetailsWidget::new(torrent),
            peers_widget: PeersWidget::new(),
            state: State::Loading,
        }
    }

    pub async fn add_peer(&mut self, peer: Peer) {
        self.peers_widget.add_peer(peer).await;
    }

    pub async fn remove_peer(&mut self, handle: &PeerHandle) {
        self.peers_widget.remove_peer(handle);
    }

    pub fn on_files_changed(&mut self, files: Vec<File>) {
        self.file_picker.set_files(files.clone());
        self.details.on_files_changed(files);

        if self.state == State::Loading {
            self.state = State::Picker;
        }
    }

    pub fn on_piece_completed(&mut self, piece: &PieceIndex) {
        self.details.on_piece_completed(piece);
    }

    pub fn on_priorities_changed(&mut self, priorities: &[(FileIndex, FilePriority)]) {
        self.details.on_priorities_changed(priorities);
    }
}

#[async_trait]
impl FXWidget for ContentWidget {
    fn name(&self) -> &str {
        "Torrent contents"
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn tick(&mut self) {
        self.peers_widget.tick().await;

        match &self.state {
            State::Loading => self.loading.tick().await,
            State::Picker => {
                if let Some(priorities) = self.file_picker.on_action() {
                    self.torrent.prioritize_files(priorities).await;
                    self.torrent.resume().await;
                    self.state = State::Details;
                }
            }
            State::Details => self.details.tick().await,
        }
    }

    fn on_key_event(&mut self, event: FXKeyEvent) {
        match &self.state {
            State::Loading => {}
            State::Picker => self.file_picker.on_key_event(event),
            State::Details => self.details.on_key_event(event),
        }
    }

    fn on_paste_event(&mut self, _: String) {
        // no-op
    }

    fn render(&mut self, frame: &mut Frame, area: Rect) {
        let content = Layout::horizontal([Percentage(60), Percentage(40)]);
        let [files_area, peers_area] = content.areas(area);

        // render the files area
        match &self.state {
            State::Loading => self.loading.render(files_area, frame.buffer_mut()),
            State::Picker => self.file_picker.render(files_area, frame.buffer_mut()),
            State::Details => self.details.render(files_area, frame.buffer_mut()),
        }
        // render the peers area
        self.peers_widget.render(peers_area, frame.buffer_mut());
    }
}

#[derive(Debug)]
struct LoadingWidget {
    cursor: usize,
}

impl LoadingWidget {
    fn new() -> Self {
        Self { cursor: 0 }
    }

    async fn tick(&mut self) {
        self.cursor = (self.cursor + 1) % 4;
    }
}

impl Widget for &LoadingWidget {
    fn render(self, area: Rect, buf: &mut Buffer)
    where
        Self: Sized,
    {
        const SPINNER_CHARS: &[&str] = &["|", "/", "-", "\\"];
        let spinner_symbol = SPINNER_CHARS[self.cursor % SPINNER_CHARS.len()];

        Paragraph::new(Line::from(vec![
            Span::from(format!("{} ", spinner_symbol)),
            Span::from("Loading, please wait..."),
        ]))
        .block(Block::bordered().title(" Files "))
        .render(area, buf);
    }
}

#[derive(Debug)]
struct DetailsWidget {
    torrent: Torrent,
    files_widget: FilesWidget,
    priority_widget: FilePriorityWidget,
    state: DetailsState,
}

impl DetailsWidget {
    fn new(torrent: Torrent) -> Self {
        Self {
            torrent,
            files_widget: FilesWidget::new(),
            priority_widget: FilePriorityWidget::new(),
            state: DetailsState::Files,
        }
    }

    fn on_key_event(&mut self, event: FXKeyEvent) {
        match self.state {
            DetailsState::Files => self.files_widget.on_key_event(event),
            DetailsState::Priority => self.priority_widget.on_key_event(event),
        }
    }

    fn on_files_changed(&mut self, files: Vec<File>) {
        self.files_widget.on_files_changed(files);
    }

    fn on_piece_completed(&mut self, piece: &PieceIndex) {
        self.files_widget.on_piece_completed(piece);
    }
    fn on_priorities_changed(&mut self, priorities: &[(FileIndex, FilePriority)]) {
        self.files_widget.on_priorities_changed(priorities);
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn tick(&mut self) {
        match self.state {
            DetailsState::Files => {
                if let Some(index) = self.files_widget.on_file_selected() {
                    if let Some(file) = self.torrent.file(&index).await {
                        self.priority_widget.set_file(file.index);
                        self.priority_widget.select(file.priority);
                        self.state = DetailsState::Priority;
                    }
                }
            }
            DetailsState::Priority => match self.priority_widget.on_action() {
                Some(PriorityAction::Ok((file, priority))) => {
                    self.torrent.prioritize_files(vec![(file, priority)]).await;
                    self.state = DetailsState::Files;
                }
                Some(PriorityAction::Cancel) => {
                    self.state = DetailsState::Files;
                }
                None => {}
            },
        }
    }
}

impl Widget for &mut DetailsWidget {
    fn render(self, area: Rect, buf: &mut Buffer)
    where
        Self: Sized,
    {
        match self.state {
            DetailsState::Files => self.files_widget.render(area, buf),
            DetailsState::Priority => self.priority_widget.render(area, buf),
        }
    }
}

#[derive(Debug, PartialEq)]
enum State {
    Loading,
    Picker,
    Details,
}

#[derive(Debug, PartialEq)]
enum DetailsState {
    Files,
    Priority,
}
