use crate::app::{FXKeyEvent, FXWidget};
use crate::torrent::command::TorrentInfoCommand;
use crate::torrent::widgets::file_selection::FileSelectionWidget;
use crate::torrent::widgets::{FilePriorityWidget, FilesWidget, PeersWidget};
use async_trait::async_trait;
use fx_torrent::peer::PeerHandle;
use fx_torrent::{File, TorrentPeer};
use ratatui::buffer::Buffer;
use ratatui::layout::Constraint::Percentage;
use ratatui::layout::{Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph, Widget};
use ratatui::Frame;
use std::time::Instant;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug)]
pub struct ContentWidget {
    loading: LoadingWidget,
    file_picker: FileSelectionWidget,
    details: DetailsWidget,
    peers_widget: PeersWidget,
    state: State,
}

impl ContentWidget {
    pub fn new(command_sender: UnboundedSender<TorrentInfoCommand>) -> Self {
        Self {
            loading: LoadingWidget::new(),
            file_picker: FileSelectionWidget::new(),
            details: DetailsWidget::new(command_sender),
            peers_widget: PeersWidget::new(),
            state: State::Loading,
        }
    }

    pub async fn add_peer(&mut self, peer: TorrentPeer) {
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
}

#[async_trait]
impl FXWidget for ContentWidget {
    fn name(&self) -> &str {
        "Torrent contents"
    }

    async fn tick(&mut self) {
        self.peers_widget.tick().await;

        match &self.state {
            State::Loading => self.loading.tick().await,
            State::Picker => {}
            State::Details => {}
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
    last_tick: Instant,
}

impl LoadingWidget {
    fn new() -> Self {
        Self {
            cursor: 0,
            last_tick: Instant::now(),
        }
    }

    async fn tick(&mut self) {
        // if self.last_tick.elapsed() > Duration::from_millis(500) {
        self.cursor = (self.cursor + 1) % 4;
        self.last_tick = Instant::now();
        // }
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
    files_widget: FilesWidget,
    priority_widget: FilePriorityWidget,
    state: DetailsState,
}

impl DetailsWidget {
    fn new(command_sender: UnboundedSender<TorrentInfoCommand>) -> Self {
        Self {
            files_widget: FilesWidget::new(command_sender.clone()),
            priority_widget: FilePriorityWidget::new(command_sender),
            state: DetailsState::Files,
        }
    }

    fn on_key_event(&mut self, event: FXKeyEvent) {
        todo!()
    }

    fn on_files_changed(&mut self, files: Vec<File>) {
        self.files_widget.on_files_changed(files);
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
