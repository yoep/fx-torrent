use crate::app::FXKeyEvent;
use crate::torrent::widgets::priority_text;
use crossterm::event::KeyCode;
use fx_torrent::{format_bytes, File, FileIndex, FilePriority, PieceIndex};
use ratatui::buffer::Buffer;
use ratatui::layout::Constraint::{Fill, Length};
use ratatui::layout::Rect;
use ratatui::prelude::{Color, StatefulWidget, Style, Widget};
use ratatui::widgets::{Block, Cell, HighlightSpacing, Row, Table, TableState};
use std::ops::Range;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

#[derive(Debug)]
pub struct FilesWidget {
    files: Vec<TorrentFileData>,
    sender: UnboundedSender<FileIndex>,
    receiver: UnboundedReceiver<FileIndex>,
    state: TableState,
}

impl FilesWidget {
    pub fn new() -> Self {
        let (sender, receiver) = unbounded_channel();
        Self {
            files: vec![],
            sender,
            receiver,
            state: TableState::new().with_selected(0),
        }
    }

    fn selected_index(&self) -> usize {
        self.state.selected().unwrap_or(0)
    }

    pub fn on_key_event(&mut self, event: FXKeyEvent) {
        match event.key_code() {
            KeyCode::Up => {
                let offset = self.state.selected().unwrap_or(0).saturating_sub(1);
                self.state.select(Some(offset));
            }
            KeyCode::Down => {
                let offset = self
                    .state
                    .selected()
                    .unwrap_or(0)
                    .saturating_add(1)
                    .min(self.files.len().saturating_sub(1));
                self.state.select(Some(offset));
            }
            KeyCode::Enter => {
                let selected = self.selected_index();
                if let Some(file) = self.files.get(selected) {
                    let _ = self.sender.send(file.index);
                }
            }
            _ => {}
        }
    }

    pub fn on_piece_completed(&mut self, piece: &PieceIndex) {
        for file in &mut self.files {
            if file.pieces.contains(piece) {
                file.completed_pieces += 1;
                file.completed_percentage =
                    ((file.completed_pieces as f32) / (file.total_pieces as f32)) * 100f32;
            }
        }
    }

    pub fn on_priorities_changed(&mut self, priorities: &[(FileIndex, FilePriority)]) {
        for priority in priorities {
            if let Some(file) = self.files.iter_mut().find(|e| e.index == priority.0) {
                file.priority = priority.1;
            }
        }
    }

    pub fn on_files_changed(&mut self, files: Vec<File>) {
        self.files = files
            .into_iter()
            .map(|file| TorrentFileData {
                index: file.index,
                name: file.filename().to_string(),
                size: file.len(),
                priority: file.priority,
                pieces: file.pieces.clone(),
                completed_percentage: 0.0,
                completed_pieces: 0,
                total_pieces: file.pieces.len(),
            })
            .collect()
    }

    pub fn on_file_selected(&mut self) -> Option<FileIndex> {
        self.receiver.try_recv().ok()
    }
}

impl Widget for &mut FilesWidget {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let header = vec!["Name", "Priority", "Size", "Progress", "Pieces"]
            .into_iter()
            .map(Cell::from)
            .collect::<Row>()
            .style(Style::new().bg(Color::DarkGray).fg(Color::White));
        let rows = self
            .files
            .iter()
            .enumerate()
            .map(|(index, file)| {
                let color = if index % 2 == 0 {
                    Color::Rgb(80, 80, 50)
                } else {
                    Color::Rgb(80, 80, 80)
                };

                Row::new(vec![
                    file.name.clone(),
                    priority_text(&file.priority).to_string(),
                    format_bytes(file.size),
                    format!("{:0.2}%", file.completed_percentage),
                    format!("{}/{}", file.completed_pieces, file.total_pieces),
                ])
                .style(Style::new().bg(color))
            })
            .collect::<Vec<Row>>();

        let table = Table::new(
            rows,
            [Fill(1), Length(12), Length(16), Length(20), Length(16)],
        )
        .header(header)
        .block(Block::bordered().title(" Files "))
        .row_highlight_style(Style::new().bg(Color::Yellow).fg(Color::DarkGray))
        .highlight_spacing(HighlightSpacing::Always);

        StatefulWidget::render(table, area, buf, &mut self.state);
    }
}

#[derive(Debug)]
struct TorrentFileData {
    index: FileIndex,
    name: String,
    size: usize,
    priority: FilePriority,
    pieces: Range<PieceIndex>,
    completed_percentage: f32,
    completed_pieces: usize,
    total_pieces: usize,
}
