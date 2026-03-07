use crate::app::FXKeyEvent;
use crate::torrent::command::TorrentInfoCommand;
use crate::torrent::widgets::priority_text;
use crossterm::event::KeyCode;
use fx_torrent::{FileIndex, FilePriority};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::prelude::{Color, StatefulWidget, Style, Widget};
use ratatui::widgets::{Block, Borders, List, ListState};
use std::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug)]
pub struct FilePriorityWidget {
    file: FileIndex,
    priorities: Vec<FilePriority>,
    state: Mutex<ListState>,
    command_sender: UnboundedSender<TorrentInfoCommand>,
}

impl FilePriorityWidget {
    pub fn new(command_sender: UnboundedSender<TorrentInfoCommand>) -> Self {
        Self {
            file: FileIndex::default(),
            priorities: FilePriority::iter().collect(),
            state: Default::default(),
            command_sender,
        }
    }

    pub fn set_file(&mut self, file: FileIndex) {
        self.file = file;
    }

    pub fn select(&mut self, priority: FilePriority) {
        if let Ok(mut state) = self.state.lock() {
            state.select(Some(
                self.priorities
                    .iter()
                    .position(|e| *e == priority)
                    .unwrap_or(0),
            ));
        }
    }

    fn selected(&self) -> FilePriority {
        let offset = self
            .state
            .lock()
            .ok()
            .and_then(|e| e.selected())
            .unwrap_or_default();
        self.priorities[offset]
    }

    pub fn on_key_event(&mut self, event: FXKeyEvent) {
        match event.key_code() {
            KeyCode::Esc | KeyCode::Backspace => {
                let _ = self.command_sender.send(TorrentInfoCommand::ShowFiles);
            }
            KeyCode::Enter => {
                let _ = self.command_sender.send(TorrentInfoCommand::UpdatePriority(
                    self.file,
                    self.selected(),
                ));
                let _ = self.command_sender.send(TorrentInfoCommand::ShowFiles);
            }
            KeyCode::Up => {
                if let Ok(mut state) = self.state.lock() {
                    let offset = state.selected().unwrap_or(0).saturating_sub(1);
                    state.select(Some(offset));
                }
            }
            KeyCode::Down => {
                if let Ok(mut state) = self.state.lock() {
                    let selected = state.selected().unwrap_or(0).saturating_add(1);
                    if selected <= self.priorities.len() - 1 {
                        state.select(Some(selected));
                    }
                }
            }
            _ => {}
        }
    }
}

impl Widget for &FilePriorityWidget {
    fn render(self, area: Rect, buf: &mut Buffer)
    where
        Self: Sized,
    {
        let items = self
            .priorities
            .iter()
            .map(|e| priority_text(e))
            .collect::<Vec<_>>();
        let menu_list = List::new(items)
            .block(Block::new().title("File priority").borders(Borders::ALL))
            .highlight_style(Style::new().bg(Color::DarkGray));

        let mut state = self.state.lock().expect("Mutex poisoned");
        StatefulWidget::render(menu_list, area, buf, &mut state);
    }
}
