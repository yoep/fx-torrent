use crate::app::FXKeyEvent;
use crate::torrent::widgets::priority_text;
use crate::torrent::ActionResult;
use crossterm::event::KeyCode;
use fx_torrent::{FileIndex, FilePriority};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::prelude::{Color, StatefulWidget, Style, Widget};
use ratatui::widgets::{Block, Borders, List, ListState};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

/// The action result type of the priority widget.
pub type PriorityAction = ActionResult<(FileIndex, FilePriority)>;

/// Widget which allows the user to configure the priority of a file.
#[derive(Debug)]
pub struct FilePriorityWidget {
    file: FileIndex,
    priorities: Vec<FilePriority>,
    sender: UnboundedSender<PriorityAction>,
    receiver: UnboundedReceiver<PriorityAction>,
    state: ListState,
}

impl FilePriorityWidget {
    pub fn new() -> Self {
        let (sender, receiver) = unbounded_channel();
        Self {
            file: FileIndex::default(),
            priorities: FilePriority::iter().collect(),
            sender,
            receiver,
            state: Default::default(),
        }
    }

    /// Set the file that is being prioritized.
    pub fn set_file(&mut self, file: FileIndex) {
        self.file = file;
    }

    /// Select the current priority of the file.
    pub fn select(&mut self, priority: FilePriority) {
        self.state.select(Some(
            self.priorities
                .iter()
                .position(|e| *e == priority)
                .unwrap_or(0),
        ));
    }

    fn selected(&self) -> FilePriority {
        let offset = self.state.selected().unwrap_or_default();
        self.priorities[offset]
    }

    pub fn on_key_event(&mut self, event: FXKeyEvent) {
        match event.key_code() {
            KeyCode::Esc | KeyCode::Backspace => {
                let _ = self.sender.send(PriorityAction::Cancel);
            }
            KeyCode::Enter => {
                let _ = self
                    .sender
                    .send(PriorityAction::Ok((self.file, self.selected())));
            }
            KeyCode::Up => {
                let offset = self.state.selected().unwrap_or(0).saturating_sub(1);
                self.state.select(Some(offset));
            }
            KeyCode::Down => {
                let selected = self.state.selected().unwrap_or(0).saturating_add(1);
                if selected <= self.priorities.len() - 1 {
                    self.state.select(Some(selected));
                }
            }
            _ => {}
        }
    }

    /// Returns the widget action result.
    pub fn on_action(&mut self) -> Option<PriorityAction> {
        self.receiver.try_recv().ok()
    }
}

impl Widget for &mut FilePriorityWidget {
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

        StatefulWidget::render(menu_list, area, buf, &mut self.state);
    }
}
