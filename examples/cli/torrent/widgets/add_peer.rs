use crate::app::FXKeyEvent;
use crate::torrent::command::TorrentInfoCommand;
use crate::widgets::InputWidget;
use crossterm::event::KeyCode;
use ratatui::layout::Constraint::{Fill, Length};
use ratatui::layout::{Layout, Rect};
use ratatui::prelude::{Color, Style, Text};
use ratatui::widgets::{Block, Borders, Widget};
use ratatui::Frame;
use std::net::SocketAddr;
use std::str::FromStr;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug)]
pub struct AddPeerWidget {
    input: InputWidget,
    error: Option<String>,
    command_sender: UnboundedSender<TorrentInfoCommand>,
}

impl AddPeerWidget {
    pub fn new(command_sender: UnboundedSender<TorrentInfoCommand>) -> Self {
        Self {
            input: InputWidget::new_with_opts("", true),
            error: None,
            command_sender,
        }
    }

    fn try_parse_addr(&mut self) -> Option<SocketAddr> {
        let addr_value = self.input.as_str();

        match SocketAddr::from_str(addr_value) {
            Ok(addr) => Some(addr),
            Err(e) => {
                self.error = Some(format!("Address is invalid, {}", e));
                None
            }
        }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.error = None;
    }

    pub fn on_key_event(&mut self, mut event: FXKeyEvent) {
        match event.key_code() {
            KeyCode::Esc => {
                event.consume();
                self.reset();
                let _ = self.command_sender.send(TorrentInfoCommand::ShowFiles);
            }
            KeyCode::Backspace => {
                event.consume();
                self.input.backspace();
            }
            KeyCode::Enter => {
                event.consume();
                if let Some(addr) = self.try_parse_addr() {
                    self.reset();
                    let _ = self.command_sender.send(TorrentInfoCommand::AddPeer(addr));
                    let _ = self.command_sender.send(TorrentInfoCommand::ShowFiles);
                }
            }
            KeyCode::Char(ch) => {
                event.consume();
                self.input.insert(ch);
            }
            KeyCode::Left => {
                event.consume();
                self.input.cursor_left();
            }
            KeyCode::Right => {
                event.consume();
                self.input.cursor_right();
            }
            _ => {}
        }
    }

    pub fn on_paste_event(&mut self, text: String) {
        self.input.append(text.as_str());
    }

    pub fn render(&self, frame: &mut Frame, area: Rect) {
        let layout = Layout::vertical([Fill(1), Length(1), Length(1)]);
        let [input_area, help_area, invalid_area] = layout.areas(area);

        // render the input widget
        let block = Block::new()
            .title("Enter peer address")
            .borders(Borders::ALL);
        self.input.render(frame, block.inner(input_area));
        frame.render_widget(block, input_area);

        // render the help info
        Text::from("Press Esc to return, Enter to add new peer")
            .style(Style::new().italic())
            .render(help_area, frame.buffer_mut());

        // render the error message
        if let Some(error) = &self.error {
            Text::from(error.as_str())
                .style(Style::new().fg(Color::Red))
                .render(invalid_area, frame.buffer_mut());
        }
    }
}
