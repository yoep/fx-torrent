use crate::app::FXKeyEvent;
use crate::widgets::CheckboxWidget;
use crossterm::event::KeyCode;
use fx_torrent::File;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::prelude::{Color, Style};
use ratatui::text::Text;
use ratatui::widgets::{Block, List, ListState, StatefulWidget, Widget};

const FILE_ICON: &str = "\u{1F4C4}";
const DIR_ICON: &str = "\u{1F4C1}";

#[derive(Debug)]
pub struct FileSelectionWidget {
    files: Vec<InnerFile>,
    state: ListState,
}

impl FileSelectionWidget {
    /// Create a new file selection widget.
    pub fn new() -> Self {
        Self {
            files: vec![],
            state: ListState::default().with_selected(Some(0)),
        }
    }

    /// Set the files for the selection widget.
    pub fn set_files(&mut self, files: Vec<File>) {
        self.files.clear();
        for file in files {
            let components = file
                .torrent_path
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>();

            if components.is_empty() {
                continue;
            }

            let components_len = components.len();
            let mut files = &mut self.files;
            for (i, component) in components.into_iter().enumerate() {
                if i == components_len - 1 {
                    let filename = format!("{} {}", FILE_ICON, file.filename());
                    files.push(InnerFile::File {
                        file,
                        widget: CheckboxWidget::new(filename, false),
                    });
                    break;
                }

                match files.iter().position(|e| e.name() == component.as_str()) {
                    None => {
                        // insert a new directory in the files
                        let cursor = files.len();
                        files.push(InnerFile::Directory {
                            name: component,
                            files: vec![],
                        });
                        files = match &mut files[cursor] {
                            InnerFile::Directory { files, .. } => files,
                            _ => unreachable!(),
                        };
                    }
                    Some(index) => {
                        // retrieve the existing directory from the files
                        files = match &mut files[index] {
                            InnerFile::Directory { files, .. } => files,
                            _ => unreachable!(),
                        }
                    }
                }
            }
        }
    }

    /// Handle the given key event.
    pub fn on_key_event(&mut self, mut event: FXKeyEvent) {
        match event.key_code() {
            KeyCode::Up => {
                event.consume();
                self.state.select_previous();
            }
            KeyCode::Down => {
                event.consume();
                self.state.select_next();
            }
            KeyCode::Char(' ') => {
                event.consume();
                let selected = self.state.selected().unwrap_or_default();
                match self.files.get_mut(selected) {
                    Some(InnerFile::File { widget, .. }) => {
                        widget.toggle();
                    }
                    _ => {}
                }
            }
            KeyCode::Enter => {
                event.consume();
            }
            _ => {}
        }
    }
}

impl Widget for &mut FileSelectionWidget {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let items = self
            .files
            .iter()
            .map(|file| file.items())
            .flatten()
            .collect::<Vec<_>>();
        let list =
            List::new(items)
                .block(Block::bordered().title(" Files ").title_bottom(
                    " Press SPACE to toggle file selection, ENTER to start downloading",
                ))
                .highlight_style(Style::new().bg(Color::DarkGray));

        StatefulWidget::render(list, area, buf, &mut self.state);
    }
}

#[derive(Debug)]
enum InnerFile {
    Directory { name: String, files: Vec<InnerFile> },
    File { file: File, widget: CheckboxWidget },
}

impl InnerFile {
    fn name(&self) -> &str {
        match self {
            InnerFile::Directory { name, .. } => name,
            InnerFile::File { file, .. } => file.filename(),
        }
    }

    fn items(&'_ self) -> Vec<Text<'_>> {
        match self {
            InnerFile::Directory { name, files } => {
                let mut items = vec![Text::from(format!("{} {}", DIR_ICON, name))];
                files
                    .iter()
                    .map(|file| file.items())
                    .flatten()
                    .for_each(|item| items.push(item));
                items
            }
            InnerFile::File { widget, .. } => vec![Text::from(widget)],
        }
    }
}
