use fx_callback::{Callback, Subscription};
use fx_torrent::{BitVec, Torrent, TorrentEvent};
use ratatui::buffer::Buffer;
use ratatui::layout::Constraint::Length;
use ratatui::layout::{Alignment, Layout, Rect};
use ratatui::prelude::{Color, Style};
use ratatui::widgets::{Block, Borders, Widget};
use ratatui::{symbols, Frame};
use std::cmp::min;

#[derive(Debug)]
pub struct ProgressWidget {
    progress: Progress,
    torrent: Torrent,
    event_receiver: Subscription<TorrentEvent>,
}

impl ProgressWidget {
    /// Create a new progress widget.
    pub fn new(torrent: Torrent) -> Self {
        let event_receiver = torrent.subscribe();

        Self {
            progress: Default::default(),
            torrent,
            event_receiver,
        }
    }

    /// Execute a single tick of the progress widget.
    pub async fn tick(&mut self) {
        self.init().await;
        self.process_torrent_events().await;
    }

    /// Render this widget for the given frame and area.
    pub fn render(&mut self, frame: &mut Frame, area: Rect) {
        let block = Block::default()
            .borders(Borders::all())
            .title_top(format!(" Progress ({:.1}%) ", self.progress.value * 100f32))
            .title_bottom(format!(
                " Availability ({:.1}%) ",
                self.progress.value * 100f32
            ))
            .title_alignment(Alignment::Center);
        let layout = Layout::vertical([Length(1), Length(1)]);
        let [downloaded_area, availability_area] = layout.areas(block.inner(area));

        BlockProgressWidget::new(&self.progress.downloaded)
            .style(Style::default().fg(Color::Yellow))
            .render(downloaded_area, frame.buffer_mut());
        BlockProgressWidget::new(&self.progress.availability)
            .style(Style::default().fg(Color::Yellow))
            .render(availability_area, frame.buffer_mut());

        block.render(area, frame.buffer_mut());
    }

    async fn init(&mut self) {
        if self.progress.downloaded.len() > 0 {
            return;
        }

        if let Some(metadata) = self.torrent.metadata().await.ok().and_then(|e| e.info) {
            self.progress.downloaded = BitVec::repeat(false, metadata.pieces.len());
            self.progress.availability = BitVec::repeat(false, metadata.pieces.len());
        }
    }

    async fn process_torrent_events(&mut self) {
        while let Ok(event) = self.event_receiver.try_recv() {
            match &*event {
                TorrentEvent::Stats(stats) => {
                    self.progress.value = stats.progress();
                }
                TorrentEvent::PieceCompleted(piece) => {
                    if self.progress.downloaded.len() < *piece {
                        self.progress.downloaded.resize(*piece, false);
                    }

                    self.progress.downloaded.set(*piece, true);
                }
                TorrentEvent::PiecesChanged(pieces) => {
                    self.progress.downloaded.resize(*pieces, false);
                    self.progress.availability.resize(*pieces, false);
                }
                _ => {}
            }
        }
    }
}

#[derive(Debug, Default)]
struct Progress {
    value: f32,
    downloaded: BitVec,
    availability: BitVec,
}

#[derive(Debug)]
struct BlockProgressWidget<'a> {
    progress: &'a BitVec,
    style: Style,
}

impl<'a> BlockProgressWidget<'a> {
    fn new(progress: &'a BitVec) -> Self {
        Self {
            progress,
            style: Default::default(),
        }
    }

    fn style<S: Into<Style>>(&mut self, style: S) -> &mut Self {
        self.style = style.into();
        self
    }
}

impl<'a> Widget for &BlockProgressWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer)
    where
        Self: Sized,
    {
        if area.is_empty() {
            return;
        }

        // set the area style
        buf.set_style(area, self.style);

        let width = area.width as usize;
        let pieces_per_block = (self.progress.len() + 1) / width;

        for y in area.top()..area.bottom() {
            for x in area.left()..area.right() {
                // count the completed pieces in the block
                let block_start = x as usize * pieces_per_block;
                let block_end = min((x as usize + 1) * pieces_per_block, self.progress.len());
                let completed = self.progress[block_start..block_end].count_ones();
                let fraction_completed = completed as f32 / pieces_per_block as f32;
                let symbol = get_unicode_block(fraction_completed);

                buf[(x, y)]
                    .set_symbol(symbol)
                    .set_fg(self.style.fg.unwrap_or(Color::Reset))
                    .set_bg(self.style.bg.unwrap_or(Color::Reset));
            }
        }
    }
}

fn get_unicode_block<'a>(frac: f32) -> &'a str {
    match (frac * 8.0).round() as u16 {
        1 => symbols::block::ONE_EIGHTH,
        2 => symbols::block::ONE_QUARTER,
        3 => symbols::block::THREE_EIGHTHS,
        4 => symbols::block::HALF,
        5 => symbols::block::FIVE_EIGHTHS,
        6 => symbols::block::THREE_QUARTERS,
        7 => symbols::block::SEVEN_EIGHTHS,
        8 => symbols::block::FULL,
        _ => " ",
    }
}
