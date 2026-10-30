use fx_callback::{Callback, Subscription};
use fx_torrent::{BitVec, PieceIndex, Torrent, TorrentEvent};
use ratatui::buffer::Buffer;
use ratatui::layout::Constraint::Length;
use ratatui::layout::{Alignment, Layout, Rect};
use ratatui::prelude::{Color, Style};
use ratatui::widgets::{Block, Borders, Widget};
use ratatui::{symbols, Frame};
use std::cmp::min;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

const AVAILABILITY_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub struct ProgressWidget {
    progress: Progress,
    torrent: Torrent,
    event_receiver: Subscription<TorrentEvent>,
    last_availability_update: Instant,
}

impl ProgressWidget {
    /// Create a new progress widget.
    pub fn new(torrent: Torrent) -> Self {
        let event_receiver = torrent.subscribe();

        Self {
            progress: Default::default(),
            torrent,
            event_receiver,
            last_availability_update: Instant::now(),
        }
    }

    /// Execute a single tick of the progress widget.
    pub async fn tick(&mut self) {
        self.init().await;
        self.process_torrent_events().await;
        self.update_availability().await;
    }

    /// Render this widget for the given frame and area.
    pub fn render(&mut self, frame: &mut Frame, area: Rect) {
        let availability_percent = self
            .progress
            .availability
            .iter()
            .filter(|(_, available)| **available > 0)
            .count() as f32
            / self.progress.availability.len().max(1) as f32;
        let downloaded: Vec<f32> = self
            .progress
            .downloaded
            .iter()
            .map(|b| if *b { 1.0 } else { 0.0 })
            .collect();
        let availability: Vec<f32> = self
            .progress
            .availability
            .values()
            .map(|c| (*c).min(1) as f32)
            .collect();
        let block = Block::default()
            .borders(Borders::all())
            .title_top(format!(" Progress ({:.1}%) ", self.progress.value * 100f32))
            .title_bottom(format!(
                " Availability ({:.1}%) ",
                availability_percent * 100f32
            ))
            .title_alignment(Alignment::Center);
        let layout = Layout::vertical([Length(1), Length(1)]);
        let [downloaded_area, availability_area] = layout.areas(block.inner(area));

        BlockProgressWidget::new(&downloaded)
            .style(Style::default().fg(Color::Yellow))
            .render(downloaded_area, frame.buffer_mut());
        BlockProgressWidget::new(&availability)
            .style(Style::default().fg(Color::Yellow))
            .render(availability_area, frame.buffer_mut());

        block.render(area, frame.buffer_mut());
    }

    async fn init(&mut self) {
        if self.progress.downloaded.len() > 0 {
            return;
        }

        if let Some(metadata) = self.torrent.metadata().await.ok().and_then(|e| e.info) {
            let num_of_pieces = metadata.pieces.len();
            self.progress.downloaded = BitVec::repeat(false, num_of_pieces);
            self.progress.availability = (0..num_of_pieces)
                .into_iter()
                .map(|index| (index, 0))
                .collect();
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
                    self.progress.availability =
                        (0..*pieces).into_iter().map(|index| (index, 0)).collect();
                }
                _ => {}
            }
        }
    }

    async fn update_availability(&mut self) {
        if self.last_availability_update.elapsed() < AVAILABILITY_INTERVAL {
            return;
        }

        self.progress.availability = self.torrent.availabilities().await;
        self.last_availability_update = Instant::now();
    }
}

#[derive(Debug, Default)]
struct Progress {
    value: f32,
    downloaded: BitVec,
    availability: BTreeMap<PieceIndex, usize>,
}

#[derive(Debug)]
struct BlockProgressWidget<'a> {
    progress: &'a [f32],
    style: Style,
}

impl<'a> BlockProgressWidget<'a> {
    fn new(progress: &'a [f32]) -> Self {
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
        let len = self.progress.len();

        for y in area.top()..area.bottom() {
            for x in area.left()..area.right() {
                // average the progress of the pieces in the block
                let col = (x - area.left()) as usize;
                let block_start = len * col / width;
                let block_end = min(len * (col + 1) / width, len);
                let block = &self.progress[block_start..block_end];
                let fraction = if block.is_empty() {
                    0.0
                } else {
                    (block.iter().sum::<f32>() / block.len() as f32).clamp(0.0, 1.0)
                };
                let symbol = get_unicode_block(fraction);

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
