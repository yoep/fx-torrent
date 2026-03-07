pub use add_peer::*;
pub use content::*;
pub use file_priority::*;
pub use files::*;
pub use peers::*;

mod add_peer;
mod content;
mod file_priority;
mod file_selection;
mod files;
mod peers;

use fx_torrent::FilePriority;

fn priority_text(priority: &FilePriority) -> &'static str {
    match *priority {
        FilePriority::None => "None",
        FilePriority::Normal => "Normal",
        FilePriority::High => "High",
        FilePriority::Readahead => "Readahead",
        FilePriority::Next => "Next",
        FilePriority::Now => "Now",
    }
}
