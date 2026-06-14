pub use disk::*;
pub use memory::*;
pub use metrics::*;
pub use types::*;

mod disk;
mod memory;
mod metrics;
mod parts_file;
mod types;

/// The result type of storage operations.
pub type Result<T> = std::result::Result<T, std::io::Error>;

/// Returns an unavailable error.
#[inline]
fn unavailable() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::NotFound, "unavailable")
}
