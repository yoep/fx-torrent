pub use disk::*;
pub use memory::*;
pub use metrics::*;
pub use mmap::*;
pub use part_file::*;
pub use types::*;

mod disk;
mod memory;
mod metrics;
mod mmap;
mod part_file;
mod types;

/// The error type of the storage operations.
pub type Error = std::io::Error;

/// The result type of storage operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Returns an unavailable error.
#[inline]
fn unavailable() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::NotFound, "unavailable")
}
