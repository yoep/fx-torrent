use crate::bencode;
use std::io;
use thiserror::Error;

/// The extension specific result type
pub type Result<T> = std::result::Result<T, Error>;

/// The errors which may occur within extensions
#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to parse extension payload, {0}")]
    Parsing(String),
    #[error("failed to execute extension operation, {0}")]
    Operation(String),
    #[error("an io error occurred, {0}")]
    Io(io::Error),
    #[error("the payload or operation is not supported")]
    Unsupported,
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Parsing(_), Self::Parsing(_)) => true,
            (Self::Operation(_), Self::Operation(_)) => true,
            (Self::Io(a), Self::Io(b)) => a.kind() == b.kind(),
            (Self::Unsupported, Self::Unsupported) => true,
            _ => false,
        }
    }
}

impl From<bencode::Error> for Error {
    fn from(error: bencode::Error) -> Self {
        Self::Parsing(error.to_string())
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}
