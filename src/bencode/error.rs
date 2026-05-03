use serde::de::{Expected, Unexpected};
use serde::{de, ser};
use std::fmt::Display;
use std::{io, result};
use thiserror::Error;

/// The error alias for bencode operations.
pub type Result<T> = result::Result<T, Error>;

/// The errors that might occur within bencode operations.
#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    InvalidType(String),
    #[error("{0}")]
    InvalidValue(String),
    #[error("{0}")]
    InvalidLength(String),
    #[error("unknown variant: {0}, (expected one of: {1}")]
    UnknownVariant(String, String),
    #[error("unknown field: {0}, (expected one of: {1})")]
    UnknownField(String, String),
    #[error("missing field: {0}")]
    MissingField(String),
    #[error("duplicate field: {0}")]
    DuplicateField(String),
    #[error("an io error occurred, {0}")]
    Io(io::Error),
    #[error("{0}")]
    Custom(String),
}

impl Error {}

impl ser::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Self::Custom(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Self::Custom(msg.to_string())
    }

    fn invalid_type(unexp: Unexpected, exp: &dyn Expected) -> Self {
        Self::InvalidType(format!("invalid type: {unexp} (expected: `{exp}`)"))
    }

    fn invalid_value(unexp: Unexpected, exp: &dyn Expected) -> Self {
        Self::InvalidValue(format!("invalid value: {unexp} (expected: `{exp}`)"))
    }

    fn invalid_length(len: usize, exp: &dyn Expected) -> Self {
        Self::InvalidLength(format!("invalid length: {len} (expected: `{exp}`)"))
    }

    fn unknown_variant(variant: &str, expected: &'static [&'static str]) -> Self {
        Self::UnknownVariant(variant.to_string(), expected.join(", "))
    }

    fn unknown_field(field: &str, expected: &'static [&'static str]) -> Self {
        Self::UnknownField(field.to_string(), expected.join(", "))
    }

    fn missing_field(field: &'static str) -> Self {
        Self::MissingField(field.to_string())
    }

    fn duplicate_field(field: &'static str) -> Self {
        Self::DuplicateField(field.to_string())
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}
