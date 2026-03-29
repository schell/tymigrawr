//! Error types for tymigrawr.

use crate::HasCrudFieldsError;
use snafu::prelude::*;

/// A wrapper for domain specific errors.
#[derive(Debug)]
pub struct DomainError<E: core::fmt::Display> {
    pub inner: E,
}

impl<E: core::fmt::Display> core::fmt::Display for DomainError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl<E: core::fmt::Display> From<E> for DomainError<E> {
    fn from(inner: E) -> Self {
        DomainError { inner }
    }
}

impl<E: core::fmt::Display + core::fmt::Debug + 'static> std::error::Error for DomainError<E> {}

/// A generic error type parameterized over a domain-specific error.
///
/// `Error<E>` wraps domain-specific errors (like `sqlite::Error` or `toml::de::Error`)
/// while providing library-level error variants for common failure cases.
///
/// # Examples
///
/// ```ignore
/// // Database operation error
/// let result: Result<(), Error<sqlite::Error>> = table.create(&connection);
///
/// // Data parsing error
/// let result: Result<MyType, Error<Infallible>> =
///     MyType::try_from_crud_fields(&fields);
/// ```
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error<E: core::fmt::Display + core::fmt::Debug + 'static = std::convert::Infallible> {
    #[snafu(display("{source}"))]
    Backend { source: DomainError<E> },

    #[snafu(display("Could not create type from crud fields: {source}"))]
    Crud { source: HasCrudFieldsError },

    #[snafu(display("Parse error: {message}"))]
    /// Failed to parse data (missing required field, type mismatch, etc).
    ParseError { message: String },

    #[snafu(display("Invalid value for '{field}': {reason}"))]
    /// A value is invalid for the given field.
    InvalidValue { field: String, reason: String },

    #[snafu(display("Missing field '{field}'"))]
    /// A required field was missing.
    MissingField { field: String },

    #[snafu(display("Operation '{operation}' failed: {reason}"))]
    /// Generic operation failed.
    OperationFailed { operation: String, reason: String },

    #[snafu(display("Serde error: {source}"))]
    /// Serde/JSON error.
    Serde { source: serde_json::Error },

    #[snafu(display("IO error: {source}"))]
    /// I/O error.
    Io { source: std::io::Error },

    #[snafu(display("Json error: {source}"))]
    Json { source: serde_json::Error },
}

impl<E: core::fmt::Display + core::fmt::Debug + 'static> From<HasCrudFieldsError> for Error<E> {
    fn from(source: HasCrudFieldsError) -> Self {
        Error::Crud { source }
    }
}

/// Conversion impls for error types that can be wrapped in Error variants.
impl<E: core::fmt::Display + core::fmt::Debug + 'static> From<serde_json::Error> for Error<E> {
    fn from(source: serde_json::Error) -> Self {
        Error::Serde { source }
    }
}

impl<E: core::fmt::Display + core::fmt::Debug + 'static> From<std::io::Error> for Error<E> {
    fn from(source: std::io::Error) -> Self {
        Error::Io { source }
    }
}

impl<E: core::fmt::Display + core::fmt::Debug + 'static> From<DomainError<E>> for Error<E> {
    fn from(source: DomainError<E>) -> Self {
        Error::Backend { source }
    }
}

pub type TymResult<T, E> = core::result::Result<T, Error<E>>;
