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

    // /// Entity not found.
    // NotFound {
    //     entity: String,
    // },

    // /// Constraint violation (e.g., duplicate key, foreign key).
    // ConstraintViolation {
    //     reason: String,
    // },
    #[snafu(display("Operation '{operation}' failed: {reason}"))]
    /// Generic operation failed.
    OperationFailed { operation: String, reason: String },

    #[snafu(display("Serde error: {source}"))]
    /// Serde/JSON error.
    Serde { source: serde_json::Error },

    #[snafu(display("IO error: {source}"))]
    /// I/O error.
    Io { source: std::io::Error },

    #[cfg(feature = "backend_sqlite")]
    /// SQLite error.
    #[snafu(display("Sqlite error: {source}"))]
    Sqlite { source: sqlite::Error },

    #[cfg(feature = "backend_toml")]
    /// TOML error.
    #[snafu(display("Toml error: {source}"))]
    Toml { source: toml::de::Error },

    /// JSON error.
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

#[cfg(feature = "backend_sqlite")]
impl<E: core::fmt::Display + core::fmt::Debug + 'static> From<sqlite::Error> for Error<E> {
    fn from(source: sqlite::Error) -> Self {
        Error::Sqlite { source }
    }
}

#[cfg(feature = "backend_toml")]
impl<E: core::fmt::Display + core::fmt::Debug + 'static> From<toml::de::Error> for Error<E> {
    fn from(source: toml::de::Error) -> Self {
        Error::Toml { source }
    }
}

// impl<E> Error<E> {
//     /// Create a parse error.
//     pub fn parse_error(message: impl Into<String>) -> Self {
//         Error::ParseError {
//             message: message.into(),
//         }
//     }

//     /// Create a missing field error.
//     pub fn missing_field(field: impl Into<String>) -> Self {
//         Error::MissingField {
//             field: field.into(),
//         }
//     }

//     /// Create an invalid value error.
//     pub fn invalid_value(field: impl Into<String>, reason: impl Into<String>) -> Self {
//         Error::InvalidValue {
//             field: field.into(),
//             reason: reason.into(),
//         }
//     }

//     /// Create a not found error.
//     pub fn not_found(entity: impl Into<String>) -> Self {
//         Error::NotFound {
//             entity: entity.into(),
//         }
//     }

//     /// Create a constraint violation error.
//     pub fn constraint_violation(reason: impl Into<String>) -> Self {
//         Error::ConstraintViolation {
//             reason: reason.into(),
//         }
//     }

//     /// Create an operation failed error.
//     pub fn operation_failed(operation: impl Into<String>, reason: impl Into<String>) -> Self {
//         Error::OperationFailed {
//             operation: operation.into(),
//             reason: reason.into(),
//         }
//     }

//     /// Create a domain error.
//     pub fn domain(err: E) -> Self {
//         Error::Domain(err)
//     }

//     /// Maps the domain error type using the provided function.
//     pub fn map_domain<F, E2>(self, f: F) -> Error<E2>
//     where
//         F: FnOnce(E) -> E2,
//     {
//         match self {
//             Error::ParseError { message } => Error::ParseError { message },
//             Error::InvalidValue { field, reason } => Error::InvalidValue { field, reason },
//             Error::MissingField { field } => Error::MissingField { field },
//             Error::NotFound { entity } => Error::NotFound { entity },
//             Error::ConstraintViolation { reason } => Error::ConstraintViolation { reason },
//             Error::OperationFailed { operation, reason } => {
//                 Error::OperationFailed { operation, reason }
//             }
//             Error::Domain(err) => Error::Domain(f(err)),
//         }
//     }
// }

/// A library error with no domain-specific error (using `Infallible`).
///
/// Used for errors that originate purely from the library layer, not from
/// backend-specific operations (e.g., data validation, parsing).
pub type LibraryError = Error<std::convert::Infallible>;

pub type TymResult<T, E> = core::result::Result<T, Error<E>>;

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_error_display() {
//         let err: LibraryError = Error::parse_error("expected integer");
//         assert_eq!(err.to_string(), "parse error: expected integer");

//         let err: LibraryError = Error::missing_field("id");
//         assert_eq!(err.to_string(), "missing required field 'id'");

//         let err: LibraryError = Error::invalid_value("age", "must be positive");
//         assert_eq!(
//             err.to_string(),
//             "invalid value for field 'age': must be positive"
//         );
//     }

//     #[test]
//     fn test_error_source_none_for_library_errors() {
//         let err: LibraryError = Error::parse_error("test");
//         assert!(err.source().is_none());
//     }

//     #[test]
//     fn test_map_domain() {
//         let err: Error<i32> = Error::Domain(42);
//         let mapped: Error<String> = err.map_domain(|e| format!("error code: {}", e));
//         assert!(matches!(mapped, Error::Domain(_)));
//     }
// }
