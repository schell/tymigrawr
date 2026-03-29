//! Create, read, update and delete operations on types.

use crate::{crud_fields::*, error::*, Migration};

/// A trait defining the interface for database backends.
///
/// Each backend (SQLite, TOML, PostgreSQL, etc.) implements this trait to provide
/// a connection type and an error type. This abstraction allows the [`Crud`] trait
/// to be backend-agnostic.
pub trait CrudBackend {
    /// The connection type for this backend.
    ///
    /// Must be `Copy` so it can be passed around easily. Implementations typically
    /// use a reference type or a copy-on-write handle.
    type Connection<'a>: Copy;

    /// The error type for this backend.
    ///
    /// Must implement `Display` and `Debug` so it can be wrapped in the library's [`Error`] type.
    type Error: core::fmt::Display + core::fmt::Debug + 'static;
}

type ReadAllResult<'a, T, Backend> = Result<
    Box<dyn Iterator<Item = Result<T, Error<<Backend as CrudBackend>::Error>>> + 'a>,
    Error<<Backend as CrudBackend>::Error>,
>;

type ReadWhereResult<'a, T, Backend> = Result<
    Box<dyn Iterator<Item = Result<T, Error<<Backend as CrudBackend>::Error>>> + 'a>,
    Error<<Backend as CrudBackend>::Error>,
>;

type ReadResult<'a, T, Backend> = Result<
    Box<dyn Iterator<Item = Result<T, Error<<Backend as CrudBackend>::Error>>> + 'a>,
    Error<<Backend as CrudBackend>::Error>,
>;

/// Provides CRUD (Create, Read, Update, Delete) operations for a type.
///
/// This trait is automatically implemented for any type that implements [`HasCrudFields`] and is `Clone`.
/// It provides database operations against any backend that implements [`CrudBackend`].
///
/// The trait is generic over the backend, allowing the same type to be used with different storage systems
/// (e.g., SQLite, TOML files).
///
/// # Examples
///
/// ```rust
/// use tymigrawr::{Crud, CrudBackend, HasCrudFields, PrimaryKey, Sqlite};
/// /// Define a business type that can be persisted.
/// #[derive(Debug, Clone, HasCrudFields)]
/// struct User {
///     id: PrimaryKey<i64>,
///     name: String,
/// }
/// /// For the most part, business logic involving persistance can be generic over the backend.
/// fn run<'a, Backend: CrudBackend>(
///     conn: Backend::Connection<'a>,
/// ) -> Result<(), tymigrawr::Error<Backend::Error>>
/// where
///     User: Crud<Backend>,
/// {
///     // Create table
///     User::create(conn)?;
///     // Insert
///     let user = User {
///         id: PrimaryKey::new(1),
///         name: "Alice".to_string(),
///     };
///     user.insert(conn)?;
///     // Read
///     let users = User::read_all(conn)?;
///     for result in users {
///         let user = result?;
///         println!("{}", user.name);
///     }
///     Ok(())
/// }
/// // Then specialize on the backend at the edges of your application
/// let conn = sqlite::open(":memory:").unwrap();
/// run::<Sqlite>(&conn).unwrap();
/// ```
pub trait Crud<Backend>
where
    Self: HasCrudFields + Clone + Sized + 'static,
    Backend: CrudBackend,
{
    /// Creates a table for this type in the database.
    ///
    /// This method generates and executes the appropriate DDL (Data Definition Language) statement
    /// to create a table with columns and constraints matching the schema defined by [`HasCrudFields`].
    fn create(connection: Backend::Connection<'_>) -> TymResult<(), Backend::Error>;

    /// Inserts a single row into the table.
    ///
    /// Fails if a row with the same primary key already exists.
    fn insert(
        &self,
        connection: Backend::Connection<'_>,
    ) -> std::result::Result<(), Error<Backend::Error>>;

    /// Inserts a row, or updates all non-primary-key columns if a row with the same primary key exists.
    ///
    /// Returns `true` if the row was inserted or updated, `false` if no change was needed.
    fn upsert(
        &self,
        connection: Backend::Connection<'_>,
    ) -> std::result::Result<bool, Error<Backend::Error>>;

    /// Reads all rows from the table.
    ///
    /// Returns an iterator over the rows. Each item is a `Result`, allowing for per-row error handling.
    fn read_all<'a>(connection: Backend::Connection<'a>) -> ReadAllResult<'a, Self, Backend>;

    /// Reads rows matching a condition.
    ///
    /// # Arguments
    ///
    /// * `connection` — The database connection.
    /// * `key_name` — The column name to filter on.
    /// * `comparison` — A SQL comparison operator (e.g., `"="`, `">"`, `"<"`, `"LIKE"`).
    /// * `key_value` — The value to compare against.
    ///
    /// Returns an iterator over matching rows.
    fn read_where<'a>(
        connection: Backend::Connection<'a>,
        key_name: &'a str,
        comparison: &'a str,
        key_value: impl IsCrudField,
    ) -> ReadWhereResult<'a, Self, Backend>;

    /// Reads a single row by its primary key.
    ///
    /// # Arguments
    ///
    /// * `connection` — The database connection.
    /// * `key` — The primary key value to look up.
    ///
    /// Returns an iterator that contains at most one row (the database guarantees unique primary keys).
    fn read<'a, Key: IsCrudField>(
        connection: Backend::Connection<'a>,
        key: Key,
    ) -> ReadResult<'a, Self, Backend>;

    /// Updates an existing row by its primary key.
    ///
    /// All columns except the primary key are updated to match the current values in this instance.
    /// Fails if no row with this primary key exists.
    fn update(&self, connection: Backend::Connection<'_>) -> TymResult<(), Backend::Error>;

    /// Deletes a row by its primary key.
    ///
    /// Consumes `self` (moves ownership) to prevent accidental use of the deleted row afterward.
    fn delete(self, connection: Backend::Connection<'_>) -> TymResult<(), Backend::Error>;

    /// Creates a migration step from type `T` to `Self`.
    ///
    /// This is used internally by the [`Migrations`] builder to chain version upgrades.
    /// Requires that `Self` implements `From<T>`.
    fn migration<T: 'static>() -> Migration<Backend::Error>
    where
        Self: From<T>,
    {
        Migration {
            table_name: Box::new(Self::table_name),
            crud_fields: Box::new(Self::crud_fields),
            from_prev: Box::new(|any: Box<dyn core::any::Any>| {
                // SAFETY: we know we can downcast because of the Self: From<T> constraint
                let t: Box<T> = any.downcast().unwrap();
                let s = Self::from(*t);
                Box::new(s)
            }),
            as_crud_fields: Box::new(|any: &Box<dyn core::any::Any>| {
                if let Some(t) = any.downcast_ref::<Self>() {
                    t.as_crud_fields()
                } else {
                    Default::default()
                }
            }),
            try_from_crud_fields: Box::new(|fields| {
                let t = Self::try_from_crud_fields(fields)?;
                Ok(Box::new(t))
            }),
        }
    }
}
