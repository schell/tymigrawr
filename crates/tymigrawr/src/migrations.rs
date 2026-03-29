//! Types and operations that perform migrations.
use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
};

use crate::{crud::*, crud_fields::*, error::*};

type FromPrevious =
    Box<dyn Fn(Box<dyn core::any::Any + 'static>) -> Box<dyn core::any::Any + 'static> + 'static>;

type AsCrudFields =
    Box<dyn for<'a> Fn(&'a Box<dyn core::any::Any + 'static>) -> HashMap<&'a str, Value> + 'static>;

type TryFromCrudFields<E> =
    Box<dyn Fn(&HashMap<&str, Value>) -> TymResult<Box<dyn core::any::Any + 'static>, E> + 'static>;

/// Represents a migration of one type through any number of versions.
pub struct Migration<Backend: CrudBackend> {
    pub(crate) table_name: Box<dyn Fn() -> &'static str>,
    pub(crate) crud_fields: Box<dyn Fn() -> Vec<CrudField>>,
    pub(crate) from_prev: FromPrevious,
    pub(crate) as_crud_fields: AsCrudFields,
    pub(crate) try_from_crud_fields: TryFromCrudFields<Backend::Error>,
}

pub(crate) type ReadAllValuesResult<'a, E> = TymResult<HashMap<&'a str, Value>, E>;

/// A trait for backends that support bulk table migration operations.
///
/// This trait extends [`CrudBackend`] with methods needed by the [`Migrations`] system
/// to read all rows from a table, insert rows, and delete entire tables during version migrations.
pub trait MigrateEntireTable: CrudBackend {
    /// Reads all rows from the specified table.
    ///
    /// Returns a vector of results, where each result is either a map of column values
    /// or an error from the backend.
    ///
    /// # Implementation Notes for Backends
    ///
    /// If the table does not exist, implementations **must return an empty vector** instead of
    /// returning an error. This is critical for the migration system to work correctly:
    ///
    /// - Users should be able to run a migration chain (e.g., V1 → V4) against a database
    ///   that only contains data from an intermediate version (e.g., only V2 exists).
    /// - The semantics of "table doesn't exist" and "table is empty" are equivalent in the
    ///   context of migrations: both mean "no rows to migrate from this version".
    /// - Implementations should log at debug level when a table is not found to help users
    ///   understand which versions were processed during migration.
    ///
    /// Errors should only be returned for actual backend failures (I/O errors, corruption, etc.),
    /// not for missing tables.
    fn read_all_values<'a>(
        connection: <Self as CrudBackend>::Connection<'a>,
        table_name: &'a str,
        fields: Vec<CrudField>,
    ) -> TymResult<Vec<ReadAllValuesResult<'a, Self::Error>>, Self::Error>;

    /// Inserts a row (represented as a field map) into the specified table.
    fn insert_fields(
        connection: <Self as CrudBackend>::Connection<'_>,
        table_name: &str,
        fields: &HashMap<&str, Value>,
    ) -> TymResult<(), Self::Error>;

    /// Deletes all rows from the specified table.
    fn delete_all(
        connection: <Self as CrudBackend>::Connection<'_>,
        table_name: &str,
    ) -> TymResult<(), Self::Error>;
}

/// A builder for chaining versioned schema migrations.
///
/// `Migrations` allows you to define a sequence of type versions and automatically migrate data
/// between them. Each call to [`with_version`](Migrations::with_version) adds a new version to the chain.
///
/// # How It Works
///
/// 1. Start with `Migrations::<V1, Backend>::default()` (which initializes with version V1).
/// 2. Chain `.with_version::<V2>()` to add V2 (requires `impl From<V1> for V2`).
/// 3. Optionally chain more versions: `.with_version::<V3>()`, etc.
/// 4. Call `.run(&connection)` or `.run_with(|table| ...)` to execute all migrations.
///
/// During execution:
/// - For each old version table found in the database, all rows are read.
/// - Rows are automatically converted through the `From` implementations to the latest version.
/// - Rows are inserted into the new table.
/// - The old table is deleted.
///
/// # Examples
///
/// ```ignore
/// // Forward migration (V1 → V2 → V3)
/// let migrations = Migrations::<PlayerV1, Sqlite>::default()
///     .with_version::<PlayerV2>()
///     .with_version::<PlayerV3>();
/// migrations.run(&conn).unwrap();
///
/// // Reverse migration (V3 → V2 → V1)
/// let migrations = Migrations::<PlayerV3, Sqlite>::default()
///     .with_version::<PlayerV2>()
///     .with_version::<PlayerV1>();
/// migrations.run(&conn).unwrap();
/// ```
pub struct Migrations<T, Backend: CrudBackend> {
    _current: PhantomData<(T, Backend)>,
    all: VecDeque<Migration<Backend>>,
}

impl<T: Crud<Backend> + HasCrudFields + Clone + Sized + 'static, Backend: MigrateEntireTable>
    Default for Migrations<T, Backend>
{
    /// Creates a new migration chain starting with version `T`.
    fn default() -> Self {
        Self {
            _current: PhantomData,
            all: Default::default(),
        }
        .with_version::<T>()
    }
}

impl<T: Crud<Backend> + HasCrudFields + Clone + Sized + 'static, Backend: MigrateEntireTable>
    Migrations<T, Backend>
{
    /// Adds the next version to the migration chain.
    ///
    /// # Type Parameters
    ///
    /// * `Next` — The next version type. Must implement `From<T>` to enable automatic conversion.
    ///
    /// # Panics
    ///
    /// Panics at runtime if `Next` does not properly implement `From<T>`.
    pub fn with_version<Next>(self) -> Migrations<Next, Backend>
    where
        Next: From<T> + Crud<Backend> + HasCrudFields + Clone + Sized + 'static,
    {
        let Self {
            _current: _,
            mut all,
        } = self;
        all.push_back(<Next as Crud<Backend>>::migration::<T>());
        Migrations {
            _current: PhantomData,
            all,
        }
    }

    /// Executes all queued migrations using a single connection for all tables.
    ///
    /// # Arguments
    ///
    /// * `connection` — The database connection to use for all migration operations.
    ///
    /// # Errors
    ///
    /// Returns an error if any database operation fails (reading, inserting, or deleting rows).
    pub fn run<'a>(
        self,
        connection: <Backend as CrudBackend>::Connection<'a>,
    ) -> TymResult<(), Backend::Error> {
        self.run_with(|_| connection)
    }

    /// Executes all queued migrations using a closure to get connections per table.
    ///
    /// This is useful when different versions are stored in different databases or files.
    ///
    /// # Arguments
    ///
    /// * `mk_connection` — A closure that takes a table name and returns a connection for that table.
    ///
    /// # Errors
    ///
    /// Returns an error if any database operation fails, or if a row cannot be converted between versions.
    pub fn run_with<'a>(
        self,
        mk_connection: impl Fn(&str) -> <Backend as CrudBackend>::Connection<'a>,
    ) -> TymResult<(), Backend::Error> {
        let Self { _current, mut all } = self;
        log::info!(
            "migrating {} versions of {:?}",
            all.len(),
            core::any::type_name::<T>()
        );

        let destination_table_name = T::table_name();
        // Ensure the destination table exists
        {
            T::create(mk_connection(destination_table_name))?;
        }
        while let Some(migration) = all.pop_front() {
            if all.is_empty() {
                log::debug!("  ...done");
                break;
            }
            let prev_table_name = (migration.table_name)();
            log::debug!("  checking {prev_table_name}");
            let fields = (migration.crud_fields)();
            // Get a cursor of each value in the prev table
            let cursor = Backend::read_all_values(
                (mk_connection)(prev_table_name),
                prev_table_name,
                fields,
            )?;
            let mut current_table_name = prev_table_name;
            let mut entries = 0;
            for res_prev in cursor {
                entries += 1;
                log::trace!("  entry {entries}");
                let values = res_prev?;
                // Serialize to the prev type
                let mut prev = (migration.try_from_crud_fields)(&values)?;
                let mut last_migration = &migration;
                // Move the type forward with From, from the prev to the most
                // current
                let mut conversions = 0;
                for target in all.iter() {
                    conversions += 1;
                    prev = (target.from_prev)(prev);
                    last_migration = target;
                }
                log::trace!("    converted entry {entries} {conversions} times from {current_table_name} to {}", (last_migration.table_name)());
                // Now prev is the most current type.
                let current = prev;
                current_table_name = (last_migration.table_name)();
                // Save it in the most current table, if need be.
                if current_table_name != prev_table_name {
                    log::trace!("    inserting into {current_table_name}");
                    let fields = (last_migration.as_crud_fields)(&current);
                    Backend::insert_fields(
                        (mk_connection)(current_table_name),
                        current_table_name,
                        &fields,
                    )?;
                }
            }
            log::debug!(
                "    migrated {entries} entries from {prev_table_name} to {destination_table_name}",
            );
            // Remove the old entries if need be
            if current_table_name != prev_table_name {
                log::debug!("    clearing out previous table {prev_table_name}");
                let conn = (mk_connection)(prev_table_name);
                Backend::delete_all(conn, prev_table_name)?;
            }
        }
        Ok(())
    }
}
