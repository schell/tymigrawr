//! Sqlite impl backed by `sqlx`.
use std::collections::HashMap;
use std::pin::Pin;

use async_stream::try_stream;
use futures::{Stream, TryStreamExt};
use sqlx::{Row as _, SqlitePool};

use crate::{
    error::{DomainError, Error, TymResult},
    Crud, CrudBackend, CrudField, HasCrudFields, IsCrudField, MigrateEntireTable,
    ReadAllValuesResult, Value, ValueType,
};

pub struct Sqlite;

impl CrudBackend for Sqlite {
    type Connection<'a> = &'a SqlitePool;
    type Error = sqlx::Error;
}

/// Helper: bind a [`Value`] into a `sqlx::Query`/`QueryBuilder` by dispatching on its variant
/// to the matching `Option<T>` Encode impl already provided by sqlx.
enum Bound {
    Int(Option<i64>),
    Float(Option<f64>),
    Text(Option<String>),
    Blob(Option<Vec<u8>>),
}

impl Bound {
    fn from(v: Value) -> Self {
        match v {
            Value::Integer(i) => Bound::Int(Some(i)),
            Value::Float(f) => Bound::Float(Some(f)),
            Value::String(s) => Bound::Text(Some(s)),
            Value::Bytes(b) => Bound::Blob(Some(b)),
            Value::None => Bound::Int(None),
        }
    }
}

impl sqlx::Type<sqlx::Sqlite> for Bound {
    fn type_info() -> sqlx::sqlite::SqliteTypeInfo {
        <i64 as sqlx::Type<sqlx::Sqlite>>::type_info()
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Sqlite> for Bound {
    fn encode(
        self,
        buf: &mut <sqlx::Sqlite as sqlx::Database>::ArgumentBuffer<'q>,
    ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Bound::Int(v) => v.encode(buf),
            Bound::Float(v) => v.encode(buf),
            Bound::Text(v) => v.encode(buf),
            Bound::Blob(v) => v.encode(buf),
        }
    }

    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Sqlite as sqlx::Database>::ArgumentBuffer<'q>,
    ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Bound::Int(v) => v.encode(buf),
            Bound::Float(v) => v.encode(buf),
            Bound::Text(v) => v.encode(buf),
            Bound::Blob(v) => v.encode(buf),
        }
    }
}

fn bind_value<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    v: Value,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    match Bound::from(v) {
        Bound::Int(v) => query.bind(v),
        Bound::Float(v) => query.bind(v),
        Bound::Text(v) => query.bind(v),
        Bound::Blob(v) => query.bind(v),
    }
}

impl<T: HasCrudFields + Clone + Sized + 'static> Crud<Sqlite> for T {
    /// Create a table for `Self`, if it doesn't already exist.
    ///
    /// Implementations *must not* truncate the table if it already exists.
    async fn create<'a>(
        connection: &SqlitePool,
    ) -> Result<(), Error<<Sqlite as CrudBackend>::Error>> {
        let table_name = Self::table_name();
        let fields: String = Self::crud_fields()
            .iter()
            .map(CrudField::sqlite_create_field)
            .collect::<Vec<_>>()
            .join(", ");
        let statement = format!("CREATE TABLE IF NOT EXISTS {table_name} ({fields});");
        sqlx::query(&statement)
            .execute(connection)
            .await
            .map_err(DomainError::from)?;
        Ok(())
    }

    async fn insert<'a>(
        &mut self,
        connection: &SqlitePool,
    ) -> Result<(), Error<<Sqlite as CrudBackend>::Error>> {
        let table_name = Self::table_name();
        let crud_fields = Self::crud_fields();
        let mut fields = self.as_crud_fields();

        // Check if there's an auto_increment field and omit it if the value is None
        let has_auto_increment = crud_fields.iter().find_map(|field| {
            if field.auto_increment && matches!(fields.get(field.name), Some(Value::None)) {
                fields.remove(field.name);
                Some(field.name)
            } else {
                None
            }
        });

        Sqlite::insert_fields(connection, table_name, &fields).await?;

        // If there was an auto_increment field with None value, update it with the generated ID
        if has_auto_increment.is_some() {
            let rowid: i64 = sqlx::query_scalar("SELECT last_insert_rowid()")
                .fetch_one(connection)
                .await
                .map_err(DomainError::from)?;
            self.set_primary_key(Value::Integer(rowid));
        }

        Ok(())
    }

    async fn upsert<'a>(
        &mut self,
        connection: &SqlitePool,
    ) -> Result<bool, Error<<Sqlite as CrudBackend>::Error>> {
        let table_name = Self::table_name();
        let crud_fields = Self::crud_fields();
        let mut field_values = self.as_crud_fields();

        // Check if primary key is auto_increment and has value None
        let auto_increment_field = crud_fields.iter().find(|f| f.auto_increment);
        let is_new_auto_increment = if let Some(field) = auto_increment_field {
            matches!(field_values.get(field.name), Some(Value::None))
        } else {
            false
        };

        // For new auto_increment records, omit the key and treat as insert
        if is_new_auto_increment {
            if let Some(field) = auto_increment_field {
                field_values.remove(field.name);
            }
        }

        let primary_key = crud_fields
            .iter()
            .find(|f| f.primary_key)
            .or_else(|| crud_fields.first())
            .map(|f| f.name)
            .ok_or_else(|| Error::OperationFailed {
                operation: "update".into(),
                reason: "no primary key field".into(),
            })?;

        // Build statement using positional ? placeholders in sorted column order.
        let mut keys: Vec<&str> = field_values.keys().copied().collect();
        keys.sort_unstable();
        let columns_csv = keys.join(", ");
        let placeholders = keys.iter().map(|_| "?").collect::<Vec<_>>().join(", ");

        let set_clause = crud_fields
            .iter()
            .filter(|f| f.name != primary_key)
            .map(|f| format!("{} = excluded.{}", f.name, f.name))
            .collect::<Vec<_>>()
            .join(", ");

        let statement = if set_clause.is_empty() || is_new_auto_increment {
            format!(
                "INSERT INTO {table_name} ({columns_csv}) VALUES ({placeholders}) \
                 ON CONFLICT({primary_key}) DO NOTHING"
            )
        } else {
            format!(
                "INSERT INTO {table_name} ({columns_csv}) VALUES ({placeholders}) \
                 ON CONFLICT({primary_key}) DO UPDATE SET {set_clause}"
            )
        };

        let mut query = sqlx::query(&statement);
        for k in keys.iter() {
            let v = field_values.get(k).cloned().unwrap_or(Value::None);
            query = bind_value(query, v);
        }

        let result = query.execute(connection).await.map_err(DomainError::from)?;
        let changed = result.rows_affected() > 0;

        // If it was a new auto_increment record, update with the generated ID
        if is_new_auto_increment {
            let rowid: i64 = sqlx::query_scalar("SELECT last_insert_rowid()")
                .fetch_one(connection)
                .await
                .map_err(DomainError::from)?;
            self.set_primary_key(Value::Integer(rowid));
        }

        Ok(changed)
    }

    async fn read_all<'a>(
        connection: <Sqlite as CrudBackend>::Connection<'a>,
    ) -> TymResult<Pin<Box<dyn Stream<Item = Result<Self, Error<sqlx::Error>>> + 'a>>, sqlx::Error>
    {
        let table_name = Self::table_name();
        let column_names: Vec<&'static str> = Self::crud_fields().iter().map(|f| f.name).collect();
        let statement = format!("SELECT * FROM {table_name};");
        let stream = try_stream! {
            let mut rows = sqlx::query(&statement).fetch(connection);
            while let Some(row) = rows.try_next().await.map_err(DomainError::from)? {
                let mut cols: HashMap<&str, Value> = HashMap::default();
                for name in column_names.iter() {
                    let v = row_to_value(&row, name);
                    cols.insert(*name, v);
                }
                yield Self::try_from_crud_fields(&cols)?;
            }
        };
        Ok(Box::pin(stream))
    }

    async fn read_where<'a, Key: IsCrudField + 'a>(
        connection: <Sqlite as CrudBackend>::Connection<'a>,
        key_name: &'a str,
        comparison: &'a str,
        key_value: Key,
    ) -> TymResult<Pin<Box<dyn Stream<Item = Result<Self, Error<sqlx::Error>>> + 'a>>, sqlx::Error>
    {
        let table_name = Self::table_name();
        let column_names: Vec<&'static str> = Self::crud_fields().iter().map(|f| f.name).collect();
        let statement = format!("SELECT * FROM {table_name} WHERE {key_name} {comparison} ?");
        let stream = try_stream! {
            let mut query = sqlx::query(&statement);
            query = bind_value(query, key_value.value());
            let mut rows = query.fetch(connection);
            while let Some(row) = rows.try_next().await.map_err(DomainError::from)? {
                let mut cols: HashMap<&str, Value> = HashMap::default();
                for name in column_names.iter() {
                    let v = row_to_value(&row, name);
                    cols.insert(*name, v);
                }
                yield Self::try_from_crud_fields(&cols)?;
            }
        };
        Ok(Box::pin(stream))
    }

    async fn read<'a, Key: IsCrudField + 'a>(
        connection: <Sqlite as CrudBackend>::Connection<'a>,
        key: Key,
    ) -> TymResult<Pin<Box<dyn Stream<Item = Result<Self, Error<sqlx::Error>>> + 'a>>, sqlx::Error>
    {
        let stream =
            <Self as Crud<Sqlite>>::read_where(connection, Self::primary_key_name(), "=", key)
                .await?;
        Ok(stream as Pin<Box<dyn Stream<Item = Result<Self, Error<sqlx::Error>>> + 'a>>)
    }

    async fn update<'a>(
        &self,
        connection: &SqlitePool,
    ) -> Result<(), Error<<Sqlite as CrudBackend>::Error>> {
        let fields = self.as_crud_fields();
        let mut primary_key: Option<&str> = None;
        let mut set_columns: Vec<&str> = Vec::new();
        for field in Self::crud_fields().iter() {
            if field.primary_key {
                primary_key = Some(field.name);
            } else {
                set_columns.push(field.name);
            }
        }
        let primary_key = primary_key.ok_or_else(|| Error::OperationFailed {
            operation: "update".into(),
            reason: "no primary key field".into(),
        })?;

        let table_name = Self::table_name();
        let set_clause = set_columns
            .iter()
            .map(|c| format!("{c} = ?"))
            .collect::<Vec<_>>()
            .join(", ");
        let statement = format!("UPDATE {table_name} SET {set_clause} WHERE {primary_key} = ?");

        let mut query = sqlx::query(&statement);
        for c in set_columns.iter() {
            let v = fields.get(c).cloned().unwrap_or(Value::None);
            query = bind_value(query, v);
        }
        let key_val = fields.get(primary_key).cloned().unwrap_or(Value::None);
        query = bind_value(query, key_val);

        let result = query.execute(connection).await.map_err(DomainError::from)?;
        if result.rows_affected() == 0 {
            return Err(Error::OperationFailed {
                operation: "update".into(),
                reason: "no rows updated".into(),
            });
        }
        Ok(())
    }

    async fn delete<'a>(
        self,
        connection: &SqlitePool,
    ) -> Result<(), Error<<Sqlite as CrudBackend>::Error>> {
        let table_name = Self::table_name();
        let key_name = Self::crud_fields()
            .into_iter()
            .find_map(|field| {
                if field.primary_key {
                    Some(field.name)
                } else {
                    None
                }
            })
            .ok_or_else(|| Error::OperationFailed {
                operation: "delete".into(),
                reason: "no primary key field".into(),
            })?;
        let key_value = self
            .as_crud_fields()
            .into_iter()
            .find_map(|(k, v)| if k == key_name { Some(v) } else { None })
            .ok_or_else(|| Error::OperationFailed {
                operation: "delete".into(),
                reason: "no key value".into(),
            })?;
        let statement = format!("DELETE FROM {table_name} WHERE {key_name} = ? RETURNING *");
        let mut stream = bind_value(sqlx::query(&statement), key_value).fetch(connection);
        // Drain RETURNING rows
        while stream
            .try_next()
            .await
            .map_err(DomainError::from)?
            .is_some()
        {}
        Ok(())
    }
}

impl CrudField {
    pub fn sqlite_create_field(&self) -> String {
        let Self {
            name,
            ty,
            nullable,
            primary_key,
            auto_increment,
        } = self;
        let ty = match ty {
            ValueType::Integer => "INTEGER",
            ValueType::Float => "FLOAT",
            ValueType::String => "TEXT",
            ValueType::Bytes => "BLOB",
        };
        let nullable = if *nullable { "" } else { "NOT NULL" };
        let prim_key = if *primary_key { "PRIMARY KEY" } else { "" };
        let inc = if *auto_increment { "AUTOINCREMENT" } else { "" };
        format!("{name} {ty} {prim_key} {inc} {nullable}")
    }
}

/// Read a column out of a `SqliteRow` into a [`Value`].
///
/// Tries the column-typed decode paths in order to detect null vs typed value.
fn row_to_value(row: &sqlx::sqlite::SqliteRow, name: &str) -> Value {
    if let Ok(v) = row.try_get::<Option<i64>, _>(name) {
        return v.map(Value::Integer).unwrap_or(Value::None);
    }
    if let Ok(v) = row.try_get::<Option<f64>, _>(name) {
        return v.map(Value::Float).unwrap_or(Value::None);
    }
    if let Ok(v) = row.try_get::<Option<String>, _>(name) {
        return v.map(Value::String).unwrap_or(Value::None);
    }
    if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(name) {
        return v.map(Value::Bytes).unwrap_or(Value::None);
    }
    Value::None
}

impl MigrateEntireTable for Sqlite {
    async fn read_all_values<'a>(
        connection: <Self as CrudBackend>::Connection<'a>,
        table_name: &'a str,
        fields: Vec<CrudField>,
    ) -> TymResult<Vec<ReadAllValuesResult<'a, Self::Error>>, Self::Error> {
        let column_names: Vec<&str> = fields.iter().map(|f| f.name).collect();
        let statement = format!("SELECT * FROM {table_name};");

        // sqlx::query().fetch() returns a Stream directly (no Result). The "no such table"
        // error surfaces on the first row poll, so handle it inside the loop.
        let mut stream = sqlx::query(&statement).fetch(connection);

        let mut cursor = Vec::new();
        loop {
            match stream.try_next().await {
                Ok(Some(row)) => {
                    let mut cols = HashMap::default();
                    for name in column_names.iter() {
                        let v = row_to_value(&row, name);
                        cols.insert(*name, v);
                    }
                    cursor.push(Ok(cols));
                }
                Ok(None) => break,
                Err(sqlx::Error::Database(ref db_err))
                    if db_err.message().contains("no such table") =>
                {
                    log::debug!("table {table_name} does not exist");
                    return Ok(Vec::new());
                }
                Err(e) => return Err(DomainError { inner: e }.into()),
            }
        }
        Ok(cursor)
    }

    async fn insert_fields<'a>(
        connection: &SqlitePool,
        table_name: &str,
        fields: &HashMap<&str, Value>,
    ) -> Result<(), Error<<Sqlite as CrudBackend>::Error>> {
        let mut keys: Vec<&str> = fields.keys().copied().collect();
        keys.sort_unstable();
        let columns = keys.join(", ");
        let placeholders = keys.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let statement = format!("INSERT INTO {table_name} ({columns}) VALUES ({placeholders});");
        let mut query = sqlx::query(&statement);
        for k in keys.iter() {
            let v = fields.get(k).cloned().unwrap_or(Value::None);
            query = bind_value(query, v);
        }
        let result = query.execute(connection).await.map_err(DomainError::from)?;
        if result.rows_affected() == 0 {
            return Err(Error::OperationFailed {
                operation: "insert".into(),
                reason: "no rows inserted".into(),
            });
        }
        Ok(())
    }

    async fn delete_all<'a>(
        connection: &SqlitePool,
        table_name: &str,
    ) -> Result<(), Error<<Sqlite as CrudBackend>::Error>> {
        let statement = format!("DELETE FROM {table_name};");
        sqlx::query(&statement)
            .execute(connection)
            .await
            .map_err(DomainError::from)?;
        Ok(())
    }
}
