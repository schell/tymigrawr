//! Doltlite backend for `tymigrawr`.
//!
//! The doltlite library is accessed as `rusqlite`. Because `rusqlite` is a blocking
//! API and the `&rusqlite::Connection` is not `Send`, this backend executes each
//! operation inline on the async executor using `tokio::task::block_in_place`.
//! Callers must drive this backend from a multi-threaded tokio runtime
//! (e.g. `#[tokio::test(flavor = "multi_thread")]`).
use std::collections::HashMap;

use async_stream::try_stream;
use futures::{Stream, TryStreamExt};
use rusqlite::{types::ToSqlOutput, Row};

use crate::{
    error::{DomainError, Error, InvalidValueSnafu, TymResult},
    Crud, CrudBackend, CrudField, HasCrudFields, IsCrudField, MigrateEntireTable,
    ReadAllValuesResult, Value, ValueType,
};

pub struct Doltlite;

impl CrudBackend for Doltlite {
    type Connection<'a> = &'a rusqlite::Connection;
    type Error = rusqlite::Error;
}

impl From<rusqlite::Error> for Error<rusqlite::Error> {
    fn from(inner: rusqlite::Error) -> Self {
        Error::Backend {
            source: DomainError { inner },
        }
    }
}

impl From<Value> for rusqlite::types::Value {
    fn from(value: Value) -> Self {
        match value {
            Value::Integer(i) => rusqlite::types::Value::Integer(i),
            Value::Float(i) => rusqlite::types::Value::Real(i),
            Value::String(i) => rusqlite::types::Value::Text(i),
            Value::Bytes(i) => rusqlite::types::Value::Blob(i),
            Value::None => rusqlite::types::Value::Null,
        }
    }
}

impl From<rusqlite::types::Value> for Value {
    fn from(value: rusqlite::types::Value) -> Self {
        match value {
            rusqlite::types::Value::Integer(i) => Value::Integer(i),
            rusqlite::types::Value::Real(i) => Value::Float(i),
            rusqlite::types::Value::Text(i) => Value::String(i),
            rusqlite::types::Value::Blob(i) => Value::Bytes(i),
            rusqlite::types::Value::Null => Value::None,
        }
    }
}

impl rusqlite::types::FromSql for Value {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Ok(match value {
            rusqlite::types::ValueRef::Null => Value::None,
            rusqlite::types::ValueRef::Integer(i) => Value::Integer(i),
            rusqlite::types::ValueRef::Real(f) => Value::Float(f),
            rusqlite::types::ValueRef::Text(items) => {
                Value::String(String::from_utf8_lossy(items).to_string())
            }
            rusqlite::types::ValueRef::Blob(items) => Value::Bytes(items.to_vec()),
        })
    }
}

impl rusqlite::types::ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            Value::Integer(i) => i.to_sql(),
            Value::Float(f) => f.to_sql(),
            Value::String(s) => s.to_sql(),
            Value::Bytes(bytes) => bytes.to_sql(),
            Value::None => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Null)),
        }
    }
}

impl MigrateEntireTable for Doltlite {
    async fn read_all_values<'a>(
        connection: <Self as CrudBackend>::Connection<'a>,
        table_name: &'a str,
        fields: Vec<CrudField>,
    ) -> TymResult<Vec<ReadAllValuesResult<'a, Self::Error>>, Self::Error> {
        tokio::task::block_in_place(|| {
            let column_names: Vec<&str> = fields.iter().map(|f| f.name).collect();
            let statement = format!("SELECT * FROM {table_name};");

            // Attempt to prepare the statement. If the table doesn't exist, return empty results.
            let mut query = match connection.prepare(&statement) {
                Ok(q) => q,
                Err(e) if e.to_string().contains("no such table") => {
                    log::debug!("table {table_name} does not exist");
                    return Ok(Vec::new());
                }
                Err(e) => return Err(DomainError { inner: e }.into()),
            };

            let mut cursor = vec![];
            let mut rows = query.query([])?;
            'next_row: while let Some(row) = rows.next()? {
                let mut cols = HashMap::<&str, Value>::default();
                for name in column_names.iter() {
                    match row.get::<_, Value>(*name) {
                        Ok(value) => {
                            cols.insert(*name, value);
                        }
                        Err(e) => {
                            cursor.push(
                                InvalidValueSnafu {
                                    field: name.to_string(),
                                    reason: e.to_string(),
                                }
                                .fail(),
                            );
                            break 'next_row;
                        }
                    }
                }
                cursor.push(Ok(cols));
            }

            Ok(cursor)
        })
    }

    async fn insert_fields<'a>(
        connection: &rusqlite::Connection,
        table_name: &str,
        fields: &HashMap<&str, Value>,
    ) -> Result<(), Error<<Doltlite as CrudBackend>::Error>> {
        tokio::task::block_in_place(|| {
            let columns = fields.iter().map(|f| *f.0).collect::<Vec<_>>().join(", ");
            let bind_keys: Vec<String> = fields.keys().map(|k| format!(":{k}")).collect();
            let bind_refs: Vec<&str> = bind_keys.iter().map(String::as_str).collect();
            let params: Vec<(&str, &dyn rusqlite::ToSql)> = bind_refs
                .iter()
                .zip(fields.values())
                .map(|(k, v)| (*k, v as &dyn rusqlite::ToSql))
                .collect();
            let sql = format!(
                "INSERT INTO {table_name} ({columns}) VALUES ({});",
                bind_refs.join(", ")
            );

            let changed = connection
                .execute(&sql, params.as_slice())
                .map_err(|e| DomainError { inner: e })?;
            if changed != 1 {
                return Err(Error::OperationFailed {
                    operation: "insert".to_string(),
                    reason: format!("expected 1 row inserted, got {changed}"),
                });
            }
            Ok(())
        })
    }

    async fn delete_all<'a>(
        connection: &rusqlite::Connection,
        table_name: &str,
    ) -> Result<(), Error<<Doltlite as CrudBackend>::Error>> {
        tokio::task::block_in_place(|| {
            let statement = format!("DELETE FROM {table_name};");
            let mut query = connection.prepare(&statement)?;
            query.execute([])?;
            Ok(())
        })
    }
}

impl<T: HasCrudFields + Clone + Sized + 'static> Crud<Doltlite> for T {
    async fn create<'a>(
        connection: <Doltlite as CrudBackend>::Connection<'a>,
    ) -> TymResult<(), <Doltlite as CrudBackend>::Error> {
        tokio::task::block_in_place(|| {
            let table_name = Self::table_name();
            let fields: String = Self::crud_fields()
                .iter()
                .map(CrudField::doltlite_create_field)
                .collect::<Vec<_>>()
                .join(", ");
            let statement = format!("CREATE TABLE IF NOT EXISTS {table_name} ({fields});");
            connection
                .execute(&statement, [])
                .map_err(|inner| DomainError { inner })?;
            Ok(())
        })
    }

    async fn insert<'a>(
        &mut self,
        connection: <Doltlite as CrudBackend>::Connection<'a>,
    ) -> std::result::Result<(), Error<<Doltlite as CrudBackend>::Error>> {
        let has_auto_increment = {
            let crud_fields = Self::crud_fields();
            let mut fields = self.as_crud_fields();
            crud_fields.iter().find_map(|field| {
                if field.auto_increment && matches!(fields.get(field.name), Some(Value::None)) {
                    fields.remove(field.name);
                    Some(field.name)
                } else {
                    None
                }
            })
        };

        let table_name = Self::table_name();
        let mut fields = self.as_crud_fields();
        if let Some(name) = has_auto_increment {
            fields.remove(name);
        }
        Doltlite::insert_fields(connection, table_name, &fields).await?;

        // If there was an auto_increment field with None value, update it with the generated ID
        if has_auto_increment.is_some() {
            let rowid: i64 = tokio::task::block_in_place(|| {
                connection
                    .query_row("SELECT last_insert_rowid()", [], |r| r.get(0))
                    .map_err(|e| DomainError { inner: e })
            })?;
            self.set_primary_key(Value::Integer(rowid));
        }

        Ok(())
    }

    async fn upsert<'a>(
        &mut self,
        connection: &rusqlite::Connection,
    ) -> Result<bool, Error<<Doltlite as CrudBackend>::Error>> {
        tokio::task::block_in_place(|| {
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
                    operation: "upsert".into(),
                    reason: "no primary key field".into(),
                })?;

            let set_clause = crud_fields
                .iter()
                .filter(|f| f.name != primary_key)
                .map(|f| format!("{} = excluded.{}", f.name, f.name))
                .collect::<Vec<_>>()
                .join(", ");

            let columns = field_values.keys().copied().collect::<Vec<_>>().join(", ");
            let binds = field_values
                .keys()
                .map(|k| format!(":{}", k))
                .collect::<Vec<_>>()
                .join(", ");

            let statement = if set_clause.is_empty() || is_new_auto_increment {
                format!(
                    "INSERT INTO {table_name} ({columns}) VALUES ({binds}) \
                     ON CONFLICT({primary_key}) DO NOTHING"
                )
            } else {
                format!(
                    "INSERT INTO {table_name} ({columns}) VALUES ({binds}) \
                     ON CONFLICT({primary_key}) DO UPDATE SET {set_clause}"
                )
            };

            let bind_keys: Vec<String> = field_values.keys().map(|k| format!(":{}", k)).collect();
            let bind_refs: Vec<&str> = bind_keys.iter().map(String::as_str).collect();
            let params: Vec<(&str, &dyn rusqlite::ToSql)> = bind_refs
                .iter()
                .zip(field_values.values())
                .map(|(k, v)| (*k, v as &dyn rusqlite::ToSql))
                .collect();

            let changed = connection
                .execute(&statement, params.as_slice())
                .map_err(|e| DomainError { inner: e })?;

            // If it was a new auto_increment record, update with the generated ID
            if is_new_auto_increment {
                let rowid: i64 = connection
                    .query_row("SELECT last_insert_rowid()", [], |r| r.get(0))
                    .map_err(|e| DomainError { inner: e })?;
                self.set_primary_key(Value::Integer(rowid));
            }

            Ok(changed > 0)
        })
    }

    async fn read_all<'a>(
        connection: <Doltlite as CrudBackend>::Connection<'a>,
    ) -> TymResult<
        std::pin::Pin<Box<dyn Stream<Item = Result<Self, Error<rusqlite::Error>>> + 'a>>,
        rusqlite::Error,
    > {
        let table_name = Self::table_name();
        let column_names: Vec<&'static str> = Self::crud_fields().iter().map(|f| f.name).collect();
        // Eagerly collect rows under block_in_place, then stream them out.
        let rows: Vec<TymResult<HashMap<&'static str, Value>, rusqlite::Error>> =
            tokio::task::block_in_place(|| -> TymResult<Vec<TymResult<HashMap<&'static str, Value>, rusqlite::Error>>, rusqlite::Error> {
                let statement = format!("SELECT * FROM {table_name};");
                let mut query = match connection.prepare(&statement) {
                    Ok(q) => q,
                    Err(e) if e.to_string().contains("no such table") => {
                        return Ok(Vec::new());
                    }
                    Err(e) => return Err(DomainError { inner: e }.into()),
                };
                let mut rows = query.query([])?;
                let mut out = Vec::new();
                while let Some(row) = rows.next()? {
                    let mut cols = HashMap::new();
                    for name in column_names.iter() {
                        let value = row.get::<_, Value>(*name)?;
                        cols.insert(*name, value);
                    }
                    out.push(Ok(cols));
                }
                Ok(out)
            })?;
        let stream = try_stream! {
            for cols in rows {
                yield Self::try_from_crud_fields(&cols?)?;
            }
        };
        Ok(Box::pin(stream))
    }

    async fn read_where<'a, Key: IsCrudField + 'a>(
        connection: <Doltlite as CrudBackend>::Connection<'a>,
        key_name: &'a str,
        comparison: &'a str,
        key_value: Key,
    ) -> TymResult<
        std::pin::Pin<Box<dyn Stream<Item = Result<Self, Error<rusqlite::Error>>> + 'a>>,
        rusqlite::Error,
    > {
        let table_name = Self::table_name();
        let column_names: Vec<&'static str> = Self::crud_fields().iter().map(|f| f.name).collect();
        let rows: Vec<TymResult<Self, rusqlite::Error>> = tokio::task::block_in_place(
            || -> TymResult<Vec<TymResult<Self, rusqlite::Error>>, rusqlite::Error> {
                let statement =
                    format!("SELECT * FROM {table_name} WHERE {key_name} {comparison} ?");
                let mut query = connection.prepare(&statement)?;
                let value = key_value.value();
                let mut rows = query.query(&[&value as &dyn rusqlite::ToSql])?;
                let mut out = Vec::new();
                while let Some(row) = rows.next()? {
                    let mut cols = HashMap::new();
                    for name in column_names.iter() {
                        let value = row.get::<_, Value>(*name)?;
                        cols.insert(*name, value);
                    }
                    out.push(Self::try_from_crud_fields(&cols).map_err(Error::from));
                }
                Ok(out)
            },
        )?;
        let stream = try_stream! {
            for r in rows {
                yield r?;
            }
        };
        Ok(Box::pin(stream))
    }

    async fn read<'a, Key: IsCrudField + 'a>(
        connection: <Doltlite as CrudBackend>::Connection<'a>,
        key: Key,
    ) -> TymResult<
        std::pin::Pin<Box<dyn Stream<Item = Result<Self, Error<rusqlite::Error>>> + 'a>>,
        rusqlite::Error,
    > {
        let stream =
            <Self as Crud<Doltlite>>::read_where(connection, Self::primary_key_name(), "=", key)
                .await?;
        Ok(stream
            as std::pin::Pin<Box<dyn Stream<Item = Result<Self, Error<rusqlite::Error>>> + 'a>>)
    }

    async fn update<'a>(
        &self,
        connection: &rusqlite::Connection,
    ) -> Result<(), Error<<Doltlite as CrudBackend>::Error>> {
        tokio::task::block_in_place(|| {
            let fields = self.as_crud_fields();
            let mut primary_key: Option<&str> = None;
            let values = Self::crud_fields()
                .iter()
                .filter_map(|field| {
                    if field.primary_key {
                        primary_key = Some(field.name);
                        None
                    } else {
                        Some(format!("{} = :{}", field.name, field.name))
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            let primary_key = primary_key.ok_or_else(|| Error::OperationFailed {
                operation: "update".into(),
                reason: "no primary key field".into(),
            })?;

            let table_name = Self::table_name();
            let statement =
                format!("UPDATE {table_name} SET {values} WHERE {primary_key} = :key_value",);

            let mut bind_keys: Vec<String> = Vec::new();
            let mut value_params: Vec<rusqlite::types::Value> = Vec::new();
            let mut key_value: Option<rusqlite::types::Value> = None;
            for (key, value) in fields.into_iter() {
                if key == primary_key {
                    key_value = Some(rusqlite::types::Value::from(value));
                    continue;
                }
                bind_keys.push(format!(":{key}"));
                value_params.push(rusqlite::types::Value::from(value));
            }
            let key_value = key_value.ok_or_else(|| Error::OperationFailed {
                operation: "update".into(),
                reason: "no key value".into(),
            })?;

            let bind_refs: Vec<&str> = bind_keys.iter().map(String::as_str).collect();
            let mut params: Vec<(&str, &dyn rusqlite::ToSql)> = bind_refs
                .iter()
                .zip(value_params.iter())
                .map(|(k, v)| (*k, v as &dyn rusqlite::ToSql))
                .collect();
            params.push((":key_value", &key_value as &dyn rusqlite::ToSql));

            let changed = connection
                .execute(&statement, params.as_slice())
                .map_err(|e| DomainError { inner: e })?;

            if changed == 0 {
                return Err(Error::OperationFailed {
                    operation: "update".into(),
                    reason: "no rows updated".into(),
                });
            }

            Ok(())
        })
    }

    async fn delete<'a>(
        self,
        connection: &rusqlite::Connection,
    ) -> Result<(), Error<<Doltlite as CrudBackend>::Error>> {
        tokio::task::block_in_place(|| {
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
            let key_value = rusqlite::types::Value::from(key_value);
            let statement = format!("DELETE FROM {table_name} WHERE {key_name} = :key_value");
            connection
                .execute(
                    &statement,
                    &[(":key_value", &key_value as &dyn rusqlite::ToSql)],
                )
                .map_err(|e| DomainError { inner: e })?;
            Ok(())
        })
    }
}

impl CrudField {
    pub fn doltlite_create_field(&self) -> String {
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
