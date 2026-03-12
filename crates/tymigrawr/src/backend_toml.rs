//! TOML flat-file backend.
//!
//! Each table is stored as a single TOML file (`<data_dir>/<table_name>.toml`)
//! containing an array of row tables under the `[[row]]` key.
//!
//! The `Connection` type is `&Path` (the data directory).
//! Byte values are stored as base64-encoded strings.
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use snafu::{OptionExt, ResultExt};

use crate::{
    Crud, CrudBackend, CrudField, HasCrudFields, IsCrudField, MigrateEntireTable, Migration, Value,
    ValueType,
};

/// Returns the path to the TOML file for a given table.
fn table_path(dir: &Path, table_name: &str) -> PathBuf {
    dir.join(format!("{table_name}.toml"))
}

/// Convert a `Value` into a `toml::Value`.
///
/// `Value::None` maps to a TOML string `"__none__"` so it can be stored
/// as a table entry (TOML has no null type).
fn value_to_toml(value: &Value) -> toml::Value {
    match value {
        Value::Integer(i) => toml::Value::Integer(*i),
        Value::Float(f) => toml::Value::Float(*f),
        Value::String(s) => toml::Value::String(s.clone()),
        Value::Bytes(b) => toml::Value::String(BASE64.encode(b)),
        Value::None => toml::Value::String("__none__".to_string()),
    }
}

/// Convert a `toml::Value` back into a `Value`, using `ValueType` to
/// disambiguate TOML strings that represent byte arrays.
fn toml_to_value(tv: &toml::Value, vt: &ValueType) -> Option<Value> {
    match tv {
        toml::Value::Integer(i) => Some(Value::Integer(*i)),
        toml::Value::Float(f) => Some(Value::Float(*f)),
        toml::Value::String(s) => {
            if s == "__none__" {
                return Some(Value::None);
            }
            match vt {
                ValueType::Bytes => {
                    let bytes = BASE64.decode(s).ok()?;
                    Some(Value::Bytes(bytes))
                }
                _ => Some(Value::String(s.clone())),
            }
        }
        _ => None,
    }
}

/// Read all rows from a table's TOML file.
///
/// Returns an empty vec if the file does not exist.
fn read_rows(path: &Path) -> Result<Vec<toml::value::Table>, snafu::Whatever> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content =
        fs::read_to_string(path).whatever_context(format!("read table file {:?}", path))?;
    if content.trim().is_empty() {
        return Ok(Vec::new());
    }
    let doc: toml::Value = content
        .parse()
        .whatever_context(format!("parse TOML from {:?}", path))?;
    let rows = doc
        .get("row")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_table().cloned())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Ok(rows)
}

/// Write rows to a table's TOML file as `[[row]]` entries.
fn write_rows(path: &Path, rows: &[toml::value::Table]) -> Result<(), snafu::Whatever> {
    let mut doc = toml::value::Table::new();
    let arr = rows
        .iter()
        .map(|r| toml::Value::Table(r.clone()))
        .collect::<Vec<_>>();
    doc.insert("row".to_string(), toml::Value::Array(arr));
    let content = toml::to_string_pretty(&doc).whatever_context("serialize TOML")?;
    fs::write(path, content).whatever_context(format!("write table file {:?}", path))
}

/// Convert a `HashMap<&str, Value>` of field values into a TOML row table.
fn fields_to_row(fields: &HashMap<&str, Value>) -> toml::value::Table {
    let mut row = toml::value::Table::new();
    for (key, value) in fields.iter() {
        row.insert((*key).to_string(), value_to_toml(value));
    }
    row
}

/// Convert a TOML row table back into a `HashMap<&str, Value>`, using the
/// field metadata to properly decode bytes vs strings.
fn row_to_fields<'a>(row: &toml::value::Table, fields: &[CrudField]) -> HashMap<&'a str, Value> {
    let mut map = HashMap::new();
    for field in fields.iter() {
        let value = match row.get(field.name) {
            Some(tv) => toml_to_value(tv, &field.ty).unwrap_or(Value::None),
            None => Value::None,
        };
        map.insert(field.name, value);
    }
    map
}

/// Compare two `Value`s using a SQL-style comparison operator.
///
/// Supports `=`, `!=`, `<`, `>`, `<=`, `>=`.
fn compare_values(lhs: &Value, comparison: &str, rhs: &Value) -> Result<bool, snafu::Whatever> {
    let ord = partial_cmp_values(lhs, rhs);
    match comparison {
        "=" => Ok(lhs == rhs),
        "!=" => Ok(lhs != rhs),
        "<" => Ok(ord.map(|o| o.is_lt()).unwrap_or(false)),
        ">" => Ok(ord.map(|o| o.is_gt()).unwrap_or(false)),
        "<=" => Ok(ord.map(|o| o.is_le()).unwrap_or(false)),
        ">=" => Ok(ord.map(|o| o.is_ge()).unwrap_or(false)),
        _ => snafu::whatever!("unsupported comparison operator: {comparison}"),
    }
}

/// Partial ordering for `Value` variants of the same type.
fn partial_cmp_values(lhs: &Value, rhs: &Value) -> Option<std::cmp::Ordering> {
    match (lhs, rhs) {
        (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::String(a), Value::String(b)) => a.partial_cmp(b),
        (Value::Bytes(a), Value::Bytes(b)) => a.partial_cmp(b),
        (Value::None, Value::None) => Some(std::cmp::Ordering::Equal),
        _ => None,
    }
}

/// TOML flat-file backend marker type.
pub struct Toml;

impl CrudBackend for Toml {
    type Connection<'a> = &'a Path;
}

impl MigrateEntireTable for Toml {
    fn read_all_values<'a>(
        connection: &'a Path,
        table_name: &'a str,
        fields: Vec<CrudField>,
    ) -> Result<Vec<Result<HashMap<&'a str, Value>, snafu::Whatever>>, snafu::Whatever> {
        let path = table_path(connection, table_name);
        let rows = read_rows(&path)?;
        let result = rows
            .into_iter()
            .map(|row| {
                let map = row_to_fields(&row, &fields);
                Ok(map)
            })
            .collect();
        Ok(result)
    }

    fn insert_fields(
        connection: &Path,
        table_name: &str,
        fields: &HashMap<&str, Value>,
    ) -> Result<(), snafu::Whatever> {
        let path = table_path(connection, table_name);
        let mut rows = read_rows(&path)?;
        rows.push(fields_to_row(fields));
        write_rows(&path, &rows)
    }

    fn delete_all(connection: &Path, table_name: &str) -> Result<(), snafu::Whatever> {
        let path = table_path(connection, table_name);
        if path.exists() {
            write_rows(&path, &[])?;
        }
        Ok(())
    }
}

impl<T: HasCrudFields + Clone + Sized + 'static> Crud<Toml> for T {
    /// Create the data directory (if needed) and an empty table file if it
    /// does not already exist.
    fn create(connection: &Path) -> Result<(), snafu::Whatever> {
        fs::create_dir_all(connection)
            .whatever_context(format!("create data dir {:?}", connection))?;
        let path = table_path(connection, Self::table_name());
        if !path.exists() {
            write_rows(&path, &[])?;
        }
        Ok(())
    }

    fn insert(&self, connection: &Path) -> Result<(), snafu::Whatever> {
        let table_name = Self::table_name();
        let fields = self.as_crud_fields();
        Toml::insert_fields(connection, table_name, &fields)
    }

    fn upsert(&self, connection: &Path) -> Result<bool, snafu::Whatever> {
        let table_name = Self::table_name();
        let path = table_path(connection, table_name);
        let crud_fields = Self::crud_fields();
        let pk_name = Self::primary_key_name();
        let new_fields = self.as_crud_fields();
        let pk_value = new_fields
            .get(pk_name)
            .whatever_context("missing primary key value for upsert")?;

        let mut rows = read_rows(&path)?;
        let pk_field = crud_fields
            .iter()
            .find(|f| f.name == pk_name)
            .whatever_context("primary key field not in schema")?;

        let mut found = false;
        for row in rows.iter_mut() {
            let matches = match row.get(pk_name) {
                Some(tv) => {
                    let row_val = toml_to_value(tv, &pk_field.ty).unwrap_or(Value::None);
                    &row_val == pk_value
                }
                None => false,
            };
            if matches {
                *row = fields_to_row(&new_fields);
                found = true;
                break;
            }
        }

        if !found {
            rows.push(fields_to_row(&new_fields));
        }

        write_rows(&path, &rows)?;
        Ok(true)
    }

    fn read_all<'a>(
        connection: <Toml as CrudBackend>::Connection<'a>,
    ) -> Result<Box<dyn Iterator<Item = Result<Self, snafu::Whatever>> + 'a>, snafu::Whatever> {
        let table_name = Self::table_name();
        let cursor = Toml::read_all_values(connection, table_name, Self::crud_fields())?;
        Ok(Box::new(
            cursor
                .into_iter()
                .map(|cols| Self::try_from_crud_fields(&cols?)),
        ))
    }

    fn read_where<'a>(
        connection: &'a Path,
        key_name: &'a str,
        comparison: &'a str,
        key_value: impl IsCrudField,
    ) -> Result<Box<dyn Iterator<Item = Result<Self, snafu::Whatever>> + 'a>, snafu::Whatever> {
        let rhs = key_value.into_value();
        let cursor = Toml::read_all_values(connection, Self::table_name(), Self::crud_fields())?;
        let iter = cursor.into_iter().filter_map(move |cols| match cols {
            Ok(ref map) => {
                let lhs = map.get(key_name)?;
                match compare_values(lhs, comparison, &rhs) {
                    Ok(true) => Some(Self::try_from_crud_fields(map)),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                }
            }
            Err(e) => Some(Err(e)),
        });
        Ok(Box::new(iter))
    }

    fn read<'a, Key: IsCrudField>(
        connection: <Toml as CrudBackend>::Connection<'a>,
        key: Key,
    ) -> Result<Box<dyn Iterator<Item = Result<Self, snafu::Whatever>> + 'a>, snafu::Whatever> {
        <Self as Crud<Toml>>::read_where(connection, Self::primary_key_name(), "=", key)
    }

    fn update(&self, connection: &Path) -> Result<(), snafu::Whatever> {
        let table_name = Self::table_name();
        let path = table_path(connection, table_name);
        let crud_fields = Self::crud_fields();
        let pk_name = Self::primary_key_name();
        let new_fields = self.as_crud_fields();
        let pk_value = new_fields
            .get(pk_name)
            .whatever_context("missing primary key value for update")?;

        let mut rows = read_rows(&path)?;
        let mut found = false;
        for row in rows.iter_mut() {
            let row_pk = row.get(pk_name);
            let matches = match row_pk {
                Some(tv) => {
                    let pk_field = crud_fields
                        .iter()
                        .find(|f| f.name == pk_name)
                        .whatever_context("primary key field not in schema")?;
                    let row_val = toml_to_value(tv, &pk_field.ty).unwrap_or(Value::None);
                    &row_val == pk_value
                }
                None => false,
            };
            if matches {
                *row = fields_to_row(&new_fields);
                found = true;
                break;
            }
        }
        snafu::ensure_whatever!(found, "no row with matching primary key to update");
        write_rows(&path, &rows)
    }

    fn delete(self, connection: &Path) -> Result<(), snafu::Whatever> {
        let table_name = Self::table_name();
        let path = table_path(connection, table_name);
        let crud_fields = Self::crud_fields();
        let pk_name = Self::primary_key_name();
        let my_fields = self.as_crud_fields();
        let pk_value = my_fields
            .get(pk_name)
            .whatever_context("missing primary key value for delete")?;

        let rows = read_rows(&path)?;
        let pk_field = crud_fields
            .iter()
            .find(|f| f.name == pk_name)
            .whatever_context("primary key field not in schema")?;
        let remaining: Vec<_> = rows
            .into_iter()
            .filter(|row| match row.get(pk_name) {
                Some(tv) => {
                    let row_val = toml_to_value(tv, &pk_field.ty).unwrap_or(Value::None);
                    &row_val != pk_value
                }
                None => true,
            })
            .collect();
        write_rows(&path, &remaining)
    }

    fn migration<S: 'static>() -> Migration
    where
        Self: From<S>,
    {
        Migration {
            table_name: Box::new(Self::table_name),
            crud_fields: Box::new(Self::crud_fields),
            from_prev: Box::new(|any: Box<dyn core::any::Any>| {
                // SAFETY: we know we can downcast because of the Self: From<S>
                // constraint
                let t: Box<S> = any.downcast().unwrap();
                let s = Self::from(*t);
                Box::new(s)
            }),
            as_crud_fields: Box::new(|any: &Box<dyn core::any::Any>| {
                if let Some(s) = any.downcast_ref::<Self>() {
                    s.as_crud_fields()
                } else {
                    Default::default()
                }
            }),
            try_from_crud_fields: Box::new(|fields| {
                let s = Self::try_from_crud_fields(fields)?;
                Ok(Box::new(s))
            }),
        }
    }
}
