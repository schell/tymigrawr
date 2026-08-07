//! IndexedDB backend for WASM targets.
//!
//! Each table is an IndexedDB object store. Each row is stored as a JSON
//! value keyed by its primary key.
//!
//! The `Connection` type is `&str` — the database name. All `Crud` trait
//! methods are `async` and await the underlying `rexie` futures directly.

use crate::{
    error::{DomainError, Error, TymResult},
    Crud, CrudBackend, CrudField, HasCrudFields, IsCrudField, MigrateEntireTable, Value,
};
use std::{collections::HashMap, pin::Pin};

use async_stream::try_stream;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use futures::{Stream, StreamExt};
use wasm_bindgen::JsValue;

/// IndexedDB backend marker type.
pub struct IndexedDb;

/// Errors from IndexedDB operations.
#[derive(Debug)]
pub struct IndexedDbError {
    pub message: String,
}

impl std::fmt::Display for IndexedDbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IndexedDB error: {}", self.message)
    }
}

impl std::error::Error for IndexedDbError {}

impl From<rexie::Error> for IndexedDbError {
    fn from(e: rexie::Error) -> Self {
        Self {
            message: e.to_string(),
        }
    }
}

impl From<serde_json::Error> for IndexedDbError {
    fn from(e: serde_json::Error) -> Self {
        Self {
            message: format!("serde error: {e}"),
        }
    }
}

impl From<IndexedDbError> for Error<IndexedDbError> {
    fn from(inner: IndexedDbError) -> Self {
        Error::Backend {
            source: DomainError { inner },
        }
    }
}

/// Wrap any error convertible to `IndexedDbError` as an `Error<IndexedDbError>`.
fn bd_err<E>(e: E) -> Error<IndexedDbError>
where
    IndexedDbError: From<E>,
{
    Error::from(IndexedDbError::from(e))
}

/// Convert a serde_json::Value to a JsValue for IDB storage.
fn to_js_value(value: &serde_json::Value) -> Result<JsValue, IndexedDbError> {
    serde_wasm_bindgen::to_value(value).map_err(|e| IndexedDbError {
        message: e.to_string(),
    })
}

/// Convert a JsValue back to a serde_json::Value.
fn from_js_value(value: &JsValue) -> Result<serde_json::Value, IndexedDbError> {
    serde_wasm_bindgen::from_value(value.clone()).map_err(|e| IndexedDbError {
        message: e.to_string(),
    })
}

impl CrudBackend for IndexedDb {
    type Connection<'a> = &'a str;
    type Error = IndexedDbError;
}

/// Open (or create) an IndexedDB database with the given store name.
async fn open_db(db_name: &str, store_name: &str) -> Result<rexie::Rexie, IndexedDbError> {
    let rexie = rexie::Rexie::builder(db_name)
        .version(1)
        .add_object_store(rexie::ObjectStore::new(store_name))
        .build()
        .await?;
    Ok(rexie)
}

/// Serialize a row's crud fields to a JSON object.
fn row_to_json(fields: &HashMap<&str, Value>) -> Result<serde_json::Value, IndexedDbError> {
    let mut map = serde_json::Map::new();
    for (key, value) in fields {
        let json_val = match value {
            Value::Integer(i) => serde_json::Value::from(*i),
            Value::Float(f) => serde_json::Value::from(*f),
            Value::String(s) => serde_json::Value::from(s.clone()),
            Value::Bytes(b) => serde_json::Value::from(BASE64.encode(b)),
            Value::None => serde_json::Value::Null,
        };
        map.insert(key.to_string(), json_val);
    }
    Ok(serde_json::Value::Object(map))
}

/// Deserialize a JSON object back to crud fields.
fn json_to_row(
    json: &serde_json::Value,
    fields: &[CrudField],
) -> Result<HashMap<&'static str, Value>, Error<IndexedDbError>> {
    let obj = json.as_object().ok_or_else(|| Error::OperationFailed {
        operation: "read".into(),
        reason: "stored value is not a JSON object".into(),
    })?;

    let mut result = HashMap::new();
    for field in fields {
        let json_val = obj.get(field.name).unwrap_or(&serde_json::Value::Null);
        let value = match (&field.ty, json_val) {
            (crate::ValueType::Integer, serde_json::Value::Number(n)) => {
                Value::Integer(n.as_i64().unwrap_or(0))
            }
            (crate::ValueType::Float, serde_json::Value::Number(n)) => {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
            (crate::ValueType::String, serde_json::Value::String(s)) => Value::String(s.clone()),
            (crate::ValueType::Bytes, serde_json::Value::String(s)) => {
                Value::Bytes(BASE64.decode(s).unwrap_or_default())
            }
            _ => Value::None,
        };
        // SAFETY: field.name is a &'static str from the HasCrudFields trait
        result.insert(
            unsafe { std::mem::transmute::<&str, &'static str>(field.name) },
            value,
        );
    }
    Ok(result)
}

/// Extract the primary key value from crud fields as a serde_json::Value.
fn pk_as_json(fields: &HashMap<&str, Value>, crud_fields: &[CrudField]) -> serde_json::Value {
    let pk_name = crud_fields
        .iter()
        .find(|f| f.primary_key)
        .map(|f| f.name)
        .or_else(|| crud_fields.first().map(|f| f.name));

    match pk_name.and_then(|name| fields.get(name)) {
        Some(Value::String(s)) => serde_json::Value::from(s.clone()),
        Some(Value::Integer(i)) => serde_json::Value::from(*i),
        _ => serde_json::Value::Null,
    }
}

/// Partial ordering for `Value` variants of the same type.
///
/// Returns `None` when the values are of different types or otherwise
/// incomparable (matching the behavior of the TOML backend).
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

impl<T: HasCrudFields + Clone + Sized + 'static> Crud<IndexedDb> for T {
    async fn create<'a>(connection: &str) -> Result<(), Error<<IndexedDb as CrudBackend>::Error>> {
        let db_name = connection.to_string();
        let store_name = Self::table_name().to_string();
        let _rexie = open_db(&db_name, &store_name).await.map_err(bd_err)?;
        Ok(())
    }

    async fn insert<'a>(
        &mut self,
        connection: &str,
    ) -> Result<(), Error<<IndexedDb as CrudBackend>::Error>> {
        let store_name = Self::table_name().to_string();
        let fields = self.as_crud_fields();
        let crud_fields = Self::crud_fields();
        let row_json = row_to_json(&fields).map_err(bd_err)?;
        let pk_json = pk_as_json(&fields, &crud_fields);
        let db_name = connection.to_string();

        let rexie = open_db(&db_name, &store_name).await.map_err(bd_err)?;
        let transaction = rexie
            .transaction(&[&store_name], rexie::TransactionMode::ReadWrite)
            .map_err(bd_err)?;
        let store = transaction.store(&store_name).map_err(bd_err)?;
        let row_js = to_js_value(&row_json).map_err(bd_err)?;
        let pk_js = to_js_value(&pk_json).map_err(bd_err)?;
        store.put(&row_js, Some(&pk_js)).await.map_err(bd_err)?;
        transaction.done().await.map_err(bd_err)?;
        Ok(())
    }

    async fn upsert<'a>(
        &mut self,
        connection: &str,
    ) -> Result<bool, Error<<IndexedDb as CrudBackend>::Error>> {
        self.insert(connection).await?;
        Ok(true)
    }

    async fn read_all<'a>(
        connection: <IndexedDb as CrudBackend>::Connection<'a>,
    ) -> TymResult<
        Pin<Box<dyn Stream<Item = Result<Self, Error<IndexedDbError>>> + 'a>>,
        IndexedDbError,
    > {
        let db_name = connection.to_string();
        let store_name = Self::table_name().to_string();
        let fields = Self::crud_fields();

        let all_rows: Vec<serde_json::Value> = {
            let rexie = open_db(&db_name, &store_name).await.map_err(bd_err)?;
            let transaction = rexie
                .transaction(&[&store_name], rexie::TransactionMode::ReadOnly)
                .map_err(bd_err)?;
            let store = transaction.store(&store_name).map_err(bd_err)?;
            let all_js = store.get_all(None, None).await.map_err(bd_err)?;
            transaction.done().await.map_err(bd_err)?;
            let mut all = Vec::with_capacity(all_js.len());
            for js in all_js {
                all.push(from_js_value(&js)?);
            }
            all
        };

        let stream = try_stream! {
            for json in all_rows {
                let cols = json_to_row(&json, &fields)?;
                yield Self::try_from_crud_fields(&cols)?;
            }
        };
        Ok(Box::pin(stream))
    }

    async fn read_where<'a, Key: IsCrudField + 'a>(
        connection: <IndexedDb as CrudBackend>::Connection<'a>,
        key_name: &'a str,
        comparison: &'a str,
        key_value: Key,
    ) -> TymResult<
        Pin<Box<dyn Stream<Item = Result<Self, Error<IndexedDbError>>> + 'a>>,
        IndexedDbError,
    > {
        let rhs = key_value.value();

        let all: Vec<_> = <Self as Crud<IndexedDb>>::read_all(connection)
            .await?
            .filter_map(|r| async move { r.ok() })
            .filter(|row| {
                let fields = row.as_crud_fields();
                let matches = match fields.get(key_name) {
                    Some(lhs) => match comparison {
                        "=" => lhs == &rhs,
                        "!=" => lhs != &rhs,
                        "<" => partial_cmp_values(lhs, &rhs)
                            .map(|o| o.is_lt())
                            .unwrap_or(false),
                        ">" => partial_cmp_values(lhs, &rhs)
                            .map(|o| o.is_gt())
                            .unwrap_or(false),
                        "<=" => partial_cmp_values(lhs, &rhs)
                            .map(|o| o.is_le())
                            .unwrap_or(false),
                        ">=" => partial_cmp_values(lhs, &rhs)
                            .map(|o| o.is_ge())
                            .unwrap_or(false),
                        _ => false,
                    },
                    None => false,
                };
                async move { matches }
            })
            .collect()
            .await;

        Ok(Box::pin(futures::stream::iter(all.into_iter().map(Ok))))
    }

    async fn read<'a, Key: IsCrudField + 'a>(
        connection: <IndexedDb as CrudBackend>::Connection<'a>,
        key: Key,
    ) -> TymResult<
        Pin<Box<dyn Stream<Item = Result<Self, Error<IndexedDbError>>> + 'a>>,
        IndexedDbError,
    > {
        <Self as Crud<IndexedDb>>::read_where(connection, Self::primary_key_name(), "=", key).await
    }

    async fn update<'a>(
        &self,
        connection: &str,
    ) -> Result<(), Error<<IndexedDb as CrudBackend>::Error>> {
        let mut clone = self.clone();
        clone.insert(connection).await
    }

    async fn delete<'a>(
        self,
        connection: &str,
    ) -> Result<(), Error<<IndexedDb as CrudBackend>::Error>> {
        let store_name = Self::table_name().to_string();
        let fields = self.as_crud_fields();
        let crud_fields = Self::crud_fields();
        let pk_json = pk_as_json(&fields, &crud_fields);
        let db_name = connection.to_string();

        let rexie = open_db(&db_name, &store_name).await.map_err(bd_err)?;
        let transaction = rexie
            .transaction(&[&store_name], rexie::TransactionMode::ReadWrite)
            .map_err(bd_err)?;
        let store = transaction.store(&store_name).map_err(bd_err)?;
        let pk_js = to_js_value(&pk_json).map_err(bd_err)?;
        store.delete(pk_js).await.map_err(bd_err)?;
        transaction.done().await.map_err(bd_err)?;
        Ok(())
    }
}

impl MigrateEntireTable for IndexedDb {
    async fn read_all_values<'a>(
        connection: <IndexedDb as CrudBackend>::Connection<'a>,
        table_name: &'a str,
        fields: Vec<CrudField>,
    ) -> TymResult<Vec<crate::ReadAllValuesResult<'a, IndexedDbError>>, IndexedDbError> {
        let db_name = connection.to_string();
        let store_name = table_name.to_string();

        let all_json: Vec<serde_json::Value> = {
            let rexie = open_db(&db_name, &store_name).await.map_err(bd_err)?;
            let transaction = rexie
                .transaction(&[&store_name], rexie::TransactionMode::ReadOnly)
                .map_err(bd_err)?;
            let store = transaction.store(&store_name).map_err(bd_err)?;
            let all_js = store.get_all(None, None).await.map_err(bd_err)?;
            transaction.done().await.map_err(bd_err)?;
            let mut all = Vec::with_capacity(all_js.len());
            for js in all_js {
                all.push(from_js_value(&js)?);
            }
            all
        };

        let rows = all_json
            .into_iter()
            .map(|json| {
                let cols = json_to_row(&json, &fields)?;
                Ok(cols)
            })
            .collect::<Vec<_>>();
        Ok(rows)
    }

    async fn insert_fields<'a>(
        connection: &str,
        table_name: &str,
        fields: &HashMap<&str, Value>,
    ) -> Result<(), Error<<IndexedDb as CrudBackend>::Error>> {
        let row_json = row_to_json(fields).map_err(bd_err)?;
        let pk_json = fields
            .iter()
            .next()
            .map(|(_, v)| match v {
                Value::String(s) => serde_json::Value::from(s.clone()),
                Value::Integer(i) => serde_json::Value::from(*i),
                _ => serde_json::Value::Null,
            })
            .unwrap_or(serde_json::Value::Null);

        let db_name = connection.to_string();
        let store_name = table_name.to_string();
        let rexie = open_db(&db_name, &store_name).await.map_err(bd_err)?;
        let transaction = rexie
            .transaction(&[&store_name], rexie::TransactionMode::ReadWrite)
            .map_err(bd_err)?;
        let store = transaction.store(&store_name).map_err(bd_err)?;
        let row_js = to_js_value(&row_json).map_err(bd_err)?;
        let pk_js = to_js_value(&pk_json).map_err(bd_err)?;
        store.put(&row_js, Some(&pk_js)).await.map_err(bd_err)?;
        transaction.done().await.map_err(bd_err)?;
        Ok(())
    }

    async fn delete_all<'a>(
        connection: &str,
        table_name: &str,
    ) -> Result<(), Error<<IndexedDb as CrudBackend>::Error>> {
        let db_name = connection.to_string();
        let store_name = table_name.to_string();
        let rexie = open_db(&db_name, &store_name).await.map_err(bd_err)?;
        let transaction = rexie
            .transaction(&[&store_name], rexie::TransactionMode::ReadWrite)
            .map_err(bd_err)?;
        let store = transaction.store(&store_name).map_err(bd_err)?;
        store.clear().await.map_err(bd_err)?;
        transaction.done().await.map_err(bd_err)?;
        Ok(())
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Integer(i) => write!(f, "{i}"),
            Value::Float(fl) => write!(f, "{fl}"),
            Value::String(s) => write!(f, "{s}"),
            Value::Bytes(b) => write!(f, "{b:?}"),
            Value::None => write!(f, "null"),
        }
    }
}
