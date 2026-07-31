//! IndexedDB backend for WASM targets.
//!
//! Each table is an IndexedDB object store. Each row is stored as a JSON
//! value keyed by its primary key. The sync `Crud` trait methods are bridged
//! to IndexedDB's async API via `wasm_bindgen_futures::spawn_local` + a
//! channel.
//!
//! The `Connection` type is `&str` — the database name.

use crate::{
    error::{DomainError, TymResult},
    Crud, CrudBackend, CrudField, Error, HasCrudFields, IsCrudField, MigrateEntireTable, Value,
};
use std::collections::HashMap;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
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
        Self { message: e.to_string() }
    }
}

impl From<serde_json::Error> for IndexedDbError {
    fn from(e: serde_json::Error) -> Self {
        Self { message: format!("serde error: {e}") }
    }
}

/// Convert a serde_json::Value to a JsValue for IDB storage.
fn to_js_value(value: &serde_json::Value) -> Result<JsValue, IndexedDbError> {
    serde_wasm_bindgen::to_value(value)
        .map_err(|e| IndexedDbError { message: e.to_string() })
}

/// Convert a JsValue back to a serde_json::Value.
fn from_js_value(value: &JsValue) -> Result<serde_json::Value, IndexedDbError> {
    serde_wasm_bindgen::from_value(value.clone())
        .map_err(|e| IndexedDbError { message: e.to_string() })
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

/// Block on an async future from sync context in WASM.
fn block_on_async<F, T>(fut: F) -> Result<T, IndexedDbError>
where
    F: std::future::Future<Output = Result<T, IndexedDbError>> + 'static,
    T: Send + 'static,
{
    use std::sync::mpsc;
    let (tx, rx) = mpsc::channel::<Result<T, IndexedDbError>>();
    wasm_bindgen_futures::spawn_local(async move {
        let result = fut.await;
        let _ = tx.send(result);
    });
    rx.recv().map_err(|_| IndexedDbError {
        message: "async operation panicked".to_string(),
    })?
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
        result.insert(unsafe { std::mem::transmute::<&str, &'static str>(field.name) }, value);
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

impl<T: HasCrudFields + Clone + Sized + 'static> Crud<IndexedDb> for T {
    fn create(connection: &str) -> Result<(), Error<<IndexedDb as CrudBackend>::Error>> {
        let db_name = connection.to_string();
        let store_name = Self::table_name().to_string();
        block_on_async(async move {
            let _rexie = open_db(&db_name, &store_name).await?;
            Ok(())
        })
        .map_err(|e| DomainError { inner: e })?;
        Ok(())
    }

    fn insert(
        &mut self,
        connection: &str,
    ) -> Result<(), Error<<IndexedDb as CrudBackend>::Error>> {
        let store_name = Self::table_name().to_string();
        let fields = self.as_crud_fields();
        let crud_fields = Self::crud_fields();
        let row_json = row_to_json(&fields).map_err(|e| DomainError { inner: e })?;
        let pk_json = pk_as_json(&fields, &crud_fields);
        let db_name = connection.to_string();

        block_on_async(async move {
            let rexie = open_db(&db_name, &store_name).await?;
            let transaction = rexie.transaction(&[&store_name], rexie::TransactionMode::ReadWrite)?;
            let store = transaction.store(&store_name)?;
            let row_js = to_js_value(&row_json)?;
            let pk_js = to_js_value(&pk_json)?;
            store.put(&row_js, Some(&pk_js)).await?;
            transaction.done().await?;
            Ok(())
        })
        .map_err(|e| DomainError { inner: e })?;
        Ok(())
    }

    fn upsert(
        &mut self,
        connection: &str,
    ) -> Result<bool, Error<<IndexedDb as CrudBackend>::Error>> {
        self.insert(connection)?;
        Ok(true)
    }

    fn read_all<'a>(
        connection: <IndexedDb as CrudBackend>::Connection<'a>,
    ) -> TymResult<Box<dyn Iterator<Item = TymResult<Self, IndexedDbError>> + 'a>, IndexedDbError> {
        let db_name = connection.to_string();
        let store_name = Self::table_name().to_string();
        let fields = Self::crud_fields();

        let all_rows: Vec<serde_json::Value> = block_on_async(async move {
            let rexie = open_db(&db_name, &store_name).await?;
            let transaction = rexie.transaction(&[&store_name], rexie::TransactionMode::ReadOnly)?;
            let store = transaction.store(&store_name)?;
            let all_js = store.get_all(None, None).await?;
            transaction.done().await?;
            let mut all = Vec::with_capacity(all_js.len());
            for js in all_js {
                all.push(from_js_value(&js)?);
            }
            Ok(all)
        })
        .map_err(|e| DomainError { inner: e })?;

        let rows: Vec<TymResult<Self, IndexedDbError>> = all_rows
            .into_iter()
            .map(|json| {
                let cols = json_to_row(&json, &fields)?;
                Ok(Self::try_from_crud_fields(&cols)?)
            })
            .collect();

        Ok(Box::new(rows.into_iter()))
    }

    fn read_where<'a>(
        connection: <IndexedDb as CrudBackend>::Connection<'a>,
        key_name: &'a str,
        _comparison: &'a str,
        key_value: impl IsCrudField,
    ) -> TymResult<Box<dyn Iterator<Item = TymResult<Self, IndexedDbError>> + 'a>, IndexedDbError> {
        let key_val = key_value.value();
        let target_key = match &key_val {
            Value::String(s) => s.clone(),
            Value::Integer(i) => i.to_string(),
            _ => key_val.to_string(),
        };

        let all: Vec<_> = <Self as Crud<IndexedDb>>::read_all(connection)?
            .filter_map(|r| r.ok())
            .filter(|row| {
                let fields = row.as_crud_fields();
                if let Some(v) = fields.get(key_name) {
                    match v {
                        Value::String(s) => s == &target_key,
                        Value::Integer(i) => i.to_string() == target_key,
                        _ => false,
                    }
                } else {
                    false
                }
            })
            .collect();

        Ok(Box::new(all.into_iter().map(Ok)))
    }

    fn read<'a, Key: IsCrudField>(
        connection: <IndexedDb as CrudBackend>::Connection<'a>,
        key: Key,
    ) -> TymResult<Box<dyn Iterator<Item = TymResult<Self, IndexedDbError>> + 'a>, IndexedDbError> {
        let iterator = <Self as Crud<IndexedDb>>::read_where(
            connection,
            Self::primary_key_name(),
            "=",
            key,
        )?;
        Ok(iterator)
    }

    fn update(&self, connection: &str) -> Result<(), Error<<IndexedDb as CrudBackend>::Error>> {
        let mut clone = self.clone();
        clone.insert(connection)
    }

    fn delete(self, connection: &str) -> Result<(), Error<<IndexedDb as CrudBackend>::Error>> {
        let store_name = Self::table_name().to_string();
        let fields = self.as_crud_fields();
        let crud_fields = Self::crud_fields();
        let pk_json = pk_as_json(&fields, &crud_fields);
        let db_name = connection.to_string();

        block_on_async(async move {
            let rexie = open_db(&db_name, &store_name).await?;
            let transaction = rexie.transaction(&[&store_name], rexie::TransactionMode::ReadWrite)?;
            let store = transaction.store(&store_name)?;
            let pk_js = to_js_value(&pk_json)?;
            store.delete(pk_js).await?;
            transaction.done().await?;
            Ok(())
        })
        .map_err(|e| DomainError { inner: e })?;
        Ok(())
    }
}

impl MigrateEntireTable for IndexedDb {
    fn read_all_values<'a>(
        connection: <IndexedDb as CrudBackend>::Connection<'a>,
        table_name: &'a str,
        fields: Vec<CrudField>,
    ) -> TymResult<Vec<crate::ReadAllValuesResult<'a, IndexedDbError>>, IndexedDbError> {
        let db_name = connection.to_string();
        let store_name = table_name.to_string();

        let all_json: Vec<serde_json::Value> = block_on_async(async move {
            let rexie = open_db(&db_name, &store_name).await?;
            let transaction = rexie.transaction(&[&store_name], rexie::TransactionMode::ReadOnly)?;
            let store = transaction.store(&store_name)?;
            let all_js = store.get_all(None, None).await?;
            transaction.done().await?;
            let mut all = Vec::with_capacity(all_js.len());
            for js in all_js {
                all.push(from_js_value(&js)?);
            }
            Ok(all)
        })
        .map_err(|e| DomainError { inner: e })?;

        let rows = all_json
            .into_iter()
            .map(|json| {
                let cols = json_to_row(&json, &fields)?;
                Ok(cols)
            })
            .collect::<Vec<_>>();
        Ok(rows)
    }

    fn insert_fields(
        connection: &str,
        table_name: &str,
        fields: &HashMap<&str, Value>,
    ) -> Result<(), Error<<IndexedDb as CrudBackend>::Error>> {
        let row_json = row_to_json(fields).map_err(|e| DomainError { inner: e })?;
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
        block_on_async(async move {
            let rexie = open_db(&db_name, &store_name).await?;
            let transaction = rexie.transaction(&[&store_name], rexie::TransactionMode::ReadWrite)?;
            let store = transaction.store(&store_name)?;
            let row_js = to_js_value(&row_json)?;
            let pk_js = to_js_value(&pk_json)?;
            store.put(&row_js, Some(&pk_js)).await?;
            transaction.done().await?;
            Ok(())
        })
        .map_err(|e| DomainError { inner: e })?;
        Ok(())
    }

    fn delete_all(
        connection: &str,
        table_name: &str,
    ) -> Result<(), Error<<IndexedDb as CrudBackend>::Error>> {
        let db_name = connection.to_string();
        let store_name = table_name.to_string();
        block_on_async(async move {
            let rexie = open_db(&db_name, &store_name).await?;
            let transaction = rexie.transaction(&[&store_name], rexie::TransactionMode::ReadWrite)?;
            let store = transaction.store(&store_name)?;
            store.clear().await?;
            transaction.done().await?;
            Ok(())
        })
        .map_err(|e| DomainError { inner: e })?;
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