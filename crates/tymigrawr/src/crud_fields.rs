//! Types and operations on crud fields.
use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
};

use snafu::prelude::*;

/// Represents the database type of a CRUD field.
///
/// This enum categorizes all supported column types in the database. Each variant corresponds
/// to a database-level type that can be stored and retrieved. The `Integer` variant is the default.
#[derive(Default)]
pub enum ValueType {
    /// 64-bit signed integer (`i64`, `i32`, `u32`).
    #[default]
    Integer,
    /// 64-bit floating-point number (`f64`, `f32`).
    Float,
    /// Text/string data (`String`).
    String,
    /// Binary data (`Vec<u8>`).
    Bytes,
}

/// Metadata for a single database column.
///
/// This struct describes the schema properties of a field, including its name, type, nullability,
/// and key constraints. It is typically generated automatically by the `#[derive(HasCrudFields)]`
/// macro.
#[derive(Default)]
pub struct CrudField {
    /// The column name in the database.
    pub name: &'static str,
    /// The database type of this column.
    pub ty: ValueType,
    /// Whether this column allows `NULL` values.
    pub nullable: bool,
    /// Whether this column is the primary key.
    pub primary_key: bool,
    /// Whether this column auto-increments on insert (for integer primary keys).
    pub auto_increment: bool,
}

/// A database-agnostic representation of a column value.
///
/// `Value` is the intermediate representation used internally to convert between Rust types
/// and database storage. It can hold any supported primitive type or be `None` for nullable columns.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// A 64-bit signed integer.
    Integer(i64),
    /// A 64-bit floating-point number.
    Float(f64),
    /// A text string.
    String(String),
    /// Binary data.
    Bytes(Vec<u8>),
    /// A NULL value (for nullable columns).
    None,
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Integer(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Float(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::Bytes(value)
    }
}

impl<T> From<Option<T>> for Value
where
    Value: From<T>,
{
    fn from(value: Option<T>) -> Self {
        value.map(Value::from).unwrap_or(Value::None)
    }
}

impl Value {
    /// Extracts an `i64` if this is an `Integer` variant.
    ///
    /// Returns `None` if this value is not an integer or is `None`.
    pub fn as_i64(&self) -> Option<i64> {
        if let Value::Integer(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    /// Extracts an `f64` if this is a `Float` variant.
    ///
    /// Returns `None` if this value is not a float or is `None`.
    pub fn as_f64(&self) -> Option<f64> {
        if let Value::Float(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    /// Extracts a string reference if this is a `String` variant.
    ///
    /// Returns `None` if this value is not a string or is `None`.
    pub fn as_string(&self) -> Option<&String> {
        if let Value::String(i) = self {
            Some(i)
        } else {
            None
        }
    }

    /// Extracts a byte slice if this is a `Bytes` variant.
    ///
    /// Returns `None` if this value is not bytes or is `None`.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        if let Value::Bytes(i) = self {
            Some(i)
        } else {
            None
        }
    }
}

/// A type that can be converted to and from a database [`Value`].
///
/// This trait is the foundation of the CRUD system. It enables conversion between Rust types
/// and the intermediate [`Value`] representation used by the database backends.
///
/// The trait is implemented for primitive types (`i64`, `i32`, `u32`, `f64`, `f32`, `bool`, `String`, `Vec<u8>`)
/// and can be derived for custom types via the `#[derive(HasCrudFields)]` macro.
///
/// # Type Parameters
///
/// * `MaybeSelf` — The return type for `maybe_from_value`. For non-nullable types, this is `Option<Self>`.
///   For nullable types (like `Option<T>`), this is the inner type's `MaybeSelf`.
pub trait IsCrudField: Sized {
    /// The type returned by [`maybe_from_value`](IsCrudField::maybe_from_value).
    type MaybeSelf;

    /// Returns the schema metadata for this field type.
    fn field() -> CrudField;

    /// Converts a reference to this value into a database [`Value`].
    fn value(&self) -> Value;

    /// Attempts to reconstruct this type from a database [`Value`].
    ///
    /// Returns `None` if the value cannot be converted to this type (e.g., wrong variant,
    /// integer overflow, invalid UTF-8, etc.).
    fn maybe_from_value(value: &Value) -> Self::MaybeSelf;
}

impl IsCrudField for i64 {
    type MaybeSelf = Option<Self>;

    fn field() -> CrudField {
        CrudField {
            ty: ValueType::Integer,
            ..Default::default()
        }
    }

    fn value(&self) -> Value {
        (*self).into()
    }

    fn maybe_from_value(value: &Value) -> Option<Self> {
        value.as_i64()
    }
}

impl IsCrudField for i32 {
    type MaybeSelf = Option<Self>;

    fn field() -> CrudField {
        CrudField {
            ty: ValueType::Integer,
            ..Default::default()
        }
    }

    fn value(&self) -> Value {
        let i = i64::from(*self);
        i.into()
    }

    fn maybe_from_value(value: &Value) -> Option<Self> {
        let i = value.as_i64()?;
        let i = i32::try_from(i).ok()?;
        Some(i)
    }
}

impl IsCrudField for u32 {
    type MaybeSelf = Option<Self>;

    fn field() -> CrudField {
        CrudField {
            ty: ValueType::Integer,
            ..Default::default()
        }
    }

    fn value(&self) -> Value {
        let i = i64::from(*self);
        i.into()
    }

    fn maybe_from_value(value: &Value) -> Self::MaybeSelf {
        let i = value.as_i64()?;
        u32::try_from(i).ok()
    }
}

impl IsCrudField for bool {
    type MaybeSelf = Option<Self>;

    fn field() -> CrudField {
        CrudField {
            ty: ValueType::Integer,
            ..Default::default()
        }
    }

    fn value(&self) -> Value {
        let i: i64 = if *self { 1 } else { 0 };
        i.into()
    }

    fn maybe_from_value(value: &Value) -> Option<Self> {
        let i = value.as_i64()?;
        Some(i != 0)
    }
}

impl IsCrudField for String {
    type MaybeSelf = Option<Self>;

    fn field() -> CrudField {
        CrudField {
            ty: ValueType::String,
            ..Default::default()
        }
    }

    fn value(&self) -> Value {
        self.clone().into()
    }

    fn maybe_from_value(value: &Value) -> Option<Self> {
        value.as_string().cloned()
    }
}

impl IsCrudField for f64 {
    type MaybeSelf = Option<Self>;

    fn field() -> CrudField {
        CrudField {
            ty: ValueType::Float,
            ..Default::default()
        }
    }

    fn value(&self) -> Value {
        (*self).into()
    }

    fn maybe_from_value(value: &Value) -> Option<Self> {
        value.as_f64()
    }
}

impl IsCrudField for f32 {
    type MaybeSelf = Option<Self>;

    fn field() -> CrudField {
        CrudField {
            ty: ValueType::Float,
            ..Default::default()
        }
    }

    fn value(&self) -> Value {
        (*self as f64).into()
    }

    fn maybe_from_value(value: &Value) -> Option<Self> {
        let f = value.as_f64()?;
        Some(f as f32)
    }
}

impl IsCrudField for Vec<u8> {
    type MaybeSelf = Option<Self>;

    fn field() -> CrudField {
        CrudField {
            ty: ValueType::Bytes,
            ..Default::default()
        }
    }

    fn value(&self) -> Value {
        self.clone().into()
    }

    fn maybe_from_value(value: &Value) -> Option<Self> {
        let bytes = value.as_bytes()?;
        Some(bytes.to_vec())
    }
}

impl<T: IsCrudField> IsCrudField for Option<T> {
    type MaybeSelf = T::MaybeSelf;

    fn field() -> CrudField {
        let mut cf = T::field();
        cf.nullable = true;
        cf
    }

    fn value(&self) -> Value {
        self.as_ref().map(T::value).unwrap_or(Value::None)
    }

    fn maybe_from_value(value: &Value) -> Self::MaybeSelf {
        T::maybe_from_value(value)
    }
}

/// A non-auto-incrementing primary key with an explicit value.
///
/// Use this wrapper for primary key fields that you provide explicitly (e.g., `PrimaryKey<i64>`, `PrimaryKey<String>`).
/// The wrapped value must be unique across all rows.
///
/// This is distinct from [`AutoPrimaryKey<T>`], which auto-generates values on insert.
///
/// # Examples
///
/// ```ignore
/// #[derive(HasCrudFields)]
/// struct User {
///     #[primary_key]
///     id: PrimaryKey<i64>,
///     name: String,
/// }
///
/// let user = User {
///     id: PrimaryKey::new(42),
///     name: "Alice".to_string(),
/// };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct PrimaryKey<T> {
    /// The primary key value.
    pub inner: T,
}

impl<T> Deref for PrimaryKey<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for PrimaryKey<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[cfg(feature = "schemars")]
impl<T: schemars::JsonSchema> schemars::JsonSchema for PrimaryKey<T> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        T::schema_name()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        T::json_schema(generator)
    }
}

impl<T> PrimaryKey<T> {
    /// Creates a new primary key with the given value.
    pub fn new(value: T) -> Self {
        Self { inner: value }
    }
}

/// An auto-incrementing primary key.
///
/// Use this wrapper for primary key fields that should be auto-generated by the database on insert.
/// Call [`AutoPrimaryKey::default()`] (which creates `None`) when inserting a new row, and the database
/// will assign a unique value.
///
/// This is distinct from [`PrimaryKey<T>`], which requires you to provide the value explicitly.
///
/// # Examples
///
/// ```ignore
/// #[derive(HasCrudFields)]
/// struct User {
///     #[primary_key]
///     id: AutoPrimaryKey<i64>,
///     name: String,
/// }
///
/// // Insert with auto-generated ID
/// let user = User {
///     id: AutoPrimaryKey::default(),
///     name: "Alice".to_string(),
/// };
/// user.insert(&conn).unwrap();
///
/// // Or with explicit ID
/// let user2 = User {
///     id: AutoPrimaryKey::new(42),
///     name: "Bob".to_string(),
/// };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct AutoPrimaryKey<T> {
    /// The primary key value, or `None` if it hasn't been assigned yet.
    pub inner: Option<T>,
}

impl<T> Deref for AutoPrimaryKey<T> {
    type Target = Option<T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T: Default> Default for AutoPrimaryKey<T> {
    fn default() -> Self {
        Self { inner: None }
    }
}

impl<T> AutoPrimaryKey<T> {
    /// Creates a new auto-primary key with an explicit value.
    ///
    /// Use this when you want to provide the key value explicitly, bypassing auto-generation.
    pub fn new(value: T) -> Self {
        Self { inner: Some(value) }
    }

    /// Returns a reference to the key value, if present.
    ///
    /// Returns `None` if the key hasn't been assigned yet (typically before insert).
    pub fn key(&self) -> Option<&T> {
        self.inner.as_ref()
    }
}

#[cfg(feature = "schemars")]
impl<T: schemars::JsonSchema> schemars::JsonSchema for AutoPrimaryKey<T> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        Option::<T>::schema_name()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        Option::<T>::json_schema(generator)
    }
}

// IsCrudField implementations for PrimaryKey<T>

impl IsCrudField for PrimaryKey<i64> {
    type MaybeSelf = Option<Self>;

    fn field() -> CrudField {
        CrudField {
            ty: ValueType::Integer,
            primary_key: true,
            auto_increment: false,
            ..Default::default()
        }
    }

    fn value(&self) -> Value {
        self.inner.into()
    }

    fn maybe_from_value(value: &Value) -> Self::MaybeSelf {
        value.as_i64().map(|i| PrimaryKey { inner: i })
    }
}

impl IsCrudField for PrimaryKey<i32> {
    type MaybeSelf = Option<Self>;

    fn field() -> CrudField {
        CrudField {
            ty: ValueType::Integer,
            primary_key: true,
            auto_increment: false,
            ..Default::default()
        }
    }

    fn value(&self) -> Value {
        let i = i64::from(self.inner);
        i.into()
    }

    fn maybe_from_value(value: &Value) -> Self::MaybeSelf {
        let i = value.as_i64()?;
        let i = i32::try_from(i).ok()?;
        Some(PrimaryKey { inner: i })
    }
}

impl IsCrudField for PrimaryKey<u32> {
    type MaybeSelf = Option<Self>;

    fn field() -> CrudField {
        CrudField {
            ty: ValueType::Integer,
            primary_key: true,
            auto_increment: false,
            ..Default::default()
        }
    }

    fn value(&self) -> Value {
        let i = i64::from(self.inner);
        i.into()
    }

    fn maybe_from_value(value: &Value) -> Self::MaybeSelf {
        let i = value.as_i64()?;
        u32::try_from(i).ok().map(|u| PrimaryKey { inner: u })
    }
}

impl IsCrudField for PrimaryKey<String> {
    type MaybeSelf = Option<Self>;

    fn field() -> CrudField {
        CrudField {
            ty: ValueType::String,
            primary_key: true,
            auto_increment: false,
            ..Default::default()
        }
    }

    fn value(&self) -> Value {
        Value::String(self.inner.clone())
    }

    fn maybe_from_value(value: &Value) -> Self::MaybeSelf {
        Some(PrimaryKey {
            inner: value.as_string()?.clone(),
        })
    }
}

// IsCrudField implementations for AutoPrimaryKey<T>

impl IsCrudField for AutoPrimaryKey<i64> {
    type MaybeSelf = Option<Self>;

    fn field() -> CrudField {
        CrudField {
            ty: ValueType::Integer,
            primary_key: true,
            auto_increment: true,
            ..Default::default()
        }
    }

    fn value(&self) -> Value {
        self.inner.into()
    }

    fn maybe_from_value(value: &Value) -> Self::MaybeSelf {
        value.as_i64().map(|i| AutoPrimaryKey { inner: Some(i) })
    }
}

impl IsCrudField for AutoPrimaryKey<i32> {
    type MaybeSelf = Option<Self>;

    fn field() -> CrudField {
        CrudField {
            ty: ValueType::Integer,
            primary_key: true,
            auto_increment: true,
            ..Default::default()
        }
    }

    fn value(&self) -> Value {
        match self.inner {
            Some(v) => {
                let i = i64::from(v);
                i.into()
            }
            None => Value::None,
        }
    }

    fn maybe_from_value(value: &Value) -> Self::MaybeSelf {
        match value {
            Value::Integer(i) => {
                let i32_val = i32::try_from(*i).ok()?;
                Some(AutoPrimaryKey {
                    inner: Some(i32_val),
                })
            }
            Value::None => Some(AutoPrimaryKey { inner: None }),
            _ => None,
        }
    }
}

impl IsCrudField for AutoPrimaryKey<u32> {
    type MaybeSelf = Option<Self>;

    fn field() -> CrudField {
        CrudField {
            ty: ValueType::Integer,
            primary_key: true,
            auto_increment: true,
            ..Default::default()
        }
    }

    fn value(&self) -> Value {
        match self.inner {
            Some(v) => {
                let i = i64::from(v);
                i.into()
            }
            None => Value::None,
        }
    }

    fn maybe_from_value(value: &Value) -> Self::MaybeSelf {
        match value {
            Value::Integer(i) => {
                let u32_val = u32::try_from(*i).ok()?;
                Some(AutoPrimaryKey {
                    inner: Some(u32_val),
                })
            }
            Value::None => Some(AutoPrimaryKey { inner: None }),
            _ => None,
        }
    }
}

/// A JSON-serialized field for storing complex types as text.
///
/// Any type implementing `serde::Serialize` and `serde::Deserialize` can be wrapped in `JsonText<T>`
/// to be stored as JSON text in the database. This enables storing structured data (e.g., nested objects,
/// arrays) in a single column.
///
/// # Examples
///
/// ```ignore
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize, Debug, Clone)]
/// struct Tags {
///     names: Vec<String>,
/// }
///
/// #[derive(HasCrudFields)]
/// struct Article {
///     #[primary_key]
///     id: PrimaryKey<i64>,
///     title: String,
///     tags: JsonText<Tags>,
/// }
/// ```
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct JsonText<T> {
    /// The inner value, which will be serialized to JSON when stored.
    pub inner: T,
}

#[cfg(feature = "schemars")]
impl<T: schemars::JsonSchema> schemars::JsonSchema for JsonText<T> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        T::schema_name()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        T::json_schema(generator)
    }
}

impl<T: serde::Serialize + serde::de::DeserializeOwned> IsCrudField for JsonText<T> {
    type MaybeSelf = Option<Self>;

    fn field() -> CrudField {
        CrudField {
            ty: ValueType::String,
            ..Default::default()
        }
    }

    fn value(&self) -> Value {
        match serde_json::to_string(&self.inner) {
            Ok(json) => Value::String(json),
            Err(e) => {
                // Serialize errors should be caught during development
                panic!("failed to serialize JsonText field: {}", e)
            }
        }
    }

    fn maybe_from_value(value: &Value) -> Self::MaybeSelf {
        match value {
            Value::String(s) => match serde_json::from_str::<T>(s) {
                Ok(inner) => Some(JsonText { inner }),
                Err(e) => {
                    // Log the error but return None to indicate failure
                    // The caller can decide how to handle deserialization failure
                    eprintln!("failed to deserialize JsonText field: {}", e);
                    None
                }
            },
            Value::None => None,
            _ => None,
        }
    }
}

impl<T> JsonText<T> {
    /// Creates a new JSON-serialized field with the given value.
    pub fn new(value: T) -> Self {
        Self { inner: value }
    }
}

#[derive(Debug, Snafu)]
pub struct HasCrudFieldsError {
    pub value: Value,
    pub reason: String,
}

/// Describes the database schema and structure of a type.
///
/// This trait maps a Rust struct to a database table, providing metadata about column names,
/// types, and the primary key. It is typically derived using the `#[derive(HasCrudFields)]` macro.
///
/// # Deriving HasCrudFields
///
/// Use the `#[derive(HasCrudFields)]` macro on any struct whose fields all implement [`IsCrudField`]:
///
/// ```ignore
/// #[derive(HasCrudFields)]
/// struct User {
///     #[primary_key]
///     id: PrimaryKey<i64>,
///     name: String,
///     email: Option<String>,
/// }
/// ```
///
/// The `#[primary_key]` attribute must mark exactly one field as the primary key.
pub trait HasCrudFields: Sized {
    /// Returns the database table name for this type.
    fn table_name() -> &'static str;

    /// Returns the metadata for all columns in this type.
    fn crud_fields() -> Vec<CrudField>;

    /// Converts all fields of this value into a map of column names to database values.
    fn as_crud_fields(&self) -> HashMap<&str, Value>;

    /// Returns the name of the primary key column.
    fn primary_key_name() -> &'static str;

    /// Extracts the primary key value from this instance.
    fn primary_key_val(&self) -> Value;

    /// Attempts to reconstruct this type from a map of column values.
    ///
    /// Returns an error if the columns are missing or have incompatible types.
    fn try_from_crud_fields(fields: &HashMap<&str, Value>) -> Result<Self, HasCrudFieldsError>;
}
