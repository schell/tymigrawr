//! Sqlite impl.
use std::collections::HashMap;

use snafu::{OptionExt, ResultExt};

use crate::{
    Crud, CrudField, HasCrudFields, IsCrudField, MigrateEntireTable, Migration, Value, ValueType,
};

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

impl From<Value> for sqlite::Value {
    fn from(value: Value) -> Self {
        match value {
            Value::Integer(i) => sqlite::Value::Integer(i),
            Value::Float(i) => sqlite::Value::Float(i),
            Value::String(i) => sqlite::Value::String(i),
            Value::Bytes(i) => sqlite::Value::Binary(i),
            Value::None => sqlite::Value::Null,
        }
    }
}

impl From<sqlite::Value> for Value {
    fn from(value: sqlite::Value) -> Self {
        match value {
            sqlite::Value::Integer(i) => Value::Integer(i),
            sqlite::Value::Float(i) => Value::Float(i),
            sqlite::Value::String(i) => Value::String(i),
            sqlite::Value::Binary(i) => Value::Bytes(i),
            sqlite::Value::Null => Value::None,
        }
    }
}

impl MigrateEntireTable for Sqlite {
    type Connection<'a> = &'a sqlite::Connection;

    fn read_all_values<'a>(
        connection: &'a sqlite::Connection,
        table_name: &'a str,
        column_names: Vec<&'a str>,
    ) -> Result<Vec<Result<HashMap<&'a str, Value>, snafu::Whatever>>, snafu::Whatever> {
        let statement = format!("SELECT * FROM {table_name};");
        let query = connection
            .prepare(statement)
            .whatever_context("read all prepare")?;
        let cursor = query
            .into_iter()
            .map(
                move |row| -> Result<HashMap<&str, Value>, snafu::Whatever> {
                    let row = row.whatever_context("row")?;
                    let mut cols = HashMap::default();
                    for name in column_names.iter() {
                        let value = &row[*name];
                        cols.insert(*name, value.clone().into());
                    }
                    Ok(cols)
                },
            )
            .collect::<Vec<_>>();
        Ok(cursor)
    }

    fn insert_fields(
        connection: &sqlite::Connection,
        table_name: &str,
        fields: &HashMap<&str, Value>,
    ) -> Result<(), snafu::Whatever> {
        let columns = fields.iter().map(|f| *f.0).collect::<Vec<_>>().join(", ");
        let binds = fields
            .iter()
            .map(|f| format!(":{}", *f.0))
            .collect::<Vec<_>>()
            .join(", ");
        let statement = format!("INSERT INTO {table_name} ({columns}) VALUES ({binds});");
        let mut query = connection
            .prepare(&statement)
            .whatever_context(format!("insert prepare: {statement}"))?;
        for (key, value) in fields.iter() {
            let key = format!(":{key}");
            let k = key.as_str();
            let value = sqlite::Value::from(value.clone());
            query.bind((k, value)).whatever_context("insert bind")?;
        }
        snafu::ensure_whatever!(
            matches!(query.next(), Ok(sqlite::State::Done)),
            "insert query not ok"
        );
        Ok(())
    }

    fn delete_all(
        connection: Self::Connection<'_>,
        table_name: &str,
    ) -> Result<(), snafu::Whatever> {
        let statement = format!("DELETE FROM {table_name};");
        let mut query = connection
            .prepare(&statement)
            .whatever_context("prepare clear table")?;
        while let Ok(_) = query.next() {}
        Ok(())
    }
}

pub struct Sqlite;

impl<T: HasCrudFields + Clone + Sized + 'static> Crud<Sqlite> for T {
    type Connection<'a> = &'a sqlite::Connection;

    /// Create a table for `Self`.
    fn create(connection: &sqlite::Connection) -> Result<(), snafu::Whatever> {
        let table_name = Self::table_name();
        let fields: String = Self::crud_fields()
            .iter()
            .map(CrudField::sqlite_create_field)
            .collect::<Vec<_>>()
            .join(", ");
        let statement = format!("CREATE TABLE IF NOT EXISTS {table_name} ({fields});");
        connection
            .execute(statement)
            .whatever_context("could not create")
    }

    fn insert(&self, connection: &sqlite::Connection) -> Result<(), snafu::Whatever> {
        let table_name = Self::table_name();
        let fields = self.as_crud_fields();
        Sqlite::insert_fields(connection, table_name, &fields)?;
        Ok(())
    }

    fn read_all<'a>(
        connection: Self::Connection<'a>,
    ) -> Result<Box<dyn Iterator<Item = Result<Self, snafu::Whatever>> + 'a>, snafu::Whatever> {
        let table_name = Self::table_name();
        let column_names = Self::crud_fields()
            .iter()
            .map(|field| field.name)
            .collect::<Vec<_>>();
        let cursor = Sqlite::read_all_values(connection, table_name, column_names)?;
        Ok(Box::new(
            cursor
                .into_iter()
                .map(|cols| Self::try_from_crud_fields(&cols?)),
        ))
    }

    fn read_where<'a>(
        connection: &'a sqlite::Connection,
        key_name: &'a str,
        comparison: &'a str,
        key_value: impl IsCrudField,
    ) -> Result<Box<dyn Iterator<Item = Result<Self, snafu::Whatever>> + 'a>, snafu::Whatever> {
        let table_name = Self::table_name();
        let column_names = Self::crud_fields()
            .iter()
            .map(|field| field.name)
            .collect::<Vec<_>>();
        let statement =
            format!("SELECT * FROM {table_name} WHERE {key_name} {comparison} :key_value");
        let mut query = connection
            .prepare(statement)
            .whatever_context("create prepare")?;
        let value = key_value.into_value();
        let value = sqlite::Value::from(value);
        query
            .bind((":key_value", value))
            .whatever_context("create bind")?;
        let cursor = query
            .into_iter()
            .map(move |row| -> Result<Self, snafu::Whatever> {
                let row = row.whatever_context("row")?;
                let mut cols = HashMap::default();
                for name in column_names.iter() {
                    let value = &row[*name];
                    let value = Value::from(value.clone());
                    cols.insert(*name, value);
                }
                Self::try_from_crud_fields(&cols)
            });
        Ok(Box::new(cursor))
    }

    fn read<'a, Key: IsCrudField>(
        connection: Self::Connection<'a>,
        key: Key,
    ) -> Result<Box<dyn Iterator<Item = Result<Self, snafu::Whatever>> + 'a>, snafu::Whatever> {
        Self::read_where(connection, Self::primary_key_name(), "=", key)
    }

    fn update(&self, connection: &sqlite::Connection) -> Result<(), snafu::Whatever> {
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
        let primary_key = primary_key.whatever_context("missing primary key")?;

        let table_name = Self::table_name();
        let statement =
            format!("UPDATE {table_name} SET {values} WHERE {primary_key} = :key_value",);
        let mut query = connection
            .prepare(statement)
            .whatever_context("update prepare")?;
        let mut key_value = None;
        for (key, value) in fields.into_iter() {
            if key == primary_key {
                key_value = Some(value);
                continue;
            }
            let key = format!(":{key}");
            let k = key.as_str();
            let v = sqlite::Value::from(value);
            query.bind((k, v)).whatever_context("update bind")?;
        }
        let key_value = key_value.whatever_context("no key value")?;
        let key_value = sqlite::Value::from(key_value);
        query
            .bind((":key_value", key_value))
            .whatever_context("update bind key_value")?;

        if let Ok(sqlite::State::Done) = query.next() {
            Ok(())
        } else {
            snafu::whatever!("update next")
        }?;

        Ok(())
    }

    fn delete(self, connection: &sqlite::Connection) -> Result<(), snafu::Whatever> {
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
            .whatever_context("missing primary key")?;
        let key_value = self
            .as_crud_fields()
            .into_iter()
            .find_map(|(k, v)| if k == key_name { Some(v) } else { None })
            .whatever_context("missing primary key value")?;
        let key_value = sqlite::Value::from(key_value);
        let statement =
            format!("DELETE FROM {table_name} WHERE {key_name} = :key_value RETURNING *");
        let mut query = connection
            .prepare(statement)
            .whatever_context("delete prepare")?;
        query
            .bind((":key_value", key_value))
            .whatever_context("delete bind key_value")?;
        while let Ok(sqlite::State::Row) = query.next() {}

        Ok(())
    }

    fn migration<S: 'static>() -> Migration
    where
        Self: From<S>,
    {
        Migration {
            table_name: Box::new(Self::table_name),
            crud_fields: Box::new(Self::crud_fields),
            from_prev: Box::new(|any: Box<dyn core::any::Any>| {
                // SAFETY: we know we can downcast because of the Self: From<T> constraint
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
