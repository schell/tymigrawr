use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
};

use snafu::prelude::*;

pub mod error;
pub use error::{Error, LibraryError};
pub use tymigrawr_derive::HasCrudFields;

#[cfg(feature = "backend_sqlite")]
mod backend_sqlite;
#[cfg(feature = "backend_sqlite")]
pub use backend_sqlite::*;

#[cfg(feature = "backend_toml")]
mod backend_toml;
#[cfg(feature = "backend_toml")]
pub use backend_toml::*;

use crate::error::TymResult;

#[derive(Default)]
pub enum ValueType {
    #[default]
    Integer,
    Float,
    String,
    Bytes,
}

#[derive(Default)]
pub struct CrudField {
    pub name: &'static str,
    pub ty: ValueType,
    pub nullable: bool,
    pub primary_key: bool,
    pub auto_increment: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
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
    pub fn as_i64(&self) -> Option<i64> {
        if let Value::Integer(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        if let Value::Float(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    pub fn as_string(&self) -> Option<&String> {
        if let Value::String(i) = self {
            Some(i)
        } else {
            None
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        if let Value::Bytes(i) = self {
            Some(i)
        } else {
            None
        }
    }
}

pub trait IsCrudField: Sized {
    type MaybeSelf;

    fn field() -> CrudField;
    #[allow(clippy::wrong_self_convention)]
    fn into_value(&self) -> Value;
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

    fn into_value(&self) -> Value {
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

    fn into_value(&self) -> Value {
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

    fn into_value(&self) -> Value {
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

    fn into_value(&self) -> Value {
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

    fn into_value(&self) -> Value {
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

    fn into_value(&self) -> Value {
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

    fn into_value(&self) -> Value {
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

    fn into_value(&self) -> Value {
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

    fn into_value(&self) -> Value {
        self.as_ref().map(T::into_value).unwrap_or(Value::None)
    }

    fn maybe_from_value(value: &Value) -> Self::MaybeSelf {
        T::maybe_from_value(value)
    }
}

#[derive(Debug, Snafu)]
pub struct HasCrudFieldsError {
    pub value: Value,
    pub reason: String,
}

pub trait HasCrudFields: Sized {
    fn table_name() -> &'static str;
    fn crud_fields() -> Vec<CrudField>;
    fn as_crud_fields(&self) -> HashMap<&str, Value>;
    fn primary_key_name() -> &'static str;
    fn primary_key_val(&self) -> Value;
    fn try_from_crud_fields(fields: &HashMap<&str, Value>) -> Result<Self, HasCrudFieldsError>;
}

type FromPrevious =
    Box<dyn Fn(Box<dyn core::any::Any + 'static>) -> Box<dyn core::any::Any + 'static> + 'static>;

type AsCrudFields =
    Box<dyn for<'a> Fn(&'a Box<dyn core::any::Any + 'static>) -> HashMap<&'a str, Value> + 'static>;

type TryFromCrudFields<E> =
    Box<dyn Fn(&HashMap<&str, Value>) -> TymResult<Box<dyn core::any::Any + 'static>, E> + 'static>;

pub struct Migration<E: core::fmt::Display + core::fmt::Debug + 'static> {
    table_name: Box<dyn Fn() -> &'static str>,
    crud_fields: Box<dyn Fn() -> Vec<CrudField>>,
    from_prev: FromPrevious,
    as_crud_fields: AsCrudFields,
    try_from_crud_fields: TryFromCrudFields<E>,
}

pub trait CrudBackend {
    type Connection<'a>: Copy;
    type Error: core::fmt::Display + core::fmt::Debug + 'static;
}

pub trait Crud<Backend>
where
    Self: HasCrudFields + Clone + Sized + 'static,
    Backend: CrudBackend,
{
    /// Create a table for `Self`.
    fn create(connection: Backend::Connection<'_>) -> TymResult<(), Backend::Error>;

    fn insert(
        &self,
        connection: Backend::Connection<'_>,
    ) -> std::result::Result<(), Error<Backend::Error>>;

    /// Insert the row, or update all non-primary-key columns if a row with
    /// the same primary key already exists.  Returns `true` when at least one
    /// row was inserted or updated.
    fn upsert(
        &self,
        connection: Backend::Connection<'_>,
    ) -> std::result::Result<bool, Error<Backend::Error>>;

    fn read_all<'a>(
        connection: Backend::Connection<'a>,
    ) -> std::result::Result<
        Box<dyn Iterator<Item = TymResult<Self, Backend::Error>> + 'a>,
        Error<Backend::Error>,
    >;

    fn read_where<'a>(
        connection: Backend::Connection<'a>,
        key_name: &'a str,
        comparison: &'a str,
        key_value: impl IsCrudField,
    ) -> std::result::Result<
        Box<dyn Iterator<Item = TymResult<Self, Backend::Error>> + 'a>,
        Error<Backend::Error>,
    >;

    fn read<'a, Key: IsCrudField>(
        connection: Backend::Connection<'a>,
        key: Key,
    ) -> std::result::Result<
        Box<dyn Iterator<Item = TymResult<Self, Backend::Error>> + 'a>,
        Error<Backend::Error>,
    >;

    fn update(&self, connection: Backend::Connection<'_>) -> TymResult<(), Backend::Error>;

    fn delete(self, connection: Backend::Connection<'_>) -> TymResult<(), Backend::Error>;

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

type ReadResult<'a, E> = TymResult<HashMap<&'a str, Value>, E>;

pub trait MigrateEntireTable: CrudBackend {
    fn read_all_values<'a>(
        connection: <Self as CrudBackend>::Connection<'a>,
        table_name: &'a str,
        fields: Vec<CrudField>,
    ) -> TymResult<Vec<ReadResult<'a, Self::Error>>, Self::Error>;

    fn insert_fields(
        connection: <Self as CrudBackend>::Connection<'_>,
        table_name: &str,
        fields: &HashMap<&str, Value>,
    ) -> TymResult<(), Self::Error>;

    fn delete_all(
        connection: <Self as CrudBackend>::Connection<'_>,
        table_name: &str,
    ) -> TymResult<(), Self::Error>;
}

pub struct Migrations<T, Backend: CrudBackend> {
    _current: PhantomData<(T, Backend)>,
    all: VecDeque<Migration<Backend::Error>>,
}

impl<T: Crud<Backend> + HasCrudFields + Clone + Sized + 'static, Backend: MigrateEntireTable>
    Default for Migrations<T, Backend>
{
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

    pub fn run<'a>(
        self,
        connection: <Backend as CrudBackend>::Connection<'a>,
    ) -> TymResult<(), Backend::Error> {
        self.run_with(|_| connection)
    }

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
        while let Some(migration) = all.pop_front() {
            if all.is_empty() {
                break;
            }
            let prev_table_name = (migration.table_name)();
            log::info!("  checking {prev_table_name}");
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
                let values = res_prev?;
                // Serialize to the prev type
                let mut prev = (migration.try_from_crud_fields)(&values)?;
                let mut last_migration = &migration;
                // Move the type forward with From, from the prev to the most
                // current
                for target in all.iter() {
                    prev = (target.from_prev)(prev);
                    last_migration = target;
                }
                // Now prev is the most current type.
                let current = prev;
                current_table_name = (last_migration.table_name)();
                // Save it in the most current table, if need be.
                if current_table_name != prev_table_name {
                    let fields = (last_migration.as_crud_fields)(&current);
                    Backend::insert_fields(
                        (mk_connection)(current_table_name),
                        current_table_name,
                        &fields,
                    )?;
                }
            }
            log::info!("    migrated {entries} entries from {prev_table_name}",);
            // Remove the old entries if need be
            if current_table_name != prev_table_name {
                log::info!("    clearing out previous table {prev_table_name}");
                let conn = (mk_connection)(prev_table_name);
                Backend::delete_all(conn, prev_table_name)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use crate::{
        self as tymigrawr, Crud, CrudBackend, HasCrudFields, IsCrudField, MigrateEntireTable,
        Migrations,
    };

    #[derive(Debug, Clone, PartialEq, HasCrudFields)]
    pub struct PlayerV1 {
        #[primary_key]
        pub id: i64,
        pub name: String,
    }

    impl From<PlayerV2> for PlayerV1 {
        fn from(value: PlayerV2) -> PlayerV1 {
            PlayerV1 {
                id: value.id,
                name: value.name,
            }
        }
    }

    impl From<PlayerV1> for PlayerV2 {
        fn from(value: PlayerV1) -> PlayerV2 {
            PlayerV2 {
                id: value.id,
                name: value.name,
                age: 0.0,
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, HasCrudFields)]
    pub struct PlayerV2 {
        #[primary_key]
        pub id: i64,
        pub name: String,
        pub age: f32,
    }

    #[derive(Debug, Clone, PartialEq, HasCrudFields)]
    pub struct PlayerV3 {
        #[primary_key]
        pub id: i64,
        pub name: String,
        pub description: String,
    }

    impl From<PlayerV2> for PlayerV3 {
        fn from(value: PlayerV2) -> Self {
            let PlayerV2 { id, name, age } = value;
            PlayerV3 {
                id,
                name,
                description: format!("{age} years old"),
            }
        }
    }

    impl From<PlayerV3> for PlayerV2 {
        fn from(value: PlayerV3) -> Self {
            let PlayerV3 {
                id,
                name,
                description: _,
            } = value;
            PlayerV2 { id, name, age: 0.0 }
        }
    }

    pub type Player = PlayerV3;

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct Color {
        pub name: String,
        pub hex: u32,
    }

    #[derive(Debug, Clone, PartialEq, HasCrudFields)]
    pub struct Palette {
        #[primary_key]
        pub id: i64,
        #[json_text]
        pub colors: Vec<Color>,
        #[json_text]
        pub metadata: Option<Vec<String>>,
    }

    #[derive(Debug, Clone, PartialEq, HasCrudFields)]
    pub struct SettingsV1 {
        #[primary_key]
        pub id: i64,
        pub api_key: Option<String>,
        pub token: Option<String>,
        pub timeout_secs: i64,
    }

    #[derive(Debug, Clone, PartialEq, HasCrudFields)]
    pub struct AutoIncrementModel {
        #[primary_key(auto_increment)]
        pub id: i64,
        pub name: String,
    }

    #[derive(Debug, Clone, PartialEq, HasCrudFields)]
    pub struct AutoIncrementModelI32 {
        #[primary_key(auto_increment)]
        pub id: i32,
        pub name: String,
    }

    fn test_p1_crud<B: CrudBackend>(conn: B::Connection<'_>)
    where
        PlayerV1: Crud<B>,
    {
        <PlayerV1 as Crud<B>>::create(conn).unwrap();
        let first_player = PlayerV1 {
            id: 0,
            name: "tymigrawr".to_string(),
        };
        <PlayerV1 as Crud<B>>::insert(&first_player, conn).unwrap();
        let player = <PlayerV1 as Crud<B>>::read(conn, 0)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(first_player, player);
        let mut second_player = PlayerV1 {
            id: 1,
            name: "developer".to_string(),
        };
        <PlayerV1 as Crud<B>>::insert(&second_player, conn).unwrap();
        let player = <PlayerV1 as Crud<B>>::read(conn, 1)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(second_player, player);

        let mut p1 = <PlayerV1 as Crud<B>>::read(conn, first_player.id).unwrap();
        assert_eq!(first_player, p1.next().unwrap().unwrap());
        let mut p2 = <PlayerV1 as Crud<B>>::read(conn, second_player.id).unwrap();
        assert_eq!(second_player, p2.next().unwrap().unwrap());

        second_player.name = "software engineer".to_string();
        <PlayerV1 as Crud<B>>::update(&second_player, conn).unwrap();
        let p2 = <PlayerV1 as Crud<B>>::read(conn, second_player.id)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(second_player, p2);

        <PlayerV1 as Crud<B>>::delete(second_player, conn).unwrap();
        let players = <PlayerV1 as Crud<B>>::read(conn, p2.id)
            .unwrap()
            .map(|p| p.unwrap())
            .collect::<Vec<_>>();
        assert!(players.is_empty());
    }

    fn test_upsert<B: CrudBackend>(conn: B::Connection<'_>)
    where
        PlayerV1: Crud<B>,
    {
        <PlayerV1 as Crud<B>>::create(conn).unwrap();

        // Upsert a new row — should insert and return true
        let player = PlayerV1 {
            id: 42,
            name: "original".to_string(),
        };
        let changed = <PlayerV1 as Crud<B>>::upsert(&player, conn).unwrap();
        assert!(changed, "upsert of new row should return true");

        // Read it back
        let from_db = <PlayerV1 as Crud<B>>::read(conn, 42)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(player, from_db);

        // Upsert with same PK but different data — should update and return true
        let updated = PlayerV1 {
            id: 42,
            name: "updated".to_string(),
        };
        let changed = <PlayerV1 as Crud<B>>::upsert(&updated, conn).unwrap();
        assert!(changed, "upsert of existing row should return true");

        // Read it back and verify update took effect
        let from_db = <PlayerV1 as Crud<B>>::read(conn, 42)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(updated, from_db);

        // Verify only one row exists with that key
        let all = <PlayerV1 as Crud<B>>::read(conn, 42)
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(1, all.len(), "upsert should not duplicate rows");
    }

    fn test_auto_increment<B: CrudBackend>(conn: B::Connection<'_>)
    where
        AutoIncrementModel: Crud<B>,
    {
        <AutoIncrementModel as Crud<B>>::create(conn).unwrap();

        // Verify that the id field has auto_increment enabled
        let crud_fields = <AutoIncrementModel as HasCrudFields>::crud_fields();
        let id_field = crud_fields
            .iter()
            .find(|f| f.name == "id")
            .expect("id field should exist");
        assert!(
            id_field.primary_key,
            "id field should be marked as primary key"
        );
        assert!(
            id_field.auto_increment,
            "id field should be marked as auto_increment"
        );

        // Insert a record with auto_increment
        let record = AutoIncrementModel {
            id: 0,
            name: "test".to_string(),
        };
        <AutoIncrementModel as Crud<B>>::insert(&record, conn).unwrap();

        // Read it back to verify it was created
        let from_db = <AutoIncrementModel as Crud<B>>::read(conn, 0)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(from_db.name, "test");
    }

    fn test_auto_increment_i32<B: CrudBackend>(conn: B::Connection<'_>)
    where
        AutoIncrementModelI32: Crud<B>,
    {
        <AutoIncrementModelI32 as Crud<B>>::create(conn).unwrap();

        // Verify that the id field has auto_increment enabled (i32 variant)
        let crud_fields = <AutoIncrementModelI32 as HasCrudFields>::crud_fields();
        let id_field = crud_fields
            .iter()
            .find(|f| f.name == "id")
            .expect("id field should exist");
        assert!(
            id_field.primary_key,
            "id field should be marked as primary key"
        );
        assert!(
            id_field.auto_increment,
            "id field should be marked as auto_increment for i32 type"
        );

        // Insert a record with auto_increment
        let record = AutoIncrementModelI32 {
            id: 0,
            name: "test i32".to_string(),
        };
        <AutoIncrementModelI32 as Crud<B>>::insert(&record, conn).unwrap();

        // Read it back to verify it was created
        let from_db = <AutoIncrementModelI32 as Crud<B>>::read(conn, 0)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(from_db.name, "test i32");
    }

    fn test_json_text<B: CrudBackend>(conn: B::Connection<'_>)
    where
        Palette: Crud<B>,
    {
        <Palette as Crud<B>>::create(conn).unwrap();

        // Insert a palette with colors and Some metadata
        let palette = Palette {
            id: 1,
            colors: vec![
                Color {
                    name: "red".into(),
                    hex: 0xFF0000,
                },
                Color {
                    name: "green".into(),
                    hex: 0x00FF00,
                },
            ],
            metadata: Some(vec!["warm".into(), "nature".into()]),
        };
        <Palette as Crud<B>>::insert(&palette, conn).unwrap();

        // Read it back and verify round-trip
        let from_db = <Palette as Crud<B>>::read(conn, 1)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(palette, from_db);

        // Insert a palette with None metadata
        let palette_no_meta = Palette {
            id: 2,
            colors: vec![Color {
                name: "blue".into(),
                hex: 0x0000FF,
            }],
            metadata: None,
        };
        <Palette as Crud<B>>::insert(&palette_no_meta, conn).unwrap();

        let from_db = <Palette as Crud<B>>::read(conn, 2)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(palette_no_meta, from_db);

        // Upsert the first palette with updated colors
        let updated = Palette {
            id: 1,
            colors: vec![Color {
                name: "purple".into(),
                hex: 0x800080,
            }],
            metadata: None,
        };
        <Palette as Crud<B>>::upsert(&updated, conn).unwrap();

        let from_db = <Palette as Crud<B>>::read(conn, 1)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(updated, from_db);

        // Verify read_all returns both palettes
        let all = <Palette as Crud<B>>::read_all(conn)
            .unwrap()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(2, all.len());
    }

    fn test_p2_crud<B: CrudBackend>(conn: B::Connection<'_>)
    where
        PlayerV2: Crud<B>,
    {
        <PlayerV2 as Crud<B>>::create(conn).unwrap();
        let mut first_player = PlayerV2 {
            id: 0,
            name: "tymigrawr".to_string(),
            age: 0.1,
        };
        <PlayerV2 as Crud<B>>::insert(&first_player, conn).unwrap();
        let mut p1 = <PlayerV2 as Crud<B>>::read(conn, first_player.id).unwrap();
        assert_eq!(first_player, p1.next().unwrap().unwrap());

        first_player.name = "software engineer".to_string();
        <PlayerV2 as Crud<B>>::update(&first_player, conn).unwrap();
        let p2 = <PlayerV2 as Crud<B>>::read(conn, first_player.id)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(first_player, p2);

        <PlayerV2 as Crud<B>>::delete(first_player, conn).unwrap();
        let players = <PlayerV2 as Crud<B>>::read(conn, p2.id)
            .unwrap()
            .map(|p| p.unwrap())
            .collect::<Vec<_>>();
        assert!(players.is_empty());
    }

    fn test_migrate<'a, B: MigrateEntireTable>(
        mk_connection: impl Fn(&str) -> <B as CrudBackend>::Connection<'a>,
    ) where
        PlayerV1: Crud<B>,
        PlayerV2: Crud<B>,
        PlayerV3: Crud<B>,
    {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Trace)
            .try_init();

        log::debug!("creating tables");
        <PlayerV1 as Crud<B>>::create((mk_connection)("playerv1")).unwrap();
        <PlayerV2 as Crud<B>>::create((mk_connection)("playerv2")).unwrap();
        <PlayerV3 as Crud<B>>::create((mk_connection)("playerv3")).unwrap();

        log::debug!("populating v1");
        let players_v1 = (0..100)
            .map(|i| PlayerV1 {
                id: i,
                name: format!("tymigrawr_{i}"),
            })
            .collect::<Vec<_>>();
        for player in players_v1.iter() {
            <PlayerV1 as Crud<B>>::insert(player, (mk_connection)("playerv1")).unwrap();
        }
        let players_v3 = players_v1
            .iter()
            .cloned()
            .map(PlayerV2::from)
            .map(Player::from)
            .collect::<Vec<_>>();

        log::debug!("running forward migrations");
        let migrations = Migrations::<PlayerV1, B>::default()
            .with_version::<PlayerV2>()
            .with_version::<Player>();
        migrations.run_with(&mk_connection).unwrap();

        let players_v1_from_db = <PlayerV1 as Crud<B>>::read_all((mk_connection)("playerv1"))
            .unwrap()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(Vec::<PlayerV1>::new(), players_v1_from_db);

        let players_v3_from_db = <PlayerV3 as Crud<B>>::read_all((mk_connection)("playerv3"))
            .unwrap()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();
        assert_eq!(players_v3, players_v3_from_db);

        log::debug!("running reverse migrations");
        let migrations = Migrations::<Player, B>::default()
            .with_version::<PlayerV2>()
            .with_version::<PlayerV1>();
        migrations.run_with(&mk_connection).unwrap();

        let players_v1_from_db = <PlayerV1 as Crud<B>>::read_all((mk_connection)("playerv1"))
            .unwrap()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(players_v1, players_v1_from_db);
    }

    #[cfg(feature = "backend_sqlite")]
    mod sqlite_tests {
        use super::*;
        use crate::Sqlite;

        #[test]
        fn p1_crud() {
            let conn = sqlite::open(":memory:").unwrap();
            test_p1_crud::<Sqlite>(&conn);
        }

        #[test]
        fn p2_crud() {
            let conn = sqlite::open(":memory:").unwrap();
            test_p2_crud::<Sqlite>(&conn);
        }

        #[test]
        fn upsert() {
            let conn = sqlite::open(":memory:").unwrap();
            test_upsert::<Sqlite>(&conn);
        }

        #[test]
        fn json_text() {
            let conn = sqlite::open(":memory:").unwrap();
            test_json_text::<Sqlite>(&conn);
        }

        #[test]
        fn try_from_row_with_nullable_fields() {
            let conn = sqlite::open(":memory:").unwrap();
            <SettingsV1 as Crud<Sqlite>>::create(&conn).unwrap();

            // Insert a row with NULL Option fields
            let settings = SettingsV1 {
                id: 1,
                api_key: None,
                token: None,
                timeout_secs: 60,
            };
            <SettingsV1 as Crud<Sqlite>>::insert(&settings, &conn).unwrap();

            // Now try to read it back using try_from_row
            let mut stmt = conn
                .prepare("SELECT * FROM settingsv1 WHERE id = 1")
                .unwrap();
            assert!(matches!(stmt.next(), Ok(sqlite::State::Row)));
            let loaded = crate::try_from_row::<SettingsV1>(&stmt).unwrap();
            assert_eq!(settings, loaded);

            // Insert another row with Some values
            let settings2 = SettingsV1 {
                id: 2,
                api_key: Some("secret-key".to_string()),
                token: Some("auth-token".to_string()),
                timeout_secs: 120,
            };
            <SettingsV1 as Crud<Sqlite>>::insert(&settings2, &conn).unwrap();

            let mut stmt = conn
                .prepare("SELECT * FROM settingsv1 WHERE id = 2")
                .unwrap();
            assert!(matches!(stmt.next(), Ok(sqlite::State::Row)));
            let loaded = crate::try_from_row::<SettingsV1>(&stmt).unwrap();
            assert_eq!(settings2, loaded);

            // Insert row with mixed Some/None
            let settings3 = SettingsV1 {
                id: 3,
                api_key: Some("key-only".to_string()),
                token: None,
                timeout_secs: 90,
            };
            <SettingsV1 as Crud<Sqlite>>::insert(&settings3, &conn).unwrap();

            let mut stmt = conn
                .prepare("SELECT * FROM settingsv1 WHERE id = 3")
                .unwrap();
            assert!(matches!(stmt.next(), Ok(sqlite::State::Row)));
            let loaded = crate::try_from_row::<SettingsV1>(&stmt).unwrap();
            assert_eq!(settings3, loaded);
        }

        #[test]
        fn auto_increment() {
            let conn = sqlite::open(":memory:").unwrap();
            test_auto_increment::<Sqlite>(&conn);
        }

        #[test]
        fn auto_increment_i32() {
            let conn = sqlite::open(":memory:").unwrap();
            test_auto_increment_i32::<Sqlite>(&conn);
        }

        #[test]
        fn migrate() {
            let tempdir = tempfile::tempdir().unwrap();
            let path = tempdir.path().join("data.db");
            let connection = sqlite::open(path).unwrap();
            let path = tempdir.path().join("data_v3.db");
            let connection_v3 = sqlite::open(path).unwrap();
            test_migrate::<Sqlite>(|table| match table {
                "playerv3" => &connection_v3,
                _ => &connection,
            });
        }
    }

    #[cfg(feature = "backend_toml")]
    mod toml_tests {
        use super::*;
        use crate::Toml;

        #[test]
        fn p1_crud() {
            let tempdir = tempfile::tempdir().unwrap();
            test_p1_crud::<Toml>(tempdir.path());
        }

        #[test]
        fn p2_crud() {
            let tempdir = tempfile::tempdir().unwrap();
            test_p2_crud::<Toml>(tempdir.path());
        }

        #[test]
        fn upsert() {
            let tempdir = tempfile::tempdir().unwrap();
            test_upsert::<Toml>(tempdir.path());
        }

        #[test]
        fn json_text() {
            let tempdir = tempfile::tempdir().unwrap();
            test_json_text::<Toml>(tempdir.path());
        }

        #[test]
        fn migrate() {
            let tempdir = tempfile::tempdir().unwrap();
            test_migrate::<Toml>(|_| tempdir.path());
        }
    }
}
