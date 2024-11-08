use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
};

use snafu::prelude::*;

pub use tymigrawr_derive::HasCrudFields;

#[cfg(feature = "backend_sqlite")]
mod backend_sqlite;
#[cfg(feature = "backend_sqlite")]
pub use backend_sqlite::*;

#[cfg(feature = "backend_dynamodb")]
mod backend_dynamodb;
#[cfg(feature = "backend_dynamdb")]
pub use backend_dynamodb::*;


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
    type MaybeSelf = Result<Self, snafu::Whatever>;

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
        let i = value.as_i64().whatever_context("not an integer")?;
        u32::try_from(i).whatever_context("can't u32 from i64")
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

pub trait HasCrudFields: Sized {
    fn table_name() -> &'static str;
    fn crud_fields() -> Vec<CrudField>;
    fn as_crud_fields(&self) -> HashMap<&str, Value>;
    fn primary_key_name() -> &'static str;
    fn primary_key_val(&self) -> Value;
    fn try_from_crud_fields(fields: &HashMap<&str, Value>) -> Result<Self, snafu::Whatever>;
}

pub struct Migration {
    table_name: Box<dyn Fn() -> &'static str>,
    crud_fields: Box<dyn Fn() -> Vec<CrudField>>,
    from_prev: Box<dyn Fn(Box<dyn core::any::Any>) -> Box<dyn core::any::Any>>,
    as_crud_fields: Box<dyn Fn(&Box<dyn core::any::Any>) -> HashMap<&str, Value>>,
    try_from_crud_fields:
        Box<dyn Fn(&HashMap<&str, Value>) -> Result<Box<dyn core::any::Any>, snafu::Whatever>>,
}

pub trait Crud<Backend>: HasCrudFields + Clone + Sized + 'static {
    type Connection<'a>;

    /// Create a table for `Self`.
    fn create(connection: Self::Connection<'_>) -> Result<(), snafu::Whatever>;

    fn insert(&self, connection: Self::Connection<'_>) -> Result<(), snafu::Whatever>;

    fn read_all<'a>(
        connection: Self::Connection<'a>,
    ) -> Result<Box<dyn Iterator<Item = Result<Self, snafu::Whatever>> + 'a>, snafu::Whatever>;

    fn read_where<'a>(
        connection: Self::Connection<'a>,
        key_name: &'a str,
        comparison: &'a str,
        key_value: impl IsCrudField,
    ) -> Result<Box<dyn Iterator<Item = Result<Self, snafu::Whatever>> + 'a>, snafu::Whatever>;

    fn read<'a, Key: IsCrudField>(
        connection: Self::Connection<'a>,
        key: Key,
    ) -> Result<Box<dyn Iterator<Item = Result<Self, snafu::Whatever>> + 'a>, snafu::Whatever>;

    fn update(&self, connection: Self::Connection<'_>) -> Result<(), snafu::Whatever>;

    fn delete(self, connection: Self::Connection<'_>) -> Result<(), snafu::Whatever>;

    fn migration<T: 'static>() -> Migration
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

pub trait MigrateEntireTable {
    type Connection<'a>: Copy;

    fn read_all_values<'a>(
        connection: Self::Connection<'a>,
        table_name: &'a str,
        column_names: Vec<&'a str>,
    ) -> Result<Vec<Result<HashMap<&'a str, Value>, snafu::Whatever>>, snafu::Whatever>;

    fn insert_fields(
        connection: Self::Connection<'_>,
        table_name: &str,
        fields: &HashMap<&str, Value>,
    ) -> Result<(), snafu::Whatever>;

    fn delete_all(
        connection: Self::Connection<'_>,
        table_name: &str,
    ) -> Result<(), snafu::Whatever>;
}

pub struct Migrations<T, Backend> {
    _current: PhantomData<(T, Backend)>,
    all: VecDeque<Migration>,
}

impl<T: HasCrudFields + Clone + Sized + 'static, Backend: MigrateEntireTable>
    Migrations<T, Backend>
{
    pub fn default() -> Self {
        Self {
            _current: PhantomData,
            all: Default::default(),
        }
        .with_version::<T>()
    }
}

impl<T: HasCrudFields + Clone + Sized + 'static, Backend: MigrateEntireTable>
    Migrations<T, Backend>
{
    pub fn with_version<Next>(self) -> Migrations<Next, Backend>
    where
        Next: From<T> + HasCrudFields + Clone + Sized + 'static,
    {
        let Self {
            _current: _,
            mut all,
        } = self;
        all.push_back(Next::migration());
        Migrations {
            _current: PhantomData,
            all,
        }
    }

    pub fn run<'a>(self, connection: Backend::Connection<'a>) -> Result<(), snafu::Whatever> {
        self.run_with(|_| connection)
    }

    pub fn run_with<'a>(
        self,
        mk_connection: impl Fn(&str) -> Backend::Connection<'a>,
    ) -> Result<(), snafu::Whatever> {
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
            let column_names = fields.iter().map(|f| f.name).collect::<Vec<_>>();
            // Get a cursor of each value in the prev table
            let cursor = Backend::read_all_values(
                (mk_connection)(prev_table_name),
                prev_table_name,
                column_names,
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
    use aws_sdk_dynamodb::types::AttributeValue;
    use snafu::prelude::*;

    use crate::{self as tymigrawr, Crud, HasCrudFields, IsCrudField, Migrations, Value, Sqlite};

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

    #[test]
    fn p1_crud() {
        let connection = sqlite::open(":memory:").unwrap();
        PlayerV1::create(&connection).unwrap();
        let first_player = PlayerV1 {
            id: 0,
            name: "tymigrawr".to_string(),
        };
        first_player.insert(&connection).unwrap();
        let player = PlayerV1::read(&connection, 0)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(first_player, player);
        let mut second_player = PlayerV1 {
            id: 1,
            name: "developer".to_string(),
        };
        second_player.insert(&connection).unwrap();
        let player = PlayerV1::read(&connection, 1)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(second_player, player);

        let mut p1 = PlayerV1::read(&connection, first_player.id).unwrap();
        assert_eq!(first_player, p1.next().unwrap().unwrap());
        let mut p2 = PlayerV1::read(&connection, second_player.id).unwrap();
        assert_eq!(second_player, p2.next().unwrap().unwrap());

        second_player.name = "software engineer".to_string();
        second_player.update(&connection).unwrap();
        let p2 = PlayerV1::read(&connection, second_player.id)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(second_player, p2);

        second_player.delete(&connection).unwrap();
        let players = PlayerV1::read(&connection, p2.id)
            .unwrap()
            .map(|p| p.unwrap())
            .collect::<Vec<_>>();
        assert!(players.is_empty());
    }

    #[test]
    fn p2_crud() {
        let connection = sqlite::open(":memory:").unwrap();
        PlayerV2::create(&connection).unwrap();
        let mut first_player = PlayerV2 {
            id: 0,
            name: "tymigrawr".to_string(),
            age: 0.1,
        };
        first_player.insert(&connection).unwrap();
        let mut p1 = PlayerV2::read(&connection, first_player.id).unwrap();
        assert_eq!(first_player, p1.next().unwrap().unwrap());

        first_player.name = "software engineer".to_string();
        first_player.update(&connection).unwrap();
        let p2 = PlayerV2::read(&connection, first_player.id)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(first_player, p2);

        first_player.delete(&connection).unwrap();
        let players = PlayerV2::read(&connection, p2.id)
            .unwrap()
            .map(|p| p.unwrap())
            .collect::<Vec<_>>();
        assert!(players.is_empty());
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

    #[test]
    fn migrate() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Trace)
            .try_init();

        log::debug!("migration setup");
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("data.db");
        let connection = sqlite::open(path).unwrap();
        let path = tempdir.path().join("data_v3.db");
        let connection_v3 = sqlite::open(path).unwrap();
        log::debug!("creating tables");
        PlayerV1::create(&connection).unwrap();
        PlayerV2::create(&connection).unwrap();
        PlayerV3::create(&connection_v3).unwrap();

        log::debug!("populating v1");
        let players_v1 = (0..100)
            .map(|i| PlayerV1 {
                id: i,
                name: format!("tymigrawr_{i}"),
            })
            .collect::<Vec<_>>();
        for player in players_v1.iter() {
            player.insert(&connection).unwrap();
        }
        let players_v3 = players_v1
            .iter()
            .cloned()
            .map(PlayerV2::from)
            .map(Player::from)
            .collect::<Vec<_>>();

        log::debug!("running forward migrations");
        let migrations = Migrations::<PlayerV1, Sqlite>::default()
            .with_version::<PlayerV2>()
            .with_version::<Player>();
        migrations
            .run_with(|table| match table {
                "playerv3" => &connection_v3,
                _ => &connection,
            })
            .unwrap();

        let players_v1_from_db = PlayerV1::read_all(&connection)
            .unwrap()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(Vec::<PlayerV1>::new(), players_v1_from_db);

        let players_v3_from_db = PlayerV3::read_all(&connection_v3)
            .unwrap()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();
        assert_eq!(players_v3, players_v3_from_db);

        log::debug!("running reverse migrations");
        let migrations = Migrations::<Player, Sqlite>::default()
            .with_version::<PlayerV2>()
            .with_version::<PlayerV1>();
        migrations
            .run_with(|table| match table {
                "playerv3" => &connection_v3,
                _ => &connection,
            })
            .unwrap();
        let players_v1_from_db = PlayerV1::read_all(&connection)
            .unwrap()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(players_v1, players_v1_from_db);
    }

    #[test]
    fn dynamodb_float_int_roundtrip() {
        let int_value = Value::Integer(66);
        let int_dydb = AttributeValue::from(int_value.clone());
        assert_eq!(int_value, Value::from(int_dydb));

        let float_value = Value::Float(600.66);
        let float_dydb = AttributeValue::from(float_value.clone());
        assert_eq!(float_value, Value::from(float_dydb));

        //let float_value = Value::Float(600.0);
        //let float_dydb = AttributeValue::from(float_value.clone());
        //assert_eq!(float_value, Value::from(float_dydb));
    }
}
