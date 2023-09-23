use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
};

use snafu::prelude::*;

#[derive(Default)]
pub enum ValueType {
    #[default]
    Integer,
    Float,
    String,
    Binary,
}

#[derive(Default)]
pub struct CrudField {
    pub name: &'static str,
    pub ty: ValueType,
    pub nullable: bool,
    pub primary_key: bool,
    pub auto_increment: bool,
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
            ValueType::Binary => "BLOB",
        };
        let nullable = if *nullable { "" } else { "NOT NULL" };
        let prim_key = if *primary_key { "PRIMARY KEY" } else { "" };
        let inc = if *auto_increment { "AUTOINCREMENT" } else { "" };
        format!("{name} {ty} {prim_key} {inc} {nullable}")
    }
}

pub trait IsCrudField: Sized {
    type MaybeSelf;

    fn field() -> CrudField;
    fn into_value(self) -> sqlite::Value;
    fn maybe_from_value(value: &sqlite::Value) -> Self::MaybeSelf;
}

impl IsCrudField for i64 {
    type MaybeSelf = Option<Self>;

    fn field() -> CrudField {
        CrudField {
            ty: ValueType::Integer,
            ..Default::default()
        }
    }

    fn into_value(self) -> sqlite::Value {
        self.into()
    }

    fn maybe_from_value(value: &sqlite::Value) -> Option<Self> {
        if let sqlite::Value::Integer(i) = value {
            Some(*i)
        } else {
            None
        }
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

    fn into_value(self) -> sqlite::Value {
        let i = i64::from(self);
        i.into()
    }

    fn maybe_from_value(value: &sqlite::Value) -> Option<Self> {
        if let sqlite::Value::Integer(i) = value {
            let i = i32::try_from(*i).ok()?;
            Some(i)
        } else {
            None
        }
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

    fn into_value(self) -> sqlite::Value {
        let i = i64::from(self);
        i.into()
    }

    fn maybe_from_value(value: &sqlite::Value) -> Self::MaybeSelf {
        if let sqlite::Value::Integer(i) = value {
            u32::try_from(*i).whatever_context("can't u32 from i64")
        } else {
            snafu::whatever!("not an integer")
        }
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

    fn into_value(self) -> sqlite::Value {
        self.into()
    }

    fn maybe_from_value(value: &sqlite::Value) -> Option<Self> {
        if let sqlite::Value::String(s) = value {
            Some(s.clone())
        } else {
            None
        }
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

    fn into_value(self) -> sqlite::Value {
        self.into()
    }

    fn maybe_from_value(value: &sqlite::Value) -> Option<Self> {
        if let sqlite::Value::Float(f) = value {
            Some(*f)
        } else {
            None
        }
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

    fn into_value(self) -> sqlite::Value {
        (self as f64).into()
    }

    fn maybe_from_value(value: &sqlite::Value) -> Option<Self> {
        if let sqlite::Value::Float(f) = value {
            Some(*f as f32)
        } else {
            None
        }
    }
}

impl IsCrudField for Vec<u8> {
    type MaybeSelf = Option<Self>;

    fn field() -> CrudField {
        CrudField {
            ty: ValueType::Binary,
            ..Default::default()
        }
    }

    fn into_value(self) -> sqlite::Value {
        self.into()
    }

    fn maybe_from_value(value: &sqlite::Value) -> Option<Self> {
        if let sqlite::Value::Binary(f) = value {
            Some(f.clone())
        } else {
            None
        }
    }
}

impl<T: IsCrudField> IsCrudField for Option<T> {
    type MaybeSelf = T::MaybeSelf;

    fn field() -> CrudField {
        let mut cf = T::field();
        cf.nullable = true;
        cf
    }

    fn into_value(self) -> sqlite::Value {
        if let Some(v) = self {
            v.into_value()
        } else {
            sqlite::Value::Null
        }
    }

    fn maybe_from_value(value: &sqlite::Value) -> Self::MaybeSelf {
        T::maybe_from_value(value)
    }
}

fn read_all_values<'a>(
    connection: &'a sqlite::Connection,
    table_name: &'a str,
    column_names: Vec<&'a str>,
) -> Result<
    impl Iterator<Item = Result<HashMap<&'a str, sqlite::Value>, snafu::Whatever>> + 'a,
    snafu::Whatever,
> {
    let statement = format!("SELECT * FROM {table_name};");
    let query = connection
        .prepare(statement)
        .whatever_context("read all prepare")?;
    let cursor = query.into_iter().map(
        move |row| -> Result<HashMap<&str, sqlite::Value>, snafu::Whatever> {
            let row = row.whatever_context("row")?;
            let mut cols = HashMap::default();
            for name in column_names.iter() {
                let value = &row[*name];
                cols.insert(*name, value.clone());
            }
            Ok(cols)
        },
    );
    Ok(cursor)
}

fn insert_fields(
    connection: &sqlite::Connection,
    table_name: &str,
    fields: &HashMap<&str, sqlite::Value>,
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
        query.bind((k, value)).whatever_context("insert bind")?;
    }
    snafu::ensure_whatever!(
        matches!(query.next(), Ok(sqlite::State::Done)),
        "insert query not ok"
    );
    Ok(())
}

pub trait HasCrudFields: Sized {
    fn table_name() -> &'static str;
    fn crud_fields() -> Vec<CrudField>;
    fn as_crud_fields(&self) -> HashMap<&str, sqlite::Value>;
    fn try_from_crud_fields(fields: &HashMap<&str, sqlite::Value>)
        -> Result<Self, snafu::Whatever>;
}

pub struct Migration {
    table_name: Box<dyn Fn() -> &'static str>,
    crud_fields: Box<dyn Fn() -> Vec<CrudField>>,
    from_prev: Box<dyn Fn(Box<dyn core::any::Any>) -> Box<dyn core::any::Any>>,
    as_crud_fields: Box<dyn Fn(&Box<dyn core::any::Any>) -> HashMap<&str, sqlite::Value>>,
    try_from_crud_fields: Box<
        dyn Fn(&HashMap<&str, sqlite::Value>) -> Result<Box<dyn core::any::Any>, snafu::Whatever>,
    >,
}

pub trait Crud: HasCrudFields + Clone + Sized + 'static {
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
        insert_fields(connection, table_name, &fields)?;
        Ok(())
    }

    fn read_all<'a>(
        connection: &'a sqlite::Connection,
    ) -> Result<Box<dyn Iterator<Item = Result<Self, snafu::Whatever>> + 'a>, snafu::Whatever> {
        let table_name = Self::table_name();
        let column_names = Self::crud_fields()
            .iter()
            .map(|field| field.name)
            .collect::<Vec<_>>();
        let cursor = read_all_values(connection, table_name, column_names)?;
        Ok(Box::new(
            cursor.map(|cols| Self::try_from_crud_fields(&cols?)),
        ))
    }

    fn read<'a>(
        connection: &'a sqlite::Connection,
        key_name: &'a str,
        key_value: impl IsCrudField,
    ) -> Result<Box<dyn Iterator<Item = Result<Self, snafu::Whatever>> + 'a>, snafu::Whatever> {
        let table_name = Self::table_name();
        let column_names = Self::crud_fields()
            .iter()
            .map(|field| field.name)
            .collect::<Vec<_>>();
        let statement = format!("SELECT * FROM {table_name} WHERE {key_name} = :key_value");
        let mut query = connection
            .prepare(statement)
            .whatever_context("create prepare")?;
        query
            .bind((":key_value", key_value.into_value()))
            .whatever_context("create bind")?;
        let cursor = query
            .into_iter()
            .map(move |row| -> Result<Self, snafu::Whatever> {
                let row = row.whatever_context("row")?;
                let mut cols = HashMap::default();
                for name in column_names.iter() {
                    let value = &row[*name];
                    cols.insert(*name, value.clone());
                }
                Self::try_from_crud_fields(&cols)
            });
        Ok(Box::new(cursor))
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
            query.bind((k, value)).whatever_context("update bind")?;
        }
        let key_value = key_value.whatever_context("no key value")?;
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

impl<T: HasCrudFields + Clone + Sized + 'static> Crud for T {}

pub struct Migrations<T> {
    _current: PhantomData<T>,
    all: VecDeque<Migration>,
}

impl<T: HasCrudFields + Clone + Sized + 'static> Default for Migrations<T> {
    fn default() -> Self {
        Self {
            _current: PhantomData,
            all: Default::default(),
        }
        .with_version::<T>()
    }
}

impl<T: HasCrudFields + Clone + Sized + 'static> Migrations<T> {
    pub fn with_version<Next>(self) -> Migrations<Next>
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

    pub fn run(self, connection: &sqlite::Connection) -> Result<(), snafu::Whatever> {
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
            let cursor = read_all_values(connection, prev_table_name, column_names)?;
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
                    insert_fields(connection, current_table_name, &fields)?;
                }
            }
            log::info!(
                "    migrated {entries} entries from {prev_table_name}",
            );
            // Remove the old entries if need be
            if current_table_name != prev_table_name {
                log::info!("    clearing out previous table {prev_table_name}");
                let statement = format!("DELETE FROM {prev_table_name};");
                let mut query = connection
                    .prepare(&statement)
                    .whatever_context("prepare clear table")?;
                while let Ok(_) = query.next() {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use snafu::prelude::*;

    use crate::{Crud, CrudField, HasCrudFields, IsCrudField, Migrations};

    #[derive(Debug, Clone, PartialEq)]
    pub struct PlayerV1 {
        pub id: i64,
        pub name: String,
    }

    impl HasCrudFields for PlayerV1 {
        fn table_name() -> &'static str {
            "player_v1"
        }

        fn crud_fields() -> Vec<CrudField> {
            let mut id = i64::field();
            id.name = "id";
            id.primary_key = true;
            id.auto_increment = true;
            let mut name = String::field();
            name.name = "name";
            vec![id, name]
        }

        fn as_crud_fields(&self) -> std::collections::HashMap<&str, sqlite::Value> {
            std::collections::HashMap::from_iter([
                ("id", self.id.into_value()),
                ("name", self.name.clone().into_value()),
            ])
        }

        fn try_from_crud_fields(
            fields: &std::collections::HashMap<&str, sqlite::Value>,
        ) -> Result<Self, snafu::Whatever> {
            let id_value = fields.get("id").whatever_context("missing id")?;
            let id = i64::maybe_from_value(id_value).whatever_context("id")?;
            let name_value = fields.get("name").whatever_context("missing name")?;
            let name = String::maybe_from_value(name_value).whatever_context("name")?;
            Ok(Self { id, name })
        }
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

    #[derive(Debug, Clone, PartialEq)]
    pub struct PlayerV2 {
        pub id: i64,
        pub name: String,
        pub age: f32,
    }

    impl HasCrudFields for PlayerV2 {
        fn table_name() -> &'static str {
            "player_v2"
        }

        fn crud_fields() -> Vec<CrudField> {
            let mut id = i64::field();
            id.name = "id";
            id.primary_key = true;
            id.auto_increment = true;
            let mut name = String::field();
            name.name = "name";
            let mut age = f32::field();
            age.name = "age";
            vec![id, name, age]
        }

        fn as_crud_fields(&self) -> std::collections::HashMap<&str, sqlite::Value> {
            std::collections::HashMap::from_iter([
                ("id", self.id.into_value()),
                ("name", self.name.clone().into_value()),
                ("age", self.age.into_value()),
            ])
        }

        fn try_from_crud_fields(
            fields: &std::collections::HashMap<&str, sqlite::Value>,
        ) -> Result<Self, snafu::Whatever> {
            let id_value = fields.get("id").whatever_context("missing id")?;
            let id = i64::maybe_from_value(id_value).whatever_context("id")?;
            let name_value = fields.get("name").whatever_context("missing name")?;
            let name = String::maybe_from_value(name_value).whatever_context("name")?;
            let age_value = fields.get("age").whatever_context("missing age")?;
            let age = f32::maybe_from_value(age_value).whatever_context("age")?;
            Ok(Self { id, name, age })
        }
    }

    #[test]
    fn p1_sanity() {
        let connection = sqlite::open(":memory:").unwrap();
        PlayerV1::create(&connection).unwrap();
        let first_player = PlayerV1 {
            id: 0,
            name: "tymigrawr".to_string(),
        };
        first_player.insert(&connection).unwrap();
        let player = PlayerV1::read(&connection, "id", 0)
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
        let player = PlayerV1::read(&connection, "id", 1)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(second_player, player);

        let mut p1 = PlayerV1::read(&connection, "id", first_player.id).unwrap();
        assert_eq!(first_player, p1.next().unwrap().unwrap());
        let mut p2 = PlayerV1::read(&connection, "id", second_player.id).unwrap();
        assert_eq!(second_player, p2.next().unwrap().unwrap());

        second_player.name = "software engineer".to_string();
        second_player.update(&connection).unwrap();
        let p2 = PlayerV1::read(&connection, "id", second_player.id)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(second_player, p2);

        second_player.delete(&connection).unwrap();
        let players = PlayerV1::read(&connection, "id", p2.id)
            .unwrap()
            .map(|p| p.unwrap())
            .collect::<Vec<_>>();
        assert!(players.is_empty());
    }

    #[test]
    fn p2_sanity() {
        let connection = sqlite::open(":memory:").unwrap();
        PlayerV2::create(&connection).unwrap();
        let mut first_player = PlayerV2 {
            id: 0,
            name: "tymigrawr".to_string(),
            age: 0.1,
        };
        first_player.insert(&connection).unwrap();
        let mut p1 = PlayerV2::read(&connection, "id", first_player.id).unwrap();
        assert_eq!(first_player, p1.next().unwrap().unwrap());

        first_player.name = "software engineer".to_string();
        first_player.update(&connection).unwrap();
        let p2 = PlayerV2::read(&connection, "id", first_player.id)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(first_player, p2);

        first_player.delete(&connection).unwrap();
        let players = PlayerV2::read(&connection, "id", p2.id)
            .unwrap()
            .map(|p| p.unwrap())
            .collect::<Vec<_>>();
        assert!(players.is_empty());
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct PlayerV3 {
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

    impl HasCrudFields for PlayerV3 {
        fn table_name() -> &'static str {
            "player_v3"
        }

        fn crud_fields() -> Vec<CrudField> {
            let mut id = i64::field();
            id.name = "id";
            id.primary_key = true;
            id.auto_increment = true;
            let mut name = String::field();
            name.name = "name";
            let mut description = String::field();
            description.name = "description";
            vec![id, name, description]
        }

        fn as_crud_fields(&self) -> std::collections::HashMap<&str, sqlite::Value> {
            std::collections::HashMap::from_iter([
                ("id", self.id.into_value()),
                ("name", self.name.clone().into_value()),
                ("description", self.description.clone().into_value()),
            ])
        }

        fn try_from_crud_fields(
            fields: &std::collections::HashMap<&str, sqlite::Value>,
        ) -> Result<Self, snafu::Whatever> {
            let id_value = fields.get("id").whatever_context("missing id")?;
            let id = i64::maybe_from_value(id_value).whatever_context("id")?;
            let name_value = fields.get("name").whatever_context("missing name")?;
            let name = String::maybe_from_value(name_value).whatever_context("name")?;
            let description_value = fields
                .get("description")
                .whatever_context("missing description")?;
            let description =
                String::maybe_from_value(description_value).whatever_context("description")?;
            Ok(Self {
                id,
                name,
                description,
            })
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
        PlayerV1::create(&connection).unwrap();
        PlayerV2::create(&connection).unwrap();
        PlayerV3::create(&connection).unwrap();

        let players_v1 = (0..10_000)
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
        let migrations = Migrations::<PlayerV1>::default()
            .with_version::<PlayerV2>()
            .with_version::<Player>();
        migrations.run(&connection).unwrap();

        let players_v1_from_db = PlayerV1::read_all(&connection)
            .unwrap()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(Vec::<PlayerV1>::new(), players_v1_from_db);

        let players_v3_from_db = PlayerV3::read_all(&connection)
            .unwrap()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();
        assert_eq!(players_v3, players_v3_from_db);

        log::debug!("running reverse migrations");
        let migrations = Migrations::<Player>::default()
            .with_version::<PlayerV2>()
            .with_version::<PlayerV1>();
        migrations.run(&connection).unwrap();
        let players_v1_from_db = PlayerV1::read_all(&connection)
            .unwrap()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(players_v1, players_v1_from_db);
    }
}
