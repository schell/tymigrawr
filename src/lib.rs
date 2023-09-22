use std::collections::HashMap;

use snafu::prelude::*;
use sqlite::ReadableWithIndex;

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
    fn maybe_from_value(value: sqlite::Value) -> Self::MaybeSelf;
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

    fn maybe_from_value(value: sqlite::Value) -> Option<Self> {
        if let sqlite::Value::Integer(i) = value {
            Some(i)
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

    fn maybe_from_value(value: sqlite::Value) -> Option<Self> {
        if let sqlite::Value::Integer(i) = value {
            let i = i32::try_from(i).ok()?;
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

    fn maybe_from_value(value: sqlite::Value) -> Self::MaybeSelf {
        if let sqlite::Value::Integer(i) = value {
            u32::try_from(i).whatever_context("can't u32 from i64")
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

    fn maybe_from_value(value: sqlite::Value) -> Option<Self> {
        if let sqlite::Value::String(s) = value {
            Some(s)
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

    fn maybe_from_value(value: sqlite::Value) -> Option<Self> {
        if let sqlite::Value::Float(f) = value {
            Some(f)
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

    fn maybe_from_value(value: sqlite::Value) -> Option<Self> {
        if let sqlite::Value::Float(f) = value {
            Some(f as f32)
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

    fn maybe_from_value(value: sqlite::Value) -> Option<Self> {
        if let sqlite::Value::Binary(f) = value {
            Some(f)
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

    fn maybe_from_value(value: sqlite::Value) -> Self::MaybeSelf {
        T::maybe_from_value(value)
    }
}

pub trait HasCrudFields: Sized {
    fn table_name() -> &'static str;
    fn crud_fields() -> Vec<CrudField>;
    fn as_crud_fields(&self) -> HashMap<&str, sqlite::Value>;
    fn try_from_crud_fields(fields: HashMap<&str, sqlite::Value>) -> Result<Self, snafu::Whatever>;
}

pub fn sqlite_insert<T: HasCrudFields>(
    value: T,
    connection: &sqlite::Connection,
) -> Result<T, snafu::Whatever> {
    let fields = value.as_crud_fields();
    let mut primary_auto_inc_key: Option<&str> = None;
    let (columns, binds): (Vec<_>, Vec<_>) = T::crud_fields()
        .iter()
        .filter_map(|field| {
            if field.primary_key && field.auto_increment {
                primary_auto_inc_key = Some(field.name);
                None
            } else {
                Some((field.name, format!(":{}", field.name)))
            }
        })
        .unzip();
    let all_columns = fields
        .iter()
        .map(|(key, _)| *key)
        .collect::<Vec<_>>()
        .join(", ");
    let columns = columns.join(", ");
    let binds = binds.join(", ");
    let table_name = T::table_name();
    let statement =
        format!("INSERT INTO {table_name} ({columns}) VALUES ({binds}) RETURNING {all_columns}");
    let mut query = connection
        .prepare(&statement)
        .whatever_context(format!("insert prepare: {statement}"))?;
    for (key, value) in fields.iter() {
        if Some(key) == primary_auto_inc_key.as_ref() {
            continue;
        }
        let key = format!(":{key}");
        let k = key.as_str();
        query.bind((k, value)).whatever_context("insert bind")?;
    }
    if let Ok(sqlite::State::Row) = query.next() {
        Ok(())
    } else {
        snafu::whatever!("insert next")
    }?;
    let mut output = vec![];
    let column_names = query.column_names();
    for (i, column) in column_names.iter().enumerate() {
        println!("column: {i} {column}");
        let value = sqlite::Value::read(&query, i).whatever_context("read")?;
        output.push((column.as_str(), value));
    }
    T::try_from_crud_fields(HashMap::from_iter(output))
}

pub fn sqlite_read<T: HasCrudFields>(
    key_name: &str,
    key_value: impl IsCrudField,
    connection: &sqlite::Connection,
) -> Result<Vec<T>, snafu::Whatever> {
    let output_fields = T::crud_fields();
    let table_name = T::table_name();
    let statement = format!("SELECT * FROM {table_name} WHERE {key_name} = :key_value");
    let mut query = connection
        .prepare(statement)
        .whatever_context("create prepare")?;
    query
        .bind((":key_value", key_value.into_value()))
        .whatever_context("create bind")?;
    let mut ts: Vec<T> = vec![];
    while let Ok(sqlite::State::Row) = query.next() {
        let mut output = vec![];
        for (i, key) in query.column_names().iter().enumerate() {
            let value =
                sqlite::Value::read(&query, i).whatever_context("read")?;
            output.push((key.as_str(), value));
        }
        ts.push(T::try_from_crud_fields(HashMap::from_iter(output))?);
    }
    Ok(ts)
}

pub fn sqlite_update<T: HasCrudFields>(
    value: &T,
    connection: &sqlite::Connection,
) -> Result<(), snafu::Whatever> {
    let fields = value.as_crud_fields();
    let mut primary_key: Option<&str> = None;
    let values = T::crud_fields()
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

    let table_name = T::table_name();
    let statement = format!(
        "UPDATE {table_name} SET {values} WHERE {primary_key} = :key_value",
    );
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

pub fn sqlite_delete<V: Into<sqlite::Value>>(
    table_name: &str,
    key_name: &str,
    key_value: V,
    connection: &sqlite::Connection,
) -> Result<usize, snafu::Whatever> {
    let statement = format!("DELETE FROM {table_name} WHERE {key_name} = :key_value RETURNING *");
    let mut query = connection
        .prepare(statement)
        .whatever_context("delete prepare")?;
    query
        .bind((":key_value", key_value.into()))
        .whatever_context("delete bind key_value")?;
    let mut deleted = 0;
    while let Ok(sqlite::State::Row) = query.next() {
        deleted += 1;
    }
    Ok(deleted)
}

pub trait Crud: HasCrudFields + Sized {
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

    fn insert(self, connection: &sqlite::Connection) -> Result<Self, snafu::Whatever> {
        sqlite_insert(self, connection)
    }

    fn read(
        connection: &sqlite::Connection,
        key_name: &str,
        key_value: impl IsCrudField,
    ) -> Result<Vec<Self>, snafu::Whatever> {
        sqlite_read::<Self>(key_name, key_value, connection)
    }

    fn update(&self, connection: &sqlite::Connection) -> Result<(), snafu::Whatever> {
        sqlite_update(self, connection)
    }

    fn delete(self, connection: &sqlite::Connection) -> Result<(), snafu::Whatever> {
        let table_name = Self::table_name();
        let key_name = Self::crud_fields().into_iter().find_map(|field| if field.primary_key {
            Some(field.name)
        } else {
            None
        }).whatever_context("missing primary key")?;
        let key_value = self.as_crud_fields().into_iter().find_map(|(k, v)| if k == key_name {
            Some(v)
        } else {
            None
        }).whatever_context("missing primary key value")?;
        sqlite_delete(table_name, key_name, key_value, connection)?;
        Ok(())
    }
}

impl<T: HasCrudFields> Crud for T {}

#[cfg(test)]
mod test {
    use snafu::prelude::*;

    use crate::{Crud, CrudField, HasCrudFields, IsCrudField};

    #[derive(Debug, PartialEq)]
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
            mut fields: std::collections::HashMap<&str, sqlite::Value>,
        ) -> Result<Self, snafu::Whatever> {
            let id_value = fields.remove("id").whatever_context("missing id")?;
            let id = i64::maybe_from_value(id_value).whatever_context("id")?;
            let name_value = fields.remove("name").whatever_context("missing name")?;
            let name = String::try_from(name_value).whatever_context("name")?;
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
            mut fields: std::collections::HashMap<&str, sqlite::Value>,
        ) -> Result<Self, snafu::Whatever> {
            let id_value = fields.remove("id").whatever_context("missing id")?;
            let id = i64::maybe_from_value(id_value).whatever_context("id")?;
            let name_value = fields.remove("name").whatever_context("missing name")?;
            let name = String::maybe_from_value(name_value).whatever_context("name")?;
            let age_value = fields.remove("age").whatever_context("missing age")?;
            let age = f32::maybe_from_value(age_value).whatever_context("age")?;
            Ok(Self { id, name, age })
        }
    }

    pub struct Player(pub PlayerV2);

    #[test]
    fn p1_sanity() {
        let connection = sqlite::open(":memory:").unwrap();
        PlayerV1::create(&connection).unwrap();
        let first_player = PlayerV1 {
            id: 0,
            name: "tymigrawr".to_string(),
        }
        .insert(&connection)
        .unwrap();
        assert_eq!(
            PlayerV1 {
                id: 1,
                name: "tymigrawr".to_string()
            },
            first_player
        );
        let mut second_player = PlayerV1 {
            id: 0,
            name: "developer".to_string(),
        }
        .insert(&connection)
        .unwrap();
        assert_eq!(
            PlayerV1 {
                id: 2,
                name: "developer".to_string()
            },
            second_player
        );

        let mut p1 = PlayerV1::read(&connection, "id", first_player.id).unwrap();
        assert_eq!(first_player, p1.pop().unwrap());
        let mut p2 = PlayerV1::read(&connection, "id", second_player.id).unwrap();
        assert_eq!(second_player, p2.pop().unwrap());

        second_player.name = "software engineer".to_string();
        second_player.update(&connection).unwrap();
        let p2 = PlayerV1::read(&connection, "id", second_player.id).unwrap().pop().unwrap();
        assert_eq!(second_player, p2);

        second_player.delete(&connection).unwrap();
        let players = PlayerV1::read(&connection, "id", p2.id).unwrap();
        assert!(players.is_empty());
    }
}
