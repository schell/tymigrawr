use snafu::prelude::*;

pub trait Crud: Sized {
    type Connection<'a>;
    type Error: std::error::Error;

    fn create<'a>(self, connection: Self::Connection<'a>) -> Result<Self, Self::Error>;
    fn read<'a>(connection: Self::Connection<'a>, id: i64) -> Result<Self, Self::Error>;
    fn update<'a>(&self, connection: Self::Connection<'a>) -> Result<(), Self::Error>;
    fn delete<'a>(self, connection: Self::Connection<'a>) -> Result<Self, Self::Error>;
}

fn create_json<T: serde::Serialize>(
    table_name: &str,
    value: T,
    connection: &sqlite::Connection,
) -> Result<T, snafu::Whatever> {
    let obj = serde_json::to_value(value)
        .whatever_context("create serialize")?
        .as_object()
        .whatever_context("not an object")?;
    let mut contains_id = false;
    let input_fields: Vec<String> = obj.keys().filter_map(|key| if key == "id" {
        contains_id = true;
        None
    } else {
        Some(format!(":{key}"))
    }).collect();
    let output_fields: Vec<String> = obj.keys().cloned().collect();

    let statement = format!(
        "INSERT INTO {table_name} VALUES ({}) RETURNING ({})",
        input_fields.join(", "),
        output_fields.clone().join(", ")
    );
    let mut query = connection
        .prepare(statement)
        .whatever_context("create prepare")?;
    for (key, value) in obj.iter() {
        let key = format!(":{key}");
        let k = key.as_str();
        match value {
            // TODO: Is this the right value for NULL?
            serde_json::Value::Null => query.bind((k, ())),
            serde_json::Value::Bool(truthy) => if truthy {
                1i64
            } else {
                0i64
            },
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {

                }
            }
            serde_json::Value::String(_) => todo!(),
            serde_json::Value::Array(_) => todo!(),
            serde_json::Value::Object(_) => todo!(),
        }.whatever_context("create bind")?;
    }
    if let Ok(sqlite::State::Row) = query.next() {
        Ok(())
    } else {
        snafu::whatever!("create next")
    }?;
    let id = query
        .read::<i64, _>("id")
        .whatever_context("create read id")?;
    Ok(Self { id, name })
}

#[cfg(test)]
mod test {
    use snafu::prelude::*;

    pub struct PlayerV1 {
        pub id: i64,
        pub name: String,
    }

    impl PlayerV1 {
        pub fn create(self, connection: &sqlite::Connection) -> Result<Self, snafu::Whatever> {
            let mut query = connection
                .prepare("INSERT INTO playerv1 VALUES (:name, :age) RETURNING id")
                .whatever_context("create prepare")?;
            let Self { id: _, name } = self;
            query
                .bind((":name", name.as_str()))
                .whatever_context("create bind name")?;
            if let Ok(sqlite::State::Row) = query.next() {
                Ok(())
            } else {
                snafu::whatever!("create next")
            }?;
            let id = query
                .read::<i64, _>("id")
                .whatever_context("create read id")?;
            Ok(Self { id, name })
        }

        pub fn read(connection: &sqlite::Connection, id: i64) -> Result<PlayerV1, snafu::Whatever> {
            let mut query = connection
                .prepare("SELECT * FROM playerv1 WHERE id = :id")
                .whatever_context("select")?;
            query.bind((":id", id)).whatever_context("bind")?;
            if let Ok(sqlite::State::Row) = query.next() {
                Ok(())
            } else {
                snafu::whatever!("query next")
            }?;
            let name = query
                .read::<String, _>("name")
                .whatever_context("query read")?;
            Ok(PlayerV1 { id, name })
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
                age: 0,
            }
        }
    }

    pub struct PlayerV2 {
        pub id: i64,
        pub name: String,
        pub age: u32,
    }

    impl PlayerV2 {
        pub fn create(self, connection: &sqlite::Connection) -> Result<Self, snafu::Whatever> {
            let mut query = connection
                .prepare("INSERT INTO playerv2 VALUES (:name, :age) RETURNING id")
                .whatever_context("create prepare")?;
            let Self { id: _, name, age } = self;
            query
                .bind((":name", name.as_str()))
                .whatever_context("create bind name")?;
            query
                .bind((":age", age as i64))
                .whatever_context("create bind age")?;
            if let Ok(sqlite::State::Row) = query.next() {
                Ok(())
            } else {
                snafu::whatever!("create next")
            }?;
            let id = query
                .read::<i64, _>("id")
                .whatever_context("create read id")?;
            Ok(Self { id, name, age })
        }

        pub fn read(connection: &sqlite::Connection, id: i64) -> Result<Self, snafu::Whatever> {
            let mut query = connection
                .prepare("SELECT * FROM playerv2 WHERE id = :id")
                .whatever_context("prepare")?;
            query.bind((":id", id)).whatever_context("bind")?;
            if let Ok(sqlite::State::Row) = query.next() {
                Ok(())
            } else {
                snafu::whatever!("query next")
            }?;
            let name = query
                .read::<String, _>("name")
                .whatever_context("read name")?;
            let age = query.read::<i64, _>("age").whatever_context("read age")? as u32;
            Ok(PlayerV2 { id, name, age })
        }

        pub fn update(&self, connection: &sqlite::Connection) -> Result<(), snafu::Whatever> {
            let mut query = connection
                .prepare("UPDATE playerv2 SET name = :name, age = :age WHERE id = :id")
                .whatever_context("prepare")?;
            let Self { id, name, age } = self;
            query.bind((":id", *id)).whatever_context("bind")?;
            query
                .bind((":name", name.as_str()))
                .whatever_context("bind")?;
            query.bind((":age", *age as i64)).whatever_context("bind")?;
            if let Ok(sqlite::State::Row) = query.next() {
                Ok(())
            } else {
                snafu::whatever!("query next")
            }?;
            let name = query
                .read::<String, _>("name")
                .whatever_context("read name")?;
            let age = query.read::<i64, _>("age").whatever_context("read age")? as u32;
            Ok(())
        }

        pub fn delete(self, connection: &sqlite::Connection) -> Result<Self, snafu::Whatever> {
            let mut query = connection
                .prepare("DELETE FROM playerv2 WHERE id = :id RETURNING id")
                .whatever_context("prepare delete")?;
            query
                .bind((":id", self.id))
                .whatever_context("delete bind id")?;
            if let Ok(sqlite::State::Row) = query.next() {
                Ok(())
            } else {
                snafu::whatever!("query next")
            }?;
            Ok(self)
        }
    }

    pub struct Player(pub PlayerV2);

    #[test]
    fn sanity() {
        let connection = sqlite::open(":memory:").unwrap();
        let create = vec![
            "CREATE TABLE IF NOT EXISTS playerv1 (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);",
            "CREATE TABLE IF NOT EXISTS playerv2 (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER);",
        ]
        .concat();
        connection.execute(create).unwrap();
    }
}
