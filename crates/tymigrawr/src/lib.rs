//! A type-safe, versioned data persistence library.
//!
//! `tymigrawr` enables you to define database schemas as versioned types (`PlayerV1`, `PlayerV2`, etc.)
//! with automatic bidirectional migrations between versions using `From` trait implementations.
//!
//! ## Core Concepts
//!
//! - **Versioned Types**: Each schema version is a separate struct (e.g., `PlayerV1`, `PlayerV2`)
//!   with automatic CRUD operations via the [`Crud`] trait.
//! - **Trait-Based Schema**: The [`HasCrudFields`] trait describes table structure, primary keys,
//!   and field metadata.
//! - **Type-Erased Migrations**: The [`Migrations`] builder chains versions and executes migrations
//!   by type-erasing previous types and automatically converting via `From` implementations.
//! - **Backend Abstraction**: Multiple storage backends (SQLite, TOML) via the [`CrudBackend`] trait.
//!
//! ## Usage Example
//!
//! ```rust,ignore
//! use tymigrawr::{HasCrudFields, PrimaryKey, Crud, Migrations, Sqlite};
//! use sqlx::SqlitePool;
//!
//! // Define version 1
//! #[derive(Debug, Clone, HasCrudFields)]
//! struct PlayerV1 {
//!     id: PrimaryKey<i64>,
//!     name: String,
//! }
//!
//! // Define version 2 with additional field
//! #[derive(Debug, Clone, HasCrudFields)]
//! struct PlayerV2 {
//!     id: PrimaryKey<i64>,
//!     name: String,
//!     age: f32,
//! }
//!
//! // Implement migration from V1 to V2
//! impl From<PlayerV1> for PlayerV2 {
//!     fn from(v1: PlayerV1) -> Self {
//!         PlayerV2 {
//!             id: v1.id,
//!             name: v1.name,
//!             age: 0.0,
//!         }
//!     }
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! // Create the backend connection (a sqlx pool)
//! let pool = SqlitePool::connect("sqlite::memory:").await?;
//!
//! // Use version 1
//! <PlayerV1 as Crud<Sqlite>>::create(&pool).await?;
//! let mut player = PlayerV1 {
//!     id: PrimaryKey::new(1),
//!     name: "Alice".to_string(),
//! };
//! <PlayerV1 as Crud<Sqlite>>::insert(&mut player, &pool).await?;
//!
//! // Use version 2
//! <PlayerV2 as Crud<Sqlite>>::create(&pool).await?;
//!
//! // Run migrations
//! let migrations = Migrations::<PlayerV1, Sqlite>::default()
//!     .with_version::<PlayerV2>();
//! migrations.run(&pool).await?;
//! # Ok(())
//! # }
//! ```

mod crud;
mod crud_fields;
pub mod error;
mod migrations;

pub use crud::*;
pub use crud_fields::*;
pub use error::Error;
pub use migrations::*;
pub use tymigrawr_derive::HasCrudFields;

#[cfg(all(feature = "backend_sqlite", feature = "backend_doltlite"))]
compile_error!(
    "the `backend_sqlite` and `backend_doltlite` features are mutually exclusive: \
     they link two incompatible SQLite-compatible C libraries (sqlite3-sys and \
     libdoltlite-sys) into the same binary, causing native-symbol collisions that \
     hang the migration tests; enable at most one at a time"
);

#[cfg(feature = "backend_sqlite")]
mod backend_sqlite;
#[cfg(feature = "backend_sqlite")]
pub use backend_sqlite::*;
#[cfg(feature = "backend_doltlite")]
mod backend_doltlite;
#[cfg(feature = "backend_doltlite")]
pub use backend_doltlite::*;
#[cfg(all(feature = "backend_indexeddb", target_arch = "wasm32"))]
mod backend_indexeddb;
#[cfg(all(feature = "backend_indexeddb", target_arch = "wasm32"))]
pub use backend_indexeddb::*;

#[cfg(feature = "backend_toml")]
mod backend_toml;
#[cfg(feature = "backend_toml")]
pub use backend_toml::*;

#[cfg(test)]
mod test {
    use crate::{
        self as tymigrawr, AutoPrimaryKey, Crud, CrudBackend, HasCrudFields, JsonText,
        MigrateEntireTable, Migrations, PrimaryKey,
    };
    use futures::StreamExt;

    #[derive(Debug, Clone, PartialEq, HasCrudFields)]
    pub struct PlayerV1 {
        pub id: PrimaryKey<i64>,
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
        pub id: PrimaryKey<i64>,
        pub name: String,
        pub age: f32,
    }

    #[derive(Debug, Clone, PartialEq, HasCrudFields)]
    pub struct PlayerV3 {
        pub id: PrimaryKey<i64>,
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

    #[derive(Debug, Clone, PartialEq, HasCrudFields)]
    pub struct PlayerV4 {
        pub id: PrimaryKey<i64>,
        pub name: String,
        pub description: String,
        pub region: String,
    }

    impl From<PlayerV3> for PlayerV4 {
        fn from(value: PlayerV3) -> Self {
            PlayerV4 {
                id: value.id,
                name: value.name,
                description: value.description,
                region: "default".to_string(),
            }
        }
    }

    impl From<PlayerV4> for PlayerV3 {
        fn from(value: PlayerV4) -> Self {
            PlayerV3 {
                id: value.id,
                name: value.name,
                description: value.description,
            }
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
        pub id: PrimaryKey<i64>,
        pub colors: JsonText<Vec<Color>>,
        pub metadata: Option<JsonText<Vec<String>>>,
    }

    #[cfg(feature = "backend_sqlite")]
    #[derive(Debug, Clone, PartialEq, HasCrudFields)]
    pub struct SettingsV1 {
        pub id: PrimaryKey<i64>,
        pub api_key: Option<String>,
        pub token: Option<String>,
        pub timeout_secs: i64,
    }

    #[derive(Debug, Clone, PartialEq, HasCrudFields)]
    pub struct AutoIncrementModel {
        pub id: AutoPrimaryKey<i64>,
        pub name: String,
    }

    #[derive(Debug, Clone, PartialEq, HasCrudFields)]
    pub struct AutoIncrementModelI32 {
        pub id: AutoPrimaryKey<i32>,
        pub name: String,
    }

    #[derive(Debug, Clone, PartialEq, HasCrudFields)]
    pub struct AutoIncrementModelU32 {
        pub id: AutoPrimaryKey<u32>,
        pub name: String,
    }

    /// Pull exactly one item from a stream, unwrapping the row `Result`.
    ///
    /// The caller is responsible for unwrapping the `read_*().await` result
    /// (the outer `Result<Stream, _>`); this macro takes the stream itself and
    /// extracts the first item, panicking on early end-of-stream or a row error.
    macro_rules! one {
        ($stream:expr) => {{
            let mut s = $stream;
            s.next()
                .await
                .expect("stream ended unexpectedly")
                .expect("row read failed")
        }};
    }

    /// Collect all items from a stream into a `Vec`, unwrapping each row `Result`.
    ///
    /// The caller is responsible for unwrapping the `read_*().await` result
    /// (the outer `Result<Stream, _>`); this macro takes the stream itself and
    /// drains it, panicking on any row error.
    macro_rules! all {
        ($stream:expr) => {{
            let mut s = $stream;
            let mut out = Vec::new();
            while let Some(item) = s.next().await {
                out.push(item.expect("row read failed"));
            }
            out
        }};
    }

    async fn test_p1_crud<B: CrudBackend>(conn: B::Connection<'_>)
    where
        PlayerV1: Crud<B>,
    {
        PlayerV1::create(conn).await.unwrap();
        let mut first_player = PlayerV1 {
            id: PrimaryKey::new(0),
            name: "tymigrawr".to_string(),
        };
        first_player.insert(conn).await.unwrap();
        let player = one!(PlayerV1::read(conn, 0).await.unwrap());
        assert_eq!(first_player, player);
        let mut second_player = PlayerV1 {
            id: PrimaryKey::new(1),
            name: "developer".to_string(),
        };
        second_player.insert(conn).await.unwrap();
        let player = one!(PlayerV1::read(conn, 1).await.unwrap());
        assert_eq!(second_player, player);

        let p1 = one!(PlayerV1::read(conn, first_player.id.inner).await.unwrap());
        assert_eq!(first_player, p1);
        let p2 = one!(PlayerV1::read(conn, second_player.id.inner).await.unwrap());
        assert_eq!(second_player, p2);

        second_player.name = "software engineer".to_string();
        PlayerV1::update(&second_player, conn).await.unwrap();
        let p2 = one!(PlayerV1::read(conn, second_player.id.inner).await.unwrap());
        assert_eq!(second_player, p2);

        PlayerV1::delete(second_player, conn).await.unwrap();
        let players = all!(PlayerV1::read(conn, p2.id.inner).await.unwrap());
        assert!(players.is_empty());
    }

    async fn test_upsert<B: CrudBackend>(conn: B::Connection<'_>)
    where
        PlayerV1: Crud<B>,
    {
        PlayerV1::create(conn).await.unwrap();

        // Upsert a new row — should insert and return true
        let mut player = PlayerV1 {
            id: PrimaryKey::new(42),
            name: "original".to_string(),
        };
        let changed = player.upsert(conn).await.unwrap();
        assert!(changed, "upsert of new row should return true");

        // Read it back
        let from_db = one!(PlayerV1::read(conn, 42).await.unwrap());
        assert_eq!(player, from_db);

        // Upsert with same PK but different data — should update and return true
        let mut updated = PlayerV1 {
            id: PrimaryKey::new(42),
            name: "updated".to_string(),
        };
        let changed = updated.upsert(conn).await.unwrap();
        assert!(changed, "upsert of existing row should return true");

        // Read it back and verify update took effect
        let from_db = one!(PlayerV1::read(conn, 42).await.unwrap());
        assert_eq!(updated, from_db);

        // Verify only one row exists with that key
        let all = all!(PlayerV1::read(conn, 42).await.unwrap());
        assert_eq!(1, all.len(), "upsert should not duplicate rows");
    }

    async fn test_auto_increment_i64<B: CrudBackend>(conn: B::Connection<'_>)
    where
        AutoIncrementModel: Crud<B>,
    {
        AutoIncrementModel::create(conn).await.unwrap();

        // Verify that the id field has auto_increment enabled
        let crud_fields = <AutoIncrementModel as HasCrudFields>::crud_fields();
        let id_field = crud_fields
            .iter()
            .find(|f| f.name == "id")
            .expect("id field should exist");
        assert!(
            id_field.primary_key,
            "i64 id field should be marked as primary key"
        );
        assert!(
            id_field.auto_increment,
            "i64 id field should be marked as auto_increment"
        );

        // Insert three records with auto_increment and verify sequential IDs
        let mut record1 = AutoIncrementModel {
            id: AutoPrimaryKey::default(),
            name: "first".to_string(),
        };
        record1.insert(conn).await.unwrap();

        let mut record2 = AutoIncrementModel {
            id: AutoPrimaryKey::default(),
            name: "second".to_string(),
        };
        record2.insert(conn).await.unwrap();

        let mut record3 = AutoIncrementModel {
            id: AutoPrimaryKey::default(),
            name: "third".to_string(),
        };
        record3.insert(conn).await.unwrap();

        // Verify all records were created with sequential IDs
        let from_db_1 = one!(<AutoIncrementModel as Crud<B>>::read(conn, 1)
            .await
            .unwrap());
        assert_eq!(from_db_1.name, "first");
        assert_eq!(from_db_1.id.inner, Some(1));

        let from_db_2 = one!(<AutoIncrementModel as Crud<B>>::read(conn, 2)
            .await
            .unwrap());
        assert_eq!(from_db_2.name, "second");
        assert_eq!(from_db_2.id.inner, Some(2));

        let from_db_3 = one!(<AutoIncrementModel as Crud<B>>::read(conn, 3)
            .await
            .unwrap());
        assert_eq!(from_db_3.name, "third");
        assert_eq!(from_db_3.id.inner, Some(3));
    }

    async fn test_auto_increment_i32<B: CrudBackend>(conn: B::Connection<'_>)
    where
        AutoIncrementModelI32: Crud<B>,
    {
        <AutoIncrementModelI32 as Crud<B>>::create(conn)
            .await
            .unwrap();

        // Verify that the id field has auto_increment enabled (i32 variant)
        let crud_fields = <AutoIncrementModelI32 as HasCrudFields>::crud_fields();
        let id_field = crud_fields
            .iter()
            .find(|f| f.name == "id")
            .expect("id field should exist");
        assert!(
            id_field.primary_key,
            "i32 id field should be marked as primary key"
        );
        assert!(
            id_field.auto_increment,
            "i32 id field should be marked as auto_increment"
        );

        // Insert three records with auto_increment and verify sequential IDs
        let mut record1 = AutoIncrementModelI32 {
            id: AutoPrimaryKey::default(),
            name: "first i32".to_string(),
        };
        record1.insert(conn).await.unwrap();

        let mut record2 = AutoIncrementModelI32 {
            id: AutoPrimaryKey::default(),
            name: "second i32".to_string(),
        };
        record2.insert(conn).await.unwrap();

        let mut record3 = AutoIncrementModelI32 {
            id: AutoPrimaryKey::default(),
            name: "third i32".to_string(),
        };
        record3.insert(conn).await.unwrap();

        // Verify all records were created with sequential IDs
        let from_db_1 = one!(<AutoIncrementModelI32 as Crud<B>>::read(conn, 1)
            .await
            .unwrap());
        assert_eq!(from_db_1.name, "first i32");
        assert_eq!(from_db_1.id.inner, Some(1));

        let from_db_2 = one!(<AutoIncrementModelI32 as Crud<B>>::read(conn, 2)
            .await
            .unwrap());
        assert_eq!(from_db_2.name, "second i32");
        assert_eq!(from_db_2.id.inner, Some(2));

        let from_db_3 = one!(<AutoIncrementModelI32 as Crud<B>>::read(conn, 3)
            .await
            .unwrap());
        assert_eq!(from_db_3.name, "third i32");
        assert_eq!(from_db_3.id.inner, Some(3));
    }

    async fn test_auto_increment_u32<B: CrudBackend>(conn: B::Connection<'_>)
    where
        AutoIncrementModelU32: Crud<B>,
    {
        AutoIncrementModelU32::create(conn).await.unwrap();

        // Verify that the id field has auto_increment enabled (u32 variant)
        let crud_fields = <AutoIncrementModelU32 as HasCrudFields>::crud_fields();
        let id_field = crud_fields
            .iter()
            .find(|f| f.name == "id")
            .expect("id field should exist");
        assert!(
            id_field.primary_key,
            "u32 id field should be marked as primary key"
        );
        assert!(
            id_field.auto_increment,
            "u32 id field should be marked as auto_increment"
        );

        // Insert three records with auto_increment and verify sequential IDs
        let mut record1 = AutoIncrementModelU32 {
            id: AutoPrimaryKey::default(),
            name: "first u32".to_string(),
        };
        record1.insert(conn).await.unwrap();

        let mut record2 = AutoIncrementModelU32 {
            id: AutoPrimaryKey::default(),
            name: "second u32".to_string(),
        };
        record2.insert(conn).await.unwrap();

        let mut record3 = AutoIncrementModelU32 {
            id: AutoPrimaryKey::default(),
            name: "third u32".to_string(),
        };
        record3.insert(conn).await.unwrap();

        // Verify all records were created with sequential IDs
        let from_db_1 = one!(<AutoIncrementModelU32 as Crud<B>>::read(conn, 1)
            .await
            .unwrap());
        assert_eq!(from_db_1.name, "first u32");
        assert_eq!(from_db_1.id.inner, Some(1));

        let from_db_2 = one!(<AutoIncrementModelU32 as Crud<B>>::read(conn, 2)
            .await
            .unwrap());
        assert_eq!(from_db_2.name, "second u32");
        assert_eq!(from_db_2.id.inner, Some(2));

        let from_db_3 = one!(<AutoIncrementModelU32 as Crud<B>>::read(conn, 3)
            .await
            .unwrap());
        assert_eq!(from_db_3.name, "third u32");
        assert_eq!(from_db_3.id.inner, Some(3));
    }

    async fn test_auto_increment_key_update<B: CrudBackend>(conn: B::Connection<'_>)
    where
        AutoIncrementModel: Crud<B>,
    {
        AutoIncrementModel::create(conn).await.unwrap();

        // Test that insert() updates the AutoPrimaryKey value
        let mut record1 = AutoIncrementModel {
            id: AutoPrimaryKey::default(),
            name: "first".to_string(),
        };
        // Before insert, the id should be None
        assert_eq!(record1.id.inner, None, "id should be None before insert");

        record1.insert(conn).await.unwrap();

        // After insert, the id should be updated to Some(1)
        assert_eq!(
            record1.id.inner,
            Some(1),
            "id should be updated to Some(1) after insert"
        );

        // Insert another record to verify sequential incrementing
        let mut record2 = AutoIncrementModel {
            id: AutoPrimaryKey::default(),
            name: "second".to_string(),
        };
        assert_eq!(record2.id.inner, None, "id should be None before insert");

        record2.insert(conn).await.unwrap();

        assert_eq!(
            record2.id.inner,
            Some(2),
            "id should be updated to Some(2) after insert"
        );

        // Test that upsert() also updates the AutoPrimaryKey value
        let mut record3 = AutoIncrementModel {
            id: AutoPrimaryKey::default(),
            name: "third".to_string(),
        };
        assert_eq!(record3.id.inner, None, "id should be None before upsert");

        let changed = record3.upsert(conn).await.unwrap();
        assert!(changed, "upsert should return true for new record");

        assert_eq!(
            record3.id.inner,
            Some(3),
            "id should be updated to Some(3) after upsert"
        );

        // Verify upsert with existing key (should update, not create new)
        record3.name = "third updated".to_string();
        let changed = record3.upsert(conn).await.unwrap();
        assert!(changed, "upsert should return true for updated record");

        // ID should remain the same
        assert_eq!(
            record3.id.inner,
            Some(3),
            "id should remain Some(3) after upsert of existing record"
        );

        // Verify from database that the name was updated
        let from_db = one!(<AutoIncrementModel as Crud<B>>::read(conn, 3)
            .await
            .unwrap());
        assert_eq!(from_db.name, "third updated");
        assert_eq!(from_db.id.inner, Some(3));
    }

    async fn test_json_text<B: CrudBackend>(conn: B::Connection<'_>)
    where
        Palette: Crud<B>,
    {
        Palette::create(conn).await.unwrap();

        // Insert a palette with colors and Some metadata
        let mut palette = Palette {
            id: PrimaryKey::new(1),
            colors: JsonText::new(vec![
                Color {
                    name: "red".into(),
                    hex: 0xFF0000,
                },
                Color {
                    name: "green".into(),
                    hex: 0x00FF00,
                },
            ]),
            metadata: Some(JsonText::new(vec!["warm".into(), "nature".into()])),
        };
        palette.insert(conn).await.unwrap();

        // Read it back and verify round-trip
        let from_db = one!(<Palette as Crud<B>>::read(conn, 1).await.unwrap());
        assert_eq!(palette, from_db);

        // Insert a palette with None metadata
        let mut palette_no_meta = Palette {
            id: PrimaryKey::new(2),
            colors: JsonText::new(vec![Color {
                name: "blue".into(),
                hex: 0x0000FF,
            }]),
            metadata: None,
        };
        palette_no_meta.insert(conn).await.unwrap();

        let from_db = one!(<Palette as Crud<B>>::read(conn, 2).await.unwrap());
        assert_eq!(palette_no_meta, from_db);

        // Upsert the first palette with updated colors
        let mut updated = Palette {
            id: PrimaryKey::new(1),
            colors: JsonText::new(vec![Color {
                name: "purple".into(),
                hex: 0x800080,
            }]),
            metadata: None,
        };
        updated.upsert(conn).await.unwrap();

        let from_db = one!(<Palette as Crud<B>>::read(conn, 1).await.unwrap());
        assert_eq!(updated, from_db);

        // Verify read_all returns both palettes
        let all = all!(<Palette as Crud<B>>::read_all(conn).await.unwrap());
        assert_eq!(2, all.len());
    }

    async fn test_p2_crud<B: CrudBackend>(conn: B::Connection<'_>)
    where
        PlayerV2: Crud<B>,
    {
        <PlayerV2 as Crud<B>>::create(conn).await.unwrap();
        let mut first_player = PlayerV2 {
            id: PrimaryKey::new(0),
            name: "tymigrawr".to_string(),
            age: 0.1,
        };
        first_player.insert(conn).await.unwrap();
        let p1 = one!(<PlayerV2 as Crud<B>>::read(conn, first_player.id.inner)
            .await
            .unwrap());
        assert_eq!(first_player, p1);

        first_player.name = "software engineer".to_string();
        <PlayerV2 as Crud<B>>::update(&first_player, conn)
            .await
            .unwrap();
        let p2 = one!(<PlayerV2 as Crud<B>>::read(conn, first_player.id.inner)
            .await
            .unwrap());
        assert_eq!(first_player, p2);

        <PlayerV2 as Crud<B>>::delete(first_player, conn)
            .await
            .unwrap();
        let players = all!(<PlayerV2 as Crud<B>>::read(conn, p2.id.inner)
            .await
            .unwrap());
        assert!(players.is_empty());
    }

    async fn test_migrate<'a, B: MigrateEntireTable>(
        mk_connection: impl Fn(&str) -> <B as CrudBackend>::Connection<'a>,
    ) where
        PlayerV1: Crud<B>,
        PlayerV2: Crud<B>,
        PlayerV3: Crud<B>,
    {
        let _ = env_logger::builder().is_test(true).try_init();

        log::debug!("creating tables");
        <PlayerV1 as Crud<B>>::create((mk_connection)("playerv1"))
            .await
            .unwrap();
        <PlayerV2 as Crud<B>>::create((mk_connection)("playerv2"))
            .await
            .unwrap();
        <PlayerV3 as Crud<B>>::create((mk_connection)("playerv3"))
            .await
            .unwrap();

        log::debug!("populating v1");
        let mut players_v1 = (0..100)
            .map(|i| PlayerV1 {
                id: PrimaryKey::new(i),
                name: format!("tymigrawr_{i}"),
            })
            .collect::<Vec<_>>();
        for player in players_v1.iter_mut() {
            player.insert((mk_connection)("playerv1")).await.unwrap();
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
        migrations.run_with(&mk_connection).await.unwrap();

        let players_v1_from_db = all!(<PlayerV1 as Crud<B>>::read_all((mk_connection)("playerv1"))
            .await
            .unwrap());
        assert_eq!(Vec::<PlayerV1>::new(), players_v1_from_db);

        let players_v3_from_db = all!(<PlayerV3 as Crud<B>>::read_all((mk_connection)("playerv3"))
            .await
            .unwrap());
        assert_eq!(players_v3, players_v3_from_db);

        log::debug!("running reverse migrations");
        let migrations = Migrations::<Player, B>::default()
            .with_version::<PlayerV2>()
            .with_version::<PlayerV1>();
        migrations.run_with(&mk_connection).await.unwrap();

        let players_v1_from_db = all!(<PlayerV1 as Crud<B>>::read_all((mk_connection)("playerv1"))
            .await
            .unwrap());
        assert_eq!(players_v1, players_v1_from_db);
    }

    async fn test_migrate_4_versions<'a, B: MigrateEntireTable>(
        scenario: &str,
        conn: <B as CrudBackend>::Connection<'a>,
    ) where
        PlayerV1: Crud<B>,
        PlayerV2: Crud<B>,
        PlayerV3: Crud<B>,
        PlayerV4: Crud<B>,
    {
        let _ = env_logger::builder().is_test(true).try_init();

        // Helper to create a closure that returns the same connection regardless of table name
        let mk_connection = |_: &str| conn;

        log::debug!("=== Test Scenario: {} ===", scenario);

        // Test data: we'll use 10 rows with IDs 0-9
        let test_data_count = 10;

        match scenario {
            "v1_only" => {
                // Scenario A: Only V1 table exists
                log::debug!("Scenario A: Only V1 table exists");
                // Create the source table for test data insertion
                <PlayerV1 as Crud<B>>::create(conn).await.unwrap();

                let mut players_v1 = (0..test_data_count)
                    .map(|i| PlayerV1 {
                        id: PrimaryKey::new(i),
                        name: format!("player_v1_{i}"),
                    })
                    .collect::<Vec<_>>();

                for player in players_v1.iter_mut() {
                    player.insert(conn).await.unwrap();
                }

                // Expected final data in V4
                let expected_v4 = players_v1
                    .iter()
                    .cloned()
                    .map(PlayerV2::from)
                    .map(PlayerV3::from)
                    .map(PlayerV4::from)
                    .collect::<Vec<_>>();

                // Run migration chain V1 -> V4
                log::debug!("Running V1 -> V4 migration chain");
                let migrations = Migrations::<PlayerV1, B>::default()
                    .with_version::<PlayerV2>()
                    .with_version::<PlayerV3>()
                    .with_version::<PlayerV4>();
                migrations.run_with(mk_connection).await.unwrap();

                // Verify V1 table is empty
                let v1_remaining = all!(<PlayerV1 as Crud<B>>::read_all(conn).await.unwrap());
                assert_eq!(
                    Vec::<PlayerV1>::new(),
                    v1_remaining,
                    "V1 table should be empty after migration"
                );

                // Verify V4 table has all data
                let v4_data = all!(<PlayerV4 as Crud<B>>::read_all(conn).await.unwrap());
                assert_eq!(
                    expected_v4, v4_data,
                    "V4 table should contain all migrated data"
                );
            }

            "v2_only" => {
                // Scenario B: Only V2 table exists
                log::debug!("Scenario B: Only V2 table exists");
                // Create the source table for test data insertion
                <PlayerV2 as Crud<B>>::create(conn).await.unwrap();

                let mut players_v2 = (0..test_data_count)
                    .map(|i| PlayerV2 {
                        id: PrimaryKey::new(i),
                        name: format!("player_v2_{i}"),
                        age: (i as f32) + 20.0,
                    })
                    .collect::<Vec<_>>();

                for player in players_v2.iter_mut() {
                    player.insert(conn).await.unwrap();
                }

                // Expected final data in V4
                let expected_v4 = players_v2
                    .iter()
                    .cloned()
                    .map(PlayerV3::from)
                    .map(PlayerV4::from)
                    .collect::<Vec<_>>();

                // Run migration chain V1 -> V4
                log::debug!("Running V1 -> V4 migration chain");
                let migrations = Migrations::<PlayerV1, B>::default()
                    .with_version::<PlayerV2>()
                    .with_version::<PlayerV3>()
                    .with_version::<PlayerV4>();
                migrations.run_with(mk_connection).await.unwrap();

                // Verify V2 table is empty
                let v2_remaining = all!(<PlayerV2 as Crud<B>>::read_all(conn).await.unwrap());
                assert_eq!(
                    Vec::<PlayerV2>::new(),
                    v2_remaining,
                    "V2 table should be empty after migration"
                );

                // Verify V4 table has all data
                let v4_data = all!(<PlayerV4 as Crud<B>>::read_all(conn).await.unwrap());
                assert_eq!(
                    expected_v4, v4_data,
                    "V4 table should contain all migrated data"
                );
            }

            "v3_only" => {
                // Scenario C: Only V3 table exists
                log::debug!("Scenario C: Only V3 table exists");
                // Create the source table for test data insertion
                <PlayerV3 as Crud<B>>::create(conn).await.unwrap();

                let mut players_v3 = (0..test_data_count)
                    .map(|i| PlayerV3 {
                        id: PrimaryKey::new(i),
                        name: format!("player_v3_{i}"),
                        description: format!("A player with ID {i}"),
                    })
                    .collect::<Vec<_>>();

                for player in players_v3.iter_mut() {
                    player.insert(conn).await.unwrap();
                }

                // Expected final data in V4
                let expected_v4 = players_v3
                    .iter()
                    .cloned()
                    .map(PlayerV4::from)
                    .collect::<Vec<_>>();

                // Run migration chain V1 -> V4
                log::debug!("Running V1 -> V4 migration chain");
                let migrations = Migrations::<PlayerV1, B>::default()
                    .with_version::<PlayerV2>()
                    .with_version::<PlayerV3>()
                    .with_version::<PlayerV4>();
                migrations.run_with(mk_connection).await.unwrap();

                // Verify V3 table is empty
                let v3_remaining = all!(<PlayerV3 as Crud<B>>::read_all(conn).await.unwrap());
                assert_eq!(
                    Vec::<PlayerV3>::new(),
                    v3_remaining,
                    "V3 table should be empty after migration"
                );

                // Verify V4 table has all data
                let v4_data = all!(<PlayerV4 as Crud<B>>::read_all(conn).await.unwrap());
                assert_eq!(
                    expected_v4, v4_data,
                    "V4 table should contain all migrated data"
                );
            }

            "v4_only" => {
                // Scenario D: Only V4 table exists (no migration needed)
                log::debug!("Scenario D: Only V4 table exists");
                // Create the source table for test data insertion
                <PlayerV4 as Crud<B>>::create(conn).await.unwrap();

                let mut players_v4 = (0..test_data_count)
                    .map(|i| PlayerV4 {
                        id: PrimaryKey::new(i),
                        name: format!("player_v4_{i}"),
                        description: format!("V4 player {i}"),
                        region: "us-west".to_string(),
                    })
                    .collect::<Vec<_>>();

                for player in players_v4.iter_mut() {
                    player.insert(conn).await.unwrap();
                }

                // Run migration chain V1 -> V4
                log::debug!("Running V1 -> V4 migration chain");
                let migrations = Migrations::<PlayerV1, B>::default()
                    .with_version::<PlayerV2>()
                    .with_version::<PlayerV3>()
                    .with_version::<PlayerV4>();
                migrations.run_with(mk_connection).await.unwrap();

                // Verify V4 table still has all data
                let v4_data = all!(PlayerV4::read_all(conn).await.unwrap());
                assert_eq!(
                    players_v4, v4_data,
                    "V4 table should remain unchanged when it's the only table"
                );
            }

            "v1_v3_mix" => {
                // Scenario E: Both V1 and V3 tables exist
                log::debug!("Scenario E: Both V1 and V3 tables exist");
                // Create the source tables for test data insertion
                <PlayerV1 as Crud<B>>::create(conn).await.unwrap();
                <PlayerV3 as Crud<B>>::create(conn).await.unwrap();

                let mut players_v1 = (0..5)
                    .map(|i| PlayerV1 {
                        id: PrimaryKey::new(i),
                        name: format!("player_v1_{i}"),
                    })
                    .collect::<Vec<_>>();

                let mut players_v3 = (5..10)
                    .map(|i| PlayerV3 {
                        id: PrimaryKey::new(i),
                        name: format!("player_v3_{i}"),
                        description: format!("V3 player {i}"),
                    })
                    .collect::<Vec<_>>();

                for player in players_v1.iter_mut() {
                    player.insert(conn).await.unwrap();
                }
                for player in players_v3.iter_mut() {
                    player.insert(conn).await.unwrap();
                }

                // Expected final data in V4 (both sources converted)
                let mut expected_v4 = players_v1
                    .iter()
                    .cloned()
                    .map(PlayerV2::from)
                    .map(PlayerV3::from)
                    .map(PlayerV4::from)
                    .collect::<Vec<_>>();

                let v3_to_v4 = players_v3
                    .iter()
                    .cloned()
                    .map(PlayerV4::from)
                    .collect::<Vec<_>>();

                expected_v4.extend(v3_to_v4);
                expected_v4.sort_by_key(|p| p.id.inner);

                // Run migration chain V1 -> V4
                log::debug!("Running V1 -> V4 migration chain");
                let migrations = Migrations::<PlayerV1, B>::default()
                    .with_version::<PlayerV2>()
                    .with_version::<PlayerV3>()
                    .with_version::<PlayerV4>();
                migrations.run_with(mk_connection).await.unwrap();

                // Verify V1 and V3 tables are empty
                let v1_remaining = all!(<PlayerV1 as Crud<B>>::read_all(conn).await.unwrap());
                assert_eq!(
                    Vec::<PlayerV1>::new(),
                    v1_remaining,
                    "V1 table should be empty after migration"
                );

                let v3_remaining = all!(<PlayerV3 as Crud<B>>::read_all(conn).await.unwrap());
                assert_eq!(
                    Vec::<PlayerV3>::new(),
                    v3_remaining,
                    "V3 table should be empty after migration"
                );

                // Verify V4 table has all data from both sources
                let mut v4_data = all!(<PlayerV4 as Crud<B>>::read_all(conn).await.unwrap());
                v4_data.sort_by_key(|p| p.id.inner);

                assert_eq!(
                    expected_v4, v4_data,
                    "V4 table should contain data from both V1 and V3"
                );
            }

            "reverse" => {
                // Scenario F: Reverse migration (V4 -> V1)
                log::debug!("Scenario F: Reverse migration V4 -> V1");
                // Create the source table for test data insertion
                <PlayerV4 as Crud<B>>::create(conn).await.unwrap();

                let mut players_v4 = (0..test_data_count)
                    .map(|i| PlayerV4 {
                        id: PrimaryKey::new(i),
                        name: format!("player_v4_{i}"),
                        description: format!("V4 player {i}"),
                        region: "eu-central".to_string(),
                    })
                    .collect::<Vec<_>>();

                for player in players_v4.iter_mut() {
                    player.insert(conn).await.unwrap();
                }

                // Expected final data in V1 (reverse converted)
                let expected_v1 = players_v4
                    .iter()
                    .cloned()
                    .map(PlayerV3::from)
                    .map(PlayerV2::from)
                    .map(PlayerV1::from)
                    .collect::<Vec<_>>();

                // Run reverse migration chain V4 -> V1
                log::debug!("Running V4 -> V1 reverse migration chain");
                let migrations = Migrations::<PlayerV4, B>::default()
                    .with_version::<PlayerV3>()
                    .with_version::<PlayerV2>()
                    .with_version::<PlayerV1>();
                migrations.run_with(mk_connection).await.unwrap();

                // Verify V4 table is empty
                let v4_remaining = all!(<PlayerV4 as Crud<B>>::read_all(conn).await.unwrap());
                assert_eq!(
                    Vec::<PlayerV4>::new(),
                    v4_remaining,
                    "V4 table should be empty after reverse migration"
                );

                // Verify V1 table has all data
                let v1_data = all!(<PlayerV1 as Crud<B>>::read_all(conn).await.unwrap());
                assert_eq!(
                    expected_v1, v1_data,
                    "V1 table should contain all reverse-migrated data"
                );
            }

            _ => panic!("Unknown scenario: {}", scenario),
        }

        log::debug!("=== Scenario {} completed successfully ===", scenario);
    }

    #[cfg(feature = "backend_sqlite")]
    mod sqlite_tests {
        use super::{
            test_auto_increment_i32, test_auto_increment_i64, test_auto_increment_key_update,
            test_auto_increment_u32, test_json_text, test_migrate, test_migrate_4_versions,
            test_p1_crud, test_p2_crud, test_upsert, SettingsV1,
        };
        use crate::{Crud, PrimaryKey, Sqlite};
        use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};

        async fn pool() -> SqlitePool {
            SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .unwrap()
        }

        #[tokio::test]
        async fn p1_crud() {
            let pool = pool().await;
            test_p1_crud::<Sqlite>(&pool).await;
        }

        #[tokio::test]
        async fn p2_crud() {
            let pool = pool().await;
            test_p2_crud::<Sqlite>(&pool).await;
        }

        #[tokio::test]
        async fn upsert() {
            let pool = pool().await;
            test_upsert::<Sqlite>(&pool).await;
        }

        #[tokio::test]
        async fn json_text() {
            let pool = pool().await;
            test_json_text::<Sqlite>(&pool).await;
        }

        #[tokio::test]
        async fn read_with_nullable_fields() {
            let pool = pool().await;
            <SettingsV1 as Crud<Sqlite>>::create(&pool).await.unwrap();

            // Insert a row with NULL Option fields
            let mut settings = SettingsV1 {
                id: PrimaryKey::new(1),
                api_key: None,
                token: None,
                timeout_secs: 60,
            };
            <SettingsV1 as Crud<Sqlite>>::insert(&mut settings, &pool)
                .await
                .unwrap();

            // Read it back via the async Crud API
            let loaded = {
                use futures::StreamExt;
                let mut s = <SettingsV1 as Crud<Sqlite>>::read(&pool, 1).await.unwrap();
                s.next()
                    .await
                    .expect("stream ended")
                    .expect("row read failed")
            };
            assert_eq!(settings, loaded);

            // Insert another row with Some values
            let mut settings2 = SettingsV1 {
                id: PrimaryKey::new(2),
                api_key: Some("secret-key".to_string()),
                token: Some("auth-token".to_string()),
                timeout_secs: 120,
            };
            <SettingsV1 as Crud<Sqlite>>::insert(&mut settings2, &pool)
                .await
                .unwrap();

            let loaded2 = {
                use futures::StreamExt;
                let mut s = <SettingsV1 as Crud<Sqlite>>::read(&pool, 2).await.unwrap();
                s.next()
                    .await
                    .expect("stream ended")
                    .expect("row read failed")
            };
            assert_eq!(settings2, loaded2);

            // Insert row with mixed Some/None
            let mut settings3 = SettingsV1 {
                id: PrimaryKey::new(3),
                api_key: Some("key-only".to_string()),
                token: None,
                timeout_secs: 90,
            };
            <SettingsV1 as Crud<Sqlite>>::insert(&mut settings3, &pool)
                .await
                .unwrap();

            let loaded3 = {
                use futures::StreamExt;
                let mut s = <SettingsV1 as Crud<Sqlite>>::read(&pool, 3).await.unwrap();
                s.next()
                    .await
                    .expect("stream ended")
                    .expect("row read failed")
            };
            assert_eq!(settings3, loaded3);
        }

        #[tokio::test]
        async fn auto_increment_i64() {
            let pool = pool().await;
            test_auto_increment_i64::<Sqlite>(&pool).await;
        }

        #[tokio::test]
        async fn auto_increment_i32() {
            let pool = pool().await;
            test_auto_increment_i32::<Sqlite>(&pool).await;
        }

        #[tokio::test]
        async fn auto_increment_u32() {
            let pool = pool().await;
            test_auto_increment_u32::<Sqlite>(&pool).await;
        }

        #[tokio::test]
        async fn auto_increment_key_update() {
            let pool = pool().await;
            test_auto_increment_key_update::<Sqlite>(&pool).await;
        }

        #[tokio::test]
        async fn migrate() {
            // Use a shared on-disk database for V1/V2 and a separate file for V3.
            let tempdir = tempfile::tempdir().unwrap();
            let url = format!(
                "sqlite://{}?mode=rwc",
                tempdir.path().join("data.db").display()
            );
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await
                .unwrap();
            let url_v3 = format!(
                "sqlite://{}?mode=rwc",
                tempdir.path().join("data_v3.db").display()
            );
            let pool_v3 = SqlitePoolOptions::new()
                .max_connections(1)
                .connect(&url_v3)
                .await
                .unwrap();
            test_migrate::<Sqlite>(|table| match table {
                "playerv3" => &pool_v3,
                _ => &pool,
            })
            .await;
        }

        #[tokio::test]
        async fn migrate_4_versions_v1_only() {
            let pool = pool().await;
            test_migrate_4_versions::<Sqlite>("v1_only", &pool).await;
        }

        #[tokio::test]
        async fn migrate_4_versions_v2_only() {
            let pool = pool().await;
            test_migrate_4_versions::<Sqlite>("v2_only", &pool).await;
        }

        #[tokio::test]
        async fn migrate_4_versions_v3_only() {
            let pool = pool().await;
            test_migrate_4_versions::<Sqlite>("v3_only", &pool).await;
        }

        #[tokio::test]
        async fn migrate_4_versions_v4_only() {
            let pool = pool().await;
            test_migrate_4_versions::<Sqlite>("v4_only", &pool).await;
        }

        #[tokio::test]
        async fn migrate_4_versions_v1_v3_mix() {
            let pool = pool().await;
            test_migrate_4_versions::<Sqlite>("v1_v3_mix", &pool).await;
        }

        #[tokio::test]
        async fn migrate_4_versions_reverse() {
            let pool = pool().await;
            test_migrate_4_versions::<Sqlite>("reverse", &pool).await;
        }
    }

    #[cfg(feature = "backend_doltlite")]
    mod doltlite_tests {
        use super::*;
        use crate::Doltlite;

        fn open_in_memory() -> rusqlite::Connection {
            rusqlite::Connection::open_in_memory().unwrap()
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn p1_crud() {
            let conn = open_in_memory();
            test_p1_crud::<Doltlite>(&conn).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn p2_crud() {
            let conn = open_in_memory();
            test_p2_crud::<Doltlite>(&conn).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn upsert() {
            let conn = open_in_memory();
            test_upsert::<Doltlite>(&conn).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn json_text() {
            let conn = open_in_memory();
            test_json_text::<Doltlite>(&conn).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn auto_increment_i64() {
            let conn = open_in_memory();
            test_auto_increment_i64::<Doltlite>(&conn).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn auto_increment_i32() {
            let conn = open_in_memory();
            test_auto_increment_i32::<Doltlite>(&conn).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn auto_increment_u32() {
            let conn = open_in_memory();
            test_auto_increment_u32::<Doltlite>(&conn).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn auto_increment_key_update() {
            let conn = open_in_memory();
            test_auto_increment_key_update::<Doltlite>(&conn).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn migrate() {
            let tempdir = tempfile::tempdir().unwrap();
            let path = tempdir.path().join("data.db");
            let connection = rusqlite::Connection::open(&path).unwrap();
            let path = tempdir.path().join("data_v3.db");
            let connection_v3 = rusqlite::Connection::open(&path).unwrap();
            test_migrate::<Doltlite>(|table| match table {
                "playerv3" => &connection_v3,
                _ => &connection,
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn migrate_4_versions_v1_only() {
            let conn = open_in_memory();
            test_migrate_4_versions::<Doltlite>("v1_only", &conn).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn migrate_4_versions_v2_only() {
            let conn = open_in_memory();
            test_migrate_4_versions::<Doltlite>("v2_only", &conn).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn migrate_4_versions_v3_only() {
            let conn = open_in_memory();
            test_migrate_4_versions::<Doltlite>("v3_only", &conn).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn migrate_4_versions_v4_only() {
            let conn = open_in_memory();
            test_migrate_4_versions::<Doltlite>("v4_only", &conn).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn migrate_4_versions_v1_v3_mix() {
            let conn = open_in_memory();
            test_migrate_4_versions::<Doltlite>("v1_v3_mix", &conn).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn migrate_4_versions_reverse() {
            let conn = open_in_memory();
            test_migrate_4_versions::<Doltlite>("reverse", &conn).await;
        }
    }

    #[cfg(feature = "backend_toml")]
    mod toml_tests {
        use super::*;
        use crate::Toml;

        #[tokio::test]
        async fn p1_crud() {
            let tempdir = tempfile::tempdir().unwrap();
            test_p1_crud::<Toml>(tempdir.path()).await;
        }

        #[tokio::test]
        async fn p2_crud() {
            let tempdir = tempfile::tempdir().unwrap();
            test_p2_crud::<Toml>(tempdir.path()).await;
        }

        #[tokio::test]
        async fn upsert() {
            let tempdir = tempfile::tempdir().unwrap();
            test_upsert::<Toml>(tempdir.path()).await;
        }

        #[tokio::test]
        async fn json_text() {
            let tempdir = tempfile::tempdir().unwrap();
            test_json_text::<Toml>(tempdir.path()).await;
        }

        #[tokio::test]
        async fn migrate() {
            let tempdir = tempfile::tempdir().unwrap();
            test_migrate::<Toml>(|_| tempdir.path()).await;
        }

        #[tokio::test]
        async fn migrate_4_versions_v1_only() {
            let tempdir = tempfile::tempdir().unwrap();
            test_migrate_4_versions::<Toml>("v1_only", tempdir.path()).await;
        }

        #[tokio::test]
        async fn migrate_4_versions_v2_only() {
            let tempdir = tempfile::tempdir().unwrap();
            test_migrate_4_versions::<Toml>("v2_only", tempdir.path()).await;
        }

        #[tokio::test]
        async fn migrate_4_versions_v3_only() {
            let tempdir = tempfile::tempdir().unwrap();
            test_migrate_4_versions::<Toml>("v3_only", tempdir.path()).await;
        }

        #[tokio::test]
        async fn migrate_4_versions_v4_only() {
            let tempdir = tempfile::tempdir().unwrap();
            test_migrate_4_versions::<Toml>("v4_only", tempdir.path()).await;
        }

        #[tokio::test]
        async fn migrate_4_versions_v1_v3_mix() {
            let tempdir = tempfile::tempdir().unwrap();
            test_migrate_4_versions::<Toml>("v1_v3_mix", tempdir.path()).await;
        }

        #[tokio::test]
        async fn migrate_4_versions_reverse() {
            let tempdir = tempfile::tempdir().unwrap();
            test_migrate_4_versions::<Toml>("reverse", tempdir.path()).await;
        }
    }

    #[cfg(feature = "backend_sqlite")]
    #[tokio::test]
    async fn module_docs() {
        use crate::{Crud, CrudBackend, HasCrudFields, PrimaryKey, Sqlite};
        use sqlx::SqlitePool;

        /// Define a business type that can be persisted.
        #[derive(Debug, Clone, HasCrudFields)]
        struct User {
            id: PrimaryKey<i64>,
            name: String,
        }

        /// For the most part, business logic involving persistence can be generic over the backend.
        async fn run<'a, Backend: CrudBackend>(
            conn: Backend::Connection<'a>,
        ) -> Result<(), tymigrawr::Error<Backend::Error>>
        where
            User: Crud<Backend>,
        {
            // Create table
            User::create(conn).await?;

            // Insert
            let mut user = User {
                id: PrimaryKey::new(1),
                name: "Alice".to_string(),
            };
            user.insert(conn).await?;

            // Read
            let mut users = User::read_all(conn).await?;
            while let Some(result) = users.next().await {
                let user = result?;
                println!("{}", user.name);
            }

            Ok(())
        }

        // Then specialize on the backend at the edges of your application
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        run::<Sqlite>(&pool).await.unwrap();
    }
}
