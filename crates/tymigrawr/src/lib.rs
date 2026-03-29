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
//! ```rust
//! use tymigrawr::{HasCrudFields, PrimaryKey, Crud, Migrations, Sqlite};
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
//! // Create the backend connection
//! let conn = sqlite::open(":memory:").unwrap();
//!
//! // Use version 1
//! PlayerV1::create(&conn).unwrap();
//! let player = PlayerV1 {
//!     id: PrimaryKey::new(1),
//!     name: "Alice".to_string(),
//! };
//! player.insert(&conn).unwrap();
//!
//! // Use version 2
//! PlayerV2::create(&conn).unwrap();
//!
//! // Run migrations
//! let migrations = Migrations::<PlayerV1, Sqlite>::default()
//!     .with_version::<PlayerV2>();
//! migrations.run(&conn).unwrap();
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

#[cfg(feature = "backend_sqlite")]
mod backend_sqlite;
#[cfg(feature = "backend_sqlite")]
pub use backend_sqlite::*;

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

    fn test_p1_crud<B: CrudBackend>(conn: B::Connection<'_>)
    where
        PlayerV1: Crud<B>,
    {
        PlayerV1::create(conn).unwrap();
        let first_player = PlayerV1 {
            id: PrimaryKey::new(0),
            name: "tymigrawr".to_string(),
        };
        PlayerV1::insert(&first_player, conn).unwrap();
        let player = PlayerV1::read(conn, 0).unwrap().next().unwrap().unwrap();
        assert_eq!(first_player, player);
        let mut second_player = PlayerV1 {
            id: PrimaryKey::new(1),
            name: "developer".to_string(),
        };
        PlayerV1::insert(&second_player, conn).unwrap();
        let player = PlayerV1::read(conn, 1).unwrap().next().unwrap().unwrap();
        assert_eq!(second_player, player);

        let mut p1 = PlayerV1::read(conn, first_player.id.inner).unwrap();
        assert_eq!(first_player, p1.next().unwrap().unwrap());
        let mut p2 = PlayerV1::read(conn, second_player.id.inner).unwrap();
        assert_eq!(second_player, p2.next().unwrap().unwrap());

        second_player.name = "software engineer".to_string();
        PlayerV1::update(&second_player, conn).unwrap();
        let p2 = PlayerV1::read(conn, second_player.id.inner)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(second_player, p2);

        PlayerV1::delete(second_player, conn).unwrap();
        let players = PlayerV1::read(conn, p2.id.inner)
            .unwrap()
            .map(|p| p.unwrap())
            .collect::<Vec<_>>();
        assert!(players.is_empty());
    }

    fn test_upsert<B: CrudBackend>(conn: B::Connection<'_>)
    where
        PlayerV1: Crud<B>,
    {
        PlayerV1::create(conn).unwrap();

        // Upsert a new row — should insert and return true
        let player = PlayerV1 {
            id: PrimaryKey::new(42),
            name: "original".to_string(),
        };
        let changed = PlayerV1::upsert(&player, conn).unwrap();
        assert!(changed, "upsert of new row should return true");

        // Read it back
        let from_db = PlayerV1::read(conn, 42).unwrap().next().unwrap().unwrap();
        assert_eq!(player, from_db);

        // Upsert with same PK but different data — should update and return true
        let updated = PlayerV1 {
            id: PrimaryKey::new(42),
            name: "updated".to_string(),
        };
        let changed = PlayerV1::upsert(&updated, conn).unwrap();
        assert!(changed, "upsert of existing row should return true");

        // Read it back and verify update took effect
        let from_db = PlayerV1::read(conn, 42).unwrap().next().unwrap().unwrap();
        assert_eq!(updated, from_db);

        // Verify only one row exists with that key
        let all = PlayerV1::read(conn, 42).unwrap().collect::<Vec<_>>();
        assert_eq!(1, all.len(), "upsert should not duplicate rows");
    }

    fn test_auto_increment_i64<B: CrudBackend>(conn: B::Connection<'_>)
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
            "i64 id field should be marked as primary key"
        );
        assert!(
            id_field.auto_increment,
            "i64 id field should be marked as auto_increment"
        );

        // Insert three records with auto_increment and verify sequential IDs
        let record1 = AutoIncrementModel {
            id: AutoPrimaryKey::default(),
            name: "first".to_string(),
        };
        <AutoIncrementModel as Crud<B>>::insert(&record1, conn).unwrap();

        let record2 = AutoIncrementModel {
            id: AutoPrimaryKey::default(),
            name: "second".to_string(),
        };
        <AutoIncrementModel as Crud<B>>::insert(&record2, conn).unwrap();

        let record3 = AutoIncrementModel {
            id: AutoPrimaryKey::default(),
            name: "third".to_string(),
        };
        <AutoIncrementModel as Crud<B>>::insert(&record3, conn).unwrap();

        // Verify all records were created with sequential IDs
        let from_db_1 = <AutoIncrementModel as Crud<B>>::read(conn, 1)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(from_db_1.name, "first");
        assert_eq!(from_db_1.id.inner, Some(1));

        let from_db_2 = <AutoIncrementModel as Crud<B>>::read(conn, 2)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(from_db_2.name, "second");
        assert_eq!(from_db_2.id.inner, Some(2));

        let from_db_3 = <AutoIncrementModel as Crud<B>>::read(conn, 3)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(from_db_3.name, "third");
        assert_eq!(from_db_3.id.inner, Some(3));
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
            "i32 id field should be marked as primary key"
        );
        assert!(
            id_field.auto_increment,
            "i32 id field should be marked as auto_increment"
        );

        // Insert three records with auto_increment and verify sequential IDs
        let record1 = AutoIncrementModelI32 {
            id: AutoPrimaryKey::default(),
            name: "first i32".to_string(),
        };
        <AutoIncrementModelI32 as Crud<B>>::insert(&record1, conn).unwrap();

        let record2 = AutoIncrementModelI32 {
            id: AutoPrimaryKey::default(),
            name: "second i32".to_string(),
        };
        <AutoIncrementModelI32 as Crud<B>>::insert(&record2, conn).unwrap();

        let record3 = AutoIncrementModelI32 {
            id: AutoPrimaryKey::default(),
            name: "third i32".to_string(),
        };
        <AutoIncrementModelI32 as Crud<B>>::insert(&record3, conn).unwrap();

        // Verify all records were created with sequential IDs
        let from_db_1 = <AutoIncrementModelI32 as Crud<B>>::read(conn, 1)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(from_db_1.name, "first i32");
        assert_eq!(from_db_1.id.inner, Some(1));

        let from_db_2 = <AutoIncrementModelI32 as Crud<B>>::read(conn, 2)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(from_db_2.name, "second i32");
        assert_eq!(from_db_2.id.inner, Some(2));

        let from_db_3 = <AutoIncrementModelI32 as Crud<B>>::read(conn, 3)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(from_db_3.name, "third i32");
        assert_eq!(from_db_3.id.inner, Some(3));
    }

    fn test_auto_increment_u32<B: CrudBackend>(conn: B::Connection<'_>)
    where
        AutoIncrementModelU32: Crud<B>,
    {
        <AutoIncrementModelU32 as Crud<B>>::create(conn).unwrap();

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
        let record1 = AutoIncrementModelU32 {
            id: AutoPrimaryKey::default(),
            name: "first u32".to_string(),
        };
        <AutoIncrementModelU32 as Crud<B>>::insert(&record1, conn).unwrap();

        let record2 = AutoIncrementModelU32 {
            id: AutoPrimaryKey::default(),
            name: "second u32".to_string(),
        };
        <AutoIncrementModelU32 as Crud<B>>::insert(&record2, conn).unwrap();

        let record3 = AutoIncrementModelU32 {
            id: AutoPrimaryKey::default(),
            name: "third u32".to_string(),
        };
        <AutoIncrementModelU32 as Crud<B>>::insert(&record3, conn).unwrap();

        // Verify all records were created with sequential IDs
        let from_db_1 = <AutoIncrementModelU32 as Crud<B>>::read(conn, 1)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(from_db_1.name, "first u32");
        assert_eq!(from_db_1.id.inner, Some(1));

        let from_db_2 = <AutoIncrementModelU32 as Crud<B>>::read(conn, 2)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(from_db_2.name, "second u32");
        assert_eq!(from_db_2.id.inner, Some(2));

        let from_db_3 = <AutoIncrementModelU32 as Crud<B>>::read(conn, 3)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(from_db_3.name, "third u32");
        assert_eq!(from_db_3.id.inner, Some(3));
    }

    fn test_json_text<B: CrudBackend>(conn: B::Connection<'_>)
    where
        Palette: Crud<B>,
    {
        <Palette as Crud<B>>::create(conn).unwrap();

        // Insert a palette with colors and Some metadata
        let palette = Palette {
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
            id: PrimaryKey::new(2),
            colors: JsonText::new(vec![Color {
                name: "blue".into(),
                hex: 0x0000FF,
            }]),
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
            id: PrimaryKey::new(1),
            colors: JsonText::new(vec![Color {
                name: "purple".into(),
                hex: 0x800080,
            }]),
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
            id: PrimaryKey::new(0),
            name: "tymigrawr".to_string(),
            age: 0.1,
        };
        <PlayerV2 as Crud<B>>::insert(&first_player, conn).unwrap();
        let mut p1 = <PlayerV2 as Crud<B>>::read(conn, first_player.id.inner).unwrap();
        assert_eq!(first_player, p1.next().unwrap().unwrap());

        first_player.name = "software engineer".to_string();
        <PlayerV2 as Crud<B>>::update(&first_player, conn).unwrap();
        let p2 = <PlayerV2 as Crud<B>>::read(conn, first_player.id.inner)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(first_player, p2);

        <PlayerV2 as Crud<B>>::delete(first_player, conn).unwrap();
        let players = <PlayerV2 as Crud<B>>::read(conn, p2.id.inner)
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
        let _ = env_logger::builder().is_test(true).try_init();

        log::debug!("creating tables");
        <PlayerV1 as Crud<B>>::create((mk_connection)("playerv1")).unwrap();
        <PlayerV2 as Crud<B>>::create((mk_connection)("playerv2")).unwrap();
        <PlayerV3 as Crud<B>>::create((mk_connection)("playerv3")).unwrap();

        log::debug!("populating v1");
        let players_v1 = (0..100)
            .map(|i| PlayerV1 {
                id: PrimaryKey::new(i),
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

    fn test_migrate_4_versions<'a, B: MigrateEntireTable>(
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
                <PlayerV1 as Crud<B>>::create(conn).unwrap();

                let players_v1 = (0..test_data_count)
                    .map(|i| PlayerV1 {
                        id: PrimaryKey::new(i),
                        name: format!("player_v1_{i}"),
                    })
                    .collect::<Vec<_>>();

                for player in players_v1.iter() {
                    <PlayerV1 as Crud<B>>::insert(player, conn).unwrap();
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
                migrations.run_with(mk_connection).unwrap();

                // Verify V1 table is empty
                let v1_remaining = <PlayerV1 as Crud<B>>::read_all(conn)
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(
                    Vec::<PlayerV1>::new(),
                    v1_remaining,
                    "V1 table should be empty after migration"
                );

                // Verify V4 table has all data
                let v4_data = <PlayerV4 as Crud<B>>::read_all(conn)
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(
                    expected_v4, v4_data,
                    "V4 table should contain all migrated data"
                );
            }

            "v2_only" => {
                // Scenario B: Only V2 table exists
                log::debug!("Scenario B: Only V2 table exists");
                // Create the source table for test data insertion
                <PlayerV2 as Crud<B>>::create(conn).unwrap();

                let players_v2 = (0..test_data_count)
                    .map(|i| PlayerV2 {
                        id: PrimaryKey::new(i),
                        name: format!("player_v2_{i}"),
                        age: (i as f32) + 20.0,
                    })
                    .collect::<Vec<_>>();

                for player in players_v2.iter() {
                    <PlayerV2 as Crud<B>>::insert(player, conn).unwrap();
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
                migrations.run_with(mk_connection).unwrap();

                // Verify V2 table is empty
                let v2_remaining = <PlayerV2 as Crud<B>>::read_all(conn)
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(
                    Vec::<PlayerV2>::new(),
                    v2_remaining,
                    "V2 table should be empty after migration"
                );

                // Verify V4 table has all data
                let v4_data = <PlayerV4 as Crud<B>>::read_all(conn)
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(
                    expected_v4, v4_data,
                    "V4 table should contain all migrated data"
                );
            }

            "v3_only" => {
                // Scenario C: Only V3 table exists
                log::debug!("Scenario C: Only V3 table exists");
                // Create the source table for test data insertion
                <PlayerV3 as Crud<B>>::create(conn).unwrap();

                let players_v3 = (0..test_data_count)
                    .map(|i| PlayerV3 {
                        id: PrimaryKey::new(i),
                        name: format!("player_v3_{i}"),
                        description: format!("A player with ID {i}"),
                    })
                    .collect::<Vec<_>>();

                for player in players_v3.iter() {
                    <PlayerV3 as Crud<B>>::insert(player, conn).unwrap();
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
                migrations.run_with(mk_connection).unwrap();

                // Verify V3 table is empty
                let v3_remaining = <PlayerV3 as Crud<B>>::read_all(conn)
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(
                    Vec::<PlayerV3>::new(),
                    v3_remaining,
                    "V3 table should be empty after migration"
                );

                // Verify V4 table has all data
                let v4_data = <PlayerV4 as Crud<B>>::read_all(conn)
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(
                    expected_v4, v4_data,
                    "V4 table should contain all migrated data"
                );
            }

            "v4_only" => {
                // Scenario D: Only V4 table exists (no migration needed)
                log::debug!("Scenario D: Only V4 table exists");
                // Create the source table for test data insertion
                <PlayerV4 as Crud<B>>::create(conn).unwrap();

                let players_v4 = (0..test_data_count)
                    .map(|i| PlayerV4 {
                        id: PrimaryKey::new(i),
                        name: format!("player_v4_{i}"),
                        description: format!("V4 player {i}"),
                        region: "us-west".to_string(),
                    })
                    .collect::<Vec<_>>();

                for player in players_v4.iter() {
                    PlayerV4::insert(player, conn).unwrap();
                }

                // Run migration chain V1 -> V4
                log::debug!("Running V1 -> V4 migration chain");
                let migrations = Migrations::<PlayerV1, B>::default()
                    .with_version::<PlayerV2>()
                    .with_version::<PlayerV3>()
                    .with_version::<PlayerV4>();
                migrations.run_with(mk_connection).unwrap();

                // Verify V4 table still has all data
                let v4_data = PlayerV4::read_all(conn)
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(
                    players_v4, v4_data,
                    "V4 table should remain unchanged when it's the only table"
                );
            }

            "v1_v3_mix" => {
                // Scenario E: Both V1 and V3 tables exist
                log::debug!("Scenario E: Both V1 and V3 tables exist");
                // Create the source tables for test data insertion
                <PlayerV1 as Crud<B>>::create(conn).unwrap();
                <PlayerV3 as Crud<B>>::create(conn).unwrap();

                let players_v1 = (0..5)
                    .map(|i| PlayerV1 {
                        id: PrimaryKey::new(i),
                        name: format!("player_v1_{i}"),
                    })
                    .collect::<Vec<_>>();

                let players_v3 = (5..10)
                    .map(|i| PlayerV3 {
                        id: PrimaryKey::new(i),
                        name: format!("player_v3_{i}"),
                        description: format!("V3 player {i}"),
                    })
                    .collect::<Vec<_>>();

                for player in players_v1.iter() {
                    <PlayerV1 as Crud<B>>::insert(player, conn).unwrap();
                }
                for player in players_v3.iter() {
                    <PlayerV3 as Crud<B>>::insert(player, conn).unwrap();
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
                migrations.run_with(mk_connection).unwrap();

                // Verify V1 and V3 tables are empty
                let v1_remaining = <PlayerV1 as Crud<B>>::read_all(conn)
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(
                    Vec::<PlayerV1>::new(),
                    v1_remaining,
                    "V1 table should be empty after migration"
                );

                let v3_remaining = <PlayerV3 as Crud<B>>::read_all(conn)
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(
                    Vec::<PlayerV3>::new(),
                    v3_remaining,
                    "V3 table should be empty after migration"
                );

                // Verify V4 table has all data from both sources
                let mut v4_data = <PlayerV4 as Crud<B>>::read_all(conn)
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect::<Vec<_>>();
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
                <PlayerV4 as Crud<B>>::create(conn).unwrap();

                let players_v4 = (0..test_data_count)
                    .map(|i| PlayerV4 {
                        id: PrimaryKey::new(i),
                        name: format!("player_v4_{i}"),
                        description: format!("V4 player {i}"),
                        region: "eu-central".to_string(),
                    })
                    .collect::<Vec<_>>();

                for player in players_v4.iter() {
                    <PlayerV4 as Crud<B>>::insert(player, conn).unwrap();
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
                migrations.run_with(mk_connection).unwrap();

                // Verify V4 table is empty
                let v4_remaining = <PlayerV4 as Crud<B>>::read_all(conn)
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(
                    Vec::<PlayerV4>::new(),
                    v4_remaining,
                    "V4 table should be empty after reverse migration"
                );

                // Verify V1 table has all data
                let v1_data = <PlayerV1 as Crud<B>>::read_all(conn)
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect::<Vec<_>>();
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
                id: PrimaryKey::new(1),
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
                id: PrimaryKey::new(2),
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
                id: PrimaryKey::new(3),
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
        fn auto_increment_i64() {
            let conn = sqlite::open(":memory:").unwrap();
            test_auto_increment_i64::<Sqlite>(&conn);
        }

        #[test]
        fn auto_increment_i32() {
            let conn = sqlite::open(":memory:").unwrap();
            test_auto_increment_i32::<Sqlite>(&conn);
        }

        #[test]
        fn auto_increment_u32() {
            let conn = sqlite::open(":memory:").unwrap();
            test_auto_increment_u32::<Sqlite>(&conn);
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

        #[test]
        fn migrate_4_versions_v1_only() {
            let conn = sqlite::open(":memory:").unwrap();
            test_migrate_4_versions::<Sqlite>("v1_only", &conn);
        }

        #[test]
        fn migrate_4_versions_v2_only() {
            let conn = sqlite::open(":memory:").unwrap();
            test_migrate_4_versions::<Sqlite>("v2_only", &conn);
        }

        #[test]
        fn migrate_4_versions_v3_only() {
            let conn = sqlite::open(":memory:").unwrap();
            test_migrate_4_versions::<Sqlite>("v3_only", &conn);
        }

        #[test]
        fn migrate_4_versions_v4_only() {
            let conn = sqlite::open(":memory:").unwrap();
            test_migrate_4_versions::<Sqlite>("v4_only", &conn);
        }

        #[test]
        fn migrate_4_versions_v1_v3_mix() {
            let conn = sqlite::open(":memory:").unwrap();
            test_migrate_4_versions::<Sqlite>("v1_v3_mix", &conn);
        }

        #[test]
        fn migrate_4_versions_reverse() {
            let conn = sqlite::open(":memory:").unwrap();
            test_migrate_4_versions::<Sqlite>("reverse", &conn);
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

        #[test]
        fn migrate_4_versions_v1_only() {
            let tempdir = tempfile::tempdir().unwrap();
            test_migrate_4_versions::<Toml>("v1_only", tempdir.path());
        }

        #[test]
        fn migrate_4_versions_v2_only() {
            let tempdir = tempfile::tempdir().unwrap();
            test_migrate_4_versions::<Toml>("v2_only", tempdir.path());
        }

        #[test]
        fn migrate_4_versions_v3_only() {
            let tempdir = tempfile::tempdir().unwrap();
            test_migrate_4_versions::<Toml>("v3_only", tempdir.path());
        }

        #[test]
        fn migrate_4_versions_v4_only() {
            let tempdir = tempfile::tempdir().unwrap();
            test_migrate_4_versions::<Toml>("v4_only", tempdir.path());
        }

        #[test]
        fn migrate_4_versions_v1_v3_mix() {
            let tempdir = tempfile::tempdir().unwrap();
            test_migrate_4_versions::<Toml>("v1_v3_mix", tempdir.path());
        }

        #[test]
        fn migrate_4_versions_reverse() {
            let tempdir = tempfile::tempdir().unwrap();
            test_migrate_4_versions::<Toml>("reverse", tempdir.path());
        }
    }

    #[test]
    fn module_docs() {
        use crate::{Crud, HasCrudFields, PrimaryKey, Sqlite};

        /// Define a business type that can be persisted.
        #[derive(Debug, Clone, HasCrudFields)]
        struct User {
            id: PrimaryKey<i64>,
            name: String,
        }

        /// For the most part, business logic involving persistance can be generic over the backend.
        fn run<'a, Backend: CrudBackend>(
            conn: Backend::Connection<'a>,
        ) -> Result<(), tymigrawr::Error<Backend::Error>>
        where
            User: Crud<Backend>,
        {
            // Create table
            User::create(conn)?;

            // Insert
            let user = User {
                id: PrimaryKey::new(1),
                name: "Alice".to_string(),
            };
            user.insert(conn)?;

            // Read
            let users = User::read_all(conn)?;
            for result in users {
                let user = result?;
                println!("{}", user.name);
            }

            Ok(())
        }

        // Then specialize on the backend at the edges of your application
        let conn = sqlite::open(":memory:").unwrap();
        run::<Sqlite>(&conn).unwrap();
    }
}
