# tymigrawr!

<div align="center">
  <h1>
    <img src="logo.png" alt="tymigrawr!" />
  </h1>
</div>

> "Clever as Dr. Ian Malcolm himself, remember that neglecting the intricate
> dance of data evolution in your modeling can turn your once-orderly systems
> into chaotic, dino-sized debacles, leaving you wishing for a T-Rex-sized
> umbrella!"
>
> -- Chat GPT after much cajolling, seemingly implying that a shit storm of
>    pain awaits you and your teamates in the event you misrepresent the
>    evolution of your program's data over time.

We often think of our current data types as the truth of our program,
disregarding previous attempts at modeling our domain.
But our programs evolve over time, they are organisms, and often there are more
than one instance of them in the wild.
Unlike organisms though, instances in the wild may be evolved or "ascended" to
the most current, and maybe our program's data types should encourage this ascension from
the get go, and maybe in doing so we save ourselves some trouble.

## Problem - What are we solving?

Data migrations are hard.
We often have to write explicit migrations in multiple languages (our app
language to manage migrations and the database language to do individual migrations).

It would be nice to migrate data automatically and have the migrations type
checked, that way we know if the program compiles the types can be migrated
successfully (and that migrations have been included).

## How

The approach here is to model our domain with types that are annotated with
their version, and then to wrap/alias the most recent version as the "current type":

```rust
pub struct PlayerV1 {
    pub name: String
}

pub struct PlayerV2 {
    pub name: String,
    pub age: u32
}

pub type PlayerCharacter = PlayerCharacterV2;
```

Each version of a type can be converted to either version adjacent to it, eg
version 1 can be converted to version 2 and version 2 can be converted back to
version 1 **without failure**:

```rust
pub struct PlayerV1 {
    pub name: String
}

impl From<PlayerV2> for PlayerV1 {
    fn from(value: PlayerV2) -> PlayerV1 {
        PlayerV1 {
            name: value.name
        }
    }
}

pub struct PlayerV2 {
    pub name: String,
    pub age: u32
}

impl From<PlayerV1> for PlayerV2 {
    fn from(value: PlayerV1) -> PlayerV2 {
        PlayerV2 {
            name: value.name,
            age: 0,
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

pub type Player = PlayerV3;
```

We can then forward migrate by constructing a type-path from the first version to the current one:

```rust
let migrations = Migrations::<PlayerV1>::default()
    .with_version::<PlayerV2>()
    .with_version::<Player>();
migrations.run(&connection).unwrap();
```

Or we can go backward by constructing a type-path from the current version to the first one:

```rust
let migrations = Migrations::<Player>::default()
    .with_version::<PlayerV2>()
    .with_version::<PlayerV1>();
migrations.run(&connection).unwrap();
```

There's no reason why the types we're migrating between have to be version of
the same type, either. You could use this same method to migration data from one table to
another that's only semi-related.

In this way I hope to move migrations from SQL to Rust, which is easier for me
to reason about because of the types and error handling that I know and love.

# What

> So then what is this repo?

This is a place to experiment with an implementation of this pattern, and if
necessary it is a Rust library containing a few traits and macros that help you
accomplish this pattern in your own project.

## Notes

I expect that the success of this pattern depends on the level of support for
various serialization targets, ie flat file vs json in a database.

> Is this an ORM?

I don't know! Maybe? Kinda.
