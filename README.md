<div align="center">
  <h1>
    <img src="logo.png" alt="tymigrawr!" />
  </h1>
</div>

> Ah, my dear interlocutors, disregarding the prudent guardianship of data migrations
> is akin to unchaining a T-Rex upon our society, foretelling a realm of disorder
> and desolation.
>
> -- Not quite Socrates

# tymigrawr!

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

It would be nice to migrate data automatically and on demand (implicitly),
instead of explicitly all at once in one big transaction.

## How

The approach here is to model our domain with types that are annotated with
their version, and then to wrap the most recent version as the "current type":

```rust
pub struct PlayerV1 {
    pub name: String
}

pub struct PlayerV2 {
    pub name: String,
    pub age: u32
}

pub struct Player(pub PlayerCharacterV2);
```

Each version of a type can be converted to either version adjacent to it, eg
version 1 can be converted to version 2 and version 2 can be converted back to
version 1:

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

impl From<PlayerV1> for PlayerV2 {
    fn from(value: PlayerV1) -> PlayerV2 {
        PlayerV2 {
            name: value.name,
            age: 0,
        }
    }
}

pub struct PlayerV2 {
    pub name: String,
    pub age: u32
}

pub struct Player(pub PlayerV2);
```

We then attempt to deserialize this type from the data's serialized origin (whatever that
may be) by walking backwards through each version's deserialization code and
then moving the type forwards through each conversion to the most current type:

```rust
impl From<PlayerV2> for Player {
    fn from(value: PlayerV2) {
        Player(value)
    }
}

impl From<PlayerV1> for Player {
    fn from(value: PlayerV1) {
        Player(value.into())
    }
}
```

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
