# AGENTS.md

## Project Overview

**tymigrawr** is a Rust library for type-safe, versioned data migrations. Types are
suffixed with version numbers (e.g. `PlayerV1`, `PlayerV2`) and bidirectional `From`
impls allow automatic forward/reverse migration between database backends.

Workspace layout:
- `crates/tymigrawr` — core library + binary stub (lib.rs, main.rs, backend modules)
- `crates/tymigrawr-derive` — proc-macro crate providing `#[derive(HasCrudFields)]`

Rust edition: 2021. No CI/CD, no rustfmt.toml, no clippy.toml — all defaults apply.

## Build / Lint / Test Commands

### Build

```sh
cargo build                                    # build entire workspace (default feature: sqlite)
cargo build -p tymigrawr --no-default-features # build with no backends
cargo build -p tymigrawr-derive                # derive crate only
```

### Lint & Format

```sh
cargo fmt --all                    # format entire workspace
cargo fmt --all -- --check         # check formatting without writing
cargo clippy --workspace           # lint entire workspace
cargo clippy -p tymigrawr          # lint core crate only
cargo clippy -p tymigrawr-derive   # lint derive crate only
```

### Test

```sh
cargo test --workspace                         # run all tests
cargo test -p tymigrawr                        # run tests in core crate only
cargo test -p tymigrawr -- p1_crud             # run a single test by name
cargo test -p tymigrawr -- migrate             # run the migration test
RUST_LOG=trace cargo test -p tymigrawr -- --nocapture   # run with log output visible
```

All tests live in a `#[cfg(test)] mod test` block at the bottom of
`crates/tymigrawr/src/lib.rs`. There are no integration tests or tests in the derive
crate. Tests use in-memory SQLite (`sqlite::open(":memory:")`) and `tempfile` for
on-disk databases.

## Architecture

### Trait Hierarchy

1. **`IsCrudField`** — implemented by primitive column types (`i64`, `f64`, `String`,
   `Vec<u8>`, `Option<T>`). Converts between Rust values and the `Value` enum.
2. **`HasCrudFields`** — derivable via `#[derive(HasCrudFields)]`. Describes a struct's
   table name, column schema, and primary key. Mark one field with `#[primary_key]`.
3. **`Crud<Backend>`** — blanket-implemented for all `T: HasCrudFields + Clone + 'static`.
   Provides `create`, `insert`, `read_all`, `read_where`, `read`, `update`, `delete`,
   and `migration()` methods.
4. **`MigrateEntireTable`** — backend-specific trait for bulk migration operations.
   Implemented by `Sqlite` marker struct.

### Backend Pattern

Backends are feature-gated unit structs (currently only `Sqlite`) with
`#[cfg(feature = "backend_sqlite")]`. The `Crud` and `MigrateEntireTable` traits use a
GAT `Connection<'a>` so each backend defines its own connection type.

### Migration Pattern

`Migrations<T, Backend>` is a builder that chains `.with_version::<NextType>()` calls.
Each step is type-erased into a `Migration` struct holding boxed closures. Call
`.run(&connection)` to execute forward or reverse migrations depending on chain order.

## Code Style Guidelines

### Imports

Group and nest `use` statements in this order, separated by blank lines:

1. `std` — nested, alphabetically sorted
2. External crates — one `use` per crate (or nested for multiple items from one crate)
3. Crate-internal (`crate::`) — nested, alphabetically sorted

```rust
use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
};

use snafu::prelude::*;

use crate::{
    Crud, CrudField, HasCrudFields, IsCrudField, MigrateEntireTable, Migration, Value, ValueType,
};
```

### Formatting

Default `rustfmt` settings (max width 100). Always use trailing commas in struct fields,
enum variants, function parameters, match arms, and macro invocations. Opening braces on
the same line (K&R / Rust default).

### Naming Conventions

| Element              | Convention     | Examples                                    |
|----------------------|----------------|---------------------------------------------|
| Types / Traits       | `PascalCase`   | `CrudField`, `HasCrudFields`, `ValueType`   |
| Functions / Methods  | `snake_case`   | `crud_fields`, `read_all`, `into_value`     |
| Variables            | `snake_case`   | `table_name`, `key_value`, `field_idents`   |
| Modules              | `snake_case`   | `backend_sqlite`                            |
| Versioned types      | Suffix `V<N>`  | `PlayerV1`, `PlayerV2`, `PlayerV3`          |
| Type aliases         | `PascalCase`   | `pub type Player = PlayerV3;`               |
| Generic params       | Short/caps     | `T`, `S`, `Key`, `Next`, `Backend`          |

### Error Handling

This project uses **`snafu ^0.7`** with the `Whatever` catch-all error type — no custom
error enums are defined.

- Return `Result<..., snafu::Whatever>` from public trait methods (fully qualified path).
- Use `.whatever_context("descriptive message")` on `Result` and `Option`.
- Use `snafu::ensure_whatever!(condition, "message")` for assertions.
- Use `snafu::whatever!("message")` for immediate error returns.
- Import via `use snafu::prelude::*;` or selectively `use snafu::{OptionExt, ResultExt};`.

### Derives and Attributes

- Structs that map to database tables derive `Debug, Clone, PartialEq, HasCrudFields`.
- Mark exactly one field with `#[primary_key]` when deriving `HasCrudFields`.
- Backend modules use `#[cfg(feature = "backend_sqlite")]`.
- The derive macro generates `#[automatically_derived] impl HasCrudFields for ...` blocks.

### Type Patterns

- Use GATs (`type Connection<'a>`) for backend-specific associated types.
- Use `PhantomData` for unused generic parameters in structs.
- Use `impl Trait` in function parameters for flexibility.
- Use `Box<dyn ...>` for type-erased closures/iterators in `Migration`.
- Place generic bounds on `impl` blocks, not on trait definitions.

### Comments

- Comments are sparse; let types and names convey intent.
- `//!` module-level doc comments: one-line description at top of backend files.
- `///` doc comments on public trait methods.
- `// SAFETY:` comments to justify unsafe downcasts.
- Avoid leaving commented-out code in non-test modules.

## Known Issues

1. **Unused dependencies**: `serde` and `serde_json` are declared in Cargo.toml but
   never used in source code (likely reserved for future serialization work).
