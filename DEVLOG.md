# Devlog / Notes

## Sep 22 2023

### CRUD

The crud stuff isn't so bad. It's mostly taken care of by having types
implement a trait `CrudFields`, which provides the bulk of serialization /
deserialization. Implementing this trait could easily be turned into a derive
macro.

## Sep 21 2023

### CRUD

There's quite a bit of serialization / deserialization code. When reading from
sqlite for example, each type needs its own table if we're going to lazy-migrate.

This means each type needs CRUD helpers.

Writing these by hand each time a type version increments is a chore.

I think it would be smart to have some macros for each serialization "target",
but I'm wary of this repo turning into an ORM.
