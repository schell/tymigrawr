# Devlog / Notes

## Sep 21 2023

### CRUD

There's quite a bit of serialization / deserialization code. When reading from
sqlite for example, each type needs its own table if we're going to lazy-migrate.

This means each type needs CRUD helpers.

Writing these by hand each time a type version increments is a chore.

I think it would be smart to have some macros for each serialization "target",
but I'm wary of this repo turning into an ORM.
