# Devlog / Notes

## Sep 23 2023

### It Works!

Forward and reverse migrations are working like a charm and are relatively
snappy when writing to disk via sqlite3 even without any optimisation.

### Multiple Backends

I'm dropping support for autoincrementing keys, at least for now. I like the
idea of supporting multiple backends more than I like the idea of this library
becoming an ORM - and I think having to know/generate a key ahead of time is
perfectly fine.

Multiple backends is an interesting feature because I feel like I could easily
support folks moving from one backend to another, eg moving from sqlite to yaml
files, and it could be set up like any other "version" update. This actually
could be one of the bigger "selling" points of the library.

Anyway - I'm still proving the initial theory that we can make migrations easy
and do them in Rust and it would kill some pain.

## Sep 22 2023

### CRUD

The crud stuff isn't so bad. It's mostly taken care of by having types
implement a trait `CrudFields`, which provides the bulk of serialization /
deserialization. Implementing this trait could easily be turned into a derive
macro.

### Migration

Now down to the meat of problem. Lazy migrations are conceptually hard because
a type may have any key (maybe even autoincrementing). It's hard to know ahead
of time what the key field is without filtering a map of the fields. This makes
migrating all at once attractive, because you could just select everything
and not worry about the keys. But then you're not gaining anything! In general
autoincrementing primary keys mess up this pattern because there are multiple
tables for each "type", one for each version, and migration means moving an entry
from one table to the other by first serializing it into the program and then
saving it. If the key is auto-generated (because it's autoincrementing) by the
insert operation then at least on sqlite we can't include the key during the
insert because it would clobber the autoincrement. But on migrate we want to do
exactly that. I think this means we'd have to have two insert operations - or
alternatively we could **not support autoincrementing keys**.

## Sep 21 2023

### CRUD

There's quite a bit of serialization / deserialization code. When reading from
sqlite for example, each type needs its own table if we're going to lazy-migrate.

This means each type needs CRUD helpers.

Writing these by hand each time a type version increments is a chore.

I think it would be smart to have some macros for each serialization "target",
but I'm wary of this repo turning into an ORM.
