# RDBC

A lightweight database facade for rust programing.

The RDBC (Rust DataBase Connectivity) crate provides a single `database` API that abstracts over the actual database client implementation.
Libraries can use the `RDBC` API provided by this crate, and the consumer of those libraries can choose the database implementation that is most suitable for its use case.
