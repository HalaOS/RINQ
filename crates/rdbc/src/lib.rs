use std::{borrow::Cow, io::Result, task::Context};

use bigdecimal::BigDecimal;
use rasi::syscall::{CancelablePoll, Handle};

/// A variant type for sql
pub enum SqlValue<'a> {
    Bool(bool),
    Int(i64),
    BigInt(i128),
    Float(f64),
    #[cfg(feature = "with-decimal")]
    Decimal(BigDecimal),
    Binary(Cow<'a, [u8]>),
    String(Cow<'a, str>),
    Null,
}

/// This type contains the name and type of a column.
pub struct ColumnType<'a> {
    /// returns the database system name of the column type.
    pub database_type_name: Cow<'a, str>,

    /// returns the scale and precision of a decimal type, if applicable.
    pub decimal_size: Option<(i64, i64)>,

    /// Returns the column type length for variable length column types.
    /// If the type length is unbounded the value will be [`i64::MAX`] (any database limits will still apply).
    /// If the column type is not variable length, such as an int, or if not supported by the driver returns `None`.
    pub length: Option<i64>,

    /// Returns the name of alias of the column.
    pub name: Cow<'a, str>,

    /// reports whether the column may be null.
    ///
    /// If a driver does not support this property, returns `None`
    pub nullable: Option<bool>,
}

/// Represents database driver that can be shared between threads, and can therefore implement a connection pool
pub trait Database {
    /// Open a new database connection with `conn_info` and not block the calling thread.
    fn start_connect(&self, conn_info: &str) -> Result<Handle>;

    /// Poll [`start_connect`](Database::start_connect) op's result.
    fn poll_connect(&self, cx: &mut Context<'_>, handle: &Handle) -> CancelablePoll<Result<()>>;

    /// Starts a transaction via one connection. The default isolation level is dependent on the driver.
    fn begin(&self, conn: &Handle) -> Result<Handle>;

    /// Aborts the transaction.
    fn rollback(&self, tx: &Handle) -> Result<()>;

    /// Commits the transaction.
    fn commit(&self, tx: &Handle) -> Result<()>;

    /// asynchronously create a prepared statement for execution via a connectio or one transaction.
    ///
    /// On success, returns the prepared statement object that is not yet ready.
    /// you should call [`poll_prepare`] to asynchronously fetch the object status.
    fn start_prepare(&self, conn_or_tx: &Handle) -> Result<Handle>;

    /// Asynchronously fetch the [`start_prepare`](Database::start_prepare)'s calling result.
    fn poll_prepare(&self, cx: &mut Context<'_>, stmt: &Handle) -> CancelablePoll<Result<()>>;

    /// Execute a query that is expected to return a result set, such as a SELECT statement
    fn start_query(&self, stmt: &Handle, values: &[SqlValue<'_>]) -> Result<Handle>;

    /// Move the cursor to the next available row in the `ResultSet` created by [`start_query`](Database::start_query)
    /// if one exists and return true if it does
    fn poll_next(&self, cx: &mut Context<'_>, result_set: &Handle) -> CancelablePoll<Result<()>>;

    /// Execute a query that is expected to update some rows.
    fn start_exec(&self, stmt: &Handle, values: &[SqlValue<'_>]) -> Result<Handle>;

    /// Poll [`exec`](Database::start_exec) result.
    ///
    /// On success, returns the `last insert id` and `rows affected`.
    fn poll_exec(&self, result: &Handle) -> CancelablePoll<Result<(i64, i64)>>;

    /// Returns the column names.
    ///
    /// Returns error, if the `ResultSet` is closed.
    fn poll_cols(
        &self,
        cx: &mut Context<'_>,
        result_set: &Handle,
    ) -> CancelablePoll<Result<Vec<String>>>;

    /// Returns column information such as column type, length, and nullable. Some information may not be available from some drivers.
    fn poll_col_types(
        &self,
        cx: &mut Context<'_>,
        result_set: &Handle,
    ) -> CancelablePoll<Result<Vec<ColumnType<'static>>>>;
}
