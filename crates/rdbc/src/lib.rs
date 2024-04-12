use std::{
    borrow::Cow,
    collections::HashMap,
    io::{self, Result},
    sync::{Arc, OnceLock, RwLock},
    task::Context,
};

use bigdecimal::BigDecimal;
use negative_impl::negative_impl;
use rasi::{
    syscall::{CancelablePoll, Handle},
    utils::cancelable_would_block,
};

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
pub trait Database: Send + Sync {
    /// Open a new database connection with `source_name` and not block the calling thread.
    fn start_connect(&self, source_name: &str) -> Result<Handle>;

    /// Poll [`start_connect`](Database::start_connect) op's result.
    fn poll_connect(&self, cx: &mut Context<'_>, handle: &Handle) -> CancelablePoll<Result<()>>;

    /// Starts a transaction via one connection. The default isolation level is dependent on the driver.
    fn begin(&self, cx: &mut Context<'_>, conn: &Handle) -> CancelablePoll<Result<Handle>>;

    /// Aborts the transaction.
    fn rollback(&self, cx: &mut Context<'_>, tx: &Handle) -> CancelablePoll<Result<()>>;

    /// Commits the transaction.
    fn commit(&self, cx: &mut Context<'_>, tx: &Handle) -> CancelablePoll<Result<()>>;

    /// asynchronously create a prepared statement for execution via a connectio or one transaction.
    ///
    /// On success, returns the prepared statement object that is not yet ready.
    /// you should call [`poll_prepare`] to asynchronously fetch the object status.
    fn start_prepare(&self, conn_or_tx: &Handle, query: &str) -> Result<Handle>;

    /// Asynchronously fetch the [`start_prepare`](Database::start_prepare)'s calling result.
    fn poll_prepare(&self, cx: &mut Context<'_>, stmt: &Handle) -> CancelablePoll<Result<()>>;

    /// Execute a query that is expected to return a result set, such as a SELECT statement
    fn start_query(&self, stmt: &Handle, values: &[SqlValue<'_>]) -> Result<Handle>;

    /// Move the cursor to the next available row in the `ResultSet` created by [`start_query`](Database::start_query)
    /// if one exists and return true if it does
    fn poll_next(&self, cx: &mut Context<'_>, result_set: &Handle) -> CancelablePoll<Result<bool>>;

    /// Returns a single field value of current row.
    fn poll_value(
        &self,
        cx: &mut Context<'_>,
        result_set: &Handle,
        col_num: usize,
    ) -> CancelablePoll<Result<SqlValue<'static>>>;

    /// Execute a query that is expected to update some rows.
    fn start_exec(&self, stmt: &Handle, values: &[SqlValue<'_>]) -> Result<Handle>;

    /// Poll [`exec`](Database::start_exec) result.
    ///
    /// On success, returns the `last insert id` and `rows affected`.
    fn poll_exec(
        &self,
        cx: &mut Context<'_>,
        result: &Handle,
    ) -> CancelablePoll<Result<(i64, i64)>>;

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

/// Represents a database connection.
pub struct DbConn {
    conn: Handle,
    database: Arc<Box<dyn Database>>,
}

impl DbConn {
    /// Creates a prepared statement for later queries or executions.
    pub async fn prepare<Q: AsRef<str>>(&self, query: Q) -> Result<Stmt> {
        let stmt_handle = self.database.start_prepare(&self.conn, query.as_ref())?;

        cancelable_would_block(|cx| self.database.poll_prepare(cx, &stmt_handle)).await?;

        Ok(Stmt {
            stmt_handle,
            database: self.database.clone(),
        })
    }

    /// Starts a transaction.
    pub async fn begin(&self) -> Result<Tx> {
        cancelable_would_block(|cx| self.database.begin(cx, &self.conn))
            .await
            .map(|tx_handle| Tx {
                tx_handle,
                database: self.database.clone(),
            })
    }
}

/// Tx is an in-progress database transaction.
pub struct Tx {
    tx_handle: Handle,
    database: Arc<Box<dyn Database>>,
}

impl Tx {
    /// Creates a prepared statement for later queries or executions.
    pub async fn prepare<Q: AsRef<str>>(&self, query: Q) -> Result<Stmt> {
        let stmt_handle = self
            .database
            .start_prepare(&self.tx_handle, query.as_ref())?;

        cancelable_would_block(|cx| self.database.poll_prepare(cx, &stmt_handle)).await?;

        Ok(Stmt {
            stmt_handle,
            database: self.database.clone(),
        })
    }

    /// Manual commits the transaction.
    pub async fn commit(&self) -> Result<()> {
        cancelable_would_block(|cx| self.database.commit(cx, &self.tx_handle)).await
    }

    /// Manual rollback the transaction.
    pub async fn rollback(&self) -> Result<()> {
        cancelable_would_block(|cx| self.database.rollback(cx, &self.tx_handle)).await
    }
}

/// Represents a prepared statement.
pub struct Stmt {
    stmt_handle: Handle,
    database: Arc<Box<dyn Database>>,
}

impl Stmt {
    /// executes a prepared query statement with the given arguments and returns the query results.
    pub async fn query(&self, values: &[SqlValue<'_>]) -> Result<ResultSet> {
        let result_set_handle = self.database.start_query(&self.stmt_handle, values)?;

        Ok(ResultSet {
            result_set_handle,
            database: self.database.clone(),
        })
    }

    /// Executes a prepared statement with the given arguments.
    ///
    /// On success, returns the `last_insert_id` and `rows_affected`.
    pub async fn exec(&self, values: &[SqlValue<'_>]) -> Result<(i64, i64)> {
        let result_handle = self.database.start_exec(&self.stmt_handle, values)?;

        cancelable_would_block(|cx| self.database.poll_exec(cx, &result_handle)).await
    }
}

/// Represents a query result set.
pub struct ResultSet {
    result_set_handle: Handle,
    database: Arc<Box<dyn Database>>,
}

#[negative_impl]
impl !Send for ResultSet {}

#[negative_impl]
impl !Sync for ResultSet {}

impl ResultSet {
    /// Returns the column names
    pub async fn columns(&self) -> Result<Vec<String>> {
        cancelable_would_block(|cx| self.database.poll_cols(cx, &self.result_set_handle)).await
    }

    /// Returns column information such as column type, length, and nullable
    pub async fn column_types(&self) -> Result<Vec<ColumnType<'static>>> {
        cancelable_would_block(|cx| self.database.poll_col_types(cx, &self.result_set_handle)).await
    }

    /// prepares the next result row for reading
    pub async fn next(&self) -> Result<bool> {
        cancelable_would_block(|cx| self.database.poll_next(cx, &self.result_set_handle)).await
    }

    /// Get column value by col number.
    pub async fn get(&self, col: usize) -> Result<SqlValue<'static>> {
        cancelable_would_block(|cx| self.database.poll_value(cx, &self.result_set_handle, col))
            .await
    }

    /// Get col value by col name or col alias.
    pub async fn get_by_col_name<C: AsRef<str>>(
        &self,
        col_name: C,
        col_types: &[ColumnType<'_>],
    ) -> Result<SqlValue<'static>> {
        let col_name = col_name.as_ref();

        let offset = col_types
            .iter()
            .enumerate()
            .find_map(|(offset, col)| {
                if col.name == col_name {
                    Some(offset)
                } else {
                    None
                }
            })
            .ok_or(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Unknown column name or alias: {}", col_name),
            ))?;

        self.get(offset).await
    }
}

#[derive(Default)]
struct GlobalRegister {
    drivers: RwLock<HashMap<String, Arc<Box<dyn Database>>>>,
}

static REGISTER: OnceLock<GlobalRegister> = OnceLock::new();

fn get_register() -> &'static GlobalRegister {
    REGISTER.get_or_init(|| Default::default())
}

/// Open opens a database specified by its database driver name and a driver-specific data source name, usually consisting of at least a database name and connection information.
pub async fn open<D: AsRef<str>, S: AsRef<str>>(driver_name: D, source_name: S) -> Result<DbConn> {
    let drivers = get_register()
        .drivers
        .read()
        .map_err(|err| io::Error::new(io::ErrorKind::Interrupted, err.to_string()))?;

    if let Some(database) = drivers.get(driver_name.as_ref()) {
        let conn = database.start_connect(source_name.as_ref())?;

        cancelable_would_block(|cx| database.poll_connect(cx, &conn)).await?;

        return Ok(DbConn {
            conn,
            database: database.clone(),
        });
    } else {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("Unknown driver: {}", driver_name.as_ref()),
        ));
    }
}

/// Register new database driver.
///
/// Cause a panic, if register same driver name twice.
pub fn register<N: AsRef<str>, D: Database + 'static>(driver_name: N, database: D) -> Result<()> {
    let mut drivers = get_register()
        .drivers
        .write()
        .map_err(|err| io::Error::new(io::ErrorKind::Interrupted, err.to_string()))?;

    assert!(
        drivers
            .insert(
                driver_name.as_ref().to_owned(),
                Arc::new(Box::new(database))
            )
            .is_none(),
        "register driver twice: {}",
        driver_name.as_ref(),
    );

    todo!()
}
