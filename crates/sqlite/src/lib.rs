use std::{
    ffi::{CStr, CString},
    io,
    ops::Deref,
    ptr::null_mut,
    str::from_utf8_unchecked,
    sync::Arc,
};

use sqlite3_sys as ffi;

use ffi::{
    sqlite3_errcode, sqlite3_errmsg, sqlite3_prepare_v2, SQLITE_OK, SQLITE_OPEN_CREATE,
    SQLITE_OPEN_FULLMUTEX, SQLITE_OPEN_READWRITE, SQLITE_OPEN_URI,
};

unsafe fn to_io_error(db: *mut sqlite3_sys::sqlite3) -> io::Error {
    io::Error::new(
        io::ErrorKind::Other,
        format!(
            "sqlite3: code={}, error={}",
            sqlite3_errcode(db),
            from_utf8_unchecked(CStr::from_ptr(sqlite3_errmsg(db)).to_bytes())
        ),
    )
}

/// A type safe wrapper of c sqlite connection.
struct RawConn(*mut ffi::sqlite3);

impl Drop for RawConn {
    fn drop(&mut self) {
        unsafe {
            ffi::sqlite3_close(self.0);
        }
    }
}

/// Safety: open sqlite with `SQLITE_OPEN_FULLMUTEX` flag.
///
/// The new database connection will use the "serialized" threading mode.
/// This means the multiple threads can safely attempt to use the same
/// database connection at the same time. (Mutexes will block any
/// actual concurrency, but in this mode there is no harm in trying.)
unsafe impl Send for RawConn {}
unsafe impl Sync for RawConn {}

impl RawConn {
    /// Create new sqlite connection with `source_name`.
    fn new(source_name: &str) -> io::Result<Self> {
        let mut db = null_mut();
        unsafe {
            let rc = sqlite3_sys::sqlite3_open_v2(
                CString::new(source_name)?.as_ptr(),
                &mut db,
                SQLITE_OPEN_CREATE
                    | SQLITE_OPEN_READWRITE
                    | SQLITE_OPEN_URI
                    | SQLITE_OPEN_FULLMUTEX,
                null_mut(),
            );

            if rc != SQLITE_OK {
                return Err(to_io_error(db));
            }
        }

        Ok(RawConn(db))
    }
}

/// sqlite3_stmt wrapper type with `Drop` trait implementation.
struct RawStmt(*mut ffi::sqlite3_stmt);

/// Safety: The RINQ framework has prohibited auto trait [`Sync`].
unsafe impl Send for RawStmt {}
unsafe impl Sync for RawStmt {}

impl Drop for RawStmt {
    fn drop(&mut self) {
        unsafe {
            ffi::sqlite3_finalize(self.0);
        }
    }
}

/// Sqlite db connection with [`Clone`] trait implementation.
#[derive(Clone)]
struct DbConn {
    raw: Arc<RawConn>,
}

impl DbConn {
    /// Create new sqlite connection with `source_name`.
    fn new(source_name: &str) -> io::Result<Self> {
        Ok(Self {
            raw: Arc::new(RawConn::new(source_name)?),
        })
    }

    fn to_c_handle(&self) -> *mut ffi::sqlite3 {
        self.raw.0
    }

    /// Execute provided `sql` with `sqlite3_exec` function.
    fn exec(&self, sql: &CStr) -> io::Result<()> {
        unsafe {
            let rc = ffi::sqlite3_exec(
                self.to_c_handle(),
                sql.as_ptr(),
                None,
                null_mut(),
                null_mut(),
            );

            if rc != SQLITE_OK {
                return Err(to_io_error(self.to_c_handle()));
            }
        }

        Ok(())
    }

    /// using `sqlite3_prepare_v2` to compile sql and create `Prepared Statement Object`
    fn prepare(&self, query: &CStr) -> io::Result<DbStmt> {
        let mut c_stmt = null_mut();

        unsafe {
            let rc = sqlite3_prepare_v2(
                self.to_c_handle(),
                query.as_ptr(),
                -1,
                &mut c_stmt,
                null_mut(),
            );

            if rc != SQLITE_OK {
                return Err(to_io_error(self.to_c_handle()));
            }
        }

        Ok(DbStmt {
            raw: RawStmt(c_stmt),
            conn: self.clone(),
        })
    }
}

struct DbStmt {
    raw: RawStmt,
    conn: DbConn,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_private() {
        // temporary in-memory database
        RawConn::new(":memory:").unwrap();
        // temporary on-disk database
        RawConn::new("").unwrap();
    }

    #[test]
    fn test_exec() {
        let conn = DbConn::new("").unwrap();

        // start a transaction.
        conn.exec(c"BEGIN TRANSACTION;").unwrap();
        // commit a transaction.
        conn.exec(c"END TRANSACTION;").unwrap();
    }
}
