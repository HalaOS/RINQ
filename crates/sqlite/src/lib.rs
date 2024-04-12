use std::{
    ffi::{c_void, CStr, CString},
    io,
    ptr::null_mut,
    str::from_utf8_unchecked,
    sync::Arc,
};

use rasi::syscall::{CancelablePoll, Handle};
use rinq_rdbc::Database;
use sqlite3_sys::{
    sqlite3_bind_blob, sqlite3_bind_double, sqlite3_bind_int, sqlite3_bind_int64,
    sqlite3_bind_null, sqlite3_bind_text, sqlite3_close, sqlite3_errcode, sqlite3_errmsg,
    sqlite3_exec, sqlite3_finalize, sqlite3_open_v2, sqlite3_prepare_v2, sqlite3_stmt, SQLITE_OK,
    SQLITE_OPEN_FULLMUTEX, SQLITE_OPEN_URI, SQLITE_TRANSIENT,
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

struct Sqlite3Handle {
    c_handle: *mut sqlite3_sys::sqlite3,
}

unsafe impl Send for Sqlite3Handle {}
unsafe impl Sync for Sqlite3Handle {}

impl Drop for Sqlite3Handle {
    fn drop(&mut self) {
        unsafe {
            sqlite3_close(self.c_handle);
        }
    }
}

struct SqliteConn {
    handle: Arc<Sqlite3Handle>,
}

impl SqliteConn {
    fn new(c_handle: *mut sqlite3_sys::sqlite3) -> Self {
        Self {
            handle: Arc::new(Sqlite3Handle { c_handle }),
        }
    }

    unsafe fn to_c_handle(&self) -> *mut sqlite3_sys::sqlite3 {
        self.handle.c_handle
    }
}

struct SqliteTx {
    handle: Arc<Sqlite3Handle>,
}

impl SqliteTx {
    unsafe fn to_c_handle(&self) -> *mut sqlite3_sys::sqlite3 {
        self.handle.c_handle
    }
}

struct SqliteStmt {
    c_stmt: *mut sqlite3_stmt,
    #[allow(unused)]
    handle: Arc<Sqlite3Handle>,
}

unsafe impl Send for SqliteStmt {}
unsafe impl Sync for SqliteStmt {}

impl Drop for SqliteStmt {
    fn drop(&mut self) {
        unsafe {
            sqlite3_finalize(self.c_stmt);
        }
    }
}

struct SqliteResultSet(*mut sqlite3_stmt);

unsafe impl Send for SqliteResultSet {}
unsafe impl Sync for SqliteResultSet {}

/// Database driver for `Sqlite`
pub struct SqliteDriver {}

impl SqliteDriver {
    fn bind_values(&self, stmt: &SqliteStmt, values: &[rinq_rdbc::SqlValue<'_>]) -> io::Result<()> {
        for (index, value) in values.iter().enumerate() {
            let offset = index as i32 + 1;
            unsafe {
                let rc = match value {
                    rinq_rdbc::SqlValue::Bool(value) => {
                        sqlite3_bind_int(stmt.c_stmt, offset, if *value { 1 } else { 0 })
                    }
                    rinq_rdbc::SqlValue::Int(value) => {
                        sqlite3_bind_int64(stmt.c_stmt, offset, *value)
                    }
                    rinq_rdbc::SqlValue::BigInt(value) => {
                        let cstr = CString::new(value.to_string())?;
                        sqlite3_bind_text(
                            stmt.c_stmt,
                            offset,
                            cstr.as_ptr(),
                            -1,
                            Some(std::mem::transmute(SQLITE_TRANSIENT as *const c_void)),
                        )
                    }
                    rinq_rdbc::SqlValue::Float(value) => {
                        sqlite3_bind_double(stmt.c_stmt, offset, *value)
                    }
                    rinq_rdbc::SqlValue::Decimal(value) => {
                        let cstr = CString::new(value.to_string())?;
                        sqlite3_bind_text(
                            stmt.c_stmt,
                            offset,
                            cstr.as_ptr(),
                            -1,
                            Some(std::mem::transmute(SQLITE_TRANSIENT as *const c_void)),
                        )
                    }
                    rinq_rdbc::SqlValue::Binary(data) => sqlite3_bind_blob(
                        stmt.c_stmt,
                        offset,
                        data.as_ptr() as *const _,
                        data.len() as i32,
                        Some(std::mem::transmute(SQLITE_TRANSIENT as *const c_void)),
                    ),
                    rinq_rdbc::SqlValue::String(value) => {
                        let cstr = CString::new(value.as_ref())?;
                        sqlite3_bind_text(
                            stmt.c_stmt,
                            offset,
                            cstr.as_ptr(),
                            -1,
                            Some(std::mem::transmute(SQLITE_TRANSIENT as *const c_void)),
                        )
                    }
                    rinq_rdbc::SqlValue::Null => sqlite3_bind_null(stmt.c_stmt, offset),
                };

                if rc != SQLITE_OK {
                    return Err(to_io_error(stmt.handle.c_handle));
                }
            }
        }

        Ok(())
    }
}

#[allow(unused)]
impl Database for SqliteDriver {
    fn start_connect(&self, source_name: &str) -> std::io::Result<rasi::syscall::Handle> {
        let mut db: *mut sqlite3_sys::sqlite3 = null_mut();

        unsafe {
            let rc = sqlite3_open_v2(
                CString::new(source_name)?.as_ptr(),
                &mut db,
                SQLITE_OPEN_URI | SQLITE_OPEN_FULLMUTEX,
                null_mut(),
            );

            if rc != SQLITE_OK {
                return Err(to_io_error(db));
            }
        }

        Ok(Handle::new(SqliteConn::new(db)))
    }

    fn poll_connect(
        &self,
        _cx: &mut std::task::Context<'_>,
        _handle: &rasi::syscall::Handle,
    ) -> rasi::syscall::CancelablePoll<std::io::Result<()>> {
        CancelablePoll::Ready(Ok(()))
    }

    fn begin(
        &self,
        _cx: &mut std::task::Context<'_>,
        conn: &Handle,
    ) -> CancelablePoll<std::io::Result<Handle>> {
        static SQL: &CStr = c"BEGIN TRANSACTION;";

        let conn = conn
            .downcast::<SqliteConn>()
            .expect("Expect sqlite3 connection");

        unsafe {
            let sqlite3 = conn.to_c_handle();

            let rc = sqlite3_exec(sqlite3, SQL.as_ptr(), None, null_mut(), null_mut());

            if rc != SQLITE_OK {
                return CancelablePoll::Ready(Err(to_io_error(sqlite3)));
            }
        }

        CancelablePoll::Ready(Ok(Handle::new(SqliteTx {
            handle: conn.handle.clone(),
        })))
    }

    fn rollback(
        &self,
        cx: &mut std::task::Context<'_>,
        tx: &rasi::syscall::Handle,
    ) -> rasi::syscall::CancelablePoll<std::io::Result<()>> {
        static SQL: &CStr = c"ROLLBACK TRANSACTION;";

        let conn = tx.downcast::<SqliteTx>().expect("Expect sqlite3 tx");

        unsafe {
            let sqlite3 = conn.to_c_handle();

            let rc = sqlite3_exec(sqlite3, SQL.as_ptr(), None, null_mut(), null_mut());

            if rc != SQLITE_OK {
                return CancelablePoll::Ready(Err(to_io_error(sqlite3)));
            }
        }

        CancelablePoll::Ready(Ok(()))
    }

    fn commit(
        &self,
        cx: &mut std::task::Context<'_>,
        tx: &rasi::syscall::Handle,
    ) -> rasi::syscall::CancelablePoll<std::io::Result<()>> {
        static SQL: &CStr = c"COMMIT TRANSACTION;";

        let conn = tx.downcast::<SqliteTx>().expect("Expect sqlite3 tx");

        unsafe {
            let sqlite3 = conn.to_c_handle();

            let rc = sqlite3_exec(sqlite3, SQL.as_ptr(), None, null_mut(), null_mut());

            if rc != SQLITE_OK {
                return CancelablePoll::Ready(Err(to_io_error(sqlite3)));
            }
        }

        CancelablePoll::Ready(Ok(()))
    }

    fn start_prepare(
        &self,
        conn_or_tx: &rasi::syscall::Handle,
        query: &str,
    ) -> std::io::Result<rasi::syscall::Handle> {
        let (sqlite3, handle) = if let Some(conn) = conn_or_tx.downcast::<SqliteConn>() {
            (unsafe { conn.to_c_handle() }, conn.handle.clone())
        } else if let Some(tx) = conn_or_tx.downcast::<SqliteTx>() {
            (unsafe { tx.to_c_handle() }, tx.handle.clone())
        } else {
            panic!("Expect sqlite connection or tx");
        };

        let query = CString::new(query)?;

        let mut c_stmt: *mut sqlite3_stmt = null_mut();

        unsafe {
            let rc = sqlite3_prepare_v2(sqlite3, query.as_ptr(), -1, &mut c_stmt, null_mut());

            if rc != SQLITE_OK {
                return Err(to_io_error(sqlite3));
            }
        }

        Ok(Handle::new(SqliteStmt { handle, c_stmt }))
    }

    fn poll_prepare(
        &self,
        cx: &mut std::task::Context<'_>,
        stmt: &rasi::syscall::Handle,
    ) -> rasi::syscall::CancelablePoll<std::io::Result<()>> {
        CancelablePoll::Ready(Ok(()))
    }

    fn start_query(
        &self,
        stmt: &rasi::syscall::Handle,
        values: &[rinq_rdbc::SqlValue<'_>],
    ) -> std::io::Result<rasi::syscall::Handle> {
        let stmt = stmt.downcast::<SqliteStmt>().expect("Expect sqlite stmt");

        self.bind_values(stmt, values)?;

        Ok(Handle::new(SqliteResultSet(stmt.c_stmt)))
    }

    fn poll_next(
        &self,
        cx: &mut std::task::Context<'_>,
        result_set: &rasi::syscall::Handle,
    ) -> rasi::syscall::CancelablePoll<std::io::Result<bool>> {
        todo!()
    }

    fn poll_value(
        &self,
        cx: &mut std::task::Context<'_>,
        result_set: &rasi::syscall::Handle,
        col_num: usize,
    ) -> rasi::syscall::CancelablePoll<std::io::Result<rinq_rdbc::SqlValue<'static>>> {
        todo!()
    }

    fn start_exec(
        &self,
        stmt: &rasi::syscall::Handle,
        values: &[rinq_rdbc::SqlValue<'_>],
    ) -> std::io::Result<rasi::syscall::Handle> {
        let stmt = stmt.downcast::<SqliteStmt>().expect("Expect sqlite stmt");

        self.bind_values(stmt, values)?;

        todo!()
    }

    fn poll_exec(
        &self,
        cx: &mut std::task::Context<'_>,
        result: &rasi::syscall::Handle,
    ) -> rasi::syscall::CancelablePoll<std::io::Result<(i64, i64)>> {
        todo!()
    }

    fn poll_cols(
        &self,
        cx: &mut std::task::Context<'_>,
        result_set: &rasi::syscall::Handle,
    ) -> rasi::syscall::CancelablePoll<std::io::Result<Vec<String>>> {
        todo!()
    }

    fn poll_col_types(
        &self,
        cx: &mut std::task::Context<'_>,
        result_set: &rasi::syscall::Handle,
    ) -> rasi::syscall::CancelablePoll<std::io::Result<Vec<rinq_rdbc::ColumnType<'static>>>> {
        todo!()
    }
}
