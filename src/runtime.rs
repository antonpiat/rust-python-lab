use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3_async_runtimes::tokio::{future_into_py_with_locals, get_current_locals, get_runtime};

use crate::handle::Handle;
use crate::journal::Journal;

struct Inner {
    journal: Arc<Mutex<Journal>>,
    next_id: AtomicU64,
}

/// Tokio-backed execution controller. Python coroutine bodies still run on asyncio.
#[pyclass(frozen)]
pub struct Runtime {
    inner: Arc<Inner>,
}

#[pymethods]
impl Runtime {
    #[new]
    fn new() -> Self {
        let _ = get_runtime();
        Self {
            inner: Arc::new(Inner {
                journal: Arc::new(Mutex::new(Journal::default())),
                next_id: AtomicU64::new(1),
            }),
        }
    }

    /// Submit an async callable. Returns a Handle; await it for the result.
    ///
    /// The callable is invoked immediately on the current asyncio thread so the
    /// coroutine object is created there. Tokio waits on the bridged future.
    #[pyo3(signature = (func, *args))]
    fn submit(
        &self,
        py: Python<'_>,
        func: Bound<'_, PyAny>,
        args: Bound<'_, PyTuple>,
    ) -> PyResult<Handle> {
        let locals = get_current_locals(py)?;
        let coro = func.call(args, None)?;
        let rust_fut = pyo3_async_runtimes::into_future_with_locals(&locals, coro)?;

        let task_id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner
            .journal
            .lock()
            .expect("journal mutex")
            .insert_running(task_id);

        let journal = Arc::clone(&self.inner.journal);
        let awaitable = future_into_py_with_locals(py, locals, async move {
            let result = rust_fut.await;
            if let Ok(mut journal) = journal.lock() {
                journal.finish(task_id, result.is_ok());
            }
            result
        })?;

        Ok(Handle::new(
            awaitable.unbind(),
            task_id,
            Arc::clone(&self.inner.journal),
        ))
    }
}
