use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use tokio_util::sync::CancellationToken;

use crate::bridge::cancel_asyncio_task;
use crate::journal::Journal;

/// Submitted work. Await this object to get the Python coroutine's result.
#[pyclass(frozen)]
pub struct Handle {
    awaitable: Py<PyAny>,
    task_id: u64,
    journal: Arc<Mutex<Journal>>,
    cancel: CancellationToken,
    event_loop: Py<PyAny>,
    py_task: Arc<Mutex<Option<Py<PyAny>>>>,
}

impl Handle {
    pub fn new(
        awaitable: Py<PyAny>,
        task_id: u64,
        journal: Arc<Mutex<Journal>>,
        cancel: CancellationToken,
        event_loop: Py<PyAny>,
        py_task: Arc<Mutex<Option<Py<PyAny>>>>,
    ) -> Self {
        Self {
            awaitable,
            task_id,
            journal,
            cancel,
            event_loop,
            py_task,
        }
    }
}

#[pymethods]
impl Handle {
    fn __await__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.awaitable.bind(py).call_method0("__await__")
    }

    /// Cancel this task: Tokio token and the asyncio Task, if it has started.
    fn cancel(&self, py: Python<'_>) -> PyResult<()> {
        self.cancel.cancel();
        if let Some(task) = self.py_task.lock().ok().and_then(|guard| {
            guard.as_ref().map(|task| task.clone_ref(py))
        }) {
            cancel_asyncio_task(&self.event_loop.bind(py), &task.bind(py))?;
        }
        Ok(())
    }

    fn cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// `queued`, `running`, `succeeded`, `failed`, `cancelled`, or `timed_out`.
    fn status(&self) -> String {
        self.journal
            .lock()
            .map(|j| j.get(self.task_id).map(|s| s.as_str().to_string()))
            .ok()
            .flatten()
            .unwrap_or_else(|| "unknown".to_string())
    }

    fn done(&self) -> bool {
        matches!(
            self.status().as_str(),
            "succeeded" | "failed" | "cancelled" | "timed_out"
        )
    }
}
