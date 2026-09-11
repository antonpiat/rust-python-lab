use std::sync::{Arc, Mutex};

use pyo3::prelude::*;

use crate::journal::Journal;

/// Submitted work. Await this object to get the Python coroutine's result.
#[pyclass(frozen)]
pub struct Handle {
    awaitable: Py<PyAny>,
    task_id: u64,
    journal: Arc<Mutex<Journal>>,
}

impl Handle {
    pub fn new(awaitable: Py<PyAny>, task_id: u64, journal: Arc<Mutex<Journal>>) -> Self {
        Self {
            awaitable,
            task_id,
            journal,
        }
    }
}

#[pymethods]
impl Handle {
    fn __await__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.awaitable.bind(py).call_method0("__await__")
    }

    /// `running`, `succeeded`, or `failed`.
    fn status(&self) -> String {
        self.journal
            .lock()
            .map(|j| j.get(self.task_id).map(|s| s.as_str().to_string()))
            .ok()
            .flatten()
            .unwrap_or_else(|| "unknown".to_string())
    }
}
