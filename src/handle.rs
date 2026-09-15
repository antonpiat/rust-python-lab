use std::sync::Arc;

use pyo3::prelude::*;
use tokio_util::sync::CancellationToken;

use crate::bridge::{PyValue, SharedPyTask, cancel_asyncio_task};
use crate::journal::TaskSlot;

/// Submitted work. Await this object to get the Python coroutine's result.
#[pyclass(frozen)]
pub struct Handle {
    awaitable: PyValue,
    slot: Arc<TaskSlot>,
    cancel: CancellationToken,
    event_loop: PyValue,
    py_task: SharedPyTask,
}

impl Handle {
    pub fn new(
        awaitable: PyValue,
        slot: Arc<TaskSlot>,
        cancel: CancellationToken,
        event_loop: PyValue,
        py_task: SharedPyTask,
    ) -> Self {
        Self {
            awaitable,
            slot,
            cancel,
            event_loop,
            py_task,
        }
    }

    pub(crate) fn clone_awaitable(&self, py: Python<'_>) -> PyValue {
        self.awaitable.clone_ref(py)
    }
}

#[pymethods]
impl Handle {
    fn __await__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.awaitable.bind(py).call_method0("__await__")
    }

    /// Cancel this task: Tokio token and the asyncio Task, if it has started.
    pub(crate) fn cancel(&self, py: Python<'_>) -> PyResult<()> {
        self.cancel.cancel();
        if let Some(task) = self
            .py_task
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(|task| task.clone_ref(py)))
        {
            cancel_asyncio_task(self.event_loop.bind(py), task.bind(py))?;
        }
        Ok(())
    }

    fn cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// `queued`, `running`, `succeeded`, `failed`, `cancelled`, or `timed_out`.
    fn status(&self) -> &'static str {
        self.slot.get().as_str()
    }

    fn done(&self) -> bool {
        self.slot.get().is_terminal()
    }
}
