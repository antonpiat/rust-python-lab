use std::sync::OnceLock;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_channel::oneshot;
use pyo3::exceptions::{PyTimeoutError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_async_runtimes::TaskLocals;
use tokio_util::sync::CancellationToken;

static CANCELLED_EXC: OnceLock<Py<PyAny>> = OnceLock::new();

fn cancelled_type(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    if let Some(cls) = CANCELLED_EXC.get() {
        return Ok(cls.bind(py).clone());
    }
    let cls = py.import("asyncio")?.getattr("CancelledError")?;
    let stored = cls.clone().unbind();
    let _ = CANCELLED_EXC.set(stored);
    Ok(cls)
}

/// Schedule `awaitable` on the captured asyncio loop and keep the Task so we can cancel it.
pub fn start_asyncio_task(
    py: Python<'_>,
    locals: &TaskLocals,
    awaitable: Bound<'_, PyAny>,
) -> PyResult<(oneshot::Receiver<Py<PyAny>>, oneshot::Receiver<PyResult<Py<PyAny>>>)> {
    let (task_tx, task_rx) = oneshot::channel();
    let (result_tx, result_rx) = oneshot::channel();
    let event_loop = locals.event_loop(py);

    let callback = Bound::new(
        py,
        CaptureTask {
            awaitable: awaitable.unbind(),
            event_loop: event_loop.clone().unbind(),
            task_tx: Some(task_tx),
            result_tx: Some(result_tx),
        },
    )?;

    let kwargs = PyDict::new(py);
    kwargs.set_item("context", locals.context(py))?;
    event_loop.call_method("call_soon_threadsafe", (callback,), Some(&kwargs))?;

    Ok((task_rx, result_rx))
}

pub fn cancel_asyncio_task(
    event_loop: &Bound<'_, PyAny>,
    task: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let cancel = task.getattr("cancel")?;
    event_loop.call_method1("call_soon_threadsafe", (cancel,))?;
    Ok(())
}

pub fn cancelled_error() -> PyErr {
    Python::attach(|py| match cancelled_type(py).and_then(|cls| cls.call0()) {
        Ok(err) => PyErr::from_value(err),
        Err(err) => err,
    })
}

pub fn is_cancelled(py: Python<'_>, err: &PyErr) -> bool {
    cancelled_type(py)
        .map(|cls| err.is_instance(py, &cls))
        .unwrap_or(false)
}

pub fn timeout_error() -> PyErr {
    PyTimeoutError::new_err("task timed out")
}

pub fn parse_timeout_secs(secs: f64) -> PyResult<Duration> {
    if !secs.is_finite() || secs < 0.0 {
        return Err(PyValueError::new_err(
            "timeout must be a non-negative finite number",
        ));
    }
    Ok(Duration::from_secs_f64(secs))
}

/// Cancels the asyncio task if the Tokio waiter is dropped mid-flight.
pub struct CancelOnDrop {
    event_loop: Py<PyAny>,
    py_task: Arc<Mutex<Option<Py<PyAny>>>>,
    armed: bool,
}

impl CancelOnDrop {
    pub fn new(event_loop: Py<PyAny>, py_task: Arc<Mutex<Option<Py<PyAny>>>>) -> Self {
        Self {
            event_loop,
            py_task,
            armed: true,
        }
    }

    pub fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Some(task) = self.py_task.lock().ok().and_then(|mut g| g.take()) else {
            return;
        };
        Python::attach(|py| {
            let _ = cancel_asyncio_task(&self.event_loop.bind(py), &task.bind(py));
        });
    }
}

pub async fn wait_with_policy(
    cancel: CancellationToken,
    timeout: Option<Duration>,
    result_rx: oneshot::Receiver<PyResult<Py<PyAny>>>,
    event_loop: Py<PyAny>,
    py_task: Arc<Mutex<Option<Py<PyAny>>>>,
) -> PyResult<Py<PyAny>> {
    let mut guard = CancelOnDrop::new(event_loop, py_task);

    let result = match timeout {
        Some(duration) => {
            tokio::select! {
                biased;
                result = result_rx => map_oneshot(result),
                _ = cancel.cancelled() => Err(cancelled_error()),
                _ = tokio::time::sleep(duration) => Err(timeout_error()),
            }
        }
        None => {
            tokio::select! {
                biased;
                result = result_rx => map_oneshot(result),
                _ = cancel.cancelled() => Err(cancelled_error()),
            }
        }
    };

    if result.is_ok() {
        guard.disarm();
    }
    result
}

fn map_oneshot(result: Result<PyResult<Py<PyAny>>, oneshot::Canceled>) -> PyResult<Py<PyAny>> {
    match result {
        Ok(inner) => inner,
        Err(_) => Err(cancelled_error()),
    }
}

#[pyclass]
struct CaptureTask {
    awaitable: Py<PyAny>,
    event_loop: Py<PyAny>,
    task_tx: Option<oneshot::Sender<Py<PyAny>>>,
    result_tx: Option<oneshot::Sender<PyResult<Py<PyAny>>>>,
}

#[pymethods]
impl CaptureTask {
    fn __call__(&mut self, py: Python<'_>) -> PyResult<()> {
        let task = self
            .event_loop
            .bind(py)
            .call_method1("create_task", (self.awaitable.bind(py),))?;
        if let Some(tx) = self.task_tx.take() {
            let _ = tx.send(task.clone().unbind());
        }
        let completer = Bound::new(
            py,
            ResultCompleter {
                tx: self.result_tx.take(),
            },
        )?;
        task.call_method1("add_done_callback", (completer,))?;
        Ok(())
    }
}

#[pyclass]
struct ResultCompleter {
    tx: Option<oneshot::Sender<PyResult<Py<PyAny>>>>,
}

#[pymethods]
impl ResultCompleter {
    fn __call__(&mut self, task: Bound<'_, PyAny>) -> PyResult<()> {
        let result = match task.call_method0("result") {
            Ok(value) => Ok(value.unbind()),
            Err(err) => Err(err),
        };
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(result);
        }
        Ok(())
    }
}
