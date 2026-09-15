use std::future::pending;
use std::sync::OnceLock;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_channel::oneshot;
use pyo3::exceptions::{PyTimeoutError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_async_runtimes::TaskLocals;
use tokio_util::sync::CancellationToken;

pub type PyValue = Py<PyAny>;
pub type PyValueResult = PyResult<PyValue>;

type PyBound<'py> = Bound<'py, PyAny>;
pub type SharedPyTask = Arc<Mutex<Option<PyValue>>>;

type TaskRx = oneshot::Receiver<PyValue>;
type ResultRx = oneshot::Receiver<PyValueResult>;
type TaskTx = oneshot::Sender<PyValue>;
type ResultTx = oneshot::Sender<PyValueResult>;

struct BridgedTask {
    task_rx: TaskRx,
    result_rx: ResultRx,
}

pub fn shared_py_task() -> SharedPyTask {
    Arc::new(Mutex::new(None))
}

static CANCELLED_EXC: OnceLock<PyValue> = OnceLock::new();

fn cancelled_type(py: Python<'_>) -> PyResult<PyBound<'_>> {
    if let Some(cls) = CANCELLED_EXC.get() {
        return Ok(cls.bind(py).clone());
    }
    let cls = py.import("asyncio")?.getattr("CancelledError")?;
    let stored = cls.clone().unbind();
    let _ = CANCELLED_EXC.set(stored);
    Ok(cls)
}

fn start_asyncio_task(
    py: Python<'_>,
    locals: &TaskLocals,
    awaitable: PyBound<'_>,
) -> PyResult<BridgedTask> {
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

    Ok(BridgedTask { task_rx, result_rx })
}

pub fn cancel_asyncio_task(event_loop: &PyBound<'_>, task: &PyBound<'_>) -> PyResult<()> {
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

struct CancelOnDrop {
    event_loop: PyValue,
    py_task: SharedPyTask,
    armed: bool,
}

impl CancelOnDrop {
    fn new(event_loop: PyValue, py_task: SharedPyTask) -> Self {
        Self {
            event_loop,
            py_task,
            armed: true,
        }
    }

    fn disarm(&mut self) {
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
            let _ = cancel_asyncio_task(self.event_loop.bind(py), task.bind(py));
        });
    }
}

/// Schedule a Python awaitable on asyncio and wait for it under cancel/timeout.
pub async fn await_bridged(
    locals: &TaskLocals,
    cancel: &CancellationToken,
    timeout: Option<Duration>,
    py_task: &SharedPyTask,
    event_loop: &PyValue,
    make_awaitable: impl FnOnce(Python<'_>) -> PyValueResult,
) -> PyValueResult {
    let (task_rx, result_rx, event_loop) = Python::attach(|py| -> PyResult<_> {
        let awaitable = make_awaitable(py)?.into_bound(py);
        let bridged = start_asyncio_task(py, locals, awaitable)?;
        Ok((bridged.task_rx, bridged.result_rx, event_loop.clone_ref(py)))
    })?;

    let started = tokio::select! {
        biased;
        task = task_rx => task.ok(),
        _ = cancel.cancelled() => None,
    };
    let Some(task) = started else {
        return Err(cancelled_error());
    };
    if let Ok(mut slot) = py_task.lock() {
        *slot = Some(task);
    }
    if cancel.is_cancelled() {
        return Err(cancelled_error());
    }

    let mut guard = CancelOnDrop::new(event_loop, Arc::clone(py_task));
    let sleep = async {
        match timeout {
            Some(duration) => tokio::time::sleep(duration).await,
            None => pending().await,
        }
    };
    let result = tokio::select! {
        biased;
        result = result_rx => map_oneshot(result),
        _ = cancel.cancelled() => Err(cancelled_error()),
        _ = sleep => Err(timeout_error()),
    };
    if result.is_ok() {
        guard.disarm();
    }
    result
}

fn map_oneshot(result: Result<PyValueResult, oneshot::Canceled>) -> PyValueResult {
    match result {
        Ok(inner) => inner,
        Err(_) => Err(cancelled_error()),
    }
}

#[pyclass]
struct CaptureTask {
    awaitable: PyValue,
    event_loop: PyValue,
    task_tx: Option<TaskTx>,
    result_tx: Option<ResultTx>,
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
    tx: Option<ResultTx>,
}

#[pymethods]
impl ResultCompleter {
    fn __call__(&mut self, task: PyBound<'_>) -> PyResult<()> {
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
