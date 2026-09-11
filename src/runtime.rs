use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3_async_runtimes::tokio::{
    future_into_py_with_locals, get_current_locals, get_runtime, scope,
};
use pyo3_async_runtimes::TaskLocals;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::bridge::{cancelled_error, parse_timeout_secs, start_asyncio_task, wait_with_policy};
use crate::handle::Handle;
use crate::journal::{Journal, TaskState};

struct Inner {
    journal: Arc<Mutex<Journal>>,
    next_id: AtomicU64,
    semaphore: Arc<Semaphore>,
    default_timeout: Option<Duration>,
}

/// Tokio-backed execution controller. Python coroutine bodies still run on asyncio.
#[pyclass(frozen)]
pub struct Runtime {
    inner: Arc<Inner>,
}

#[pymethods]
impl Runtime {
    #[new]
    #[pyo3(signature = (max_concurrency=32, default_timeout=None))]
    fn new(max_concurrency: usize, default_timeout: Option<f64>) -> PyResult<Self> {
        if max_concurrency == 0 {
            return Err(PyValueError::new_err("max_concurrency must be at least 1"));
        }
        let _ = get_runtime();
        let default_timeout = default_timeout.map(parse_timeout_secs).transpose()?;
        Ok(Self {
            inner: Arc::new(Inner {
                journal: Arc::new(Mutex::new(Journal::default())),
                next_id: AtomicU64::new(1),
                semaphore: Arc::new(Semaphore::new(max_concurrency)),
                default_timeout,
            }),
        })
    }

    /// Submit an async callable. Returns a Handle; await it for the result.
    ///
    /// The coroutine is not started until a concurrency permit is available.
    /// Tokio owns timeout and cancel; asyncio still executes the coroutine body.
    #[pyo3(signature = (func, *args, timeout=None))]
    fn submit(
        &self,
        py: Python<'_>,
        func: Py<PyAny>,
        args: Bound<'_, PyTuple>,
        timeout: Option<f64>,
    ) -> PyResult<Handle> {
        let locals = get_current_locals(py)?;
        let timeout = match timeout {
            Some(secs) => Some(parse_timeout_secs(secs)?),
            None => self.inner.default_timeout,
        };
        let args = args.unbind();
        let cancel = CancellationToken::new();
        let py_task = Arc::new(Mutex::new(None));
        let event_loop = locals.event_loop(py).unbind();

        let task_id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner
            .journal
            .lock()
            .expect("journal mutex")
            .insert_running(task_id);

        let journal = Arc::clone(&self.inner.journal);
        let semaphore = Arc::clone(&self.inner.semaphore);
        let cancel_for_task = cancel.clone();
        let py_task_for_run = Arc::clone(&py_task);
        let event_loop_for_run = event_loop.clone_ref(py);

        let awaitable = future_into_py_with_locals(
            py,
            locals.clone(),
            scope(locals.clone(), async move {
                let result = run_task(
                    locals,
                    func,
                    args,
                    semaphore,
                    cancel_for_task,
                    timeout,
                    Arc::clone(&py_task_for_run),
                    event_loop_for_run,
                )
                .await;
                if let Ok(mut journal) = journal.lock() {
                    journal.finish(task_id, state_for(&result));
                }
                result
            }),
        )?;

        Ok(Handle::new(
            awaitable.unbind(),
            task_id,
            Arc::clone(&self.inner.journal),
            cancel,
            event_loop,
            py_task,
        ))
    }
}

fn state_for(result: &PyResult<Py<PyAny>>) -> TaskState {
    match result {
        Ok(_) => TaskState::Succeeded,
        Err(err) => Python::attach(|py| {
            if err.is_instance_of::<pyo3::exceptions::PyTimeoutError>(py) {
                TaskState::TimedOut
            } else if is_cancelled(py, err) {
                TaskState::Cancelled
            } else {
                TaskState::Failed
            }
        }),
    }
}

fn is_cancelled(py: Python<'_>, err: &PyErr) -> bool {
    py.import("asyncio")
        .and_then(|asyncio| asyncio.getattr("CancelledError"))
        .map(|cls| err.is_instance(py, &cls))
        .unwrap_or(false)
}

async fn run_task(
    locals: TaskLocals,
    func: Py<PyAny>,
    args: Py<PyTuple>,
    semaphore: Arc<Semaphore>,
    cancel: CancellationToken,
    timeout: Option<Duration>,
    py_task: Arc<Mutex<Option<Py<PyAny>>>>,
    event_loop: Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    let _permit = acquire_permit(semaphore, &cancel).await?;
    if cancel.is_cancelled() {
        return Err(cancelled_error());
    }

    let (task_rx, result_rx) = Python::attach(|py| {
        let coro = func.bind(py).call(args.bind(py), None)?;
        start_asyncio_task(py, &locals, coro)
    })?;

    let started = tokio::select! {
        _ = cancel.cancelled() => None,
        task = task_rx => task.ok(),
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

    wait_with_policy(cancel, timeout, result_rx, event_loop, py_task).await
}

async fn acquire_permit(
    semaphore: Arc<Semaphore>,
    cancel: &CancellationToken,
) -> PyResult<OwnedSemaphorePermit> {
    tokio::select! {
        _ = cancel.cancelled() => Err(cancelled_error()),
        permit = semaphore.acquire_owned() => {
            permit.map_err(|_| PyValueError::new_err("runtime semaphore closed"))
        }
    }
}
