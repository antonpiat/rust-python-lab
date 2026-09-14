use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use pyo3_async_runtimes::tokio::{
    future_into_py_with_locals, get_current_locals, get_runtime, scope,
};
use pyo3_async_runtimes::TaskLocals;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::bridge::{cancelled_error, parse_timeout_secs, start_asyncio_task, wait_with_policy};
use crate::error::QueueFull;
use crate::handle::Handle;
use crate::journal::{Journal, TaskState};
use crate::policy::{OnFull, RetryConfig, RetryPolicy};

struct Inner {
    journal: Arc<Mutex<Journal>>,
    next_id: AtomicU64,
    concurrency: Arc<Semaphore>,
    admission: Arc<Semaphore>,
    on_full: OnFull,
    default_timeout: Option<Duration>,
    retry: RetryConfig,
}

/// Tokio-backed execution controller. Python coroutine bodies still run on asyncio.
#[pyclass(frozen)]
pub struct Runtime {
    inner: Arc<Inner>,
}

#[pymethods]
impl Runtime {
    #[new]
    #[pyo3(signature = (
        max_concurrency=32,
        queue_capacity=128,
        on_full=OnFull::REJECT,
        default_timeout=None,
        retry=None
    ))]
    fn new(
        max_concurrency: usize,
        queue_capacity: usize,
        on_full: OnFull,
        default_timeout: Option<f64>,
        retry: Option<Bound<'_, RetryPolicy>>,
    ) -> PyResult<Self> {
        if max_concurrency == 0 {
            return Err(PyValueError::new_err("max_concurrency must be at least 1"));
        }
        let admission_cap = max_concurrency
            .checked_add(queue_capacity)
            .ok_or_else(|| PyValueError::new_err("queue_capacity is too large"))?;
        let _ = get_runtime();
        let default_timeout = default_timeout.map(parse_timeout_secs).transpose()?;
        let retry = retry
            .map(|policy| RetryConfig::from_policy(&policy.borrow()))
            .unwrap_or_else(RetryConfig::none);
        Ok(Self {
            inner: Arc::new(Inner {
                journal: Arc::new(Mutex::new(Journal::default())),
                next_id: AtomicU64::new(1),
                concurrency: Arc::new(Semaphore::new(max_concurrency)),
                admission: Arc::new(Semaphore::new(admission_cap)),
                on_full,
                default_timeout,
                retry,
            }),
        })
    }

    /// Submit an async callable. Returns a Handle; await it for the result.
    ///
    /// The coroutine is not started until a concurrency permit is available.
    /// Tokio owns timeout, cancel, retries, and queue admission.
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

        let admission_permit = match self.inner.on_full {
            OnFull::REJECT => match Arc::clone(&self.inner.admission).try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    return Err(QueueFull::new_err("admission queue is full"));
                }
            },
            OnFull::WAIT => None,
        };

        let task_id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner
            .journal
            .lock()
            .expect("journal mutex")
            .insert_queued(task_id);

        let journal = Arc::clone(&self.inner.journal);
        let concurrency = Arc::clone(&self.inner.concurrency);
        let admission = Arc::clone(&self.inner.admission);
        let retry = self.inner.retry;
        let cancel_for_task = cancel.clone();
        let py_task_for_run = Arc::clone(&py_task);
        let event_loop_for_run = event_loop.clone_ref(py);

        let awaitable = future_into_py_with_locals(
            py,
            locals.clone(),
            scope(locals.clone(), async move {
                let result = run_task(RunArgs {
                    locals,
                    func,
                    args,
                    admission,
                    admission_permit,
                    concurrency,
                    cancel: cancel_for_task,
                    timeout,
                    retry,
                    task_id,
                    py_task: Arc::clone(&py_task_for_run),
                    event_loop: event_loop_for_run,
                    journal: Arc::clone(&journal),
                })
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

    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let stats = self
            .inner
            .journal
            .lock()
            .expect("journal mutex")
            .stats();
        let dict = PyDict::new(py);
        dict.set_item("queued", stats.queued)?;
        dict.set_item("in_flight", stats.in_flight)?;
        dict.set_item("succeeded", stats.succeeded)?;
        dict.set_item("failed", stats.failed)?;
        dict.set_item("cancelled", stats.cancelled)?;
        dict.set_item("timed_out", stats.timed_out)?;
        dict.set_item("completed", stats.completed())?;
        Ok(dict)
    }
}

struct RunArgs {
    locals: TaskLocals,
    func: Py<PyAny>,
    args: Py<PyTuple>,
    admission: Arc<Semaphore>,
    admission_permit: Option<OwnedSemaphorePermit>,
    concurrency: Arc<Semaphore>,
    cancel: CancellationToken,
    timeout: Option<Duration>,
    retry: RetryConfig,
    task_id: u64,
    py_task: Arc<Mutex<Option<Py<PyAny>>>>,
    event_loop: Py<PyAny>,
    journal: Arc<Mutex<Journal>>,
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

async fn run_task(args: RunArgs) -> PyResult<Py<PyAny>> {
    let _admission = match args.admission_permit {
        Some(permit) => permit,
        None => acquire_permit(args.admission, &args.cancel).await?,
    };
    if args.cancel.is_cancelled() {
        return Err(cancelled_error());
    }

    let _concurrency = acquire_permit(args.concurrency, &args.cancel).await?;
    if args.cancel.is_cancelled() {
        return Err(cancelled_error());
    }
    if let Ok(mut journal) = args.journal.lock() {
        journal.mark_running(args.task_id);
    }

    let attempts = args.retry.max_attempts.max(1);
    let mut last_err: Option<PyErr> = None;
    for attempt in 0..attempts {
        if args.cancel.is_cancelled() {
            return Err(cancelled_error());
        }
        if attempt > 0 {
            let delay = args.retry.backoff_for(attempt, args.task_id);
            if !delay.is_zero() {
                tokio::select! {
                    _ = args.cancel.cancelled() => return Err(cancelled_error()),
                    _ = tokio::time::sleep(delay) => {}
                }
            }
        }
        if let Ok(mut slot) = args.py_task.lock() {
            *slot = None;
        }
        match run_once(
            &args.locals,
            &args.func,
            &args.args,
            args.cancel.clone(),
            args.timeout,
            Arc::clone(&args.py_task),
            Python::attach(|py| args.event_loop.clone_ref(py)),
        )
        .await
        {
            Ok(value) => return Ok(value),
            Err(err) => {
                let retryable = Python::attach(|py| {
                    !is_cancelled(py, &err)
                });
                last_err = Some(err);
                if !retryable || attempt + 1 >= attempts {
                    break;
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(cancelled_error))
}

async fn run_once(
    locals: &TaskLocals,
    func: &Py<PyAny>,
    args: &Py<PyTuple>,
    cancel: CancellationToken,
    timeout: Option<Duration>,
    py_task: Arc<Mutex<Option<Py<PyAny>>>>,
    event_loop: Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    let (task_rx, result_rx) = Python::attach(|py| {
        let coro = func.bind(py).call(args.bind(py), None)?;
        start_asyncio_task(py, locals, coro)
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
