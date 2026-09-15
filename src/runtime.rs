use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pyo3::exceptions::{PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use pyo3_async_runtimes::tokio::{
    future_into_py_with_locals, get_current_locals, get_runtime, scope,
};
use pyo3_async_runtimes::TaskLocals;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::bridge::{cancelled_error, parse_timeout_secs, start_asyncio_task, wait_with_policy};
use crate::error::{QueueFull, RuntimeClosed};
use crate::handle::Handle;
use crate::journal::{Journal, TaskState};
use crate::policy::{OnFull, RetryConfig, RetryPolicy};
use crate::stream::{completion_channel, item_channel, CompletionStream, ItemStream};
use crate::task::Task;
use futures_util::stream::{FuturesUnordered, StreamExt};

struct Inner {
    journal: Arc<Mutex<Journal>>,
    next_id: AtomicU64,
    concurrency: Arc<Semaphore>,
    admission: Arc<Semaphore>,
    on_full: OnFull,
    default_timeout: Option<Duration>,
    retry: RetryConfig,
    closed: AtomicBool,
    ever_worked: AtomicBool,
    work_count: AtomicUsize,
    shutdown: CancellationToken,
    work_started: Notify,
    became_idle: Notify,
    idle_ttl: Option<Duration>,
}

impl Inner {
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn ensure_open(&self) -> PyResult<()> {
        if self.is_closed() {
            Err(RuntimeClosed::new_err("runtime is closed"))
        } else {
            Ok(())
        }
    }

    fn begin_work(&self) {
        self.ever_worked.store(true, Ordering::Release);
        self.work_count.fetch_add(1, Ordering::SeqCst);
        self.work_started.notify_waiters();
    }

    fn end_work(&self) {
        let prev = self.work_count.fetch_sub(1, Ordering::SeqCst);
        if prev == 1 {
            self.became_idle.notify_waiters();
        }
    }
}

/// Drops `end_work` when the Tokio task finishes or the submit fails to spawn.
struct WorkGuard {
    inner: Arc<Inner>,
}

impl WorkGuard {
    fn try_begin(inner: Arc<Inner>) -> PyResult<Self> {
        inner.ensure_open()?;
        inner.begin_work();
        if inner.is_closed() {
            inner.end_work();
            return Err(RuntimeClosed::new_err("runtime is closed"));
        }
        Ok(Self { inner })
    }
}

impl Drop for WorkGuard {
    fn drop(&mut self) {
        self.inner.end_work();
    }
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
        retry=None,
        idle_ttl=None
    ))]
    fn new(
        max_concurrency: usize,
        queue_capacity: usize,
        on_full: OnFull,
        default_timeout: Option<f64>,
        retry: Option<Bound<'_, RetryPolicy>>,
        idle_ttl: Option<f64>,
    ) -> PyResult<Self> {
        if max_concurrency == 0 {
            return Err(PyValueError::new_err("max_concurrency must be at least 1"));
        }
        let admission_cap = max_concurrency
            .checked_add(queue_capacity)
            .ok_or_else(|| PyValueError::new_err("queue_capacity is too large"))?;
        let _ = get_runtime();
        let default_timeout = default_timeout.map(parse_timeout_secs).transpose()?;
        let idle_ttl = idle_ttl.map(parse_timeout_secs).transpose()?;
        let retry = retry
            .map(|policy| RetryConfig::from_policy(&policy.borrow()))
            .unwrap_or_else(RetryConfig::none);
        let inner = Arc::new(Inner {
            journal: Arc::new(Mutex::new(Journal::default())),
            next_id: AtomicU64::new(1),
            concurrency: Arc::new(Semaphore::new(max_concurrency)),
            admission: Arc::new(Semaphore::new(admission_cap)),
            on_full,
            default_timeout,
            retry,
            closed: AtomicBool::new(false),
            ever_worked: AtomicBool::new(false),
            work_count: AtomicUsize::new(0),
            shutdown: CancellationToken::new(),
            work_started: Notify::new(),
            became_idle: Notify::new(),
            idle_ttl,
        });
        spawn_idle_watcher(Arc::clone(&inner));
        Ok(Self { inner })
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
        self.inner.ensure_open()?;
        let locals = get_current_locals(py)?;
        let timeout = match timeout {
            Some(secs) => Some(parse_timeout_secs(secs)?),
            None => self.inner.default_timeout,
        };
        let args = args.unbind();
        let cancel = self.inner.shutdown.child_token();
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
        let work = WorkGuard::try_begin(Arc::clone(&self.inner))?;

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
                let _work = work;
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

    /// Wait for all tasks, returning results in input order.
    #[pyo3(signature = (tasks, return_exceptions=false))]
    fn gather<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        tasks: Bound<'py, PyAny>,
        return_exceptions: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        slf.get().inner.ensure_open()?;
        let locals = get_current_locals(py)?;
        let specs = extract_tasks(&tasks)?;
        let mut handles = Vec::with_capacity(specs.len());
        let mut futs = Vec::with_capacity(specs.len());
        for spec in specs {
            let handle = submit_spec(slf, py, &spec)?;
            let fut = pyo3_async_runtimes::into_future_with_locals(
                &locals,
                handle.clone_awaitable(py).into_bound(py),
            )?;
            handles.push(handle);
            futs.push(fut);
        }
        future_into_py_with_locals(
            py,
            locals,
            async move {
                let n = futs.len();
                let mut pending = FuturesUnordered::new();
                for (index, fut) in futs.into_iter().enumerate() {
                    pending.push(async move { (index, fut.await) });
                }
                let mut slots: Vec<Option<PyResult<Py<PyAny>>>> = (0..n).map(|_| None).collect();
                while let Some((index, result)) = pending.next().await {
                    match result {
                        Ok(value) => slots[index] = Some(Ok(value)),
                        Err(err) if return_exceptions => slots[index] = Some(Err(err)),
                        Err(err) => {
                            Python::attach(|py| {
                                for handle in &handles {
                                    let _ = handle.cancel(py);
                                }
                            });
                            return Err(err);
                        }
                    }
                }
                Python::attach(|py| {
                    let list = PyList::empty(py);
                    for slot in slots {
                        match slot.unwrap_or_else(|| Err(cancelled_error())) {
                            Ok(value) => list.append(value.bind(py))?,
                            Err(err) => list.append(err.value(py))?,
                        }
                    }
                    Ok(list.unbind().into_any())
                })
            },
        )
    }

    /// Yield `Completion` items as tasks finish (out of order).
    fn as_completed<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        tasks: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, CompletionStream>> {
        slf.get().inner.ensure_open()?;
        let locals = get_current_locals(py)?;
        let specs = extract_tasks(&tasks)?;
        let (tx, rx) = completion_channel(specs.len());
        let mut waiters = Vec::with_capacity(specs.len());
        for (index, spec) in specs.into_iter().enumerate() {
            let handle = submit_spec(slf, py, &spec)?;
            let fut = pyo3_async_runtimes::into_future_with_locals(
                &locals,
                handle.clone_awaitable(py).into_bound(py),
            )?;
            waiters.push((index, fut));
        }
        get_runtime().spawn(async move {
            let mut set = FuturesUnordered::new();
            for (index, fut) in waiters {
                set.push(async move { (index, fut.await) });
            }
            while let Some(item) = set.next().await {
                if tx.send(item).await.is_err() {
                    break;
                }
            }
        });
        Bound::new(py, CompletionStream::new(rx))
    }

    /// Stream items from a Python async generator. `buffer` bounds in-flight yields.
    #[pyo3(signature = (func, *args, timeout=None, buffer=8))]
    fn stream(
        &self,
        py: Python<'_>,
        func: Py<PyAny>,
        args: Bound<'_, PyTuple>,
        timeout: Option<f64>,
        buffer: usize,
    ) -> PyResult<ItemStream> {
        if buffer == 0 {
            return Err(PyValueError::new_err("buffer must be at least 1"));
        }
        self.inner.ensure_open()?;
        let locals = get_current_locals(py)?;
        let timeout = match timeout {
            Some(secs) => Some(parse_timeout_secs(secs)?),
            None => self.inner.default_timeout,
        };
        let args = args.unbind();
        let cancel = self.inner.shutdown.child_token();
        let py_task = Arc::new(Mutex::new(None));
        let event_loop = locals.event_loop(py).unbind();
        let (tx, rx) = item_channel(buffer);

        let admission_permit = match self.inner.on_full {
            OnFull::REJECT => match Arc::clone(&self.inner.admission).try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => return Err(QueueFull::new_err("admission queue is full")),
            },
            OnFull::WAIT => None,
        };
        let work = WorkGuard::try_begin(Arc::clone(&self.inner))?;

        let task_id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner
            .journal
            .lock()
            .expect("journal mutex")
            .insert_queued(task_id);

        let journal = Arc::clone(&self.inner.journal);
        let concurrency = Arc::clone(&self.inner.concurrency);
        let admission = Arc::clone(&self.inner.admission);
        let cancel_for_task = cancel.clone();
        let event_loop_for_run = event_loop.clone_ref(py);

        get_runtime().spawn(scope(locals.clone(), async move {
            let _work = work;
            let result = run_stream(StreamArgs {
                locals,
                func,
                args,
                admission,
                admission_permit,
                concurrency,
                cancel: cancel_for_task,
                timeout,
                task_id,
                py_task,
                event_loop: event_loop_for_run,
                journal: Arc::clone(&journal),
                tx,
            })
            .await;
            let state = match &result {
                Ok(()) => TaskState::Succeeded,
                Err(err) => state_for_err(err),
            };
            if let Ok(mut journal) = journal.lock() {
                journal.finish(task_id, state);
            }
        }));

        Ok(ItemStream::new(rx, cancel))
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

    /// Stop admissions, wait for in-flight work up to `timeout`, then cancel the rest.
    #[pyo3(signature = (timeout=None))]
    fn shutdown<'py>(&self, py: Python<'py>, timeout: Option<f64>) -> PyResult<Bound<'py, PyAny>> {
        let timeout = timeout.map(parse_timeout_secs).transpose()?;
        let inner = Arc::clone(&self.inner);
        let locals = get_current_locals(py)?;
        future_into_py_with_locals(py, locals, async move {
            drain_or_cancel(inner, timeout).await;
            Ok(())
        })
    }

    #[getter]
    fn closed(&self) -> bool {
        self.inner.is_closed()
    }

    fn __aenter__<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        slf.get().inner.ensure_open()?;
        let slf = slf.unbind();
        let locals = get_current_locals(py)?;
        future_into_py_with_locals(py, locals, async move { Ok(slf) })
    }

    #[pyo3(signature = (exc_type, exc, tb))]
    fn __aexit__<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        exc_type: Option<&Bound<'py, PyAny>>,
        exc: Option<&Bound<'py, PyAny>>,
        tb: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let _ = (exc_type, exc, tb);
        slf.get().shutdown(py, None)
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
        Err(err) => state_for_err(err),
    }
}

fn state_for_err(err: &PyErr) -> TaskState {
    Python::attach(|py| {
        if err.is_instance_of::<pyo3::exceptions::PyTimeoutError>(py) {
            TaskState::TimedOut
        } else if is_cancelled(py, err) {
            TaskState::Cancelled
        } else {
            TaskState::Failed
        }
    })
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

fn extract_tasks(tasks: &Bound<'_, PyAny>) -> PyResult<Vec<Py<Task>>> {
    let mut out = Vec::new();
    for item in tasks.try_iter()? {
        out.push(item?.extract()?);
    }
    Ok(out)
}

fn submit_spec(rt: &Bound<'_, Runtime>, py: Python<'_>, spec: &Py<Task>) -> PyResult<Handle> {
    let task = spec.bind(py).borrow();
    rt.get().submit(
        py,
        task.func.clone_ref(py),
        task.args.bind(py).clone(),
        task.timeout,
    )
}

struct StreamArgs {
    locals: TaskLocals,
    func: Py<PyAny>,
    args: Py<PyTuple>,
    admission: Arc<Semaphore>,
    admission_permit: Option<OwnedSemaphorePermit>,
    concurrency: Arc<Semaphore>,
    cancel: CancellationToken,
    timeout: Option<Duration>,
    task_id: u64,
    py_task: Arc<Mutex<Option<Py<PyAny>>>>,
    event_loop: Py<PyAny>,
    journal: Arc<Mutex<Journal>>,
    tx: tokio::sync::mpsc::Sender<PyResult<Py<PyAny>>>,
}

async fn run_stream(args: StreamArgs) -> PyResult<()> {
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

    let agen = Python::attach(|py| {
        args.func
            .bind(py)
            .call(args.args.bind(py), None)
            .map(|value| value.unbind())
    })?;

    loop {
        if args.cancel.is_cancelled() {
            return Err(cancelled_error());
        }
        if args.tx.is_closed() {
            return Ok(());
        }

        let (task_rx, result_rx) = Python::attach(|py| {
            let next = agen.bind(py).call_method0("__anext__")?;
            start_asyncio_task(py, &args.locals, next)
        })?;

        let started = tokio::select! {
            _ = args.cancel.cancelled() => None,
            task = task_rx => task.ok(),
        };
        let Some(task) = started else {
            return Err(cancelled_error());
        };
        if let Ok(mut slot) = args.py_task.lock() {
            *slot = Some(task);
        }

        let event_loop = Python::attach(|py| args.event_loop.clone_ref(py));
        match wait_with_policy(
            args.cancel.clone(),
            args.timeout,
            result_rx,
            event_loop,
            Arc::clone(&args.py_task),
        )
        .await
        {
            Ok(item) => {
                if args.tx.send(Ok(item)).await.is_err() {
                    return Ok(());
                }
            }
            Err(err) => {
                let stop = Python::attach(|py| err.is_instance_of::<PyStopAsyncIteration>(py));
                if stop {
                    return Ok(());
                }
                let _ = args.tx.send(Err(err)).await;
                return Ok(());
            }
        }
    }
}

fn spawn_idle_watcher(inner: Arc<Inner>) {
    let Some(ttl) = inner.idle_ttl else {
        return;
    };
    get_runtime().spawn(async move {
        loop {
            if inner.is_closed() {
                return;
            }
            if inner.ever_worked.load(Ordering::Acquire) {
                break;
            }
            tokio::select! {
                _ = inner.work_started.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
        }
        loop {
            if inner.is_closed() {
                return;
            }
            if inner.work_count.load(Ordering::Acquire) > 0 {
                tokio::select! {
                    _ = inner.became_idle.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                }
                continue;
            }
            tokio::select! {
                _ = inner.work_started.notified() => {}
                _ = tokio::time::sleep(ttl) => {
                    if inner.work_count.load(Ordering::Acquire) == 0 {
                        inner.closed.store(true, Ordering::Release);
                        inner.shutdown.cancel();
                        return;
                    }
                }
            }
        }
    });
}

async fn drain_or_cancel(inner: Arc<Inner>, timeout: Option<Duration>) {
    inner.closed.store(true, Ordering::Release);
    if wait_until_idle(&inner, timeout).await {
        return;
    }
    inner.shutdown.cancel();
    let _ = wait_until_idle(&inner, Some(Duration::from_secs(5))).await;
}

async fn wait_until_idle(inner: &Inner, timeout: Option<Duration>) -> bool {
    let deadline = timeout.map(|d| tokio::time::Instant::now() + d);
    loop {
        if inner.work_count.load(Ordering::Acquire) == 0 {
            return true;
        }
        match deadline {
            None => {
                tokio::select! {
                    _ = inner.became_idle.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(20)) => {}
                }
            }
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return inner.work_count.load(Ordering::Acquire) == 0;
                }
                tokio::select! {
                    _ = inner.became_idle.notified() => {}
                    _ = tokio::time::sleep(remaining.min(Duration::from_millis(20))) => {}
                }
            }
        }
    }
}
