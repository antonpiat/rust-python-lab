use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use pyo3::exceptions::{PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use pyo3_async_runtimes::TaskLocals;
use pyo3_async_runtimes::tokio::{
    future_into_py_with_locals, get_current_locals, get_runtime, scope,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::bridge::{
    PyValue, PyValueResult, SharedPyTask, await_bridged, cancelled_error, is_cancelled,
    parse_timeout_secs, shared_py_task,
};
use crate::error::{QueueFull, RuntimeClosed};
use crate::handle::Handle;
use crate::journal::{Journal, TaskSlot, TaskState};
use crate::policy::{OnFull, RetryConfig, RetryPolicy};
use crate::stream::{CompletionStream, ItemResult, ItemStream, completion_channel, item_channel};
use crate::task::Task;
use futures_util::stream::{FuturesUnordered, StreamExt};

struct Inner {
    journal: Journal,
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
        self.work_count.fetch_add(1, Ordering::Release);
        self.work_started.notify_waiters();
    }

    fn end_work(&self) {
        if self.work_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.became_idle.notify_waiters();
        }
    }

    fn resolve_timeout(&self, timeout: Option<f64>) -> PyResult<Option<Duration>> {
        match timeout {
            Some(secs) => parse_timeout_secs(secs).map(Some),
            None => Ok(self.default_timeout),
        }
    }

    fn try_admit(&self) -> PyResult<Option<OwnedSemaphorePermit>> {
        match self.on_full {
            OnFull::Reject => match Arc::clone(&self.admission).try_acquire_owned() {
                Ok(permit) => Ok(Some(permit)),
                Err(_) => Err(QueueFull::new_err("admission queue is full")),
            },
            OnFull::Wait => Ok(None),
        }
    }

    fn queue_slot(&self) -> Arc<TaskSlot> {
        let slot = TaskSlot::queued();
        self.journal.insert_queued();
        slot
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

struct Admitted {
    locals: TaskLocals,
    args: Py<PyTuple>,
    cancel: CancellationToken,
    timeout: Option<Duration>,
    py_task: SharedPyTask,
    event_loop: PyValue,
    admission_permit: Option<OwnedSemaphorePermit>,
    work: WorkGuard,
    slot: Arc<TaskSlot>,
    task_id: u64,
}

impl Admitted {
    fn into_ctx(self, inner: Arc<Inner>, func: PyValue) -> (WorkGuard, WorkCtx) {
        (
            self.work,
            WorkCtx {
                inner,
                locals: self.locals,
                func,
                args: self.args,
                admission_permit: self.admission_permit,
                cancel: self.cancel,
                timeout: self.timeout,
                py_task: self.py_task,
                event_loop: self.event_loop,
                slot: self.slot,
                task_id: self.task_id,
            },
        )
    }
}

struct WorkCtx {
    inner: Arc<Inner>,
    locals: TaskLocals,
    func: PyValue,
    args: Py<PyTuple>,
    admission_permit: Option<OwnedSemaphorePermit>,
    cancel: CancellationToken,
    timeout: Option<Duration>,
    py_task: SharedPyTask,
    event_loop: PyValue,
    slot: Arc<TaskSlot>,
    task_id: u64,
}

/// Tokio-backed execution controller. Python coroutine bodies still run on asyncio.
#[pyclass(frozen)]
pub struct Runtime {
    inner: Arc<Inner>,
}

impl Runtime {
    fn admit(
        &self,
        py: Python<'_>,
        args: Bound<'_, PyTuple>,
        timeout: Option<f64>,
    ) -> PyResult<Admitted> {
        self.inner.ensure_open()?;
        let locals = get_current_locals(py)?;
        let timeout = self.inner.resolve_timeout(timeout)?;
        let admission_permit = self.inner.try_admit()?;
        let work = WorkGuard::try_begin(Arc::clone(&self.inner))?;
        let event_loop = locals.event_loop(py).unbind();
        Ok(Admitted {
            locals,
            args: args.unbind(),
            cancel: self.inner.shutdown.child_token(),
            timeout,
            py_task: shared_py_task(),
            event_loop,
            admission_permit,
            work,
            slot: self.inner.queue_slot(),
            task_id: self.inner.next_id.fetch_add(1, Ordering::Relaxed),
        })
    }
}

#[pymethods]
impl Runtime {
    #[new]
    #[pyo3(signature = (
        max_concurrency=32,
        queue_capacity=128,
        on_full=OnFull::Reject,
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
            journal: Journal::default(),
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
        if inner.idle_ttl.is_some() {
            spawn_idle_watcher(Arc::clone(&inner));
        }
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
        let admitted = self.admit(py, args, timeout)?;
        let cancel = admitted.cancel.clone();
        let py_task = Arc::clone(&admitted.py_task);
        let event_loop = admitted.event_loop.clone_ref(py);
        let slot = Arc::clone(&admitted.slot);
        let locals = admitted.locals.clone();
        let (work, ctx) = admitted.into_ctx(Arc::clone(&self.inner), func);

        let awaitable = future_into_py_with_locals(
            py,
            locals.clone(),
            scope(locals, async move {
                let _work = work;
                run_work(ctx, run_task_inner).await
            }),
        )?;

        Ok(Handle::new(
            awaitable.unbind(),
            slot,
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
        let (locals, handles) = submit_all(slf, py, &tasks)?;
        let mut futs = Vec::with_capacity(handles.len());
        for handle in &handles {
            futs.push(handle_future(py, &locals, handle)?);
        }
        future_into_py_with_locals(py, locals, async move {
            let n = futs.len();
            let mut pending = indexed_set(futs);
            let mut slots = Vec::with_capacity(n);
            slots.resize_with(n, || None);
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
        })
    }

    /// Yield `Completion` items as tasks finish (out of order).
    fn as_completed<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        tasks: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, CompletionStream>> {
        let (locals, handles) = submit_all(slf, py, &tasks)?;
        let (tx, rx) = completion_channel(handles.len());
        let mut futs = Vec::with_capacity(handles.len());
        for handle in &handles {
            futs.push(handle_future(py, &locals, handle)?);
        }
        get_runtime().spawn(async move {
            let mut set = indexed_set(futs);
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
        let admitted = self.admit(py, args, timeout)?;
        let (tx, rx) = item_channel(buffer);
        let cancel = admitted.cancel.clone();
        let (work, ctx) = admitted.into_ctx(Arc::clone(&self.inner), func);
        get_runtime().spawn(scope(ctx.locals.clone(), async move {
            let _work = work;
            let _ = run_work(ctx, |ctx| run_stream_inner(ctx, tx)).await;
        }));
        Ok(ItemStream::new(rx, cancel))
    }

    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let stats = self.inner.journal.stats();
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

    fn __aenter__<'py>(slf: Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
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

fn state_from_result<T>(result: &PyResult<T>) -> TaskState {
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

async fn run_work<T, F, Fut>(ctx: WorkCtx, run: F) -> PyResult<T>
where
    F: FnOnce(WorkCtx) -> Fut,
    Fut: Future<Output = PyResult<T>>,
{
    let slot = Arc::clone(&ctx.slot);
    let inner = Arc::clone(&ctx.inner);
    let result = run(ctx).await;
    inner.journal.finish(&slot, state_from_result(&result));
    result
}

async fn acquire_execution(
    ctx: &mut WorkCtx,
) -> PyResult<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
    let admission = match ctx.admission_permit.take() {
        Some(permit) => permit,
        None => acquire_permit(Arc::clone(&ctx.inner.admission), &ctx.cancel).await?,
    };
    let concurrency = acquire_permit(Arc::clone(&ctx.inner.concurrency), &ctx.cancel).await?;
    ctx.inner.journal.mark_running(&ctx.slot);
    Ok((admission, concurrency))
}

async fn run_task_inner(mut ctx: WorkCtx) -> PyValueResult {
    let retry = ctx.inner.retry;
    let _permits = acquire_execution(&mut ctx).await?;

    let attempts = retry.max_attempts.max(1);
    let mut last_err: Option<PyErr> = None;
    for attempt in 0..attempts {
        if ctx.cancel.is_cancelled() {
            return Err(cancelled_error());
        }
        if attempt > 0 {
            if let Ok(mut slot) = ctx.py_task.lock() {
                *slot = None;
            }
            let delay = retry.backoff_for(attempt, ctx.task_id);
            if !delay.is_zero() {
                tokio::select! {
                    biased;
                    _ = ctx.cancel.cancelled() => return Err(cancelled_error()),
                    _ = tokio::time::sleep(delay) => {}
                }
            }
        }
        match await_call(&ctx).await {
            Ok(value) => return Ok(value),
            Err(err) => {
                let more = attempt + 1 < attempts && Python::attach(|py| !is_cancelled(py, &err));
                last_err = Some(err);
                if !more {
                    break;
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(cancelled_error))
}

async fn await_call(ctx: &WorkCtx) -> PyValueResult {
    await_bridged(
        &ctx.locals,
        &ctx.cancel,
        ctx.timeout,
        &ctx.py_task,
        &ctx.event_loop,
        |py| Ok(ctx.func.bind(py).call(ctx.args.bind(py), None)?.unbind()),
    )
    .await
}

async fn await_anext(ctx: &WorkCtx, agen: &PyValue) -> PyValueResult {
    await_bridged(
        &ctx.locals,
        &ctx.cancel,
        ctx.timeout,
        &ctx.py_task,
        &ctx.event_loop,
        |py| Ok(agen.bind(py).call_method0("__anext__")?.unbind()),
    )
    .await
}

async fn acquire_permit(
    semaphore: Arc<Semaphore>,
    cancel: &CancellationToken,
) -> PyResult<OwnedSemaphorePermit> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(cancelled_error()),
        permit = semaphore.acquire_owned() => {
            permit.map_err(|_| PyValueError::new_err("runtime semaphore closed"))
        }
    }
}

fn extract_tasks(tasks: &Bound<'_, PyAny>) -> PyResult<Vec<Py<Task>>> {
    let cap = tasks.len().unwrap_or(0);
    let mut out = Vec::with_capacity(cap);
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

fn submit_all(
    rt: &Bound<'_, Runtime>,
    py: Python<'_>,
    tasks: &Bound<'_, PyAny>,
) -> PyResult<(TaskLocals, Vec<Handle>)> {
    rt.get().inner.ensure_open()?;
    let locals = get_current_locals(py)?;
    let specs = extract_tasks(tasks)?;
    let mut handles = Vec::with_capacity(specs.len());
    for spec in specs {
        handles.push(submit_spec(rt, py, &spec)?);
    }
    Ok((locals, handles))
}

fn handle_future(
    py: Python<'_>,
    locals: &TaskLocals,
    handle: &Handle,
) -> PyResult<impl Future<Output = PyValueResult> + Send + use<>> {
    pyo3_async_runtimes::into_future_with_locals(locals, handle.clone_awaitable(py).into_bound(py))
}

fn indexed_set<F: Future>(
    futs: impl IntoIterator<Item = F>,
) -> FuturesUnordered<impl Future<Output = (usize, F::Output)>> {
    futs.into_iter()
        .enumerate()
        .map(|(index, fut)| async move { (index, fut.await) })
        .collect()
}

async fn run_stream_inner(
    mut ctx: WorkCtx,
    tx: tokio::sync::mpsc::Sender<ItemResult>,
) -> PyResult<()> {
    let _permits = acquire_execution(&mut ctx).await?;
    let agen = Python::attach(|py| {
        ctx.func
            .bind(py)
            .call(ctx.args.bind(py), None)
            .map(|value| value.unbind())
    })?;

    loop {
        if ctx.cancel.is_cancelled() {
            break Err(cancelled_error());
        }
        if tx.is_closed() {
            break Ok(());
        }
        match await_anext(&ctx, &agen).await {
            Ok(item) => {
                if tx.send(Ok(item)).await.is_err() {
                    break Ok(());
                }
            }
            Err(err) => {
                let stop = Python::attach(|py| err.is_instance_of::<PyStopAsyncIteration>(py));
                if stop {
                    break Ok(());
                }
                let _ = tx.send(Err(err)).await;
                break Ok(());
            }
        }
    }
}

fn spawn_idle_watcher(inner: Arc<Inner>) {
    let Some(ttl) = inner.idle_ttl else {
        return;
    };
    get_runtime().spawn(async move {
        if !wait_until(&inner, &inner.work_started, || {
            inner.ever_worked.load(Ordering::Acquire)
        })
        .await
        {
            return;
        }
        loop {
            if inner.is_closed() {
                return;
            }
            if inner.work_count.load(Ordering::Acquire) > 0 {
                if !wait_until(&inner, &inner.became_idle, || {
                    inner.work_count.load(Ordering::Acquire) == 0
                })
                .await
                {
                    return;
                }
                continue;
            }
            tokio::select! {
                biased;
                _ = inner.shutdown.cancelled() => return,
                _ = wait_until(&inner, &inner.work_started, || {
                    inner.work_count.load(Ordering::Acquire) > 0
                }) => {}
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

async fn wait_until(inner: &Inner, notify: &Notify, ready: impl Fn() -> bool) -> bool {
    loop {
        if inner.is_closed() {
            return false;
        }
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if ready() {
            return true;
        }
        tokio::select! {
            biased;
            _ = inner.shutdown.cancelled() => return false,
            _ = notified => {}
        }
    }
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
        let notified = inner.became_idle.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if inner.work_count.load(Ordering::Acquire) == 0 {
            return true;
        }
        match deadline {
            None => notified.await,
            Some(deadline) => {
                tokio::select! {
                    biased;
                    _ = notified => {}
                    _ = tokio::time::sleep_until(deadline) => {
                        return inner.work_count.load(Ordering::Acquire) == 0;
                    }
                }
            }
        }
    }
}
