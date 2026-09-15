use std::sync::Arc;

use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::{future_into_py_with_locals, get_current_locals};
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::bridge::{PyValue, PyValueResult};

pub type ItemResult = PyValueResult;
pub type CompletionItem = (usize, ItemResult);

type SharedRx<T> = Arc<AsyncMutex<Option<mpsc::Receiver<T>>>>;

/// One finished gather/as_completed item, in completion order.
#[pyclass(frozen)]
pub struct Completion {
    #[pyo3(get)]
    index: usize,
    #[pyo3(get)]
    ok: bool,
    #[pyo3(get)]
    value: PyValue,
}

impl Completion {
    pub fn new(py: Python<'_>, index: usize, result: ItemResult) -> Self {
        match result {
            Ok(value) => Self {
                index,
                ok: true,
                value,
            },
            Err(err) => Self {
                index,
                ok: false,
                value: err.value(py).clone().into_any().unbind(),
            },
        }
    }
}

#[pyclass]
pub struct CompletionStream {
    rx: SharedRx<CompletionItem>,
}

impl CompletionStream {
    pub fn new(rx: mpsc::Receiver<CompletionItem>) -> Self {
        Self { rx: share_rx(rx) }
    }
}

#[pymethods]
impl CompletionStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        poll_rx(py, Arc::clone(&self.rx), |(index, result)| {
            Python::attach(|py| {
                Ok(Bound::new(py, Completion::new(py, index, result))?
                    .into_any()
                    .unbind())
            })
        })
    }
}

#[pyclass]
pub struct ItemStream {
    rx: SharedRx<ItemResult>,
    cancel: CancellationToken,
}

impl ItemStream {
    pub fn new(rx: mpsc::Receiver<ItemResult>, cancel: CancellationToken) -> Self {
        Self {
            rx: share_rx(rx),
            cancel,
        }
    }
}

#[pymethods]
impl ItemStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn cancel(&self) {
        self.cancel.cancel();
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        poll_rx(py, Arc::clone(&self.rx), |result| result)
    }
}

pub fn item_channel(buffer: usize) -> (mpsc::Sender<ItemResult>, mpsc::Receiver<ItemResult>) {
    mpsc::channel(buffer.max(1))
}

pub fn completion_channel(
    n: usize,
) -> (mpsc::Sender<CompletionItem>, mpsc::Receiver<CompletionItem>) {
    mpsc::channel(n.max(1))
}

fn share_rx<T>(rx: mpsc::Receiver<T>) -> SharedRx<T> {
    Arc::new(AsyncMutex::new(Some(rx)))
}

fn poll_rx<'py, T, F>(py: Python<'py>, rx: SharedRx<T>, map: F) -> PyResult<Bound<'py, PyAny>>
where
    T: Send + 'static,
    F: FnOnce(T) -> PyResult<PyValue> + Send + 'static,
{
    let locals = get_current_locals(py)?;
    future_into_py_with_locals(py, locals, async move {
        let mut guard = rx.lock().await;
        let Some(channel) = guard.as_mut() else {
            return Err(PyStopAsyncIteration::new_err(()));
        };
        match channel.recv().await {
            Some(item) => map(item),
            None => {
                *guard = None;
                Err(PyStopAsyncIteration::new_err(()))
            }
        }
    })
}
