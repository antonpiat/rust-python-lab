use std::sync::Arc;

use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::{future_into_py_with_locals, get_current_locals};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

/// One finished gather/as_completed item, in completion order.
#[pyclass(frozen)]
pub struct Completion {
    #[pyo3(get)]
    index: usize,
    #[pyo3(get)]
    ok: bool,
    #[pyo3(get)]
    value: Py<PyAny>,
}

impl Completion {
    pub fn new(py: Python<'_>, index: usize, result: PyResult<Py<PyAny>>) -> Self {
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
    rx: Arc<Mutex<Option<mpsc::Receiver<(usize, PyResult<Py<PyAny>>)>>>>,
}

impl CompletionStream {
    pub fn new(rx: mpsc::Receiver<(usize, PyResult<Py<PyAny>>)>) -> Self {
        Self {
            rx: Arc::new(Mutex::new(Some(rx))),
        }
    }
}

#[pymethods]
impl CompletionStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let locals = get_current_locals(py)?;
        let rx = Arc::clone(&self.rx);
        future_into_py_with_locals(py, locals, async move {
            let mut guard = rx.lock().await;
            let Some(channel) = guard.as_mut() else {
                return Err(PyStopAsyncIteration::new_err(()));
            };
            match channel.recv().await {
                Some((index, result)) => Python::attach(|py| {
                    Ok(Bound::new(py, Completion::new(py, index, result))?.unbind())
                }),
                None => {
                    *guard = None;
                    Err(PyStopAsyncIteration::new_err(()))
                }
            }
        })
    }
}

pub type ItemResult = PyResult<Py<PyAny>>;

#[pyclass]
pub struct ItemStream {
    rx: Arc<Mutex<Option<mpsc::Receiver<ItemResult>>>>,
    cancel: CancellationToken,
}

impl ItemStream {
    pub fn new(rx: mpsc::Receiver<ItemResult>, cancel: CancellationToken) -> Self {
        Self {
            rx: Arc::new(Mutex::new(Some(rx))),
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
        let locals = get_current_locals(py)?;
        let rx = Arc::clone(&self.rx);
        future_into_py_with_locals(py, locals, async move {
            let mut guard = rx.lock().await;
            let Some(channel) = guard.as_mut() else {
                return Err(PyStopAsyncIteration::new_err(()));
            };
            match channel.recv().await {
                Some(Ok(value)) => Ok(value),
                Some(Err(err)) => Err(err),
                None => {
                    *guard = None;
                    Err(PyStopAsyncIteration::new_err(()))
                }
            }
        })
    }
}

pub fn item_channel(buffer: usize) -> (mpsc::Sender<ItemResult>, mpsc::Receiver<ItemResult>) {
    mpsc::channel(buffer.max(1))
}

pub fn completion_channel(
    n: usize,
) -> (
    mpsc::Sender<(usize, PyResult<Py<PyAny>>)>,
    mpsc::Receiver<(usize, PyResult<Py<PyAny>>)>,
) {
    mpsc::channel(n.max(1))
}
