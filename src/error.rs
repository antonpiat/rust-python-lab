use pyo3::create_exception;
use pyo3::exceptions::PyRuntimeError;

create_exception!(
    rust_python_lab,
    QueueFull,
    PyRuntimeError,
    "The runtime admission queue is full."
);
