use pyo3::create_exception;
use pyo3::exceptions::PyRuntimeError;

create_exception!(
    rust_python_lab,
    QueueFull,
    PyRuntimeError,
    "The runtime admission queue is full."
);

create_exception!(
    rust_python_lab,
    RuntimeClosed,
    PyRuntimeError,
    "The runtime is closed and no longer accepts work."
);
