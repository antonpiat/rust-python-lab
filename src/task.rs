use pyo3::prelude::*;
use pyo3::types::PyTuple;

/// Spec for work to submit: callable plus args. Re-invoked on retry.
#[pyclass(frozen)]
pub struct Task {
    pub func: Py<PyAny>,
    pub args: Py<PyTuple>,
    pub timeout: Option<f64>,
}

#[pymethods]
impl Task {
    #[new]
    #[pyo3(signature = (func, *args, timeout=None))]
    fn new(func: Py<PyAny>, args: Bound<'_, PyTuple>, timeout: Option<f64>) -> Self {
        Self {
            func,
            args: args.unbind(),
            timeout,
        }
    }
}
