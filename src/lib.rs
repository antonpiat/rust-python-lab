mod bridge;
mod error;
mod handle;
mod journal;
mod policy;
mod runtime;

use pyo3::prelude::*;

/// A Python module implemented in Rust.
#[pymodule]
mod rust_python_lab {
    #[pymodule_export]
    use crate::error::QueueFull;
    #[pymodule_export]
    use crate::handle::Handle;
    #[pymodule_export]
    use crate::policy::OnFull;
    #[pymodule_export]
    use crate::policy::RetryPolicy;
    #[pymodule_export]
    use crate::runtime::Runtime;
}
