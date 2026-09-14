mod bridge;
mod error;
mod handle;
mod journal;
mod policy;
mod runtime;
mod stream;
mod task;

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
    #[pymodule_export]
    use crate::stream::Completion;
    #[pymodule_export]
    use crate::task::Task;
}
