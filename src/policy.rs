use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyclass(eq, eq_int, frozen, from_py_object, name = "OnFull")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OnFull {
    REJECT,
    WAIT,
}

#[pyclass(frozen, from_py_object)]
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    #[pyo3(get)]
    max_attempts: u32,
    #[pyo3(get)]
    backoff_ms: u64,
    #[pyo3(get)]
    backoff_multiplier: f64,
    #[pyo3(get)]
    max_backoff_ms: u64,
    #[pyo3(get)]
    jitter: bool,
}

#[pymethods]
impl RetryPolicy {
    #[new]
    #[pyo3(signature = (
        max_attempts=3,
        backoff_ms=100,
        backoff_multiplier=2.0,
        max_backoff_ms=5000,
        jitter=true
    ))]
    fn new(
        max_attempts: u32,
        backoff_ms: u64,
        backoff_multiplier: f64,
        max_backoff_ms: u64,
        jitter: bool,
    ) -> PyResult<Self> {
        if max_attempts == 0 {
            return Err(PyValueError::new_err("max_attempts must be at least 1"));
        }
        if !backoff_multiplier.is_finite() || backoff_multiplier < 1.0 {
            return Err(PyValueError::new_err(
                "backoff_multiplier must be finite and >= 1",
            ));
        }
        Ok(Self {
            max_attempts,
            backoff_ms,
            backoff_multiplier,
            max_backoff_ms,
            jitter,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub backoff: Duration,
    pub multiplier: f64,
    pub max_backoff: Duration,
    pub jitter: bool,
}

impl RetryConfig {
    pub fn none() -> Self {
        Self {
            max_attempts: 1,
            backoff: Duration::from_millis(0),
            multiplier: 1.0,
            max_backoff: Duration::from_millis(0),
            jitter: false,
        }
    }

    pub fn from_policy(policy: &RetryPolicy) -> Self {
        Self {
            max_attempts: policy.max_attempts,
            backoff: Duration::from_millis(policy.backoff_ms),
            multiplier: policy.backoff_multiplier,
            max_backoff: Duration::from_millis(policy.max_backoff_ms),
            jitter: policy.jitter,
        }
    }

    pub fn backoff_for(&self, attempt: u32, task_id: u64) -> Duration {
        if attempt == 0 || self.backoff.is_zero() {
            return Duration::ZERO;
        }
        let mut ms = self.backoff.as_millis() as f64
            * self.multiplier.powi(attempt.saturating_sub(1) as i32);
        let cap = self.max_backoff.as_millis() as f64;
        if cap > 0.0 {
            ms = ms.min(cap);
        }
        if self.jitter {
            let factor = 0.5 + ((task_id.wrapping_add(attempt as u64) * 17) % 51) as f64 / 100.0;
            ms *= factor;
        }
        Duration::from_millis(ms.max(0.0) as u64)
    }
}
