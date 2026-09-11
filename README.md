# rust-python-lab

A Rust async execution controller for Python, exposed through PyO3. Python remains the authoring surface; Rust owns scheduling, limits, and lifecycle. Python's asyncio event loop still executes coroutine bodies.

## Setup

Requires Rust (stable) and Python 3.12 (local development uses 3.12.3). CI installs CPython 3.12.

```bash
python -m venv .venv
source .venv/bin/activate
pip install maturin
maturin develop --extras dev
```

## Test

```bash
pytest
```

CI runs `maturin develop` and `pytest` on pull requests. No API keys are required.
