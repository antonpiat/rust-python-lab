from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_package_ships_typing_marker():
    import rust_python_lab

    package_dir = Path(rust_python_lab.__file__).resolve().parent
    assert (package_dir / "py.typed").is_file()
    assert (package_dir / "__init__.pyi").is_file()


def test_agent_tools_example():
    result = subprocess.run(
        [sys.executable, str(ROOT / "examples" / "agent_tools.py")],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert "tools:" in result.stdout
    assert "llm tokens:" in result.stdout


def test_live_llm_skips_without_keys(monkeypatch: pytest.MonkeyPatch):
    env = os.environ.copy()
    env.pop("OPENAI_API_KEY", None)
    env.pop("ANTHROPIC_API_KEY", None)
    result = subprocess.run(
        [sys.executable, str(ROOT / "examples" / "live_llm.py")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
    )
    assert result.returncode == 0
    assert "skip" in result.stdout.lower()


def test_bench_fanout_smoke():
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "bench_fanout.py"),
            "--n",
            "20",
            "--concurrency",
            "4",
            "--delay",
            "0",
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert "runtime gather:" in result.stdout
    assert "asyncio gather+sem:" in result.stdout


@pytest.mark.skipif(os.environ.get("LIVE_LLM") != "1", reason="set LIVE_LLM=1 to hit a provider")
@pytest.mark.skipif(
    not (os.environ.get("OPENAI_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")),
    reason="no API key",
)
def test_live_llm_example():
    subprocess.run(
        [sys.executable, str(ROOT / "examples" / "live_llm.py")],
        cwd=ROOT,
        check=True,
        timeout=120,
    )
