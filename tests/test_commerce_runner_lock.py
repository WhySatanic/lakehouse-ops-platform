from __future__ import annotations

import errno
import subprocess
import sys
from pathlib import Path
from typing import BinaryIO

import pytest

from lakehouse_ops import commerce_runner_lock as locking

CHILD = """
import sys
from pathlib import Path
from lakehouse_ops.commerce_runner_lock import commerce_runner_lock
with commerce_runner_lock(Path(sys.argv[1])):
    print('acquired')
"""


def child(path: Path, code: str = CHILD) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", code, str(path)],
        capture_output=True, text=True, timeout=15, check=False,
    )


def test_lock_rejects_other_process_then_allows_it_after_release(tmp_path: Path) -> None:
    path = tmp_path / "state" / "runner.lock"
    with locking.commerce_runner_lock(path):
        result = child(path)
        assert result.returncode != 0
        assert "CommerceRunnerBusyError" in result.stderr
        assert result.stdout == ""
    assert path.exists()
    result = child(path)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "acquired"


def test_process_death_releases_os_lock_without_removing_file(tmp_path: Path) -> None:
    path = tmp_path / "runner.lock"
    result = child(path, """
import os, sys
from pathlib import Path
from lakehouse_ops.commerce_runner_lock import commerce_runner_lock
guard = commerce_runner_lock(Path(sys.argv[1]))
guard.__enter__()
os._exit(0)
""")
    assert result.returncode == 0, result.stderr
    assert path.exists()
    with locking.commerce_runner_lock(path):
        pass


def test_exception_releases_lock_and_preserves_lock_file(tmp_path: Path) -> None:
    path = tmp_path / "runner.lock"
    with pytest.raises(RuntimeError, match="failed stage"), locking.commerce_runner_lock(path):
        raise RuntimeError("failed stage")
    with locking.commerce_runner_lock(path):
        pass
    assert path.read_bytes() == b"\0"


def test_unexpected_lock_error_is_not_reported_as_contention(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    def fail(handle: BinaryIO) -> None:
        raise OSError(errno.EIO, "storage error")

    monkeypatch.setattr(locking, "_lock", fail)
    with (
        pytest.raises(OSError, match="storage error"),
        locking.commerce_runner_lock(tmp_path / "runner.lock"),
    ):
        pytest.fail("must not start work")
