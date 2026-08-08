from __future__ import annotations

import os
import tempfile
from contextlib import suppress
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol


class BucketClient(Protocol):
    def head_bucket(self, **kwargs: Any) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    status: str
    target: str
    message: str


@dataclass(frozen=True, slots=True)
class DoctorReport:
    checks: tuple[CheckResult, ...]

    @property
    def healthy(self) -> bool:
        return all(check.status == "passed" for check in self.checks)

    def as_dict(self) -> dict[str, object]:
        return {
            "status": "ready" if self.healthy else "failed",
            "checks": [asdict(check) for check in self.checks],
        }


def check_file_landing(root: Path) -> CheckResult:
    target = str(root.resolve())
    probe: Path | None = None
    try:
        root.mkdir(parents=True, exist_ok=True)
        descriptor, name = tempfile.mkstemp(dir=root, prefix=".lakeops-doctor-", suffix=".tmp")
        probe = Path(name)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(b"lakeops-storage-probe")
            handle.flush()
            os.fsync(handle.fileno())
        probe.unlink()
    except OSError as error:
        return CheckResult(
            name="file_landing_write",
            status="failed",
            target=target,
            message=f"{type(error).__name__}: {error}",
        )
    finally:
        if probe is not None and probe.exists():
            with suppress(OSError):
                probe.unlink()

    return CheckResult(
        name="file_landing_write",
        status="passed",
        target=target,
        message="landing directory accepts durable writes",
    )


def check_s3_bucket(client: BucketClient, bucket: str) -> CheckResult:
    target = f"s3://{bucket}"
    try:
        client.head_bucket(Bucket=bucket)
    except Exception as error:
        return CheckResult(
            name="s3_bucket_access",
            status="failed",
            target=target,
            message=f"{type(error).__name__}: {error}",
        )

    return CheckResult(
        name="s3_bucket_access",
        status="passed",
        target=target,
        message="bucket exists and credentials permit access",
    )
