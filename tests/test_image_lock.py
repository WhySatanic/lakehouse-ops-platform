from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest

from lakehouse_ops.image_lock import ImageLockError, verify_image_lock

DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64


def write_fixture(root: Path) -> tuple[Path, Path, list[Path], Path]:
    lock = root / "images.lock.json"
    compose = root / "compose.yaml"
    dockerfile = root / "Dockerfile"
    upgrade = root / "upgrade.json"
    lock.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "images": [
                    {"tag": "registry/runtime:1", "digest": DIGEST_A},
                    {"tag": "registry/base:2", "digest": DIGEST_B},
                ],
            }
        ),
        encoding="utf-8",
    )
    compose.write_text(
        f"services:\n  app:\n    image: registry/runtime:1@{DIGEST_A}\n"
        "  local:\n    image: lakehouse-ops/local:1\n",
        encoding="utf-8",
    )
    dockerfile.write_text(f"FROM registry/base:2@{DIGEST_B}\n", encoding="utf-8")
    upgrade.write_text(
        json.dumps(
            {
                "source": {"image": f"registry/runtime:1@{DIGEST_A}"},
                "target": {"image": f"registry/runtime:1@{DIGEST_A}"},
            }
        ),
        encoding="utf-8",
    )
    return lock, compose, [dockerfile], upgrade


def test_verify_image_lock_covers_external_sources(tmp_path: Path) -> None:
    report = verify_image_lock(*write_fixture(tmp_path))

    assert report["status"] == "ready"
    assert report["images"] == 2
    assert report["uses"] == 4
    assert report["sources"] == {"compose": 1, "dockerfile": 1, "upgrade": 2}
    assert len(report["lock_sha256"]) == 64


def test_verify_image_lock_supports_compose_variable_default(tmp_path: Path) -> None:
    lock, compose, dockerfiles, upgrade = write_fixture(tmp_path)
    compose.write_text(
        f"services:\n  app:\n    image: ${{APP_IMAGE:-registry/runtime:1@{DIGEST_A}}}\n",
        encoding="utf-8",
    )

    report = verify_image_lock(lock, compose, dockerfiles, upgrade)

    assert report["uses"] == 4


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda paths: paths[1].write_text(
                "services:\n  app:\n    image: registry/runtime:1\n", encoding="utf-8"
            ),
            "not digest-pinned",
        ),
        (
            lambda paths: paths[1].write_text(
                f"services:\n  app:\n    image: registry/runtime:1@{DIGEST_B}\n",
                encoding="utf-8",
            ),
            "does not match lock",
        ),
        (
            lambda paths: paths[1].write_text(
                f"services:\n  app:\n    image: registry/unknown:1@{DIGEST_A}\n",
                encoding="utf-8",
            ),
            "not present in lock",
        ),
    ],
)
def test_verify_image_lock_rejects_untrusted_compose_reference(
    tmp_path: Path, mutate: Callable[[tuple[Path, Path, list[Path], Path]], object], message: str
) -> None:
    paths = write_fixture(tmp_path)
    mutate(paths)

    with pytest.raises(ImageLockError, match=message):
        verify_image_lock(*paths)


def test_verify_image_lock_rejects_unused_lock_entry(tmp_path: Path) -> None:
    lock, compose, dockerfiles, upgrade = write_fixture(tmp_path)
    dockerfiles[0].write_text(f"FROM registry/runtime:1@{DIGEST_A}\n", encoding="utf-8")

    with pytest.raises(ImageLockError, match="unused images: registry/base:2"):
        verify_image_lock(lock, compose, dockerfiles, upgrade)


def test_verify_image_lock_rejects_duplicate_lock_entry(tmp_path: Path) -> None:
    lock, compose, dockerfiles, upgrade = write_fixture(tmp_path)
    payload = json.loads(lock.read_text(encoding="utf-8"))
    payload["images"].append(payload["images"][0])
    lock.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ImageLockError, match="duplicate locked image"):
        verify_image_lock(lock, compose, dockerfiles, upgrade)


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"schema_version": "2.0", "images": []}, "schema_version"),
        ({"schema_version": "1.0", "images": []}, "non-empty images"),
        ({"schema_version": "1.0", "images": [None]}, "must be objects"),
        (
            {
                "schema_version": "1.0",
                "images": [{"tag": "registry/runtime", "digest": DIGEST_A}],
            },
            "explicit tag",
        ),
        (
            {
                "schema_version": "1.0",
                "images": [{"tag": "registry/runtime:1", "digest": "sha256:bad"}],
            },
            "invalid digest",
        ),
    ],
)
def test_verify_image_lock_rejects_invalid_lock(
    tmp_path: Path, payload: dict[str, object], message: str
) -> None:
    lock, compose, dockerfiles, upgrade = write_fixture(tmp_path)
    lock.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ImageLockError, match=message):
        verify_image_lock(lock, compose, dockerfiles, upgrade)


def test_verify_image_lock_requires_dockerfile_base(tmp_path: Path) -> None:
    lock, compose, dockerfiles, upgrade = write_fixture(tmp_path)
    dockerfiles[0].write_text("RUN true\n", encoding="utf-8")

    with pytest.raises(ImageLockError, match="no FROM instruction"):
        verify_image_lock(lock, compose, dockerfiles, upgrade)


def test_verify_image_lock_rejects_invalid_upgrade_plan(tmp_path: Path) -> None:
    lock, compose, dockerfiles, upgrade = write_fixture(tmp_path)
    upgrade.write_text("{}", encoding="utf-8")

    with pytest.raises(ImageLockError, match="cannot read upgrade image references"):
        verify_image_lock(lock, compose, dockerfiles, upgrade)


def test_verify_image_lock_reports_missing_source(tmp_path: Path) -> None:
    lock, compose, dockerfiles, upgrade = write_fixture(tmp_path)
    compose.unlink()

    with pytest.raises(ImageLockError, match="cannot read image source"):
        verify_image_lock(lock, compose, dockerfiles, upgrade)
