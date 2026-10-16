from __future__ import annotations

import errno
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier

import pytest

from lakehouse_ops.ingestion import landing
from lakehouse_ops.ingestion.models import Location, WeatherPayload


@pytest.mark.parametrize("attempt", range(3))
def test_two_synchronized_writers_preserve_the_first_published_body(
    tmp_path, valid_source_payload, monkeypatch, attempt,
):
    at = datetime(2026, 8, 6, 10, 30, tzinfo=UTC) + timedelta(seconds=attempt)
    payloads = [WeatherPayload.from_source(Location("moscow", 55.75, 37.62),
                                         {**valid_source_payload, "generationtime_ms": n})
                for n in (1.25, 2.5)]
    times = [at, at + timedelta(seconds=1)]
    barrier, fsync = Barrier(2, timeout=10), landing.os.fsync

    def synchronized_flush(fd):
        fsync(fd)
        # Both writers saw no destination and finished their private temporary body.
        barrier.wait()

    monkeypatch.setattr(landing.os, "fsync", synchronized_flush)
    with ThreadPoolExecutor(max_workers=2) as pool:
        tasks = [pool.submit(landing.FileLandingZone(tmp_path).write, payload, ingested_at=when)
                 for payload, when in zip(payloads, times, strict=True)]
        results = [task.result(timeout=15) for task in tasks]
    assert sorted(result.created for result in results) == [False, True]
    assert results[0].path == results[1].path
    winner = next(i for i, result in enumerate(results) if result.created)
    assert results[winner].path.read_bytes() == landing.prepare_landing_object(
        payloads[winner], ingested_at=times[winner],
    ).body
    assert not list(tmp_path.rglob("*.tmp"))


def test_object_published_during_flush_is_audited_and_never_overwritten(
    tmp_path, valid_source_payload, monkeypatch,
):
    payload = WeatherPayload.from_source(Location("moscow", 55.75, 37.62), valid_source_payload)
    at = datetime(2026, 8, 6, tzinfo=UTC)
    destination = tmp_path / landing.prepare_landing_object(payload, ingested_at=at).key
    fsync = landing.os.fsync

    def concurrent_publication(fd):
        fsync(fd)
        destination.write_bytes(b"foreign damaged publication")

    monkeypatch.setattr(landing.os, "fsync", concurrent_publication)
    with pytest.raises(ValueError, match="existing filesystem object conflicts"):
        landing.FileLandingZone(tmp_path).write(payload, ingested_at=at)
    assert destination.read_bytes() == b"foreign damaged publication"
    assert not list(tmp_path.rglob("*.tmp"))


def test_unsupported_exclusive_publication_fails_without_fallback_or_partial_object(
    tmp_path, valid_source_payload, monkeypatch,
):
    payload = WeatherPayload.from_source(Location("moscow", 55.75, 37.62), valid_source_payload)
    at = datetime(2026, 8, 6, tzinfo=UTC)

    def unavailable(*args):
        raise OSError(errno.EOPNOTSUPP, "hard links unavailable")

    monkeypatch.setattr(landing.os, "link", unavailable)
    with pytest.raises(OSError, match="hard links unavailable"):
        landing.FileLandingZone(tmp_path).write(payload, ingested_at=at)
    assert not (tmp_path / landing.prepare_landing_object(payload, ingested_at=at).key).exists()
    assert not list(tmp_path.rglob("*.tmp"))
