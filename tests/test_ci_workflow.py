import json
from pathlib import Path


def test_lakehouse_integration_timeout_covers_observed_runtime() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
    job = workflow.split("  lakehouse-integration:\n", maxsplit=1)[1]
    timeout_line = next(
        line for line in job.splitlines() if line.startswith("    timeout-minutes:")
    )

    assert int(timeout_line.split(":", maxsplit=1)[1]) >= 30


def test_trino_upgrade_source_stays_pinned_through_gold_query() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
    source = json.loads(Path("config/trino/upgrade-rehearsal.json").read_text(encoding="utf-8"))[
        "source"
    ]["image"]

    for step in (
        "Start Trino coordinator and workers",
        "Query Iceberg tables through Trino",
        "Query commerce daily gold through Trino",
    ):
        section = workflow.split(f"      - name: {step}\n", maxsplit=1)[1].split(
            "      - name:", maxsplit=1
        )[0]
        assert f"TRINO_SERVER_IMAGE: {source}" in section


def test_commerce_runner_ci_retains_transient_recovery_and_idle_evidence() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
    section = workflow.split(
        "      - name: Run one pending commerce batch with transient Trino recovery "
        "and idle rerun\n",
        maxsplit=1,
    )[1].split("      - name:", maxsplit=1)[0]
    assert "tests/integration/exercise_commerce_runner_retry.py" in section
    assert "--retry-evidence artifacts/commerce-runner-retry.json" in section
    assert "--attempts 3 --retry-delay-seconds 0" in section
    assert "uv run lakeops run-commerce-batch" in section
    assert "artifacts/commerce-runner-idle.json" in section
    upload = workflow.split("      - name: Upload core and recovery evidence\n", maxsplit=1)[1]
    upload = upload.split("      - name:", maxsplit=1)[0]
    for name in ("commerce-runner", "commerce-runner-idle", "commerce-runner-retry"):
        assert f"artifacts/{name}.json" in upload
