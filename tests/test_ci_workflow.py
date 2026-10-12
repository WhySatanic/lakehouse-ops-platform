import json
from pathlib import Path


def test_catalog_compute_builds_keep_checksums_and_enable_bounded_endpoint_fallback():
    workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert workflow.count("--attempts 3 --maven-central-fallback") == 3
    dockerfile = Path("infra/spark/Dockerfile").read_text()
    assert "ARG MAVEN_CENTRAL_URL=https://repo1.maven.org/maven2" in dockerfile
    for digest in ("94b8e36fc329f0293d44ba9e01b784a56e9501affec1842d898144c51f6e486a",
                   "38f01da7e96850cdd05e6616d758b77b43314b712a8808e3f9a824d56976162f"):
        assert f"ADD --checksum=sha256:{digest}" in dockerfile


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


def test_scheduled_cycle_ci_processes_pending_batch_then_idles() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert workflow.index("      - name: Start commerce notification endpoint\n") < workflow.index(
        "      - name: Exercise scheduled commerce cycle against the live stack\n"
    ) < workflow.index("      - name: Seed one changed customer batch\n")
    section = workflow.split(
        "      - name: Exercise scheduled commerce cycle against the live stack\n", maxsplit=1
    )[1].split("      - name:", maxsplit=1)[0]
    assert section.count("uv run python -m lakehouse_ops.commerce_cycle") == 2
    assert section.count("--state data/state/commerce-cycle-ci.json") == 2
    assert "--state data/state/commerce-runner-ci.json" not in section
    assert "tests/integration/check_commerce_cycle.py" in section
    assert "artifacts/commerce-cycle-validation.json" in section
    upload = workflow.split("      - name: Upload core and recovery evidence\n", maxsplit=1)[1]
    upload = upload.split("      - name:", maxsplit=1)[0]
    for name in (
        "commerce-cycle.jsonl",
        "commerce-cycle-idle.jsonl",
        "commerce-cycle-validation.json",
    ):
        assert f"artifacts/{name}" in upload


def test_quality_ci_retains_commerce_fixture_scale_evidence() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
    quality = workflow.split("  quality:\n", maxsplit=1)[1].split(
        "  serving-integration:\n", maxsplit=1
    )[0]
    assert "python -m lakehouse_ops.commerce_fixture_profile" in quality
    assert "--output-root artifacts/commerce-fixture-scale-data" in quality
    assert "artifacts/commerce-fixture-scale.json" in quality
    assert "name: commerce-fixture-scale" in quality
