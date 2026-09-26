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
