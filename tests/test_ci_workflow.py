from pathlib import Path


def test_lakehouse_integration_timeout_covers_observed_runtime() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
    job = workflow.split("  lakehouse-integration:\n", maxsplit=1)[1]
    timeout_line = next(
        line for line in job.splitlines() if line.startswith("    timeout-minutes:")
    )

    assert int(timeout_line.split(":", maxsplit=1)[1]) >= 30
