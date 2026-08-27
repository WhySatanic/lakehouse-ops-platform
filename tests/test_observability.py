import importlib.util
from pathlib import Path

ROOT = Path(__file__).parents[1]
CHECK_PATH = ROOT / "tests" / "integration" / "check_prometheus_targets.py"


def _load_checker():
    spec = importlib.util.spec_from_file_location("check_prometheus_targets", CHECK_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_prometheus_config_probes_every_expected_target() -> None:
    checker = _load_checker()
    config = (ROOT / "config" / "observability" / "prometheus.yml").read_text()

    assert {
        target for target in checker.EXPECTED_TARGETS if target in config
    } == checker.EXPECTED_TARGETS
    assert "blackbox-exporter:9115" in config


def test_successful_targets_only_returns_up_instances() -> None:
    checker = _load_checker()
    payload = {
        "status": "success",
        "data": {
            "result": [
                {"metric": {"instance": "ready"}, "value": [1, "1"]},
                {"metric": {"instance": "down"}, "value": [1, "0"]},
            ]
        },
    }

    assert checker.successful_targets(payload) == {"ready"}


def test_successful_targets_rejects_failed_api_response() -> None:
    checker = _load_checker()

    assert checker.successful_targets({"status": "error"}) == set()
