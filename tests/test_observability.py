import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).parents[1]
CHECK_PATH = ROOT / "tests" / "integration" / "check_prometheus_targets.py"
GRAFANA_CHECK_PATH = ROOT / "tests" / "integration" / "check_grafana_readiness.py"


def _load_checker():
    spec = importlib.util.spec_from_file_location("check_prometheus_targets", CHECK_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_grafana_checker():
    spec = importlib.util.spec_from_file_location("check_grafana_readiness", GRAFANA_CHECK_PATH)
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


def test_grafana_dashboard_uses_provisioned_prometheus_datasource() -> None:
    checker = _load_grafana_checker()
    dashboard_path = (
        ROOT / "config" / "observability" / "grafana" / "dashboards" / "core-readiness.json"
    )
    dashboard = json.loads(dashboard_path.read_text())

    assert dashboard["uid"] == checker.DASHBOARD_UID
    assert {panel["type"] for panel in dashboard["panels"]} == {"stat", "timeseries"}
    assert all(
        panel["datasource"]["uid"] == checker.DATASOURCE_UID for panel in dashboard["panels"]
    )
    assert {target["expr"] for panel in dashboard["panels"] for target in panel["targets"]} == {
        "min(probe_success)",
        "probe_success",
    }


def test_grafana_checker_requires_exact_provisioned_dashboard() -> None:
    checker = _load_grafana_checker()

    assert checker.dashboard_is_provisioned(
        [{"uid": checker.DASHBOARD_UID, "title": "Lakehouse Core Readiness"}]
    )
    assert not checker.dashboard_is_provisioned(
        [{"uid": "other", "title": "Lakehouse Core Readiness"}]
    )
