import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).parents[1]
CHECK_PATH = ROOT / "tests" / "integration" / "check_prometheus_targets.py"
GRAFANA_CHECK_PATH = ROOT / "tests" / "integration" / "check_grafana_readiness.py"
WORKLOAD_CHECK_PATH = ROOT / "tests" / "integration" / "check_grafana_workload.py"
OPERATIONAL_CHECK_PATH = (
    ROOT / "tests" / "integration" / "check_grafana_operational.py"
)
ALERT_CHECK_PATH = ROOT / "tests" / "integration" / "check_alert_delivery.py"


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


def _load_workload_checker():
    spec = importlib.util.spec_from_file_location("check_grafana_workload", WORKLOAD_CHECK_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_operational_checker():
    spec = importlib.util.spec_from_file_location(
        "check_grafana_operational", OPERATIONAL_CHECK_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_alert_checker():
    spec = importlib.util.spec_from_file_location("check_alert_delivery", ALERT_CHECK_PATH)
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


def test_prometheus_scrapes_trino_workload_with_dedicated_identity() -> None:
    config = (ROOT / "config" / "observability" / "prometheus.yml").read_text()

    assert "job_name: trino-workload" in config
    assert "metrics_path: /metrics" in config
    assert "username: lakehouse-observer" in config
    assert "trino-coordinator:8080" in config


def test_prometheus_scrapes_operational_metrics_exporter() -> None:
    config = (ROOT / "config" / "observability" / "prometheus.yml").read_text()

    assert "job_name: lakehouse-operational" in config
    assert "lakehouse-metrics-exporter:9108" in config


def test_operational_metrics_use_isolated_read_only_identity() -> None:
    groups = json.loads(
        (ROOT / "infra" / "trino" / "coordinator" / "resource-groups.json").read_text()
    )
    operational = next(
        group
        for group in groups["rootGroups"][0]["subGroups"]
        if group["name"] == "operational"
    )
    selector = next(
        selector
        for selector in groups["selectors"]
        if selector.get("user") == "lakehouse-operational-metrics"
    )
    policy = json.loads((ROOT / "config" / "access" / "role-policy.json").read_text())
    binding = next(
        binding
        for binding in policy["bindings"]
        if "lakehouse-operational-metrics" in binding["users"]
    )

    assert operational["hardConcurrencyLimit"] == 1
    assert operational["maxQueued"] == 1
    assert selector["group"] == "global.operational"
    assert binding["roles"] == ["operations_reader"]


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


def test_grafana_workload_dashboard_uses_query_manager_metrics() -> None:
    checker = _load_workload_checker()
    dashboard_path = (
        ROOT
        / "config"
        / "observability"
        / "grafana"
        / "dashboards"
        / "trino-workload.json"
    )
    dashboard = json.loads(dashboard_path.read_text())
    expressions = {
        target["expr"] for panel in dashboard["panels"] for target in panel["targets"]
    }

    assert dashboard["uid"] == checker.DASHBOARD_UID
    assert dashboard["title"] == checker.DASHBOARD_TITLE
    assert {panel["type"] for panel in dashboard["panels"]} == {"stat", "timeseries"}
    assert all(
        panel["datasource"]["uid"] == checker.DATASOURCE_UID
        for panel in dashboard["panels"]
    )
    assert {
        "trino_execution_name_QueryManager_RunningQueries",
        "trino_execution_name_QueryManager_QueuedQueries",
        "trino_execution_name_QueryManager_WaitingForResourcesQueries",
        "rate(trino_execution_name_QueryManager_StartedQueries[5m])",
        "rate(trino_execution_name_QueryManager_CompletedQueries[5m])",
        "rate(trino_execution_name_QueryManager_FailedQueries[5m])",
    } <= expressions


def test_grafana_workload_checker_requires_dashboard_and_numeric_sample() -> None:
    checker = _load_workload_checker()

    assert checker.dashboard_is_provisioned(
        [{"uid": checker.DASHBOARD_UID, "title": checker.DASHBOARD_TITLE}]
    )
    assert not checker.dashboard_is_provisioned(
        [{"uid": checker.DASHBOARD_UID, "title": "Other"}]
    )
    assert checker.sample_at_least(
        {"status": "success", "data": {"result": [{"value": [1, "2"]}]}}, 1
    )
    assert not checker.sample_at_least(
        {"status": "success", "data": {"result": [{"value": [1, "NaN"]}]}}, 0
    )
    assert not checker.sample_at_least({"status": "error"}, 0)


def test_grafana_operational_dashboard_uses_live_table_metrics() -> None:
    checker = _load_operational_checker()
    dashboard_path = (
        ROOT
        / "config"
        / "observability"
        / "grafana"
        / "dashboards"
        / "maintenance-freshness.json"
    )
    dashboard = json.loads(dashboard_path.read_text())
    expressions = {
        target["expr"] for panel in dashboard["panels"] for target in panel["targets"]
    }

    assert dashboard["uid"] == checker.DASHBOARD_UID
    assert dashboard["title"] == checker.DASHBOARD_TITLE
    assert {panel["type"] for panel in dashboard["panels"]} == {"stat", "timeseries"}
    assert all(
        panel["datasource"]["uid"] == checker.DATASOURCE_UID
        for panel in dashboard["panels"]
    )
    assert {
        "lakehouse_ingestion_freshness_age_seconds",
        "1000 * lakehouse_ingestion_latest_timestamp_seconds",
        "lakehouse_maintenance_data_files",
        "lakehouse_maintenance_small_file_backlog",
        "lakehouse_maintenance_snapshots",
        "lakehouse_operational_collector_success",
    } <= expressions


def test_grafana_operational_checker_requires_live_metrics() -> None:
    checker = _load_operational_checker()

    assert checker.dashboard_is_provisioned(
        [{"uid": checker.DASHBOARD_UID, "title": checker.DASHBOARD_TITLE}]
    )
    assert not checker.dashboard_is_provisioned(
        [{"uid": checker.DASHBOARD_UID, "title": "Other"}]
    )
    assert checker.sample_at_least(
        {"status": "success", "data": {"result": [{"value": [1, "1"]}]}}, 1
    )
    assert not checker.sample_at_least(
        {"status": "success", "data": {"result": [{"value": [1, "NaN"]}]}}, 0
    )
    assert not checker.sample_at_least({"status": "error"}, 0)


def test_alert_rule_has_actionable_target_context() -> None:
    rules = (ROOT / "config" / "observability" / "alerts.yml").read_text()
    prometheus = (ROOT / "config" / "observability" / "prometheus.yml").read_text()
    alertmanager = (ROOT / "config" / "observability" / "alertmanager.yml").read_text()

    assert "alert: LakehouseCoreTargetDown" in rules
    assert "expr: probe_success == 0" in rules
    assert "{{ $labels.instance }}" in rules
    assert "/etc/prometheus/rules/*.yml" in prometheus
    assert "alertmanager:9093" in prometheus
    assert "http://alert-webhook:8080/alerts" in alertmanager
    assert "send_resolved: true" in alertmanager


def test_alert_checker_matches_exact_status_and_target() -> None:
    checker = _load_alert_checker()
    events = [
        {
            "alerts": [
                {
                    "status": "firing",
                    "labels": {
                        "alertname": checker.ALERT_NAME,
                        "instance": checker.DEFAULT_TARGET,
                    },
                }
            ]
        }
    ]

    assert checker.has_matching_alert(events, "firing", checker.DEFAULT_TARGET)
    assert not checker.has_matching_alert(events, "resolved", checker.DEFAULT_TARGET)
    assert not checker.has_matching_alert(events, "firing", "http://other:8080/v1/info")
