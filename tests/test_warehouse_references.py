from __future__ import annotations

import pytest

from lakehouse_ops.warehouse_references import file_references, reconcile_references


def reference(key, size, content=0):
    return {"file_path": "s3a://lakehouse/warehouse/" + key, "bytes": size, "content": content}


def object_version(key, size, latest=True):
    return {"key": "warehouse/" + key, "bytes": size, "is_latest": latest,
            "version_id": "latest" if latest else "previous"}


def test_payload_partition_includes_retained_deletes_without_double_counting_shared_files():
    current = [reference("live", 10), reference("position", 2, 1)]
    retained = [*current, reference("old", 7), reference("equality", 3, 2)]
    tables = [{"table": "silver.a", "current": current, "retained": retained},
              {"table": "gold.shared", "current": current, "retained": retained}]
    inventory = {"status": "observed", "prefix": "warehouse/", "bucket": "lakehouse",
                 "total_payload_bytes": 40, "objects": [
                     object_version("live", 10), object_version("live", 13, False),
                     object_version("position", 2), object_version("old", 7),
                     object_version("equality", 3), object_version("metadata/a.json", 5),
                 ]}
    report = reconcile_references(inventory, tables)
    assert report["groups"] == {
        "current_data": {"versions": 1, "bytes": 10},
        "current_delete": {"versions": 1, "bytes": 2},
        "retained_only_data": {"versions": 1, "bytes": 7},
        "retained_only_delete": {"versions": 1, "bytes": 3},
        "latest_not_in_selected_file_references": {"versions": 1, "bytes": 5},
        "s3_noncurrent": {"versions": 1, "bytes": 13},
    }
    assert report["orphan_classification"] == "not_performed"


@pytest.mark.parametrize("fault", ["missing", "size", "not-retained", "conflict", "total"])
def test_inconsistent_reference_accounting_fails_instead_of_completing(fault):
    row = reference("file", 10)
    tables = [{"table": "silver.a", "current": [row], "retained": [row]}]
    objects = [object_version("file", 9 if fault == "size" else 10,
                              latest=fault != "missing")]
    if fault == "not-retained":
        tables[0]["retained"] = []
    if fault == "conflict":
        tables.append({"current": [reference("file", 11)], "retained": []})
    inventory = {"status": "observed", "prefix": "warehouse/", "bucket": "lakehouse",
                 "total_payload_bytes": 11 if fault == "total" else objects[0]["bytes"],
                 "objects": objects}
    with pytest.raises(ValueError):
        reconcile_references(inventory, tables)


@pytest.mark.parametrize("path", ["s3://other/warehouse/file", "file:///warehouse/file",
                                  "s3://lakehouse/landing/file", "s3://lakehouse/warehouse/a%20b",
                                  "s3://lakehouse/warehouse/file?versionId=old"])
def test_ambiguous_or_out_of_scope_locations_are_not_guessed(path):
    row = {"file_path": path, "bytes": 10, "content": 0}
    with pytest.raises(ValueError, match="URI scope"):
        reconcile_references({"status": "observed", "prefix": "warehouse/",
                              "bucket": "lakehouse", "objects": [], "total_payload_bytes": 0},
                             [{"current": [], "retained": [row]}])


@pytest.mark.parametrize("fault", [None, "snapshots", "files", "current", "duplicate",
                                   "content", "size"])
def test_catalog_queries_quote_identifiers_and_reject_partial_evidence(fault):
    queries = []

    class Client:
        def query(self, sql):
            queries.append(sql)
            assert 'lakehouse."silver"."commerce_odd""name$' in sql
            if "$snapshots" in sql:
                return [{"snapshot_id": n} for n in range(101 if fault == "snapshots" else 2)]
            row = reference("live", -1 if fault == "size" else 10,
                             content=3 if fault == "content" else 0)
            if "$all_entries" in sql:
                assert "WHERE status IN (0, 1) LIMIT 100001" in sql
                if fault == "current":
                    return []
                if fault == "files":
                    return [row] * 100001
                return [row, {**row, "bytes": 11}] if fault == "duplicate" else [row, row]
            assert "file_size_in_bytes AS bytes, content" in sql
            return [row]

    if fault:
        with pytest.raises(ValueError):
            file_references(Client(), [{"table_schema": "silver",
                                        "table_name": 'commerce_odd"name'}])
    else:
        result = file_references(Client(), [{"table_schema": "silver",
                                            "table_name": 'commerce_odd"name'}])
        assert len(result[0]["retained"]) == len(result[0]["current"]) == 1
        assert len(queries) == 3


def test_table_limit_prevents_any_catalog_requests():
    with pytest.raises(ValueError, match="32 tables"):
        file_references(None, [{}] * 33)
