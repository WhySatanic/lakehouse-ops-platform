"""Reject semantic corruptions in a downloaded historical prefix without editing it."""

from __future__ import annotations

import argparse
import copy
import importlib.util
import json
from pathlib import Path
from unittest.mock import patch


def check(base):
    path = Path(__file__).with_name("audit-customer-history-100-prefix.py")
    spec = importlib.util.spec_from_file_location("prefix_auditor", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    before = {p.relative_to(base).as_posix(): module.sha(p) for p in base.rglob("*") if p.is_file()}
    real_load = module.load
    rejected = []
    for fault in (
        "source",
        "partial_count",
        "checkpoint",
        "active_manifest",
        "period",
        "revenue",
        "storage_groups",
        "cleanup_claim",
    ):

        def faulty_load(path, *, selected=fault):
            value = copy.deepcopy(real_load(path))
            if path.name == "experiment.json":
                if selected == "source":
                    value["source_sha"] = "a" * 40
                elif selected == "cleanup_claim":
                    value["cleanup_error"] = None
            elif path.name == "checkpoint.json" and selected == "checkpoint":
                value["processed_batches"].pop(next(iter(value["processed_batches"])))
            elif path.name == "checkpoint.json.active.json" and selected == "active_manifest":
                value["batch"]["manifest_sha256"] = "a" * 64
            elif path.name == "profile.json":
                if selected == "partial_count":
                    value["samples"].pop()
                elif selected == "period":
                    value["samples"][-1]["customer_history"]["rows"][-1]["valid_to"] = (
                        "2026-12-01T00:00:00Z"
                    )
                elif selected == "revenue":
                    value["samples"][-1]["current_query"]["rows"][0]["revenue"] = 1
            if selected == "storage_groups":
                if path.name == "profile.json":
                    storage = value["samples"][-1]["warehouse_storage"]
                    storage["reference_accounting"]["groups"]["current_data"]["bytes"] += 1
                elif path.name == "sample-90.warehouse.json":
                    value["reference_accounting"]["groups"]["current_data"]["bytes"] += 1
            return value

        with patch.object(module, "load", faulty_load):
            try:
                module.audit(base)
            except AssertionError:
                rejected.append(fault)
            else:
                raise ValueError("corruption accepted: " + fault)
    assert len(rejected) == 8
    assert before == {
        p.relative_to(base).as_posix(): module.sha(p) for p in base.rglob("*") if p.is_file()
    }
    return {
        "status": "verified",
        "semantic_corruptions_rejected": rejected,
        "raw_files_unchanged": len(before),
        "method": "in-memory load substitutions",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact_directory", type=Path)
    args = parser.parse_args()
    print(json.dumps(check(args.artifact_directory.resolve()), indent=2))
