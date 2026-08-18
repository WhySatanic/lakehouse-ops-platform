# Iceberg maintenance planning

`lakeops plan-iceberg-maintenance` evaluates a metadata report without changing the
table. It emits a versioned JSON plan with the observed values, policy thresholds,
reasons, and bounded proposed actions.

## Create a plan

Collect a fresh observation and pass it to the planner:

```bash
uv run lakeops collect-iceberg-metadata \
  --server http://localhost:8080 \
  --catalog lakehouse --schema silver --table weather_hourly \
  > weather-hourly-metadata.json

uv run lakeops plan-iceberg-maintenance \
  --input weather-hourly-metadata.json \
  > weather-hourly-maintenance-plan.json
```

The command exits non-zero when the input is not valid JSON, the metadata contract is
unsupported, or a policy value is invalid.

## Plan contract

Schema `1.0` includes:

- `plan_id`: a deterministic identifier derived from the observation and policy;
- `status`: `healthy`, `maintenance_recommended`, or `review_required`;
- `source`: metadata schema, collection time, table, and expected snapshot ID;
- `policy`: the complete set of thresholds used for the decision;
- `checks`: every evaluated rule with observations, thresholds, outcome, and reason;
- `actions`: proposed `rewrite_data_files` and `rewrite_manifests` actions.

Every action requires dry-run execution, pins the expected snapshot, and limits
concurrency to one job. Re-running the same report with the same policy produces the
same plan and action identifiers.

## Rules and overrides

The data-file rule recommends compaction when at least four data files have an average
size below 64 MiB. Its target size is 128 MiB. If delete files are present, the rule is
deferred because the collector's aggregate size includes both file types.

The manifest rule recommends a rewrite when there are at least eight manifests and at
least two manifests per data file. Override thresholds with
`--target-file-size-bytes`, `--small-file-ratio`, `--min-data-files`,
`--min-manifest-count`, and `--max-manifests-per-data-file`.

## Safety boundary

This release only creates plans. It does not invoke Spark procedures or mutate Iceberg
metadata. An executor must independently verify the expected snapshot and post-
conditions before it may report success.
