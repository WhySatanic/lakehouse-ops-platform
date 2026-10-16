# Observed 50-snapshot customer history

[Run 38922050656](https://github.com/WhySatanic/lakehouse-ops-platform/actions/runs/38922050656)
completed on October 16, 2026, using source
`d53976d2378ed578e7fb19df18de10c8436070a7`. This is one fresh hosted stack with
4 logical CPUs and 16765378560 B RAM. The measured source predates later main
changes. The [public report](../evidence/customer-history-50-2026-10-16.json)
records all 50 samples, manifest hashes, raw report hashes, resource provenance
and storage groups. It is an observation of this workload, not a capacity limit.

Each chronological full snapshot contains 10000 customers and 100 canonical orders.
Customer revision changes every day from February 1 through March 22: revisions
0 through 49. The ordinary nine-stage runner completed all 50 batches. For every
batch, independent SQL checks confirmed 100 orders and 277225 cents captured
revenue in detail, batch daily and current daily results. The final dimension has
500000 versions and 10000 current customers. Every period has 10000 distinct
customer IDs and 10000 names matching the input revision. All 50 manifest hashes
match the checkpoint, and each ordinary replay is idle without compute stages.

| Snapshot | History rows | Runner seconds | SCD2 stage seconds | Warehouse payload B |
|---|---:|---:|---:|---:|
| 1 | 10000 | 167.019 | 16.175 | 1964833 |
| 10 | 100000 | 170.662 | 20.192 | 30509288 |
| 25 | 250000 | 178.580 | 22.146 | 82451386 |
| 50 | 500000 | 195.225 | 26.882 | 178115713 |

The profile subprocess took 9749.685 seconds, including measurement and idle
observations. Sum of the 50 runner totals was 8993.806 seconds; runner min/median/max
were 167.019/177.084/199.598 seconds. These sequential observations mix history
depth and elapsed stack lifetime. They do not isolate the cause of increasing time,
prove an optimization, or provide repeated A/B comparisons. The largest sampled
stack memory was 4438796272 B. Docker sampling excludes inactive file cache on
Linux and is neither host RSS nor an exact simultaneous cgroup peak.

## Full retained payload

| Final group | Versions | Bytes |
|---|---:|---:|
| Current data | 620 | 85728336 |
| Retained-only data | 98 | 59289236 |
| Latest objects outside selected data/delete references | 2265 | 33098141 |
| Current/retained delete files and S3 noncurrent versions | 0 | 0 |
| Total | 2983 | 178115713 |

The third group consists of 2262 metadata objects and three zero-byte directory
objects in this inventory. Its label does not classify objects as orphaned.
An independent audit recomputed disjoint groups directly from S3 object versions
and current/retained Iceberg references for every sample, including reference size
and existence checks. Counting current data alone misses 92387377 B. This is
object payload, excluding replicas, filesystem overhead, Postgres and billing.
No expiration or data deletion was performed. This run's final SCD2 table has
69 current files; it is distinct from the earlier 33-snapshot compaction trial.

## Recheck the retained evidence

Download the existing run artifact once; do not dispatch another experiment just
to recheck JSON. The audit uses only the standard Python library:

```bash
gh run download 38922050656 --repo WhySatanic/lakehouse-ops-platform \
  --name customer-history-experiment --dir artifacts/customer-history-remote50
python docs/evidence/audit-customer-history-50.py \
  artifacts/customer-history-remote50 > artifacts/customer-history-50-audit.json
```

Run without Python's `-O` option because the audit uses assertions. It verifies
the exact source, raw profile hash, all input payload hashes and row counts,
customer IDs/names, literal period boundaries, all 50 checkpoint entries,
detail/daily/current revenue, idle behavior, storage/reference accounting and
successful owned cleanup. All 665 artifact files are hashed before and after to
confirm that the audit leaves them unchanged. Eight in-memory corruption probes
rejected wrong current revenue, period boundaries, customer-name counts, partial
profiles, idle compute, object sizes, missing objects and incorrect storage totals.
The public report records those probe outcomes; downloaded raw files were preserved.

The remote owned stack was stopped successfully and its network was absent.
Compose down retained named volumes at that time; eventual disposal of the hosted
runner is external. Uploaded JSON and logs are evidence, not a warehouse backup.

The earlier [four-snapshot baseline](customer-history-experiment.md) and local
33-snapshot recovery/maintenance trials remain separate experiments. This run
confirms the 50-snapshot point of the history-depth plan. It does not prove 100
snapshots, realistic order/customer distributions, commit-time crash recovery or
distributed exactly-once processing. A 100-snapshot run still needs an explicit
resource/time budget and a workload accepted by the experiment harness.
