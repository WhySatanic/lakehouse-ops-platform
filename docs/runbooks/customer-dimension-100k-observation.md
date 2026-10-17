# Local 100000-customer dimension observation

Two chronological full snapshots completed on a fresh local reference stack on
October 17, 2026. The unchanged nine-stage commerce runner loaded 100000 customers
and 100 orders per snapshot, with customer revisions `0,1`. History grew from
100000 to 200000 rows, while current customers stayed at 100000. Each snapshot's
detail, daily and current revenue was **267012 cents for 100 orders**. Both idle
calls started no compute and preserved files and snapshot IDs.

The [derived report](../evidence/customer-dimension-100k-2026-10-17.json) retains
source, input/profile/checkpoint hashes, timings, memory, storage groups, query IDs,
literal history checks and cleanup. Raw JSON, manifests, JSONL and service/stage
logs remain local under `artifacts/customer-dimension-100k`, bound by those hashes.
This is one cardinality observation, separate from the
[10000-customer history-depth experiment](customer-history-experiment.md).

## Observed results

| Snapshot | History / current rows | Runner seconds | SCD2 stage seconds | Warehouse payload bytes | Current SCD2 files |
|---|---:|---:|---:|---:|---:|
| February 1, revision 0 | 100000 / 100000 | 106.977 | 12.805 | 17458817 | 1 |
| February 2, revision 1 | 200000 / 100000 | 112.305 | 15.056 | 42631752 | 2 |

The final listed warehouse had 132 object versions: current data accounted for
34072956 bytes, retained-only data for 8084196 bytes, and latest objects outside
selected data/delete references for 474600 bytes. These disjoint groups sum to
42631752 bytes. Current/retained delete files and S3 noncurrent versions were zero.
The last group includes metadata and directory objects; it is not an orphan-file
classification. Payload bytes are not physical disk usage or storage bills.

The Docker VM exposed 12 CPUs and 16637538304 bytes of memory. Spark used the
existing `local[2]` configuration and cached Spark 3.5.9 / Iceberg 1.11.0 image.
The Compose Spark services had no explicit `mem_limit`; no additional limit was
introduced. Sampled whole-project memory maxima were 3989831679 and 4324591666
bytes. Maximum observed Spark cgroup peaks were 1385873408 and 1510232064 bytes.
Sampled Docker usage is not a simultaneous RSS peak or a capacity guarantee.

The profile child took 265.162 seconds, including fixture generation, SQL and
inventory checks beyond runner time. Startup took 49.353 seconds and owned shutdown
5.213 seconds. Stage times include container/JVM startup and transformations.
The second snapshot used a warm catalog; neither snapshot proves cold host caches.
One pre-existing unrelated Trino worker remained running, so this was not an
isolated-host benchmark. All 14 pre-existing containers retained their IDs/states.

## Independent verification

The [read-only auditor](../evidence/customer-dimension-100k-audit.py.txt) verifies
both manifests and every input payload hash, all customer IDs/names, literal SCD2
periods `[Feb1,open)` followed by `[Feb1,Feb2),[Feb2,open)`, current flags and counts.
It calculates captured revenue from input payments and checks the literal 267012
against detail/daily/current SQL, all nine completed stages and checkpoint hashes.
It also reconciles versioned storage, verifies referenced objects and sizes, and
compares idle files/snapshot IDs with the pre-idle observation.

Eight [in-memory corruption probes](../evidence/customer-dimension-100k-faults.py.txt)
rejected incorrect revenue, history totals, period boundaries, matching-name counts,
partial status, idle compute, warehouse totals and a missing stage. Profile hashes
were recomputed for semantic probes so checksum rejection did not substitute for
the intended checks. All 37 raw files remained byte-identical during that audit.
Do not use Python's `-O` option: the auditor uses assertions.

## Reproduce

Use a clean checkout of measured source
`8535dc5ff28c40767d286fcd268d1e59fb215bea`, prebuilt reference images and Docker.
Copy the retained [operator](../evidence/customer-dimension-100k-operator.py.txt)
outside that checkout as `dimension100k_operator.py` and run it from the checkout:

```powershell
uv run --frozen python C:\path\to\dimension100k_operator.py
```

The operator refuses an existing output directory, selects free ports and creates
one unique `lakehouse-ops-scale-dimension-*` project. It enables the existing
listing-only warehouse observer and retained-reference flags. The child has a
3600-second limit and unchanged 300-second stage limits. On failure, preserve
partial reports; a failed or timed-out run is not a completed observation.
Only its owned services/network are stopped, with named volumes retained.

Copy the auditor as `verify_dimension100k.py`, then run:

```powershell
uv run --frozen python C:\path\to\verify_dimension100k.py `
  --root artifacts/customer-dimension-100k --output artifacts/dimension100k-audit.json
```

The historical corruption driver expects this auditor at
`../../work/verify_dimension100k.py` relative to the checkout. It uses the same raw
directory and writes `artifacts/dimension100k-faults.json`; it does not edit raw data.
The `.py.txt` files preserve exact experiment/audit source bytes outside runtime
source discovery. Their SHA256 values are recorded in the report.

## Limits

Changing customer cardinality changes seeded customer selection during order
generation and can change subsequent product/quantity choices. Therefore this
workload's 267012 cents differs from the 10000-customer fixture's 277225 cents.
It is not a matched A/B timing comparison or evidence of a speedup.

One fresh project and two snapshots do not establish repeatability, deep history,
concurrency, OOM/crash recovery, production capacity or distributed exactly-once.
The separate remote 100-snapshot experiment has a different workload and source;
this local result does not establish its completion. No production algorithm,
checkpoint/full-snapshot rule, release version or mandatory CI gate changed.
