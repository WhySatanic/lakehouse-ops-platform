# Compaction of retained commerce customer history

One guarded `rewrite_data_files` action compacted the existing 33-batch customer
history on an independently restored local fixture. Current customer-history files
fell from 44 to 1, while the complete listed warehouse payload **increased** by
25,754,035 bytes. Compaction improves the current layout without reclaiming files
still referenced by retained historical snapshots.

The [derived evidence](../evidence/commerce-maintenance-2026-10-16.json) includes
the exact plan and action, dry-run and applied reports, literal SQL and query IDs,
before/after table inventories, object-version accounting and independent checks.
Complete raw object listings remain local, identified by report and canonical
object-list hashes. The evidence is an observed experiment, not a new scheduler,
automatic retention policy or capacity guarantee.

## Scope and recovery prerequisite

Runtime source was `cca72020cdfd480de6f7a7c0025973df848e0849`, with the existing
Compose configuration, memory overlay and cached images. The source fixture
contains 33 accepted full snapshots with 10,000 customers each, revisions `0..32`,
and 100 orders per batch. A completed
[cold restore rehearsal](https://github.com/WhySatanic/lakehouse-ops-platform/pull/276)
had independently compared all six restored volumes, archive members and file
hashes, then served the literal SQL oracles and an idle runner replay.

This experiment used only that owned restored project
`lakehouse-ops-scale-restore-d726818e472145ebab90dfa06198972d`.
The six original volumes, original continuation checkpoint/journal and all six
backup archives remained unchanged; their hashes and existence were checked again.
A fresh working checkpoint and SQLite backup were copied into the experiment
output. Separate local locks do not permit concurrent writers to either warehouse.

Before startup, all containers were inspected for users of the restored volumes.
The destination had six expected volumes and no containers. Only its five core
services and one maintenance job at a time were started, without builds or pulls.
All source/silver business-history rows and landing payloads were retained.

## Exact guarded action

Fresh metadata for `lakehouse.gold.dim_customers_scd2` produced plan
`plan-9e897a06bc3781ce`, action `action-4d809259b3a550d1`.
It pinned snapshot `4080505965285324963`. The unchanged planner's default target
was 128 MiB; the executor bounded concurrency and file groups using the existing
[rewrite contract](iceberg-data-file-rewrite.md).

The dry-run reported `dry_run`, `applied: false`, identical before/after state and
unchanged live table file/snapshot inventories. Only after examining that report
did the operator apply the exact same plan/action/snapshot approvals. The job
reported `succeeded`, 44 rewritten files, one added file and zero failed files.
The new snapshot was `7283836129631754092`; all 33 previous snapshot IDs remained.
No expiration, orphan removal or other planned action was executed.

To reproduce on a separately owned grown fixture, first complete its restore
rehearsal and record the source inputs, volumes, state and SQL oracles. Collect
fresh metadata and create a plan using the
[planning commands](iceberg-maintenance-planning.md). Select its single
`rewrite_data_files` action, pass the exact plan path and action ID to
`spark-maintenance`, and retain the dry-run report. Recheck unchanged live state,
then apply with the same reviewed identifiers and `MAINTENANCE_APPLY=true`.
Never reuse this experiment's snapshot/action IDs on a different table state.
The JSON evidence records this invocation's complete parameters and safety bounds.

## Measured layout and retained cost

| Metric | Before | After |
|---|---:|---:|
| Current customer-history rows | 330,000 | 330,000 |
| Current customer versions | 10,000 | 10,000 |
| Current customer-history data files | 44 | 1 |
| Current customer-history file bytes | 25,980,191 | 25,443,395 |
| Current customer-history manifests | 32 | 33 |
| Customer-history snapshots | 33 | 34 |
| All commerce current data bytes | 56,786,312 | 56,249,516 |
| Data bytes referenced only by retained snapshots | 38,064,968 | 64,045,159 |
| Latest bytes outside selected data/delete references | 17,047,167 | 17,357,807 |
| Complete listed warehouse payload bytes | 111,898,447 | 137,652,482 |

The last unmatched-reference group includes metadata; it is not an orphan delete
list. Current-delete, retained-delete and noncurrent-S3 payload groups were zero.
The independent verifier reconciled all groups to complete versioned listings and
confirmed every previously listed object/version still existed unchanged afterward.
The 43 fewer current files therefore coexist with approximately 23% more retained
payload. These are object payload bytes, not physical disk usage or storage bills.

## SQL, repeat reads and limits

The existing literal history oracle checked every customer's ID, expected name,
33 validity periods and current flag before and after. All 330,000 rows matched;
current gold remained 100 orders and 277,225 captured revenue cents. File counts,
byte totals and snapshot IDs of every other commerce table were unchanged.

Two identical SQL queries were repeated three times before and three times after:

```sql
SELECT count(*) AS versions, count_if(is_current) AS current_versions
FROM lakehouse.gold.dim_customers_scd2;

SELECT count(*) AS versions
FROM lakehouse.gold.dim_customers_scd2
WHERE customer_id = 'C00000001';
```

| Median per repeated SQL | Before | After |
|---|---:|---:|
| All-period count: reported elapsed ms | 77 | 48 |
| All-period count: physical input bytes | 25,980,191 | 50,080 |
| One-customer period count: reported elapsed ms | 200 | 74 |
| One-customer period count: physical input bytes | 6,518,557 | 629,430 |

The one-customer query returned 33 in every repetition. Query IDs, individual
samples and CPU/worker-wall counters are retained. Worker-wall totals sum task
work and must not be presented as end-to-end latency. This fixed before-then-after
order does not control warming, cache state or other time effects; it is not three
independent randomized A/B pairs. No isolated planning-time metric was collected,
and no production speedup, planning improvement or PERF-05 acceptance is claimed.
Use a separate matched experiment before introducing an optimization on that basis.

Finally, the ordinary runner returned idle with no stages and unchanged checkpoint,
files and snapshot sets. The owned project was torn down without deleting volumes;
all 14 unrelated container IDs were unchanged. Original state and backup archives
remain valid recovery inputs. The restored copy now includes the compaction snapshot
and must not be mistaken for its pre-maintenance state. Preserve the
[retention contract](commerce-retention.md); snapshot expiration requires its own
fresh exact-ID plan, dry-run, historical-read verification and explicit review.
