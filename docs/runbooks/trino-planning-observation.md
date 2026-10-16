# Observe Trino coordinator planning time

`TrinoClient.query_with_planning(sql)` returns a `TrinoPlanningObservation` with
the ordinary `result` and an optional `planning_time_ms`. It reads
`planningTimeMillis` from the **final** statement response, using the same request
loop, deadline, pagination checks and cancellation as `query_with_stats`.

```python
from dataclasses import asdict
from lakehouse_ops.trino import TrinoClient

with TrinoClient("http://localhost:8080", query_timeout=30) as client:
    observation = client.query_with_planning("SELECT 1")
    print(asdict(observation))
```

The metric is exposed by the pinned
[Trino 483 statement protocol](https://github.com/trinodb/trino/blob/483/client/trino-client/src/main/java/io/trino/client/StatementStats.java).
An absent field yields `None` (`null` in JSON), meaning unobserved. A reported
integer zero remains zero. Explicit null, booleans, negative values, strings and
fractional numbers are invalid observations and raise `TrinoProtocolError`.
Earlier result pages are never substituted for a missing final observation.

`query()` and `query_with_stats()` retain their existing result/statistics shape,
including strict baseline and experiment JSON contracts. They do not validate
this additional optional field. Planning observations are opt-in and must be
stored separately from those existing versioned report formats. No old report
is backfilled with invented planning times.

Planning time is the coordinator's reported counter. It is not client round-trip
time, complete query elapsed time or the sum of worker task-wall times. Keep query
IDs, SQL, final execution statistics and the observation boundary together.

## Retained commerce snapshot observation

The [recorded evidence](../evidence/trino-planning-observation-2026-10-16.json)
contains six live queries in three counterbalanced pairs on the independently
restored commerce fixture. The earlier
[compaction experiment](https://github.com/WhySatanic/lakehouse-ops-platform/pull/277)
preserved both snapshots: `4080505965285324963` with 44 current files, and
`7283836129631754092` with one. This read-only observation restarted only the owned
fixture's five core services; it performed no Spark job or maintenance action.

Each query used the same aggregate with only the snapshot selector changed:

```sql
SELECT count(*) AS versions, count_if(is_current) AS current_versions,
       sum(length(full_name)) AS name_length
FROM lakehouse.gold.dim_customers_scd2 FOR VERSION AS OF 4080505965285324963;
```

The other selector was `7283836129631754092`. Every query returned literal
`330000`, `10000`, `9360000`, independently derived from 10,000 fixture customers
and revisions `0..32`. Reading the old selector also demonstrates that this
retained snapshot remained readable after compaction. It is not a new restore or
snapshot-expiration acceptance test.

| Pair | Order | Old planning ms | New planning ms | Old elapsed ms | New elapsed ms |
|---|---|---:|---:|---:|---:|
| 1 | old, new | 18 | 9 | 673 | 115 |
| 2 | new, old | 9 | 8 | 90 | 87 |
| 3 | old, new | 9 | 10 | 123 | 91 |

Median planning time was 9 ms for both layouts: the third pair was slightly worse
after compaction. These samples do not establish a consistent planning benefit.
Physical input was 25,980,191 versus 751,825 bytes in every pair, but the first old
query includes restart/warming effects. The observation uses one warehouse and
shared process/cache state, not three independent whole-pipeline A/B runs.
Do not claim a production speedup, capacity result or PERF-05 acceptance from it.

The current file counts and all retained snapshot sets were unchanged throughout.
Scoped cleanup removed the owned containers/network and preserved all volumes;
14 unrelated container identities were unchanged. A subsequent optimization still
needs a specific hypothesis, equivalent inputs, at least three matched trials,
correctness oracles and a result that justifies the additional complexity.
