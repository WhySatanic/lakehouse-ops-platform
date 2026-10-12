# Inspect commerce execution attempts

The opt-in Python module reads an existing SQLite execution journal in read-only
mode and reports the most recent attempts first. It never creates, repairs, clears
or advances a journal/checkpoint, starts Docker, or retries a batch.

```powershell
uv run --frozen python -m lakehouse_ops.commerce_journal_inspection `
  --journal artifacts/commerce-owner-exit-20261012/journal.sqlite3 --limit 10
```

The default limit is ten attempts, allowed range 1 through 100. Output size is
bounded by the limit, but grouping still scans journal history; this is not an
unbounded-history performance benchmark. Missing, corrupt or unsupported databases
fail with exit code 2 and no success JSON; a valid empty events table returns no runs.

Each run includes its identity, batch/manifest binding as recorded on its last
event, sequence boundaries, first/last recorded UTC timestamps, event count,
last phase/event/error and count of recorded completed checkpoint events.
The first timestamp is the first journal event, not necessarily process startup.

`outcome` follows the last event:

- `ready` or `idle`: corresponding terminal `run` event;
- `failed`: last event records a caught failure;
- `unfinished`: no terminal success/failure event, including abrupt process exit.

`unfinished` does not mean dead or safe to restart: a process can still be running.
Conversely a `ready` or `checkpoint_completed_events` entry is journal evidence,
not an independent verification of the processing checkpoint or Iceberg state.
No automatic restart decision is made. Acquire the existing runner lock and inspect
the authoritative checkpoint/catalog before a recovery operation.

The report scope is `diagnostic_journal_not_checkpoint_or_liveness`. SQL inspection
runs in one read transaction. Additional unrelated tables, including container
ownership events, do not affect this run-event summary. Container ownership is not
yet presented here; this increment is independent of the ownership-journal PR.

## Verification and upgrade

```powershell
uv run --frozen pytest tests/test_commerce_journal_inspection.py -q --no-cov
uv run --frozen python -m lakehouse_ops.local_quality
```

No data migration, default runner change, public `lakeops` command or product
version change. Remove the module to roll back; no persistent state needs reversing.

Retained local acceptance on 2026-10-12 inspected the existing owner-exit journal:
the interrupted owner had 17 events ending at `spark-commerce-daily-gold/started`
and no completed checkpoint event; a distinct retry had 23 events ending at
`run/ready` with one completed checkpoint event and the same batch/manifest binding.
SHA-256 hashes of journal and authoritative checkpoint were unchanged by inspection.
This reads an earlier real forced-exit experiment; it does not perform a new kill,
start Docker or independently verify the warehouse.

Eleven unit cases and the retained-journal acceptance passed (12 tests, 0.59 seconds).
JUnit `artifacts/commerce-journal-inspection-20261012.xml`, SHA-256:
`e2355ffa6846cdb921c68f4a8036e6461f8bc653397f52759b7c86c378d94d1f`.
Fresh local quality passed all seven gates: 905 passed, ten opt-in skips, 90.76%
coverage. Remote mandatory CI is still required for merging.
