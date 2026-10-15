# Opt-in Trino query time budget

The Python Trino adapter accepts `query_timeout` in seconds. Unlike its existing
HTTP phase timeout, this budget spans submission and every result page. Each query
gets a new monotonic deadline. Partial or late results are never returned as success.
The default remains `None`, preserving existing CLI and Python callers.

```python
from lakehouse_ops.trino import TrinoClient, TrinoQueryTimeoutError

with TrinoClient("http://localhost:18300", user="lakehouse-performance",
                 query_timeout=15) as client:
    try:
        rows = client.query("SELECT 1 AS value")
    except TrinoQueryTimeoutError as error:
        print(error.query_id, error.cancellation)
        raise
```

When supplied to `run_commerce_batch` as `client.query`, a timeout during gold
verification follows the existing failure path and cannot advance the checkpoint.
The commerce CLI does not yet enable this optional adapter budget. This increment
does not change its public arguments, retry policy or default behavior.

On budget exhaustion or an HTTP phase timeout, the adapter attempts a `DELETE` on
the latest validated `nextUri`, routed through the configured coordinator with the
same identity headers. This is the [Trino client cancellation protocol](https://trino.io/docs/current/develop/client-protocol.html).
It never cancels by an arbitrary query ID or issues a broad administrative kill.

`TrinoQueryTimeoutError` is a `TrinoProtocolError` and exposes `query_id` and:

- `accepted`: cancellation endpoint returned HTTP 2xx, not proof of resource reclamation;
- `http_<status>`: server rejected cancellation;
- `transport_failed`: cancellation transport failed;
- `not_available`: no usable URI, including submission timeout before query identity
  was received, or a terminal result received after the deadline.

Cancellation has a separate five-second total HTTP budget. No cancellation outcome
turns the failed query into success. An unknown submission outcome requires operator
inspection; never automatically resubmit mutating SQL based on this exception.

## Limits and upgrade

Budgeted HTTP requests run in a bounded reader thread so continuously arriving body
chunks cannot keep the caller waiting past its network deadline. The reader owns
and closes its response; connect/read/write/pool phase limits preserve shorter
caller settings. There are at most two unfinished readers per client, allowing a
request and its cancellation to close concurrently. A custom transport that ignores
timeouts can retain a reader until it returns. JSON decoding and row assembly still
run in the caller and are not preempted. Cancellation can add up to five seconds.
This is not hard process preemption or a server-side execution guarantee.
Server-side resource groups and execution limits remain separate controls.

## Result page safety limit

Every query accepts at most 1000 HTTP result responses, including the initial POST
response. A terminal response at page 1000 succeeds. If page 1000 still has a
validated `nextUri`, the adapter stops before another GET and attempts cancellation
using that URI, routed through the configured coordinator and identity headers.
The same five-second cancellation budget applies even without `query_timeout`.

`TrinoPageLimitError` extends `TrinoProtocolError` and exposes `query_id` and
`cancellation` with the outcomes listed above. Rejected or failed cancellation does
not hide the page limit error. Partial rows are discarded, and gold verification
cannot advance the commerce checkpoint. An accepted DELETE is not proof that the
server has reclaimed resources. Other protocol failures do not acquire a new
cancellation policy in this increment.

Reproduce boundary and cancellation checks without a running Trino server:

```powershell
uv run --frozen pytest tests/test_trino_page_limit.py tests/test_trino_deadline.py tests/test_trino_deadline_http.py -q --no-cov
```

No data/config migration is needed. Existing clients are unchanged. To opt out,
remove `query_timeout`; rolling back does not undo committed SQL or prove cancellation.

## Local acceptance

Use only a project-owned OSS Trino coordinator. The smoke executes `SELECT 1`, delays
this client's initial response processing to exhaust its budget, sends real scoped
cancellation, and checks that a subsequent query still succeeds. It does not simulate
a slow Spark job or measure server capacity.

```powershell
$env:LAKEOPS_RUN_TRINO_DEADLINE_SMOKE = "1"
$env:LAKEOPS_TRINO_DEADLINE_SERVER = "http://localhost:18300"
uv run --frozen pytest tests/integration/test_trino_deadline_smoke.py -q --no-cov
Remove-Item Env:LAKEOPS_RUN_TRINO_DEADLINE_SMOKE
Remove-Item Env:LAKEOPS_TRINO_DEADLINE_SERVER
uv run --frozen python -m lakehouse_ops.local_quality
```

Retained local evidence on 2026-10-12: the project-owned
`lakehouse-ops-scale-query-v1` coordinator on port 18300 accepted cancellation;
the follow-up query succeeded. The smoke passed in 5.59 seconds. JUnit artifact
`artifacts/trino-deadline-20261012.xml` SHA-256:
`d6efcf4085dbbffe05e46fb3f86b8dbec018cb06736a2706d30bbc3c88b0e74a`.
Fresh local quality passed all seven gates with 909 tests passed, 10 opt-in tests
skipped, and 90.77% coverage. This is not mandatory remote CI evidence.
