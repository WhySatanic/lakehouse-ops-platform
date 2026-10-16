# Payment states in complete snapshots

Commerce inputs are complete snapshots, not payment events or a refund ledger.
A payment ID identifies one row within a selected batch. Its state in a later
complete snapshot replaces its contribution to current revenue. Earlier bronze,
silver and daily batch partitions remain history; never sum their revenue as events.

| Exact lowercase status | Current captured revenue contribution |
|---|---|
| `captured` | Positive `amount_cents` |
| `pending` | Zero |
| `declined` | Zero |
| `failed` | Zero |
| `cancelled` | Zero |
| `refunded` | Zero, a full refund of this payment |

`refunded` is a state of the same payment, not a second negative payment row.
Payment `p1` captured for 125 cents in snapshot A and refunded in B contributes
125 to A's historical mart and zero to B's current mart. Ordered amount does not
change merely because a payment was refunded. This defines payment cancellation;
an order cancellation field and partial refunds are unsupported. Multiple distinct
captured payment IDs for an order remain additive.

Silver quarantines an unknown nonempty status with `unknown_status` in
`commerce_payment_rejects`, outside valid payment revenue. This includes case
changes, padded values and `partially_refunded`. It does not normalize or infer
a state. Null/whitespace-only status retains `missing_status`. Existing positive
amount, ID, duplicate and `paid_at` requirements apply to all states. `paid_at`
remains a required source timestamp even for noncaptured states; it does not
provide event ordering or a refund posting date.

Gold exposes quarantined rows through `rejected_payment_count`; this alone does
not fail the entire batch. Rejected amounts contribute no captured revenue. A
known order without any valid or rejected payment in the selected snapshot has
zero captured revenue and increments `missing_payment_count`. Payment absence is
not interpreted as a refund event. Entirely empty or incomplete batches remain
unsupported and are refused by existing loader/stage checks before checkpoint
advancement. Orphaned payment order IDs remain a gold validation error.

The runner still selects one immutable manifest-bound batch. Replay does not
update that batch's silver rows; corrections require a new snapshot. Accepted
chronological snapshots drive the current mart; stale snapshots cannot roll it
back. Customer SCD2 and atomic publication remain unchanged. No payment transition
state machine, event deduplication or distributed exactly-once claim is introduced.

## Verification and next scenario

`check_payment_states` in the ordinary payment-silver integration check exercises
12 literal classifications and captured/refunded/cancelled contributions 125/0/0
through production classification and gold aggregation. It runs in the existing
mandatory lakehouse CI job. A local Spark 3.5.9 container first reproduced four
unknown statuses being accepted, then passed after the fix. It writes no catalog
or warehouse and does not substitute for a runner proof.

The [local evidence](../evidence/commerce-payment-states-2026-10-16.json) binds
the source and red/green log hashes. To reproduce without a catalog, save this as
`artifacts/payment-contract/probe.py`:

```python
from pyspark.sql import SparkSession
from check_commerce_payment_silver import check_payment_states

spark = SparkSession.builder.master("local[2]").config(
    "spark.sql.shuffle.partitions", "2"
).getOrCreate()
try:
    check_payment_states(spark)
finally:
    spark.stop()
```

In PowerShell at the checkout root, using the already verified cached image:

```powershell
$probeName = 'lakeops-payment-check-' + [guid]::NewGuid().ToString('N')
docker run --rm --name $probeName --network none --memory 2g --cpus 2 `
  --mount "type=bind,source=$((Get-Location).Path)\jobs\spark,target=/jobs,readonly" `
  --mount "type=bind,source=$((Get-Location).Path)\tests\integration,target=/checks,readonly" `
  --mount "type=bind,source=$((Get-Location).Path)\artifacts\payment-contract,target=/probe,readonly" `
  -e PYTHONPATH=/jobs:/checks -e SPARK_LOCAL_IP=127.0.0.1 `
  --entrypoint /opt/spark/bin/spark-submit `
  lakehouse-ops/spark-iceberg:3.5.9-1.11.0 /probe/probe.py
```

This increment establishes the state boundary. The complete initial/correction/
refund/late-arrival/replay/following-batch scenario with independent historical and
current SQL oracles through the ordinary nine-stage runner remains the next step.
