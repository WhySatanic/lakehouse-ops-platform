# Full snapshot payment refund acceptance

Run one business sequence through the existing commerce runner, bronze/silver,
customer SCD2, historical daily gold and current gold. This adds an acceptance
fixture and SQL oracles, not another pipeline or a payment event processor.

| February snapshot | First payment | Other captured payments | Current revenue cents | Orders |
|---|---|---|---:|---:|
| 1, initial | captured 1000 | none | 1000 | 1 |
| 2, correction | captured 1500 | none | 1500 | 1 |
| 3, refund | refunded 1500 | none | 0 | 1 |
| 4, late arrival | refunded 1500 | 700, order dated January 1 | 700 | 2 |
| 5, following | refunded 1500 | 700 and new 300 | 1000 | 3 |

Each input is a complete snapshot. The same first payment ID changes state; a full
refund contributes zero in the new snapshot, not a negative amount or an additional
refund row. The corrected order remains 1500 cents of ordered value after refund.
Partial refunds, an order cancellation field, monetary refund postings and event
ordering are outside this scenario. Missing payments and empty snapshots are not
used to represent refunds. The first customer's changed name creates one closed
and one current SCD2 period; later payment changes must not add customer versions.

Before each checkpoint, literal SQL oracles verify current daily revenue/order
count, customer periods, all accepted historical gold rows and all batch-scoped
silver payment IDs/states/amounts. They check historical rows separately from
current rows: the five snapshot revenues sum to 4200, which is not current revenue
or a ledger total. Final current days are January 1 = 700, January 31 = 0 and
February 5 = 300 cents. Wrong current revenue, rewritten historical gold or a
missing refund state blocks checkpoint advancement.

After the refund batch reaches checkpoint, the same current publication is
replayed before the late/following batches. Revenue must remain zero and the
current Iceberg snapshot count must remain three. The final current publication
also replays without a new snapshot. Conditional source replay creates no new
objects; a final ordinary runner invocation must be idle with no compute. The
publication replays are explicit stage replays, not a claim that the runner
reprocesses an already checkpointed batch. No failure inside an Iceberg commit or
distributed exactly-once guarantee is claimed.

## Run

The [verified local report](../evidence/commerce-refund-scenario-2026-10-16.json)
binds actual current/history results, source hashes, checkpoint, journal and scoped
cleanup. The authoritative trial is `demo-r2`: the first trial's last order time
followed its snapshot time, so its otherwise passing result was preserved separately
and all five batches were rerun after correcting that synthetic timestamp.

Prepare the repository's pinned core/compute images using the standard verified
build process. From the checkout root with Docker available:

```powershell
uv run --frozen python -m lakehouse_ops.commerce_linked_demo --refund `
  --output artifacts/commerce-refund-local
$env:LAKEOPS_REFUND_DIRECTORY = 'artifacts/commerce-refund-local/acceptance'
uv run --frozen pytest tests/integration/test_commerce_refund.py -q -o addopts=''
Remove-Item Env:LAKEOPS_REFUND_DIRECTORY
```

Use a fresh empty output path. The existing demo chooses a unique owned project
and free ports, starts only the five core services plus their init dependency,
then stops its own containers/network while retaining named volumes. It refuses
in-flight/manual history drill flags combined with refund mode. Preserve the raw
`demo.json`, `acceptance/linked.json`, checkpoint, journal, fixture and stage logs
on failure. `--scenario refund` on `commerce_change_scenario` generates only the
fixture; it does not run or verify the pipeline.

The `commerce-refund-integration` job runs this same demo on pull requests and
main, checks the retained checkpoint/journal independently and uploads partial
or complete evidence. Release readiness waits for this job as well as the existing
quality and integration jobs. Local success does not replace exact-head CI.
The default three-snapshot correction fixture and recovery drills remain unchanged.
