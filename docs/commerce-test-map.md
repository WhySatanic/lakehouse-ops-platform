# Commerce invariants and executable checks

Use this map to choose evidence for a commerce change. It describes existing
checks, not a new execution or a coverage percentage target. The
[CI workflow](../.github/workflows/ci.yml) runs live checks. Ordinary local pytest
also includes substituted runners and skips opt-in integration tests; a passing
local suite alone does not establish Spark correctness.

## Transformations

| Production transformation | Business invariant checked | Real Spark check and execution path | Failure case or limit |
|---|---|---|---|
| [Bronze preparation](../jobs/spark/bronze_commerce.py) | Declared row count, schema and required values agree. Duplicate rows retain distinct occurrence numbers and the same content hash. | [Bronze validation](../tests/integration/check_commerce_bronze_validation.py), called by [Spark memory smoke](../tests/integration/check_spark_memory.py) in `serving-integration`. | Malformed JSON, wrong types, missing values and row-count mismatches fail before MERGE. Real Spark, but no catalog writes or commit-failure injection. |
| [Customer silver](../jobs/spark/silver_commerce_customers.py) | Fixture valid + rejected rows account for bronze; valid keys are unique; missing-email flags and email validity agree. | [Customer checker](../tests/integration/check_commerce_customer_silver.py), Compose `commerce-customer-silver-check` in `lakehouse-integration`. | Reject IDs are unique. Fixture counts and predicates are checked, not every possible malformed customer. |
| [Product silver](../jobs/spark/silver_commerce_products.py) | Fixture rows reconcile; valid keys are unique, names/categories nonempty and prices positive. | [Product checker](../tests/integration/check_commerce_product_silver.py), Compose `commerce-product-silver-check` in `lakehouse-integration`. | Invalid rows cannot enter the checked valid output; this is fixture acceptance, not exhaustive classification coverage. |
| [Order silver](../jobs/spark/silver_commerce_orders.py) | Duplicate rejects and late-order counts match the fixture; valid order IDs are unique and customer/product keys resolve. | [Order checker](../tests/integration/check_commerce_order_silver.py), Compose `commerce-order-silver-check` in `lakehouse-integration`. | The fixture exercises duplicate rejection; it does not independently enumerate every unknown-key or arithmetic fault. |
| [Payment silver](../jobs/spark/silver_commerce_payments.py) | Known states are exact lowercase values; missing/unknown states are rejected. Captured/refunded/cancelled snapshots yield 125/0/0 captured cents while preserving ordered amount. | [Payment checker and `check_payment_states`](../tests/integration/check_commerce_payment_silver.py), Compose `commerce-payment-silver-check` in `lakehouse-integration`. | Twelve literal classification cases include typo, uppercase, padding, partial-refund status and blank/null. The catalog fixture also checks missing-amount rejects. This is not an additive refund ledger. |
| [Daily gold](../jobs/spark/gold_commerce_daily.py) | Payment fanout cannot multiply order/buyer/ordered totals; captured, noncaptured, missing and rejected counts remain distinct. | [`check_payment_fanout` and catalog reconciliation](../tests/integration/check_commerce_daily_gold.py), Compose `commerce-daily-gold-check`; [Trino check](../tests/integration/check_commerce_daily_gold_trino.sh) reads the written table in `lakehouse-integration`. | Literal two-order case expects 300 ordered cents and 120 captured cents. Catalog reconciliation alone is not an independent literal oracle; related-snapshot checks below supply one. |
| [Forward SCD2](../jobs/spark/commerce_customer_scd2.py) | One current version, unique version IDs, valid contiguous periods and attributes matching the selected input. | [Catalog checker](../tests/integration/check_commerce_customer_scd2.py), Compose `commerce-customer-scd2-check` in `lakehouse-integration`; [atomic checker](../tests/integration/check_commerce_scd2_atomic.py) called by the related-snapshot scenario in `serving-integration`. | Atomic checker wraps real `spark.sql`: failure before publication preserves rows/snapshots; failure after MERGE observes complete replacement and replay adds no snapshot. It corrupts only its disposable table to reject a missing current version. No failure is injected inside commit. |
| [Current gold](../jobs/spark/gold_commerce_current.py) and retained SCD2 rebuild | Current revenue follows the newest full snapshot; older publication cannot replace it. Rebuild preserves literal periods; unchanged replay adds no history. | [Related-snapshot Spark scenario](../tests/integration/check_commerce_change_scenario.py), `serving-integration`, uses a disposable Hadoop Iceberg catalog and production publication/history functions. | Real Spark/Iceberg, but directly prepared DataFrames. This check alone does not exercise all nine runner stages, MinIO or the metastore. |

## Runner failures and business scenarios

| Scenario | Executable evidence | What the failure establishes |
|---|---|---|
| Late correction, retry and next ordinary snapshot | [History-correction walkthrough](runbooks/commerce-history-correction-demo.md) runs production stages and literal Trino oracles in `serving-integration`; [retained verifier](../tests/integration/test_commerce_retained_history_correction.py) reads its report/checkpoint/journal afterward. | Caught failure after batch gold leaves checkpoint unchanged; complete retry, next batch and compute-free idle preserve the declared rows/files/history/current revenue. This is a stage-boundary failure, not a process kill. |
| Active Spark interruption | [Live exercise](../tests/integration/exercise_commerce_active_spark.py), `serving-integration`; [operator runbook](runbooks/commerce-active-spark.md). | Observes running Spark tasks, reconfirms ownership and immutable container ID, kills that container, checks checkpoint preservation and retries the same landed input. It does not identify the exact instant of Iceberg commit. |
| Full refund and later snapshots | [Refund walkthrough](runbooks/commerce-refund-scenario.md), `commerce-refund-integration`; [retained verifier](../tests/integration/test_commerce_refund.py). | Five full snapshots yield literal revenues 1000/1500/0/700/1000 cents. Publication replay at the refund snapshot stays zero; final replay and idle preserve checkpoint/history. No partial refunds or order cancellation. |

The atomic check's small in-flight queue exercise uses an in-memory landing marker
and a partial runner. It covers queue/history publication boundaries but cannot
replace the live interruption or complete correction evidence. Retained verifiers
read evidence without starting Spark; their value depends on the originating live
run and its source identity.

## Choose and inspect a run

For a fresh complete scenario, follow image preparation and the command in the
[supported walkthrough](../README.md#supported-commerce-walkthrough). The
[training fixture runbook](runbooks/commerce-training-fixture.md) documents the
catalog-backed silver/gold commands and fixture counts. Helpers in the table need
their existing container environment, input and/or disposable catalog; bare host
Python execution is not a substitute. [Compose](../compose.yaml) binds catalog
checkers to the real Spark image.

Before accepting a result, inspect its exact source commit, CI job and unskipped
steps, return codes and retained artifacts. In particular:

- `serving-evidence` retains correction and interruption evidence;
  `commerce-refund-evidence` retains the refund scenario and source identity.
- Reports saying `running` or `failed` are partial observations, not completed
  correctness/throughput measurements. Preserve them for diagnosis.
- A mock proving command invocation cannot prove Spark ran.
  [Demo unit tests](../tests/test_commerce_linked_demo.py) and
  [interruption unit tests](../tests/test_commerce_active_spark.py) check
  orchestration and rejection paths separately from the live jobs above.
- This is a bounded single-writer full-snapshot contract. Complete retained
  customer silver is required for rebuild. The checks do not establish distributed
  exactly-once, arbitrary CDC/deletes or failure inside Iceberg commit.

Use the [recovery state table](runbooks/commerce-recovery-states.md) for publication,
checkpoint ownership and retry decisions. Changing those boundaries requires the
corresponding live scenario, not a higher coverage percentage.
