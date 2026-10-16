# Bounded customer history and retained warehouse experiment

The separate `Customer history experiment` workflow uses one fresh GitHub runner
and one uniquely named `lakehouse-ops-scale-history-*` stack. It runs for pull requests
changing its workflow or experiment tests, and can be dispatched manually once the
workflow is present on the default branch. Ordinary CI and its five mandatory gates
are unchanged. This experiment is not a release candidate or capacity certification.

Inputs are fixed: 100 canonical orders, 10000 customers per chronological full snapshot,
four February 1..4 snapshots, customer revisions `0,1,1,2`. The existing production
profile runs all nine normal stages and verifies independent order/revenue input
oracles, every customer's SCD2 periods/names/current flags and idle checkpoint replay.
Expected history totals are 10000, 20000, 20000 and 30000, with 10000 current each time.
These are acceptance oracles; a queued workflow is not an observed result.

The profile enables both existing warehouse inventory and retained-reference observers.
The test bootstraps the existing listing-only identity inside its own MinIO before
the profile starts. Writers keep their existing credentials; observer policy is not
expanded. Trino current/retained references are reconciled with versioned S3 payloads
after every sample, with reference changes around listing rejected by the profile.
Metadata and unselected references are not classified as orphan, and payload bytes
do not establish physical disk usage or billing. Writers and maintenance are quiescent
during each observation; multiple requests are not one atomic distributed snapshot.

Images are built/preloaded before the isolated experiment. Runner CPU, memory, kernel,
Docker information and checkout SHA are saved separately. This is fresh services/catalog
with locally available images, not a cold image download or cleared host cache.
The child has a 1800-second total limit and the existing 300-second stage budgets.
The job is bounded to 60 minutes including build and upload. Timeout, memory failure,
partial history or missing observations are failed evidence, not successful samples.

Download the `customer-history-experiment` artifact from the workflow run. It retains
`customer-history-ci/experiment.json`, raw `profile/profile.json` with its SHA256,
source fixtures/manifests, SQL/results/query IDs, runner and memory reports, retained
object/reference inventories, checkpoint and service logs. The outer report is ready
only after the four sample and storage checks and successful owned cleanup. Partial
reports and logs upload even on failure. Verify report source SHA against the workflow
checkout and PR head/merge parents before citing results.

The test uses scoped Compose down and validates ownership of the empty network.
It does not delete volumes, images, unrelated containers or host data. GitHub runner
disposal remains external: raw JSON artifacts are evidence, not a backup of warehouse
volumes. Repeat on independent fresh stacks for comparisons; one run does not prove
performance improvement, scaling to 50/100 snapshots, commit-time crash recovery or
distributed exactly-once processing.

## Observed baseline, 2026-10-16

[CI run 38859196975](https://github.com/WhySatanic/lakehouse-ops-platform/actions/runs/38859196975)
and three sequential fresh local stacks completed the workload above. The
[recorded evidence](../evidence/customer-history-baseline-2026-10-16.json) contains
source/report hashes, input manifest hashes, per-stage times, query IDs/statistics,
memory observations and reconciled storage groups. CI checkout ecce85a8727443a45ac95bc532bbde1142fe8bdb
has verified parents main d3462a2 and experiment head a977142; all local rounds used
the clean a977142 checkout. These observations belong to those sources.

All four runs verified history totals 10000/20000/20000/30000 and 10000 current
customers per sample. Every customer had periods [Feb1,Feb2), [Feb2,Feb4),
[Feb4,open). SCD2 snapshot counts were 1/2/2/3; the repeated revision preserved the
same snapshot set between samples two and three. All manifests were byte-identical
across runs. Independent raw checks repeated fixture hashes, literal period/count
SQL, revenue/order inputs, checkpoint hashes, compute-free idle and S3/reference
reconciliation. Each local cleanup preserved six volumes and all seven preexisting
foreign container IDs, with no owned containers/network remaining.

| Snapshot | History rows | Local median runner, s | Local min to max runner, s | Local min to max warehouse payload, B |
|---|---:|---:|---:|---:|
| 1 | 10000 | 113.182 | 105.218 to 117.870 | 1964833 to 1964853 |
| 2 | 20000 | 104.159 | 103.283 to 116.951 | 4745077 to 4745116 |
| 3 | 20000 | 105.192 | 102.878 to 109.459 | 5924745 to 5924811 |
| 4 | 30000 | 108.609 | 103.781 to 110.594 | 8961378 to 8961432 |

Local sum of four runner totals: 415.161, 437.395, 448.621 s; median 437.395 s.

The final CI warehouse payload was 8961468 B: current data 6126036 B,
retained-only data 1846021 B and metadata 989411 B. Delete-reference and S3 noncurrent
payload bytes were zero. Current-only accounting would miss 2835432 B. Current and
retained data sizes matched all local rounds; metadata bytes varied. Matching file
sizes and SQL results are not a content-hash audit of Parquet payloads.

Local Docker Linux 29.8.2/cgroup2 had 12 CPU and 16637530112 B RAM under Windows;
seven other containers were active. Images/source were the same for all three
rounds; caches were not cleared. CI had 4 CPU and 16766414848 B RAM. Exclude CI from
local medians. Runner timing includes stage launch/SQL/IO and excludes subsequent
profile observers. Memory is sampled, not an exact continuous host peak. This is
n=3 local baseline, not an optimization result, isolated capacity test or proof of
50/100-batch behavior. A bounded history-depth experiment remains the next step.

To reproduce, run the integration experiment above on a fresh owned stack, retain
the raw artifact, compare its source/manifest/report hashes and independently check
the expected rows, checkpoint and storage sums. Local raw files are operator evidence;
the linked CI artifact is the published raw run. Successful measurement does not
replace the five mandatory exact-head checks required for merging.
