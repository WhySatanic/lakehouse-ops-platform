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
