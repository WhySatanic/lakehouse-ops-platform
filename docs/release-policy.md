# Early-access release policy

## Product contract

Lakehouse Ops Platform is deliberately developed in public as an early-access product.
It is never expected to become “finished”; it is expected to remain usable while its
operational surface grows.

At every merge to `main`:

- at least one documented end-to-end path must work;
- implemented, experimental, and planned capabilities must be clearly distinguished;
- the quality gate must pass;
- migrations or configuration changes must include an upgrade note;
- incomplete integrations must be isolated behind an opt-in profile or feature flag;
- the previous working path must not be silently removed.

## Versioning

The project uses semantic versioning with a `0.y.z` early-access line:

- patch (`0.y.z+1`): fixes, tests, documentation, and compatible operational changes;
- minor (`0.y+1.0`): a new working capability or a deliberately changed interface;
- `1.0.0`: reserved for a stable control-plane contract and a complete core recovery drill.

Every tagged release includes a short capability matrix, known limitations, verification
commands, and upgrade notes. A roadmap checkbox does not make a release; executable
acceptance evidence does.

The public CLI and versioned JSON reports follow the
[control-plane compatibility policy](control-plane-compatibility.md). CI compares the
current parser with contract `1.0.0`; a breaking change requires a new product major.

## Expansion seams

The architecture remains open through stable boundaries rather than speculative code:

- landing-zone adapters allow local filesystem and S3 implementations;
- catalog configuration allows Hive Metastore and later REST-catalog profiles;
- maintenance planners are separate from Spark executors;
- authorization policy is versioned independently from the Ranger deployment;
- optional Compose profiles add observability, security, and serving components;
- collectors emit normalized observations so new engines can be added later.

## Merge discipline

Prefer one complete vertical slice over several activity-only commits. A change may be
small, but it must improve behavior, safety, evidence, or understanding. The public
history should tell the real story of the product: decisions, experiments, failures,
measurements, and corrections.
