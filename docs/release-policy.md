# Release policy

## Product contract

Lakehouse Ops Platform is developed in public as a stable reference product. Version
`1.0.0` freezes the public control-plane contract while the operational surface keeps
growing. Stable does not mean production capacity or a managed service; limitations stay
explicit and every supported path remains executable.

At every merge to `main`:

- at least one documented end-to-end path must work;
- implemented, experimental, and planned capabilities must be clearly distinguished;
- the quality gate must pass;
- migrations or configuration changes must include an upgrade note;
- incomplete integrations must be isolated behind an opt-in profile or feature flag;
- the previous working path must not be silently removed.

## Versioning

The project uses semantic versioning:

- patch (`1.y.z+1`): fixes, tests, documentation, and compatible operational changes;
- minor (`1.y+1.0`): additive working capabilities and deprecations that preserve the
  stable contract;
- major (`2.0.0`): an intentionally incompatible CLI or JSON contract change with
  migration and rollback guidance.

The historical `0.y.z` line was early access. Version `1.0.0` requires the stable
control-plane contract, complete core and authorization paths, observability, recovery
drills, and a retained clean-checkout release-candidate evidence bundle.

Every tagged release includes a short capability matrix, known limitations, verification
commands, and upgrade notes. A roadmap checkbox does not make a release; executable
acceptance evidence does.

The public CLI and versioned JSON reports follow the
[control-plane compatibility policy](control-plane-compatibility.md). CI compares the
current parser with contract `1.0.0`; a breaking change requires a new product major.

CI action upgrades follow the [action upgrade runbook](runbooks/ci-action-upgrades.md):
reviewed commit pins, compatible runner runtimes, and strict evidence download hashes.

## Expansion seams

The architecture remains open through stable boundaries rather than speculative code:

- landing-zone adapters allow local filesystem and S3 implementations;
- catalog configuration allows Hive Metastore and later REST-catalog profiles;
- maintenance planners are separate from Spark executors;
- authorization policy is versioned independently from the Ranger deployment;
- optional Compose profiles add observability, security, and serving components;
- collectors emit normalized observations so new engines can be added later;
- the image lock separates reviewed upstream tag changes from immutable runtime and
  build-base digests.

## Merge discipline

Prefer one complete vertical slice over several activity-only commits. A change may be
small, but it must improve behavior, safety, evidence, or understanding. The public
history should tell the real story of the product: decisions, experiments, failures,
measurements, and corrections.
