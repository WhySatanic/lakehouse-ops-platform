# Release readiness attestation

The release-readiness job is the final cross-profile gate for the `1.0.0` evidence set.
It runs only after quality, core lakehouse, Ranger, and serving jobs succeed. Each job
uploads the JSON reports it already validated, and the final job downloads those reports
into one immutable evidence bundle.

## Contract

`config/release/readiness-contract.json` is the versioned source of truth. Schema `1.0`
requires evidence for:

- the Spark, Iceberg, Hive Metastore, MinIO, and Trino core path;
- deny-by-default file authorization and centralized Ranger authorization;
- Ranger row filtering and column masking with observed values;
- Hive Metastore outage recovery, metadata-database restore, and abrupt worker recovery;
- five live platform SLO objectives;
- the optional ClickHouse direct-Iceberg serving check.

The verifier repeats semantic validation after artifact download. Presence alone does not
pass the gate. It also proves that the core metadata report and all three recovery reports
refer to the same Iceberg snapshot and the same two-row silver fixture.

## Run against downloaded CI artifacts

```bash
uv run lakeops verify-release-readiness \
  --contract config/release/readiness-contract.json \
  --evidence-root evidence \
  --source-revision "$(git rev-parse HEAD)" \
  --output artifacts/release-readiness.json
```

The command exits non-zero for a missing or malformed report, unsupported validator,
failed capability check, path traversal, snapshot drift, or row-count drift. A passing
attestation records the source revision, contract digest, every evidence digest, and the
cross-recovery invariants. Preserve both the generated attestation and the source CI
artifacts with a release candidate.

## Limitations

- The attestation proves the deterministic single-node development profiles exercised by
  CI. It is not production capacity, high-availability, TLS, or multi-region evidence.
- GitHub artifact retention is controlled by repository settings. Attach the final
  attestation and evidence bundle to the eventual `1.0.0` release if longer retention is
  required.
- A green attestation is necessary for `1.0.0`, but the public control-plane compatibility
  policy and clean-checkout release-candidate rehearsal remain separate gates.
