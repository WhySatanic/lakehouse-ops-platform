# Clean-checkout release-candidate rehearsal

The `release-candidate` CI job runs after the complete cross-profile attestation on a
push to `main`. It checks out the exact source revision without persisted credentials,
proves that the checkout is clean, downloads only artifacts from the same workflow run,
and produces `lakehouse-ops-1.0.0-rc-evidence.tar.gz`.

The archive contains:

- the release-readiness attestation and both public contracts;
- every digest-verified cross-profile evidence report;
- the pinned Trino upgrade plan and four-phase upgrade/rollback report;
- a deterministic manifest with the source revision and SHA-256 of every member.

The builder rechecks every digest recorded by the attestation, validates the attestation
source revision and readiness-contract digest, and reruns the Trino upgrade/rollback
validator. Archive metadata is normalized, so identical inputs produce identical bytes.

## Reproduce from downloaded artifacts

Download all artifacts from one successful `main` workflow run into `evidence/`, then
run from a clean checkout of the run's source revision:

```bash
test -z "$(git status --porcelain --untracked-files=all)"
SOURCE_REVISION="$(git rev-parse HEAD)"
uv run lakeops build-release-candidate \
  --evidence-root evidence \
  --attestation evidence/release-readiness-attestation/release-readiness.json \
  --readiness-contract config/release/readiness-contract.json \
  --control-plane-contract config/control-plane/contract.json \
  --upgrade-report evidence/lakehouse-evidence/trino-upgrade-rehearsal.json \
  --upgrade-plan config/trino/upgrade-rehearsal.json \
  --source-revision "$SOURCE_REVISION" \
  --output artifacts/lakehouse-ops-1.0.0-rc-evidence.tar.gz
```

Retain both the archive and `release-candidate.json`. Attach them to the GitHub Release
and confirm its tag resolves to `source_revision` before publishing any stable release.

## Failure policy

Do not publish when the checkout is dirty, the source revision differs, any attested
digest changes, the readiness contract changes, or upgrade/rollback validation fails.
Regenerate all evidence in one new workflow run instead of mixing artifacts across runs.
