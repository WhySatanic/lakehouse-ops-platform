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
CI then verifies the two downloadable assets through the same public command available to
release consumers. On `main`, GitHub OIDC and Sigstore also bind all three final assets to
the repository, signer workflow, source SHA, and source ref. The retained Sigstore bundle
allows the provenance to be checked separately from the GitHub Release download channel.

## Verify downloaded release assets

Obtain the expected full source revision from a trusted release tag, download the archive
and `release-candidate.json` into one directory, then run:

```bash
uv run lakeops verify-release-candidate \
  --bundle lakehouse-ops-1.0.0-rc-evidence.tar.gz \
  --report release-candidate.json \
  --expected-source-revision "$EXPECTED_SOURCE_REVISION"
```

The verifier validates the report against its public schema, binds it to the expected
revision, streams the archive digest, and reads tar members without extracting them. It
rejects duplicate or unsafe paths, non-regular members, missing or additional content,
and every manifest digest mismatch. Archive reads are bounded to 256 regular files,
64 MiB per file, and 256 MiB in total.

## Verify release provenance

Install the open-source GitHub CLI, download
`release-candidate-provenance.sigstore.json` with the three subject files, and verify each
subject using the identity constraints enforced by CI:

```bash
gh attestation verify lakehouse-ops-1.0.0-rc-evidence.tar.gz \
  --bundle release-candidate-provenance.sigstore.json \
  --repo WhySatanic/lakehouse-ops-platform \
  --signer-workflow github.com/WhySatanic/lakehouse-ops-platform/.github/workflows/ci.yml \
  --source-digest "$EXPECTED_SOURCE_REVISION" \
  --source-ref refs/heads/main \
  --cert-oidc-issuer https://token.actions.githubusercontent.com \
  --deny-self-hosted-runners
```

Repeat the command for `release-candidate.json` and
`release-candidate-verification.json`. Supplying `--bundle` prevents attestation lookup
through the repository API. Verification still needs trusted Sigstore root material;
obtain or cache that independently of the release assets for a fully offline ceremony.

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
  --schema config/control-plane/schemas/release-candidate-bundle-report.schema.json \
  --source-revision "$SOURCE_REVISION" \
  --output artifacts/lakehouse-ops-1.0.0-rc-evidence.tar.gz
```

Retain the archive, `release-candidate.json`, `release-candidate-verification.json`,
`release-candidate-provenance.sigstore.json`, and the machine-readable
`release-candidate-provenance-verification.json`. Attach them to the GitHub Release and
confirm its tag resolves to `source_revision` before publishing any stable release.

## Failure policy

Do not publish when the checkout is dirty, the source revision differs, any attested
digest changes, the readiness contract changes, or upgrade/rollback validation fails.
Regenerate all evidence in one new workflow run instead of mixing artifacts across runs.
The builder validates `release-candidate.json` against its Draft 2020-12 schema before
returning success. Digest verification alone proves internal integrity and source binding.
Provenance verification additionally proves the signing workflow identity and witnessed
signature, but it cannot make a compromised repository workflow trustworthy. Treat
changes to the pinned action, workflow permissions, signer workflow, and branch protection
as security-sensitive review items.
