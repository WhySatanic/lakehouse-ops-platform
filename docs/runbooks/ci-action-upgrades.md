# CI action upgrades

The CI workflow pins external actions to full upstream commit SHAs. Version comments
identify the reviewed release; moving an upstream tag cannot change the executed action.
This complements the [container image lock](container-image-digest-lock.md).

## Reviewed actions

| Action | Release | Purpose |
| --- | --- | --- |
| actions/checkout | [v7.0.1](https://github.com/actions/checkout/releases/tag/v7.0.1) | Check out the tested source |
| actions/setup-python | [v7.0.0](https://github.com/actions/setup-python/releases/tag/v7.0.0) | Install Python 3.12 and restore the pip cache |
| actions/upload-artifact | [v7.0.1](https://github.com/actions/upload-artifact/releases/tag/v7.0.1) | Archive evidence for downstream jobs |
| actions/download-artifact | [v8.0.1](https://github.com/actions/download-artifact/releases/tag/v8.0.1) | Download evidence and fail on a digest mismatch |

All four actions declare Node 24 in their pinned `action.yml`. The workflow uses
GitHub-hosted `ubuntu-latest` runners. Self-hosted forks need Actions Runner 2.327.1
or newer; authenticated Git commands inside Docker container actions need 2.329.0
or newer with checkout v6 and later.

## Upgrade behavior

Checkout v7 rejects unsafe fork checkout under `pull_request_target` and
`workflow_run` by default. This repository uses `pull_request` and `push`; it does
not enable the unsafe override. The release candidate still checks out the exact
push SHA with `persist-credentials: false` and checks that the working tree is clean.

Uploads explicitly use `archive: true` so evidence remains a ZIP containing the same
files. Downloads preserve separate artifact-name directories and explicitly use
`digest-mismatch: error`. A transfer hash mismatch stops readiness or candidate
assembly before semantic evidence validation. Do not downgrade it to a warning.

## Refresh and verification

1. Review the upstream release notes and the release's `action.yml`, including runtime,
   runner requirements, inputs, and changes to artifact extraction.
2. Resolve the reviewed tag in the official repository, for example:
   `gh api repos/actions/checkout/commits/v7.0.1 --jq .sha`.
3. Replace every occurrence of that action in `.github/workflows/ci.yml` with the
   full SHA and update its version comment and the table above.
4. Run the local quality gate and verify that all `uses:` references are full SHAs.
   Require the PR's quality, serving, Ranger, lakehouse, and release-readiness jobs.
5. After merge, require the main run's release-candidate job. Its evidence archive
   must retain the matching source revision and pass manifest validation.

If migration breaks checkout, caching, or evidence transfer, revert the workflow
change in a reviewed PR and rerun the same gates. Keep the failed run for diagnosis.
Never publish evidence from a failed run or weaken its validation to finish an upgrade.

The SHA pins freeze action code, not the hosted runner image, downloaded Python
distribution, or packages installed outside the project lockfile. They are not an
attestation that upstream action code is free of defects.
