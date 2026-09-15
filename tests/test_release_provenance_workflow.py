from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"

SUBJECTS = (
    "artifacts/lakehouse-ops-1.0.0-rc-evidence.tar.gz",
    "artifacts/release-candidate.json",
    "artifacts/release-candidate-verification.json",
)


def _release_candidate_job() -> str:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    return workflow.split("  release-candidate:\n", maxsplit=1)[1]


def test_release_candidate_has_minimum_signing_permissions() -> None:
    job = _release_candidate_job()

    assert """    permissions:
      artifact-metadata: write
      attestations: write
      contents: read
      id-token: write
""" in job


def test_release_candidate_attests_and_retains_every_subject() -> None:
    job = _release_candidate_job()

    assert (
        "uses: actions/attest@1e69f48acb82d1966a394da916b4c1698aa569d6 # v4.2.2"
        in job
    )
    assert "artifacts/release-candidate-provenance.sigstore.json" in job
    assert "artifacts/release-candidate-provenance-verification.json" in job
    for subject in SUBJECTS:
        assert job.count(subject) >= 3


def test_release_candidate_enforces_provenance_identity() -> None:
    job = _release_candidate_job()

    required_policy = (
        "--repo WhySatanic/lakehouse-ops-platform",
        "--signer-workflow "
        "github.com/WhySatanic/lakehouse-ops-platform/.github/workflows/ci.yml",
        '--source-digest "$SOURCE_REVISION"',
        "--source-ref refs/heads/main",
        "--cert-oidc-issuer https://token.actions.githubusercontent.com",
        "--deny-self-hosted-runners",
        "--format json",
    )
    for policy in required_policy:
        assert policy in job

    assert (
        "test \"$(jq 'length' "
        "artifacts/release-candidate-provenance-verification.json)\" = 3"
        in job
    )
