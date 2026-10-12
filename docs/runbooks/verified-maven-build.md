# Checksum-preserving Maven Central build retry

Serving, Ranger and lakehouse CI retain all mandatory jobs and the three-attempt
verified build wrapper. A recurring Spark build failure returned the same incorrect
digest for two different Iceberg JARs on all three attempts. It was a rejected
download, not an accepted artifact or a change to the reviewed SHA256 values.

The private wrapper now accepts opt-in `--maven-central-fallback` for Compose build:

```powershell
uv run python -m lakehouse_ops.verified_build --attempts 3 `
  --delay-seconds 5 --maven-central-fallback -- `
  docker compose --profile catalog --profile compute build `
  minio hive-metastore spark-bronze spark-silver
```

The first attempt retains the default `https://repo1.maven.org/maven2`. Only after
a `digest mismatch` does the wrapper add the fixed build argument
`MAVEN_CENTRAL_URL=https://repo.maven.apache.org/maven2`. Later attempts retain it.
Both addresses are [supported Maven Central TLS endpoints](https://central.sonatype.org/faq/ssl/).
The same two reviewed `ADD --checksum` values still apply. This changes the remote
source, not artifact versions or checksum requirements. BuildKit can reuse an
already validated content-addressed artifact across URLs, even with `--no-cache`;
the fallback does not invalidate this shared cache. A second mismatch
still fails after the bounded attempts; syntax/other non-checksum failures stop immediately.
An explicit command-line endpoint combined with fallback is rejected, not overwritten.
Without the flag the old retry behavior and local default endpoint are unchanged.

This is dependency-download resilience, not cross-job image caching or a measured
CI speedup. Both official endpoints can fail together. No protection is bypassed,
no arbitrary mirror is automatically selected, and no mandatory test is removed.
The Spark Dockerfile exposes the endpoint build argument; manual callers remain
responsible for an explicit override, which must still satisfy pinned checksums.

## Real bounded acceptance

```powershell
$env:LAKEOPS_RUN_MAVEN_FALLBACK_SMOKE = '1'
uv run pytest tests/integration/test_maven_fallback.py -q -s -o addopts=''
Remove-Item Env:LAKEOPS_RUN_MAVEN_FALLBACK_SMOKE
```

The test temporarily serves deliberately incorrect bytes from a local host port
accessible by Docker. A uniquely named temporary docker-container builder starts
with a cold source cache; otherwise existing valid artifacts can avoid the injected
HTTP response entirely. Its BuildKit image is pinned by digest. It builds only a
uniquely tagged test image and never selects itself as the user's default builder.
It requires Docker and outbound HTTPS; the server closes after the experiment. The first
build must actually reject the body. The retry uses the official alternate
endpoint, then a network-disabled container verifies both resulting JAR hashes.
A separate wrong-pin case must fail even on the alternate endpoint. The source
Dockerfile is not modified by the test; temporary inputs are isolated. No existing
image tags, services or volumes are overwritten or removed. Only the newly created
builder and its private cache are removed in teardown; test images are retained.

Rollback: remove the three opt-in CI flags to restore same-endpoint retries. The
default Dockerfile endpoint, artifact hashes and public CLI contract are unchanged.

## Retained local execution, 2026-10-12

On current main plus this change, lint, generated access policy, stable contract
and image lock passed. The full default suite passed 957 tests with 15 expected
opt-in skips, 91.26% coverage, in 27.98 seconds.

Both real cold-builder cases passed in 183.56 seconds, including setup/teardown.
The deliberately incorrect HTTP body was requested and rejected on the first
attempt. With reviewed pins, the alternate HTTPS endpoint completed the actual
Spark Dockerfile build on the second attempt; a network-disabled container
verified both resulting JAR SHA256 values. With an intentionally wrong runtime
pin, both attempts were rejected. The temporary builder was removed and the
default builder remained unchanged. This is a bounded local failure-injection
observation, not a promise that either external endpoint will always be available.

The positive test image is
`lakehouse-ops/maven-fallback-smoke:171afeebd33649028028799c2d1fb5e1`.
Per-attempt logs and temporary Docker/Compose inputs are retained locally under
`artifacts/maven-fallback-20261012/`. JUnit evidence:
`artifacts/maven-fallback-quality-20261012-v3.xml` and
`artifacts/maven-fallback-docker-20261012-v5.xml`.
The Docker JUnit SHA256 is
`50f25792227430faaa9d1439cc7aa7865c479eec7929b36b6b7b0c7160e02bb6`.
These local observations are not shipped release assets. Mandatory exact-head CI
and release-candidate evidence remain separate requirements.
