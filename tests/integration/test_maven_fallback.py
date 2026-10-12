"""Opt-in real rejected download and checksum-preserving Maven endpoint fallback."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from lakehouse_ops.verified_build import run_verified_build


@pytest.fixture(scope="module")
def cold_builder():
    """Never invalidate the user's shared content-addressed source cache."""
    name = f"lakeops-maven-smoke-{uuid.uuid4().hex}"
    buildkit = ("moby/buildkit@sha256:"
                "cec9f139f45e93c5c69c60f8b07cfad9f43f4ef6b6a6cd917527fea5ff2e3dea")
    subprocess.run([
        "docker", "buildx", "create", "--name", name, "--driver", "docker-container",
        "--driver-opt", "default-load=true",
        "--driver-opt", f"image={buildkit}",
    ], check=True, capture_output=True, text=True, timeout=30)
    try:
        subprocess.run(["docker", "buildx", "inspect", "--bootstrap", name],
                       check=True, capture_output=True, text=True, timeout=180)
        if sys.platform == "win32":
            host = "host.docker.internal"
        else:
            host = subprocess.run([
                "docker", "inspect", f"buildx_buildkit_{name}0", "--format",
                "{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}",
            ], check=True, capture_output=True, text=True, timeout=30).stdout.strip()
        yield name, host
    finally:
        subprocess.run(["docker", "buildx", "rm", name], check=True,
                       capture_output=True, text=True, timeout=60)


@pytest.mark.skipif(os.getenv("LAKEOPS_RUN_MAVEN_FALLBACK_SMOKE") != "1",
                    reason="requires Docker and HTTPS Maven Central")
@pytest.mark.parametrize("wrong_pin", [False, True])
def test_real_checksum_rejection_then_endpoint_fallback(tmp_path: Path, wrong_pin: bool,
                                                      cold_builder):
    root = Path(__file__).resolve().parents[2]
    builder, host = cold_builder
    requests = []

    class WrongArtifact(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            body = b"intentionally incorrect artifact for checksum acceptance"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", 0), WrongArtifact)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    source = (root / "infra/spark/Dockerfile").read_text()
    runtime_hash = "94b8e36fc329f0293d44ba9e01b784a56e9501affec1842d898144c51f6e486a"
    bundle_hash = "38f01da7e96850cdd05e6616d758b77b43314b712a8808e3f9a824d56976162f"
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(source.replace(runtime_hash, "0" * 64) if wrong_pin else source)
    image = f"lakehouse-ops/maven-fallback-smoke:{uuid.uuid4().hex}"
    build = {"context": str(root), "dockerfile": str(dockerfile), "args": {
        "MAVEN_CENTRAL_URL": f"http://{host}:{server.server_port}/maven2",
    }}
    compose = tmp_path / "compose.json"
    compose.write_text(json.dumps({"services": {"spark": {"image": image, "build": build}}}))
    commands, logs = [], []

    def run(command):
        commands.append(list(command))
        result = subprocess.run(command, cwd=root, capture_output=True, text=True, timeout=300)
        output = result.stdout + result.stderr
        logs.append(output)
        (tmp_path / f"attempt-{len(logs)}.log").write_text(output, encoding="utf-8")
        return result.returncode, output

    try:
        result = run_verified_build(
            ["docker", "compose", "--file", str(compose), "--project-name",
             f"lakehouse-ops-maven-smoke-{uuid.uuid4().hex}", "build", "--builder", builder,
             "spark"],
            attempts=2, delay_seconds=0, run=run, maven_central_fallback=True,
        )
        assert requests, "\n".join(logs)
        assert len(commands) == 2
        assert "digest mismatch" in logs[0].lower()
        assert "MAVEN_CENTRAL_URL=https://repo.maven.apache.org/maven2" in commands[1]
        if wrong_pin:
            assert result != 0
            assert "digest mismatch" in logs[1].lower()
        else:
            assert result == 0, logs[1]
            hashes = subprocess.run([
                "docker", "run", "--rm", "--network", "none", "--entrypoint", "sha256sum",
                image, "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.11.0.jar",
                "/opt/spark/jars/iceberg-aws-bundle-1.11.0.jar",
            ], check=True, capture_output=True, text=True, timeout=30).stdout
            assert runtime_hash in hashes and bundle_hash in hashes
        print(json.dumps({"status": "ready", "wrong_pin": wrong_pin,
                          "attempts": len(commands), "incorrect_response_rejected": True,
                          "fallback_returncode": result, "image": image}, sort_keys=True))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
