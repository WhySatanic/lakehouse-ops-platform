from __future__ import annotations

import argparse
import json
from pathlib import Path

from lakehouse_ops.authorization_evidence import validate_authorization_evidence_manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--evidence-root", type=Path, required=True)
    parser.add_argument("--expected-source-revision", required=True)
    parser.add_argument("--strict-membership", action="store_true")
    args = parser.parse_args()
    report = json.loads(args.manifest.read_text(encoding="utf-8"))
    result = validate_authorization_evidence_manifest(
        report,
        args.evidence_root,
        expected_source_revision=args.expected_source_revision,
        strict_membership=args.strict_membership,
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
