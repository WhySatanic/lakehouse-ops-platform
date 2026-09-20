from __future__ import annotations

import argparse
import json
from pathlib import Path

from lakehouse_ops.authorization_evidence import build_authorization_evidence_manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence_root", type=Path)
    parser.add_argument("--source-revision", required=True)
    args = parser.parse_args()
    report = build_authorization_evidence_manifest(
        args.evidence_root, source_revision=args.source_revision
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
