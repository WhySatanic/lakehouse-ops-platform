from __future__ import annotations

import hashlib
from pathlib import Path


def normalized_text_digest(path: Path) -> str:
    content = path.read_bytes().replace(b"\r\n", b"\n")
    return hashlib.sha256(content).hexdigest()
