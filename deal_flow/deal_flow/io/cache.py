from __future__ import annotations

import hashlib
import json
from pathlib import Path


def cache_key(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]


def file_checksum(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def load_metadata(meta_path: Path) -> dict:
    if meta_path.exists():
        return json.loads(meta_path.read_text(encoding="utf-8"))
    return {}


def save_metadata(meta_path: Path, payload: dict) -> None:
    meta_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
