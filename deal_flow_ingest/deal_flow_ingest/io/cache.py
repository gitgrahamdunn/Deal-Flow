from __future__ import annotations

import hashlib
import json
from pathlib import Path


def cache_key_from_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def content_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def metadata_path(raw_dir: Path, source: str) -> Path:
    return raw_dir / source / "_meta.json"


def load_metadata(raw_dir: Path, source: str) -> dict:
    path = metadata_path(raw_dir, source)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"artifacts": {}}


def save_metadata(raw_dir: Path, source: str, metadata: dict) -> None:
    path = metadata_path(raw_dir, source)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
