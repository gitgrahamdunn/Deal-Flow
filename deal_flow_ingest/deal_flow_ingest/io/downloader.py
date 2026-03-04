from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import requests

from deal_flow_ingest.io.cache import cache_key_from_url, content_sha256, load_metadata, save_metadata


@dataclass
class DownloadResult:
    path: Path
    downloaded: bool


class Downloader:
    def __init__(self, raw_root: Path):
        self.raw_root = raw_root

    def fetch(self, source: str, url: str, refresh: bool = False, timeout: int = 30) -> DownloadResult:
        source_dir = self.raw_root / source
        source_dir.mkdir(parents=True, exist_ok=True)
        meta = load_metadata(self.raw_root, source)
        key = cache_key_from_url(url)
        file_path = source_dir / f"{key}.bin"

        headers = {}
        if not refresh and meta.get("etag"):
            headers["If-None-Match"] = meta["etag"]
        if not refresh and meta.get("last_modified"):
            headers["If-Modified-Since"] = meta["last_modified"]

        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.status_code == 304 and file_path.exists():
            return DownloadResult(path=file_path, downloaded=False)
        resp.raise_for_status()

        body = resp.content
        digest = content_sha256(body)
        file_path = source_dir / f"{digest}.bin"
        file_path.write_bytes(body)

        save_metadata(
            self.raw_root,
            source,
            {
                "url": url,
                "etag": resp.headers.get("ETag"),
                "last_modified": resp.headers.get("Last-Modified"),
                "sha256": digest,
                "file": file_path.name,
            },
        )
        return DownloadResult(path=file_path, downloaded=True)
