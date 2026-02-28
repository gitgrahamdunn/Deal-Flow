from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import requests

from deal_flow.io.cache import cache_key, file_checksum, load_metadata, save_metadata
from deal_flow.utils.retry import with_retries


class Downloader:
    def __init__(self, raw_dir: Path, refresh: bool = False) -> None:
        self.raw_dir = raw_dir
        self.refresh = refresh
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def fetch(self, source: str, url: str) -> Path:
        key = cache_key(url)
        out_path = self.raw_dir / f"{source}_{key}"
        meta_path = out_path.with_suffix(".metadata.json")
        existing = load_metadata(meta_path)

        headers: dict[str, str] = {}
        if not self.refresh:
            if etag := existing.get("etag"):
                headers["If-None-Match"] = etag
            if modified := existing.get("last_modified"):
                headers["If-Modified-Since"] = modified
            if out_path.exists() and not headers:
                return out_path

        def _request() -> requests.Response:
            return requests.get(url, timeout=60, headers=headers)

        resp = with_retries(_request)
        if resp.status_code == 304 and out_path.exists():
            return out_path
        resp.raise_for_status()
        out_path.write_bytes(resp.content)
        payload = {
            "source": source,
            "url": url,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "size": out_path.stat().st_size,
            "checksum_sha256": file_checksum(out_path),
            "etag": resp.headers.get("ETag"),
            "last_modified": resp.headers.get("Last-Modified"),
        }
        save_metadata(meta_path, payload)
        return out_path
