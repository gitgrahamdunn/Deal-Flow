from __future__ import annotations

import json
import mimetypes
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZipFile

import requests

from deal_flow_ingest.io.cache import cache_key_from_url, content_sha256, load_metadata, save_metadata

DEFAULT_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-CA,en;q=0.9",
}


@dataclass
class DownloadResult:
    path: Path
    downloaded: bool
    status_code: int
    checksum: str
    content_type: str
    extracted_dir: Path | None = None


class Downloader:
    def __init__(self, raw_root: Path):
        self.raw_root = raw_root
        self.last_result: DownloadResult | None = None
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_BROWSER_HEADERS)

    def _load_metadata(self, source: str) -> dict:
        return load_metadata(self.raw_root, source)

    def _save_metadata(self, source: str, metadata: dict) -> None:
        save_metadata(self.raw_root, source, metadata)

    def fetch(
        self,
        source: str,
        url: str,
        refresh: bool = False,
        timeout: int = 30,
        file_type: str | None = None,
        extract_zip: bool = False,
    ) -> DownloadResult:
        source_dir = self.raw_root / source
        source_dir.mkdir(parents=True, exist_ok=True)
        meta = load_metadata(self.raw_root, source)
        artifacts = meta.setdefault("artifacts", {})
        url_key = cache_key_from_url(url)
        url_meta = artifacts.get(url_key, {})

        headers = {}
        if not refresh and url_meta.get("etag"):
            headers["If-None-Match"] = url_meta["etag"]
        if not refresh and url_meta.get("last_modified"):
            headers["If-Modified-Since"] = url_meta["last_modified"]

        request_headers = dict(headers)
        if file_type and file_type.lower() == "html":
            request_headers.setdefault("Referer", url)

        resp = self.session.get(url, headers=request_headers, timeout=timeout)
        cached_name = url_meta.get("file")
        if resp.status_code == 304 and cached_name:
            cached_path = source_dir / cached_name
            if cached_path.exists():
                extracted_dir = self._extract_if_needed(cached_path, extract_zip)
                self.last_result = DownloadResult(
                    path=cached_path,
                    downloaded=False,
                    status_code=304,
                    checksum=url_meta.get("checksum", url_meta.get("sha256", "")),
                    content_type=url_meta.get("content_type", ""),
                    extracted_dir=extracted_dir,
                )
                return self.last_result

        resp.raise_for_status()
        body = resp.content
        checksum = content_sha256(body)
        content_type = resp.headers.get("Content-Type", "").split(";")[0].strip()
        suffix = self._guess_suffix(file_type or content_type)
        file_path = source_dir / f"{checksum}{suffix}"
        file_path.write_bytes(body)

        artifact_meta = {
            "source_key": source,
            "url": url,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "status_code": resp.status_code,
            "checksum": checksum,
            "content_type": content_type,
            "etag": resp.headers.get("ETag"),
            "last_modified": resp.headers.get("Last-Modified"),
            "file": file_path.name,
        }
        (file_path.with_suffix(file_path.suffix + ".metadata.json")).write_text(
            json.dumps(artifact_meta, indent=2), encoding="utf-8"
        )

        artifacts[url_key] = artifact_meta
        meta["last_updated"] = artifact_meta["fetched_at"]
        save_metadata(self.raw_root, source, meta)

        extracted_dir = self._extract_if_needed(file_path, extract_zip)
        self.last_result = DownloadResult(
            path=file_path,
            downloaded=True,
            status_code=resp.status_code,
            checksum=checksum,
            content_type=content_type,
            extracted_dir=extracted_dir,
        )
        return self.last_result

    def _extract_if_needed(self, file_path: Path, extract_zip: bool) -> Path | None:
        if not extract_zip or file_path.suffix.lower() != ".zip":
            return None
        extract_dir = file_path.parent / f"{file_path.stem}_extracted"
        extract_dir.mkdir(parents=True, exist_ok=True)
        with ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)
        return extract_dir

    @staticmethod
    def _guess_suffix(file_type: str | None) -> str:
        if not file_type:
            return ".bin"
        token = file_type.lower().strip()
        if token in {"csv", "txt", "zip", "json", "xml", "html"}:
            return f".{token}"
        if "/" in token:
            guessed = mimetypes.guess_extension(token)
            if guessed:
                return guessed
        return ".bin"
