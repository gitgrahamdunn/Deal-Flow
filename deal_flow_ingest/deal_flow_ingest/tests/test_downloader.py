from __future__ import annotations

from pathlib import Path

import requests

from deal_flow_ingest.io.downloader import Downloader


class _Response:
    status_code = 200
    headers = {"Content-Type": "text/plain"}
    content = b"ok"

    def raise_for_status(self) -> None:
        return None


def test_fetch_retries_transient_request_exception(tmp_path: Path) -> None:
    downloader = Downloader(tmp_path)
    calls = {"count": 0}

    def fake_get(url, headers=None, timeout=None):  # noqa: ANN001
        calls["count"] += 1
        if calls["count"] == 1:
            raise requests.exceptions.ChunkedEncodingError("truncated")
        return _Response()

    downloader.session.get = fake_get  # type: ignore[method-assign]

    result = downloader.fetch("test_source", "https://example.com/data.txt", refresh=True, file_type="txt")

    assert calls["count"] == 2
    assert result.path.exists()
    assert result.status_code == 200
