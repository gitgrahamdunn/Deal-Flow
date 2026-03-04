from __future__ import annotations

from pathlib import Path

import pandas as pd

from deal_flow_ingest.io.downloader import Downloader


def load(downloader: Downloader, sample_dir: Path, dry_run: bool, refresh: bool, url: str | None) -> pd.DataFrame:
    if dry_run:
        return pd.read_csv(sample_dir / "production_sample.csv")
    if not url:
        raise ValueError("aer URL missing")
    file_path = downloader.fetch("aer", url, refresh=refresh).path
    return pd.read_csv(file_path)
