from __future__ import annotations

from pathlib import Path

import pandas as pd

from deal_flow_ingest.config import SourcePayload
from deal_flow_ingest.io.downloader import Downloader


def load_dataset(downloader: Downloader, sample_dir: Path, source: SourcePayload, dry_run: bool, refresh: bool) -> pd.DataFrame:
    if dry_run:
        if not source.local_sample:
            raise ValueError(f"No sample file configured for {source.key}")
        return pd.read_csv(sample_dir / source.local_sample)

    if source.data_kind == "open_alberta":
        return load_open_alberta_placeholder(downloader, source, refresh)

    if not source.url:
        raise ValueError(f"URL missing for {source.key}")

    file_path = downloader.fetch(source.key, source.url, refresh=refresh).path
    return pd.read_csv(file_path)


def load_open_alberta_placeholder(downloader: Downloader, source: SourcePayload, refresh: bool) -> pd.DataFrame:
    if not source.url:
        raise ValueError("Open Alberta placeholder url missing")
    file_path = downloader.fetch(source.key, source.url, refresh=refresh).path
    return pd.read_csv(file_path)
