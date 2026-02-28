from __future__ import annotations

from pathlib import Path

import pandas as pd

from deal_flow.config import SourceConfig
from deal_flow.io.downloader import Downloader


def load_aer_wells(config: SourceConfig, downloader: Downloader | None, dry_run_dir: Path | None) -> pd.DataFrame:
    if dry_run_dir is not None:
        path = dry_run_dir / "aer_wells_sample.csv"
    else:
        if downloader is None or not config.artifacts:
            return pd.DataFrame()
        path = downloader.fetch("aer", config.artifacts[0].url)
    df = pd.read_csv(path)
    rename_map = {
        "uwi": "well_id",
        "licensee": "operator",
        "well_status": "status",
        "surface_location": "surface_location",
        "lat": "latitude",
        "lon": "longitude",
        "facility_id": "facility_id",
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.rename(columns={old: new})
    for col in ["well_id", "operator", "status", "surface_location", "latitude", "longitude", "facility_id"]:
        if col not in df.columns:
            df[col] = None
    return df[["well_id", "operator", "status", "surface_location", "latitude", "longitude", "facility_id"]]
