from __future__ import annotations

from pathlib import Path

import pandas as pd

from deal_flow.config import SourceConfig
from deal_flow.io.downloader import Downloader


PROD_RENAME = {
    "production_month": "month",
    "operator_name": "operator",
    "oil_bbl": "oil_bbl",
    "gas_mcf": "gas_mcf",
    "water_bbl": "water_bbl",
    "facility_id": "facility_id",
    "well_id": "well_id",
}


def load_petrinex(config: SourceConfig, downloader: Downloader | None, dry_run_dir: Path | None) -> pd.DataFrame:
    if dry_run_dir is not None:
        path = dry_run_dir / "petrinex_production_sample.csv"
    else:
        if downloader is None or not config.artifacts:
            return pd.DataFrame()
        path = downloader.fetch("petrinex", config.artifacts[0].url)
    df = pd.read_csv(path)
    for old, new in PROD_RENAME.items():
        if old in df.columns:
            df = df.rename(columns={old: new})
    needed = ["month", "operator", "oil_bbl", "gas_mcf", "water_bbl", "facility_id", "well_id"]
    for col in needed:
        if col not in df.columns:
            df[col] = None
    df["month"] = pd.to_datetime(df["month"]).dt.to_period("M").dt.to_timestamp()
    df["source"] = "petrinex"
    df["basis_level"] = df["well_id"].notna().map({True: "well", False: "facility"})
    return df[needed + ["source", "basis_level"]]
