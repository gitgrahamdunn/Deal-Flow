from __future__ import annotations

import pandas as pd

from deal_flow.config import SourceConfig
from deal_flow.io.downloader import Downloader


def load_open_alberta(config: SourceConfig, downloader: Downloader | None) -> pd.DataFrame:
    if downloader is None or not config.artifacts:
        return pd.DataFrame()
    path = downloader.fetch("open_alberta", config.artifacts[0].url)
    df = pd.read_csv(path)
    df["source"] = "open_alberta"
    return df
