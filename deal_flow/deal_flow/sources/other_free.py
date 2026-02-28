from __future__ import annotations

import pandas as pd

from deal_flow.config import SourceConfig
from deal_flow.io.downloader import Downloader


def load_other_free(config: SourceConfig, downloader: Downloader | None) -> pd.DataFrame:
    if not config.enabled:
        return pd.DataFrame()
    if config.require_clear_terms:
        return pd.DataFrame()
    if downloader is None or not config.artifacts:
        return pd.DataFrame()
    path = downloader.fetch("other_free", config.artifacts[0].url)
    return pd.read_csv(path)
