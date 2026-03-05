from __future__ import annotations

import logging

import pandas as pd

from deal_flow_ingest.config import SourcePayload
from deal_flow_ingest.io.downloader import Downloader

LOGGER = logging.getLogger(__name__)


def load_placeholder(downloader: Downloader, source: SourcePayload, refresh: bool) -> pd.DataFrame:
    if source.landing_page_url:
        try:
            downloader.fetch(source.key, source.landing_page_url, refresh=refresh, file_type="html")
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch Open Alberta landing page: %s", exc)
    if not source.dataset_url:
        LOGGER.info("Open Alberta placeholder configured without dataset; returning empty frame")
        return pd.DataFrame()
    result = downloader.fetch(source.key, source.dataset_url, refresh=refresh, file_type=source.file_type)
    return pd.read_csv(result.path)
