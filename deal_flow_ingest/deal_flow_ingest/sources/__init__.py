from __future__ import annotations

from pathlib import Path

import pandas as pd

from deal_flow_ingest.config import SourcePayload
from deal_flow_ingest.io.downloader import Downloader
from deal_flow_ingest.sources.aer import (
    load_general_well_data,
    load_liability,
    load_spatial_pipelines,
    load_spatial,
    load_st102_facility_list,
    load_st37,
)
from deal_flow_ingest.sources.ami import load_ami_local_source, load_ami_sample
from deal_flow_ingest.sources.open_alberta import load_placeholder
from deal_flow_ingest.sources.petrinex import load_public_data


def load_dataset(
    downloader: Downloader,
    sample_dir: Path,
    source: SourcePayload,
    dry_run: bool,
    refresh: bool,
    start_date=None,
    end_date=None,
) -> pd.DataFrame:
    if dry_run:
        if not source.local_sample:
            raise ValueError(f"No sample file configured for {source.key}")
        sample_path = sample_dir / source.local_sample
        if source.parser_name.startswith("ami_"):
            return load_ami_sample(sample_path, source.parser_name, source.key)
        return pd.read_csv(sample_path)

    if source.parser_name == "aer_st37":
        return load_st37(downloader, source, refresh)
    if source.parser_name == "aer_general_well_data":
        return load_general_well_data(downloader, source, refresh)
    if source.parser_name == "aer_st102_facility_list":
        return load_st102_facility_list(downloader, source, refresh)
    if source.parser_name == "aer_spatial_pipelines":
        return load_spatial_pipelines(downloader, source, refresh)
    if source.parser_name == "aer_spatial":
        return load_spatial(downloader, source, refresh)
    if source.parser_name == "aer_liability":
        return load_liability(downloader, source, refresh)
    if source.parser_name.startswith("ami_"):
        return load_ami_local_source(downloader, source, refresh)
    if source.parser_name == "petrinex_public":
        return load_public_data(downloader, source, refresh, start_date=start_date, end_date=end_date)
    if source.parser_name == "open_alberta_placeholder":
        return load_placeholder(downloader, source, refresh)

    if not source.dataset_url:
        raise ValueError(f"URL missing for {source.key}")

    file_path = downloader.fetch(source.key, source.dataset_url, refresh=refresh, file_type=source.file_type).path
    return pd.read_csv(file_path)
