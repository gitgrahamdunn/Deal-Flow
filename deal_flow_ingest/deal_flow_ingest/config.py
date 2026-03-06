from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class SourceEntry(BaseModel):
    enabled: bool = True
    source_name: str
    local_sample: str | None = None
    local_live_file: str | None = None
    parser_name: str = "csv"
    landing_page_url: str | None = None
    dataset_url: str | None = None
    file_type: str = "csv"
    refresh_frequency: str = "unknown"
    url: str | None = None
    kind: str | None = None


class AppConfig(BaseModel):
    sources: dict[str, SourceEntry] = Field(default_factory=dict)


class SourcePayload(BaseModel):
    key: str
    source_name: str
    data_kind: str
    enabled: bool
    local_sample: str | None
    local_live_file: str | None
    parser_name: str
    landing_page_url: str | None
    dataset_url: str | None
    file_type: str
    refresh_frequency: str


DATASET_KIND_MAP = {
    "aer_st37": "wells",
    "aer_liability": "liability",
    "aer_spatial_data": "aer_spatial",
    "petrinex_public_monthly_production": "facility_production",
    "petrinex_public_well_facility_bridge": "well_facility_bridge",
    "petrinex_public_facility_master": "facility_master",
    "petrinex_public_data": "operators",
    "open_alberta_placeholder": "open_alberta",
    # Legacy keys
    "aer_st37_well_list": "wells",
    "aer_liability_llr_lca": "liability",
    "aer_licencee_operator_list": "operators",
}


def load_config(config_path: str | Path) -> AppConfig:
    with open(config_path, "r", encoding="utf-8") as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}
    srcs: dict[str, SourceEntry] = {}
    for key, value in (raw.get("sources") or {}).items():
        payload = dict(value or {})
        payload.setdefault("source_name", key)
        srcs[key] = SourceEntry(**payload)
    return AppConfig(sources=srcs)


def iter_enabled_sources(cfg: AppConfig) -> list[SourcePayload]:
    entries: list[SourcePayload] = []
    for key, entry in cfg.sources.items():
        entries.append(
            SourcePayload(
                key=key,
                source_name=entry.source_name,
                data_kind=DATASET_KIND_MAP.get(key, "unknown"),
                enabled=entry.enabled,
                local_sample=entry.local_sample,
                local_live_file=entry.local_live_file,
                parser_name=entry.parser_name,
                landing_page_url=entry.landing_page_url,
                dataset_url=entry.dataset_url or entry.url,
                file_type=entry.file_type or entry.kind or "csv",
                refresh_frequency=entry.refresh_frequency,
            )
        )
    return entries


def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL", "sqlite:///./data/deal_flow.db")
    if database_url.startswith("sqlite:///"):
        db_path = Path(database_url.removeprefix("sqlite:///"))
        if not db_path.is_absolute():
            db_path = Path.cwd() / db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
    return database_url
