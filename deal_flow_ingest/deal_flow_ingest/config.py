from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class SourceEntry(BaseModel):
    enabled: bool = True
    url: str | None = None
    kind: str = "csv"
    source_name: str
    local_sample: str | None = None


class AppConfig(BaseModel):
    sources: dict[str, SourceEntry] = Field(default_factory=dict)


class SourcePayload(BaseModel):
    key: str
    source_name: str
    data_kind: str
    kind: str
    enabled: bool
    url: str | None
    local_sample: str | None


DATASET_KIND_MAP = {
    "aer_st37_well_list": "wells",
    "aer_licencee_operator_list": "operators",
    "aer_liability_llr_lca": "liability",
    "petrinex_public_monthly_production": "facility_production",
    "petrinex_public_well_facility_bridge": "well_facility_bridge",
    "petrinex_public_facility_master": "facility_master",
    "open_alberta_placeholder": "open_alberta",
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
                kind=entry.kind,
                enabled=entry.enabled,
                url=entry.url,
                local_sample=entry.local_sample,
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
