from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class SourceConfig(BaseModel):
    enabled: bool = True
    url: str | None = None
    kind: str = "csv"
    source_name: str
    basis_level: str = "facility"


class AppConfig(BaseModel):
    sources: dict[str, SourceConfig] = Field(default_factory=dict)


def load_config(config_path: str | Path) -> AppConfig:
    with open(config_path, "r", encoding="utf-8") as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}
    srcs = {
        key: SourceConfig(source_name=key, **value)
        for key, value in (raw.get("sources") or {}).items()
    }
    return AppConfig(sources=srcs)


def get_database_url() -> str:
    return os.getenv("DATABASE_URL", "sqlite:///./data/deal_flow.db")
