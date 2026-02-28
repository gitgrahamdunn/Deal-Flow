from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class SourceArtifact(BaseModel):
    name: str
    url: str
    file_pattern: str | None = None


class SourceConfig(BaseModel):
    enabled: bool = True
    require_clear_terms: bool = False
    page_url: str
    license_note: str = ""
    artifacts: list[SourceArtifact] = Field(default_factory=list)


class AppConfig(BaseModel):
    sources: dict[str, SourceConfig]


def load_config(path: str | Path) -> AppConfig:
    with Path(path).open("r", encoding="utf-8") as fh:
        data: dict[str, Any] = yaml.safe_load(fh)
    return AppConfig.model_validate(data)
