from __future__ import annotations

from deal_flow.config import AppConfig


def terms_rows(config: AppConfig) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for name, src in config.sources.items():
        rows.append(
            {
                "source": name,
                "public_page": src.page_url,
                "usage_notes": src.license_note or "Public/free source as configured by user.",
            }
        )
    return rows
