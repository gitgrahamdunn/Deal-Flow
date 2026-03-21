"""Compatibility wrapper to support `python -m deal_flow_ingest.cli` from repo root."""

from .deal_flow_ingest.cli import (
    _read_curated_frame,
    check_sources,
    export_opportunities,
    export_package_candidates,
    export_seller_theses,
    get_package_candidates_frame,
    get_seller_theses_frame,
    main,
    reset_database,
    run_ingestion,
)

__all__ = [
    "main",
    "_read_curated_frame",
    "run_ingestion",
    "check_sources",
    "export_opportunities",
    "export_package_candidates",
    "export_seller_theses",
    "get_package_candidates_frame",
    "get_seller_theses_frame",
    "reset_database",
]

if __name__ == "__main__":
    main()
