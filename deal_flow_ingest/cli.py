"""Compatibility wrapper to support `python -m deal_flow_ingest.cli` from repo root."""

from .deal_flow_ingest.cli import export_opportunities, main, run_ingestion

__all__ = ["main", "run_ingestion", "export_opportunities"]

if __name__ == "__main__":
    main()
