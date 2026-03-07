"""Compatibility wrapper to support `python -m deal_flow_ingest.cli` from repo root."""

from .deal_flow_ingest.cli import export_opportunities, main, reset_database, run_ingestion

__all__ = ["main", "run_ingestion", "export_opportunities", "reset_database"]

if __name__ == "__main__":
    main()
