"""Compatibility wrapper to support `python -m deal_flow_ingest.cli` from repo root."""

from .deal_flow_ingest.cli import main

if __name__ == "__main__":
    main()
