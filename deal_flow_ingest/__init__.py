"""Repository-root package shim for running without installation."""

from pathlib import Path

_pkg_dir = Path(__file__).resolve().parent / "deal_flow_ingest"
if _pkg_dir.is_dir():
    __path__.append(str(_pkg_dir))

__all__: list[str] = []
