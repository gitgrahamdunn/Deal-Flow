from __future__ import annotations

from datetime import date, timedelta


def default_window(end: date) -> tuple[date, date]:
    return (end - timedelta(days=365), end)
