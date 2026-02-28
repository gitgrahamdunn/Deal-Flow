from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def with_retries(func: Callable[[], T], retries: int = 3, backoff_s: float = 1.5) -> T:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return func()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt == retries:
                break
            time.sleep(backoff_s * attempt)
    raise RuntimeError(f"Failed after {retries} attempts") from last_error
