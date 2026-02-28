from datetime import date

import pandas as pd

from deal_flow.transform.metrics import compute_operator_metrics


def test_compute_operator_metrics_basic() -> None:
    df = pd.DataFrame(
        {
            "month": pd.to_datetime(["2026-01-01", "2026-01-01", "2025-08-01"]),
            "operator": ["A", "B", "A"],
            "oil_bbl": [3000, 1500, 1200],
        }
    )
    out = compute_operator_metrics(df, date(2026, 1, 31))
    row_a = out[out["operator"] == "A"].iloc[0]
    assert row_a["total_oil_bbl_30d"] == 3000
    assert row_a["total_oil_bbl_365d"] == 4200
