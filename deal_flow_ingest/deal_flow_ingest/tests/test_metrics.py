from datetime import date

import pandas as pd

from deal_flow_ingest.transform.metrics import compute_operator_metrics


def test_compute_metrics_basic():
    df = pd.DataFrame(
        {
            "month": ["2025-04-01", "2025-05-01", "2024-08-01"],
            "operator_id": [1, 1, 2],
            "oil_bbl": [3000.0, 3100.0, 3650.0],
        }
    )
    out = compute_operator_metrics(df, date(2025, 5, 31))
    op1 = out[out["operator_id"] == 1].iloc[0]
    assert round(op1["total_oil_bbl_30d"], 2) == 3100.0
    assert round(op1["avg_oil_bpd_30d"], 2) == round(3100.0 / 30.0, 2)


def test_compute_metrics_empty():
    out = compute_operator_metrics(pd.DataFrame(columns=["month", "operator_id", "oil_bbl"]), date(2025, 5, 31))
    assert out.empty
