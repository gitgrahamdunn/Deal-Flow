from datetime import date

import pandas as pd

from deal_flow_ingest.transform.metrics import compute_restart_score, month_oil_to_30d_total


def test_restart_score_bounds():
    score = compute_restart_score(20.0, 18.0, 90)
    assert 0 <= score <= 100
    assert score > 50


def test_monthly_to_30d_approximation():
    m = pd.Timestamp("2025-02-01")
    total30 = month_oil_to_30d_total(2800, m)
    assert round(total30, 2) == round(2800 * (30 / 28), 2)
