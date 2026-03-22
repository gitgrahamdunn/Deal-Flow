from datetime import date

import pandas as pd

from deal_flow_ingest.transform.metrics import compute_operator_metrics, compute_restart_score, compute_well_restart_scores, month_oil_to_30d_total


def test_restart_score_bounds():
    score = compute_restart_score(20.0, 18.0, 90)
    assert 0 <= score <= 100
    assert score > 50


def test_monthly_to_30d_approximation():
    m = pd.Timestamp("2025-02-01")
    total30 = month_oil_to_30d_total(2800, m)
    assert round(total30, 2) == round(2800 * (30 / 28), 2)


def test_compute_well_restart_scores_empty_prod_short_circuits():
    wells = pd.DataFrame(
        {
            "well_id": [f"W{i}" for i in range(5000)],
            "status": ["SUSPENDED"] * 2500 + ["ACTIVE"] * 2500,
        }
    )

    out = compute_well_restart_scores(wells, pd.DataFrame(), date(2025, 1, 31))

    assert out.empty
    assert list(out.columns) == [
        "as_of_date",
        "well_id",
        "current_status",
        "last_prod_month",
        "avg_oil_bpd_last_3mo_before_shutin",
        "avg_oil_bpd_last_12mo_before_shutin",
        "shutin_recency_days",
        "restart_score",
        "flags",
    ]


def test_compute_well_restart_scores_only_uses_suspended_candidates():
    wells = pd.DataFrame(
        {
            "well_id": ["W1", "W2", "W3", "W4"],
            "status": ["SUSPENDED", "inactive", "SHUT-IN", "ACTIVE"],
        }
    )
    prod = pd.DataFrame(
        {
            "month": ["2025-01-01", "2025-01-01", "2025-01-01", "2025-01-01"],
            "well_id": ["W1", "W2", "W3", "W4"],
            "oil_bbl": [100.0, 50.0, 10.0, 500.0],
            "gas_mcf": [0.0, 0.0, 0.0, 0.0],
            "water_bbl": [0.0, 0.0, 0.0, 0.0],
            "is_estimated": [False, True, False, False],
        }
    )

    out = compute_well_restart_scores(wells, prod, date(2025, 1, 31))

    assert set(out["well_id"]) == {"W1", "W2", "W3"}
    assert "W4" not in set(out["well_id"])



def test_compute_operator_metrics_returns_rows_without_production():
    wells = pd.DataFrame({"well_id": ["W1"], "licensee_operator_id": [101], "status": ["SUSPENDED"]})
    restart = pd.DataFrame({
        "well_id": ["W1"],
        "restart_score": [60.0],
        "avg_oil_bpd_last_3mo_before_shutin": [5.0],
    })
    liability = pd.DataFrame({"operator_id": [101], "ratio": [0.8], "inactive_wells": [10], "active_wells": [5]})

    out = compute_operator_metrics(pd.DataFrame(), liability, restart, wells, date(2025, 1, 31))

    assert len(out) == 1
    assert int(out.iloc[0]["operator_id"]) == 101
    assert float(out.iloc[0]["avg_oil_bpd_30d"]) == 0.0
    assert int(out.iloc[0]["restart_candidates_count"]) == 1


def test_compute_operator_metrics_aggregates_without_per_operator_scans():
    op_prod = pd.DataFrame(
        {
            "month": ["2025-01-01", "2024-12-01", "2024-02-01", "2025-01-01"],
            "operator_id": [101, 101, 101, 202],
            "oil_bbl": [310.0, 620.0, 3100.0, 620.0],
        }
    )
    wells = pd.DataFrame(
        {
            "well_id": ["W1", "W2", "W3"],
            "licensee_operator_id": [101, 101, 202],
            "status": ["SUSPENDED", "INACTIVE", "SUSPENDED"],
        }
    )
    restart = pd.DataFrame(
        {
            "well_id": ["W1", "W2", "W3"],
            "restart_score": [60.0, 40.0, 80.0],
            "avg_oil_bpd_last_3mo_before_shutin": [5.0, 2.0, 9.0],
        }
    )
    liability = pd.DataFrame(
        {
            "operator_id": [101, 202],
            "as_of_date": [date(2025, 1, 31), date(2025, 1, 31)],
            "ratio": [0.8, 1.1],
            "inactive_wells": [10, 1],
            "active_wells": [5, 10],
        }
    )

    out = compute_operator_metrics(op_prod, liability, restart, wells, date(2025, 1, 31)).sort_values("operator_id")

    op101 = out[out["operator_id"] == 101].iloc[0]
    op202 = out[out["operator_id"] == 202].iloc[0]

    assert len(out) == 2
    assert round(float(op101["avg_oil_bpd_30d"]), 2) == 10.0
    assert int(op101["suspended_wells_count"]) == 2
    assert int(op101["restart_candidates_count"]) == 1
    assert float(op101["restart_upside_bpd_est"]) == 5.0
    assert int(op202["restart_candidates_count"]) == 1
    assert float(op202["restart_upside_bpd_est"]) == 9.0
