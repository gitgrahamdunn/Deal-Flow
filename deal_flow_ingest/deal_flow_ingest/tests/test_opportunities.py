from datetime import date

import pandas as pd

from deal_flow_ingest.transform.opportunities import compute_well_opportunities


def _base_inputs():
    wells = pd.DataFrame(
        [
            {"well_id": "W1", "status": "SUSPENDED", "licensee_operator_id": 1},
            {"well_id": "W2", "status": "SUSPENDED", "licensee_operator_id": 2},
            {"well_id": "W3", "status": "INACTIVE", "licensee_operator_id": 2},
        ]
    )
    restart = pd.DataFrame(
        [
            {
                "well_id": "W1",
                "current_status": "SUSPENDED",
                "last_prod_month": date(2024, 12, 1),
                "avg_oil_bpd_last_3mo_before_shutin": 8.0,
                "avg_oil_bpd_last_12mo_before_shutin": 6.0,
                "restart_score": 72.0,
            },
            {
                "well_id": "W3",
                "current_status": "INACTIVE",
                "last_prod_month": date(2024, 11, 1),
                "avg_oil_bpd_last_3mo_before_shutin": 3.0,
                "avg_oil_bpd_last_12mo_before_shutin": 2.0,
                "restart_score": 40.0,
            },
        ]
    )
    operator_metrics = pd.DataFrame(
        [
            {"operator_id": 1, "distress_score": 20.0, "restart_upside_bpd_est": 50.0},
            {"operator_id": 2, "distress_score": 80.0, "restart_upside_bpd_est": 120.0},
        ]
    )
    return wells, restart, operator_metrics


def test_well_with_no_production_history_penalized():
    wells, restart, operator_metrics = _base_inputs()
    prod = pd.DataFrame(columns=["well_id", "month", "oil_bbl", "is_estimated"])

    out = compute_well_opportunities(wells, prod, restart, operator_metrics, as_of_date=date(2025, 1, 31))
    w2 = out[out["well_id"] == "W2"].iloc[0]
    assert "no_production_history" in w2["screening_notes"]
    assert w2["stripper_score"] < 30


def test_suspended_recent_production_prioritized():
    wells, restart, operator_metrics = _base_inputs()
    prod = pd.DataFrame(
        [
            {"well_id": "W1", "month": "2024-12-01", "oil_bbl": 240.0, "is_estimated": False},
            {"well_id": "W1", "month": "2024-11-01", "oil_bbl": 220.0, "is_estimated": False},
        ]
    )

    out = compute_well_opportunities(wells, prod, restart, operator_metrics, as_of_date=date(2025, 1, 31))
    w1 = out[out["well_id"] == "W1"].iloc[0]
    assert w1["stripper_score"] >= 70
    assert w1["opportunity_tier"] == "A"
    assert "recent_shutin" in w1["screening_notes"]


def test_estimated_production_flagged():
    wells, restart, operator_metrics = _base_inputs()
    prod = pd.DataFrame(
        [{"well_id": "W1", "month": "2024-12-01", "oil_bbl": 200.0, "is_estimated": True}]
    )

    out = compute_well_opportunities(wells, prod, restart, operator_metrics, as_of_date=date(2025, 1, 31))
    w1 = out[out["well_id"] == "W1"].iloc[0]
    assert bool(w1["is_estimated_production"]) is True
    assert "estimated_well_production" in w1["screening_notes"]


def test_operator_distress_increases_ranking():
    wells = pd.DataFrame(
        [
            {"well_id": "A", "status": "SUSPENDED", "licensee_operator_id": 1},
            {"well_id": "B", "status": "SUSPENDED", "licensee_operator_id": 2},
        ]
    )
    restart = pd.DataFrame(
        [
            {
                "well_id": "A",
                "current_status": "SUSPENDED",
                "last_prod_month": date(2024, 12, 1),
                "avg_oil_bpd_last_3mo_before_shutin": 6.0,
                "avg_oil_bpd_last_12mo_before_shutin": 5.0,
                "restart_score": 60.0,
            },
            {
                "well_id": "B",
                "current_status": "SUSPENDED",
                "last_prod_month": date(2024, 12, 1),
                "avg_oil_bpd_last_3mo_before_shutin": 6.0,
                "avg_oil_bpd_last_12mo_before_shutin": 5.0,
                "restart_score": 60.0,
            },
        ]
    )
    operator_metrics = pd.DataFrame(
        [
            {"operator_id": 1, "distress_score": 10.0, "restart_upside_bpd_est": 10.0},
            {"operator_id": 2, "distress_score": 90.0, "restart_upside_bpd_est": 10.0},
        ]
    )
    prod = pd.DataFrame(
        [
            {"well_id": "A", "month": "2024-12-01", "oil_bbl": 180.0, "is_estimated": False},
            {"well_id": "B", "month": "2024-12-01", "oil_bbl": 180.0, "is_estimated": False},
        ]
    )

    out = compute_well_opportunities(wells, prod, restart, operator_metrics, as_of_date=date(2025, 1, 31))
    assert float(out[out["well_id"] == "B"]["stripper_score"].iloc[0]) > float(
        out[out["well_id"] == "A"]["stripper_score"].iloc[0]
    )


def test_tier_thresholds():
    wells = pd.DataFrame(
        [
            {"well_id": "C1", "status": "SUSPENDED", "licensee_operator_id": 1},
            {"well_id": "C2", "status": "SUSPENDED", "licensee_operator_id": 1},
            {"well_id": "C3", "status": "SUSPENDED", "licensee_operator_id": 1},
        ]
    )
    restart = pd.DataFrame(
        [
            {"well_id": "C1", "current_status": "SUSPENDED", "last_prod_month": date(2024, 12, 1), "avg_oil_bpd_last_3mo_before_shutin": 0.0, "avg_oil_bpd_last_12mo_before_shutin": 0.0, "restart_score": 95.0},
            {"well_id": "C2", "current_status": "SUSPENDED", "last_prod_month": date(2023, 1, 1), "avg_oil_bpd_last_3mo_before_shutin": 0.0, "avg_oil_bpd_last_12mo_before_shutin": 0.0, "restart_score": 92.0},
            {"well_id": "C3", "current_status": "SUSPENDED", "last_prod_month": date(2022, 1, 1), "avg_oil_bpd_last_3mo_before_shutin": 0.0, "avg_oil_bpd_last_12mo_before_shutin": 0.0, "restart_score": 60.0},
        ]
    )
    operator_metrics = pd.DataFrame([{"operator_id": 1, "distress_score": 0.0, "restart_upside_bpd_est": 0.0}])
    prod = pd.DataFrame(
        [
            {"well_id": "C1", "month": "2024-12-01", "oil_bbl": 30.0, "is_estimated": False},
            {"well_id": "C2", "month": "2023-01-01", "oil_bbl": 30.0, "is_estimated": False},
            {"well_id": "C3", "month": "2022-01-01", "oil_bbl": 30.0, "is_estimated": False},
        ]
    )

    out = compute_well_opportunities(wells, prod, restart, operator_metrics, as_of_date=date(2025, 1, 31))
    tiers = dict(zip(out["well_id"], out["opportunity_tier"]))
    assert tiers["C1"] == "A"
    assert tiers["C2"] == "B"
    assert tiers["C3"] == "C"
