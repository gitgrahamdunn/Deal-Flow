from __future__ import annotations

from datetime import date

import pandas as pd

SUSPENDED_STATUSES = {"SUSPENDED", "INACTIVE", "SHUT-IN", "SHUTIN"}


def _to_month_start(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.to_period("M").dt.to_timestamp()


def _compute_months_since(last_prod_month: pd.Series, as_of_date: date) -> pd.Series:
    as_of_period = pd.Period(as_of_date, freq="M")
    last_period = pd.to_datetime(last_prod_month, errors="coerce").dt.to_period("M")
    months = as_of_period - last_period
    return months.map(lambda v: float(v.n) if pd.notna(v) else float("nan"))


def compute_well_opportunities(
    wells_df: pd.DataFrame,
    well_prod_df: pd.DataFrame,
    restart_df: pd.DataFrame,
    operator_metrics_df: pd.DataFrame,
    as_of_date: date,
) -> pd.DataFrame:
    if wells_df.empty:
        return pd.DataFrame(
            columns=[
                "as_of_date",
                "well_id",
                "operator_id",
                "current_status",
                "last_prod_month",
                "months_since_last_production",
                "avg_oil_bpd_last_3mo_before_shutin",
                "avg_oil_bpd_last_12mo_before_shutin",
                "restart_score",
                "operator_distress_score",
                "operator_restart_upside_bpd_est",
                "is_estimated_production",
                "stripper_flag",
                "stripper_score",
                "opportunity_tier",
                "screening_notes",
            ]
        )

    wells = wells_df[["well_id", "status", "licensee_operator_id"]].copy()
    wells["current_status"] = wells["status"].fillna("UNKNOWN").astype(str).str.upper().str.strip()
    wells["operator_id"] = pd.to_numeric(wells["licensee_operator_id"], errors="coerce")

    restart = restart_df.copy()
    if restart.empty:
        restart = pd.DataFrame(columns=[
            "well_id",
            "current_status",
            "last_prod_month",
            "avg_oil_bpd_last_3mo_before_shutin",
            "avg_oil_bpd_last_12mo_before_shutin",
            "restart_score",
        ])

    out = wells.merge(
        restart[
            [
                "well_id",
                "current_status",
                "last_prod_month",
                "avg_oil_bpd_last_3mo_before_shutin",
                "avg_oil_bpd_last_12mo_before_shutin",
                "restart_score",
            ]
        ],
        on="well_id",
        how="left",
        suffixes=("", "_restart"),
    )

    out["current_status"] = out["current_status_restart"].fillna(out["current_status"])
    out = out.drop(columns=["status", "licensee_operator_id", "current_status_restart"])

    for col in [
        "avg_oil_bpd_last_3mo_before_shutin",
        "avg_oil_bpd_last_12mo_before_shutin",
        "restart_score",
    ]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)

    if not well_prod_df.empty:
        prod = well_prod_df[["well_id", "month", "oil_bbl", "is_estimated"]].copy()
        prod["month"] = _to_month_start(prod["month"])
        prod["oil_bbl"] = pd.to_numeric(prod["oil_bbl"], errors="coerce").fillna(0.0)
        prod["is_estimated"] = prod["is_estimated"].fillna(False).astype(bool)
        prod_by_well = prod.groupby("well_id", as_index=False).agg(
            has_non_zero_oil=("oil_bbl", lambda s: bool((s > 0).any())),
            is_estimated_production=("is_estimated", "any"),
            last_prod_month_from_prod=("month", "max"),
        )
    else:
        prod_by_well = pd.DataFrame(columns=["well_id", "has_non_zero_oil", "is_estimated_production", "last_prod_month_from_prod"])

    out = out.merge(prod_by_well, on="well_id", how="left")
    out["has_non_zero_oil"] = out["has_non_zero_oil"].astype("boolean").fillna(False).astype(bool)
    out["is_estimated_production"] = out["is_estimated_production"].astype("boolean").fillna(False).astype(bool)

    out["last_prod_month"] = out["last_prod_month"].where(out["last_prod_month"].notna(), out["last_prod_month_from_prod"])
    out["last_prod_month"] = _to_month_start(out["last_prod_month"]).dt.date
    out["months_since_last_production"] = _compute_months_since(out["last_prod_month"], as_of_date)

    op = operator_metrics_df[["operator_id", "distress_score", "restart_upside_bpd_est"]].copy() if not operator_metrics_df.empty else pd.DataFrame(columns=["operator_id", "distress_score", "restart_upside_bpd_est"])
    op["operator_id"] = pd.to_numeric(op["operator_id"], errors="coerce")
    op = op.rename(columns={"distress_score": "operator_distress_score", "restart_upside_bpd_est": "operator_restart_upside_bpd_est"})
    out = out.merge(op, on="operator_id", how="left")
    out["operator_distress_score"] = pd.to_numeric(out["operator_distress_score"], errors="coerce").fillna(0.0)
    out["operator_restart_upside_bpd_est"] = pd.to_numeric(out["operator_restart_upside_bpd_est"], errors="coerce").fillna(0.0)

    out["stripper_flag"] = out["avg_oil_bpd_last_12mo_before_shutin"] <= 10.0
    recent_component = (12.0 - out["months_since_last_production"].fillna(36.0).clip(lower=0.0, upper=12.0)) / 12.0
    prod_component = (out["avg_oil_bpd_last_12mo_before_shutin"].clip(lower=0.0, upper=10.0) / 10.0)

    out["stripper_score"] = (
        out["restart_score"] * 0.55
        + recent_component * 25.0
        + prod_component * 10.0
        + out["operator_distress_score"] * 0.20
    )

    suspended_mask = out["current_status"].isin(SUSPENDED_STATUSES)
    out.loc[~suspended_mask, "stripper_score"] -= 20.0
    out.loc[~out["has_non_zero_oil"], "stripper_score"] -= 25.0
    out.loc[out["is_estimated_production"], "stripper_score"] -= 5.0

    out["stripper_score"] = out["stripper_score"].clip(lower=0.0, upper=100.0)

    out["opportunity_tier"] = pd.cut(
        out["stripper_score"],
        bins=[-0.01, 30.0, 50.0, 70.0, 100.0],
        labels=["IGNORE", "C", "B", "A"],
        right=False,
    ).astype(str)

    def build_notes(row: pd.Series) -> str:
        rules: list[str] = []
        if row["months_since_last_production"] <= 12:
            rules.append("recent_shutin")
        if row["has_non_zero_oil"]:
            rules.append("historical_oil_present")
        else:
            rules.append("no_production_history")
        if row["is_estimated_production"]:
            rules.append("estimated_well_production")
        if row["operator_distress_score"] >= 60:
            rules.append("operator_distress_high")
        if row["stripper_flag"]:
            rules.append("stripper_profile")
        notes = {
            "rules": rules,
            "status": row["current_status"],
            "restart_score": round(float(row["restart_score"]), 2),
            "months_since_last_production": None
            if pd.isna(row["months_since_last_production"])
            else int(row["months_since_last_production"]),
        }
        return str(notes)

    out["screening_notes"] = out.apply(build_notes, axis=1)
    out["as_of_date"] = as_of_date

    return out[
        [
            "as_of_date",
            "well_id",
            "operator_id",
            "current_status",
            "last_prod_month",
            "months_since_last_production",
            "avg_oil_bpd_last_3mo_before_shutin",
            "avg_oil_bpd_last_12mo_before_shutin",
            "restart_score",
            "operator_distress_score",
            "operator_restart_upside_bpd_est",
            "is_estimated_production",
            "stripper_flag",
            "stripper_score",
            "opportunity_tier",
            "screening_notes",
        ]
    ].sort_values(["stripper_score", "restart_score"], ascending=False)
