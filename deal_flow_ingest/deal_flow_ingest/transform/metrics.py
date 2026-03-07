from __future__ import annotations

from calendar import monthrange
from datetime import date
import logging

import numpy as np
import pandas as pd


LOGGER = logging.getLogger(__name__)

RESTART_SCORE_COLUMNS = [
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


def empty_restart_scores_df() -> pd.DataFrame:
    return pd.DataFrame(columns=RESTART_SCORE_COLUMNS)


def month_oil_to_30d_total(oil_bbl: float, month: pd.Timestamp) -> float:
    days = monthrange(month.year, month.month)[1]
    return float(oil_bbl) * (30.0 / days)


def compute_restart_score(avg_3mo_bpd: float, avg_12mo_bpd: float, shutin_recency_days: int) -> float:
    rate_component = min(max(avg_3mo_bpd, 0.0) * 3.0, 60.0)
    recency_component = max(0.0, 30.0 * (1 - (shutin_recency_days / 730.0)))
    if avg_12mo_bpd <= 0:
        stability_component = 2.0
    else:
        ratio = min(avg_3mo_bpd, avg_12mo_bpd) / max(avg_3mo_bpd, avg_12mo_bpd)
        stability_component = 10.0 * ratio
    return max(0.0, min(100.0, rate_component + recency_component + stability_component))


def compute_well_restart_scores(
    wells_df: pd.DataFrame,
    well_prod_df: pd.DataFrame,
    as_of_date: date,
    suspended_statuses: set[str] | None = None,
) -> pd.DataFrame:
    suspended_statuses = suspended_statuses or {"SUSPENDED", "INACTIVE", "SHUT-IN", "SHUTIN"}
    if wells_df.empty:
        return empty_restart_scores_df()

    candidate_wells = wells_df.copy()
    candidate_wells["current_status"] = candidate_wells["status"].astype(str).str.upper().str.strip()
    candidate_wells = candidate_wells[candidate_wells["current_status"].isin(suspended_statuses)][["well_id", "current_status"]]
    if candidate_wells.empty:
        return empty_restart_scores_df()

    if well_prod_df.empty:
        LOGGER.info("Skipping restart score computation because no well production data is available")
        return empty_restart_scores_df()

    prod = well_prod_df.copy()
    prod["month"] = pd.to_datetime(prod["month"]).dt.to_period("M").dt.to_timestamp()
    prod["well_id"] = prod["well_id"].astype(str)

    production_cols = [c for c in ["oil_bbl", "gas_mcf", "water_bbl"] if c in prod.columns]
    if not production_cols:
        production_cols = ["oil_bbl"]
    prod[production_cols] = prod[production_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    nonzero_mask = prod[production_cols].ne(0.0).any(axis=1)
    if not bool(nonzero_mask.any()):
        LOGGER.info("Skipping restart score computation because no well production data is available")
        return empty_restart_scores_df()

    candidate_wells["well_id"] = candidate_wells["well_id"].astype(str)
    prod = prod[nonzero_mask & prod["well_id"].isin(candidate_wells["well_id"])]

    last_prod_month = prod.groupby("well_id")["month"].max().rename("last_prod_month")
    prod = prod.join(last_prod_month, on="well_id")
    month_delta = (
        (prod["last_prod_month"].dt.year - prod["month"].dt.year) * 12
        + (prod["last_prod_month"].dt.month - prod["month"].dt.month)
    )
    avg3 = (prod[month_delta <= 2].groupby("well_id")["oil_bbl"].mean() / 30.0).rename(
        "avg_oil_bpd_last_3mo_before_shutin"
    )
    avg12 = (prod[month_delta <= 11].groupby("well_id")["oil_bbl"].mean() / 30.0).rename(
        "avg_oil_bpd_last_12mo_before_shutin"
    )
    est_col = prod["is_estimated"] if "is_estimated" in prod.columns else pd.Series(False, index=prod.index)
    is_estimated = est_col.fillna(False).astype(bool).groupby(prod["well_id"]).any().rename("is_estimated")

    scores = candidate_wells.merge(last_prod_month.reset_index(), on="well_id", how="left")
    scores = scores.merge(avg3.reset_index(), on="well_id", how="left")
    scores = scores.merge(avg12.reset_index(), on="well_id", how="left")
    scores = scores.merge(is_estimated.reset_index(), on="well_id", how="left")

    has_prod = scores["last_prod_month"].notna()
    scores["avg_oil_bpd_last_3mo_before_shutin"] = scores["avg_oil_bpd_last_3mo_before_shutin"].fillna(0.0)
    scores["avg_oil_bpd_last_12mo_before_shutin"] = scores["avg_oil_bpd_last_12mo_before_shutin"].fillna(0.0)
    scores["shutin_recency_days"] = np.where(
        has_prod,
        (pd.Timestamp(as_of_date) - (scores["last_prod_month"] + pd.offsets.MonthEnd(0))).dt.days,
        np.nan,
    )

    avg3_series = scores["avg_oil_bpd_last_3mo_before_shutin"]
    avg12_series = scores["avg_oil_bpd_last_12mo_before_shutin"]
    rate_component = (avg3_series.clip(lower=0.0) * 3.0).clip(upper=60.0)
    recency_component = (30.0 * (1 - (scores["shutin_recency_days"].fillna(730.0) / 730.0))).clip(lower=0.0)
    ratio = np.minimum(avg3_series, avg12_series) / np.maximum(avg3_series, avg12_series).replace(0.0, np.nan)
    stability_component = np.where(avg12_series <= 0.0, 2.0, 10.0 * np.nan_to_num(ratio, nan=0.0))
    computed_scores = (rate_component + recency_component + stability_component).clip(lower=0.0, upper=100.0)
    scores["restart_score"] = np.where(has_prod, computed_scores, 0.0)
    scores["flags"] = np.where(
        has_prod,
        np.where(scores["is_estimated"].fillna(False), {"method": "screening_estimate"}, {"method": "observed"}),
        {"reason": "no_production_history"},
    )
    scores["as_of_date"] = as_of_date
    scores["last_prod_month"] = pd.to_datetime(scores["last_prod_month"], errors="coerce").dt.date
    scores["shutin_recency_days"] = pd.to_numeric(scores["shutin_recency_days"], errors="coerce").astype("Int64")
    scores.loc[~has_prod, "shutin_recency_days"] = pd.NA

    return scores[RESTART_SCORE_COLUMNS]


def compute_operator_metrics(
    operator_prod_df: pd.DataFrame,
    liability_df: pd.DataFrame,
    restart_df: pd.DataFrame,
    wells_df: pd.DataFrame,
    as_of_date: date,
) -> pd.DataFrame:
    if operator_prod_df.empty:
        return pd.DataFrame()

    prod = operator_prod_df.copy()
    prod["month"] = pd.to_datetime(prod["month"]).dt.to_period("M").dt.to_timestamp()
    as_of_ts = pd.Timestamp(as_of_date)
    current_month = as_of_ts.to_period("M").to_timestamp()

    rows = []
    for operator_id, grp in prod.groupby("operator_id"):
        grp = grp.sort_values("month")
        latest = grp[grp["month"] == current_month]
        latest_oil = float(latest["oil_bbl"].sum()) if not latest.empty else 0.0
        total30 = month_oil_to_30d_total(latest_oil, current_month)
        avg30 = total30 / 30.0

        trailing12 = grp[grp["month"] > current_month - pd.DateOffset(months=12)]
        prev12 = grp[(grp["month"] <= current_month - pd.DateOffset(months=12)) & (grp["month"] > current_month - pd.DateOffset(months=24))]
        total365 = float(trailing12["oil_bbl"].sum())
        avg365 = total365 / 365.0
        prev_total = float(prev12["oil_bbl"].sum())
        yoy = ((total365 - prev_total) / prev_total * 100.0) if prev_total > 0 else None
        decline_score = min(100.0, max(0.0, -(yoy or 0.0)))

        op_liability = liability_df[liability_df["operator_id"] == operator_id]
        ratio = float(op_liability["ratio"].iloc[-1]) if not op_liability.empty and pd.notna(op_liability["ratio"].iloc[-1]) else None
        inactive = float(op_liability["inactive_wells"].iloc[-1]) if not op_liability.empty and pd.notna(op_liability["inactive_wells"].iloc[-1]) else 0.0
        active = float(op_liability["active_wells"].iloc[-1]) if not op_liability.empty and pd.notna(op_liability["active_wells"].iloc[-1]) else 0.0
        inactive_ratio = inactive / active if active else 0.0

        wells = wells_df[wells_df["licensee_operator_id"] == operator_id]
        op_restart = restart_df[restart_df["well_id"].isin(wells["well_id"])] if not restart_df.empty else pd.DataFrame()
        suspended = int(len(op_restart))
        candidates = op_restart[op_restart["restart_score"] >= 50.0] if not op_restart.empty else pd.DataFrame()
        restart_count = int(len(candidates))
        upside = float(candidates["avg_oil_bpd_last_3mo_before_shutin"].sum()) if not candidates.empty else 0.0

        distress = min(
            100.0,
            max(
                0.0,
                ((1 - (ratio if ratio is not None else 1.0)) * 45.0)
                + (min(inactive_ratio, 2.0) / 2.0) * 35.0
                + (max(-(yoy or 0.0), 0.0) / 100.0) * 20.0,
            ),
        )

        rows.append(
            {
                "as_of_date": as_of_date,
                "operator_id": operator_id,
                "avg_oil_bpd_30d": avg30,
                "avg_oil_bpd_365d": avg365,
                "total_oil_bbl_30d": total30,
                "total_oil_bbl_365d": total365,
                "yoy_change_pct": yoy,
                "decline_score": decline_score,
                "distress_score": distress,
                "suspended_wells_count": suspended,
                "restart_candidates_count": restart_count,
                "restart_upside_bpd_est": upside,
                "source_notes": {
                    "distress_formula": "(1-ratio)*45 + inactive_active_ratio*35 + negative_yoy*20",
                    "restart_threshold": 50,
                },
            }
        )

    return pd.DataFrame(rows)
