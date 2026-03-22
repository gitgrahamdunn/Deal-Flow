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
    prod = operator_prod_df.copy()
    if not prod.empty and "month" in prod.columns:
        prod["month"] = pd.to_datetime(prod["month"]).dt.to_period("M").dt.to_timestamp()
        prod["operator_id"] = pd.to_numeric(prod["operator_id"], errors="coerce")
        prod["oil_bbl"] = pd.to_numeric(prod.get("oil_bbl", 0.0), errors="coerce").fillna(0.0)
        prod = prod.dropna(subset=["operator_id"]).copy()
        if not prod.empty:
            prod["operator_id"] = prod["operator_id"].astype(int)
    else:
        prod = pd.DataFrame(columns=["month", "operator_id", "oil_bbl"])
    as_of_ts = pd.Timestamp(as_of_date)
    current_month = as_of_ts.to_period("M").to_timestamp()

    operator_ids: set[int] = set()
    if not prod.empty:
        operator_ids.update(prod["operator_id"].tolist())
    if not wells_df.empty and "licensee_operator_id" in wells_df.columns:
        operator_ids.update(pd.to_numeric(wells_df["licensee_operator_id"], errors="coerce").dropna().astype(int).tolist())
    if not liability_df.empty and "operator_id" in liability_df.columns:
        operator_ids.update(pd.to_numeric(liability_df["operator_id"], errors="coerce").dropna().astype(int).tolist())

    if not operator_ids:
        return pd.DataFrame()

    metrics = pd.DataFrame({"operator_id": sorted(operator_ids)})

    if not prod.empty:
        latest_oil = (
            prod.loc[prod["month"] == current_month]
            .groupby("operator_id", as_index=False)["oil_bbl"]
            .sum()
            .rename(columns={"oil_bbl": "latest_oil_bbl"})
        )
        trailing12 = (
            prod.loc[prod["month"] > current_month - pd.DateOffset(months=12)]
            .groupby("operator_id", as_index=False)["oil_bbl"]
            .sum()
            .rename(columns={"oil_bbl": "total_oil_bbl_365d"})
        )
        prev12 = (
            prod.loc[
                (prod["month"] <= current_month - pd.DateOffset(months=12))
                & (prod["month"] > current_month - pd.DateOffset(months=24))
            ]
            .groupby("operator_id", as_index=False)["oil_bbl"]
            .sum()
            .rename(columns={"oil_bbl": "prev_total_oil_bbl_365d"})
        )
        metrics = metrics.merge(latest_oil, on="operator_id", how="left")
        metrics = metrics.merge(trailing12, on="operator_id", how="left")
        metrics = metrics.merge(prev12, on="operator_id", how="left")

    if not liability_df.empty and "operator_id" in liability_df.columns:
        liability = liability_df.copy()
        liability["operator_id"] = pd.to_numeric(liability["operator_id"], errors="coerce")
        liability = liability.dropna(subset=["operator_id"]).copy()
        if not liability.empty:
            liability["operator_id"] = liability["operator_id"].astype(int)
            if "as_of_date" in liability.columns:
                liability["as_of_date"] = pd.to_datetime(liability["as_of_date"], errors="coerce")
                liability = liability.sort_values(["operator_id", "as_of_date"]).drop_duplicates("operator_id", keep="last")
            else:
                liability = liability.drop_duplicates("operator_id", keep="last")
            metrics = metrics.merge(
                liability[[c for c in ["operator_id", "ratio", "inactive_wells", "active_wells"] if c in liability.columns]],
                on="operator_id",
                how="left",
            )

    if (
        not restart_df.empty
        and "well_id" in restart_df.columns
        and not wells_df.empty
        and "licensee_operator_id" in wells_df.columns
        and "well_id" in wells_df.columns
    ):
        well_ops = wells_df[["well_id", "licensee_operator_id"]].copy()
        well_ops["operator_id"] = pd.to_numeric(well_ops["licensee_operator_id"], errors="coerce")
        well_ops = well_ops.dropna(subset=["operator_id"]).copy()
        if not well_ops.empty:
            well_ops["operator_id"] = well_ops["operator_id"].astype(int)
            well_ops["well_id"] = well_ops["well_id"].astype(str)
            restart = restart_df.copy()
            restart["well_id"] = restart["well_id"].astype(str)
            restart["restart_score"] = pd.to_numeric(restart.get("restart_score", 0.0), errors="coerce").fillna(0.0)
            restart["avg_oil_bpd_last_3mo_before_shutin"] = pd.to_numeric(
                restart.get("avg_oil_bpd_last_3mo_before_shutin", 0.0), errors="coerce"
            ).fillna(0.0)
            restart = restart.merge(well_ops[["well_id", "operator_id"]], on="well_id", how="inner")
            if not restart.empty:
                suspended = restart.groupby("operator_id", as_index=False).size().rename(columns={"size": "suspended_wells_count"})
                candidates = restart[restart["restart_score"] >= 50.0]
                restart_counts = (
                    candidates.groupby("operator_id", as_index=False)
                    .size()
                    .rename(columns={"size": "restart_candidates_count"})
                )
                upside = (
                    candidates.groupby("operator_id", as_index=False)["avg_oil_bpd_last_3mo_before_shutin"]
                    .sum()
                    .rename(columns={"avg_oil_bpd_last_3mo_before_shutin": "restart_upside_bpd_est"})
                )
                metrics = metrics.merge(suspended, on="operator_id", how="left")
                metrics = metrics.merge(restart_counts, on="operator_id", how="left")
                metrics = metrics.merge(upside, on="operator_id", how="left")

    metrics["latest_oil_bbl"] = pd.to_numeric(
        metrics.get("latest_oil_bbl", pd.Series([0.0] * len(metrics))), errors="coerce"
    ).fillna(0.0)
    metrics["total_oil_bbl_365d"] = pd.to_numeric(
        metrics.get("total_oil_bbl_365d", pd.Series([0.0] * len(metrics))), errors="coerce"
    ).fillna(0.0)
    metrics["prev_total_oil_bbl_365d"] = pd.to_numeric(
        metrics.get("prev_total_oil_bbl_365d", pd.Series([0.0] * len(metrics))), errors="coerce"
    ).fillna(0.0)
    days_in_current_month = monthrange(current_month.year, current_month.month)[1]
    metrics["total_oil_bbl_30d"] = metrics["latest_oil_bbl"] * (30.0 / days_in_current_month)
    metrics["avg_oil_bpd_30d"] = metrics["total_oil_bbl_30d"] / 30.0
    metrics["avg_oil_bpd_365d"] = metrics["total_oil_bbl_365d"] / 365.0
    metrics["yoy_change_pct"] = np.where(
        metrics["prev_total_oil_bbl_365d"] > 0.0,
        ((metrics["total_oil_bbl_365d"] - metrics["prev_total_oil_bbl_365d"]) / metrics["prev_total_oil_bbl_365d"]) * 100.0,
        np.nan,
    )
    metrics["decline_score"] = (-pd.Series(metrics["yoy_change_pct"]).fillna(0.0)).clip(lower=0.0, upper=100.0)

    metrics["ratio"] = pd.to_numeric(metrics.get("ratio", pd.Series([np.nan] * len(metrics))), errors="coerce")
    metrics["inactive_wells"] = pd.to_numeric(
        metrics.get("inactive_wells", pd.Series([0.0] * len(metrics))), errors="coerce"
    ).fillna(0.0)
    metrics["active_wells"] = pd.to_numeric(
        metrics.get("active_wells", pd.Series([0.0] * len(metrics))), errors="coerce"
    ).fillna(0.0)
    inactive_ratio = np.where(metrics["active_wells"] > 0.0, metrics["inactive_wells"] / metrics["active_wells"], 0.0)
    metrics["distress_score"] = (
        ((1 - metrics["ratio"].fillna(1.0)) * 45.0)
        + (np.minimum(inactive_ratio, 2.0) / 2.0) * 35.0
        + (np.maximum(-pd.Series(metrics["yoy_change_pct"]).fillna(0.0), 0.0) / 100.0) * 20.0
    ).clip(lower=0.0, upper=100.0)

    metrics["suspended_wells_count"] = pd.to_numeric(
        metrics.get("suspended_wells_count", pd.Series([0] * len(metrics))), errors="coerce"
    ).fillna(0).astype(int)
    metrics["restart_candidates_count"] = pd.to_numeric(
        metrics.get("restart_candidates_count", pd.Series([0] * len(metrics))), errors="coerce"
    ).fillna(0).astype(int)
    metrics["restart_upside_bpd_est"] = pd.to_numeric(
        metrics.get("restart_upside_bpd_est", pd.Series([0.0] * len(metrics))), errors="coerce"
    ).fillna(0.0)
    metrics["as_of_date"] = as_of_date
    metrics["source_notes"] = [
        {
            "distress_formula": "(1-ratio)*45 + inactive_active_ratio*35 + negative_yoy*20",
            "restart_threshold": 50,
        }
        for _ in range(len(metrics))
    ]

    return metrics[
        [
            "as_of_date",
            "operator_id",
            "avg_oil_bpd_30d",
            "avg_oil_bpd_365d",
            "total_oil_bbl_30d",
            "total_oil_bbl_365d",
            "yoy_change_pct",
            "decline_score",
            "distress_score",
            "suspended_wells_count",
            "restart_candidates_count",
            "restart_upside_bpd_est",
            "source_notes",
        ]
    ]
