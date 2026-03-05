from __future__ import annotations

from calendar import monthrange
from datetime import date

import pandas as pd


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
        return pd.DataFrame()

    prod = well_prod_df.copy()
    if not prod.empty:
        prod["month"] = pd.to_datetime(prod["month"]).dt.to_period("M").dt.to_timestamp()

    rows: list[dict] = []
    for _, well in wells_df.iterrows():
        status = str(well.get("status") or "").upper().strip()
        if status not in suspended_statuses:
            continue
        well_id = well["well_id"]
        wp = prod[prod["well_id"] == well_id].sort_values("month") if not prod.empty else pd.DataFrame()
        if wp.empty:
            rows.append(
                {
                    "as_of_date": as_of_date,
                    "well_id": well_id,
                    "current_status": status,
                    "last_prod_month": None,
                    "avg_oil_bpd_last_3mo_before_shutin": 0.0,
                    "avg_oil_bpd_last_12mo_before_shutin": 0.0,
                    "shutin_recency_days": None,
                    "restart_score": 0.0,
                    "flags": {"reason": "no_production_history"},
                }
            )
            continue

        last_prod_month = wp["month"].max()
        last3 = wp[wp["month"] >= last_prod_month - pd.DateOffset(months=2)]
        last12 = wp[wp["month"] >= last_prod_month - pd.DateOffset(months=11)]
        avg3_monthly = float(last3["oil_bbl"].mean()) if not last3.empty else 0.0
        avg12_monthly = float(last12["oil_bbl"].mean()) if not last12.empty else 0.0
        avg3_bpd = avg3_monthly / 30.0
        avg12_bpd = avg12_monthly / 30.0
        shutin_recency_days = (pd.Timestamp(as_of_date) - (last_prod_month + pd.offsets.MonthEnd(0))).days
        score = compute_restart_score(avg3_bpd, avg12_bpd, shutin_recency_days)
        rows.append(
            {
                "as_of_date": as_of_date,
                "well_id": well_id,
                "current_status": status,
                "last_prod_month": last_prod_month.date(),
                "avg_oil_bpd_last_3mo_before_shutin": avg3_bpd,
                "avg_oil_bpd_last_12mo_before_shutin": avg12_bpd,
                "shutin_recency_days": shutin_recency_days,
                "restart_score": score,
                "flags": {"method": "screening_estimate" if bool(wp.get("is_estimated", False).any()) else "observed"},
            }
        )

    return pd.DataFrame(rows)


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
