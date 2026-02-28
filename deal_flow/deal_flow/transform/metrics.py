from __future__ import annotations

from datetime import date

import pandas as pd


def compute_operator_metrics(prod: pd.DataFrame, end_date: date) -> pd.DataFrame:
    end_ts = pd.Timestamp(end_date)
    start_30 = end_ts - pd.Timedelta(days=30)
    start_365 = end_ts - pd.Timedelta(days=365)

    last_30 = prod[(prod["month"] >= start_30) & (prod["month"] <= end_ts)]
    last_365 = prod[(prod["month"] >= start_365) & (prod["month"] <= end_ts)]

    m30 = last_30.groupby("operator", as_index=False).agg(total_oil_bbl_30d=("oil_bbl", "sum"))
    m365 = last_365.groupby("operator", as_index=False).agg(total_oil_bbl_365d=("oil_bbl", "sum"))

    out = m365.merge(m30, on="operator", how="outer").fillna(0.0)
    out["avg_oil_bpd_30d"] = out["total_oil_bbl_30d"] / 30.0
    out["avg_oil_bpd_365d"] = out["total_oil_bbl_365d"] / 365.0
    active = last_30[last_30["oil_bbl"] > 0].groupby("operator").size().rename("count_active_records").reset_index()
    out = out.merge(active, on="operator", how="left").fillna({"count_active_records": 0})
    out = out.sort_values("avg_oil_bpd_30d", ascending=False)
    return out
