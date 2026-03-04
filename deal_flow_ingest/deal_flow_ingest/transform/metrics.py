from __future__ import annotations

from datetime import date, timedelta

import pandas as pd


def compute_operator_metrics(monthly_df: pd.DataFrame, as_of_date: date) -> pd.DataFrame:
    if monthly_df.empty:
        return pd.DataFrame(
            columns=[
                "as_of_date",
                "operator_id",
                "avg_oil_bpd_30d",
                "avg_oil_bpd_365d",
                "total_oil_bbl_30d",
                "total_oil_bbl_365d",
            ]
        )

    d30 = pd.Timestamp(as_of_date - timedelta(days=30))
    d365 = pd.Timestamp(as_of_date - timedelta(days=365))
    monthly_df = monthly_df.copy()
    monthly_df["month"] = pd.to_datetime(monthly_df["month"])

    in_30 = monthly_df[monthly_df["month"] >= d30]
    in_365 = monthly_df[monthly_df["month"] >= d365]

    total30 = in_30.groupby("operator_id", as_index=False)["oil_bbl"].sum().rename(columns={"oil_bbl": "total_oil_bbl_30d"})
    total365 = in_365.groupby("operator_id", as_index=False)["oil_bbl"].sum().rename(columns={"oil_bbl": "total_oil_bbl_365d"})

    merged = pd.merge(total365, total30, on="operator_id", how="outer").fillna(0.0)
    merged["avg_oil_bpd_30d"] = merged["total_oil_bbl_30d"] / 30.0
    merged["avg_oil_bpd_365d"] = merged["total_oil_bbl_365d"] / 365.0
    merged["as_of_date"] = pd.Timestamp(as_of_date)
    return merged[
        [
            "as_of_date",
            "operator_id",
            "avg_oil_bpd_30d",
            "avg_oil_bpd_365d",
            "total_oil_bbl_30d",
            "total_oil_bbl_365d",
        ]
    ]
