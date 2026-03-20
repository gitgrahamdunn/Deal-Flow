from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
from sqlalchemy import text

from deal_flow_ingest.apply_saved_sql import apply_saved_sql
from deal_flow_ingest.cli import get_package_candidates_frame, get_seller_theses_frame, run_ingestion
from deal_flow_ingest.config import get_database_url
from deal_flow_ingest.db.schema import get_engine


def load_operator_detail(operator_name: str) -> dict[str, pd.DataFrame]:
    engine = get_engine(get_database_url())
    with engine.connect() as conn:
        theses = pd.read_sql(
            text("select * from seller_theses where operator = :operator order by thesis_score desc"),
            conn,
            params={"operator": operator_name},
        )
        packages = pd.read_sql(
            text("select * from package_candidates where operator = :operator order by package_score desc"),
            conn,
            params={"operator": operator_name},
        )
        wells = pd.read_sql(
            text(
                "select * from restart_well_candidates "
                "where operator = :operator order by restart_score desc, avg_oil_bpd_last_3mo_before_shutin desc"
            ),
            conn,
            params={"operator": operator_name},
        )
    return {"theses": theses, "packages": packages, "wells": wells}


def run_refresh(start: str | None, end: str | None) -> int:
    status = run_ingestion(
        SimpleNamespace(
            start=start or None,
            end=end or None,
            refresh=False,
            dry_run=False,
            config="deal_flow_ingest/deal_flow_ingest/configs/sources.yaml",
        )
    )
    if status == 0:
        apply_saved_sql()
    return status


def main() -> None:
    import streamlit as st

    st.set_page_config(page_title="Deal Flow", layout="wide")
    st.title("Deal Flow")
    st.caption("Acquisition screening over the local Deal Flow warehouse")

    with st.sidebar:
        st.header("Refresh")
        start = st.text_input("Start date", value="")
        end = st.text_input("End date", value="")
        if st.button("Refresh Data", use_container_width=True):
            with st.spinner("Refreshing ingestion and rebuilding views..."):
                status = run_refresh(start.strip() or None, end.strip() or None)
            if status == 0:
                st.success("Refresh complete")
            else:
                st.error("Refresh failed; check terminal logs")

        st.header("Seller Filters")
        mode = st.selectbox("Mode", ["Top theses", "Low production", "Custom theses", "Packages"])
        min_score = float(st.number_input("Min score", value=0.0, step=5.0))
        limit = int(st.number_input("Limit", value=50, step=25))
        max_prod = st.text_input("Max avg oil bpd 30d", value="")
        max_prod_value = float(max_prod) if max_prod.strip() else None
        sort_by = st.selectbox(
            "Sort by",
            ["thesis_score", "seller_score", "opportunity_score", "avg_oil_bpd_30d"],
        )
        ascending = st.checkbox("Ascending", value=False)

    if mode == "Top theses":
        seller_args = SimpleNamespace(
            min_score=min_score,
            limit=limit,
            output="",
            sort_by="thesis_score",
            ascending=False,
            max_avg_oil_bpd_30d=max_prod_value,
        )
        seller_frame = get_seller_theses_frame(seller_args)
    elif mode == "Low production":
        seller_args = SimpleNamespace(
            min_score=min_score,
            limit=limit,
            output="",
            sort_by="avg_oil_bpd_30d",
            ascending=True,
            max_avg_oil_bpd_30d=max_prod_value,
        )
        seller_frame = get_seller_theses_frame(seller_args)
    elif mode == "Custom theses":
        seller_args = SimpleNamespace(
            min_score=min_score,
            limit=limit,
            output="",
            sort_by=sort_by,
            ascending=ascending,
            max_avg_oil_bpd_30d=max_prod_value,
        )
        seller_frame = get_seller_theses_frame(seller_args)
    else:
        seller_frame = pd.DataFrame()

    if mode == "Packages":
        package_args = SimpleNamespace(min_score=min_score, limit=limit, output="")
        package_frame = get_package_candidates_frame(package_args)
    else:
        package_frame = pd.DataFrame()

    tab1, tab2, tab3 = st.tabs(["Targets", "Packages", "Operator Detail"])

    with tab1:
        if seller_frame.empty:
            st.info("No seller theses available for the selected filters.")
        else:
            st.subheader("Seller Theses")
            st.dataframe(
                seller_frame[
                    [
                        "operator",
                        "avg_oil_bpd_30d",
                        "avg_oil_bpd_365d",
                        "thesis_priority",
                        "thesis_score",
                        "seller_score",
                        "opportunity_score",
                        "package_count",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )

    with tab2:
        if package_frame.empty:
            st.info("No package candidates available for the selected filters.")
        else:
            st.subheader("Package Candidates")
            st.dataframe(
                package_frame[
                    [
                        "operator",
                        "area_key",
                        "suspended_well_count",
                        "high_priority_well_count",
                        "linked_facility_count",
                        "estimated_restart_upside_bpd",
                        "package_score",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )

    with tab3:
        if seller_frame.empty and package_frame.empty:
            st.info("Load theses or packages first.")
        else:
            operator_options = sorted(
                set(seller_frame.get("operator", pd.Series(dtype=str)).dropna().tolist())
                | set(package_frame.get("operator", pd.Series(dtype=str)).dropna().tolist())
            )
            selected_operator = st.selectbox("Operator", operator_options)
            if selected_operator:
                detail = load_operator_detail(selected_operator)
                left, right = st.columns([1, 1])
                with left:
                    st.subheader("Operator Thesis")
                    st.dataframe(detail["theses"], use_container_width=True, hide_index=True)
                    st.subheader("Packages")
                    st.dataframe(detail["packages"], use_container_width=True, hide_index=True)
                with right:
                    st.subheader("Restart Wells")
                    st.dataframe(detail["wells"], use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
