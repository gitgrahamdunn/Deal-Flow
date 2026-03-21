from __future__ import annotations

import math
from types import SimpleNamespace

import pandas as pd
from sqlalchemy.exc import OperationalError

from deal_flow_ingest.apply_saved_sql import apply_saved_sql
from deal_flow_ingest.cli import _read_curated_frame, get_package_candidates_frame, get_seller_theses_frame, run_ingestion
from deal_flow_ingest.config import get_default_config_path


def load_operator_detail(operator_name: str) -> dict[str, pd.DataFrame]:
    theses = _read_curated_frame(
        "select * from seller_theses where operator = :operator order by thesis_score desc",
        {"operator": operator_name},
    )
    packages = _read_curated_frame(
        "select * from package_candidates where operator = :operator order by package_score desc",
        {"operator": operator_name},
    )
    wells = _read_curated_frame(
        "select * from restart_well_candidates "
        "where operator = :operator order by restart_score desc, avg_oil_bpd_last_3mo_before_shutin desc",
        {"operator": operator_name},
    )
    return {"theses": theses, "packages": packages, "wells": wells}


def _format_scalar(value: object, *, suffix: str = "", decimals: int = 1) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if math.isnan(number):
        return "n/a"
    return f"{number:,.{decimals}f}{suffix}"


def _build_thesis_lines(thesis_row: pd.Series) -> list[str]:
    lines: list[str] = []
    operator = thesis_row.get("operator", "Unknown operator")
    priority = thesis_row.get("thesis_priority", "n/a")
    footprint = thesis_row.get("footprint_type", "n/a")
    core_area = thesis_row.get("core_area_key", "n/a")
    lines.append(f"{operator} is currently a `{priority}` acquisition screen with a `{footprint}` footprint centered on `{core_area}`.")

    avg_oil_30d = _format_scalar(thesis_row.get("avg_oil_bpd_30d"), suffix=" bbl/d")
    avg_oil_365d = _format_scalar(thesis_row.get("avg_oil_bpd_365d"), suffix=" bbl/d")
    lines.append(f"Recent oil production is {avg_oil_30d} over the last 30 days versus {avg_oil_365d} over the last year.")

    package_count = int(float(thesis_row.get("package_count", 0) or 0))
    top_package_score = _format_scalar(thesis_row.get("top_package_score"), decimals=1)
    packaged_upside = _format_scalar(thesis_row.get("packaged_restart_upside_bpd"), suffix=" bbl/d")
    lines.append(
        f"The current screen sees {package_count} package candidates, with a top package score of {top_package_score} and packaged restart upside of {packaged_upside}."
    )

    seller_score = _format_scalar(thesis_row.get("seller_score"), decimals=1)
    opportunity_score = _format_scalar(thesis_row.get("opportunity_score"), decimals=1)
    thesis_score = _format_scalar(thesis_row.get("thesis_score"), decimals=1)
    lines.append(
        f"Composite scoring is seller={seller_score}, opportunity={opportunity_score}, thesis={thesis_score}."
    )
    return lines


def render_thesis_detail(st, seller_frame: pd.DataFrame) -> None:
    if seller_frame.empty:
        st.info("No thesis rows available for the current filters.")
        return

    operator_options = seller_frame["operator"].dropna().astype(str).tolist()
    selected_operator = st.selectbox("View thesis for operator", operator_options, key="thesis_operator")
    selected = seller_frame[seller_frame["operator"] == selected_operator]
    if selected.empty:
        st.info("No thesis rows available for that operator.")
        return

    thesis_row = selected.sort_values(["thesis_score", "seller_score"], ascending=False).iloc[0]

    metric_cols = st.columns(4)
    metric_cols[0].metric("Thesis Score", _format_scalar(thesis_row.get("thesis_score"), decimals=1))
    metric_cols[1].metric("Seller Score", _format_scalar(thesis_row.get("seller_score"), decimals=1))
    metric_cols[2].metric("Opportunity Score", _format_scalar(thesis_row.get("opportunity_score"), decimals=1))
    metric_cols[3].metric("Priority", str(thesis_row.get("thesis_priority", "n/a")))

    metric_cols = st.columns(4)
    metric_cols[0].metric("Avg Oil 30d", _format_scalar(thesis_row.get("avg_oil_bpd_30d"), suffix=" bbl/d"))
    metric_cols[1].metric("Avg Oil 365d", _format_scalar(thesis_row.get("avg_oil_bpd_365d"), suffix=" bbl/d"))
    metric_cols[2].metric("Packages", str(int(float(thesis_row.get("package_count", 0) or 0))))
    metric_cols[3].metric("Core Area", str(thesis_row.get("core_area_key", "n/a")))

    st.subheader("Thesis")
    for line in _build_thesis_lines(thesis_row):
        st.write(line)

    with st.expander("Underlying thesis row", expanded=False):
        st.dataframe(selected, use_container_width=True, hide_index=True)


def run_refresh(start: str | None, end: str | None) -> int:
    status = run_ingestion(
        SimpleNamespace(
            start=start or None,
            end=end or None,
            refresh=False,
            dry_run=False,
            config=get_default_config_path(),
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

    view_error: str | None = None

    with st.sidebar:
        st.header("Refresh")
        st.caption("Use Rebuild Curated Views for a quick refresh of the SQL layer. Use Full Data Refresh only when you want to download and ingest fresh source data.")
        start = st.text_input("Start date", value="")
        end = st.text_input("End date", value="")

        if st.button("Load Existing Data / Rebuild Views", use_container_width=True):
            with st.spinner("Rebuilding curated SQL views..."):
                try:
                    apply_saved_sql()
                except Exception as exc:  # pragma: no cover - defensive UI path
                    st.error(f"Could not rebuild curated views: {exc}")
                else:
                    st.success("Curated views rebuilt from the existing database")

        with st.expander("Full Data Refresh", expanded=False):
            st.warning("This runs a full ingestion pull, rebuilds the warehouse, and reapplies curated views.")
            st.caption("Expected runtime can range from roughly 5 to 20 minutes depending on source availability and cache state.")
            confirm_refresh = st.checkbox(
                "I understand this will download source data and may take a while",
                value=False,
                key="confirm_full_refresh",
            )
            confirm_phrase = st.text_input(
                "Type REFRESH to enable the run",
                value="",
                key="confirm_refresh_phrase",
            )
            if st.button("Run Full Data Refresh", use_container_width=True):
                if not confirm_refresh or confirm_phrase.strip().upper() != "REFRESH":
                    st.warning("To run a full ingestion, check the confirmation box and type REFRESH.")
                else:
                    with st.spinner("Refreshing ingestion and rebuilding views..."):
                        status = run_refresh(start.strip() or None, end.strip() or None)
                    if status == 0:
                        st.success("Full refresh complete")
                    else:
                        st.error("Full refresh failed; check terminal logs")

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

    try:
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
    except OperationalError as exc:
        seller_frame = pd.DataFrame()
        package_frame = pd.DataFrame()
        view_error = str(exc)
    except Exception as exc:  # pragma: no cover - defensive UI path
        seller_frame = pd.DataFrame()
        package_frame = pd.DataFrame()
        view_error = str(exc)

    if view_error:
        st.error(
            "The curated views are not available yet. Use 'Rebuild Curated Views' in the sidebar or run "
            "`dealflow build-sql` after ingestion."
        )
        st.caption(view_error)

    tab1, tab2, tab3 = st.tabs(["Targets", "Packages", "Operator Detail"])

    with tab1:
        if seller_frame.empty:
            st.info("No seller theses available. If this is a fresh setup, use Load Existing Data / Rebuild Views first, then run Full Data Refresh only if the database is empty.")
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
            render_thesis_detail(st, seller_frame)

    with tab2:
        if package_frame.empty:
            st.info("No package candidates available. If this is a fresh setup, use Load Existing Data / Rebuild Views first, then run Full Data Refresh only if the database is empty.")
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
            st.info("No operator detail is available yet. Use Load Existing Data / Rebuild Views first, then run Full Data Refresh only if the database is empty.")
        else:
            operator_options = sorted(
                set(seller_frame.get("operator", pd.Series(dtype=str)).dropna().tolist())
                | set(package_frame.get("operator", pd.Series(dtype=str)).dropna().tolist())
            )
            selected_operator = st.selectbox("Operator", operator_options)
            if selected_operator:
                try:
                    detail = load_operator_detail(selected_operator)
                except Exception as exc:  # pragma: no cover - defensive UI path
                    st.error(f"Could not load operator detail: {exc}")
                else:
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
