import os
import sqlite3
from argparse import Namespace
from pathlib import Path

from deal_flow_ingest.apply_saved_sql import apply_saved_sql
from deal_flow_ingest.cli import run_ingestion
from deal_flow_ingest.config import get_default_config_path


def _fetch_one(db_path: Path, query: str):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(query).fetchone()
    finally:
        conn.close()


def test_apply_saved_sql_builds_curated_views(tmp_path: Path):
    db_path = tmp_path / "deal_flow.db"
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"

    args = Namespace(
        start=None,
        end="2025-03-31",
        refresh=False,
        dry_run=True,
        config=get_default_config_path(),
    )

    assert run_ingestion(args) == 0
    assert apply_saved_sql() == 0

    targets = _fetch_one(
        db_path,
        "select operator, seller_score, liability_ratio from deal_flow_targets order by seller_score desc limit 1",
    )
    assert targets is not None
    assert targets[0] == "BETA PETROLEUM INC"
    assert float(targets[1]) > 0
    assert targets[2] is None or float(targets[2]) < 1

    restart = _fetch_one(
        db_path,
        "select operator, restart_tier, area_key from restart_well_candidates order by restart_score desc limit 1",
    )
    assert restart is not None
    assert restart[0] == "ALPHA ENERGY LTD"
    assert restart[1] in {"A", "B", "C"}
    assert restart[2]

    clusters = _fetch_one(
        db_path,
        "select operator, area_key, suspended_well_count, cluster_score from asset_clusters order by cluster_score desc limit 1",
    )
    assert clusters is not None
    assert clusters[0] == "ALPHA ENERGY LTD"
    assert clusters[1]
    assert int(clusters[2]) >= 1
    assert float(clusters[3]) > 0

    packages = _fetch_one(
        db_path,
        "select operator, linked_facility_count, battery_count, facility_linked_well_count, package_score "
        "from package_candidates order by package_score desc limit 1",
    )
    assert packages is not None
    assert packages[0] == "ALPHA ENERGY LTD"
    assert int(packages[1]) >= 1
    assert int(packages[2]) >= 1
    assert int(packages[3]) >= 1
    assert float(packages[4]) > 0

    footprints = _fetch_one(
        db_path,
        "select operator, package_count, core_area_key, footprint_type, footprint_score "
        "from operator_area_footprints order by footprint_score desc limit 1",
    )
    assert footprints is not None
    assert footprints[0] == "ALPHA ENERGY LTD"
    assert int(footprints[1]) >= 1
    assert footprints[2]
    assert footprints[3] in {"single_area", "core_area", "scattered"}
    assert float(footprints[4]) > 0

    opportunities = _fetch_one(
        db_path,
        "select operator, opportunity_score from deal_flow_opportunities order by opportunity_score desc limit 1",
    )
    assert opportunities is not None
    assert opportunities[0] in {"ALPHA ENERGY LTD", "BETA PETROLEUM INC"}
    assert float(opportunities[1]) > 0

    theses = _fetch_one(
        db_path,
        "select operator, thesis_priority, thesis_score from seller_theses order by thesis_score desc limit 1",
    )
    assert theses is not None
    assert theses[0]
    assert theses[1] in {"HIGH", "MEDIUM", "LOW"}
    assert float(theses[2]) >= 0

    registry_well = _fetch_one(
        db_path,
        "select well_id, license_number, well_name, field_name from asset_registry_wells order by well_id limit 1",
    )
    assert registry_well is not None
    assert registry_well[0]
    assert registry_well[1]
    assert registry_well[2]
    assert registry_well[3]

    registry_facility = _fetch_one(
        db_path,
        "select facility_id, facility_name, license_number, facility_subtype from asset_registry_facilities order by facility_id limit 1",
    )
    assert registry_facility is not None
    assert registry_facility[0]
    assert registry_facility[1]
    assert registry_facility[2]
    assert registry_facility[3]

    registry_pipeline = _fetch_one(
        db_path,
        "select pipeline_id, license_number, company_name, segment_status from asset_registry_pipelines order by pipeline_id limit 1",
    )
    assert registry_pipeline is not None
    assert registry_pipeline[0]
    assert registry_pipeline[1]
    assert registry_pipeline[2]
    assert registry_pipeline[3]
