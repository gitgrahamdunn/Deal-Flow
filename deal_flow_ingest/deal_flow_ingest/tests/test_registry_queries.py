import os
from argparse import Namespace
from pathlib import Path

from sqlalchemy import text

from deal_flow_ingest.apply_saved_sql import apply_saved_sql
from deal_flow_ingest.cli import run_ingestion
from deal_flow_ingest.config import get_default_config_path
from deal_flow_ingest.db.schema import get_engine
from deal_flow_ingest.services.registry_queries import (
    MAX_LIMIT_PER_LAYER,
    RegistryMapFilters,
    get_combined_registry_map_frame,
    get_registry_filter_options,
    get_registry_map_layers,
)


def _build_sample_db(tmp_path: Path) -> None:
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


def test_registry_map_layers_support_operator_and_candidate_filters(tmp_path: Path) -> None:
    _build_sample_db(tmp_path)

    alpha_layers = get_registry_map_layers(RegistryMapFilters(operator="ALPHA ENERGY LTD"))

    assert set(alpha_layers) == {"wells", "facilities", "pipelines"}
    assert not alpha_layers["wells"].empty
    assert not alpha_layers["facilities"].empty
    assert alpha_layers["wells"]["operator"].eq("ALPHA ENERGY LTD").all()
    assert alpha_layers["facilities"]["operator"].eq("ALPHA ENERGY LTD").all()
    assert alpha_layers["wells"]["location_method"].isin(["surveyed", "dls_approx"]).all()
    assert alpha_layers["wells"]["lat"].notna().all()
    assert alpha_layers["wells"]["lon"].notna().all()
    assert alpha_layers["pipelines"].empty

    candidate_pipelines = get_registry_map_layers(RegistryMapFilters(asset_types=("pipelines",), candidate_only=True))
    assert candidate_pipelines["pipelines"].empty


def test_registry_map_layers_support_bbox_and_combined_frame(tmp_path: Path) -> None:
    _build_sample_db(tmp_path)

    combined = get_combined_registry_map_frame(
        RegistryMapFilters(min_lat=52.09, max_lat=52.15, min_lon=-114.02, max_lon=-113.98)
    )

    assert not combined.empty
    assert set(combined["asset_type"]).issubset({"well", "facility", "pipeline"})
    assert combined["lat"].between(52.09, 52.15).all()
    assert combined["lon"].between(-114.02, -113.98).all()


def test_registry_wells_fall_back_to_dls_display_locations(tmp_path: Path) -> None:
    _build_sample_db(tmp_path)
    engine = get_engine(os.environ["DATABASE_URL"])
    with engine.begin() as conn:
        conn.execute(text("UPDATE dim_well SET lat = NULL, lon = NULL"))

    layers = get_registry_map_layers(RegistryMapFilters(asset_types=("wells",)))

    assert not layers["wells"].empty
    assert "location_method" in layers["wells"].columns
    assert layers["wells"]["location_method"].eq("dls_approx").any()
    approx_rows = layers["wells"].loc[layers["wells"]["location_method"] == "dls_approx"]
    assert approx_rows["lat"].between(49.0, 61.0).all()
    assert approx_rows["lon"].between(-121.0, -109.0).all()


def test_registry_filter_options_expose_frontend_choices(tmp_path: Path) -> None:
    _build_sample_db(tmp_path)

    options = get_registry_filter_options()

    assert "ALPHA ENERGY LTD" in options["operators"]
    assert "ACTIVE" in options["well_statuses"]
    assert "ACTIVE" in options["facility_statuses"]
    assert "Operating" in options["pipeline_statuses"]


def test_registry_pipeline_layer_exposes_line_geometry(tmp_path: Path) -> None:
    _build_sample_db(tmp_path)

    layers = get_registry_map_layers(RegistryMapFilters(asset_types=("pipelines",)))

    assert not layers["pipelines"].empty
    assert "geometry_wkt" in layers["pipelines"].columns
    assert layers["pipelines"]["geometry_wkt"].notna().all()
    assert layers["pipelines"]["geometry_wkt"].str.startswith("LINESTRING (").all()


def test_registry_filters_normalize_and_cap_payloads(tmp_path: Path) -> None:
    _build_sample_db(tmp_path)

    layers = get_registry_map_layers(
        RegistryMapFilters(
            asset_types=("wells", "wells", "facilities"),
            operator="  ALPHA ENERGY LTD  ",
            statuses=(" ACTIVE ", "", "ACTIVE"),
            limit_per_layer=MAX_LIMIT_PER_LAYER + 5000,
        )
    )

    assert set(layers) == {"wells", "facilities"}
    assert len(layers["wells"]) <= MAX_LIMIT_PER_LAYER
    assert layers["wells"]["operator"].eq("ALPHA ENERGY LTD").all()


def test_registry_filters_reject_invalid_ranges_and_asset_types(tmp_path: Path) -> None:
    _build_sample_db(tmp_path)

    try:
        get_registry_map_layers(RegistryMapFilters(asset_types=("wells", "leases")))
    except ValueError as exc:
        assert "Unknown asset_types" in str(exc)
    else:  # pragma: no cover - explicit failure path
        raise AssertionError("Expected invalid asset_types to raise ValueError")

    try:
        get_registry_map_layers(RegistryMapFilters(min_lat=53.0, max_lat=52.0))
    except ValueError as exc:
        assert "min_lat cannot be greater than max_lat" in str(exc)
    else:  # pragma: no cover - explicit failure path
        raise AssertionError("Expected invalid latitude bounds to raise ValueError")
