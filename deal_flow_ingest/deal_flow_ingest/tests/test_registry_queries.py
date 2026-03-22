import os
from argparse import Namespace
from pathlib import Path

from deal_flow_ingest.apply_saved_sql import apply_saved_sql
from deal_flow_ingest.cli import run_ingestion
from deal_flow_ingest.config import get_default_config_path
from deal_flow_ingest.services.registry_queries import (
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


def test_registry_filter_options_expose_frontend_choices(tmp_path: Path) -> None:
    _build_sample_db(tmp_path)

    options = get_registry_filter_options()

    assert "ALPHA ENERGY LTD" in options["operators"]
    assert "ACTIVE" in options["well_statuses"]
    assert "ACTIVE" in options["facility_statuses"]
    assert "Operating" in options["pipeline_statuses"]
