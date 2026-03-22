from pathlib import Path

import pandas as pd
import shapefile

from deal_flow_ingest.config import SourcePayload
from deal_flow_ingest.sources.aer import (
    _discover_aer_artifact_url,
    load_general_well_data_frame,
    load_general_well_data,
    load_spatial_pipelines_frame,
    load_st102_facility_list_frame,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "aer"


def test_parse_general_well_data() -> None:
    source = pd.read_csv(FIXTURE_DIR / "general_well_data.csv")

    parsed = load_general_well_data_frame(source)

    assert parsed.iloc[0]["uwi"] == "100011100100W400"
    assert parsed.iloc[0]["license_number"] == 482521
    assert parsed.iloc[0]["well_name"] == "ALPHA 01-01-050-08W4"
    assert parsed.iloc[0]["field_name"] == "ALPHA FIELD"
    assert parsed.iloc[0]["pool_name"] == "ALPHA POOL"


def test_parse_st102_facility_list() -> None:
    source = pd.read_csv(FIXTURE_DIR / "st102_facility_list.csv")

    parsed = load_st102_facility_list_frame(source)

    assert parsed.iloc[0]["facility_id"] == "ABCPF001"
    assert parsed.iloc[0]["license_number"] == "FAC-1001"
    assert parsed.iloc[0]["facility_type"] == "BATTERY"
    assert parsed.iloc[0]["facility_subtype"] == "OIL BATTERY"
    assert parsed.iloc[0]["facility_operator"] == "Alpha Energy Ltd."


def test_parse_st102_facility_shapefile(tmp_path: Path) -> None:
    shp_path = tmp_path / "st102_facility_gcs.shp"
    with shapefile.Writer(str(shp_path)) as writer:
        writer.field("FAC_ID", "C")
        writer.field("FAC_NAME", "C")
        writer.field("LIC_NUMBER", "C")
        writer.field("FAC_SUB_TY", "C")
        writer.field("EDCT_DESCR", "C")
        writer.field("OPERATOR", "C")
        writer.field("FAC_STATUS", "C")
        writer.point(-110.97, 52.30)
        writer.record("ABBT0040017", "Morgan Provost 3-32", "6427", "Crude Oil Single-Well Battery", "Battery", "Tallgrass Energy Corp.", "Abandoned")

    parsed = load_st102_facility_list_frame(shp_path)

    assert parsed.iloc[0]["facility_id"] == "ABBT0040017"
    assert parsed.iloc[0]["license_number"] == "6427"
    assert parsed.iloc[0]["facility_name"] == "Morgan Provost 3-32"
    assert parsed.iloc[0]["facility_subtype"] == "Crude Oil Single-Well Battery"
    assert parsed.iloc[0]["facility_type"] == "Battery"
    assert parsed.iloc[0]["facility_operator"] == "Tallgrass Energy Corp."
    assert parsed.iloc[0]["facility_status"] == "Abandoned"
    assert parsed.iloc[0]["lat"] == 52.30
    assert parsed.iloc[0]["lon"] == -110.97


def test_parse_pipeline_shapefile(tmp_path: Path) -> None:
    shp_path = tmp_path / "pipelines_gcs.shp"
    with shapefile.Writer(str(shp_path), shapeType=shapefile.POLYLINE) as writer:
        writer.field("PLLICSEGID", "C")
        writer.field("LICENCE_NO", "C")
        writer.field("LINE_NO", "C")
        writer.field("LIC_LI_NO", "C")
        writer.field("COMP_NAME", "C")
        writer.field("BA_CODE", "C")
        writer.field("SEG_STATUS", "C")
        writer.field("FROM_FAC", "C")
        writer.field("FROM_LOC", "C")
        writer.field("TO_FAC", "C")
        writer.field("TO_LOC", "C")
        writer.field("SUBSTANCE1", "C")
        writer.field("SEG_LENGTH", "N", decimal=2)
        writer.field("GEOM_SRCE", "C")
        writer.line([[[-110.97, 52.30], [-110.95, 52.31], [-110.90, 52.33]]])
        writer.record("51822", "11130", "1", "11130-1", "ATCO Gas And Pipelines Ltd.", "0144", "Operating", "Regulator Station", "16-33-044-24W4", "Regulator Station", "04-06-045-24W4", "Natural gas", 10.27, "Mapping")

    parsed = load_spatial_pipelines_frame(shp_path)

    assert parsed.iloc[0]["pipeline_id"] == "51822"
    assert parsed.iloc[0]["license_number"] == "11130"
    assert parsed.iloc[0]["licence_line_number"] == "11130-1"
    assert parsed.iloc[0]["company_name"] == "ATCO Gas And Pipelines Ltd."
    assert parsed.iloc[0]["segment_status"] == "Operating"
    assert parsed.iloc[0]["substance1"] == "Natural gas"
    assert parsed.iloc[0]["geometry_wkt"].startswith("LINESTRING (")
    assert round(float(parsed.iloc[0]["centroid_lat"]), 3) == 52.315
    assert round(float(parsed.iloc[0]["centroid_lon"]), 3) == -110.935


def test_discover_aer_artifact_url_prefers_matching_tabular_links() -> None:
    html = """
    <a href="/files/general-well-data.pdf">General Well Data PDF</a>
    <a href="/files/general-well-data.xlsx">General Well Data XLSX</a>
    <a href="/files/st102-facility-list.csv">ST102 Facility List CSV</a>
    """

    assert (
        _discover_aer_artifact_url(
            html,
            "https://www.aer.ca/data-and-performance-reports/activity-and-data/lists-and-activities/general-well-data",
            ["general", "well", "data"],
        )
        == "https://www.aer.ca/files/general-well-data.xlsx"
    )


def test_discover_aer_artifact_url_rejects_wrong_general_well_workbook() -> None:
    html = """
    <a href="https://www1.aer.ca/ProductCatalogue/index.html">Products and Services Catalogue</a>
    <a href="/prd/documents/data/List-of-Wells-with-Greater-than-9-Drilling-Event-Sequences.xlsx">
      List of Wells with Greater than 9 Drilling Event Sequences
    </a>
    """

    assert (
        _discover_aer_artifact_url(
            html,
            "https://www.aer.ca/data-and-performance-reports/activity-and-data/lists-and-activities/general-well-data",
            ["general", "well", "data"],
        )
        is None
    )


class _NoopDownloader:
    def fetch(self, *_args, **_kwargs):  # pragma: no cover - should not be called in this test
        raise AssertionError("Downloader should not be called when general well data has no local override")


def test_general_well_data_requires_local_override_for_live_bulk() -> None:
    source = SourcePayload(
        key="aer_general_well_data",
        source_name="aer_general_well_data",
        data_kind="wells",
        enabled=True,
        local_sample=None,
        local_live_file="",
        parser_name="aer_general_well_data",
        landing_page_url="https://www.aer.ca/data-and-performance-reports/activity-and-data/lists-and-activities/general-well-data",
        dataset_url="",
        file_type="xlsx",
        refresh_frequency="daily",
    )

    parsed = load_general_well_data(downloader=_NoopDownloader(), source=source, refresh=False)

    assert parsed.empty
