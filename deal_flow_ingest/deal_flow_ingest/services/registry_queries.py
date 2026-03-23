from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sqlalchemy import text

from deal_flow_ingest.apply_saved_sql import apply_saved_sql
from deal_flow_ingest.config import get_database_url
from deal_flow_ingest.db.schema import get_engine


ALLOWED_ASSET_TYPES = {"wells", "facilities", "pipelines"}
DEFAULT_LIMIT_PER_LAYER = 50000
MAX_LIMIT_PER_LAYER = 100000
APPROX_LAT_DEGREES_PER_MILE = 1.0 / 69.172
APPROX_LON_DEGREES_PER_MILE = 1.0 / 44.5


@dataclass(slots=True)
class RegistryMapFilters:
    asset_types: tuple[str, ...] = ("wells", "facilities", "pipelines")
    operator: str | None = None
    statuses: tuple[str, ...] = ()
    candidate_only: bool = False
    zoom: float | None = None
    min_lat: float | None = None
    max_lat: float | None = None
    min_lon: float | None = None
    max_lon: float | None = None
    limit_per_layer: int = DEFAULT_LIMIT_PER_LAYER


def _normalize_filters(filters: RegistryMapFilters) -> RegistryMapFilters:
    asset_types = tuple(dict.fromkeys(asset_type.strip().lower() for asset_type in filters.asset_types if str(asset_type).strip()))
    if not asset_types:
        asset_types = tuple(sorted(ALLOWED_ASSET_TYPES))
    unknown_asset_types = sorted(set(asset_types) - ALLOWED_ASSET_TYPES)
    if unknown_asset_types:
        raise ValueError(f"Unknown asset_types: {', '.join(unknown_asset_types)}")

    statuses = tuple(dict.fromkeys(status.strip() for status in filters.statuses if str(status).strip()))
    operator = filters.operator.strip() if isinstance(filters.operator, str) and filters.operator.strip() else None

    limit_per_layer = int(filters.limit_per_layer)
    if limit_per_layer <= 0:
        raise ValueError("limit_per_layer must be positive")
    limit_per_layer = min(limit_per_layer, MAX_LIMIT_PER_LAYER)

    if filters.min_lat is not None and filters.max_lat is not None and filters.min_lat > filters.max_lat:
        raise ValueError("min_lat cannot be greater than max_lat")
    if filters.min_lon is not None and filters.max_lon is not None and filters.min_lon > filters.max_lon:
        raise ValueError("min_lon cannot be greater than max_lon")

    return RegistryMapFilters(
        asset_types=asset_types,
        operator=operator,
        statuses=statuses,
        candidate_only=bool(filters.candidate_only),
        zoom=float(filters.zoom) if filters.zoom is not None else None,
        min_lat=filters.min_lat,
        max_lat=filters.max_lat,
        min_lon=filters.min_lon,
        max_lon=filters.max_lon,
        limit_per_layer=limit_per_layer,
    )


def _read_frame(sql: str, params: dict[str, object] | None = None) -> pd.DataFrame:
    engine = get_engine(get_database_url())
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(sql), conn, params=params)
    except Exception as exc:
        message = str(exc).lower()
        if "no such table:" not in message and "no such view:" not in message:
            raise

    apply_saved_sql()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def _append_common_filters(
    clauses: list[str],
    params: dict[str, object],
    *,
    alias: str,
    status_column: str,
    lat_column: str,
    lon_column: str,
    filters: RegistryMapFilters,
) -> None:
    if filters.operator:
        clauses.append(f"{alias}.operator = :operator")
        params["operator"] = filters.operator
    if filters.statuses:
        placeholders = []
        for idx, status in enumerate(filters.statuses):
            key = f"status_{idx}"
            placeholders.append(f":{key}")
            params[key] = status
        clauses.append(f"{alias}.{status_column} IN ({', '.join(placeholders)})")
    if filters.min_lat is not None:
        clauses.append(f"{alias}.{lat_column} >= :min_lat")
        params["min_lat"] = filters.min_lat
    if filters.max_lat is not None:
        clauses.append(f"{alias}.{lat_column} <= :max_lat")
        params["max_lat"] = filters.max_lat
    if filters.min_lon is not None:
        clauses.append(f"{alias}.{lon_column} >= :min_lon")
        params["min_lon"] = filters.min_lon
    if filters.max_lon is not None:
        clauses.append(f"{alias}.{lon_column} <= :max_lon")
        params["max_lon"] = filters.max_lon


def _get_wells_layer(filters: RegistryMapFilters) -> pd.DataFrame:
    clauses = ["w.display_lat IS NOT NULL", "w.display_lon IS NOT NULL"]
    params: dict[str, object] = {"limit": filters.limit_per_layer}
    if filters.operator:
        clauses.append("w.operator = :operator")
        params["operator"] = filters.operator
    if filters.statuses:
        placeholders = []
        for idx, status in enumerate(filters.statuses):
            key = f"status_{idx}"
            placeholders.append(f":{key}")
            params[key] = status
        clauses.append(f"w.status IN ({', '.join(placeholders)})")
    if filters.min_lat is not None:
        clauses.append("w.display_lat >= :min_lat")
        params["min_lat"] = filters.min_lat
    if filters.max_lat is not None:
        clauses.append("w.display_lat <= :max_lat")
        params["max_lat"] = filters.max_lat
    if filters.min_lon is not None:
        clauses.append("w.display_lon >= :min_lon")
        params["min_lon"] = filters.min_lon
    if filters.max_lon is not None:
        clauses.append("w.display_lon <= :max_lon")
        params["max_lon"] = filters.max_lon
    if filters.candidate_only:
        clauses.append("(seller.operator IS NOT NULL OR restart.well_id IS NOT NULL)")

    sql = f"""
    WITH seller AS (
        SELECT DISTINCT operator
        FROM seller_theses
    ),
    restart AS (
        SELECT DISTINCT well_id
        FROM restart_well_candidates
    ),
    wells_with_display AS (
        SELECT
            base.*,
            COALESCE(base.lat, base.approx_lat) AS display_lat,
            COALESCE(base.lon, base.approx_lon) AS display_lon,
            CASE
                WHEN base.lat IS NOT NULL AND base.lon IS NOT NULL THEN 'surveyed'
                WHEN base.approx_lat IS NOT NULL AND base.approx_lon IS NOT NULL THEN 'dls_approx'
                ELSE NULL
            END AS location_method
        FROM (
            SELECT
                w.*,
                CASE
                    WHEN w.section IS NULL OR w.township IS NULL OR w.range IS NULL OR w.meridian IS NULL THEN NULL
                    ELSE
                        49.0
                        + (
                            ((w.township - 1) * 6.0)
                            + CAST(((w.section - 1) / 6) AS INTEGER)
                            + CASE
                                WHEN CAST(w.lsd AS INTEGER) BETWEEN 1 AND 16
                                THEN (CAST(((CAST(w.lsd AS INTEGER) - 1) / 4) AS INTEGER) * 0.25) + 0.125
                                ELSE 0.5
                              END
                        ) * {APPROX_LAT_DEGREES_PER_MILE}
                END AS approx_lat,
                CASE
                    WHEN w.section IS NULL OR w.township IS NULL OR w.range IS NULL OR w.meridian IS NULL THEN NULL
                    ELSE
                        (
                            CASE
                                WHEN w.meridian = 1 THEN -97.4578917
                                WHEN w.meridian = 2 THEN -102.0
                                WHEN w.meridian = 3 THEN -106.0
                                WHEN w.meridian = 4 THEN -110.0
                                WHEN w.meridian = 5 THEN -114.0
                                WHEN w.meridian = 6 THEN -118.0
                                WHEN w.meridian = 7 THEN -122.0
                                ELSE NULL
                            END
                        )
                        - (
                            ((w.range - 1) * 6.0)
                            + CASE
                                WHEN MOD(CAST(((w.section - 1) / 6) AS INTEGER), 2) = 0
                                THEN (w.section - (CAST(((w.section - 1) / 6) AS INTEGER) * 6) - 1)
                                ELSE (6 - (w.section - (CAST(((w.section - 1) / 6) AS INTEGER) * 6)))
                              END
                            + CASE
                                WHEN CAST(w.lsd AS INTEGER) BETWEEN 1 AND 16
                                THEN (
                                    CASE
                                        WHEN MOD(CAST(((CAST(w.lsd AS INTEGER) - 1) / 4) AS INTEGER), 2) = 0
                                        THEN (CAST(w.lsd AS INTEGER) - (CAST(((CAST(w.lsd AS INTEGER) - 1) / 4) AS INTEGER) * 4) - 1) * 0.25 + 0.125
                                        ELSE (4 - (CAST(w.lsd AS INTEGER) - (CAST(((CAST(w.lsd AS INTEGER) - 1) / 4) AS INTEGER) * 4))) * 0.25 + 0.125
                                    END
                                )
                                ELSE 0.5
                              END
                        ) * {APPROX_LON_DEGREES_PER_MILE}
                END AS approx_lon
            FROM asset_registry_wells w
        ) base
    )
    SELECT
        'well' AS asset_type,
        w.well_id AS asset_id,
        w.well_name AS asset_name,
        w.license_number,
        w.operator,
        w.status,
        w.display_lat AS lat,
        w.display_lon AS lon,
        w.field_name,
        w.pool_name,
        w.restart_score,
        w.location_method,
        CASE WHEN seller.operator IS NOT NULL THEN 1 ELSE 0 END AS candidate_operator,
        CASE WHEN restart.well_id IS NOT NULL THEN 1 ELSE 0 END AS candidate_restart,
        CASE WHEN seller.operator IS NOT NULL OR restart.well_id IS NOT NULL THEN 1 ELSE 0 END AS candidate_any
    FROM wells_with_display w
    LEFT JOIN seller
        ON seller.operator = w.operator
    LEFT JOIN restart
        ON restart.well_id = w.well_id
    WHERE {' AND '.join(clauses)}
    ORDER BY candidate_any DESC, COALESCE(w.restart_score, 0) DESC, w.well_id
    LIMIT :limit
    """
    return _read_frame(sql, params)


def _get_facilities_layer(filters: RegistryMapFilters) -> pd.DataFrame:
    clauses = ["f.lat IS NOT NULL", "f.lon IS NOT NULL"]
    params: dict[str, object] = {"limit": filters.limit_per_layer}
    _append_common_filters(
        clauses,
        params,
        alias="f",
        status_column="facility_status",
        lat_column="lat",
        lon_column="lon",
        filters=filters,
    )
    if filters.candidate_only:
        clauses.append("(seller.operator IS NOT NULL OR restart_facility.facility_id IS NOT NULL)")

    sql = f"""
    WITH seller AS (
        SELECT DISTINCT operator
        FROM seller_theses
    ),
    restart_facility AS (
        SELECT DISTINCT bwf.facility_id
        FROM bridge_well_facility bwf
        JOIN restart_well_candidates rwc
            ON rwc.well_id = bwf.well_id
    )
    SELECT
        'facility' AS asset_type,
        f.facility_id AS asset_id,
        f.facility_name AS asset_name,
        f.license_number,
        f.operator,
        f.facility_status AS status,
        f.lat,
        f.lon,
        f.facility_type,
        f.facility_subtype,
        f.linked_well_count,
        CASE WHEN seller.operator IS NOT NULL THEN 1 ELSE 0 END AS candidate_operator,
        CASE WHEN restart_facility.facility_id IS NOT NULL THEN 1 ELSE 0 END AS candidate_restart,
        CASE WHEN seller.operator IS NOT NULL OR restart_facility.facility_id IS NOT NULL THEN 1 ELSE 0 END AS candidate_any
    FROM asset_registry_facilities f
    LEFT JOIN seller
        ON seller.operator = f.operator
    LEFT JOIN restart_facility
        ON restart_facility.facility_id = f.facility_id
    WHERE {' AND '.join(clauses)}
    ORDER BY candidate_any DESC, COALESCE(f.linked_well_count, 0) DESC, f.facility_id
    LIMIT :limit
    """
    return _read_frame(sql, params)


def _get_pipelines_layer(filters: RegistryMapFilters) -> pd.DataFrame:
    clauses = ["p.centroid_lat IS NOT NULL", "p.centroid_lon IS NOT NULL"]
    params: dict[str, object] = {"limit": filters.limit_per_layer}
    _append_common_filters(
        clauses,
        params,
        alias="p",
        status_column="segment_status",
        lat_column="centroid_lat",
        lon_column="centroid_lon",
        filters=filters,
    )
    if filters.candidate_only:
        clauses.append("seller.operator IS NOT NULL")

    geometry_expr = "p.geometry_wkt"
    dialect_name = get_engine(get_database_url()).dialect.name
    if dialect_name == "postgresql" and filters.zoom is not None and filters.zoom < 10:
        tolerance = 0.004 if filters.zoom < 6 else 0.0015 if filters.zoom < 8 else 0.0005
        geometry_expr = (
            "CASE "
            "WHEN p.geometry_wkt IS NULL OR TRIM(p.geometry_wkt) = '' THEN p.geometry_wkt "
            f"ELSE ST_AsText(ST_SimplifyPreserveTopology(ST_GeomFromText(p.geometry_wkt, 4326), {tolerance})) "
            "END"
        )

    sql = f"""
    WITH seller AS (
        SELECT DISTINCT operator
        FROM seller_theses
    )
    SELECT
        'pipeline' AS asset_type,
        p.pipeline_id AS asset_id,
        p.licence_line_number AS asset_name,
        p.license_number,
        p.operator,
        p.segment_status AS status,
        p.centroid_lat AS lat,
        p.centroid_lon AS lon,
        p.company_name,
        p.substance1,
        p.segment_length_km,
        {geometry_expr} AS geometry_wkt,
        CASE WHEN seller.operator IS NOT NULL THEN 1 ELSE 0 END AS candidate_operator,
        0 AS candidate_restart,
        CASE WHEN seller.operator IS NOT NULL THEN 1 ELSE 0 END AS candidate_any
    FROM asset_registry_pipelines p
    LEFT JOIN seller
        ON seller.operator = p.operator
    WHERE {' AND '.join(clauses)}
    ORDER BY candidate_any DESC, COALESCE(p.segment_length_km, 0) DESC, p.pipeline_id
    LIMIT :limit
    """
    return _read_frame(sql, params)


def get_registry_map_layers(filters: RegistryMapFilters | None = None) -> dict[str, pd.DataFrame]:
    filters = _normalize_filters(filters or RegistryMapFilters())
    requested = set(filters.asset_types)
    layers: dict[str, pd.DataFrame] = {}
    if "wells" in requested:
        layers["wells"] = _get_wells_layer(filters)
    if "facilities" in requested:
        layers["facilities"] = _get_facilities_layer(filters)
    if "pipelines" in requested:
        layers["pipelines"] = _get_pipelines_layer(filters)
    return layers


def get_combined_registry_map_frame(filters: RegistryMapFilters | None = None) -> pd.DataFrame:
    layers = get_registry_map_layers(filters)
    frames = [frame for frame in layers.values() if not frame.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def get_registry_filter_options() -> dict[str, list[str]]:
    operators = _read_frame(
        """
        SELECT operator
        FROM (
            SELECT DISTINCT operator FROM asset_registry_wells WHERE operator IS NOT NULL AND TRIM(operator) <> ''
            UNION
            SELECT DISTINCT operator FROM asset_registry_facilities WHERE operator IS NOT NULL AND TRIM(operator) <> ''
            UNION
            SELECT DISTINCT operator FROM asset_registry_pipelines WHERE operator IS NOT NULL AND TRIM(operator) <> ''
        )
        ORDER BY operator
        """
    )
    well_statuses = _read_frame(
        "SELECT DISTINCT status FROM asset_registry_wells WHERE status IS NOT NULL AND TRIM(status) <> '' ORDER BY status"
    )
    facility_statuses = _read_frame(
        "SELECT DISTINCT facility_status AS status FROM asset_registry_facilities "
        "WHERE facility_status IS NOT NULL AND TRIM(facility_status) <> '' ORDER BY facility_status"
    )
    pipeline_statuses = _read_frame(
        "SELECT DISTINCT segment_status AS status FROM asset_registry_pipelines "
        "WHERE segment_status IS NOT NULL AND TRIM(segment_status) <> '' ORDER BY segment_status"
    )
    return {
        "operators": operators["operator"].dropna().astype(str).tolist() if "operator" in operators.columns else [],
        "well_statuses": well_statuses["status"].dropna().astype(str).tolist() if "status" in well_statuses.columns else [],
        "facility_statuses": facility_statuses["status"].dropna().astype(str).tolist()
        if "status" in facility_statuses.columns
        else [],
        "pipeline_statuses": pipeline_statuses["status"].dropna().astype(str).tolist()
        if "status" in pipeline_statuses.columns
        else [],
    }


def get_seller_candidates(limit: int = 200, min_score: float = 0.0) -> pd.DataFrame:
    sql = """
    SELECT *
    FROM seller_theses
    WHERE COALESCE(thesis_score, 0) >= :min_score
    ORDER BY thesis_score DESC, seller_score DESC, operator
    LIMIT :limit
    """
    return _read_frame(sql, {"limit": int(limit), "min_score": float(min_score)})


def get_package_candidates(limit: int = 200, min_score: float = 0.0) -> pd.DataFrame:
    sql = """
    SELECT *
    FROM package_candidates
    WHERE COALESCE(package_score, 0) >= :min_score
    ORDER BY package_score DESC, estimated_restart_upside_bpd DESC, operator, area_key
    LIMIT :limit
    """
    return _read_frame(sql, {"limit": int(limit), "min_score": float(min_score)})


def get_registry_summary() -> dict[str, int]:
    summary_sql = """
    SELECT
        (SELECT COUNT(*) FROM dim_well) AS wells,
        (SELECT COUNT(*) FROM dim_facility) AS facilities,
        (SELECT COUNT(*) FROM dim_pipeline) AS pipelines,
        (SELECT COUNT(*) FROM seller_theses) AS seller_candidates,
        (SELECT COUNT(*) FROM package_candidates) AS package_candidates
    """
    frame = _read_frame(summary_sql)
    if frame.empty:
        return {"wells": 0, "facilities": 0, "pipelines": 0, "seller_candidates": 0, "package_candidates": 0}
    row = frame.iloc[0].to_dict()
    return {key: int(value or 0) for key, value in row.items()}


def get_asset_detail(asset_type: str, asset_id: str) -> dict[str, object] | None:
    normalized = asset_type.strip().lower()
    if normalized == "well":
        detail = _read_frame("SELECT * FROM asset_registry_wells WHERE well_id = :asset_id", {"asset_id": asset_id})
        if detail.empty:
            return None
        links = _read_frame(
            """
            SELECT
                f.facility_id,
                f.facility_name,
                f.operator,
                f.facility_type,
                f.facility_status
            FROM bridge_well_facility bwf
            JOIN asset_registry_facilities f
                ON f.facility_id = bwf.facility_id
            WHERE bwf.well_id = :asset_id
            ORDER BY f.facility_id
            """,
            {"asset_id": asset_id},
        )
        payload = detail.iloc[0].where(pd.notna(detail.iloc[0]), None).to_dict()
        payload["linked_facilities"] = links.where(pd.notna(links), None).to_dict(orient="records")
        payload["asset_type"] = "well"
        return payload

    if normalized == "facility":
        detail = _read_frame("SELECT * FROM asset_registry_facilities WHERE facility_id = :asset_id", {"asset_id": asset_id})
        if detail.empty:
            return None
        links = _read_frame(
            """
            SELECT
                w.well_id,
                w.well_name,
                w.operator,
                w.status,
                w.restart_score
            FROM bridge_well_facility bwf
            JOIN asset_registry_wells w
                ON w.well_id = bwf.well_id
            WHERE bwf.facility_id = :asset_id
            ORDER BY w.well_id
            LIMIT 250
            """,
            {"asset_id": asset_id},
        )
        payload = detail.iloc[0].where(pd.notna(detail.iloc[0]), None).to_dict()
        payload["linked_wells"] = links.where(pd.notna(links), None).to_dict(orient="records")
        payload["asset_type"] = "facility"
        return payload

    if normalized == "pipeline":
        detail = _read_frame("SELECT * FROM asset_registry_pipelines WHERE pipeline_id = :asset_id", {"asset_id": asset_id})
        if detail.empty:
            return None
        payload = detail.iloc[0].where(pd.notna(detail.iloc[0]), None).to_dict()
        payload["asset_type"] = "pipeline"
        return payload

    raise ValueError(f"Unsupported asset_type: {asset_type}")
