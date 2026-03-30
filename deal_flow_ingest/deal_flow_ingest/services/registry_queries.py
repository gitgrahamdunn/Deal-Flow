from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import re

import pandas as pd
from sqlalchemy import text

from deal_flow_ingest.apply_saved_sql import apply_saved_sql
from deal_flow_ingest.config import get_database_url
from deal_flow_ingest.db.schema import get_engine


ALLOWED_ASSET_TYPES = {"wells", "facilities", "pipelines"}
DEFAULT_LIMIT_PER_LAYER = 50000
MAX_LIMIT_PER_LAYER = 100000
DEFAULT_OVERLAY_LIMIT = 2500
MAX_OVERLAY_LIMIT = 8000
APPROX_LAT_DEGREES_PER_MILE = 1.0 / 69.172
APPROX_LON_DEGREES_PER_MILE = 1.0 / 44.5
LOW_ZOOM_WELL_AGGREGATION_THRESHOLD = 6.8
AREA_KEY_PATTERN = re.compile(r"^(?:(\d{2})-)?(\d{2})-(\d{3})-(\d{2})W(\d)$")
MERIDIAN_BASE_LONGITUDES = {
    1: -97.4578917,
    2: -102.0,
    3: -106.0,
    4: -110.0,
    5: -114.0,
    6: -118.0,
    7: -122.0,
}


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


def _empty_feature_collection() -> dict[str, object]:
    return {"type": "FeatureCollection", "features": []}


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
    aggregate_wells = bool(
        filters.zoom is not None
        and filters.zoom < LOW_ZOOM_WELL_AGGREGATION_THRESHOLD
        and not filters.operator
        and not filters.candidate_only
    )
    grid_size = 0.32 if filters.zoom is None or filters.zoom < 5.5 else 0.18
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

    if aggregate_wells:
        params["grid_size"] = grid_size
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
        ),
        filtered_wells AS (
            SELECT
                w.*,
                CASE WHEN seller.operator IS NOT NULL THEN 1 ELSE 0 END AS candidate_operator,
                CASE WHEN restart.well_id IS NOT NULL THEN 1 ELSE 0 END AS candidate_restart,
                CASE WHEN seller.operator IS NOT NULL OR restart.well_id IS NOT NULL THEN 1 ELSE 0 END AS candidate_any
            FROM wells_with_display w
            LEFT JOIN seller
                ON seller.operator = w.operator
            LEFT JOIN restart
                ON restart.well_id = w.well_id
            WHERE {' AND '.join(clauses)}
        )
        SELECT
            'well' AS asset_type,
            'agg:' || CAST(ROUND(MIN(display_lat) / :grid_size) * :grid_size AS TEXT) || ':' ||
                CAST(ROUND(MIN(display_lon) / :grid_size) * :grid_size AS TEXT) AS asset_id,
            CAST(COUNT(*) AS TEXT) || ' wells' AS asset_name,
            NULL AS license_number,
            NULL AS operator,
            'AGGREGATED' AS status,
            AVG(display_lat) AS lat,
            AVG(display_lon) AS lon,
            NULL AS field_name,
            NULL AS pool_name,
            NULL AS restart_score,
            'aggregated' AS location_method,
            MAX(candidate_operator) AS candidate_operator,
            MAX(candidate_restart) AS candidate_restart,
            MAX(candidate_any) AS candidate_any,
            COUNT(*) AS well_count,
            1 AS is_aggregate
        FROM filtered_wells
        GROUP BY
            ROUND(display_lat / :grid_size),
            ROUND(display_lon / :grid_size)
        ORDER BY MAX(candidate_any) DESC, COUNT(*) DESC
        LIMIT :limit
        """
        return _read_frame(sql, params)

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
        CASE WHEN seller.operator IS NOT NULL OR restart.well_id IS NOT NULL THEN 1 ELSE 0 END AS candidate_any,
        1 AS well_count,
        0 AS is_aggregate
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


def _normalize_overlay_limit(limit: int) -> int:
    normalized_limit = int(limit)
    if normalized_limit <= 0:
        raise ValueError("limit must be positive")
    return min(normalized_limit, MAX_OVERLAY_LIMIT)


def _parse_area_key(area_key: str | None) -> dict[str, int] | None:
    normalized = str(area_key or "").strip().upper()
    if not normalized:
        return None
    matched = AREA_KEY_PATTERN.match(normalized)
    if not matched:
        return None
    lsd_raw, section_raw, township_raw, range_raw, meridian_raw = matched.groups()
    return {
        "lsd": int(lsd_raw) if lsd_raw else 0,
        "section": int(section_raw),
        "township": int(township_raw),
        "range": int(range_raw),
        "meridian": int(meridian_raw),
    }


def _get_section_offsets(section: int) -> tuple[int, int] | None:
    if section < 1 or section > 36:
        return None
    row = (section - 1) // 6
    position = section - (row * 6)
    col = position - 1 if row % 2 == 0 else 6 - position
    return row, col


def _get_lsd_offsets(lsd: int) -> tuple[int, int] | None:
    if lsd < 1 or lsd > 16:
        return None
    row = (lsd - 1) // 4
    position = lsd - (row * 4)
    col = position - 1 if row % 2 == 0 else 4 - position
    return row, col


def _get_area_bounds(area_key: str | None) -> dict[str, float] | None:
    parsed = _parse_area_key(area_key)
    if parsed is None:
        return None
    section_offsets = _get_section_offsets(parsed["section"])
    if section_offsets is None:
        return None
    meridian_lon = MERIDIAN_BASE_LONGITUDES.get(parsed["meridian"])
    if meridian_lon is None:
        return None

    section_row, section_col = section_offsets
    lat_offset_miles = ((parsed["township"] - 1) * 6.0) + section_row
    lon_offset_miles = ((parsed["range"] - 1) * 6.0) + section_col
    cell_size_miles = 1.0

    if parsed["lsd"]:
        lsd_offsets = _get_lsd_offsets(parsed["lsd"])
        if lsd_offsets is None:
            return None
        lsd_row, lsd_col = lsd_offsets
        lat_offset_miles += lsd_row * 0.25
        lon_offset_miles += lsd_col * 0.25
        cell_size_miles = 0.25

    south = 49.0 + (lat_offset_miles * APPROX_LAT_DEGREES_PER_MILE)
    north = 49.0 + ((lat_offset_miles + cell_size_miles) * APPROX_LAT_DEGREES_PER_MILE)
    east = meridian_lon - (lon_offset_miles * APPROX_LON_DEGREES_PER_MILE)
    west = meridian_lon - ((lon_offset_miles + cell_size_miles) * APPROX_LON_DEGREES_PER_MILE)

    return {
        "south": south,
        "north": north,
        "west": west,
        "east": east,
    }


def _bounds_to_polygon(bounds: dict[str, float]) -> list[list[float]]:
    return [
        [bounds["west"], bounds["south"]],
        [bounds["east"], bounds["south"]],
        [bounds["east"], bounds["north"]],
        [bounds["west"], bounds["north"]],
        [bounds["west"], bounds["south"]],
    ]


def _bbox_contains_bounds(
    bounds: dict[str, float],
    *,
    min_lat: float | None,
    max_lat: float | None,
    min_lon: float | None,
    max_lon: float | None,
) -> bool:
    if min_lat is not None and bounds["north"] < min_lat:
        return False
    if max_lat is not None and bounds["south"] > max_lat:
        return False
    if min_lon is not None and bounds["east"] < min_lon:
        return False
    if max_lon is not None and bounds["west"] > max_lon:
        return False
    return True


def _package_overlay_centroid_sql(alias: str) -> tuple[str, str]:
    lsd_expr = f"CAST(SUBSTR({alias}.area_key, 1, 2) AS INTEGER)"
    section_expr = f"CAST(SUBSTR({alias}.area_key, 4, 2) AS INTEGER)"
    lat_expr = f"""
    CASE
        WHEN {alias}.area_key IS NULL OR TRIM({alias}.area_key) = '' THEN NULL
        WHEN {alias}.township IS NULL OR {alias}.range IS NULL OR {alias}.meridian IS NULL THEN NULL
        ELSE
            49.0
            + (
                (({alias}.township - 1) * 6.0)
                + CAST((({section_expr} - 1) / 6) AS INTEGER)
                + CASE
                    WHEN {lsd_expr} BETWEEN 1 AND 16
                    THEN (CAST((({lsd_expr} - 1) / 4) AS INTEGER) * 0.25) + 0.125
                    ELSE 0.5
                  END
            ) * {APPROX_LAT_DEGREES_PER_MILE}
    END
    """
    lon_expr = f"""
    CASE
        WHEN {alias}.area_key IS NULL OR TRIM({alias}.area_key) = '' THEN NULL
        WHEN {alias}.township IS NULL OR {alias}.range IS NULL OR {alias}.meridian IS NULL THEN NULL
        ELSE
            (
                CASE
                    WHEN {alias}.meridian = 1 THEN -97.4578917
                    WHEN {alias}.meridian = 2 THEN -102.0
                    WHEN {alias}.meridian = 3 THEN -106.0
                    WHEN {alias}.meridian = 4 THEN -110.0
                    WHEN {alias}.meridian = 5 THEN -114.0
                    WHEN {alias}.meridian = 6 THEN -118.0
                    WHEN {alias}.meridian = 7 THEN -122.0
                    ELSE NULL
                END
            )
            - (
                (({alias}.range - 1) * 6.0)
                + CASE
                    WHEN MOD(CAST((({section_expr} - 1) / 6) AS INTEGER), 2) = 0
                    THEN ({section_expr} - (CAST((({section_expr} - 1) / 6) AS INTEGER) * 6) - 1)
                    ELSE (6 - ({section_expr} - (CAST((({section_expr} - 1) / 6) AS INTEGER) * 6)))
                  END
                + CASE
                    WHEN {lsd_expr} BETWEEN 1 AND 16
                    THEN (
                        CASE
                            WHEN MOD(CAST((({lsd_expr} - 1) / 4) AS INTEGER), 2) = 0
                            THEN ({lsd_expr} - (CAST((({lsd_expr} - 1) / 4) AS INTEGER) * 4) - 1) * 0.25 + 0.125
                            ELSE (4 - ({lsd_expr} - (CAST((({lsd_expr} - 1) / 4) AS INTEGER) * 4))) * 0.25 + 0.125
                        END
                    )
                    ELSE 0.5
                  END
            ) * {APPROX_LON_DEGREES_PER_MILE}
    END
    """
    return lat_expr, lon_expr


def _get_package_overlay_rows(
    *,
    operator: str | None,
    min_lat: float | None,
    max_lat: float | None,
    min_lon: float | None,
    max_lon: float | None,
    limit: int,
) -> pd.DataFrame:
    params: dict[str, object] = {"limit": limit}
    base_clauses = [
        "p.area_key IS NOT NULL",
        "p.township IS NOT NULL",
        "p.range IS NOT NULL",
        "p.meridian IS NOT NULL",
    ]
    outer_clauses = ["overlay_rows.centroid_lat IS NOT NULL", "overlay_rows.centroid_lon IS NOT NULL"]
    if operator:
        base_clauses.append("p.operator = :operator")
        params["operator"] = operator
    else:
        if min_lat is not None:
            outer_clauses.append("overlay_rows.centroid_lat >= :min_lat")
            params["min_lat"] = min_lat
        if max_lat is not None:
            outer_clauses.append("overlay_rows.centroid_lat <= :max_lat")
            params["max_lat"] = max_lat
        if min_lon is not None:
            outer_clauses.append("overlay_rows.centroid_lon >= :min_lon")
            params["min_lon"] = min_lon
        if max_lon is not None:
            outer_clauses.append("overlay_rows.centroid_lon <= :max_lon")
            params["max_lon"] = max_lon

    centroid_lat_sql, centroid_lon_sql = _package_overlay_centroid_sql("p")
    sql = f"""
    WITH overlay_rows AS (
        SELECT
            p.operator_id,
            p.operator,
            p.area_key,
            p.township,
            p.range,
            p.meridian,
            p.package_score,
            p.suspended_well_count,
            p.high_priority_well_count,
            p.estimated_restart_upside_bpd,
            p.linked_facility_count,
            p.facility_linked_well_count,
            o.package_count,
            o.total_suspended_wells,
            o.total_high_priority_wells,
            o.total_estimated_restart_upside_bpd,
            o.avg_package_score,
            o.top_package_score,
            o.core_area_key,
            o.footprint_type,
            o.footprint_score,
            {centroid_lat_sql} AS centroid_lat,
            {centroid_lon_sql} AS centroid_lon
        FROM package_candidates p
        LEFT JOIN operator_area_footprints o
            ON o.operator_id = p.operator_id
        WHERE {' AND '.join(base_clauses)}
    )
    SELECT *
    FROM overlay_rows
    WHERE {' AND '.join(outer_clauses)}
    ORDER BY package_score DESC, operator, area_key
    LIMIT :limit
    """
    return _read_frame(sql, params)


def get_registry_map_overlays(
    *,
    operator: str | None = None,
    min_lat: float | None = None,
    max_lat: float | None = None,
    min_lon: float | None = None,
    max_lon: float | None = None,
    include_package_areas: bool = True,
    include_operator_footprints: bool = True,
    limit: int = DEFAULT_OVERLAY_LIMIT,
) -> dict[str, object]:
    normalized_operator = operator.strip() if isinstance(operator, str) and operator.strip() else None
    normalized_limit = _normalize_overlay_limit(limit)
    if min_lat is not None and max_lat is not None and min_lat > max_lat:
        raise ValueError("min_lat cannot be greater than max_lat")
    if min_lon is not None and max_lon is not None and min_lon > max_lon:
        raise ValueError("min_lon cannot be greater than max_lon")
    if not include_package_areas and not include_operator_footprints:
        return {
            "package_areas": _empty_feature_collection(),
            "operator_footprints": _empty_feature_collection(),
            "counts": {"package_areas": 0, "operator_footprints": 0},
        }

    rows = _get_package_overlay_rows(
        operator=normalized_operator,
        min_lat=min_lat,
        max_lat=max_lat,
        min_lon=min_lon,
        max_lon=max_lon,
        limit=normalized_limit,
    )
    if rows.empty:
        return {
            "package_areas": _empty_feature_collection(),
            "operator_footprints": _empty_feature_collection(),
            "counts": {"package_areas": 0, "operator_footprints": 0},
        }

    package_features: list[dict[str, object]] = []
    footprint_features: list[dict[str, object]] = []
    grouped_polygons: dict[tuple[object, object], list[list[list[float]]]] = defaultdict(list)
    grouped_properties: dict[tuple[object, object], dict[str, object]] = {}

    for row in rows.where(pd.notna(rows), None).to_dict(orient="records"):
        bounds = _get_area_bounds(row.get("area_key"))
        if bounds is None:
            continue
        if normalized_operator is None and not _bbox_contains_bounds(
            bounds,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
        ):
            continue
        polygon = _bounds_to_polygon(bounds)
        if include_package_areas:
            package_features.append(
                {
                    "type": "Feature",
                    "id": f"package:{row.get('operator_id') or row.get('operator')}:{row.get('area_key')}",
                    "geometry": {"type": "Polygon", "coordinates": [polygon]},
                    "properties": {
                        "overlay_type": "package_area",
                        "operator_id": row.get("operator_id"),
                        "operator": row.get("operator"),
                        "area_key": row.get("area_key"),
                        "package_score": row.get("package_score"),
                        "suspended_well_count": row.get("suspended_well_count"),
                        "high_priority_well_count": row.get("high_priority_well_count"),
                        "estimated_restart_upside_bpd": row.get("estimated_restart_upside_bpd"),
                        "linked_facility_count": row.get("linked_facility_count"),
                        "facility_linked_well_count": row.get("facility_linked_well_count"),
                        "footprint_type": row.get("footprint_type"),
                        "core_area_key": row.get("core_area_key"),
                        "is_core_area": row.get("area_key") == row.get("core_area_key"),
                    },
                }
            )
        if include_operator_footprints:
            group_key = (row.get("operator_id"), row.get("operator"))
            grouped_polygons[group_key].append([polygon])
            grouped_properties[group_key] = {
                "overlay_type": "operator_footprint",
                "operator_id": row.get("operator_id"),
                "operator": row.get("operator"),
                "package_count": row.get("package_count"),
                "visible_package_cells": len(grouped_polygons[group_key]),
                "footprint_type": row.get("footprint_type"),
                "footprint_score": row.get("footprint_score"),
                "top_package_score": row.get("top_package_score"),
                "avg_package_score": row.get("avg_package_score"),
                "core_area_key": row.get("core_area_key"),
                "total_suspended_wells": row.get("total_suspended_wells"),
                "total_high_priority_wells": row.get("total_high_priority_wells"),
                "total_estimated_restart_upside_bpd": row.get("total_estimated_restart_upside_bpd"),
            }

    if include_operator_footprints:
        for (operator_id, operator_name), polygons in grouped_polygons.items():
            properties = grouped_properties[(operator_id, operator_name)] | {"visible_package_cells": len(polygons)}
            footprint_features.append(
                {
                    "type": "Feature",
                    "id": f"footprint:{operator_id or operator_name}",
                    "geometry": {"type": "MultiPolygon", "coordinates": polygons},
                    "properties": properties,
                }
            )
        footprint_features.sort(
            key=lambda feature: (
                -(float(feature["properties"].get("footprint_score") or 0.0)),
                str(feature["properties"].get("operator") or ""),
            )
        )

    return {
        "package_areas": {"type": "FeatureCollection", "features": package_features},
        "operator_footprints": {"type": "FeatureCollection", "features": footprint_features},
        "counts": {
            "package_areas": len(package_features),
            "operator_footprints": len(footprint_features),
        },
    }


def get_registry_filter_options() -> dict[str, list[str]]:
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
        "well_statuses": well_statuses["status"].dropna().astype(str).tolist() if "status" in well_statuses.columns else [],
        "facility_statuses": facility_statuses["status"].dropna().astype(str).tolist()
        if "status" in facility_statuses.columns
        else [],
        "pipeline_statuses": pipeline_statuses["status"].dropna().astype(str).tolist()
        if "status" in pipeline_statuses.columns
        else [],
    }


def get_operator_suggestions(query: str = "", limit: int = 25) -> list[str]:
    normalized_query = query.strip()
    normalized_limit = max(1, min(int(limit), 100))
    params: dict[str, object] = {"limit": normalized_limit}
    where_clause = ""
    if normalized_query:
        params["query"] = f"%{normalized_query.lower()}%"
        where_clause = "WHERE LOWER(operator) LIKE :query"

    frame = _read_frame(
        f"""
        SELECT operator
        FROM (
            SELECT DISTINCT operator FROM asset_registry_wells WHERE operator IS NOT NULL AND TRIM(operator) <> ''
            UNION
            SELECT DISTINCT operator FROM asset_registry_facilities WHERE operator IS NOT NULL AND TRIM(operator) <> ''
            UNION
            SELECT DISTINCT operator FROM asset_registry_pipelines WHERE operator IS NOT NULL AND TRIM(operator) <> ''
        ) operators
        {where_clause}
        ORDER BY LENGTH(operator), operator
        LIMIT :limit
        """,
        params,
    )
    if "operator" not in frame.columns:
        return []
    return frame["operator"].dropna().astype(str).tolist()


def _is_package_candidate(operator: str | None) -> bool:
    if not operator:
        return False
    frame = _read_frame(
        "SELECT 1 AS matched FROM package_candidates WHERE operator = :operator LIMIT 1",
        {"operator": operator},
    )
    return not frame.empty


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


def _get_operator_metrics(operator: str | None) -> dict[str, object] | None:
    if not operator:
        return None
    metrics = _read_frame(
        """
        SELECT
            fm.as_of_date,
            fm.avg_oil_bpd_30d,
            fm.avg_oil_bpd_365d,
            fm.yoy_change_pct,
            fm.decline_score,
            fm.distress_score,
            fm.suspended_wells_count,
            fm.restart_candidates_count,
            fm.restart_upside_bpd_est
        FROM fact_operator_metrics fm
        JOIN dim_operator o
            ON o.operator_id = fm.operator_id
        WHERE o.name_norm = :operator
        ORDER BY fm.as_of_date DESC
        LIMIT 1
        """,
        {"operator": operator},
    )
    if metrics.empty:
        return None
    return metrics.iloc[0].where(pd.notna(metrics.iloc[0]), None).to_dict()


def _get_well_production_summary(well_id: str) -> dict[str, object] | None:
    summary = _read_frame(
        """
        SELECT
            MAX(month) AS latest_month,
            COUNT(*) AS active_months,
            SUM(COALESCE(oil_bbl, 0)) AS oil_bbl_total,
            SUM(COALESCE(gas_mcf, 0)) AS gas_mcf_total,
            SUM(COALESCE(water_bbl, 0)) AS water_bbl_total,
            MAX(CASE WHEN is_estimated THEN 1 ELSE 0 END) AS has_estimated
        FROM fact_well_production_monthly
        WHERE well_id = :well_id
        """,
        {"well_id": well_id},
    )
    if summary.empty:
        return None
    payload = summary.iloc[0].where(pd.notna(summary.iloc[0]), None).to_dict()
    if not payload.get("latest_month"):
        return None
    return payload


def _get_facility_production_summary(facility_id: str) -> dict[str, object] | None:
    summary = _read_frame(
        """
        SELECT
            MAX(month) AS latest_month,
            COUNT(*) AS active_months,
            SUM(COALESCE(oil_bbl, 0)) AS oil_bbl_total,
            SUM(COALESCE(gas_mcf, 0)) AS gas_mcf_total,
            SUM(COALESCE(water_bbl, 0)) AS water_bbl_total,
            SUM(COALESCE(condensate_bbl, 0)) AS condensate_bbl_total
        FROM fact_facility_production_monthly
        WHERE facility_id = :facility_id
        """,
        {"facility_id": facility_id},
    )
    if summary.empty:
        return None
    payload = summary.iloc[0].where(pd.notna(summary.iloc[0]), None).to_dict()
    if not payload.get("latest_month"):
        return None
    return payload


def _is_seller_candidate(operator: str | None) -> bool:
    if not operator:
        return False
    frame = _read_frame(
        "SELECT 1 AS matched FROM seller_theses WHERE operator = :operator LIMIT 1",
        {"operator": operator},
    )
    return not frame.empty


def get_operator_detail(operator: str) -> dict[str, object] | None:
    normalized_operator = operator.strip()
    if not normalized_operator:
        return None

    counts = _read_frame(
        """
        SELECT
            (SELECT COUNT(*) FROM asset_registry_wells WHERE operator = :operator) AS wells,
            (SELECT COUNT(*) FROM asset_registry_facilities WHERE operator = :operator) AS facilities,
            (SELECT COUNT(*) FROM asset_registry_pipelines WHERE operator = :operator) AS pipelines
        """,
        {"operator": normalized_operator},
    )
    count_payload = counts.iloc[0].where(pd.notna(counts.iloc[0]), 0).to_dict() if not counts.empty else {}
    asset_counts = {key: int(count_payload.get(key) or 0) for key in ("wells", "facilities", "pipelines")}

    seller = _read_frame(
        """
        SELECT
            operator,
            as_of_date,
            thesis_score,
            thesis_priority,
            seller_score,
            restart_upside_bpd_est,
            package_count,
            top_package_score,
            core_area_key
        FROM seller_theses
        WHERE operator = :operator
        ORDER BY thesis_score DESC, seller_score DESC
        LIMIT 1
        """,
        {"operator": normalized_operator},
    )
    package_candidates = _read_frame(
        """
        SELECT
            area_key,
            package_score,
            estimated_restart_upside_bpd,
            suspended_well_count,
            high_priority_well_count
        FROM package_candidates
        WHERE operator = :operator
        ORDER BY package_score DESC, estimated_restart_upside_bpd DESC, area_key
        LIMIT 8
        """,
        {"operator": normalized_operator},
    )
    top_wells = _read_frame(
        """
        SELECT
            well_id,
            well_name,
            status,
            restart_score
        FROM asset_registry_wells
        WHERE operator = :operator
        ORDER BY COALESCE(restart_score, 0) DESC, well_id
        LIMIT 12
        """,
        {"operator": normalized_operator},
    )
    top_facilities = _read_frame(
        """
        SELECT
            facility_id,
            facility_name,
            facility_status
        FROM asset_registry_facilities
        WHERE operator = :operator
        ORDER BY facility_id
        LIMIT 12
        """,
        {"operator": normalized_operator},
    )
    top_pipelines = _read_frame(
        """
        SELECT
            pipeline_id,
            licence_line_number,
            segment_status
        FROM asset_registry_pipelines
        WHERE operator = :operator
        ORDER BY pipeline_id
        LIMIT 12
        """,
        {"operator": normalized_operator},
    )

    operator_metrics = _get_operator_metrics(normalized_operator)
    has_results = any(asset_counts.values()) or operator_metrics or not seller.empty or not package_candidates.empty
    if not has_results:
        return None

    return {
        "operator": normalized_operator,
        "asset_counts": asset_counts,
        "operator_metrics": operator_metrics,
        "candidate_flags": {
            "seller_candidate": _is_seller_candidate(normalized_operator),
            "package_candidate": _is_package_candidate(normalized_operator),
        },
        "seller_thesis": seller.iloc[0].where(pd.notna(seller.iloc[0]), None).to_dict() if not seller.empty else None,
        "package_candidates": package_candidates.where(pd.notna(package_candidates), None).to_dict(orient="records"),
        "top_wells": top_wells.where(pd.notna(top_wells), None).to_dict(orient="records"),
        "top_facilities": top_facilities.where(pd.notna(top_facilities), None).to_dict(orient="records"),
        "top_pipelines": top_pipelines.where(pd.notna(top_pipelines), None).to_dict(orient="records"),
    }


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
        payload["production_summary"] = _get_well_production_summary(asset_id)
        payload["operator_metrics"] = _get_operator_metrics(payload.get("operator"))
        payload["candidate_flags"] = {
            "seller_candidate": _is_seller_candidate(payload.get("operator")),
            "restart_candidate": bool(payload.get("restart_score")),
        }
        payload["location"] = {
            "lat": payload.get("lat"),
            "lon": payload.get("lon"),
            "location_method": "surveyed" if payload.get("lat") is not None and payload.get("lon") is not None else "dls_approx",
        }
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
        payload["production_summary"] = _get_facility_production_summary(asset_id)
        payload["operator_metrics"] = _get_operator_metrics(payload.get("operator"))
        payload["candidate_flags"] = {
            "seller_candidate": _is_seller_candidate(payload.get("operator")),
            "restart_candidate": any(item.get("restart_score") for item in payload["linked_wells"]),
        }
        payload["location"] = {
            "lat": payload.get("lat"),
            "lon": payload.get("lon"),
            "location_method": "surveyed" if payload.get("lat") is not None and payload.get("lon") is not None else "missing",
        }
        payload["asset_type"] = "facility"
        return payload

    if normalized == "pipeline":
        detail = _read_frame("SELECT * FROM asset_registry_pipelines WHERE pipeline_id = :asset_id", {"asset_id": asset_id})
        if detail.empty:
            return None
        payload = detail.iloc[0].where(pd.notna(detail.iloc[0]), None).to_dict()
        payload["operator_metrics"] = _get_operator_metrics(payload.get("operator"))
        payload["candidate_flags"] = {
            "seller_candidate": _is_seller_candidate(payload.get("operator")),
            "restart_candidate": False,
        }
        payload["location"] = {
            "lat": payload.get("centroid_lat"),
            "lon": payload.get("centroid_lon"),
            "location_method": "surveyed" if payload.get("centroid_lat") is not None and payload.get("centroid_lon") is not None else "missing",
        }
        payload["asset_type"] = "pipeline"
        return payload

    raise ValueError(f"Unsupported asset_type: {asset_type}")
