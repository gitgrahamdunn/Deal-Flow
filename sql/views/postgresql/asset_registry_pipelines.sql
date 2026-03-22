CREATE VIEW asset_registry_pipelines AS
SELECT
    p.pipeline_id,
    p.license_number,
    p.line_number,
    p.licence_line_number,
    COALESCE(o.name_norm, p.company_name) AS operator,
    p.ba_code,
    p.segment_status,
    p.from_facility_type,
    p.from_location,
    p.to_facility_type,
    p.to_location,
    p.substance1,
    p.substance2,
    p.substance3,
    ROUND((COALESCE(p.segment_length_km, 0))::numeric, 4) AS segment_length_km,
    p.geometry_source,
    p.geometry_wkt,
    p.centroid_lat,
    p.centroid_lon,
    p.source
FROM dim_pipeline p
LEFT JOIN dim_operator o
    ON o.operator_id = p.operator_id;
