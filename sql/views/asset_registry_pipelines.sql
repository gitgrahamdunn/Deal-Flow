CREATE VIEW asset_registry_pipelines AS
SELECT
    p.pipeline_id,
    p.license_number,
    p.line_number,
    p.licence_line_number,
    p.company_name,
    o.name_norm AS operator,
    p.ba_code,
    p.segment_status,
    p.from_facility_type,
    p.from_location,
    p.to_facility_type,
    p.to_location,
    p.substance1,
    p.substance2,
    p.substance3,
    ROUND(COALESCE(p.segment_length_km, 0), 4) AS segment_length_km,
    p.geometry_source,
    p.centroid_lat,
    p.centroid_lon,
    p.source
FROM dim_pipeline p
LEFT JOIN dim_operator o
    ON o.operator_id = p.operator_id;
