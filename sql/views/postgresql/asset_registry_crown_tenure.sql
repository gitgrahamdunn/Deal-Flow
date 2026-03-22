CREATE VIEW asset_registry_crown_tenure AS
WITH asset_locations AS (
    SELECT
        'well' AS asset_type,
        w.well_id AS asset_id,
        w.well_name AS asset_name,
        w.license_number AS asset_license_number,
        o.name_norm AS asset_operator,
        w.lsd,
        w.section,
        w.township,
        w.range,
        w.meridian
    FROM dim_well w
    LEFT JOIN dim_operator o
        ON o.operator_id = w.licensee_operator_id
    WHERE w.section IS NOT NULL
      AND w.township IS NOT NULL
      AND w.range IS NOT NULL
      AND w.meridian IS NOT NULL

    UNION ALL

    SELECT
        'facility' AS asset_type,
        f.facility_id AS asset_id,
        f.facility_name AS asset_name,
        f.license_number AS asset_license_number,
        o.name_norm AS asset_operator,
        f.lsd,
        f.section,
        f.township,
        f.range,
        f.meridian
    FROM dim_facility f
    LEFT JOIN dim_operator o
        ON o.operator_id = f.facility_operator_id
    WHERE f.section IS NOT NULL
      AND f.township IS NOT NULL
      AND f.range IS NOT NULL
      AND f.meridian IS NOT NULL
),
holder_rollup AS (
    SELECT
        bc.disposition_id,
        STRING_AGG(DISTINCT cc.client_name_raw, ', ') AS holder_names,
        COUNT(DISTINCT bc.client_id) AS holder_count
    FROM bridge_crown_disposition_client bc
    JOIN dim_crown_client cc
        ON cc.client_id = bc.client_id
    GROUP BY bc.disposition_id
)
SELECT
    a.asset_type,
    a.asset_id,
    a.asset_name,
    a.asset_license_number,
    a.asset_operator,
    d.disposition_id,
    d.agreement_no,
    d.disposition_type,
    d.disposition_status,
    d.effective_from,
    d.effective_to,
    l.tract_no,
    l.lsd AS disposition_lsd,
    l.section AS disposition_section,
    l.township AS disposition_township,
    l.range AS disposition_range,
    l.meridian AS disposition_meridian,
    CASE
        WHEN l.lsd IS NOT NULL AND TRIM(l.lsd) <> '' THEN 'lsd_exact'
        ELSE 'section'
    END AS match_level,
    h.holder_names,
    COALESCE(h.holder_count, 0) AS holder_count,
    d.source AS disposition_source,
    l.source AS land_source
FROM asset_locations a
JOIN bridge_crown_disposition_land l
    ON l.section = a.section
   AND l.township = a.township
   AND l.range = a.range
   AND l.meridian = a.meridian
   AND (
        l.lsd IS NULL
        OR TRIM(l.lsd) = ''
        OR l.lsd = a.lsd
   )
JOIN dim_crown_disposition d
    ON d.disposition_id = l.disposition_id
LEFT JOIN holder_rollup h
    ON h.disposition_id = d.disposition_id;
