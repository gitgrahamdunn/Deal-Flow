CREATE VIEW package_candidates AS
WITH latest_facility_prod AS (
    SELECT
        facility_id,
        SUM(COALESCE(oil_bbl, 0)) AS trailing_oil_bbl,
        SUM(COALESCE(gas_mcf, 0)) AS trailing_gas_mcf
    FROM fact_facility_production_monthly
    GROUP BY facility_id
),
cluster_wells AS (
    SELECT
        c.operator_id,
        c.operator,
        c.area_key,
        c.township,
        c.range,
        c.meridian,
        c.suspended_well_count,
        c.high_priority_well_count,
        c.avg_restart_score,
        c.estimated_restart_upside_bpd,
        c.operator_distress_score,
        c.cluster_score,
        r.well_id
    FROM asset_clusters c
    JOIN restart_well_candidates r
        ON r.operator_id = c.operator_id
       AND r.area_key = c.area_key
)
SELECT
    cw.operator_id,
    cw.operator,
    cw.area_key,
    cw.township,
    cw.range,
    cw.meridian,
    cw.suspended_well_count,
    cw.high_priority_well_count,
    cw.avg_restart_score,
    cw.estimated_restart_upside_bpd,
    cw.operator_distress_score,
    COUNT(DISTINCT bwf.facility_id) AS linked_facility_count,
    SUM(CASE WHEN df.facility_type = 'BATTERY' THEN 1 ELSE 0 END) AS battery_count,
    COUNT(DISTINCT CASE WHEN bwf.facility_id IS NOT NULL THEN cw.well_id END) AS facility_linked_well_count,
    ROUND((COALESCE(SUM(lfp.trailing_oil_bbl), 0))::numeric, 2) AS linked_facility_trailing_oil_bbl,
    ROUND((COALESCE(SUM(lfp.trailing_gas_mcf), 0))::numeric, 2) AS linked_facility_trailing_gas_mcf,
    ROUND(
        (
        LEAST(
            100.0,
            cw.cluster_score * 0.55
            + COUNT(DISTINCT bwf.facility_id) * 4.0
            + SUM(CASE WHEN df.facility_type = 'BATTERY' THEN 1 ELSE 0 END) * 3.0
            + COUNT(DISTINCT CASE WHEN bwf.facility_id IS NOT NULL THEN cw.well_id END) * 1.5
            + CASE
                WHEN cw.suspended_well_count > 0
                THEN (COUNT(DISTINCT CASE WHEN bwf.facility_id IS NOT NULL THEN cw.well_id END) * 1.0 / cw.suspended_well_count) * 12.0
                ELSE 0
              END
        )
        )::numeric,
        2
    ) AS package_score
FROM cluster_wells cw
LEFT JOIN bridge_well_facility bwf
    ON bwf.well_id = cw.well_id
LEFT JOIN dim_facility df
    ON df.facility_id = bwf.facility_id
LEFT JOIN latest_facility_prod lfp
    ON lfp.facility_id = bwf.facility_id
GROUP BY
    cw.operator_id,
    cw.operator,
    cw.area_key,
    cw.township,
    cw.range,
    cw.meridian,
    cw.suspended_well_count,
    cw.high_priority_well_count,
    cw.avg_restart_score,
    cw.estimated_restart_upside_bpd,
    cw.operator_distress_score,
    cw.cluster_score;
