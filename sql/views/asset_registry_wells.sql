CREATE VIEW asset_registry_wells AS
WITH latest_prod AS (
    SELECT
        well_id,
        MAX(month) AS latest_prod_month,
        SUM(COALESCE(oil_bbl, 0)) AS total_oil_bbl,
        SUM(COALESCE(gas_mcf, 0)) AS total_gas_mcf,
        SUM(COALESCE(water_bbl, 0)) AS total_water_bbl
    FROM fact_well_production_monthly
    GROUP BY well_id
),
latest_restart AS (
    SELECT
        well_id,
        MAX(as_of_date) AS as_of_date
    FROM fact_well_restart_score
    GROUP BY well_id
)
SELECT
    w.well_id,
    w.uwi_raw,
    w.license_number,
    w.well_name,
    w.field_name,
    w.pool_name,
    w.status,
    w.spud_date,
    w.lsd,
    w.section,
    w.township,
    w.range,
    w.meridian,
    w.lat,
    w.lon,
    o.name_norm AS operator,
    w.source,
    p.latest_prod_month,
    ROUND(COALESCE(p.total_oil_bbl, 0), 2) AS total_oil_bbl,
    ROUND(COALESCE(p.total_gas_mcf, 0), 2) AS total_gas_mcf,
    ROUND(COALESCE(p.total_water_bbl, 0), 2) AS total_water_bbl,
    r.restart_score
FROM dim_well w
LEFT JOIN dim_operator o
    ON o.operator_id = w.licensee_operator_id
LEFT JOIN latest_prod p
    ON p.well_id = w.well_id
LEFT JOIN fact_well_restart_score r
    ON r.well_id = w.well_id
   AND r.as_of_date = (SELECT lr.as_of_date FROM latest_restart lr WHERE lr.well_id = w.well_id);
