CREATE VIEW asset_registry_facilities AS
WITH prod_rollup AS (
    SELECT
        facility_id,
        MAX(month) AS latest_prod_month,
        SUM(COALESCE(oil_bbl, 0)) AS total_oil_bbl,
        SUM(COALESCE(gas_mcf, 0)) AS total_gas_mcf,
        SUM(COALESCE(water_bbl, 0)) AS total_water_bbl,
        SUM(COALESCE(condensate_bbl, 0)) AS total_condensate_bbl
    FROM fact_facility_production_monthly
    GROUP BY facility_id
),
linked_wells AS (
    SELECT
        facility_id,
        COUNT(DISTINCT well_id) AS linked_well_count
    FROM bridge_well_facility
    GROUP BY facility_id
)
SELECT
    f.facility_id,
    f.facility_name,
    f.license_number,
    f.facility_type,
    f.facility_subtype,
    o.name_norm AS operator,
    f.facility_status,
    f.lsd,
    f.section,
    f.township,
    f.range,
    f.meridian,
    f.lat,
    f.lon,
    COALESCE(w.linked_well_count, 0) AS linked_well_count,
    p.latest_prod_month,
    ROUND((COALESCE(p.total_oil_bbl, 0))::numeric, 2) AS total_oil_bbl,
    ROUND((COALESCE(p.total_gas_mcf, 0))::numeric, 2) AS total_gas_mcf,
    ROUND((COALESCE(p.total_water_bbl, 0))::numeric, 2) AS total_water_bbl,
    ROUND((COALESCE(p.total_condensate_bbl, 0))::numeric, 2) AS total_condensate_bbl,
    f.source
FROM dim_facility f
LEFT JOIN dim_operator o
    ON o.operator_id = f.facility_operator_id
LEFT JOIN linked_wells w
    ON w.facility_id = f.facility_id
LEFT JOIN prod_rollup p
    ON p.facility_id = f.facility_id;
