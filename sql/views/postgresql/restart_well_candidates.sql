CREATE VIEW restart_well_candidates AS
WITH latest_restart AS (
    SELECT MAX(as_of_date) AS as_of_date
    FROM fact_well_restart_score
),
latest_metrics AS (
    SELECT MAX(as_of_date) AS as_of_date
    FROM fact_operator_metrics
),
latest_well_prod AS (
    SELECT
        p.well_id,
        MAX(p.month) AS last_prod_month
    FROM fact_well_production_monthly p
    WHERE COALESCE(p.oil_bbl, 0) <> 0
       OR COALESCE(p.gas_mcf, 0) <> 0
       OR COALESCE(p.water_bbl, 0) <> 0
    GROUP BY p.well_id
)
SELECT
    r.as_of_date,
    r.well_id,
    o.operator_id,
    o.name_norm AS operator,
    w.status AS current_status,
    w.licensee_operator_id,
    w.lsd,
    w.section,
    w.township,
    w.range,
    w.meridian,
    w.lat,
    w.lon,
    (
        RIGHT('00' || COALESCE(w.lsd, ''), 2)
        || '-'
        || RIGHT('00' || COALESCE(w.section::text, ''), 2)
        || '-'
        || RIGHT('000' || COALESCE(w.township::text, ''), 3)
        || '-'
        || RIGHT('00' || COALESCE(w.range::text, ''), 2)
        || 'W'
        || COALESCE(w.meridian::text, '')
    ) AS area_key,
    r.last_prod_month,
    CAST(
        (
            (EXTRACT(YEAR FROM r.as_of_date) - EXTRACT(YEAR FROM COALESCE(r.last_prod_month, p.last_prod_month))) * 12
            + (EXTRACT(MONTH FROM r.as_of_date) - EXTRACT(MONTH FROM COALESCE(r.last_prod_month, p.last_prod_month)))
        ) AS INTEGER
    ) AS months_since_last_production,
    r.avg_oil_bpd_last_3mo_before_shutin,
    r.avg_oil_bpd_last_12mo_before_shutin,
    r.shutin_recency_days,
    r.restart_score,
    m.distress_score AS operator_distress_score,
    m.restart_upside_bpd_est AS operator_restart_upside_bpd_est,
    CASE
        WHEN r.restart_score >= 70 THEN 'A'
        WHEN r.restart_score >= 50 THEN 'B'
        WHEN r.restart_score >= 30 THEN 'C'
        ELSE 'IGNORE'
    END AS restart_tier
FROM fact_well_restart_score r
JOIN latest_restart lr
    ON r.as_of_date = lr.as_of_date
JOIN dim_well w
    ON w.well_id = r.well_id
LEFT JOIN latest_well_prod p
    ON p.well_id = r.well_id
LEFT JOIN fact_operator_metrics m
    ON m.operator_id = w.licensee_operator_id
   AND m.as_of_date = (SELECT as_of_date FROM latest_metrics)
LEFT JOIN dim_operator o
    ON o.operator_id = w.licensee_operator_id;
