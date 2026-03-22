CREATE VIEW deal_flow_targets AS
WITH latest_metrics AS (
    SELECT MAX(as_of_date) AS as_of_date
    FROM fact_operator_metrics
),
latest_liability AS (
    SELECT MAX(as_of_date) AS as_of_date
    FROM fact_operator_liability
)
SELECT
    m.operator_id,
    o.name_norm AS operator,
    m.as_of_date,
    m.avg_oil_bpd_30d,
    m.avg_oil_bpd_365d,
    m.total_oil_bbl_30d,
    m.total_oil_bbl_365d,
    m.yoy_change_pct,
    m.decline_score,
    m.distress_score,
    m.suspended_wells_count,
    m.restart_candidates_count,
    m.restart_upside_bpd_est,
    l.inactive_wells,
    l.active_wells,
    l.deemed_assets,
    l.deemed_liabilities,
    l.ratio AS liability_ratio,
    (
        CASE
            WHEN COALESCE(m.avg_oil_bpd_30d, 0) < 10 THEN 25
            WHEN COALESCE(m.avg_oil_bpd_30d, 0) < 25 THEN 15
            ELSE 0
        END
        + CASE
            WHEN COALESCE(l.ratio, 999) < 1 THEN 25
            WHEN COALESCE(l.ratio, 999) < 2 THEN 10
            ELSE 0
        END
        + CASE
            WHEN COALESCE(m.yoy_change_pct, 0) < -25 THEN 20
            WHEN COALESCE(m.yoy_change_pct, 0) < -10 THEN 10
            ELSE 0
        END
        + LEAST(COALESCE(m.distress_score, 0), 30.0)
        + LEAST(COALESCE(l.inactive_wells, 0) * 2.0, 20.0)
    ) AS seller_score
FROM fact_operator_metrics m
JOIN latest_metrics lm
    ON m.as_of_date = lm.as_of_date
JOIN dim_operator o
    ON o.operator_id = m.operator_id
LEFT JOIN fact_operator_liability l
    ON l.operator_id = m.operator_id
   AND l.as_of_date = (SELECT as_of_date FROM latest_liability);
