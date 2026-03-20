CREATE VIEW asset_clusters AS
SELECT
    r.operator_id,
    r.operator,
    r.area_key,
    r.township,
    r.range,
    r.meridian,
    COUNT(*) AS suspended_well_count,
    SUM(CASE WHEN r.restart_tier IN ('A', 'B') THEN 1 ELSE 0 END) AS high_priority_well_count,
    ROUND(AVG(COALESCE(r.restart_score, 0)), 2) AS avg_restart_score,
    ROUND(SUM(COALESCE(r.avg_oil_bpd_last_3mo_before_shutin, 0)), 2) AS estimated_restart_upside_bpd,
    ROUND(MAX(COALESCE(r.operator_distress_score, 0)), 2) AS operator_distress_score,
    ROUND(
        MIN(
            100,
            COUNT(*) * 4
            + SUM(CASE WHEN r.restart_tier IN ('A', 'B') THEN 1 ELSE 0 END) * 8
            + ROUND(AVG(COALESCE(r.restart_score, 0)), 2) * 0.35
            + ROUND(SUM(COALESCE(r.avg_oil_bpd_last_3mo_before_shutin, 0)), 2) * 0.5
            + ROUND(MAX(COALESCE(r.operator_distress_score, 0)), 2) * 0.15
        ),
        2
    ) AS cluster_score
FROM restart_well_candidates r
WHERE r.area_key IS NOT NULL
GROUP BY
    r.operator_id,
    r.operator,
    r.area_key,
    r.township,
    r.range,
    r.meridian;
