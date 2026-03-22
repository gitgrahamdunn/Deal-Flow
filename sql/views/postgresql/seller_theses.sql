CREATE VIEW seller_theses AS
WITH package_rollup AS (
    SELECT
        operator_id,
        COUNT(*) AS package_count,
        MAX(package_score) AS top_package_score,
        SUM(suspended_well_count) AS packaged_suspended_wells,
        SUM(high_priority_well_count) AS packaged_high_priority_wells,
        SUM(linked_facility_count) AS packaged_linked_facilities,
        SUM(estimated_restart_upside_bpd) AS packaged_restart_upside_bpd
    FROM package_candidates
    GROUP BY operator_id
)
SELECT
    t.operator_id,
    t.operator,
    t.as_of_date,
    t.avg_oil_bpd_30d,
    t.avg_oil_bpd_365d,
    t.yoy_change_pct,
    t.decline_score,
    t.distress_score,
    t.suspended_wells_count,
    t.restart_candidates_count,
    t.restart_upside_bpd_est,
    t.seller_score,
    o.opportunity_score,
    COALESCE(p.package_count, 0) AS package_count,
    COALESCE(p.top_package_score, 0) AS top_package_score,
    COALESCE(p.packaged_suspended_wells, 0) AS packaged_suspended_wells,
    COALESCE(p.packaged_high_priority_wells, 0) AS packaged_high_priority_wells,
    COALESCE(p.packaged_linked_facilities, 0) AS packaged_linked_facilities,
    ROUND((COALESCE(p.packaged_restart_upside_bpd, 0))::numeric, 2) AS packaged_restart_upside_bpd,
    f.core_area_key,
    f.core_area_well_share,
    f.footprint_type,
    f.footprint_score,
    ROUND(
        (
            COALESCE(t.seller_score, 0) * 0.35
            + COALESCE(o.opportunity_score, 0) * 0.30
            + COALESCE(f.footprint_score, 0) * 0.20
            + COALESCE(p.top_package_score, 0) * 0.15
        )::numeric,
        2
    ) AS thesis_score,
    CASE
        WHEN COALESCE(t.seller_score, 0) >= 60
         AND COALESCE(o.opportunity_score, 0) >= 60
         AND COALESCE(f.footprint_type, 'scattered') IN ('single_area', 'core_area')
            THEN 'HIGH'
        WHEN COALESCE(t.seller_score, 0) >= 40
         AND COALESCE(o.opportunity_score, 0) >= 40
            THEN 'MEDIUM'
        ELSE 'LOW'
    END AS thesis_priority
FROM deal_flow_targets t
LEFT JOIN deal_flow_opportunities o
    ON o.operator_id = t.operator_id
LEFT JOIN package_rollup p
    ON p.operator_id = t.operator_id
LEFT JOIN operator_area_footprints f
    ON f.operator_id = t.operator_id;
