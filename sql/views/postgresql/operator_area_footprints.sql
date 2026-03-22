CREATE VIEW operator_area_footprints AS
WITH ranked_packages AS (
    SELECT
        p.*,
        ROW_NUMBER() OVER (PARTITION BY p.operator_id ORDER BY p.package_score DESC, p.area_key) AS package_rank
    FROM package_candidates p
),
operator_totals AS (
    SELECT
        operator_id,
        operator,
        COUNT(*) AS package_count,
        SUM(suspended_well_count) AS total_suspended_wells,
        SUM(high_priority_well_count) AS total_high_priority_wells,
        SUM(estimated_restart_upside_bpd) AS total_estimated_restart_upside_bpd,
        SUM(linked_facility_count) AS total_linked_facilities,
        SUM(facility_linked_well_count) AS total_facility_linked_wells,
        AVG(package_score) AS avg_package_score,
        MAX(package_score) AS top_package_score
    FROM package_candidates
    GROUP BY operator_id, operator
)
SELECT
    ot.operator_id,
    ot.operator,
    ot.package_count,
    ot.total_suspended_wells,
    ot.total_high_priority_wells,
    ROUND((ot.total_estimated_restart_upside_bpd)::numeric, 2) AS total_estimated_restart_upside_bpd,
    ot.total_linked_facilities,
    ot.total_facility_linked_wells,
    ROUND((ot.avg_package_score)::numeric, 2) AS avg_package_score,
    ROUND((ot.top_package_score)::numeric, 2) AS top_package_score,
    rp.area_key AS core_area_key,
    rp.suspended_well_count AS core_area_suspended_well_count,
    rp.high_priority_well_count AS core_area_high_priority_well_count,
    rp.linked_facility_count AS core_area_linked_facility_count,
    ROUND(
        CASE
            WHEN COALESCE(ot.total_suspended_wells, 0) > 0
            THEN rp.suspended_well_count * 1.0 / ot.total_suspended_wells
            ELSE 0
        END::numeric,
        4
    ) AS core_area_well_share,
    CASE
        WHEN ot.package_count = 1 THEN 'single_area'
        WHEN (
            CASE
                WHEN COALESCE(ot.total_suspended_wells, 0) > 0
                THEN rp.suspended_well_count * 1.0 / ot.total_suspended_wells
                ELSE 0
            END
        ) >= 0.6 THEN 'core_area'
        ELSE 'scattered'
    END AS footprint_type,
    ROUND(
        (
        LEAST(
            100.0,
            ot.top_package_score * 0.5
            + ot.avg_package_score * 0.25
            + CASE WHEN ot.package_count = 1 THEN 12.0 ELSE 0 END
            + CASE
                WHEN (
                    CASE
                        WHEN COALESCE(ot.total_suspended_wells, 0) > 0
                        THEN rp.suspended_well_count * 1.0 / ot.total_suspended_wells
                        ELSE 0
                    END
                ) >= 0.6 THEN 10.0
                ELSE 0
              END
            - CASE WHEN ot.package_count >= 4 THEN 8.0 ELSE 0 END
        )
        )::numeric,
        2
    ) AS footprint_score
FROM operator_totals ot
JOIN ranked_packages rp
    ON rp.operator_id = ot.operator_id
   AND rp.package_rank = 1;
