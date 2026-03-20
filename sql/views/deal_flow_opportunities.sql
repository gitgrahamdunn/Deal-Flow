CREATE VIEW deal_flow_opportunities AS
SELECT
    t.operator_id,
    t.operator,
    t.avg_oil_bpd_30d,
    t.avg_oil_bpd_365d,
    t.restart_upside_bpd_est,
    t.suspended_wells_count,
    t.restart_candidates_count,
    t.inactive_wells,
    t.active_wells,
    t.liability_ratio,
    t.distress_score,
    ROUND(
        MAX(
            0,
            MIN(
                100,
                COALESCE(t.restart_upside_bpd_est, 0) * 0.6
                + COALESCE(t.restart_candidates_count, 0) * 6
                + COALESCE(t.suspended_wells_count, 0) * 1.5
                + COALESCE(t.distress_score, 0) * 0.5
                + CASE
                    WHEN COALESCE(t.liability_ratio, 999) < 1 THEN 15
                    WHEN COALESCE(t.liability_ratio, 999) < 2 THEN 6
                    ELSE 0
                  END
                - COALESCE(t.avg_oil_bpd_30d, 0) * 0.01
            )
        ),
        2
    ) AS opportunity_score
FROM deal_flow_targets t;
