SELECT
    operator_id,
    operator,
    opportunity_score,
    restart_upside_bpd_est,
    restart_candidates_count,
    suspended_wells_count,
    liability_ratio,
    distress_score,
    avg_oil_bpd_30d,
    avg_oil_bpd_365d
FROM deal_flow_opportunities
ORDER BY opportunity_score DESC, operator;
