from argparse import Namespace

from deal_flow_ingest.services.pipeline import run_source_diagnostics


def test_run_source_diagnostics_dry_run_reports_required_source():
    args = Namespace(
        refresh=False,
        dry_run=True,
        config="deal_flow_ingest/deal_flow_ingest/configs/sources.yaml",
    )

    status, report = run_source_diagnostics(args)

    assert status == 0
    assert report["required_failures"] == 0
    aer_summary = next(item for item in report["source_summaries"] if item["source"] == "aer_st37")
    assert aer_summary["required_for_live"] is True
    assert aer_summary["maturity"] == "live_working"
    assert aer_summary["sample_file"] == "wells_sample.csv"
