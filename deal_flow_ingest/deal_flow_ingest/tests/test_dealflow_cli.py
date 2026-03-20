from __future__ import annotations

from deal_flow_ingest import dealflow


def test_dealflow_top50_dispatches_with_limit_50(monkeypatch):
    called = {}

    monkeypatch.setattr(
        dealflow,
        "parse_args",
        lambda: dealflow.argparse.Namespace(command="top50", min_score=12.5, output="out.csv"),
    )
    monkeypatch.setattr(dealflow, "_configure_logging", lambda: None)

    def _fake_export(args):
        called["min_score"] = args.min_score
        called["limit"] = args.limit
        called["output"] = args.output
        return 0

    monkeypatch.setattr(dealflow, "export_seller_theses", _fake_export)

    assert dealflow.main() == 0
    assert called == {"min_score": 12.5, "limit": 50, "output": "out.csv"}


def test_dealflow_ingest_runs_sql_after_success(monkeypatch):
    called = {"run": 0, "sql": 0}

    monkeypatch.setattr(
        dealflow,
        "parse_args",
        lambda: dealflow.argparse.Namespace(
            command="ingest",
            start=None,
            end=None,
            refresh=False,
            dry_run=False,
            skip_sql=False,
            config="cfg.yaml",
        ),
    )
    monkeypatch.setattr(dealflow, "_configure_logging", lambda: None)
    monkeypatch.setattr(dealflow, "run_ingestion", lambda args: called.__setitem__("run", called["run"] + 1) or 0)
    monkeypatch.setattr(dealflow, "apply_saved_sql", lambda: called.__setitem__("sql", called["sql"] + 1) or 0)

    assert dealflow.main() == 0
    assert called == {"run": 1, "sql": 1}


def test_dealflow_top100_dispatches_with_limit_100(monkeypatch):
    called = {}

    monkeypatch.setattr(
        dealflow,
        "parse_args",
        lambda: dealflow.argparse.Namespace(command="top100", min_score=5.0, output="top100.csv"),
    )
    monkeypatch.setattr(dealflow, "_configure_logging", lambda: None)

    def _fake_export(args):
        called["min_score"] = args.min_score
        called["limit"] = args.limit
        called["output"] = args.output
        return 0

    monkeypatch.setattr(dealflow, "export_seller_theses", _fake_export)

    assert dealflow.main() == 0
    assert called == {"min_score": 5.0, "limit": 100, "output": "top100.csv"}


def test_dealflow_doctor_dispatches_to_source_checks(monkeypatch):
    called = {}

    monkeypatch.setattr(
        dealflow,
        "parse_args",
        lambda: dealflow.argparse.Namespace(
            command="doctor",
            refresh=True,
            dry_run=False,
            config="cfg.yaml",
        ),
    )
    monkeypatch.setattr(dealflow, "_configure_logging", lambda: None)

    def _fake_check(args):
        called["refresh"] = args.refresh
        called["dry_run"] = args.dry_run
        called["config"] = args.config
        return 0

    monkeypatch.setattr(dealflow, "check_sources", _fake_check)

    assert dealflow.main() == 0
    assert called == {"refresh": True, "dry_run": False, "config": "cfg.yaml"}


def test_dealflow_lowprod_sorts_by_production(monkeypatch):
    called = {}

    monkeypatch.setattr(
        dealflow,
        "parse_args",
        lambda: dealflow.argparse.Namespace(
            command="lowprod",
            min_score=10.0,
            limit=25,
            max_prod=15.0,
            output="lowprod.csv",
        ),
    )
    monkeypatch.setattr(dealflow, "_configure_logging", lambda: None)

    def _fake_export(args):
        called["sort_by"] = args.sort_by
        called["ascending"] = args.ascending
        called["max_avg_oil_bpd_30d"] = args.max_avg_oil_bpd_30d
        called["limit"] = args.limit
        return 0

    monkeypatch.setattr(dealflow, "export_seller_theses", _fake_export)

    assert dealflow.main() == 0
    assert called == {
        "sort_by": "avg_oil_bpd_30d",
        "ascending": True,
        "max_avg_oil_bpd_30d": 15.0,
        "limit": 25,
    }


def test_dealflow_ui_lowprod_mode(monkeypatch):
    answers = iter(["l", "12", "q"])
    called = {}

    monkeypatch.setattr("builtins.input", lambda prompt="": next(answers))
    monkeypatch.setattr(dealflow, "_clear_screen", lambda: None)

    def _fake_fetch(args):
        called["sort_by"] = args.sort_by
        called["ascending"] = args.ascending
        called["max_avg_oil_bpd_30d"] = args.max_avg_oil_bpd_30d
        return dealflow.pd.DataFrame()

    monkeypatch.setattr(dealflow, "get_seller_theses_frame", _fake_fetch)

    assert dealflow.run_ui() == 0
    assert called == {
        "sort_by": "avg_oil_bpd_30d",
        "ascending": True,
        "max_avg_oil_bpd_30d": 12.0,
    }


def test_dealflow_app_dispatches(monkeypatch):
    called = {}

    monkeypatch.setattr(
        dealflow,
        "parse_args",
        lambda: dealflow.argparse.Namespace(command="app", port=8765),
    )
    monkeypatch.setattr(dealflow, "_configure_logging", lambda: None)
    monkeypatch.setattr(dealflow, "launch_app", lambda port: called.__setitem__("port", port) or 0)

    assert dealflow.main() == 0
    assert called == {"port": 8765}
