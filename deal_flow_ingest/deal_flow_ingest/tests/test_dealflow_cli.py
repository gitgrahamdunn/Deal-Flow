from __future__ import annotations

import sys

from deal_flow_ingest import dealflow
from deal_flow_ingest.config import get_default_config_path


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
        lambda: dealflow.argparse.Namespace(command="app", port=8765, host="0.0.0.0"),
    )
    monkeypatch.setattr(dealflow, "_configure_logging", lambda: None)
    monkeypatch.setattr(
        dealflow,
        "launch_app",
        lambda port, host: called.update({"port": port, "host": host}) or 0,
    )

    assert dealflow.main() == 0
    assert called == {"port": 8765, "host": "0.0.0.0"}


def test_parse_args_uses_package_default_config(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["dealflow", "ingest"])

    args = dealflow.parse_args()

    assert args.config == get_default_config_path()


def test_launch_app_reuses_existing_port(monkeypatch, capsys):
    monkeypatch.setattr(dealflow, "_is_port_open", lambda host, port: True)

    called = {"subprocess": 0}
    monkeypatch.setattr(dealflow.subprocess, "call", lambda cmd: called.__setitem__("subprocess", called["subprocess"] + 1) or 0)

    assert dealflow.launch_app(8443, "127.0.0.1") == 0
    assert called["subprocess"] == 0
    assert "GUI already running at http://127.0.0.1:8443" in capsys.readouterr().out


def test_launch_app_probes_loopback_for_wildcard_host(monkeypatch, capsys):
    called = {"probe": None, "subprocess": 0}

    monkeypatch.setattr(
        dealflow,
        "_is_port_open",
        lambda host, port: called.__setitem__("probe", (host, port)) or True,
    )
    monkeypatch.setattr(
        dealflow.subprocess,
        "call",
        lambda cmd: called.__setitem__("subprocess", called["subprocess"] + 1) or 0,
    )

    assert dealflow.launch_app(8443, "0.0.0.0") == 0
    assert called["probe"] == ("127.0.0.1", 8443)
    assert called["subprocess"] == 0
    assert "GUI already running at http://0.0.0.0:8443" in capsys.readouterr().out


def test_dealflow_funnel_dispatches(monkeypatch):
    called = {}

    monkeypatch.setattr(
        dealflow,
        "parse_args",
        lambda: dealflow.argparse.Namespace(
            command="funnel",
            port=8502,
            https_port=8443,
            bg=True,
            yes=True,
            status=False,
            reset=False,
        ),
    )
    monkeypatch.setattr(dealflow, "_configure_logging", lambda: None)
    monkeypatch.setattr(dealflow, "run_funnel", lambda args: called.update(vars(args)) or 0)

    assert dealflow.main() == 0
    assert called == {
        "command": "funnel",
        "port": 8502,
        "https_port": 8443,
        "bg": True,
        "yes": True,
        "status": False,
        "reset": False,
    }
