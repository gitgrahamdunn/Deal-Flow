from __future__ import annotations

import argparse
import logging
import os
import shutil
import socket
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from deal_flow_ingest.apply_saved_sql import apply_saved_sql
from deal_flow_ingest.cli import (
    check_sources,
    export_package_candidates,
    export_seller_theses,
    get_package_candidates_frame,
    get_seller_theses_frame,
    reset_database,
    run_ingestion,
)
from deal_flow_ingest.config import get_default_config_path


def _configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="dealflow", description="Operator-facing Deal Flow CLI")
    sub = parser.add_subparsers(dest="command", required=True)
    default_config_path = get_default_config_path()

    ingest = sub.add_parser("ingest", help="Refresh the warehouse and rebuild curated SQL views")
    ingest.add_argument("--start", type=str)
    ingest.add_argument("--end", type=str)
    ingest.add_argument("--refresh", action="store_true")
    ingest.add_argument("--dry-run", action="store_true")
    ingest.add_argument("--skip-sql", action="store_true")
    ingest.add_argument("--config", default=default_config_path)

    refresh = sub.add_parser("refresh", help="Alias for ingest")
    refresh.add_argument("--start", type=str)
    refresh.add_argument("--end", type=str)
    refresh.add_argument("--refresh", action="store_true")
    refresh.add_argument("--dry-run", action="store_true")
    refresh.add_argument("--skip-sql", action="store_true")
    refresh.add_argument("--config", default=default_config_path)

    sources = sub.add_parser("sources", help="Check source availability")
    sources.add_argument("--refresh", action="store_true")
    sources.add_argument("--dry-run", action="store_true")
    sources.add_argument("--config", default=default_config_path)

    doctor = sub.add_parser("doctor", help="Alias for sources")
    doctor.add_argument("--refresh", action="store_true")
    doctor.add_argument("--dry-run", action="store_true")
    doctor.add_argument("--config", default=default_config_path)

    top50 = sub.add_parser("top50", help="Export the top 50 seller theses")
    top50.add_argument("--min-score", type=float, default=0.0)
    top50.add_argument("--output", type=str, default="data/exports/top50_seller_theses.csv")

    top100 = sub.add_parser("top100", help="Export the top 100 seller theses")
    top100.add_argument("--min-score", type=float, default=0.0)
    top100.add_argument("--output", type=str, default="data/exports/top100_seller_theses.csv")

    lowprod = sub.add_parser("lowprod", help="Export the lowest-producing seller theses")
    lowprod.add_argument("--min-score", type=float, default=0.0)
    lowprod.add_argument("--limit", type=int, default=100)
    lowprod.add_argument("--max-prod", type=float)
    lowprod.add_argument("--output", type=str, default="data/exports/low_production_targets.csv")

    packages = sub.add_parser("packages", help="Export package candidates")
    packages.add_argument("--min-score", type=float, default=0.0)
    packages.add_argument("--limit", type=int, default=100)
    packages.add_argument("--output", type=str, default="data/exports/package_candidates.csv")

    theses = sub.add_parser("theses", help="Export seller theses")
    theses.add_argument("--min-score", type=float, default=0.0)
    theses.add_argument("--limit", type=int, default=100)
    theses.add_argument("--sort-by", type=str, default="thesis_score")
    theses.add_argument("--ascending", action="store_true")
    theses.add_argument("--max-prod", type=float)
    theses.add_argument("--output", type=str, default="data/exports/seller_theses.csv")

    sub.add_parser("ui", help="Interactive export helper")
    web = sub.add_parser("web", help="Launch the new self-hosted web app")
    web.add_argument("--port", type=int, default=8000)
    web.add_argument("--host", type=str, default="127.0.0.1")
    app = sub.add_parser("app", help="Launch the local web GUI")
    app.add_argument("--port", type=int, default=8443)
    app.add_argument("--host", type=str, default="127.0.0.1")
    gui = sub.add_parser("gui", help="Alias for app")
    gui.add_argument("--port", type=int, default=8443)
    gui.add_argument("--host", type=str, default="127.0.0.1")
    funnel = sub.add_parser("funnel", help="Expose the GUI through Tailscale Funnel")
    funnel.add_argument("--port", type=int, default=8443)
    funnel.add_argument("--https-port", type=int, default=8443)
    funnel.add_argument("--bg", action="store_true")
    funnel.add_argument("--yes", action="store_true")
    funnel.add_argument("--status", action="store_true")
    funnel.add_argument("--reset", action="store_true")

    build_sql = sub.add_parser("build-sql", help="Rebuild curated SQL views")

    reset = sub.add_parser("reset", help="Reset the local database")
    reset.add_argument("--force", action="store_true")
    reset.add_argument("--include-cache", action="store_true")

    return parser.parse_args()


def main() -> int:
    _configure_logging()
    args = parse_args()

    if args.command in {"ingest", "refresh"}:
        run_args = SimpleNamespace(
            start=args.start,
            end=args.end,
            refresh=args.refresh,
            dry_run=args.dry_run,
            config=args.config,
        )
        status = run_ingestion(run_args)
        if status != 0:
            return status
        if args.dry_run or args.skip_sql:
            return 0
        return apply_saved_sql()

    if args.command in {"sources", "doctor"}:
        check_args = SimpleNamespace(refresh=args.refresh, dry_run=args.dry_run, config=args.config)
        return check_sources(check_args)

    if args.command == "top50":
        export_args = SimpleNamespace(
            min_score=args.min_score,
            limit=50,
            output=args.output,
            sort_by="thesis_score",
            ascending=False,
            max_avg_oil_bpd_30d=None,
        )
        return export_seller_theses(export_args)

    if args.command == "top100":
        export_args = SimpleNamespace(
            min_score=args.min_score,
            limit=100,
            output=args.output,
            sort_by="thesis_score",
            ascending=False,
            max_avg_oil_bpd_30d=None,
        )
        return export_seller_theses(export_args)

    if args.command == "lowprod":
        export_args = SimpleNamespace(
            min_score=args.min_score,
            limit=args.limit,
            output=args.output,
            sort_by="avg_oil_bpd_30d",
            ascending=True,
            max_avg_oil_bpd_30d=args.max_prod,
        )
        return export_seller_theses(export_args)

    if args.command == "packages":
        export_args = SimpleNamespace(min_score=args.min_score, limit=args.limit, output=args.output)
        return export_package_candidates(export_args)

    if args.command == "theses":
        export_args = SimpleNamespace(
            min_score=args.min_score,
            limit=args.limit,
            output=args.output,
            sort_by=args.sort_by,
            ascending=args.ascending,
            max_avg_oil_bpd_30d=args.max_prod,
        )
        return export_seller_theses(export_args)

    if args.command == "ui":
        return run_ui()

    if args.command == "web":
        return launch_web(args.port, args.host)

    if args.command in {"app", "gui"}:
        return launch_app(args.port, args.host)

    if args.command == "funnel":
        return run_funnel(args)

    if args.command == "build-sql":
        return apply_saved_sql()

    if args.command == "reset":
        reset_args = SimpleNamespace(force=args.force, include_cache=args.include_cache)
        return reset_database(reset_args)

    return 1


def _prompt(prompt: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{prompt}{suffix}: ").strip()
    return value or default


def _clear_screen() -> None:
    os.system("clear")


def _preview_frame(frame: pd.DataFrame, columns: list[str], limit: int = 20) -> str:
    if frame.empty:
        return "No rows."
    width = shutil.get_terminal_size((140, 40)).columns
    display = frame[[col for col in columns if col in frame.columns]].head(limit).copy()
    with pd.option_context(
        "display.max_columns",
        None,
        "display.width",
        width,
        "display.max_colwidth",
        28,
    ):
        return display.to_string(index=False)


def _build_theses_args(
    *,
    min_score: float,
    limit: int,
    sort_by: str,
    ascending: bool,
    max_prod: float | None,
    output: str,
) -> SimpleNamespace:
    return SimpleNamespace(
        min_score=min_score,
        limit=limit,
        output=output,
        sort_by=sort_by,
        ascending=ascending,
        max_avg_oil_bpd_30d=max_prod,
    )


def _render_dashboard(
    title: str,
    subtitle: str,
    frame: pd.DataFrame,
    columns: list[str],
    last_export: str | None,
) -> None:
    _clear_screen()
    print(f"Deal Flow Dashboard: {title}")
    print(subtitle)
    if last_export:
        print(f"last export: {last_export}")
    print("")
    print(_preview_frame(frame, columns))
    print("")
    print("Actions")
    print("  r = refresh data")
    print("  d = source doctor")
    print("  t = top theses")
    print("  l = low production targets")
    print("  p = package candidates")
    print("  c = custom theses")
    print("  e = export current view")
    print("  q = quit")


def run_ui() -> int:
    current_mode = "theses"
    current_args = _build_theses_args(
        min_score=0.0,
        limit=50,
        sort_by="thesis_score",
        ascending=False,
        max_prod=None,
        output="data/exports/top50_seller_theses.csv",
    )
    current_frame = get_seller_theses_frame(current_args)
    last_export: str | None = None

    while True:
        if current_mode == "packages":
            title = "Package Candidates"
            subtitle = "Sorted by package score"
            columns = [
                "operator",
                "area_key",
                "suspended_well_count",
                "high_priority_well_count",
                "linked_facility_count",
                "estimated_restart_upside_bpd",
                "package_score",
            ]
        else:
            title = "Seller Theses" if current_mode == "theses" else "Low Production Targets"
            subtitle = (
                f"sort={current_args.sort_by} ascending={current_args.ascending} "
                f"min_score={current_args.min_score} max_prod={current_args.max_avg_oil_bpd_30d}"
            )
            columns = [
                "operator",
                "avg_oil_bpd_30d",
                "avg_oil_bpd_365d",
                "thesis_priority",
                "thesis_score",
                "seller_score",
                "opportunity_score",
                "package_count",
            ]

        _render_dashboard(title, subtitle, current_frame, columns, last_export)
        action = _prompt("Choice", "q").lower()

        if action == "q":
            return 0

        if action == "r":
            start = _prompt("Start date YYYY-MM-DD (blank = default)", "")
            end = _prompt("End date YYYY-MM-DD (blank = default)", "")
            status = run_ingestion(
                SimpleNamespace(
                    start=start or None,
                    end=end or None,
                    refresh=False,
                    dry_run=False,
                    config=get_default_config_path(),
                )
            )
            if status == 0:
                apply_saved_sql()
                if current_mode == "packages":
                    current_frame = get_package_candidates_frame(SimpleNamespace(min_score=0.0, limit=100, output=""))
                else:
                    current_frame = get_seller_theses_frame(current_args)
        if action == "d":
            check_sources(
                SimpleNamespace(
                    refresh=False,
                    dry_run=False,
                    config=get_default_config_path(),
                )
            )
            input("Press Enter to continue...")
            continue

        if action == "t":
            current_mode = "theses"
            current_args = _build_theses_args(
                min_score=0.0,
                limit=50,
                sort_by="thesis_score",
                ascending=False,
                max_prod=None,
                output="data/exports/top50_seller_theses.csv",
            )
            current_frame = get_seller_theses_frame(current_args)
            continue

        if action == "l":
            max_prod_raw = _prompt("Max avg oil bpd 30d", "15")
            current_mode = "lowprod"
            current_args = _build_theses_args(
                min_score=0.0,
                limit=100,
                sort_by="avg_oil_bpd_30d",
                ascending=True,
                max_prod=float(max_prod_raw) if max_prod_raw else None,
                output="data/exports/low_production_targets.csv",
            )
            current_frame = get_seller_theses_frame(current_args)
            continue

        if action == "p":
            min_score = float(_prompt("Minimum package score", "0"))
            limit = int(_prompt("Limit", "100"))
            current_mode = "packages"
            current_args = SimpleNamespace(min_score=min_score, limit=limit, output="data/exports/package_candidates.csv")
            current_frame = get_package_candidates_frame(current_args)
            continue

        if action == "c":
            min_score = float(_prompt("Minimum thesis score", "0"))
            limit = int(_prompt("Limit", "100"))
            sort_by = _prompt("Sort by", "thesis_score")
            ascending = _prompt("Ascending? (y/n)", "n").lower().startswith("y")
            max_prod_raw = _prompt("Max avg oil bpd 30d (blank for none)", "")
            current_mode = "theses"
            current_args = _build_theses_args(
                min_score=min_score,
                limit=limit,
                sort_by=sort_by,
                ascending=ascending,
                max_prod=float(max_prod_raw) if max_prod_raw else None,
                output="data/exports/seller_theses.csv",
            )
            current_frame = get_seller_theses_frame(current_args)
            continue

        if action == "e":
            output = _prompt(
                "Output CSV",
                current_args.output if hasattr(current_args, "output") else "data/exports/export.csv",
            )
            current_args.output = output
            if current_mode == "packages":
                export_package_candidates(current_args)
            else:
                export_seller_theses(current_args)
            last_export = output
            input("Press Enter to continue...")
            continue


def launch_app(port: int, host: str = "127.0.0.1") -> int:
    probe_host = _app_probe_host(host)
    if _is_port_open(probe_host, port):
        print(f"GUI already running at http://{host}:{port}")
        return 0

    app_path = Path(__file__).resolve().parent / "app.py"
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_path),
        "--server.address",
        str(host),
        "--server.port",
        str(port),
        "--server.headless",
        "true",
    ]
    print(f"Launching GUI at http://{host}:{port}")
    return subprocess.call(cmd)


def launch_web(port: int, host: str = "127.0.0.1") -> int:
    probe_host = _app_probe_host(host)
    if _is_port_open(probe_host, port):
        print(f"Web app already running at http://{host}:{port}")
        return 0

    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "deal_flow_ingest.web.api:app",
        "--host",
        str(host),
        "--port",
        str(port),
    ]
    print(f"Launching web app at http://{host}:{port}")
    return subprocess.call(cmd)


def _is_port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _app_probe_host(host: str) -> str:
    if host in {"0.0.0.0", "::"}:
        return "127.0.0.1"
    return host


def run_funnel(args: argparse.Namespace) -> int:
    if args.status:
        return subprocess.call(["tailscale", "funnel", "status"])
    if args.reset:
        return subprocess.call(["tailscale", "funnel", "reset"])

    cmd = ["tailscale", "funnel", f"--https={args.https_port}"]
    if args.bg:
        cmd.append("--bg")
    if args.yes:
        cmd.append("--yes")
    cmd.append(f"http://127.0.0.1:{args.port}")
    print(
        f"Exposing Deal Flow GUI through Tailscale Funnel at external port {args.https_port} "
        f"to local port {args.port}"
    )
    return subprocess.call(cmd)
