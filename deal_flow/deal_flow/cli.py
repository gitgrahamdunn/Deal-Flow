from __future__ import annotations

import argparse
import logging
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from deal_flow.config import AppConfig, load_config
from deal_flow.excel.writer import write_workbook
from deal_flow.io.downloader import Downloader
from deal_flow.sources.aer import load_aer_wells
from deal_flow.sources.other_free import load_other_free
from deal_flow.sources.petrinex import load_petrinex
from deal_flow.transform.joins import attach_operator_from_wells
from deal_flow.transform.metrics import compute_operator_metrics
from deal_flow.transform.normalize import normalize_production, normalize_wells
from deal_flow.utils.dates import default_window
from deal_flow.utils.licensing import terms_rows

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
LOGGER = logging.getLogger("deal_flow")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="deal_flow")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("--start", type=str)
    run.add_argument("--end", type=str)
    run.add_argument("--out", type=Path, default=Path("./output"))
    run.add_argument("--refresh", action="store_true")
    run.add_argument("--geo", type=str, default=None)
    run.add_argument("--bbox", type=str, default=None)
    run.add_argument("--top", type=int, default=50)
    run.add_argument("--config", type=Path, default=Path("configs/sources.yaml"))
    run.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _resolve_dates(start_raw: str | None, end_raw: str | None) -> tuple[date, date]:
    end = datetime.strptime(end_raw, "%Y-%m-%d").date() if end_raw else date.today()
    if start_raw:
        start = datetime.strptime(start_raw, "%Y-%m-%d").date()
    else:
        start, _ = default_window(end)
    return start, end


def run_pipeline(args: argparse.Namespace) -> Path:
    config: AppConfig = load_config(args.config)
    start_date, end_date = _resolve_dates(args.start, args.end)
    LOGGER.info("Run started: start=%s end=%s dry_run=%s", start_date, end_date, args.dry_run)

    downloader = None if args.dry_run else Downloader(Path("data/raw"), refresh=args.refresh)
    dry_run_dir = Path("tests/data") if args.dry_run else None

    prod_frames: list[pd.DataFrame] = []
    wells = pd.DataFrame()

    if config.sources["petrinex"].enabled:
        try:
            p = load_petrinex(config.sources["petrinex"], downloader, dry_run_dir)
            LOGGER.info("petrinex rows=%s", len(p))
            prod_frames.append(p)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Skipping petrinex due to error: %s", exc)
    if config.sources["aer"].enabled:
        try:
            wells = load_aer_wells(config.sources["aer"], downloader, dry_run_dir)
            LOGGER.info("aer wells rows=%s", len(wells))
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Skipping AER wells due to error: %s", exc)

    if config.sources.get("other_free") and config.sources["other_free"].enabled:
        try:
            other = load_other_free(config.sources["other_free"], downloader)
            LOGGER.info("other_free rows=%s", len(other))
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Skipping other_free due to error: %s", exc)

    production = pd.concat(prod_frames, ignore_index=True) if prod_frames else pd.DataFrame(
        columns=["month", "operator", "oil_bbl", "gas_mcf", "water_bbl", "facility_id", "well_id", "source", "basis_level"]
    )
    production = normalize_production(production)
    if not wells.empty:
        wells = normalize_wells(wells)
    production = attach_operator_from_wells(production, wells)
    production = production[(production["month"] >= pd.Timestamp(start_date)) & (production["month"] <= pd.Timestamp(end_date))]

    summary = compute_operator_metrics(production, end_date)
    if args.top:
        summary = summary.head(args.top)

    operator_detail = summary.copy()
    operator_detail["notes"] = "Licensee/operator attribution from public datasets"
    dq = pd.DataFrame(
        [
            {"table": "production", "rows": len(production), "missing_operator": int(production["operator"].isna().sum())},
            {"table": "wells", "rows": len(wells), "missing_operator": int(wells["operator"].isna().sum()) if not wells.empty else 0},
        ]
    )
    readme_rows = [
        {"section": "Generated", "details": datetime.utcnow().isoformat()},
        {"section": "Caveat", "details": "MVP title holder is licensee/operator attribution."},
        {"section": "Basis", "details": "Production basis can be well or facility depending on source rows."},
    ]
    for row in terms_rows(config):
        readme_rows.append({"section": f"Terms - {row['source']}", "details": f"{row['public_page']} | {row['usage_notes']}"})

    output = write_workbook(args.out, readme_rows, summary, operator_detail, production, wells, dq)
    processed = Path("data/processed")
    processed.mkdir(parents=True, exist_ok=True)
    summary.to_csv(processed / "summary.csv", index=False)
    LOGGER.info("Workbook generated at %s", output)
    return output


def main() -> None:
    args = parse_args()
    if args.command == "run":
        run_pipeline(args)


if __name__ == "__main__":
    main()
