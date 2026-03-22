import os
import sqlite3
from argparse import Namespace
from pathlib import Path

import yaml

from deal_flow_ingest.apply_saved_sql import apply_saved_sql
from deal_flow_ingest.cli import run_ingestion
from deal_flow_ingest.config import get_default_config_path, load_config
from deal_flow_ingest.sources.ami import (
    load_ami_crown_clients_frame,
    load_ami_crown_dispositions_frame,
    load_ami_crown_land_keys_frame,
    load_ami_crown_participants_frame,
)
from deal_flow_ingest.transform.normalize import normalize_ats_location


SAMPLE_DIR = Path(__file__).resolve().parents[1] / "sample_data"


def _fetch_one(db_path: Path, query: str):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(query).fetchone()
    finally:
        conn.close()


def test_parse_ami_crown_frames() -> None:
    dispositions = load_ami_crown_dispositions_frame(SAMPLE_DIR / "ami_crown_dispositions_sample.csv")
    clients = load_ami_crown_clients_frame(SAMPLE_DIR / "ami_crown_clients_sample.csv")
    land = load_ami_crown_land_keys_frame(SAMPLE_DIR / "ami_crown_land_keys_sample.csv")
    participants = load_ami_crown_participants_frame(SAMPLE_DIR / "ami_crown_participants_sample.csv")

    assert dispositions.iloc[0]["disposition_id"] == "DISP-1001"
    assert dispositions.iloc[0]["agreement_no"] == "AG-1001"
    assert clients.iloc[0]["client_id"] == "100001"
    assert clients.iloc[0]["client_name_norm"] == "ALPHA ENERGY LTD"
    assert land.iloc[0]["lsd"] == "10"
    assert int(land.iloc[0]["township"]) == 50
    assert participants.iloc[0]["role_type"] == "holder"
    assert float(participants.iloc[0]["interest_pct"]) == 100.0


def test_normalize_ats_location() -> None:
    normalized = normalize_ats_location(lsd="1", section="02", township="050", range_="08", meridian="4")

    assert normalized == {"lsd": "01", "section": 2, "township": 50, "range": 8, "meridian": 4}


def test_apply_saved_sql_builds_crown_tenure_view(tmp_path: Path) -> None:
    db_path = tmp_path / "deal_flow_crown.db"
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"

    cfg = load_config(get_default_config_path())
    for source_key in [
        "ami_crown_dispositions",
        "ami_crown_clients",
        "ami_crown_land_keys",
        "ami_crown_participants",
    ]:
        cfg.sources[source_key].enabled = True

    config_path = tmp_path / "sources.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "sources": {
                    key: value.model_dump(exclude_none=True)
                    for key, value in cfg.sources.items()
                }
            }
        ),
        encoding="utf-8",
    )

    args = Namespace(
        start=None,
        end="2025-03-31",
        refresh=False,
        dry_run=True,
        config=str(config_path),
    )

    assert run_ingestion(args) == 0
    assert apply_saved_sql() == 0

    tenure = _fetch_one(
        db_path,
        "select asset_type, asset_id, disposition_id, agreement_no, holder_names, match_level "
        "from asset_registry_crown_tenure where disposition_id = 'DISP-1001' order by asset_type, asset_id limit 1",
    )
    assert tenure is not None
    assert tenure[0] in {"well", "facility"}
    assert tenure[2] == "DISP-1001"
    assert tenure[3] == "AG-1001"
    assert "ALPHA ENERGY LTD" in tenure[4]
    assert tenure[5] in {"lsd_exact", "section"}
