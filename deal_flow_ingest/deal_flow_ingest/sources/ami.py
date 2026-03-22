from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from deal_flow_ingest.config import SourcePayload
from deal_flow_ingest.io.downloader import Downloader
from deal_flow_ingest.transform.normalize import normalize_ats_location, normalize_lsd, normalize_operator_name

LOGGER = logging.getLogger(__name__)


def load_ami_local_source(_downloader: Downloader, source: SourcePayload, _refresh: bool) -> pd.DataFrame:
    local_live_file = (source.local_live_file or "").strip()
    if not local_live_file:
        LOGGER.info("AMI source %s has no local_live_file configured; skipping rows", source.key)
        return pd.DataFrame()

    local_path = Path(local_live_file).expanduser()
    if not local_path.exists() or not local_path.is_file():
        LOGGER.warning("AMI local file does not exist for %s: %s", source.key, local_path)
        return pd.DataFrame()

    parsed = _parse_ami_source(local_path, source.parser_name)
    if not parsed.empty:
        parsed["source"] = source.key
    return parsed


def load_ami_sample(path: Path, parser_name: str, source_key: str) -> pd.DataFrame:
    parsed = _parse_ami_source(path, parser_name)
    if not parsed.empty:
        parsed["source"] = source_key
    return parsed


def _parse_ami_source(path: Path, parser_name: str) -> pd.DataFrame:
    if parser_name == "ami_crown_dispositions":
        return load_ami_crown_dispositions_frame(path)
    if parser_name == "ami_crown_clients":
        return load_ami_crown_clients_frame(path)
    if parser_name == "ami_crown_land_keys":
        return load_ami_crown_land_keys_frame(path)
    if parser_name == "ami_crown_participants":
        return load_ami_crown_participants_frame(path)
    raise ValueError(f"Unsupported AMI parser: {parser_name}")


def _coerce_tabular_frame(source: pd.DataFrame | Path) -> pd.DataFrame:
    if isinstance(source, pd.DataFrame):
        return source.copy()
    suffix = source.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(source)
    if suffix in {".txt", ".tsv"}:
        return pd.read_csv(source, sep=None, engine="python")
    return pd.read_csv(source)


def _normalize_column_name(value: object) -> str:
    return str(value).strip().lower().replace(" ", "_").replace("-", "_")


def _column_as_series(df: pd.DataFrame, cols: dict[str, str], aliases: tuple[str, ...], default) -> pd.Series:
    for alias in aliases:
        column = cols.get(alias)
        if column is not None:
            return df[column]
    return pd.Series([default] * len(df), index=df.index)


def load_ami_crown_dispositions_frame(source: pd.DataFrame | Path) -> pd.DataFrame:
    df = _coerce_tabular_frame(source)
    if df.empty:
        return pd.DataFrame()

    cols = {_normalize_column_name(column): column for column in df.columns}
    out = pd.DataFrame(index=df.index)
    out["disposition_id"] = _column_as_series(
        df,
        cols,
        ("disposition_id", "agreement_id", "agreement_no", "agreement_number", "contract_no", "contract_number"),
        "",
    )
    out["agreement_no"] = _column_as_series(
        df,
        cols,
        ("agreement_no", "agreement_number", "contract_no", "contract_number", "disposition_no"),
        "",
    )
    out["disposition_type"] = _column_as_series(df, cols, ("disposition_type", "agreement_type", "contract_type"), "")
    out["disposition_status"] = _column_as_series(df, cols, ("disposition_status", "agreement_status", "status"), "")
    out["effective_from"] = _column_as_series(df, cols, ("effective_from", "effective_date", "start_date"), None)
    out["effective_to"] = _column_as_series(df, cols, ("effective_to", "expiry_date", "end_date", "termination_date"), None)
    out["source"] = _column_as_series(df, cols, ("source",), None)

    out["disposition_id"] = out["disposition_id"].fillna("").astype(str).str.strip()
    out["agreement_no"] = out["agreement_no"].fillna("").astype(str).str.strip()
    out["disposition_id"] = out["disposition_id"].where(out["disposition_id"] != "", out["agreement_no"])
    out["effective_from"] = pd.to_datetime(out["effective_from"], errors="coerce").dt.date
    out["effective_to"] = pd.to_datetime(out["effective_to"], errors="coerce").dt.date
    out = out[out["disposition_id"] != ""].copy()
    return out


def load_ami_crown_clients_frame(source: pd.DataFrame | Path) -> pd.DataFrame:
    df = _coerce_tabular_frame(source)
    if df.empty:
        return pd.DataFrame()

    cols = {_normalize_column_name(column): column for column in df.columns}
    out = pd.DataFrame(index=df.index)
    out["client_id"] = _column_as_series(df, cols, ("client_id", "owner_id", "client_no", "client_number"), "")
    out["client_name_raw"] = _column_as_series(df, cols, ("client_name", "owner_name", "name"), "")
    out["client_name_norm"] = out["client_name_raw"].fillna("").astype(str).map(normalize_operator_name).replace({"": None})
    out["source"] = _column_as_series(df, cols, ("source",), None)
    out["client_id"] = out["client_id"].fillna("").astype(str).str.strip()
    return out[out["client_id"] != ""].copy()


def load_ami_crown_land_keys_frame(source: pd.DataFrame | Path) -> pd.DataFrame:
    df = _coerce_tabular_frame(source)
    if df.empty:
        return pd.DataFrame()

    cols = {_normalize_column_name(column): column for column in df.columns}
    out = pd.DataFrame(index=df.index)
    out["disposition_id"] = _column_as_series(
        df,
        cols,
        ("disposition_id", "agreement_id", "agreement_no", "agreement_number", "contract_no", "contract_number"),
        "",
    )
    out["tract_no"] = _column_as_series(df, cols, ("tract_no", "tract_number", "tract"), "")

    lsd = _column_as_series(df, cols, ("lsd", "legal_subdivision"), None)
    section = _column_as_series(df, cols, ("section", "sec"), None)
    township = _column_as_series(df, cols, ("township", "twp"), None)
    range_ = _column_as_series(df, cols, ("range", "rng"), None)
    meridian = _column_as_series(df, cols, ("meridian", "m"), None)

    normalized = [
        normalize_ats_location(lsd=lsd_value, section=section_value, township=township_value, range_=range_value, meridian=meridian_value)
        for lsd_value, section_value, township_value, range_value, meridian_value in zip(
            lsd, section, township, range_, meridian, strict=False
        )
    ]
    normalized_df = pd.DataFrame(normalized, index=df.index)
    out = pd.concat([out, normalized_df], axis=1)
    out["source"] = _column_as_series(df, cols, ("source",), None)
    out["disposition_id"] = out["disposition_id"].fillna("").astype(str).str.strip()
    out["tract_no"] = out["tract_no"].fillna("").astype(str).str.strip().replace({"": None})
    out["lsd"] = out["lsd"].map(normalize_lsd)
    out = out[out["disposition_id"] != ""].copy()
    out = out.dropna(subset=["section", "township", "range", "meridian"], how="any")
    return out


def load_ami_crown_participants_frame(source: pd.DataFrame | Path) -> pd.DataFrame:
    df = _coerce_tabular_frame(source)
    if df.empty:
        return pd.DataFrame()

    cols = {_normalize_column_name(column): column for column in df.columns}
    out = pd.DataFrame(index=df.index)
    out["disposition_id"] = _column_as_series(
        df,
        cols,
        ("disposition_id", "agreement_id", "agreement_no", "agreement_number", "contract_no", "contract_number"),
        "",
    )
    out["client_id"] = _column_as_series(df, cols, ("client_id", "owner_id", "client_no", "client_number"), "")
    out["role_type"] = _column_as_series(df, cols, ("role_type", "participant_role", "role"), "holder")
    out["interest_pct"] = _column_as_series(df, cols, ("interest_pct", "ownership_pct", "percentage"), None)
    out["effective_from"] = _column_as_series(df, cols, ("effective_from", "effective_date", "start_date"), None)
    out["effective_to"] = _column_as_series(df, cols, ("effective_to", "expiry_date", "end_date", "termination_date"), None)
    out["source"] = _column_as_series(df, cols, ("source",), None)

    out["disposition_id"] = out["disposition_id"].fillna("").astype(str).str.strip()
    out["client_id"] = out["client_id"].fillna("").astype(str).str.strip()
    out["role_type"] = out["role_type"].fillna("holder").astype(str).str.strip().replace({"": "holder"})
    out["interest_pct"] = pd.to_numeric(out["interest_pct"], errors="coerce")
    out["effective_from"] = pd.to_datetime(out["effective_from"], errors="coerce").dt.date
    out["effective_to"] = pd.to_datetime(out["effective_to"], errors="coerce").dt.date
    out = out[(out["disposition_id"] != "") & (out["client_id"] != "")].copy()
    return out
