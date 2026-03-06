from pathlib import Path

from deal_flow_ingest.config import SourcePayload
from deal_flow_ingest.sources.aer import _parse_st37_text, load_st37


def test_parse_st37_text_delimited_sample_extracts_minimum_fields():
    sample = Path(__file__).parent / "fixtures" / "st37_sample.txt"

    parsed = _parse_st37_text(sample)

    assert not parsed.empty
    assert {
        "uwi",
        "status",
        "licensee",
        "township",
        "range",
        "section",
        "meridian",
    }.issubset(set(parsed.columns))
    assert len(parsed) == 2
    assert parsed.iloc[0]["uwi"] == "00/12-34-056-07W4"


class _NoopDownloader:
    def fetch(self, *_args, **_kwargs):  # pragma: no cover - should not be called in this test
        raise AssertionError("Downloader should not be called when local_live_file is set")


def test_load_st37_uses_local_live_file(tmp_path: Path):
    local_st37 = tmp_path / "st37_local.txt"
    local_st37.write_text(
        "uwi|status|licensee\n"
        "00/12-34-056-07W4|ACTIVE|ALPHA ENERGY\n",
        encoding="utf-8",
    )

    source = SourcePayload(
        key="aer_st37",
        source_name="aer_st37",
        data_kind="wells",
        enabled=True,
        local_sample=None,
        local_live_file=str(local_st37),
        parser_name="aer_st37",
        landing_page_url="https://example.com/landing",
        dataset_url="https://example.com/dataset.txt",
        file_type="txt",
        refresh_frequency="monthly",
    )

    parsed = load_st37(downloader=_NoopDownloader(), source=source, refresh=False)

    assert len(parsed) == 1
    assert parsed.iloc[0]["uwi"] == "00/12-34-056-07W4"
    assert parsed.iloc[0]["status"] == "ACTIVE"
