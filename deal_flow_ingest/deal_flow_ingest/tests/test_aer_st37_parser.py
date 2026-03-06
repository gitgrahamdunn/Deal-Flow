from pathlib import Path
from zipfile import ZipFile

from deal_flow_ingest.config import SourcePayload
from deal_flow_ingest.sources.aer import _discover_st37_artifact_url, _parse_st37_text, load_st37


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



def test_discover_st37_artifact_url_prefers_text_zip():
    html = """
    <html><body>
      <a href="/files/ST37-Report.pdf">ST37 PDF</a>
      <a href="/files/ST37-Shape.zip">ST37 Shape [ZIP]</a>
      <a href="/files/ST37-Text.zip">ST37 Text Format [ZIP]</a>
    </body></html>
    """

    discovered = _discover_st37_artifact_url(html, "https://www.aer.ca/data-and-performance-reports/statistical-reports/st37")

    assert discovered == "https://www.aer.ca/files/ST37-Text.zip"


class _FakeResult:
    def __init__(self, path: Path, extracted_dir: Path | None = None):
        self.path = path
        self.extracted_dir = extracted_dir


class _FakeDownloader:
    def __init__(self, landing_path: Path, zip_path: Path, extracted_dir: Path):
        self.landing_path = landing_path
        self.zip_path = zip_path
        self.extracted_dir = extracted_dir

    def fetch(self, _source: str, url: str, **_kwargs):
        if url.endswith("/st37"):
            return _FakeResult(self.landing_path)
        if url.endswith("ST37-Text.zip"):
            return _FakeResult(self.zip_path, extracted_dir=self.extracted_dir)
        raise AssertionError(f"Unexpected url requested: {url}")


def test_load_st37_uses_discovered_artifact_when_dataset_url_missing(tmp_path: Path):
    landing = tmp_path / "landing.html"
    landing.write_text(
        '<a href="/files/ST37-Text.zip">ST37 Text Format [ZIP]</a>',
        encoding="utf-8",
    )

    extracted_dir = tmp_path / "st37_extracted"
    extracted_dir.mkdir()
    extracted_txt = extracted_dir / "st37_current.txt"
    extracted_txt.write_text(
        "uwi|status|licensee\n"
        "00/12-34-056-07W4|ACTIVE|ALPHA ENERGY\n",
        encoding="utf-8",
    )

    zip_path = tmp_path / "ST37-Text.zip"
    with ZipFile(zip_path, "w") as archive:
        archive.write(extracted_txt, arcname="st37_current.txt")

    source = SourcePayload(
        key="aer_st37",
        source_name="aer_st37",
        data_kind="wells",
        enabled=True,
        local_sample=None,
        local_live_file="",
        parser_name="aer_st37",
        landing_page_url="https://www.aer.ca/data-and-performance-reports/statistical-reports/st37",
        dataset_url="",
        file_type="zip",
        refresh_frequency="monthly",
    )

    parsed = load_st37(
        downloader=_FakeDownloader(landing_path=landing, zip_path=zip_path, extracted_dir=extracted_dir),
        source=source,
        refresh=False,
    )

    assert not parsed.empty
    assert parsed.iloc[0]["uwi"] == "00/12-34-056-07W4"
