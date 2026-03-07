from pathlib import Path
from zipfile import ZipFile

from deal_flow_ingest.config import SourcePayload
from deal_flow_ingest.sources.aer import (
    _discover_st37_artifact_url,
    _find_best_licensee_column,
    _parse_st37_text,
    _parse_well_table,
    load_st37,
)


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
    def __init__(self, responses: dict[str, _FakeResult]):
        self.responses = responses
        self.calls: list[str] = []

    def fetch(self, _source: str, url: str, **_kwargs):
        self.calls.append(url)
        result = self.responses.get(url)
        if result is None:
            raise AssertionError(f"Unexpected url requested: {url}")
        return result

    def _load_metadata(self, _source: str) -> dict:
        return {}

    def _save_metadata(self, _source: str, _meta: dict) -> None:
        return None


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
        downloader=_FakeDownloader(
            responses={
                "https://www.aer.ca/data-and-performance-reports/statistical-reports/st37": _FakeResult(landing),
                "https://www.aer.ca/files/ST37-Text.zip": _FakeResult(zip_path, extracted_dir=extracted_dir),
            }
        ),
        source=source,
        refresh=False,
    )

    assert not parsed.empty
    assert parsed.iloc[0]["uwi"] == "00/12-34-056-07W4"


def test_load_st37_falls_back_to_alternate_landing_page(tmp_path: Path):
    alt_landing = tmp_path / "landing_alt.html"
    alt_landing.write_text(
        '<a href="/files/ST37-Wells-in-Alberta-Text.zip">ST37 Wells in Alberta Text Format [ZIP]</a>',
        encoding="utf-8",
    )

    extracted_dir = tmp_path / "extracted_alt"
    extracted_dir.mkdir()
    extracted_txt = extracted_dir / "st37_alt.txt"
    extracted_txt.write_text("uwi|status|licensee\n00/01-01-001-01W4|ACTIVE|BETA\n", encoding="utf-8")

    zip_path = tmp_path / "ST37-Wells-in-Alberta-Text.zip"
    with ZipFile(zip_path, "w") as archive:
        archive.write(extracted_txt, arcname="st37_alt.txt")

    downloader = _FakeDownloader(
        responses={
            "https://www1.aer.ca/ProductCatalogue/10.html": _FakeResult(alt_landing),
            "https://www1.aer.ca/files/ST37-Wells-in-Alberta-Text.zip": _FakeResult(zip_path, extracted_dir=extracted_dir),
        }
    )

    source = SourcePayload(
        key="aer_st37",
        source_name="aer_st37",
        data_kind="wells",
        enabled=True,
        local_sample=None,
        local_live_file="",
        parser_name="aer_st37",
        landing_page_url="https://www.aer.ca/data-and-performance-reports/statistical-reports/st37",
        alternate_landing_page_urls=["https://www1.aer.ca/ProductCatalogue/10.html"],
        dataset_url="",
        file_type="zip",
        refresh_frequency="monthly",
    )

    parsed = load_st37(downloader=downloader, source=source, refresh=False)

    assert len(parsed) == 1
    assert parsed.iloc[0]["uwi"] == "00/01-01-001-01W4"
    assert downloader.calls[0].endswith("/st37")
    assert downloader.calls[1].endswith("10.html")


def test_load_st37_prefers_cached_discovered_artifact_url(tmp_path: Path):
    txt = tmp_path / "cached.txt"
    txt.write_text("uwi|status|licensee\n00/02-02-002-02W4|ACTIVE|GAMMA\n", encoding="utf-8")

    class _CachedDownloader(_FakeDownloader):
        def __init__(self):
            super().__init__(responses={"https://cached.example.com/st37.txt": _FakeResult(txt)})

        def _load_metadata(self, _source: str) -> dict:
            return {"discovered_artifact_url": "https://cached.example.com/st37.txt"}

    source = SourcePayload(
        key="aer_st37",
        source_name="aer_st37",
        data_kind="wells",
        enabled=True,
        local_sample=None,
        local_live_file="",
        parser_name="aer_st37",
        landing_page_url="https://www.aer.ca/data-and-performance-reports/statistical-reports/st37",
        alternate_landing_page_urls=["https://www1.aer.ca/ProductCatalogue/10.html"],
        dataset_url="",
        file_type="txt",
        refresh_frequency="monthly",
    )

    downloader = _CachedDownloader()
    parsed = load_st37(downloader=downloader, source=source, refresh=False)

    assert len(parsed) == 1
    assert downloader.calls == ["https://cached.example.com/st37.txt"]


def test_load_st37_persists_discovered_artifact_url(tmp_path: Path):
    landing = tmp_path / "landing_cache.html"
    landing.write_text('<a href="/files/ST37-Text.zip">ST37 Text Format [ZIP]</a>', encoding="utf-8")

    extracted_dir = tmp_path / "cache_extract"
    extracted_dir.mkdir()
    data = extracted_dir / "st37_cached.txt"
    data.write_text("uwi|status|licensee\n00/03-03-003-03W4|ACTIVE|DELTA\n", encoding="utf-8")

    zip_path = tmp_path / "ST37-Text.zip"
    with ZipFile(zip_path, "w") as archive:
        archive.write(data, arcname="st37_cached.txt")

    class _PersistingDownloader(_FakeDownloader):
        def __init__(self):
            super().__init__(
                responses={
                    "https://www.aer.ca/data-and-performance-reports/statistical-reports/st37": _FakeResult(landing),
                    "https://www.aer.ca/files/ST37-Text.zip": _FakeResult(zip_path, extracted_dir=extracted_dir),
                }
            )
            self.meta = {}

        def _load_metadata(self, _source: str) -> dict:
            return dict(self.meta)

        def _save_metadata(self, _source: str, meta: dict) -> None:
            self.meta = dict(meta)

    source = SourcePayload(
        key="aer_st37",
        source_name="aer_st37",
        data_kind="wells",
        enabled=True,
        local_sample=None,
        local_live_file="",
        parser_name="aer_st37",
        landing_page_url="https://www.aer.ca/data-and-performance-reports/statistical-reports/st37",
        alternate_landing_page_urls=None,
        dataset_url="",
        file_type="zip",
        refresh_frequency="monthly",
    )

    downloader = _PersistingDownloader()
    parsed = load_st37(downloader=downloader, source=source, refresh=False)

    assert len(parsed) == 1
    assert downloader.meta["discovered_artifact_url"] == "https://www.aer.ca/files/ST37-Text.zip"


def test_find_best_licensee_column_prefers_licensee_name_variants():
    columns = ["UWI", "STATUS", "LICENSEE_NAME", "LICENSEE_ID", "WELL_NAME"]
    assert _find_best_licensee_column(columns) == "LICENSEE_NAME"


def test_find_best_licensee_column_supports_business_associate_labels():
    columns = ["UWI", "Business Associate Name", "BA_ID", "Pool"]
    assert _find_best_licensee_column(columns) == "Business Associate Name"


def test_find_best_licensee_column_prefers_operator_name_over_codes():
    columns = ["Operator Code", "Operator Name", "Field Code", "UWI"]
    assert _find_best_licensee_column(columns) == "Operator Name"


def test_find_best_licensee_column_recognizes_ba_name():
    columns = ["BA_NAME", "BA_ID", "Status", "UWI"]
    assert _find_best_licensee_column(columns) == "BA_NAME"


def test_parse_well_table_uses_detected_licensee_column(tmp_path: Path):
    sample = tmp_path / "st37_licensee_name.csv"
    sample.write_text(
        "UWI,STATUS,LICENSEE_NAME\n"
        "00/12-34-056-07W4,ACTIVE,ALPHA ENERGY\n"
        "00/11-33-055-06W4,SUSPENDED,BETA RESOURCES\n",
        encoding="utf-8",
    )

    parsed = _parse_well_table(sample)

    assert list(parsed["licensee"]) == ["ALPHA ENERGY", "BETA RESOURCES"]
