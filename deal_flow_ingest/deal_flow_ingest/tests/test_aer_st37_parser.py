from pathlib import Path

from deal_flow_ingest.sources.aer import _parse_st37_text


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
