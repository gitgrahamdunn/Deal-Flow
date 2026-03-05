from deal_flow_ingest.transform.normalize import normalize_operator_name


def test_operator_normalization():
    assert normalize_operator_name("  Alpha Energy Incorporated. ") == "ALPHA ENERGY INC"
    assert normalize_operator_name("Beta-Petroleum, Limited") == "BETA PETROLEUM LTD"
