from deal_flow_ingest.transform.normalize import normalize_operator_name


def test_normalize_operator_name_variants():
    assert normalize_operator_name("  Acme Oil Limited ") == "ACME OIL LTD"
    assert normalize_operator_name("Acme Oil Ltd.") == "ACME OIL LTD"
    assert normalize_operator_name("Bravo Energy Incorporated") == "BRAVO ENERGY INC"
    assert normalize_operator_name("Coyote Resources Company") == "COYOTE RESOURCES CO"
