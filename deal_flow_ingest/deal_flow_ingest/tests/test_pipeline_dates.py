from datetime import date, timedelta

from deal_flow_ingest.services.pipeline import _default_live_end_date


def test_default_live_end_date_is_prior_month_end():
    end_date = _default_live_end_date()
    today = date.today()
    expected = today.replace(day=1) - timedelta(days=1)
    assert end_date == expected
