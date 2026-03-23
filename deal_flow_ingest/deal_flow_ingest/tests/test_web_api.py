from __future__ import annotations

import pandas as pd
from fastapi.testclient import TestClient

from deal_flow_ingest.web.api import create_app


def test_web_api_endpoints(monkeypatch) -> None:
    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_summary", lambda: {"wells": 10, "facilities": 4, "pipelines": 2, "seller_candidates": 1, "package_candidates": 1})
    monkeypatch.setattr(
        "deal_flow_ingest.web.api.get_registry_filter_options",
        lambda: {
            "operators": ["ALPHA ENERGY LTD"],
            "well_statuses": ["SUSPENDED"],
            "facility_statuses": ["ACTIVE"],
            "pipeline_statuses": ["OPERATING"],
        },
    )
    monkeypatch.setattr(
        "deal_flow_ingest.web.api.get_registry_map_layers",
        lambda _filters: {
            "facilities": pd.DataFrame(
                [
                    {
                        "asset_type": "facility",
                        "asset_id": "FAC-1",
                        "asset_name": "Alpha Battery",
                        "operator": "ALPHA ENERGY LTD",
                        "status": "ACTIVE",
                        "lat": 54.1,
                        "lon": -115.2,
                        "candidate_any": 1,
                    }
                ]
            ),
            "pipelines": pd.DataFrame(
                [
                    {
                        "asset_type": "pipeline",
                        "asset_id": "PIPE-1",
                        "asset_name": "PIPE-1",
                        "operator": "ALPHA ENERGY LTD",
                        "status": "OPERATING",
                        "lat": 54.12,
                        "lon": -115.25,
                        "geometry_wkt": "LINESTRING(-115.25 54.12, -115.10 54.18)",
                        "candidate_any": 1,
                    }
                ]
            ),
        },
    )
    monkeypatch.setattr(
        "deal_flow_ingest.web.api.get_seller_candidates",
        lambda limit=200, min_score=0.0: pd.DataFrame([{"operator": "ALPHA ENERGY LTD", "thesis_score": 77.5}]),
    )
    monkeypatch.setattr(
        "deal_flow_ingest.web.api.get_package_candidates",
        lambda limit=200, min_score=0.0: pd.DataFrame([{"operator": "ALPHA ENERGY LTD", "package_score": 68.2}]),
    )
    monkeypatch.setattr(
        "deal_flow_ingest.web.api.get_asset_detail",
        lambda asset_type, asset_id: {"asset_type": asset_type, "asset_id": asset_id, "asset_name": "Alpha Battery"},
    )

    client = TestClient(create_app())

    assert client.get("/api/health").status_code == 200
    assert client.get("/api/summary").json()["wells"] == 10
    assert client.get("/api/map/filters").json()["operators"] == ["ALPHA ENERGY LTD"]

    asset_payload = client.get("/api/map/assets?asset_types=facilities,pipelines&candidate_only=true").json()
    assert asset_payload["counts"] == {"facilities": 1, "pipelines": 1}
    assert asset_payload["layers"]["facilities"][0]["asset_id"] == "FAC-1"

    assert client.get("/api/candidates/sellers").json()["count"] == 1
    assert client.get("/api/candidates/packages").json()["count"] == 1
    assert client.get("/api/assets/facility/FAC-1").json()["asset_name"] == "Alpha Battery"


def test_web_api_not_found(monkeypatch) -> None:
    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_summary", lambda: {})
    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_filter_options", lambda: {})
    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_map_layers", lambda _filters: {})
    monkeypatch.setattr("deal_flow_ingest.web.api.get_seller_candidates", lambda limit=200, min_score=0.0: pd.DataFrame())
    monkeypatch.setattr("deal_flow_ingest.web.api.get_package_candidates", lambda limit=200, min_score=0.0: pd.DataFrame())
    monkeypatch.setattr("deal_flow_ingest.web.api.get_asset_detail", lambda asset_type, asset_id: None)

    client = TestClient(create_app())
    response = client.get("/api/assets/well/UNKNOWN")

    assert response.status_code == 404
