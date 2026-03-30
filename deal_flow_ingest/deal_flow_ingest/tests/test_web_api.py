from __future__ import annotations

import base64

import pandas as pd
from fastapi.testclient import TestClient

from deal_flow_ingest.web.api import create_app


def test_web_api_endpoints(monkeypatch) -> None:
    monkeypatch.delenv("DEALFLOW_WEB_USERNAME", raising=False)
    monkeypatch.delenv("DEALFLOW_WEB_PASSWORD", raising=False)
    captured = {}

    def fake_map_layers(_filters):
        captured["filters"] = _filters
        return {
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
        }

    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_summary", lambda: {"wells": 10, "facilities": 4, "pipelines": 2, "seller_candidates": 1, "package_candidates": 1})
    monkeypatch.setattr(
        "deal_flow_ingest.web.api.get_registry_filter_options",
        lambda: {
            "well_statuses": ["SUSPENDED"],
            "facility_statuses": ["ACTIVE"],
            "pipeline_statuses": ["OPERATING"],
        },
    )
    monkeypatch.setattr("deal_flow_ingest.web.api.get_operator_suggestions", lambda q="", limit=25: ["ALPHA ENERGY LTD"])
    monkeypatch.setattr(
        "deal_flow_ingest.web.api.get_operator_detail",
        lambda operator: {"operator": operator, "asset_counts": {"wells": 2, "facilities": 1, "pipelines": 1}},
    )
    monkeypatch.setattr(
        "deal_flow_ingest.web.api.get_registry_map_overlays",
        lambda **_kwargs: {
            "package_areas": {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "id": "package:1:10-01-050-08W4",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [[[-114.01, 52.10], [-114.0, 52.10], [-114.0, 52.11], [-114.01, 52.11], [-114.01, 52.10]]],
                        },
                        "properties": {"operator": "ALPHA ENERGY LTD", "area_key": "10-01-050-08W4"},
                    }
                ],
            },
            "operator_footprints": {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "id": "footprint:1",
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [[[[-114.01, 52.10], [-114.0, 52.10], [-114.0, 52.11], [-114.01, 52.11], [-114.01, 52.10]]]],
                        },
                        "properties": {"operator": "ALPHA ENERGY LTD", "visible_package_cells": 1},
                    }
                ],
            },
            "counts": {"package_areas": 1, "operator_footprints": 1},
        },
    )
    monkeypatch.setattr(
        "deal_flow_ingest.web.api.get_registry_map_layers",
        fake_map_layers,
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
    assert client.get("/api/map/filters").json()["well_statuses"] == ["SUSPENDED"]
    assert client.get("/api/operators?q=alpha").json()["rows"] == ["ALPHA ENERGY LTD"]
    assert client.get("/api/operators/ALPHA%20ENERGY%20LTD").json()["asset_counts"]["wells"] == 2

    asset_payload = client.get("/api/map/assets?asset_types=facilities,pipelines&candidate_only=true&zoom=7.5").json()
    assert asset_payload["counts"] == {"facilities": 1, "pipelines": 1}
    assert asset_payload["layers"]["facilities"][0]["asset_id"] == "FAC-1"
    assert captured["filters"].zoom == 7.5
    overlay_payload = client.get(
        "/api/map/overlays?include_package_areas=true&include_operator_footprints=true&min_lat=52.0&max_lat=53.0"
    ).json()
    assert overlay_payload["counts"] == {"package_areas": 1, "operator_footprints": 1}
    assert overlay_payload["package_areas"]["features"][0]["properties"]["area_key"] == "10-01-050-08W4"

    assert client.get("/api/candidates/sellers").json()["count"] == 1
    assert client.get("/api/candidates/packages").json()["count"] == 1
    assert client.get("/api/assets/facility/FAC-1").json()["asset_name"] == "Alpha Battery"


def test_web_api_not_found(monkeypatch) -> None:
    monkeypatch.delenv("DEALFLOW_WEB_USERNAME", raising=False)
    monkeypatch.delenv("DEALFLOW_WEB_PASSWORD", raising=False)
    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_summary", lambda: {})
    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_filter_options", lambda: {})
    monkeypatch.setattr("deal_flow_ingest.web.api.get_operator_suggestions", lambda q="", limit=25: [])
    monkeypatch.setattr("deal_flow_ingest.web.api.get_operator_detail", lambda operator: None)
    monkeypatch.setattr(
        "deal_flow_ingest.web.api.get_registry_map_overlays",
        lambda **_kwargs: {
            "package_areas": {"type": "FeatureCollection", "features": []},
            "operator_footprints": {"type": "FeatureCollection", "features": []},
            "counts": {"package_areas": 0, "operator_footprints": 0},
        },
    )
    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_map_layers", lambda _filters: {})
    monkeypatch.setattr("deal_flow_ingest.web.api.get_seller_candidates", lambda limit=200, min_score=0.0: pd.DataFrame())
    monkeypatch.setattr("deal_flow_ingest.web.api.get_package_candidates", lambda limit=200, min_score=0.0: pd.DataFrame())
    monkeypatch.setattr("deal_flow_ingest.web.api.get_asset_detail", lambda asset_type, asset_id: None)

    client = TestClient(create_app())
    response = client.get("/api/assets/well/UNKNOWN")
    operator_response = client.get("/api/operators/UNKNOWN")

    assert response.status_code == 404
    assert operator_response.status_code == 404


def test_web_api_supports_basic_auth(monkeypatch) -> None:
    monkeypatch.setenv("DEALFLOW_WEB_USERNAME", "dealflow")
    monkeypatch.setenv("DEALFLOW_WEB_PASSWORD", "secret")
    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_summary", lambda: {"wells": 1})
    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_filter_options", lambda: {})
    monkeypatch.setattr("deal_flow_ingest.web.api.get_operator_suggestions", lambda q="", limit=25: [])
    monkeypatch.setattr("deal_flow_ingest.web.api.get_operator_detail", lambda operator: {"operator": operator})
    monkeypatch.setattr(
        "deal_flow_ingest.web.api.get_registry_map_overlays",
        lambda **_kwargs: {
            "package_areas": {"type": "FeatureCollection", "features": []},
            "operator_footprints": {"type": "FeatureCollection", "features": []},
            "counts": {"package_areas": 0, "operator_footprints": 0},
        },
    )
    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_map_layers", lambda _filters: {})
    monkeypatch.setattr("deal_flow_ingest.web.api.get_seller_candidates", lambda limit=200, min_score=0.0: pd.DataFrame())
    monkeypatch.setattr("deal_flow_ingest.web.api.get_package_candidates", lambda limit=200, min_score=0.0: pd.DataFrame())
    monkeypatch.setattr("deal_flow_ingest.web.api.get_asset_detail", lambda asset_type, asset_id: None)

    client = TestClient(create_app())
    unauthorized = client.get("/api/summary")
    health = client.get("/api/health")
    token = base64.b64encode(b"dealflow:secret").decode("ascii")
    authorized = client.get("/api/summary", headers={"Authorization": f"Basic {token}"})

    assert unauthorized.status_code == 401
    assert health.status_code == 200
    assert health.json()["auth_enabled"] is True
    assert authorized.status_code == 200


def test_web_api_restricts_cors_to_configured_origins(monkeypatch) -> None:
    monkeypatch.delenv("DEALFLOW_WEB_USERNAME", raising=False)
    monkeypatch.delenv("DEALFLOW_WEB_PASSWORD", raising=False)
    monkeypatch.setenv(
        "DEALFLOW_WEB_ALLOWED_ORIGINS",
        "https://dealflow-frontend.onrender.com, https://dealflow.example.com/",
    )
    monkeypatch.delenv("DEALFLOW_WEB_ALLOWED_ORIGIN_REGEX", raising=False)
    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_summary", lambda: {"wells": 1})
    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_filter_options", lambda: {})
    monkeypatch.setattr("deal_flow_ingest.web.api.get_operator_suggestions", lambda q="", limit=25: [])
    monkeypatch.setattr("deal_flow_ingest.web.api.get_operator_detail", lambda operator: {"operator": operator})
    monkeypatch.setattr(
        "deal_flow_ingest.web.api.get_registry_map_overlays",
        lambda **_kwargs: {
            "package_areas": {"type": "FeatureCollection", "features": []},
            "operator_footprints": {"type": "FeatureCollection", "features": []},
            "counts": {"package_areas": 0, "operator_footprints": 0},
        },
    )
    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_map_layers", lambda _filters: {})
    monkeypatch.setattr("deal_flow_ingest.web.api.get_seller_candidates", lambda limit=200, min_score=0.0: pd.DataFrame())
    monkeypatch.setattr("deal_flow_ingest.web.api.get_package_candidates", lambda limit=200, min_score=0.0: pd.DataFrame())
    monkeypatch.setattr("deal_flow_ingest.web.api.get_asset_detail", lambda asset_type, asset_id: None)

    client = TestClient(create_app())
    allowed = client.get("/api/summary", headers={"Origin": "https://dealflow.example.com"})
    blocked = client.get("/api/summary", headers={"Origin": "https://evil.example.com"})

    assert allowed.status_code == 200
    assert allowed.headers["access-control-allow-origin"] == "https://dealflow.example.com"
    assert blocked.status_code == 200
    assert "access-control-allow-origin" not in blocked.headers


def test_web_api_supports_cors_origin_regex(monkeypatch) -> None:
    monkeypatch.delenv("DEALFLOW_WEB_USERNAME", raising=False)
    monkeypatch.delenv("DEALFLOW_WEB_PASSWORD", raising=False)
    monkeypatch.delenv("DEALFLOW_WEB_ALLOWED_ORIGINS", raising=False)
    monkeypatch.setenv("DEALFLOW_WEB_ALLOWED_ORIGIN_REGEX", r"https://dealflow-frontend-.*\.onrender\.com")
    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_summary", lambda: {"wells": 1})
    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_filter_options", lambda: {})
    monkeypatch.setattr("deal_flow_ingest.web.api.get_operator_suggestions", lambda q="", limit=25: [])
    monkeypatch.setattr("deal_flow_ingest.web.api.get_operator_detail", lambda operator: {"operator": operator})
    monkeypatch.setattr(
        "deal_flow_ingest.web.api.get_registry_map_overlays",
        lambda **_kwargs: {
            "package_areas": {"type": "FeatureCollection", "features": []},
            "operator_footprints": {"type": "FeatureCollection", "features": []},
            "counts": {"package_areas": 0, "operator_footprints": 0},
        },
    )
    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_map_layers", lambda _filters: {})
    monkeypatch.setattr("deal_flow_ingest.web.api.get_seller_candidates", lambda limit=200, min_score=0.0: pd.DataFrame())
    monkeypatch.setattr("deal_flow_ingest.web.api.get_package_candidates", lambda limit=200, min_score=0.0: pd.DataFrame())
    monkeypatch.setattr("deal_flow_ingest.web.api.get_asset_detail", lambda asset_type, asset_id: None)

    client = TestClient(create_app())
    allowed = client.get("/api/summary", headers={"Origin": "https://dealflow-frontend-pr-42.onrender.com"})
    blocked = client.get("/api/summary", headers={"Origin": "https://dealflow.example.com"})

    assert allowed.status_code == 200
    assert allowed.headers["access-control-allow-origin"] == "https://dealflow-frontend-pr-42.onrender.com"
    assert blocked.status_code == 200
    assert "access-control-allow-origin" not in blocked.headers


def test_web_api_allows_private_network_access_preflight(monkeypatch) -> None:
    monkeypatch.delenv("DEALFLOW_WEB_USERNAME", raising=False)
    monkeypatch.delenv("DEALFLOW_WEB_PASSWORD", raising=False)
    monkeypatch.setenv("DEALFLOW_WEB_ALLOWED_ORIGINS", "https://dealflow-frontend-sy22.onrender.com")
    monkeypatch.delenv("DEALFLOW_WEB_ALLOWED_ORIGIN_REGEX", raising=False)
    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_summary", lambda: {"wells": 1})
    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_filter_options", lambda: {})
    monkeypatch.setattr("deal_flow_ingest.web.api.get_operator_suggestions", lambda q="", limit=25: [])
    monkeypatch.setattr("deal_flow_ingest.web.api.get_operator_detail", lambda operator: {"operator": operator})
    monkeypatch.setattr(
        "deal_flow_ingest.web.api.get_registry_map_overlays",
        lambda **_kwargs: {
            "package_areas": {"type": "FeatureCollection", "features": []},
            "operator_footprints": {"type": "FeatureCollection", "features": []},
            "counts": {"package_areas": 0, "operator_footprints": 0},
        },
    )
    monkeypatch.setattr("deal_flow_ingest.web.api.get_registry_map_layers", lambda _filters: {})
    monkeypatch.setattr("deal_flow_ingest.web.api.get_seller_candidates", lambda limit=200, min_score=0.0: pd.DataFrame())
    monkeypatch.setattr("deal_flow_ingest.web.api.get_package_candidates", lambda limit=200, min_score=0.0: pd.DataFrame())
    monkeypatch.setattr("deal_flow_ingest.web.api.get_asset_detail", lambda asset_type, asset_id: None)

    client = TestClient(create_app())
    response = client.options(
        "/api/summary",
        headers={
            "Origin": "https://dealflow-frontend-sy22.onrender.com",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Private-Network": "true",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "https://dealflow-frontend-sy22.onrender.com"
    assert response.headers["access-control-allow-private-network"] == "true"
