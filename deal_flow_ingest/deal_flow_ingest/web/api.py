from __future__ import annotations

import base64
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from deal_flow_ingest.services.registry_queries import (
    RegistryMapFilters,
    get_asset_detail,
    get_package_candidates,
    get_registry_filter_options,
    get_registry_map_layers,
    get_registry_summary,
    get_seller_candidates,
)


def _frontend_dist_dir() -> Path:
    return Path(__file__).resolve().parents[3] / "web_frontend" / "dist"


def _frame_to_records(frame):
    return frame.where(frame.notna(), None).to_dict(orient="records")


def _get_basic_auth_credentials() -> tuple[str, str] | None:
    username = os.getenv("DEALFLOW_WEB_USERNAME", "").strip()
    password = os.getenv("DEALFLOW_WEB_PASSWORD", "").strip()
    if not username or not password:
        return None
    return username, password


def _is_authorized(request: Request, credentials: tuple[str, str] | None) -> bool:
    if credentials is None or request.url.path == "/api/health":
        return True
    header = request.headers.get("authorization", "")
    if not header.startswith("Basic "):
        return False
    try:
        decoded = base64.b64decode(header.split(" ", 1)[1]).decode("utf-8")
    except Exception:  # pragma: no cover - malformed auth header
        return False
    username, _, password = decoded.partition(":")
    return (username, password) == credentials


def create_app() -> FastAPI:
    app = FastAPI(title="Deal Flow Web API", version="0.1.0")
    credentials = _get_basic_auth_credentials()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def basic_auth_guard(request: Request, call_next):  # noqa: ANN001
        if _is_authorized(request, credentials):
            return await call_next(request)
        return JSONResponse(
            {"detail": "Authentication required"},
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="dealflow"'},
        )

    @app.get("/api/health")
    def health() -> dict[str, object]:
        return {"status": "ok", "frontend_built": _frontend_dist_dir().exists(), "auth_enabled": credentials is not None}

    @app.get("/api/summary")
    def summary() -> dict[str, int]:
        return get_registry_summary()

    @app.get("/api/map/filters")
    def map_filters() -> dict[str, list[str]]:
        return get_registry_filter_options()

    @app.get("/api/map/assets")
    def map_assets(
        asset_types: str = Query("wells,facilities,pipelines"),
        operator: str | None = None,
        statuses: str | None = None,
        candidate_only: bool = False,
        zoom: float | None = None,
        min_lat: float | None = None,
        max_lat: float | None = None,
        min_lon: float | None = None,
        max_lon: float | None = None,
        limit_per_layer: int = 50000,
    ) -> dict[str, object]:
        filters = RegistryMapFilters(
            asset_types=tuple(part.strip() for part in asset_types.split(",") if part.strip()),
            operator=operator,
            statuses=tuple(part.strip() for part in (statuses or "").split(",") if part.strip()),
            candidate_only=candidate_only,
            zoom=zoom,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
            limit_per_layer=limit_per_layer,
        )
        layers = get_registry_map_layers(filters)
        return {
            "layers": {key: _frame_to_records(value) for key, value in layers.items()},
            "counts": {key: len(value.index) for key, value in layers.items()},
        }

    @app.get("/api/candidates/sellers")
    def seller_candidates(limit: int = 200, min_score: float = 0.0) -> dict[str, object]:
        frame = get_seller_candidates(limit=limit, min_score=min_score)
        return {"rows": _frame_to_records(frame), "count": len(frame.index)}

    @app.get("/api/candidates/packages")
    def package_candidates(limit: int = 200, min_score: float = 0.0) -> dict[str, object]:
        frame = get_package_candidates(limit=limit, min_score=min_score)
        return {"rows": _frame_to_records(frame), "count": len(frame.index)}

    @app.get("/api/assets/{asset_type}/{asset_id}")
    def asset_detail(asset_type: str, asset_id: str) -> dict[str, object]:
        try:
            payload = get_asset_detail(asset_type, asset_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if payload is None:
            raise HTTPException(status_code=404, detail="Asset not found")
        return payload

    dist_dir = _frontend_dist_dir()
    if dist_dir.exists():
        assets_dir = dist_dir / "assets"
        if assets_dir.exists():
            app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

        @app.get("/{full_path:path}", include_in_schema=False)
        def frontend(full_path: str):  # noqa: ANN001
            target = dist_dir / full_path
            if full_path and target.exists() and target.is_file():
                return FileResponse(target)
            index_path = dist_dir / "index.html"
            return FileResponse(index_path)

    return app


app = create_app()
