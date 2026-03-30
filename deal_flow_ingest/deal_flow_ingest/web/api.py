from __future__ import annotations

import base64
import os
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from deal_flow_ingest.services.registry_queries import (
    RegistryMapFilters,
    get_asset_detail,
    get_registry_map_overlays,
    get_operator_detail,
    get_operator_suggestions,
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


_CACHE_TTL_SECONDS = 120
_cache_store: dict[str, tuple[float, object]] = {}


def _get_basic_auth_credentials() -> tuple[str, str] | None:
    username = os.getenv("DEALFLOW_WEB_USERNAME", "").strip()
    password = os.getenv("DEALFLOW_WEB_PASSWORD", "").strip()
    if not username or not password:
        return None
    return username, password


def _get_allowed_origins() -> list[str]:
    raw = os.getenv("DEALFLOW_WEB_ALLOWED_ORIGINS", "").strip()
    if not raw:
        return []
    return [part.strip().rstrip("/") for part in raw.split(",") if part.strip()]


def _get_allowed_origin_regex() -> str | None:
    raw = os.getenv("DEALFLOW_WEB_ALLOWED_ORIGIN_REGEX", "").strip()
    return raw or None


def _wants_private_network_access(request: Request) -> bool:
    return request.headers.get("access-control-request-private-network", "").lower() == "true"


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


def _cache_get(key: str):
    cached = _cache_store.get(key)
    if cached is None:
        return None
    expires_at, value = cached
    if expires_at <= time.monotonic():
        _cache_store.pop(key, None)
        return None
    return value


def _cache_put(key: str, value):
    _cache_store[key] = (time.monotonic() + _CACHE_TTL_SECONDS, value)
    return value


def _cached(key: str, factory):
    cached = _cache_get(key)
    if cached is not None:
        return cached
    return _cache_put(key, factory())


def create_app() -> FastAPI:
    _cache_store.clear()
    app = FastAPI(title="Deal Flow Web API", version="0.1.0")
    credentials = _get_basic_auth_credentials()
    allowed_origins = _get_allowed_origins()
    allowed_origin_regex = _get_allowed_origin_regex()
    if not allowed_origins and allowed_origin_regex is None:
        allowed_origins = ["*"]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_origin_regex=allowed_origin_regex,
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
        allow_private_network=True,
    )

    @app.middleware("http")
    async def basic_auth_guard(request: Request, call_next):  # noqa: ANN001
        if _is_authorized(request, credentials):
            response = await call_next(request)
            if _wants_private_network_access(request):
                response.headers["Access-Control-Allow-Private-Network"] = "true"
            return response
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
        return _cached("summary", get_registry_summary)

    @app.get("/api/map/filters")
    def map_filters() -> dict[str, list[str]]:
        return _cached("map_filters", get_registry_filter_options)

    @app.get("/api/operators")
    def operators(q: str = "", limit: int = 25) -> dict[str, object]:
        normalized_limit = max(1, min(int(limit), 100))
        cache_key = f"operators:{q.strip().lower()}:{normalized_limit}"
        rows = _cached(cache_key, lambda: get_operator_suggestions(q, normalized_limit))
        return {"rows": rows, "count": len(rows)}

    @app.get("/api/operators/{operator_name:path}")
    def operator_detail(operator_name: str) -> dict[str, object]:
        normalized_name = operator_name.strip()
        if not normalized_name:
            raise HTTPException(status_code=400, detail="Operator name is required")
        payload = _cached(f"operator_detail:{normalized_name.lower()}", lambda: get_operator_detail(normalized_name))
        if payload is None:
            raise HTTPException(status_code=404, detail="Operator not found")
        return payload

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

    @app.get("/api/map/overlays")
    def map_overlays(
        operator: str | None = None,
        min_lat: float | None = None,
        max_lat: float | None = None,
        min_lon: float | None = None,
        max_lon: float | None = None,
        include_package_areas: bool = True,
        include_operator_footprints: bool = True,
        limit: int = 2500,
    ) -> dict[str, object]:
        cache_key = (
            "map_overlays:"
            f"{(operator or '').strip().lower()}:"
            f"{round(min_lat, 4) if min_lat is not None else 'na'}:"
            f"{round(max_lat, 4) if max_lat is not None else 'na'}:"
            f"{round(min_lon, 4) if min_lon is not None else 'na'}:"
            f"{round(max_lon, 4) if max_lon is not None else 'na'}:"
            f"{int(include_package_areas)}:{int(include_operator_footprints)}:{int(limit)}"
        )
        return _cached(
            cache_key,
            lambda: get_registry_map_overlays(
                operator=operator,
                min_lat=min_lat,
                max_lat=max_lat,
                min_lon=min_lon,
                max_lon=max_lon,
                include_package_areas=include_package_areas,
                include_operator_footprints=include_operator_footprints,
                limit=limit,
            ),
        )

    @app.get("/api/candidates/sellers")
    def seller_candidates(limit: int = 200, min_score: float = 0.0) -> dict[str, object]:
        frame = _cached(f"sellers:{int(limit)}:{float(min_score)}", lambda: get_seller_candidates(limit=limit, min_score=min_score))
        return {"rows": _frame_to_records(frame), "count": len(frame.index)}

    @app.get("/api/candidates/packages")
    def package_candidates(limit: int = 200, min_score: float = 0.0) -> dict[str, object]:
        frame = _cached(f"packages:{int(limit)}:{float(min_score)}", lambda: get_package_candidates(limit=limit, min_score=min_score))
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
