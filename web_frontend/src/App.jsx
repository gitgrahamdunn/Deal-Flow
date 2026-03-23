import { startTransition, useEffect, useMemo, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import wellknown from "wellknown";
import {
  getAssetDetail,
  getFilterOptions,
  getMapAssets,
  getPackageCandidates,
  getSellerCandidates,
  getSummary,
} from "./api";

const DEFAULT_CENTER = [-114.5, 54.7];
const DEFAULT_ZOOM = 5.1;

const rasterStyle = {
  version: 8,
  sources: {
    osm: {
      type: "raster",
      tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
      tileSize: 256,
      attribution: "© OpenStreetMap contributors",
    },
  },
  layers: [
    {
      id: "osm",
      type: "raster",
      source: "osm",
    },
  ],
};

function boundsToQuery(bounds) {
  if (!bounds) {
    return {};
  }
  return {
    min_lon: bounds.getWest(),
    max_lon: bounds.getEast(),
    min_lat: bounds.getSouth(),
    max_lat: bounds.getNorth(),
  };
}

function toPointFeatures(rows) {
  return {
    type: "FeatureCollection",
    features: rows
      .filter((row) => row.lon !== null && row.lat !== null)
      .map((row) => ({
        type: "Feature",
        id: `${row.asset_type}:${row.asset_id}`,
        geometry: {
          type: "Point",
          coordinates: [Number(row.lon), Number(row.lat)],
        },
        properties: row,
      })),
  };
}

function toLineFeatures(rows) {
  return {
    type: "FeatureCollection",
    features: rows
      .map((row) => {
        if (!row.geometry_wkt) {
          return null;
        }
        const geometry = wellknown.parse(row.geometry_wkt);
        if (!geometry) {
          return null;
        }
        return {
          type: "Feature",
          id: `pipeline:${row.asset_id}`,
          geometry,
          properties: row,
        };
      })
      .filter(Boolean),
  };
}

function formatNumber(value) {
  if (value === null || value === undefined || value === "") {
    return "n/a";
  }
  if (typeof value === "number") {
    return value.toLocaleString();
  }
  return String(value);
}

function buildAssetKey(assetType, assetId) {
  return `${assetType}:${assetId}`;
}

function getBoundsForRows(rows) {
  const points = rows
    .filter((row) => row.lon !== null && row.lat !== null)
    .map((row) => [Number(row.lon), Number(row.lat)]);
  if (!points.length) {
    return null;
  }
  const bounds = new maplibregl.LngLatBounds(points[0], points[0]);
  points.slice(1).forEach((point) => bounds.extend(point));
  return bounds;
}

function getSelectedRow(layers, selectedAsset) {
  if (!selectedAsset) {
    return null;
  }
  const rows = Object.values(layers).flat();
  return rows.find(
    (row) => row.asset_type === selectedAsset.assetType && String(row.asset_id) === String(selectedAsset.assetId),
  );
}

function getLayerLimit(map, filters) {
  const zoom = map?.getZoom() || DEFAULT_ZOOM;
  if (filters.candidateOnly) {
    return 25000;
  }
  if (zoom >= 9) {
    return 25000;
  }
  if (zoom >= 6.8) {
    return 12000;
  }
  return 4000;
}

function App() {
  const mapNodeRef = useRef(null);
  const mapRef = useRef(null);
  const popupRef = useRef(null);
  const mapRequestRef = useRef({ id: 0, controller: null, timer: null });

  const [summary, setSummary] = useState(null);
  const [options, setOptions] = useState({
    operators: [],
    well_statuses: [],
    facility_statuses: [],
    pipeline_statuses: [],
  });
  const [layers, setLayers] = useState({ wells: [], facilities: [], pipelines: [] });
  const [counts, setCounts] = useState({ wells: 0, facilities: 0, pipelines: 0 });
  const [sellerRows, setSellerRows] = useState([]);
  const [packageRows, setPackageRows] = useState([]);
  const [detail, setDetail] = useState(null);
  const [selectedAsset, setSelectedAsset] = useState(null);
  const [error, setError] = useState("");
  const [loadingMap, setLoadingMap] = useState(false);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [mapReady, setMapReady] = useState(false);
  const [viewportToken, setViewportToken] = useState(0);
  const [pendingFocus, setPendingFocus] = useState(null);
  const [candidateQuery, setCandidateQuery] = useState("");
  const [wellLoadingState, setWellLoadingState] = useState("full");
  const [filters, setFilters] = useState({
    assetTypes: {
      wells: true,
      facilities: true,
      pipelines: true,
    },
    operator: "",
    statuses: "",
    candidateOnly: false,
  });

  const activeAssetTypes = useMemo(
    () =>
      Object.entries(filters.assetTypes)
        .filter(([, active]) => active)
        .map(([key]) => key),
    [filters.assetTypes],
  );

  const filteredSellerRows = useMemo(() => {
    const query = candidateQuery.trim().toLowerCase();
    if (!query) {
      return sellerRows;
    }
    return sellerRows.filter(
      (row) =>
        String(row.operator || "")
          .toLowerCase()
          .includes(query) || String(row.core_area_key || "").toLowerCase().includes(query),
    );
  }, [sellerRows, candidateQuery]);

  const filteredPackageRows = useMemo(() => {
    const query = candidateQuery.trim().toLowerCase();
    if (!query) {
      return packageRows;
    }
    return packageRows.filter(
      (row) =>
        String(row.operator || "")
          .toLowerCase()
          .includes(query) || String(row.area_key || "").toLowerCase().includes(query),
    );
  }, [packageRows, candidateQuery]);

  const approximateWellCount = useMemo(
    () =>
      (layers.wells || [])
        .filter((row) => row.location_method === "dls_approx")
        .reduce((sum, row) => sum + Number(row.well_count || 1), 0),
    [layers.wells],
  );
  const visibleWellTotal = useMemo(
    () => (layers.wells || []).reduce((sum, row) => sum + Number(row.well_count || 1), 0),
    [layers.wells],
  );

  useEffect(() => {
    let cancelled = false;
    Promise.all([getSummary(), getFilterOptions(), getSellerCandidates(), getPackageCandidates()])
      .then(([summaryPayload, filterPayload, sellerPayload, packagePayload]) => {
        if (cancelled) {
          return;
        }
        startTransition(() => {
          setSummary(summaryPayload);
          setOptions(filterPayload);
          setSellerRows(sellerPayload.rows);
          setPackageRows(packagePayload.rows);
        });
      })
      .catch((err) => {
        if (!cancelled) {
          setError(`Failed initial load: ${err.message}`);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (mapRef.current || !mapNodeRef.current) {
      return undefined;
    }

    const map = new maplibregl.Map({
      container: mapNodeRef.current,
      style: rasterStyle,
      center: DEFAULT_CENTER,
      zoom: DEFAULT_ZOOM,
      attributionControl: true,
    });
    map.addControl(new maplibregl.NavigationControl({ visualizePitch: true }), "top-right");
    mapRef.current = map;

    map.on("load", () => {
      map.addSource("facilities", {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
        cluster: true,
        clusterRadius: 42,
        clusterMaxZoom: 9,
      });
      map.addSource("wells", {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
        cluster: true,
        clusterRadius: 48,
        clusterMaxZoom: 10,
      });
      map.addSource("pipelines", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
      map.addSource("selection-point", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
      map.addSource("selection-line", { type: "geojson", data: { type: "FeatureCollection", features: [] } });

      map.addLayer({
        id: "pipelines",
        type: "line",
        source: "pipelines",
        paint: {
          "line-color": [
            "case",
            ["==", ["get", "candidate_any"], 1],
            "#d4562e",
            "#254f6f",
          ],
          "line-width": [
            "interpolate",
            ["linear"],
            ["zoom"],
            5,
            0.8,
            10,
            2.3,
          ],
          "line-opacity": 0.78,
        },
      });

      map.addLayer({
        id: "facilities-clusters",
        type: "circle",
        source: "facilities",
        filter: ["has", "point_count"],
        paint: {
          "circle-radius": [
            "step",
            ["get", "point_count"],
            14,
            25,
            18,
            100,
            24,
          ],
          "circle-color": "#166b68",
          "circle-opacity": 0.78,
          "circle-stroke-width": 1,
          "circle-stroke-color": "#f5f1e8",
        },
      });

      map.addLayer({
        id: "facilities-cluster-count",
        type: "symbol",
        source: "facilities",
        filter: ["has", "point_count"],
        layout: {
          "text-field": ["get", "point_count_abbreviated"],
          "text-font": ["Open Sans Semibold"],
          "text-size": 11,
        },
        paint: {
          "text-color": "#f5f1e8",
        },
      });

      map.addLayer({
        id: "facilities",
        type: "circle",
        source: "facilities",
        filter: ["!", ["has", "point_count"]],
        paint: {
          "circle-radius": [
            "interpolate",
            ["linear"],
            ["zoom"],
            5,
            2.5,
            10,
            7,
          ],
          "circle-color": [
            "case",
            ["==", ["get", "candidate_any"], 1],
            "#e7842b",
            "#1d9a6c",
          ],
          "circle-stroke-width": 1,
          "circle-stroke-color": "#f5f1e8",
        },
      });

      map.addLayer({
        id: "wells-clusters",
        type: "circle",
        source: "wells",
        filter: ["has", "point_count"],
        paint: {
          "circle-radius": [
            "step",
            ["get", "point_count"],
            14,
            25,
            20,
            100,
            28,
          ],
          "circle-color": "#a14a66",
          "circle-opacity": 0.78,
          "circle-stroke-width": 1,
          "circle-stroke-color": "#f5f1e8",
        },
      });

      map.addLayer({
        id: "wells-cluster-count",
        type: "symbol",
        source: "wells",
        filter: ["has", "point_count"],
        layout: {
          "text-field": ["get", "point_count_abbreviated"],
          "text-font": ["Open Sans Semibold"],
          "text-size": 11,
        },
        paint: {
          "text-color": "#fff6f2",
        },
      });

      map.addLayer({
        id: "wells",
        type: "circle",
        source: "wells",
        filter: ["!", ["has", "point_count"]],
        paint: {
          "circle-radius": [
            "interpolate",
            ["linear"],
            ["zoom"],
            5,
            1.5,
            10,
            5,
          ],
          "circle-color": [
            "case",
            ["==", ["get", "candidate_any"], 1],
            "#7c1d49",
            "#f2c078",
          ],
          "circle-opacity": 0.9,
        },
      });

      map.addLayer({
        id: "selection-line",
        type: "line",
        source: "selection-line",
        paint: {
          "line-color": "#ffe38a",
          "line-width": [
            "interpolate",
            ["linear"],
            ["zoom"],
            5,
            2,
            10,
            5,
          ],
          "line-opacity": 0.95,
        },
      });

      map.addLayer({
        id: "selection-point",
        type: "circle",
        source: "selection-point",
        paint: {
          "circle-radius": [
            "interpolate",
            ["linear"],
            ["zoom"],
            5,
            4,
            10,
            9,
          ],
          "circle-color": "#fff7cf",
          "circle-stroke-color": "#7c1d49",
          "circle-stroke-width": 2,
        },
      });

      const handlePointer = (event) => {
        const features = map.queryRenderedFeatures(event.point, {
          layers: ["facilities", "wells", "pipelines", "facilities-clusters", "wells-clusters"],
        });
        map.getCanvas().style.cursor = features.length ? "pointer" : "";
      };
      map.on("mousemove", handlePointer);

      const handleClick = (event) => {
        const features = map.queryRenderedFeatures(event.point, {
          layers: ["facilities", "wells", "pipelines", "facilities-clusters", "wells-clusters"],
        });
        if (!features.length) {
          return;
        }
        const feature = features[0];
        const layerId = feature.layer?.id;
        if (layerId === "facilities-clusters" || layerId === "wells-clusters") {
          const sourceName = layerId.startsWith("facilities") ? "facilities" : "wells";
          map.getSource(sourceName)?.getClusterExpansionZoom(feature.properties.cluster_id, (err, zoom) => {
            if (err) {
              return;
            }
            map.easeTo({
              center: feature.geometry.coordinates,
              zoom,
              duration: 500,
            });
          });
          return;
        }
        const props = feature.properties || {};
        if (Number(props.is_aggregate) === 1) {
          map.easeTo({
            center: event.lngLat,
            zoom: Math.max((map.getZoom() || DEFAULT_ZOOM) + 1.4, 7.2),
            duration: 500,
          });
          return;
        }
        if (popupRef.current) {
          popupRef.current.remove();
        }
        popupRef.current = new maplibregl.Popup({ closeButton: false, offset: 12 })
          .setLngLat(event.lngLat)
          .setHTML(
            `<div class="map-popup"><strong>${props.asset_name || props.asset_id}</strong><br/>${props.operator || "Unknown operator"}<br/>${props.status || "Unknown status"}${props.location_method === "dls_approx" ? "<br/>Approximate DLS location" : ""}${Number(props.is_aggregate) === 1 ? "<br/>Zoom in for individual wells" : ""}</div>`,
          )
          .addTo(map);

        setSelectedAsset({ assetType: props.asset_type, assetId: props.asset_id });
        setLoadingDetail(true);
        getAssetDetail(props.asset_type, props.asset_id)
          .then((payload) => setDetail(payload))
          .catch((err) => setError(`Failed to load asset detail: ${err.message}`))
          .finally(() => setLoadingDetail(false));
      };
      map.on("click", handleClick);

      map.on("moveend", () => setViewportToken((current) => current + 1));

      setMapReady(true);
    });

    return () => {
      if (mapRequestRef.current.timer) {
        window.clearTimeout(mapRequestRef.current.timer);
      }
      mapRequestRef.current.controller?.abort();
      map.remove();
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!mapRef.current || !mapReady) {
      return;
    }
    const map = mapRef.current;
    const bounds = map.getBounds();
    const effectiveAssetTypes = activeAssetTypes.length ? activeAssetTypes : ["facilities", "pipelines"];
    const limitPerLayer = getLayerLimit(map, filters);
    const params = {
      asset_types: effectiveAssetTypes.join(","),
      operator: filters.operator,
      statuses: filters.statuses,
      candidate_only: filters.candidateOnly,
      zoom: map.getZoom().toFixed(2),
      limit_per_layer: limitPerLayer,
      ...boundsToQuery(bounds),
    };

    setWellLoadingState((map.getZoom() || DEFAULT_ZOOM) < 6.8 && filters.assetTypes.wells ? "aggregated" : "full");
    if (mapRequestRef.current.timer) {
      window.clearTimeout(mapRequestRef.current.timer);
    }
    mapRequestRef.current.controller?.abort();
    const controller = new AbortController();
    const requestId = mapRequestRef.current.id + 1;
    mapRequestRef.current = { id: requestId, controller, timer: null };

    setLoadingMap(true);
    mapRequestRef.current.timer = window.setTimeout(() => {
      getMapAssets({ ...params, signal: controller.signal })
        .then((payload) => {
          if (mapRequestRef.current.id !== requestId) {
            return;
          }
          startTransition(() => {
            setLayers(payload.layers);
            setCounts(payload.counts);
          });
          map.getSource("facilities")?.setData(toPointFeatures(payload.layers.facilities || []));
          map.getSource("wells")?.setData(toPointFeatures(payload.layers.wells || []));
          map.getSource("pipelines")?.setData(toLineFeatures(payload.layers.pipelines || []));
          setError("");
        })
        .catch((err) => {
          if (err.name === "AbortError" || mapRequestRef.current.id !== requestId) {
            return;
          }
          setError(`Failed map load: ${err.message}`);
        })
        .finally(() => {
          if (mapRequestRef.current.id === requestId) {
            setLoadingMap(false);
          }
        });
    }, 180);
  }, [filters, mapReady, activeAssetTypes, viewportToken]);

  useEffect(() => {
    if (!mapRef.current || !mapReady) {
      return;
    }
    const selectedRow = getSelectedRow(layers, selectedAsset);
    const pointSource = mapRef.current.getSource("selection-point");
    const lineSource = mapRef.current.getSource("selection-line");
    if (!selectedRow) {
      pointSource?.setData({ type: "FeatureCollection", features: [] });
      lineSource?.setData({ type: "FeatureCollection", features: [] });
      return;
    }
    if (selectedRow.asset_type === "pipeline" && selectedRow.geometry_wkt) {
      const geometry = wellknown.parse(selectedRow.geometry_wkt);
      lineSource?.setData({
        type: "FeatureCollection",
        features: geometry
          ? [{ type: "Feature", id: buildAssetKey(selectedRow.asset_type, selectedRow.asset_id), geometry, properties: selectedRow }]
          : [],
      });
      pointSource?.setData({ type: "FeatureCollection", features: [] });
      return;
    }
    if (selectedRow.lon !== null && selectedRow.lat !== null) {
      pointSource?.setData({
        type: "FeatureCollection",
        features: [
          {
            type: "Feature",
            id: buildAssetKey(selectedRow.asset_type, selectedRow.asset_id),
            geometry: { type: "Point", coordinates: [Number(selectedRow.lon), Number(selectedRow.lat)] },
            properties: selectedRow,
          },
        ],
      });
      lineSource?.setData({ type: "FeatureCollection", features: [] });
      return;
    }
    pointSource?.setData({ type: "FeatureCollection", features: [] });
    lineSource?.setData({ type: "FeatureCollection", features: [] });
  }, [layers, mapReady, selectedAsset]);

  useEffect(() => {
    if (!mapRef.current || !pendingFocus) {
      return;
    }
    const rows = Object.values(layers).flat().filter((row) => {
      if (pendingFocus.assetType && row.asset_type !== pendingFocus.assetType) {
        return false;
      }
      if (pendingFocus.operator && row.operator !== pendingFocus.operator) {
        return false;
      }
      if (pendingFocus.assetId && String(row.asset_id) !== String(pendingFocus.assetId)) {
        return false;
      }
      return true;
    });
    const bounds = getBoundsForRows(rows);
    if (bounds) {
      mapRef.current.fitBounds(bounds, { padding: 72, duration: 700, maxZoom: pendingFocus.maxZoom || 11 });
      setPendingFocus(null);
    }
  }, [layers, pendingFocus]);

  function selectAsset(assetType, assetId, { focus = false } = {}) {
    setSelectedAsset({ assetType, assetId });
    if (focus) {
      setPendingFocus({ assetType, assetId, maxZoom: 12 });
    }
    setLoadingDetail(true);
    getAssetDetail(assetType, assetId)
      .then((payload) => setDetail(payload))
      .catch((err) => setError(`Failed to load asset detail: ${err.message}`))
      .finally(() => setLoadingDetail(false));
  }

  function applyOperatorFocus(operator, candidateOnly = false) {
    setSelectedAsset(null);
    setDetail(null);
    setPendingFocus({ operator, maxZoom: 10 });
    setFilters((current) => ({
      ...current,
      operator,
      candidateOnly,
    }));
  }

  const activeStatusOptions = useMemo(() => {
    const statuses = [];
    if (filters.assetTypes.wells) {
      statuses.push(...options.well_statuses);
    }
    if (filters.assetTypes.facilities) {
      statuses.push(...options.facility_statuses);
    }
    if (filters.assetTypes.pipelines) {
      statuses.push(...options.pipeline_statuses);
    }
    return [...new Set(statuses)];
  }, [filters.assetTypes, options]);

  return (
    <div className="shell">
      <aside className="left-rail">
        <div className="brand-block">
          <div className="eyebrow">Alberta Upstream Registry</div>
          <h1>Deal Flow Web</h1>
          <p>
            Facilities and pipelines use loaded geometry now. Wells are shown with approximate DLS
            placement until the AER well geometry source lands.
          </p>
        </div>

        <section className="stats-grid">
          <div className="stat-card">
            <span>Wells</span>
            <strong>{formatNumber(summary?.wells)}</strong>
          </div>
          <div className="stat-card">
            <span>Facilities</span>
            <strong>{formatNumber(summary?.facilities)}</strong>
          </div>
          <div className="stat-card">
            <span>Pipelines</span>
            <strong>{formatNumber(summary?.pipelines)}</strong>
          </div>
          <div className="stat-card">
            <span>Seller Theses</span>
            <strong>{formatNumber(summary?.seller_candidates)}</strong>
          </div>
        </section>

        <section className="panel">
          <div className="panel-header">
            <h2>Map Filters</h2>
            {loadingMap ? <span className="chip">Refreshing map</span> : null}
          </div>

          <label className="field">
            <span>Operator</span>
            <select
              value={filters.operator}
              onChange={(event) => setFilters((current) => ({ ...current, operator: event.target.value }))}
            >
              <option value="">All operators</option>
              {options.operators.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>

          <label className="field">
            <span>Status</span>
            <select
              value={filters.statuses}
              onChange={(event) => setFilters((current) => ({ ...current, statuses: event.target.value }))}
            >
              <option value="">All statuses</option>
              {activeStatusOptions.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>

          <label className="field">
            <span>Candidate Search</span>
            <input
              value={candidateQuery}
              placeholder="Operator or area"
              onChange={(event) => setCandidateQuery(event.target.value)}
            />
          </label>

          <div className="toggle-group">
            {[
              ["wells", "Wells"],
              ["facilities", "Facilities"],
              ["pipelines", "Pipelines"],
            ].map(([key, label]) => (
              <button
                key={key}
                type="button"
                className={filters.assetTypes[key] ? "toggle active" : "toggle"}
                onClick={() =>
                  setFilters((current) => ({
                    ...current,
                    assetTypes: { ...current.assetTypes, [key]: !current.assetTypes[key] },
                  }))
                }
              >
                {label}
              </button>
            ))}
          </div>

          <label className="checkbox">
            <input
              type="checkbox"
              checked={filters.candidateOnly}
              onChange={(event) =>
                setFilters((current) => ({
                  ...current,
                  candidateOnly: event.target.checked,
                }))
              }
            />
            <span>Candidate-only view</span>
          </label>

          <div className="layer-counts">
            <div>Visible facilities: {formatNumber(counts.facilities)}</div>
            <div>Visible pipelines: {formatNumber(counts.pipelines)}</div>
            <div>Visible wells: {formatNumber(visibleWellTotal)}</div>
          </div>
          <div className="chip">Approx well points: {formatNumber(approximateWellCount)}</div>
          {wellLoadingState === "aggregated" ? (
            <div className="chip muted">Low zoom wells are aggregated</div>
          ) : null}
        </section>

        <section className="panel scroll-panel">
          <div className="panel-header">
            <h2>Seller Candidates</h2>
            <span className="chip">{filteredSellerRows.length}</span>
          </div>
          {filteredSellerRows.slice(0, 12).map((row) => (
            <button
              type="button"
              key={`${row.operator_id}-${row.core_area_key || "na"}`}
              className="list-card"
              onClick={() => applyOperatorFocus(row.operator, false)}
            >
              <strong>{row.operator}</strong>
              <span>{row.thesis_priority} priority</span>
              <span>Thesis {formatNumber(row.thesis_score)}</span>
            </button>
          ))}
        </section>

        <section className="panel scroll-panel">
          <div className="panel-header">
            <h2>Package Candidates</h2>
            <span className="chip">{filteredPackageRows.length}</span>
          </div>
          {filteredPackageRows.slice(0, 12).map((row) => (
            <button
              type="button"
              key={`${row.operator_id}-${row.area_key}`}
              className="list-card"
              onClick={() => applyOperatorFocus(row.operator, true)}
            >
              <strong>{row.operator}</strong>
              <span>{row.area_key}</span>
              <span>Package {formatNumber(row.package_score)}</span>
            </button>
          ))}
        </section>
      </aside>

      <main className="map-stage">
        <div ref={mapNodeRef} className="map-surface" />
        {error ? <div className="error-banner">{error}</div> : null}
      </main>

      <aside className="right-rail">
        <div className="panel detail-panel">
          <div className="panel-header">
            <h2>Asset Detail</h2>
            {loadingDetail ? <span className="chip">Loading</span> : null}
          </div>
          {!detail ? (
            <div className="empty-state">
              Click a facility, pipeline, or mapped well to inspect the registry record and linked
              assets.
            </div>
          ) : (
            <div className="detail-content">
              <div className="detail-title">{detail.asset_name || detail.asset_id}</div>
              <div className="detail-subtitle">{detail.asset_type}</div>
              <div className="detail-grid">
                <div>
                  <span>Operator</span>
                  <strong>{formatNumber(detail.operator || detail.asset_operator)}</strong>
                </div>
                <div>
                  <span>Status</span>
                  <strong>{formatNumber(detail.status || detail.facility_status || detail.segment_status)}</strong>
                </div>
                <div>
                  <span>License</span>
                  <strong>{formatNumber(detail.license_number || detail.asset_license_number)}</strong>
                </div>
                <div>
                  <span>Field / Type</span>
                  <strong>{formatNumber(detail.field_name || detail.facility_type || detail.substance1)}</strong>
                </div>
              </div>

              {detail.linked_facilities?.length ? (
                <section className="detail-section">
                  <h3>Linked Facilities</h3>
                  {detail.linked_facilities.slice(0, 20).map((item) => (
                    <div key={item.facility_id} className="detail-list-row">
                      <button
                        type="button"
                        className="detail-link"
                        onClick={() => selectAsset("facility", item.facility_id, { focus: true })}
                      >
                        {item.facility_name || item.facility_id}
                      </button>
                      <span>{item.facility_status || item.operator || "n/a"}</span>
                    </div>
                  ))}
                </section>
              ) : null}

              {detail.linked_wells?.length ? (
                <section className="detail-section">
                  <h3>Linked Wells</h3>
                  {detail.linked_wells.slice(0, 20).map((item) => (
                    <div key={item.well_id} className="detail-list-row">
                      <button
                        type="button"
                        className="detail-link"
                        onClick={() => selectAsset("well", item.well_id, { focus: true })}
                      >
                        {item.well_name || item.well_id}
                      </button>
                      <span>{item.status || item.operator || "n/a"}</span>
                    </div>
                  ))}
                </section>
              ) : null}
            </div>
          )}
        </div>
      </aside>
    </div>
  );
}

export default App;
