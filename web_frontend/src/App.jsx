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

function App() {
  const mapNodeRef = useRef(null);
  const mapRef = useRef(null);
  const popupRef = useRef(null);

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
  const [error, setError] = useState("");
  const [loadingMap, setLoadingMap] = useState(false);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [mapReady, setMapReady] = useState(false);
  const [viewportToken, setViewportToken] = useState(0);
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
      map.addSource("facilities", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
      map.addSource("wells", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
      map.addSource("pipelines", { type: "geojson", data: { type: "FeatureCollection", features: [] } });

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
        id: "facilities",
        type: "circle",
        source: "facilities",
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
        id: "wells",
        type: "circle",
        source: "wells",
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

      const handlePointer = (event) => {
        const features = map.queryRenderedFeatures(event.point, {
          layers: ["facilities", "wells", "pipelines"],
        });
        map.getCanvas().style.cursor = features.length ? "pointer" : "";
      };
      map.on("mousemove", handlePointer);

      const handleClick = (event) => {
        const features = map.queryRenderedFeatures(event.point, {
          layers: ["facilities", "wells", "pipelines"],
        });
        if (!features.length) {
          return;
        }
        const feature = features[0];
        const props = feature.properties || {};
        if (popupRef.current) {
          popupRef.current.remove();
        }
        popupRef.current = new maplibregl.Popup({ closeButton: false, offset: 12 })
          .setLngLat(event.lngLat)
          .setHTML(
            `<div class="map-popup"><strong>${props.asset_name || props.asset_id}</strong><br/>${props.operator || "Unknown operator"}<br/>${props.status || "Unknown status"}${props.location_method === "dls_approx" ? "<br/>Approximate DLS location" : ""}</div>`,
          )
          .addTo(map);

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
    const assetTypes = activeAssetTypes.length ? activeAssetTypes : ["facilities", "pipelines"];
    const params = {
      asset_types: assetTypes.join(","),
      operator: filters.operator,
      statuses: filters.statuses,
      candidate_only: filters.candidateOnly,
      limit_per_layer: 15000,
      ...boundsToQuery(bounds),
    };

    setLoadingMap(true);
    getMapAssets(params)
      .then((payload) => {
        startTransition(() => {
          setLayers(payload.layers);
          setCounts(payload.counts);
        });
        map.getSource("facilities")?.setData(toPointFeatures(payload.layers.facilities || []));
        map.getSource("wells")?.setData(toPointFeatures(payload.layers.wells || []));
        map.getSource("pipelines")?.setData(toLineFeatures(payload.layers.pipelines || []));
      })
      .catch((err) => setError(`Failed map load: ${err.message}`))
      .finally(() => setLoadingMap(false));
  }, [filters, mapReady, activeAssetTypes, viewportToken]);

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
            <div>Visible wells: {formatNumber(counts.wells)}</div>
          </div>
          <div className="chip">Well points are approximate for now</div>
        </section>

        <section className="panel scroll-panel">
          <div className="panel-header">
            <h2>Seller Candidates</h2>
            <span className="chip">{sellerRows.length}</span>
          </div>
          {sellerRows.slice(0, 12).map((row) => (
            <button
              type="button"
              key={`${row.operator_id}-${row.core_area_key || "na"}`}
              className="list-card"
              onClick={() => setFilters((current) => ({ ...current, operator: row.operator }))}
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
            <span className="chip">{packageRows.length}</span>
          </div>
          {packageRows.slice(0, 12).map((row) => (
            <button
              type="button"
              key={`${row.operator_id}-${row.area_key}`}
              className="list-card"
              onClick={() =>
                setFilters((current) => ({
                  ...current,
                  operator: row.operator,
                  candidateOnly: true,
                }))
              }
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
                      <strong>{item.facility_name || item.facility_id}</strong>
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
                      <strong>{item.well_name || item.well_id}</strong>
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
