import { startTransition, useEffect, useMemo, useRef, useState } from "react";
import {
  getAssetDetail,
  getFilterOptions,
  getMapAssets,
  getOperatorDetail,
  getOperatorSuggestions,
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

function toLineFeatures(rows, wellknownLib) {
  return {
    type: "FeatureCollection",
    features: rows
      .map((row) => {
        if (!row.geometry_wkt || !wellknownLib) {
          return null;
        }
        const geometry = wellknownLib.parse(row.geometry_wkt);
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

function getBoundsForRows(rows, maplibreLib) {
  const points = rows
    .filter((row) => row.lon !== null && row.lat !== null)
    .map((row) => [Number(row.lon), Number(row.lat)]);
  if (!points.length) {
    return null;
  }
  const bounds = new maplibreLib.LngLatBounds(points[0], points[0]);
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

function readUrlState() {
  const params = new URLSearchParams(window.location.search);
  const layers = new Set((params.get("layers") || "wells,facilities,pipelines").split(",").filter(Boolean));
  const selectedRaw = params.get("selected") || "";
  const [selectedType, ...selectedIdParts] = selectedRaw.split(":");
  return {
    filters: {
      assetTypes: {
        wells: layers.has("wells"),
        facilities: layers.has("facilities"),
        pipelines: layers.has("pipelines"),
      },
      operator: params.get("operator") || "",
      statuses: params.get("status") || "",
      candidateOnly: params.get("candidate") === "1",
    },
    mapView: {
      center: [
        Number(params.get("lon")) || DEFAULT_CENTER[0],
        Number(params.get("lat")) || DEFAULT_CENTER[1],
      ],
      zoom: Number(params.get("z")) || DEFAULT_ZOOM,
    },
    selectedAsset:
      selectedType && selectedIdParts.length
        ? { assetType: selectedType, assetId: selectedIdParts.join(":") }
        : null,
  };
}

function writeUrlState({ filters, selectedAsset, map }) {
  const params = new URLSearchParams(window.location.search);
  const activeLayers = Object.entries(filters.assetTypes)
    .filter(([, active]) => active)
    .map(([key]) => key);
  if (activeLayers.length) {
    params.set("layers", activeLayers.join(","));
  } else {
    params.delete("layers");
  }
  if (filters.operator) {
    params.set("operator", filters.operator);
  } else {
    params.delete("operator");
  }
  if (filters.statuses) {
    params.set("status", filters.statuses);
  } else {
    params.delete("status");
  }
  if (filters.candidateOnly) {
    params.set("candidate", "1");
  } else {
    params.delete("candidate");
  }
  if (selectedAsset) {
    params.set("selected", `${selectedAsset.assetType}:${selectedAsset.assetId}`);
  } else {
    params.delete("selected");
  }
  if (map) {
    const center = map.getCenter();
    params.set("lat", center.lat.toFixed(5));
    params.set("lon", center.lng.toFixed(5));
    params.set("z", map.getZoom().toFixed(2));
  }
  const next = `${window.location.pathname}?${params.toString()}`;
  window.history.replaceState({}, "", next);
}

function App() {
  const initialUrlState = useMemo(() => readUrlState(), []);
  const mapNodeRef = useRef(null);
  const mapRef = useRef(null);
  const popupRef = useRef(null);
  const mapRequestRef = useRef({ id: 0, controller: null, timer: null });
  const maplibreRef = useRef(null);
  const wellknownRef = useRef(null);
  const initialSelectionRef = useRef(initialUrlState.selectedAsset);

  const [summary, setSummary] = useState(null);
  const [options, setOptions] = useState({
    well_statuses: [],
    facility_statuses: [],
    pipeline_statuses: [],
  });
  const [operatorOptions, setOperatorOptions] = useState([]);
  const [layers, setLayers] = useState({ wells: [], facilities: [], pipelines: [] });
  const [counts, setCounts] = useState({ wells: 0, facilities: 0, pipelines: 0 });
  const [sellerRows, setSellerRows] = useState([]);
  const [packageRows, setPackageRows] = useState([]);
  const [detail, setDetail] = useState(null);
  const [selectedAsset, setSelectedAsset] = useState(initialUrlState.selectedAsset);
  const [selectedOperator, setSelectedOperator] = useState(
    initialUrlState.selectedAsset ? "" : initialUrlState.filters.operator,
  );
  const [operatorDetail, setOperatorDetail] = useState(null);
  const [error, setError] = useState("");
  const [loadingMap, setLoadingMap] = useState(false);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [loadingOperatorDetail, setLoadingOperatorDetail] = useState(false);
  const [loadingSummary, setLoadingSummary] = useState(false);
  const [loadingCandidates, setLoadingCandidates] = useState(false);
  const [mapReady, setMapReady] = useState(false);
  const [viewportToken, setViewportToken] = useState(0);
  const [pendingFocus, setPendingFocus] = useState(null);
  const [candidateQuery, setCandidateQuery] = useState("");
  const [operatorSearch, setOperatorSearch] = useState(initialUrlState.filters.operator);
  const [wellLoadingState, setWellLoadingState] = useState("full");
  const [summaryError, setSummaryError] = useState("");
  const [candidateError, setCandidateError] = useState("");
  const [filters, setFilters] = useState(initialUrlState.filters);

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
    getFilterOptions()
      .then((filterPayload) => {
        if (!cancelled) {
          startTransition(() => setOptions(filterPayload));
        }
      })
      .catch(() => {});
    setLoadingSummary(true);
    getSummary()
      .then((summaryPayload) => {
        if (!cancelled) {
          startTransition(() => setSummary(summaryPayload));
          setSummaryError("");
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setSummaryError(`Summary unavailable: ${err.message}`);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoadingSummary(false);
        }
      });
    setLoadingCandidates(true);
    Promise.all([getSellerCandidates(25, 0), getPackageCandidates(25, 0)])
      .then(([sellerPayload, packagePayload]) => {
        if (!cancelled) {
          startTransition(() => {
            setSellerRows(sellerPayload.rows);
            setPackageRows(packagePayload.rows);
          });
          setCandidateError("");
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setCandidateError(`Candidate panels unavailable: ${err.message}`);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoadingCandidates(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    const timer = window.setTimeout(() => {
      getOperatorSuggestions(operatorSearch, 12)
        .then((payload) => {
          if (!cancelled) {
            setOperatorOptions(payload.rows || []);
          }
        })
        .catch(() => {
          if (!cancelled) {
            setOperatorOptions([]);
          }
        });
    }, 180);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [operatorSearch]);

  useEffect(() => {
    writeUrlState({ filters, selectedAsset, map: mapRef.current });
  }, [filters, selectedAsset, viewportToken]);

  useEffect(() => {
    if (!selectedOperator) {
      setOperatorDetail(null);
      setLoadingOperatorDetail(false);
      return;
    }
    let cancelled = false;
    setLoadingOperatorDetail(true);
    getOperatorDetail(selectedOperator)
      .then((payload) => {
        if (!cancelled) {
          setOperatorDetail(payload);
          setError("");
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setOperatorDetail(null);
          setError(`Failed to load operator detail: ${err.message}`);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoadingOperatorDetail(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [selectedOperator]);

  useEffect(() => {
    if (mapRef.current || !mapNodeRef.current) {
      return undefined;
    }
    let disposed = false;
    let createdMap = null;

    Promise.all([import("maplibre-gl"), import("wellknown")]).then(([maplibreModule, wellknownModule]) => {
      if (disposed || !mapNodeRef.current) {
        return;
      }
      const maplibreLib = maplibreModule.default;
      const wellknownLib = wellknownModule.default || wellknownModule;
      maplibreRef.current = maplibreLib;
      wellknownRef.current = wellknownLib;

      const map = new maplibreLib.Map({
        container: mapNodeRef.current,
        style: rasterStyle,
        center: initialUrlState.mapView.center,
        zoom: initialUrlState.mapView.zoom,
        attributionControl: true,
      });
      createdMap = map;
      map.addControl(new maplibreLib.NavigationControl({ visualizePitch: true }), "top-right");
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
        popupRef.current = new maplibreLib.Popup({ closeButton: false, offset: 12 })
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
    });

    return () => {
      disposed = true;
      if (mapRequestRef.current.timer) {
        window.clearTimeout(mapRequestRef.current.timer);
      }
      mapRequestRef.current.controller?.abort();
      createdMap?.remove();
      mapRef.current = null;
    };
  }, [initialUrlState.mapView.center, initialUrlState.mapView.zoom]);

  useEffect(() => {
    if (!mapReady || !initialSelectionRef.current) {
      return;
    }
    const selection = initialSelectionRef.current;
    initialSelectionRef.current = null;
    setLoadingDetail(true);
    getAssetDetail(selection.assetType, selection.assetId)
      .then((payload) => setDetail(payload))
      .catch((err) => setError(`Failed to load asset detail: ${err.message}`))
      .finally(() => setLoadingDetail(false));
  }, [mapReady]);

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
          map.getSource("pipelines")?.setData(toLineFeatures(payload.layers.pipelines || [], wellknownRef.current));
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
      const geometry = wellknownRef.current?.parse(selectedRow.geometry_wkt);
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
    if (!maplibreRef.current) {
      return;
    }
    const bounds = getBoundsForRows(rows, maplibreRef.current);
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
    const normalizedOperator = String(operator || "").trim();
    if (!normalizedOperator) {
      return;
    }
    setSelectedAsset(null);
    setDetail(null);
    setPendingFocus({ operator: normalizedOperator, maxZoom: 10 });
    setOperatorSearch(normalizedOperator);
    setSelectedOperator(normalizedOperator);
    setFilters((current) => ({
      ...current,
      operator: normalizedOperator,
      candidateOnly,
    }));
  }

  function viewOperatorFromSearch() {
    const query = operatorSearch.trim();
    if (!query) {
      return;
    }
    const matchedOperator =
      operatorOptions.find((option) => option.toLowerCase() === query.toLowerCase()) || query;
    applyOperatorFocus(matchedOperator, filters.candidateOnly);
  }

  function clearOperatorFilter() {
    setOperatorSearch("");
    setSelectedOperator("");
    setSelectedAsset(null);
    setDetail(null);
    setError("");
    setFilters((current) => ({
      ...current,
      operator: "",
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
        {loadingSummary ? <div className="chip muted">Loading summary</div> : null}
        {summaryError ? <div className="chip muted">{summaryError}</div> : null}

        <section className="panel">
          <div className="panel-header">
            <h2>Map Filters</h2>
            {loadingMap ? <span className="chip">Refreshing map</span> : null}
          </div>

          <label className="field">
            <span>Operator</span>
            <input
              list="operator-options"
              value={operatorSearch}
              placeholder="Search operator"
              onChange={(event) => setOperatorSearch(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter") {
                  event.preventDefault();
                  viewOperatorFromSearch();
                }
              }}
            />
            <datalist id="operator-options">
              {operatorOptions.map((option) => (
                <option key={option} value={option} />
              ))}
            </datalist>
          </label>
          <div className="toggle-group">
            <button type="button" className="toggle active" onClick={viewOperatorFromSearch}>
              View Operator
            </button>
            <button type="button" className="toggle" onClick={clearOperatorFilter}>
              Clear
            </button>
          </div>

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
            <span className="chip">{loadingCandidates ? "..." : filteredSellerRows.length}</span>
          </div>
          {candidateError ? <div className="chip muted">{candidateError}</div> : null}
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
            <span className="chip">{loadingCandidates ? "..." : filteredPackageRows.length}</span>
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
            <h2>{detail ? "Asset Detail" : selectedOperator ? "Operator Detail" : "Detail"}</h2>
            {loadingDetail || loadingOperatorDetail ? <span className="chip">Loading</span> : null}
          </div>
          {detail ? (
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

              <section className="detail-section">
                <h3>Context</h3>
                <div className="detail-grid">
                  <div>
                    <span>Location Source</span>
                    <strong>{formatNumber(detail.location?.location_method)}</strong>
                  </div>
                  <div>
                    <span>Seller Candidate</span>
                    <strong>{detail.candidate_flags?.seller_candidate ? "Yes" : "No"}</strong>
                  </div>
                  <div>
                    <span>Restart Candidate</span>
                    <strong>{detail.candidate_flags?.restart_candidate ? "Yes" : "No"}</strong>
                  </div>
                  <div>
                    <span>Coordinates</span>
                    <strong>
                      {detail.location?.lat !== null && detail.location?.lat !== undefined && detail.location?.lon !== null && detail.location?.lon !== undefined
                        ? `${Number(detail.location.lat).toFixed(4)}, ${Number(detail.location.lon).toFixed(4)}`
                        : "n/a"}
                    </strong>
                  </div>
                </div>
              </section>

              {detail.production_summary ? (
                <section className="detail-section">
                  <h3>Production Summary</h3>
                  <div className="detail-grid">
                    <div>
                      <span>Latest Month</span>
                      <strong>{formatNumber(detail.production_summary.latest_month)}</strong>
                    </div>
                    <div>
                      <span>Active Months</span>
                      <strong>{formatNumber(detail.production_summary.active_months)}</strong>
                    </div>
                    <div>
                      <span>Oil</span>
                      <strong>{formatNumber(detail.production_summary.oil_bbl_total)}</strong>
                    </div>
                    <div>
                      <span>Gas</span>
                      <strong>{formatNumber(detail.production_summary.gas_mcf_total)}</strong>
                    </div>
                  </div>
                </section>
              ) : null}

              {detail.operator_metrics ? (
                <section className="detail-section">
                  <h3>Operator Metrics</h3>
                  <div className="detail-grid">
                    <div>
                      <span>As Of</span>
                      <strong>{formatNumber(detail.operator_metrics.as_of_date)}</strong>
                    </div>
                    <div>
                      <span>30d Oil BPD</span>
                      <strong>{formatNumber(detail.operator_metrics.avg_oil_bpd_30d)}</strong>
                    </div>
                    <div>
                      <span>Restart Upside</span>
                      <strong>{formatNumber(detail.operator_metrics.restart_upside_bpd_est)}</strong>
                    </div>
                    <div>
                      <span>Suspended Wells</span>
                      <strong>{formatNumber(detail.operator_metrics.suspended_wells_count)}</strong>
                    </div>
                  </div>
                </section>
              ) : null}

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
          ) : operatorDetail ? (
            <div className="detail-content">
              <div className="detail-title">{operatorDetail.operator}</div>
              <div className="detail-subtitle">operator</div>
              <div className="detail-grid">
                <div>
                  <span>Wells</span>
                  <strong>{formatNumber(operatorDetail.asset_counts?.wells)}</strong>
                </div>
                <div>
                  <span>Facilities</span>
                  <strong>{formatNumber(operatorDetail.asset_counts?.facilities)}</strong>
                </div>
                <div>
                  <span>Pipelines</span>
                  <strong>{formatNumber(operatorDetail.asset_counts?.pipelines)}</strong>
                </div>
                <div>
                  <span>Package Candidates</span>
                  <strong>{formatNumber(operatorDetail.package_candidates?.length)}</strong>
                </div>
              </div>

              <section className="detail-section">
                <h3>Context</h3>
                <div className="detail-grid">
                  <div>
                    <span>Seller Candidate</span>
                    <strong>{operatorDetail.candidate_flags?.seller_candidate ? "Yes" : "No"}</strong>
                  </div>
                  <div>
                    <span>Package Candidate</span>
                    <strong>{operatorDetail.candidate_flags?.package_candidate ? "Yes" : "No"}</strong>
                  </div>
                  <div>
                    <span>Top Area</span>
                    <strong>{formatNumber(operatorDetail.seller_thesis?.core_area_key)}</strong>
                  </div>
                  <div>
                    <span>Top Thesis</span>
                    <strong>{formatNumber(operatorDetail.seller_thesis?.thesis_score)}</strong>
                  </div>
                </div>
              </section>

              {operatorDetail.operator_metrics ? (
                <section className="detail-section">
                  <h3>Operator Metrics</h3>
                  <div className="detail-grid">
                    <div>
                      <span>As Of</span>
                      <strong>{formatNumber(operatorDetail.operator_metrics.as_of_date)}</strong>
                    </div>
                    <div>
                      <span>30d Oil BPD</span>
                      <strong>{formatNumber(operatorDetail.operator_metrics.avg_oil_bpd_30d)}</strong>
                    </div>
                    <div>
                      <span>Restart Upside</span>
                      <strong>{formatNumber(operatorDetail.operator_metrics.restart_upside_bpd_est)}</strong>
                    </div>
                    <div>
                      <span>Suspended Wells</span>
                      <strong>{formatNumber(operatorDetail.operator_metrics.suspended_wells_count)}</strong>
                    </div>
                  </div>
                </section>
              ) : null}

              {operatorDetail.seller_thesis ? (
                <section className="detail-section">
                  <h3>Seller Thesis</h3>
                  <div className="detail-grid">
                    <div>
                      <span>Priority</span>
                      <strong>{formatNumber(operatorDetail.seller_thesis.thesis_priority)}</strong>
                    </div>
                    <div>
                      <span>Seller Score</span>
                      <strong>{formatNumber(operatorDetail.seller_thesis.seller_score)}</strong>
                    </div>
                    <div>
                      <span>Restart Upside</span>
                      <strong>{formatNumber(operatorDetail.seller_thesis.restart_upside_bpd_est)}</strong>
                    </div>
                    <div>
                      <span>Package Count</span>
                      <strong>{formatNumber(operatorDetail.seller_thesis.package_count)}</strong>
                    </div>
                  </div>
                </section>
              ) : null}

              {operatorDetail.package_candidates?.length ? (
                <section className="detail-section">
                  <h3>Package Candidates</h3>
                  {operatorDetail.package_candidates.map((item) => (
                    <div key={item.area_key} className="detail-list-row">
                      <strong>{item.area_key}</strong>
                      <span>
                        Package {formatNumber(item.package_score)} · Restart {formatNumber(item.estimated_restart_upside_bpd)}
                      </span>
                    </div>
                  ))}
                </section>
              ) : null}

              {operatorDetail.top_facilities?.length ? (
                <section className="detail-section">
                  <h3>Facilities</h3>
                  {operatorDetail.top_facilities.map((item) => (
                    <div key={item.facility_id} className="detail-list-row">
                      <button
                        type="button"
                        className="detail-link"
                        onClick={() => selectAsset("facility", item.facility_id, { focus: true })}
                      >
                        {item.facility_name || item.facility_id}
                      </button>
                      <span>{item.facility_status || "n/a"}</span>
                    </div>
                  ))}
                </section>
              ) : null}

              {operatorDetail.top_wells?.length ? (
                <section className="detail-section">
                  <h3>Wells</h3>
                  {operatorDetail.top_wells.map((item) => (
                    <div key={item.well_id} className="detail-list-row">
                      <button
                        type="button"
                        className="detail-link"
                        onClick={() => selectAsset("well", item.well_id, { focus: true })}
                      >
                        {item.well_name || item.well_id}
                      </button>
                      <span>
                        {item.status || "n/a"}
                        {item.restart_score ? ` · Restart ${formatNumber(item.restart_score)}` : ""}
                      </span>
                    </div>
                  ))}
                </section>
              ) : null}

              {operatorDetail.top_pipelines?.length ? (
                <section className="detail-section">
                  <h3>Pipelines</h3>
                  {operatorDetail.top_pipelines.map((item) => (
                    <div key={item.pipeline_id} className="detail-list-row">
                      <button
                        type="button"
                        className="detail-link"
                        onClick={() => selectAsset("pipeline", item.pipeline_id, { focus: true })}
                      >
                        {item.licence_line_number || item.pipeline_id}
                      </button>
                      <span>{item.segment_status || "n/a"}</span>
                    </div>
                  ))}
                </section>
              ) : null}
            </div>
          ) : (
            <div className="empty-state">
              Search for an operator to load its footprint and metrics, or click a facility,
              pipeline, or mapped well to inspect an asset.
            </div>
          )}
        </div>
      </aside>
    </div>
  );
}

export default App;
