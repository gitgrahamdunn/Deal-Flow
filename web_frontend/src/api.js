const API_BASE = import.meta.env.VITE_API_BASE_URL || "";

async function fetchJson(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: { Accept: "application/json" },
    ...options,
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json();
}

export function getSummary() {
  return fetchJson("/api/summary");
}

export function getFilterOptions() {
  return fetchJson("/api/map/filters");
}

export function getOperatorSuggestions(query, limit = 12) {
  const params = new URLSearchParams();
  if (query) {
    params.set("q", query);
  }
  params.set("limit", String(limit));
  return fetchJson(`/api/operators?${params.toString()}`);
}

export function getOperatorDetail(operator) {
  return fetchJson(`/api/operators/${encodeURIComponent(operator)}`);
}

export function getMapAssets(params) {
  const { signal, ...queryParams } = params;
  const query = new URLSearchParams();
  Object.entries(queryParams).forEach(([key, value]) => {
    if (value === null || value === undefined || value === "") {
      return;
    }
    query.set(key, String(value));
  });
  return fetchJson(`/api/map/assets?${query.toString()}`, signal ? { signal } : {});
}

export function getSellerCandidates(limit = 100, minScore = 0) {
  return fetchJson(`/api/candidates/sellers?limit=${limit}&min_score=${minScore}`);
}

export function getPackageCandidates(limit = 100, minScore = 0) {
  return fetchJson(`/api/candidates/packages?limit=${limit}&min_score=${minScore}`);
}

export function getAssetDetail(assetType, assetId) {
  return fetchJson(`/api/assets/${assetType}/${encodeURIComponent(assetId)}`);
}
