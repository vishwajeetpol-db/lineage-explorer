const BASE = "/api";

let _liveMode = false;
export function setLiveMode(live: boolean) { _liveMode = live; }
export function getLiveMode() { return _liveMode; }

function appendLive(url: string): string {
  if (!_liveMode) return url;
  return url + (url.includes("?") ? "&" : "?") + "live=true";
}

async function fetchJson<T>(url: string): Promise<T> {
  const res = await fetch(appendLive(url));
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`API error ${res.status}: ${err}`);
  }
  return res.json();
}

export interface TableNode {
  id: string;
  name: string;
  full_name: string;
  table_type: string;
  owner: string | null;
  comment: string | null;
  columns: { name: string; type: string; nullable: boolean }[];
  created_at: string | null;
  updated_at: string | null;
  upstream_count: number;
  downstream_count: number;
}

export interface LineageEdge {
  source: string;
  target: string;
}

export interface ColumnLineageEdge {
  source_table: string;
  source_column: string;
  target_table: string;
  target_column: string;
}

export interface LineageResponse {
  nodes: TableNode[];
  edges: LineageEdge[];
  cached?: boolean;
  cached_at?: string | null;
  cache_expires_at?: string | null;
  fetch_duration_ms?: number | null;
}

export interface ColumnLineageResponse {
  edges: ColumnLineageEdge[];
}

export interface UserInfo {
  email: string | null;
  isAdmin: boolean;
}

export const api = {
  getUserInfo: () => fetchJson<UserInfo>(`${BASE}/user-info`),

  getCatalogs: () => fetchJson<{ catalogs: string[] }>(`${BASE}/catalogs`),

  getSchemas: (catalog: string) =>
    fetchJson<{ schemas: string[] }>(`${BASE}/schemas?catalog=${encodeURIComponent(catalog)}`),

  getLineage: (catalog: string, schema: string) =>
    fetchJson<LineageResponse>(
      `${BASE}/lineage?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}`
    ),

  getColumnLineage: (catalog: string, schema: string, table: string, column: string) =>
    fetchJson<ColumnLineageResponse>(
      `${BASE}/column-lineage?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}&column=${encodeURIComponent(column)}`
    ),
};
