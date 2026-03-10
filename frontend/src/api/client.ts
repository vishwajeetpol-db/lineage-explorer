const BASE = "/api";

async function fetchJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
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
}

export interface ColumnLineageResponse {
  edges: ColumnLineageEdge[];
}

export const api = {
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
