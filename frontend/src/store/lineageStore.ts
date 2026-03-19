import { create } from "zustand";
import type { TableNode, LineageEdge, ColumnLineageEdge } from "../api/client";

interface LineageState {
  // Selectors
  catalog: string;
  schema: string;
  columnLineageEnabled: boolean;
  liveMode: boolean;

  // Data
  catalogs: string[];
  schemas: string[];
  nodes: TableNode[];
  edges: LineageEdge[];
  columnEdges: ColumnLineageEdge[];

  // Cache metadata
  cached: boolean;
  cachedAt: string | null;

  // UI state
  loading: boolean;
  error: string | null;
  expandedNodes: Set<string>;
  selectedNode: string | null;
  selectedColumn: { table: string; column: string } | null;
  hoveredNode: string | null;
  searchQuery: string;
  searchOpen: boolean;

  // Actions
  setCatalog: (catalog: string) => void;
  setSchema: (schema: string) => void;
  setColumnLineageEnabled: (enabled: boolean) => void;
  setLiveMode: (live: boolean) => void;
  setCatalogs: (catalogs: string[]) => void;
  setSchemas: (schemas: string[]) => void;
  setLineageData: (nodes: TableNode[], edges: LineageEdge[], cached?: boolean, cachedAt?: string | null) => void;
  setColumnEdges: (edges: ColumnLineageEdge[]) => void;
  setLoading: (loading: boolean) => void;
  setError: (error: string | null) => void;
  toggleNodeExpanded: (nodeId: string) => void;
  setSelectedNode: (nodeId: string | null) => void;
  setSelectedColumn: (col: { table: string; column: string } | null) => void;
  setHoveredNode: (nodeId: string | null) => void;
  setSearchQuery: (query: string) => void;
  setSearchOpen: (open: boolean) => void;
  reset: () => void;
}

export const useLineageStore = create<LineageState>((set) => ({
  catalog: "",
  schema: "",
  columnLineageEnabled: false,
  liveMode: false,
  catalogs: [],
  schemas: [],
  nodes: [],
  edges: [],
  columnEdges: [],
  cached: false,
  cachedAt: null,
  loading: false,
  error: null,
  expandedNodes: new Set(),
  selectedNode: null,
  selectedColumn: null,
  hoveredNode: null,
  searchQuery: "",
  searchOpen: false,

  setCatalog: (catalog) => set({ catalog, schema: "", schemas: [], nodes: [], edges: [], columnEdges: [], expandedNodes: new Set(), selectedNode: null, selectedColumn: null, cached: false, cachedAt: null }),
  setSchema: (schema) => set({ schema, nodes: [], edges: [], columnEdges: [], expandedNodes: new Set(), selectedNode: null, selectedColumn: null, cached: false, cachedAt: null }),
  setColumnLineageEnabled: (enabled) => set({ columnLineageEnabled: enabled, columnEdges: [], selectedColumn: null, expandedNodes: new Set() }),
  setLiveMode: (live) => set({ liveMode: live }),
  setCatalogs: (catalogs) => set({ catalogs }),
  setSchemas: (schemas) => set({ schemas }),
  setLineageData: (nodes, edges, cached, cachedAt) => set({ nodes, edges, loading: false, error: null, cached: cached ?? false, cachedAt: cachedAt ?? null }),
  setColumnEdges: (columnEdges) => set({ columnEdges }),
  setLoading: (loading) => set({ loading }),
  setError: (error) => set({ error, loading: false }),
  toggleNodeExpanded: (nodeId) =>
    set((state) => {
      const next = new Set(state.expandedNodes);
      if (next.has(nodeId)) {
        next.delete(nodeId);
        // Clear column selection if collapsing the selected table
        const newSelectedColumn =
          state.selectedColumn?.table === nodeId ? null : state.selectedColumn;
        return { expandedNodes: next, selectedColumn: newSelectedColumn, columnEdges: newSelectedColumn ? state.columnEdges : [] };
      } else {
        next.add(nodeId);
        return { expandedNodes: next };
      }
    }),
  setSelectedNode: (nodeId) => set({ selectedNode: nodeId }),
  setSelectedColumn: (col) => set({ selectedColumn: col }),
  setHoveredNode: (nodeId) => set({ hoveredNode: nodeId }),
  setSearchQuery: (searchQuery) => set({ searchQuery }),
  setSearchOpen: (searchOpen) => set({ searchOpen }),
  reset: () =>
    set({
      nodes: [],
      edges: [],
      columnEdges: [],
      expandedNodes: new Set(),
      selectedNode: null,
      selectedColumn: null,
      hoveredNode: null,
      loading: false,
      error: null,
    }),
}));
