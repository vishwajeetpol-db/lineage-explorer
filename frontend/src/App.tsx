import { useCallback, useEffect, useRef, useState } from "react";
import { ReactFlowProvider } from "reactflow";
import Toolbar from "./components/layout/Toolbar";
import LineageCanvas from "./components/graph/LineageCanvas";
import LandingPage from "./components/LandingPage";
import AdminDashboard from "./components/AdminDashboard";
import { useLineageStore } from "./store/lineageStore";
import { api, setLiveMode } from "./api/client";

const TABLE_LOAD_MAX_RETRIES = 3;
const TABLE_LOAD_RETRY_DELAY = 2000; // ms between retries

export default function App() {
  const focusTable = useLineageStore((s) => s.focusTable);
  const catalog = useLineageStore((s) => s.catalog);
  const schema = useLineageStore((s) => s.schema);
  const liveMode = useLineageStore((s) => s.liveMode);
  const isAdmin = useLineageStore((s) => s.isAdmin);
  const retryCount = useRef(0);
  const lineageAbortRef = useRef<AbortController | null>(null);
  const [adminPage, setAdminPage] = useState(false);

  // Fetch user info (admin status) on mount, then process deep link if present
  useEffect(() => {
    api.getUserInfo()
      .then((info) => useLineageStore.getState().setIsAdmin(info.isAdmin))
      .catch(() => useLineageStore.getState().setIsAdmin(false))
      .finally(() => {
        const params = new URLSearchParams(window.location.search);

        // Admin page: ?admin=true opens standalone admin dashboard
        if (params.get("admin") === "true") {
          setAdminPage(true);
          return;
        }

        // Deep link: ?table=catalog.schema.table jumps straight to lineage
        const table = params.get("table");
        if (table && table.split(".").length === 3) {
          const parts = table.split(".");
          useLineageStore.getState().setFocusTable(table);
          window.history.replaceState({}, "", window.location.pathname);
          setLiveMode(false);
          useLineageStore.setState({ loading: true, error: null });
          api.getLineage(parts[0], parts[1])
            .then((data) => {
              useLineageStore.getState().setLineageData({
                nodes: data.nodes,
                edges: data.edges,
                cached: data.cached,
                cachedAt: data.cached_at,
                cacheExpiresAt: data.cache_expires_at,
                fetchDurationMs: data.fetch_duration_ms,
              });
            })
            .catch((err: any) => {
              useLineageStore.getState().setError(err.message || "Failed to load lineage data");
            });
        }
      });
  }, []);

  // Load all tables on mount with retry — only retry on transient failures.
  // Auth/permission errors (4xx) are NOT retried; they signal a real config issue.
  const loadTables = useCallback(() => {
    useLineageStore.getState().setAllTablesLoading(true);
    api.getTables()
      .then((r) => {
        if (r.tables.length > 0) {
          retryCount.current = 0;
          useLineageStore.getState().setAllTables(r.tables);
        } else if (retryCount.current < TABLE_LOAD_MAX_RETRIES) {
          // Empty result — backend might still be preloading. Retry.
          retryCount.current++;
          setTimeout(loadTables, TABLE_LOAD_RETRY_DELAY);
        } else {
          useLineageStore.getState().setAllTables([]);
        }
      })
      .catch((e: any) => {
        console.error("Failed to load tables:", e);
        const msg = String(e?.message || "");
        const is4xx = /API error 4\d\d/.test(msg);
        if (!is4xx && retryCount.current < TABLE_LOAD_MAX_RETRIES) {
          retryCount.current++;
          setTimeout(loadTables, TABLE_LOAD_RETRY_DELAY);
        } else {
          useLineageStore.getState().setAllTablesLoading(false);
        }
      });
  }, []);

  useEffect(() => {
    loadTables();
  }, [loadTables]);

  // Auto-fetch lineage when focusTable is selected. Cancels any in-flight
  // request so quick table switches don't race.
  const fetchLineage = useCallback(async (cat: string, sch: string) => {
    setLiveMode(useLineageStore.getState().liveMode);
    lineageAbortRef.current?.abort();
    const controller = new AbortController();
    lineageAbortRef.current = controller;
    useLineageStore.setState({ loading: true, error: null });
    try {
      const data = await api.getLineage(cat, sch, controller.signal);
      if (controller.signal.aborted) return;
      useLineageStore.getState().setLineageData({
        nodes: data.nodes,
        edges: data.edges,
        cached: data.cached,
        cachedAt: data.cached_at,
        cacheExpiresAt: data.cache_expires_at,
        fetchDurationMs: data.fetch_duration_ms,
      });
    } catch (err: any) {
      if (err?.name === "AbortError") return;
      useLineageStore.getState().setError(err.message || "Failed to load lineage data");
    }
  }, []);

  // Re-fetch lineage immediately when live mode is toggled (if a table is already selected)
  const prevLiveMode = useRef(liveMode);
  useEffect(() => {
    if (prevLiveMode.current !== liveMode && focusTable && catalog && schema) {
      fetchLineage(catalog, schema);
    }
    prevLiveMode.current = liveMode;
  }, [liveMode, focusTable, catalog, schema, fetchLineage]);

  const handleSelectTable = useCallback((fqdn: string) => {
    useLineageStore.getState().setFocusTable(fqdn);
    const parts = fqdn.split(".");
    fetchLineage(parts[0], parts[1]);
  }, [fetchLineage]);

  const handleGenerate = useCallback(async () => {
    if (!catalog || !schema) return;
    fetchLineage(catalog, schema);
  }, [catalog, schema, fetchLineage]);

  // Standalone admin dashboard page
  if (adminPage) {
    if (!isAdmin) {
      return (
        <div className="h-screen w-screen flex items-center justify-center bg-surface">
          <div className="text-center">
            <div className="text-red-400 text-[14px] font-medium mb-2">Access Denied</div>
            <div className="text-slate-500 text-[13px]">Admin dashboard is only available to workspace admins.</div>
          </div>
        </div>
      );
    }
    return <AdminDashboard open={true} onClose={() => window.close()} />;
  }

  // Landing page when no table is selected
  if (!focusTable) {
    return <LandingPage onSelectTable={handleSelectTable} />;
  }

  return (
    <ReactFlowProvider>
      <div className="h-screen w-screen flex flex-col overflow-hidden bg-surface">
        <Toolbar onGenerate={handleGenerate} />
        <div className="flex-1 relative">
          <LineageCanvas />
        </div>
      </div>
    </ReactFlowProvider>
  );
}
