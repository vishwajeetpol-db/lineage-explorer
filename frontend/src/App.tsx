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
  const {
    focusTable, catalog, schema, liveMode,
    setFocusTable, setLineageData, setError, setAllTables, setAllTablesLoading,
  } = useLineageStore();
  const setIsAdmin = useLineageStore((s) => s.setIsAdmin);
  const isAdmin = useLineageStore((s) => s.isAdmin);
  const retryCount = useRef(0);
  const [adminPage, setAdminPage] = useState(false);

  // Fetch user info (admin status) on mount, then process deep link if present
  useEffect(() => {
    api.getUserInfo()
      .then((info) => setIsAdmin(info.isAdmin))
      .catch(() => setIsAdmin(false))
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
              setLineageData(data.nodes, data.edges, data.cached, data.cached_at, data.cache_expires_at, data.fetch_duration_ms);
            })
            .catch((err: any) => {
              setError(err.message || "Failed to load lineage data");
            });
        }
      });
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Load all tables on mount with retry logic
  const loadTables = useCallback(() => {
    setAllTablesLoading(true);
    api.getTables()
      .then((r) => {
        if (r.tables.length > 0) {
          retryCount.current = 0;
          setAllTables(r.tables);
        } else if (retryCount.current < TABLE_LOAD_MAX_RETRIES) {
          // Backend might still be preloading — retry
          retryCount.current++;
          setTimeout(loadTables, TABLE_LOAD_RETRY_DELAY);
        } else {
          setAllTables([]);
        }
      })
      .catch((e) => {
        console.error("Failed to load tables:", e);
        if (retryCount.current < TABLE_LOAD_MAX_RETRIES) {
          retryCount.current++;
          setTimeout(loadTables, TABLE_LOAD_RETRY_DELAY);
        } else {
          setAllTablesLoading(false);
        }
      });
  }, [setAllTables, setAllTablesLoading]);

  useEffect(() => {
    loadTables();
  }, [loadTables]);

  // Auto-fetch lineage when focusTable is selected
  const fetchLineage = useCallback(async (cat: string, sch: string) => {
    setLiveMode(liveMode);
    useLineageStore.setState({ loading: true, error: null });
    const minDisplay = liveMode ? 2500 : 1200;
    const start = Date.now();
    try {
      const data = await api.getLineage(cat, sch);
      const elapsed = Date.now() - start;
      if (elapsed < minDisplay) {
        await new Promise((r) => setTimeout(r, minDisplay - elapsed));
      }
      setLineageData(data.nodes, data.edges, data.cached, data.cached_at, data.cache_expires_at, data.fetch_duration_ms);
    } catch (err: any) {
      setError(err.message || "Failed to load lineage data");
    }
  }, [liveMode, setLineageData, setError]);

  // Re-fetch lineage immediately when live mode is toggled (if a table is already selected)
  const prevLiveMode = useRef(liveMode);
  useEffect(() => {
    if (prevLiveMode.current !== liveMode && focusTable && catalog && schema) {
      fetchLineage(catalog, schema);
    }
    prevLiveMode.current = liveMode;
  }, [liveMode, focusTable, catalog, schema, fetchLineage]);

  const handleSelectTable = useCallback((fqdn: string) => {
    setFocusTable(fqdn);
    const parts = fqdn.split(".");
    fetchLineage(parts[0], parts[1]);
  }, [setFocusTable, fetchLineage]);

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
