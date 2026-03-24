import { useCallback } from "react";
import { ReactFlowProvider } from "reactflow";
import Toolbar from "./components/layout/Toolbar";
import LineageCanvas from "./components/graph/LineageCanvas";
import { useLineageStore } from "./store/lineageStore";
import { api, setLiveMode } from "./api/client";

export default function App() {
  const { catalog, schema, liveMode, setLineageData, setError } = useLineageStore();

  const handleGenerate = useCallback(async () => {
    if (!catalog || !schema) return;
    setLiveMode(liveMode);
    // Clear error and set loading in one tick — setError(null) would override loading
    useLineageStore.setState({ loading: true, error: null });
    const minDisplay = liveMode ? 2500 : 1200; // ensure loading animation is visible
    const start = Date.now();
    try {
      const data = await api.getLineage(catalog, schema);
      const elapsed = Date.now() - start;
      if (elapsed < minDisplay) {
        await new Promise((r) => setTimeout(r, minDisplay - elapsed));
      }
      setLineageData(data.nodes, data.edges, data.cached, data.cached_at, data.cache_expires_at, data.fetch_duration_ms);
    } catch (err: any) {
      setError(err.message || "Failed to load lineage data");
    }
  }, [catalog, schema, liveMode, setLineageData, setError]);

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
