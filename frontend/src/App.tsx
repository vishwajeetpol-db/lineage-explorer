import { useCallback } from "react";
import { ReactFlowProvider } from "reactflow";
import Toolbar from "./components/layout/Toolbar";
import LineageCanvas from "./components/graph/LineageCanvas";
import { useLineageStore } from "./store/lineageStore";
import { api } from "./api/client";

export default function App() {
  const { catalog, schema, setLineageData, setLoading, setError } = useLineageStore();

  const handleGenerate = useCallback(async () => {
    if (!catalog || !schema) return;
    setLoading(true);
    setError(null);
    try {
      const data = await api.getLineage(catalog, schema);
      setLineageData(data.nodes, data.edges);
    } catch (err: any) {
      setError(err.message || "Failed to load lineage data");
    }
  }, [catalog, schema, setLineageData, setLoading, setError]);

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
