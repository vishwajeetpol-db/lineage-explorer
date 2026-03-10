import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react";
import ReactFlow, {
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  useReactFlow,
  type Node,
  type Edge,
  type NodeTypes,
  type EdgeTypes,
} from "reactflow";
import "reactflow/dist/style.css";
import { AnimatePresence, motion } from "framer-motion";
import { useLineageStore } from "../../store/lineageStore";
import { layoutGraph } from "../../lib/elkLayout";

import TableNodeComponent from "./TableNode";
import AnimatedEdge from "./AnimatedEdge";
import TableTooltip from "../ui/TableTooltip";
import SearchDialog from "../ui/SearchDialog";
import Skeleton from "../ui/Skeleton";

const nodeTypes: NodeTypes = {
  tableNode: TableNodeComponent,
};

const edgeTypes: EdgeTypes = {
  animated: AnimatedEdge,
};

function LineageCanvas() {
  const {
    nodes: rawNodes,
    edges: rawEdges,
    columnEdges,
    expandedNodes,
    selectedNode,
    selectedColumn,
    hoveredNode,
    loading,
    error,
    columnLineageEnabled,
    setSelectedNode,
    setSelectedColumn,
    setColumnEdges,
    catalog,
    schema,
  } = useLineageStore();

  const [flowNodes, setFlowNodes] = useState<Node[]>([]);
  const [flowEdges, setFlowEdges] = useState<Edge[]>([]);
  const [tooltipData, setTooltipData] = useState<{
    node: (typeof rawNodes)[0];
    position: { x: number; y: number };
  } | null>(null);
  const reactFlowInstance = useReactFlow();
  const tooltipTimer = useRef<ReturnType<typeof setTimeout>>();

  // Compute connected nodes for highlighting
  const connectedNodes = useMemo(() => {
    if (!selectedNode && !hoveredNode) return new Set<string>();
    const target = selectedNode || hoveredNode;
    const connected = new Set<string>();
    if (target) {
      connected.add(target);
      // Traverse upstream
      const findUpstream = (nodeId: string) => {
        rawEdges.forEach((e) => {
          if (e.target === nodeId && !connected.has(e.source)) {
            connected.add(e.source);
            findUpstream(e.source);
          }
        });
      };
      // Traverse downstream
      const findDownstream = (nodeId: string) => {
        rawEdges.forEach((e) => {
          if (e.source === nodeId && !connected.has(e.target)) {
            connected.add(e.target);
            findDownstream(e.target);
          }
        });
      };
      findUpstream(target);
      findDownstream(target);
    }
    return connected;
  }, [selectedNode, hoveredNode, rawEdges]);

  const connectedEdges = useMemo(() => {
    if (!selectedNode && !hoveredNode) return new Set<string>();
    const edgeSet = new Set<string>();
    rawEdges.forEach((e) => {
      if (connectedNodes.has(e.source) && connectedNodes.has(e.target)) {
        edgeSet.add(`${e.source}->${e.target}`);
      }
    });
    return edgeSet;
  }, [connectedNodes, rawEdges, selectedNode, hoveredNode]);

  // Compute column lineage client-side (instant, no API call)
  useEffect(() => {
    if (!selectedColumn) {
      setColumnEdges([]);
      return;
    }
    const { table: selTable, column: selCol } = selectedColumn;
    const colLower = selCol.toLowerCase();
    const inferred: { source_table: string; source_column: string; target_table: string; target_column: string }[] = [];

    // Build column lookup: tableId -> Set<columnName>
    const colsByTable = new Map<string, Set<string>>();
    for (const node of rawNodes) {
      colsByTable.set(node.id, new Set(node.columns.map((c) => c.name.toLowerCase())));
    }

    // Find upstream tables (tables that feed into this table)
    const upstreamTables = new Set<string>();
    const downstreamTables = new Set<string>();
    for (const edge of rawEdges) {
      if (edge.target === selTable) upstreamTables.add(edge.source);
      if (edge.source === selTable) downstreamTables.add(edge.target);
    }

    // Match: if upstream table has same column name, it flows into this table
    for (const upTable of upstreamTables) {
      if (colsByTable.get(upTable)?.has(colLower)) {
        inferred.push({ source_table: upTable, source_column: selCol, target_table: selTable, target_column: selCol });
      }
    }

    // Match: if downstream table has same column name, this table feeds it
    for (const downTable of downstreamTables) {
      if (colsByTable.get(downTable)?.has(colLower)) {
        inferred.push({ source_table: selTable, source_column: selCol, target_table: downTable, target_column: selCol });
      }
    }

    setColumnEdges(inferred);
  }, [selectedColumn, rawNodes, rawEdges, setColumnEdges]);

  // Store laid-out positions so we can re-style without re-layout
  const layoutPositions = useRef<Map<string, { x: number; y: number }>>(new Map());
  const isInitialLayout = useRef(true);

  // Layout nodes with ELK — only when data or expand state changes
  useEffect(() => {
    if (rawNodes.length === 0) {
      setFlowNodes([]);
      setFlowEdges([]);
      layoutPositions.current.clear();
      isInitialLayout.current = true;
      return;
    }

    const rfNodes: Node[] = rawNodes.map((n) => ({
      id: n.id,
      type: "tableNode",
      position: { x: 0, y: 0 },
      data: {
        ...n,
        isExpanded: expandedNodes.has(n.id),
        isSelected: false,
        isHighlighted: true,
        isDimmed: false,
      },
    }));

    const rfEdges: Edge[] = rawEdges.map((e) => ({
      id: `e-${e.source}-${e.target}`,
      source: e.source,
      target: e.target,
      type: "animated",
      data: { isHighlighted: false, isDimmed: false, isColumnEdge: false },
    }));

    layoutGraph(rfNodes, rfEdges, expandedNodes).then(({ nodes, edges }) => {
      // Cache positions
      const positions = new Map<string, { x: number; y: number }>();
      nodes.forEach((n) => positions.set(n.id, { ...n.position }));
      layoutPositions.current = positions;

      setFlowNodes(nodes);
      setFlowEdges(edges);

      // Only fitView on initial layout (new data loaded)
      if (isInitialLayout.current) {
        isInitialLayout.current = false;
        setTimeout(() => {
          reactFlowInstance.fitView({ padding: 0.15, duration: 400 });
        }, 50);
      }
    });
  }, [rawNodes, rawEdges, expandedNodes, reactFlowInstance]);

  // Update node/edge styling on select/hover — no re-layout, no fitView
  useEffect(() => {
    if (flowNodes.length === 0) return;

    const hasHighlight = selectedNode || hoveredNode;

    setFlowNodes((prev) =>
      prev.map((n) => ({
        ...n,
        data: {
          ...n.data,
          isSelected: n.id === selectedNode,
          isHighlighted: !hasHighlight || connectedNodes.has(n.id),
          isDimmed: !!hasHighlight && !connectedNodes.has(n.id),
        },
      }))
    );

    setFlowEdges((prev) => {
      // Keep only table-level edges, rebuild styling
      const tableEdges = prev
        .filter((e) => !e.id.startsWith("col-e-"))
        .map((e) => {
          const edgeKey = `${e.source}->${e.target}`;
          const isHl = !hasHighlight || connectedEdges.has(edgeKey);
          return {
            ...e,
            data: {
              isHighlighted: !!hasHighlight && isHl,
              isDimmed: !!hasHighlight && !isHl,
              isColumnEdge: false,
            },
          };
        });

      // Add column-level edges
      if (selectedColumn && columnEdges.length > 0) {
        columnEdges.forEach((ce, i) => {
          tableEdges.push({
            id: `col-e-${i}`,
            source: ce.source_table,
            sourceHandle: `${ce.source_table}__col__${ce.source_column}__source`,
            target: ce.target_table,
            targetHandle: `${ce.target_table}__col__${ce.target_column}__target`,
            type: "animated",
            data: { isHighlighted: false, isDimmed: false, isColumnEdge: true },
          });
        });
      }

      return tableEdges;
    });
  }, [selectedNode, hoveredNode, connectedNodes, connectedEdges, columnEdges, selectedColumn]);

  // Tooltip on hover
  useEffect(() => {
    if (hoveredNode && !selectedNode) {
      tooltipTimer.current = setTimeout(() => {
        const node = rawNodes.find((n) => n.id === hoveredNode);
        const rfNode = flowNodes.find((n) => n.id === hoveredNode);
        if (node && rfNode) {
          const viewportPos = reactFlowInstance.flowToScreenPosition({
            x: rfNode.position.x + (rfNode.style?.width as number || 220),
            y: rfNode.position.y,
          });
          setTooltipData({ node, position: viewportPos });
        }
      }, 300);
    } else {
      clearTimeout(tooltipTimer.current);
      setTooltipData(null);
    }
    return () => clearTimeout(tooltipTimer.current);
  }, [hoveredNode, selectedNode, rawNodes, flowNodes, reactFlowInstance]);

  const handlePaneClick = useCallback(() => {
    setSelectedNode(null);
    setSelectedColumn(null);
  }, [setSelectedNode, setSelectedColumn]);

  const handleNodeClick = useCallback(
    (_: React.MouseEvent, node: Node) => {
      if (!columnLineageEnabled) {
        setSelectedNode(selectedNode === node.id ? null : node.id);
      }
    },
    [columnLineageEnabled, selectedNode, setSelectedNode]
  );

  const handleSearchSelect = useCallback(
    (nodeId: string) => {
      const rfNode = flowNodes.find((n) => n.id === nodeId);
      if (rfNode) {
        reactFlowInstance.setCenter(
          rfNode.position.x + 110,
          rfNode.position.y + 24,
          { zoom: 1.5, duration: 600 }
        );
        setSelectedNode(nodeId);
      }
    },
    [flowNodes, reactFlowInstance, setSelectedNode]
  );

  if (loading) return <Skeleton />;

  if (error) {
    return (
      <div className="absolute inset-0 flex items-center justify-center">
        <div className="text-center">
          <div className="text-red-400 text-[14px] font-medium mb-2">Error loading lineage</div>
          <div className="text-slate-500 text-[13px] max-w-[400px]">{error}</div>
        </div>
      </div>
    );
  }

  if (rawNodes.length === 0 && !loading) {
    return (
      <div className="absolute inset-0 flex items-center justify-center">
        <div className="text-center">
          {/* Animated logo */}
          <motion.div
            animate={{ opacity: [0.15, 0.25, 0.15] }}
            transition={{ duration: 4, repeat: Infinity, ease: "easeInOut" }}
            className="mb-6"
          >
            <div className="w-16 h-16 mx-auto rounded-2xl bg-gradient-to-br from-accent/20 to-purple-500/20 border border-white/[0.04] flex items-center justify-center">
              <GitBranchPlaceholder />
            </div>
          </motion.div>
          <div className="text-slate-400 text-[15px] font-medium tracking-tight">
            Select a catalog and schema to explore lineage
          </div>
          <div className="text-slate-600 text-[12px] mt-2 max-w-[280px] leading-relaxed">
            Choose from the dropdowns above, then click Generate Lineage to visualize table dependencies
          </div>
        </div>
      </div>
    );
  }

  return (
    <>
      <ReactFlow
        nodes={flowNodes}
        edges={flowEdges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onPaneClick={handlePaneClick}
        onNodeClick={handleNodeClick}
        fitView
        fitViewOptions={{ padding: 0.15 }}
        minZoom={0.1}
        maxZoom={3}
        proOptions={{ hideAttribution: true }}
        defaultEdgeOptions={{ animated: false }}
      >
        <Background
          variant={BackgroundVariant.Dots}
          gap={24}
          size={0.8}
          color="rgba(255,255,255,0.03)"
        />
        <Controls showInteractive={false} />
        <MiniMap
          nodeStrokeWidth={3}
          zoomable
          pannable
          style={{ width: 160, height: 100 }}
        />
      </ReactFlow>

      <AnimatePresence>
        {tooltipData && (
          <TableTooltip
            node={tooltipData.node}
            position={tooltipData.position}
          />
        )}
      </AnimatePresence>

      <SearchDialog onSelectNode={handleSearchSelect} />
    </>
  );
}

function GitBranchPlaceholder() {
  return (
    <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" className="text-slate-600 mx-auto">
      <line x1="6" y1="3" x2="6" y2="15" />
      <circle cx="18" cy="6" r="3" />
      <circle cx="6" cy="18" r="3" />
      <path d="M18 9a9 9 0 0 1-9 9" />
    </svg>
  );
}

export default memo(LineageCanvas);
