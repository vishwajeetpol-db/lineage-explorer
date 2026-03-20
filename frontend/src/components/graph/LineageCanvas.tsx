import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react";
import ReactFlow, {
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  useReactFlow,
  useUpdateNodeInternals,
  applyNodeChanges,
  type Node,
  type Edge,
  type NodeTypes,
  type EdgeTypes,
  type NodeChange,
} from "reactflow";
import "reactflow/dist/style.css";
import { AnimatePresence, motion } from "framer-motion";
import { RotateCcw } from "lucide-react";
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
  } = useLineageStore();

  const [flowNodes, setFlowNodes] = useState<Node[]>([]);
  const [flowEdges, setFlowEdges] = useState<Edge[]>([]);
  const [tooltipData, setTooltipData] = useState<{
    node: (typeof rawNodes)[0];
    position: { x: number; y: number };
  } | null>(null);
  const [revealCounter, setRevealCounter] = useState(-1);
  const [layoutKey, setLayoutKey] = useState(0);
  const reactFlowInstance = useReactFlow();
  const updateNodeInternals = useUpdateNodeInternals();
  const tooltipTimer = useRef<ReturnType<typeof setTimeout>>();
  const flowNodesRef = useRef<Node[]>(flowNodes);
  flowNodesRef.current = flowNodes;

  // Allow dragging nodes by applying position changes
  const onNodesChange = useCallback(
    (changes: NodeChange[]) => {
      setFlowNodes((nds) => applyNodeChanges(changes, nds));
    },
    [setFlowNodes]
  );

  // Pre-compute adjacency maps for O(1) lookups (avoids O(n²) on every hover/select)
  const adjacency = useMemo(() => {
    const upstream = new Map<string, string[]>();  // target -> sources
    const downstream = new Map<string, string[]>(); // source -> targets
    for (const e of rawEdges) {
      if (!upstream.has(e.target)) upstream.set(e.target, []);
      upstream.get(e.target)!.push(e.source);
      if (!downstream.has(e.source)) downstream.set(e.source, []);
      downstream.get(e.source)!.push(e.target);
    }
    return { upstream, downstream };
  }, [rawEdges]);

  // Compute connected nodes for highlighting using adjacency maps
  const connectedNodes = useMemo(() => {
    if (!selectedNode && !hoveredNode) return new Set<string>();
    const target = selectedNode || hoveredNode;
    const connected = new Set<string>();
    if (target) {
      connected.add(target);
      const findUpstream = (nodeId: string) => {
        for (const src of adjacency.upstream.get(nodeId) || []) {
          if (!connected.has(src)) {
            connected.add(src);
            findUpstream(src);
          }
        }
      };
      const findDownstream = (nodeId: string) => {
        for (const tgt of adjacency.downstream.get(nodeId) || []) {
          if (!connected.has(tgt)) {
            connected.add(tgt);
            findDownstream(tgt);
          }
        }
      };
      findUpstream(target);
      findDownstream(target);
    }
    return connected;
  }, [selectedNode, hoveredNode, adjacency]);

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

  // Compute column lineage client-side with full transitive traversal
  useEffect(() => {
    if (!selectedColumn) {
      setColumnEdges([]);
      return;
    }
    const { table: selTable, column: selCol } = selectedColumn;
    const colLower = selCol.toLowerCase();
    const inferred: { source_table: string; source_column: string; target_table: string; target_column: string }[] = [];
    const edgesSeen = new Set<string>();

    const colsByTable = new Map<string, Set<string>>();
    for (const node of rawNodes) {
      colsByTable.set(node.id, new Set(node.columns.map((c) => c.name.toLowerCase())));
    }

    const addEdge = (src: string, tgt: string) => {
      const key = `${src}|${tgt}`;
      if (edgesSeen.has(key)) return;
      edgesSeen.add(key);
      inferred.push({ source_table: src, source_column: selCol, target_table: tgt, target_column: selCol });
    };

    const traceUpstream = (tableId: string, visited: Set<string>) => {
      if (visited.has(tableId)) return;
      visited.add(tableId);
      for (const edge of rawEdges) {
        if (edge.target === tableId && colsByTable.get(edge.source)?.has(colLower)) {
          addEdge(edge.source, tableId);
          traceUpstream(edge.source, visited);
        }
      }
    };

    const traceDownstream = (tableId: string, visited: Set<string>) => {
      if (visited.has(tableId)) return;
      visited.add(tableId);
      for (const edge of rawEdges) {
        if (edge.source === tableId && colsByTable.get(edge.target)?.has(colLower)) {
          addEdge(tableId, edge.target);
          traceDownstream(edge.target, visited);
        }
      }
    };

    traceUpstream(selTable, new Set<string>());
    traceDownstream(selTable, new Set<string>());

    setColumnEdges(inferred);
  }, [selectedColumn, rawNodes, rawEdges, setColumnEdges]);

  // =========================================================================
  // LAYOUT EFFECT — runs ONLY when raw data changes or reset is pressed.
  // NEVER runs on expand/collapse (expandedNodes is NOT a dependency).
  // =========================================================================
  useEffect(() => {
    if (rawNodes.length === 0) {
      setFlowNodes([]);
      setFlowEdges([]);
      return;
    }

    const rfNodes: Node[] = rawNodes.map((n) => ({
      id: n.id,
      type: "tableNode",
      position: { x: 0, y: 0 },
      data: {
        ...n,
        isExpanded: false,
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
      data: { isHighlighted: false, isDimmed: false, isColumnEdge: false, isVisible: true },
    }));

    // ELK layout — always uses collapsed dimensions for stable positioning
    layoutGraph(rfNodes, rfEdges, new Set()).then(({ nodes, edges }) => {
      // Staggered reveal: sort by x-position (left-to-right = topological order)
      const sorted = [...nodes].sort((a, b) => a.position.x - b.position.x);
      const orderMap = new Map<string, number>();
      sorted.forEach((n, i) => orderMap.set(n.id, i));

      const revealNodes = nodes.map((n) => ({
        ...n,
        data: {
          ...n.data,
          revealOrder: orderMap.get(n.id) ?? 0,
          isRevealed: false,
        },
      }));

      const revealEdges = edges.map((e) => ({
        ...e,
        data: { ...e.data, isVisible: false },
      }));

      setFlowNodes(revealNodes);
      setFlowEdges(revealEdges);
      setRevealCounter(-1);

      setTimeout(() => {
        reactFlowInstance.fitView({ padding: 0.15, duration: 400 });
      }, 50);
    });
    // expandedNodes is intentionally NOT in the dependency array.
    // Expand/collapse is handled by a separate effect below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rawNodes, rawEdges, reactFlowInstance, layoutKey]);

  // =========================================================================
  // EXPAND/COLLAPSE EFFECT — updates node data in place without re-running ELK.
  // After framer-motion animation completes (~350ms), force React Flow to
  // recalculate handle positions so edges route correctly.
  // =========================================================================
  useEffect(() => {
    setFlowNodes((prev) => {
      if (prev.length === 0) return prev;
      return prev.map((n) => ({
        ...n,
        data: {
          ...n.data,
          isExpanded: expandedNodes.has(n.id),
        },
      }));
    });

    // Wait for framer-motion AnimatePresence height animation to finish,
    // then tell React Flow to re-measure all handle positions.
    const timer = setTimeout(() => {
      flowNodesRef.current.forEach((n) => updateNodeInternals(n.id));
    }, 350);
    return () => clearTimeout(timer);
  }, [expandedNodes, updateNodeInternals]);

  // Staggered reveal: increment counter every 50ms to reveal nodes left-to-right
  useEffect(() => {
    if (revealCounter < 0 && flowNodes.length > 0 && flowNodes.some((n) => !n.data.isRevealed)) {
      setRevealCounter(0);
      return;
    }
    if (revealCounter < 0) return;

    const maxOrder = Math.max(...flowNodes.map((n) => n.data.revealOrder ?? 0), 0);
    if (revealCounter > maxOrder) return;

    const timer = setInterval(() => {
      setRevealCounter((c) => {
        if (c > maxOrder) {
          clearInterval(timer);
          return c;
        }
        return c + 1;
      });
    }, 50);
    return () => clearInterval(timer);
  }, [revealCounter, flowNodes.length]);

  // Update revealed state on nodes and edge visibility based on revealCounter
  useEffect(() => {
    if (revealCounter < 0) return;

    setFlowNodes((prev) =>
      prev.map((n) => ({
        ...n,
        data: {
          ...n.data,
          isRevealed: (n.data.revealOrder ?? 0) <= revealCounter,
        },
      }))
    );

    setFlowEdges((prev) =>
      prev.map((e) => {
        const currentNodes = flowNodesRef.current;
        const sourceNode = currentNodes.find((n) => n.id === e.source);
        const targetNode = currentNodes.find((n) => n.id === e.target);
        const sourceRevealed = (sourceNode?.data.revealOrder ?? 0) <= revealCounter;
        const targetRevealed = (targetNode?.data.revealOrder ?? 0) <= revealCounter;
        return {
          ...e,
          data: {
            ...e.data,
            isVisible: sourceRevealed && targetRevealed,
          },
        };
      })
    );
  }, [revealCounter]);

  // Update node/edge styling on select/hover — preserves isVisible and isRevealed
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
      const tableEdges = prev
        .filter((e) => !e.id.startsWith("col-e-"))
        .map((e) => {
          const edgeKey = `${e.source}->${e.target}`;
          const isHl = !hasHighlight || connectedEdges.has(edgeKey);
          return {
            ...e,
            data: {
              ...e.data, // preserves isVisible
              isHighlighted: !!hasHighlight && isHl,
              isDimmed: !!hasHighlight && !isHl,
              isColumnEdge: false,
            },
          };
        });

      if (selectedColumn && columnEdges.length > 0) {
        columnEdges.forEach((ce, i) => {
          tableEdges.push({
            id: `col-e-${i}`,
            source: ce.source_table,
            sourceHandle: `${ce.source_table}__col__${ce.source_column}__source`,
            target: ce.target_table,
            targetHandle: `${ce.target_table}__col__${ce.target_column}__target`,
            type: "animated",
            data: { isHighlighted: false, isDimmed: false, isColumnEdge: true, isVisible: true },
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
            x: rfNode.position.x + 220,
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

  const handleResetLayout = useCallback(() => {
    setSelectedNode(null);
    setSelectedColumn(null);
    setLayoutKey((k) => k + 1);
  }, [setSelectedNode, setSelectedColumn]);

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
        onNodesChange={onNodesChange}
        onPaneClick={handlePaneClick}
        onNodeClick={handleNodeClick}
        nodesDraggable
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

      {/* Reset Layout button */}
      <button
        onClick={handleResetLayout}
        title="Reset layout"
        className="
          absolute bottom-[140px] left-3 z-10
          flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg
          bg-[#161625]/90 backdrop-blur-md border border-white/[0.06]
          hover:border-white/[0.15] hover:bg-[#1E1E2E]
          text-slate-500 hover:text-slate-300
          transition-all duration-200 group
          shadow-[0_2px_12px_rgba(0,0,0,0.3)]
        "
      >
        <RotateCcw size={13} className="group-hover:rotate-[-180deg] transition-transform duration-500" />
        <span className="text-[10px] font-medium tracking-wide">Reset</span>
      </button>

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
