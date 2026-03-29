import ELK from "elkjs/lib/elk.bundled.js";
import type { Node, Edge } from "reactflow";

const elk = new ELK();

const COMPACT_WIDTH = 280;
const COMPACT_HEIGHT = 52;
const EXPANDED_BASE_HEIGHT = 56;
const COLUMN_ROW_HEIGHT = 28;
const EXPANDED_WIDTH = 320;

export interface LayoutResult {
  nodes: Node[];
  edges: Edge[];
}

export async function layoutGraph(
  nodes: Node[],
  edges: Edge[],
  expandedNodes: Set<string>
): Promise<LayoutResult> {
  // Separate connected nodes from orphans (no edges at all)
  const connectedIds = new Set<string>();
  for (const edge of edges) {
    connectedIds.add(edge.source);
    connectedIds.add(edge.target);
  }

  const connectedNodes = nodes.filter((n) => connectedIds.has(n.id));
  const orphanNodes = nodes.filter((n) => !connectedIds.has(n.id));

  function toElkNode(node: Node) {
    const isExpanded = expandedNodes.has(node.id);
    const columnCount = node.data?.columns?.length || 0;
    const width = isExpanded ? EXPANDED_WIDTH : COMPACT_WIDTH;
    const height = isExpanded
      ? EXPANDED_BASE_HEIGHT + columnCount * COLUMN_ROW_HEIGHT + 12
      : COMPACT_HEIGHT;
    return { id: node.id, width, height };
  }

  const elkEdges = edges.map((edge, i) => ({
    id: edge.id || `e-${i}`,
    sources: [edge.source],
    targets: [edge.target],
  }));

  // Layout connected nodes with ELK
  const graph = await elk.layout({
    id: "root",
    layoutOptions: {
      "elk.algorithm": "layered",
      "elk.direction": "RIGHT",
      "elk.spacing.nodeNode": "50",
      "elk.layered.spacing.nodeNodeBetweenLayers": "100",
      "elk.layered.crossingMinimization.strategy": "LAYER_SWEEP",
      "elk.layered.crossingMinimization.greedySwitch.type": "TWO_SIDED",
      "elk.layered.nodePlacement.strategy": "NETWORK_SIMPLEX",
      "elk.layered.considerModelOrder.strategy": "NODES_AND_EDGES",
      "elk.padding": "[top=60,left=60,bottom=60,right=60]",
      "elk.layered.mergeEdges": "true",
      "elk.layered.spacing.edgeEdgeBetweenLayers": "20",
      "elk.layered.spacing.edgeNodeBetweenLayers": "30",
      "elk.edgeRouting": "SPLINES",
    },
    children: connectedNodes.map(toElkNode),
    edges: elkEdges,
  });

  // Find the bottom of the connected graph
  let graphBottom = 0;
  let graphLeft = 60;
  for (const child of graph.children || []) {
    const bottom = (child.y || 0) + (child.height || COMPACT_HEIGHT);
    if (bottom > graphBottom) graphBottom = bottom;
  }

  // Position orphan nodes in a grid below the main graph
  const ORPHAN_GAP_Y = 80; // gap between connected graph and orphan section
  const ORPHAN_SPACING_X = 40;
  const ORPHAN_SPACING_Y = 30;
  const ORPHAN_COLS = 3;
  const orphanStartY = connectedNodes.length > 0 ? graphBottom + ORPHAN_GAP_Y : 60;

  const orphanPositions = new Map<string, { x: number; y: number }>();
  orphanNodes.forEach((node, i) => {
    const col = i % ORPHAN_COLS;
    const row = Math.floor(i / ORPHAN_COLS);
    orphanPositions.set(node.id, {
      x: graphLeft + col * (COMPACT_WIDTH + ORPHAN_SPACING_X),
      y: orphanStartY + row * (COMPACT_HEIGHT + ORPHAN_SPACING_Y),
    });
  });

  // Merge positions
  const positionedNodes = nodes.map((node) => {
    const elkNode = graph.children?.find((n) => n.id === node.id);
    const orphanPos = orphanPositions.get(node.id);

    return {
      ...node,
      position: {
        x: elkNode?.x || orphanPos?.x || 0,
        y: elkNode?.y || orphanPos?.y || 0,
      },
    };
  });

  return { nodes: positionedNodes, edges };
}
