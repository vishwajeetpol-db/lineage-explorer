import ELK from "elkjs/lib/elk.bundled.js";
import type { Node, Edge } from "reactflow";

const elk = new ELK();

const COMPACT_WIDTH = 280;
const COMPACT_HEIGHT = 52;
const EXPANDED_BASE_HEIGHT = 56;
const COLUMN_ROW_HEIGHT = 28;
const EXPANDED_WIDTH = 320;
const ENTITY_WIDTH = 200;
const ENTITY_HEIGHT = 44;

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
    if (node.data?.node_type === "entity") {
      return { id: node.id, width: ENTITY_WIDTH, height: ENTITY_HEIGHT };
    }
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
      "elk.spacing.nodeNode": "60",
      "elk.layered.spacing.nodeNodeBetweenLayers": "160",
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

  // Position orphan nodes in rows below the main graph
  // Flow left-to-right, 10 per row, with enough gap to avoid overlap
  const ORPHAN_GAP_Y = 100; // gap between connected graph and orphan section
  const ORPHAN_NODE_GAP = 60; // horizontal gap between orphan nodes
  const ORPHAN_ROW_GAP = 50; // vertical gap between orphan rows
  const ORPHAN_PER_ROW = 10;
  const orphanStartY = connectedNodes.length > 0 ? graphBottom + ORPHAN_GAP_Y : 60;

  const orphanPositions = new Map<string, { x: number; y: number }>();
  let currentX = graphLeft;
  let currentY = orphanStartY;
  let countInRow = 0;

  orphanNodes.forEach((node) => {
    if (countInRow >= ORPHAN_PER_ROW) {
      currentX = graphLeft;
      currentY += COMPACT_HEIGHT + ORPHAN_ROW_GAP;
      countInRow = 0;
    }
    orphanPositions.set(node.id, { x: currentX, y: currentY });
    // Estimate width based on name length (wider names need more space)
    const nameWidth = Math.max(COMPACT_WIDTH, (node.data?.name?.length || 15) * 11 + 120);
    currentX += nameWidth + ORPHAN_NODE_GAP;
    countInRow++;
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
