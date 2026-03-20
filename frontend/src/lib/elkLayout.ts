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
  const elkNodes = nodes.map((node) => {
    const isExpanded = expandedNodes.has(node.id);
    const columnCount = node.data?.columns?.length || 0;
    const width = isExpanded ? EXPANDED_WIDTH : COMPACT_WIDTH;
    const height = isExpanded
      ? EXPANDED_BASE_HEIGHT + columnCount * COLUMN_ROW_HEIGHT + 12
      : COMPACT_HEIGHT;

    return {
      id: node.id,
      width,
      height,
    };
  });

  const elkEdges = edges.map((edge, i) => ({
    id: edge.id || `e-${i}`,
    sources: [edge.source],
    targets: [edge.target],
  }));

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
    children: elkNodes,
    edges: elkEdges,
  });

  const positionedNodes = nodes.map((node) => {
    const elkNode = graph.children?.find((n) => n.id === node.id);

    return {
      ...node,
      position: {
        x: elkNode?.x || 0,
        y: elkNode?.y || 0,
      },
    };
  });

  return { nodes: positionedNodes, edges };
}
