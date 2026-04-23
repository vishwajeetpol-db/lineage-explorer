import type { ElkNode } from "elkjs/lib/elk.bundled.js";
import type { Node, Edge } from "reactflow";
import ElkWorker from "./elkWorker?worker";

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

// Lazy-spawned long-lived worker. ELK init is heavy (~1s cold),
// so we pay that cost once and reuse across layout calls.
let worker: Worker | null = null;
let nextRequestId = 1;
const pending = new Map<
  number,
  { resolve: (r: ElkNode) => void; reject: (e: Error) => void }
>();

function getWorker(): Worker {
  if (worker) return worker;
  const w = new ElkWorker();
  w.onmessage = (
    e: MessageEvent<{ id: number; result?: ElkNode; error?: string }>
  ) => {
    const handler = pending.get(e.data.id);
    if (!handler) return; // aborted — drop stale result
    pending.delete(e.data.id);
    if (e.data.error) handler.reject(new Error(e.data.error));
    else if (e.data.result) handler.resolve(e.data.result);
    else handler.reject(new Error("Worker returned empty result"));
  };
  w.onerror = (e) => {
    // Reject everything still pending; the worker is in an unknown state
    for (const [, h] of pending) h.reject(new Error(`Worker error: ${e.message}`));
    pending.clear();
    worker = null;
  };
  worker = w;
  return w;
}

function runLayoutInWorker(
  graph: ElkNode,
  signal?: AbortSignal
): Promise<ElkNode> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new DOMException("Layout aborted", "AbortError"));
      return;
    }
    const id = nextRequestId++;
    pending.set(id, { resolve, reject });
    if (signal) {
      const onAbort = () => {
        if (pending.delete(id)) {
          reject(new DOMException("Layout aborted", "AbortError"));
        }
      };
      signal.addEventListener("abort", onAbort, { once: true });
    }
    getWorker().postMessage({ id, graph });
  });
}

export async function layoutGraph(
  nodes: Node[],
  edges: Edge[],
  expandedNodes: Set<string>,
  signal?: AbortSignal
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

  const graph: ElkNode = {
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
  };

  const laidOut = await runLayoutInWorker(graph, signal);

  // Find the bottom of the connected graph
  let graphBottom = 0;
  const graphLeft = 60;
  for (const child of laidOut.children || []) {
    const bottom = (child.y || 0) + (child.height || COMPACT_HEIGHT);
    if (bottom > graphBottom) graphBottom = bottom;
  }

  // Position orphan nodes in rows below the main graph
  // Flow left-to-right, 10 per row, with enough gap to avoid overlap
  const ORPHAN_GAP_Y = 100;
  const ORPHAN_NODE_GAP = 60;
  const ORPHAN_ROW_GAP = 50;
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
    const nameWidth = Math.max(COMPACT_WIDTH, (node.data?.name?.length || 15) * 11 + 120);
    currentX += nameWidth + ORPHAN_NODE_GAP;
    countInRow++;
  });

  const positionedNodes = nodes.map((node) => {
    const elkNode = laidOut.children?.find((n) => n.id === node.id);
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
