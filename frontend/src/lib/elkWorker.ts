import ELK, { type ElkNode } from "elkjs/lib/elk.bundled.js";

const elk = new ELK();

interface LayoutRequest {
  id: number;
  graph: ElkNode;
}

interface LayoutSuccess {
  id: number;
  result: ElkNode;
}

interface LayoutFailure {
  id: number;
  error: string;
}

self.onmessage = async (e: MessageEvent<LayoutRequest>) => {
  const { id, graph } = e.data;
  try {
    const result = await elk.layout(graph);
    const msg: LayoutSuccess = { id, result };
    (self as unknown as Worker).postMessage(msg);
  } catch (err) {
    const msg: LayoutFailure = {
      id,
      error: err instanceof Error ? err.message : String(err),
    };
    (self as unknown as Worker).postMessage(msg);
  }
};

export {};
