import type { LineageEdge } from "../api/client";

const isEntityId = (id: string) => id.startsWith("entity:");

/**
 * Collapse entity-mediated edges (table → pipeline → table) into direct
 * table → table edges, mirroring the canvas "Tables" view. Pure table→table
 * edges pass through unchanged. Shared by the in-app preview and the
 * client-side export fallback.
 */
export function collapseToTableEdges(edges: LineageEdge[]): { source: string; target: string }[] {
  const entitySources = new Map<string, Set<string>>();
  const entityTargets = new Map<string, Set<string>>();
  const seen = new Set<string>();
  const out: { source: string; target: string }[] = [];

  const add = (source: string, target: string) => {
    if (source === target) return;
    const k = `${source}|${target}`;
    if (seen.has(k)) return;
    seen.add(k);
    out.push({ source, target });
  };

  for (const e of edges) {
    const se = isEntityId(e.source);
    const te = isEntityId(e.target);
    if (!se && !te) {
      add(e.source, e.target);
    } else if (!se && te) {
      if (!entitySources.has(e.target)) entitySources.set(e.target, new Set());
      entitySources.get(e.target)!.add(e.source);
    } else if (se && !te) {
      if (!entityTargets.has(e.source)) entityTargets.set(e.source, new Set());
      entityTargets.get(e.source)!.add(e.target);
    }
  }

  for (const [entity, sources] of entitySources) {
    const targets = entityTargets.get(entity);
    if (!targets) continue;
    for (const s of sources) for (const t of targets) add(s, t);
  }
  return out;
}

export function splitFqdn(fullName: string): { catalog: string; schema: string } {
  const parts = fullName.split(".");
  if (parts.length === 3) return { catalog: parts[0], schema: parts[1] };
  return { catalog: "", schema: "" };
}
