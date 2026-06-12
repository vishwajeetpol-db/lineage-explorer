import { memo, useEffect, useMemo, useState, type ReactNode } from "react";
import { motion } from "framer-motion";
import { X, Download, Loader2, ChevronUp, ChevronDown, Search, FileSpreadsheet } from "lucide-react";
import { useLineageStore } from "../../store/lineageStore";
import { api } from "../../api/client";
import { exportLineageToExcel } from "../../lib/exportLineage";
import { collapseToTableEdges, splitFqdn } from "../../lib/lineageRows";
import type { TableNode, EntityNode } from "../../api/client";

const STATUS_CHIP: Record<string, string> = {
  orphan: "bg-amber-500/15 text-amber-300 border-amber-500/25",
  root: "bg-sky-500/15 text-sky-300 border-sky-500/25",
  leaf: "bg-violet-500/15 text-violet-300 border-violet-500/25",
  connected: "bg-emerald-500/15 text-emerald-300 border-emerald-500/25",
};

const MAX_RENDER = 500; // keep the modal snappy; the download always has everything

interface Col {
  key: string;
  label: string;
  mono?: boolean;
  align?: "right";
  render?: (row: Record<string, unknown>) => ReactNode;
}

function DataTable({ columns, rows }: { columns: Col[]; rows: Record<string, unknown>[] }) {
  const [filter, setFilter] = useState("");
  const [sort, setSort] = useState<{ key: string; dir: 1 | -1 } | null>(null);

  const filtered = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter((r) => columns.some((c) => String(r[c.key] ?? "").toLowerCase().includes(q)));
  }, [rows, columns, filter]);

  const sorted = useMemo(() => {
    if (!sort) return filtered;
    const { key, dir } = sort;
    return [...filtered].sort((a, b) => {
      const av = a[key];
      const bv = b[key];
      if (typeof av === "number" && typeof bv === "number") return (av - bv) * dir;
      return String(av ?? "").localeCompare(String(bv ?? "")) * dir;
    });
  }, [filtered, sort]);

  const shown = sorted.slice(0, MAX_RENDER);

  const toggleSort = (key: string) =>
    setSort((s) => (s?.key === key ? { key, dir: s.dir === 1 ? -1 : 1 } : { key, dir: 1 }));

  return (
    <div className="flex flex-col h-full min-h-0">
      <div className="flex items-center justify-between gap-3 px-1 pb-3">
        <div className="relative w-64">
          <Search size={13} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-slate-500" />
          <input
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="Filter rows..."
            className="w-full pl-8 pr-2.5 py-1.5 bg-white/[0.03] border border-white/[0.06] rounded-lg text-[12px] text-slate-200 placeholder:text-slate-600 outline-none focus:border-accent/40 font-mono"
          />
        </div>
        <div className="text-[11px] text-slate-500 font-mono">
          {filtered.length.toLocaleString()} row{filtered.length !== 1 && "s"}
          {filtered.length > MAX_RENDER && ` · showing ${MAX_RENDER}`}
        </div>
      </div>

      <div className="flex-1 min-h-0 overflow-auto border border-white/[0.06] rounded-lg">
        <table className="w-full text-left border-collapse">
          <thead className="sticky top-0 z-10 bg-[#1A1A2A]">
            <tr>
              {columns.map((c) => (
                <th
                  key={c.key}
                  onClick={() => toggleSort(c.key)}
                  className={`px-3 py-2 text-[10px] font-semibold uppercase tracking-wider text-slate-400 cursor-pointer select-none border-b border-white/[0.08] hover:text-slate-200 ${c.align === "right" ? "text-right" : ""}`}
                >
                  <span className="inline-flex items-center gap-1">
                    {c.label}
                    {sort?.key === c.key && (sort.dir === 1 ? <ChevronUp size={11} /> : <ChevronDown size={11} />)}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {shown.map((r, i) => (
              <tr key={i} className="hover:bg-white/[0.03] border-b border-white/[0.04] last:border-b-0">
                {columns.map((c) => (
                  <td
                    key={c.key}
                    className={`px-3 py-1.5 text-[12px] text-slate-300 align-top ${c.mono ? "font-mono" : ""} ${c.align === "right" ? "text-right tabular-nums" : ""}`}
                  >
                    {c.render ? c.render(r) : String(r[c.key] ?? "")}
                  </td>
                ))}
              </tr>
            ))}
            {shown.length === 0 && (
              <tr>
                <td colSpan={columns.length} className="px-3 py-8 text-center text-[12px] text-slate-500">
                  No rows
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function LineagePreview() {
  const previewOpen = useLineageStore((s) => s.previewOpen);
  const setPreviewOpen = useLineageStore((s) => s.setPreviewOpen);
  const nodes = useLineageStore((s) => s.nodes);
  const edges = useLineageStore((s) => s.edges);
  const columnEdges = useLineageStore((s) => s.columnEdges);
  const scope = useLineageStore((s) => s.scope);
  const catalog = useLineageStore((s) => s.catalog);
  const schema = useLineageStore((s) => s.schema);

  const [tab, setTab] = useState("tables");
  const [downloading, setDownloading] = useState(false);
  const [note, setNote] = useState<string | null>(null);

  useEffect(() => {
    if (!previewOpen) return;
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && setPreviewOpen(false);
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [previewOpen, setPreviewOpen]);

  const tableNodes = useMemo(() => nodes.filter((n): n is TableNode => n.node_type === "table"), [nodes]);
  const entityNodes = useMemo(() => nodes.filter((n): n is EntityNode => n.node_type === "entity"), [nodes]);
  const tableEdges = useMemo(() => collapseToTableEdges(edges), [edges]);

  const tableRows = useMemo(
    () =>
      tableNodes.map((t) => {
        const { catalog: cat, schema: sch } = splitFqdn(t.full_name);
        return {
          full_name: t.full_name, name: t.name, catalog: cat, schema: sch,
          type: t.table_type, owner: t.owner ?? "",
          up: t.upstream_count, down: t.downstream_count,
          status: t.lineage_status, cols: t.columns?.length ?? 0,
        };
      }),
    [tableNodes]
  );
  const lineageRows = useMemo(() => tableEdges.map((e) => ({ source: e.source, target: e.target })), [tableEdges]);
  const pipelineRows = useMemo(
    () =>
      entityNodes.map((en) => ({
        type: en.entity_type, name: en.display_name ?? "", id: en.entity_id,
        last_run: en.last_run ?? "", cost: en.cost_usd ?? null,
      })),
    [entityNodes]
  );
  const columnRows = useMemo(
    () =>
      columnEdges.map((c) => ({
        source_table: c.source_table, source_column: c.source_column,
        target_table: c.target_table, target_column: c.target_column,
      })),
    [columnEdges]
  );

  if (!previewOpen) return null;

  const scopeLabel = scope === "catalog" ? catalog : scope === "schema" ? `${catalog}.${schema}` : `${catalog}.${schema}`;

  const tabs = [
    { id: "tables", label: "Tables", count: tableRows.length },
    { id: "lineage", label: "Lineage", count: lineageRows.length },
    ...(pipelineRows.length ? [{ id: "pipelines", label: "Pipelines", count: pipelineRows.length }] : []),
    ...(columnRows.length ? [{ id: "columns", label: "Columns", count: columnRows.length }] : []),
  ];

  async function handleDownload() {
    setDownloading(true);
    setNote(null);
    const sch = scope === "catalog" ? undefined : schema;
    try {
      const res = await fetch(api.lineageExportUrl(catalog, sch));
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        throw new Error(text || `status ${res.status}`);
      }
      const blob = await res.blob();
      const cd = res.headers.get("Content-Disposition") || "";
      const m = /filename="?([^"]+)"?/.exec(cd);
      const fname = m ? m[1] : `lineage_${scopeLabel}.xlsx`;
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = fname;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    } catch (e) {
      // Fall back to building the workbook in-browser from the loaded data.
      console.warn("Server export failed, using client-side fallback:", e);
      setNote("Server export unavailable — generated the file locally instead.");
      try {
        exportLineageToExcel({ nodes, edges, columnEdges, scope, catalog, schema, focusTable: null });
      } catch (e2) {
        console.error("Client-side export also failed:", e2);
        setNote("Export failed — see console for details.");
      }
    } finally {
      setDownloading(false);
    }
  }

  return (
    <div className="fixed inset-0 z-[130] flex items-center justify-center p-6" onClick={() => setPreviewOpen(false)}>
      <div className="absolute inset-0 bg-black/65 backdrop-blur-sm" />
      <motion.div
        initial={{ opacity: 0, y: 10, scale: 0.99 }}
        animate={{ opacity: 1, y: 0, scale: 1 }}
        transition={{ duration: 0.18 }}
        onClick={(e) => e.stopPropagation()}
        className="relative w-full max-w-5xl h-[80vh] flex flex-col bg-[#14141F] border border-white/[0.08] rounded-2xl shadow-[0_24px_64px_rgba(0,0,0,0.6)] overflow-hidden"
      >
        {/* Header */}
        <div className="flex items-center gap-3 px-5 py-3.5 border-b border-white/[0.06]">
          <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-emerald-500/25 to-teal-500/25 flex items-center justify-center">
            <FileSpreadsheet size={17} className="text-emerald-400" />
          </div>
          <div className="flex-1 min-w-0">
            <div className="text-[14px] font-semibold text-slate-100">Export preview</div>
            <div className="text-[11px] text-slate-500 font-mono truncate">
              {scope === "catalog" ? "Catalog" : "Schema"}: {scopeLabel}
            </div>
          </div>
          <button
            onClick={handleDownload}
            disabled={downloading}
            className="flex items-center gap-2 px-3.5 py-2 rounded-lg bg-emerald-500/15 hover:bg-emerald-500/25 border border-emerald-500/30 text-emerald-200 text-[12px] font-medium transition-all duration-200 disabled:opacity-60"
          >
            {downloading ? <Loader2 size={14} className="animate-spin" /> : <Download size={14} />}
            {downloading ? "Preparing..." : "Download Excel"}
          </button>
          <button
            onClick={() => setPreviewOpen(false)}
            className="flex items-center justify-center w-8 h-8 rounded-lg hover:bg-white/[0.06] text-slate-500 hover:text-slate-300 transition-colors"
            aria-label="Close"
          >
            <X size={16} />
          </button>
        </div>

        {note && (
          <div className="px-5 py-1.5 text-[11px] text-amber-300/90 bg-amber-500/5 border-b border-amber-500/10">{note}</div>
        )}

        {/* Tabs */}
        <div className="flex items-center gap-1 px-4 pt-3">
          {tabs.map((t) => (
            <button
              key={t.id}
              onClick={() => setTab(t.id)}
              className={`px-3 py-1.5 rounded-lg text-[11px] font-medium transition-colors ${
                tab === t.id ? "bg-accent/15 text-accent-light border border-accent/30" : "text-slate-500 hover:text-slate-300 border border-transparent"
              }`}
            >
              {t.label} <span className="text-slate-600">({t.count.toLocaleString()})</span>
            </button>
          ))}
        </div>

        {/* Body */}
        <div className="flex-1 min-h-0 p-4">
          {tab === "tables" && (
            <DataTable
              columns={[
                { key: "full_name", label: "Full name", mono: true },
                { key: "type", label: "Type" },
                { key: "owner", label: "Owner" },
                { key: "up", label: "Up", align: "right" },
                { key: "down", label: "Down", align: "right" },
                {
                  key: "status", label: "Status",
                  render: (r) => (
                    <span className={`text-[10px] font-medium px-1.5 py-0.5 rounded border ${STATUS_CHIP[String(r.status)] ?? "bg-white/5 text-slate-300 border-white/10"}`}>
                      {String(r.status)}
                    </span>
                  ),
                },
                { key: "cols", label: "Cols", align: "right" },
              ]}
              rows={tableRows}
            />
          )}
          {tab === "lineage" && (
            <DataTable
              columns={[
                { key: "source", label: "Source", mono: true },
                { key: "target", label: "Target", mono: true },
              ]}
              rows={lineageRows}
            />
          )}
          {tab === "pipelines" && (
            <DataTable
              columns={[
                { key: "type", label: "Type" },
                { key: "name", label: "Name" },
                { key: "id", label: "Entity ID", mono: true },
                { key: "last_run", label: "Last run" },
                {
                  key: "cost", label: "Cost (30d)", align: "right",
                  render: (r) => (r.cost == null ? "—" : `$${Number(r.cost).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`),
                },
              ]}
              rows={pipelineRows}
            />
          )}
          {tab === "columns" && (
            <DataTable
              columns={[
                { key: "source_table", label: "Source table", mono: true },
                { key: "source_column", label: "Source column" },
                { key: "target_table", label: "Target table", mono: true },
                { key: "target_column", label: "Target column" },
              ]}
              rows={columnRows}
            />
          )}
        </div>
      </motion.div>
    </div>
  );
}

export default memo(LineagePreview);
