import { memo, useMemo, useState } from "react";
import { motion } from "framer-motion";
import { Search, Database, Eye, Layers, ChevronRight, Zap, FolderOpen, HardDrive } from "lucide-react";
import { useLineageStore } from "../../store/lineageStore";
import Breadcrumb from "./Breadcrumb";
import PageShell from "./PageShell";

const typeIcons: Record<string, typeof Database> = {
  MANAGED: Database,
  TABLE: Database,
  EXTERNAL: Database,
  VIEW: Eye,
  MATERIALIZED_VIEW: Layers,
  STREAMING_TABLE: Zap,
  VOLUME: FolderOpen,
  PATH: HardDrive,
};

const typeColors: Record<string, string> = {
  MANAGED: "text-blue-400",
  TABLE: "text-blue-400",
  EXTERNAL: "text-emerald-400",
  VIEW: "text-emerald-400",
  MATERIALIZED_VIEW: "text-amber-400",
  STREAMING_TABLE: "text-rose-400",
  VOLUME: "text-violet-400",
  PATH: "text-orange-400",
};

const typeBadge: Record<string, string> = {
  MANAGED: "bg-blue-500/10 text-blue-400",
  TABLE: "bg-blue-500/10 text-blue-400",
  EXTERNAL: "bg-emerald-500/10 text-emerald-400",
  VIEW: "bg-emerald-500/10 text-emerald-400",
  MATERIALIZED_VIEW: "bg-amber-500/10 text-amber-400",
  STREAMING_TABLE: "bg-rose-500/10 text-rose-400",
  VOLUME: "bg-violet-500/10 text-violet-400",
  PATH: "bg-orange-500/10 text-orange-400",
};

interface Props {
  catalog: string;
  schema: string;
  onSelectTable: (fqdn: string) => void;
}

function TableListView({ catalog, schema, onSelectTable }: Props) {
  const allTables = useLineageStore((s) => s.allTables);
  const [filter, setFilter] = useState("");

  const tables = useMemo(
    () =>
      allTables
        .filter((t) => t.catalog === catalog && t.schema === schema)
        .sort((a, b) => a.name.localeCompare(b.name)),
    [allTables, catalog, schema]
  );

  const filtered = useMemo(() => {
    if (!filter.trim()) return tables;
    const q = filter.toLowerCase();
    return tables.filter((t) => t.name.toLowerCase().includes(q));
  }, [tables, filter]);

  return (
    <PageShell>
      <div className="flex items-center justify-between mb-6">
        <div className="space-y-2">
          <Breadcrumb catalog={catalog} schema={schema} />
          <h2 className="text-[20px] font-semibold text-white tracking-tight">
            {schema}
            <span className="text-slate-500 font-normal text-[14px] ml-2">
              ({tables.length} table{tables.length !== 1 && "s"})
            </span>
          </h2>
        </div>

        <div className="relative w-72">
          <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500" />
          <input
            type="text"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="Filter tables..."
            className="w-full pl-9 pr-3 py-2 bg-surface-50/80 border border-white/[0.06] rounded-lg text-[13px] text-slate-200 placeholder:text-slate-600 outline-none focus:border-accent/40 transition-colors font-mono"
          />
        </div>
      </div>

      {tables.length === 0 ? (
        <div className="flex items-center justify-center py-16">
          <p className="text-sm text-slate-500">No tables in this schema.</p>
        </div>
      ) : filtered.length === 0 ? (
        <div className="flex items-center justify-center py-16">
          <p className="text-sm text-slate-500">No tables matching &ldquo;{filter}&rdquo;</p>
        </div>
      ) : (
        <div className="border border-white/[0.06] rounded-xl overflow-hidden bg-surface-50/40">
          {filtered.map((t, i) => {
            const Icon = typeIcons[t.table_type] || Database;
            const color = typeColors[t.table_type] || "text-blue-400";
            const badge = typeBadge[t.table_type] || "bg-blue-500/10 text-blue-400";
            return (
              <motion.button
                key={t.fqdn}
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: Math.min(i * 0.01, 0.1) }}
                onClick={() => onSelectTable(t.fqdn)}
                className="group w-full flex items-center gap-3 px-5 py-3 hover:bg-accent/[0.06] transition-colors border-b border-white/[0.04] last:border-b-0 text-left"
              >
                <Icon size={14} className={color} />
                <span className="font-mono text-[13px] text-slate-200 group-hover:text-accent-light transition-colors flex-1 truncate">
                  {t.name}
                </span>
                <span className={`text-[9px] font-medium tracking-wider uppercase px-1.5 py-0.5 rounded ${badge}`}>
                  {t.table_type === "MATERIALIZED_VIEW" ? "MV" : t.table_type}
                </span>
                <ChevronRight size={12} className="text-slate-600 group-hover:text-accent-light transition-colors" />
              </motion.button>
            );
          })}
        </div>
      )}
    </PageShell>
  );
}

export default memo(TableListView);
