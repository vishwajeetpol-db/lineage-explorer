import { memo, useCallback, useMemo, useState } from "react";
import { motion } from "framer-motion";
import { GitBranch, Database, Eye, Layers, Loader2, RefreshCw, Search, ChevronRight, BarChart3, PieChart, FolderOpen, HardDrive, Zap } from "lucide-react";
import { useLineageStore } from "../store/lineageStore";
import { api } from "../api/client";
import type { TableSearchItem } from "../api/client";

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

const typeBgColors: Record<string, string> = {
  MANAGED: "bg-blue-500/10 text-blue-400",
  TABLE: "bg-blue-500/10 text-blue-400",
  EXTERNAL: "bg-emerald-500/10 text-emerald-400",
  VIEW: "bg-emerald-500/10 text-emerald-400",
  MATERIALIZED_VIEW: "bg-amber-500/10 text-amber-400",
  STREAMING_TABLE: "bg-rose-500/10 text-rose-400",
  VOLUME: "bg-violet-500/10 text-violet-400",
  PATH: "bg-orange-500/10 text-orange-400",
};

// Chart colors for catalogs
const CATALOG_COLORS = [
  "bg-indigo-500", "bg-emerald-500", "bg-amber-500", "bg-rose-500",
  "bg-cyan-500", "bg-purple-500", "bg-orange-500", "bg-teal-500",
];

const TYPE_CHART_COLORS: Record<string, { fill: string; stroke: string }> = {
  TABLE: { fill: "#6366f1", stroke: "#818cf8" },
  MANAGED: { fill: "#6366f1", stroke: "#818cf8" },
  EXTERNAL: { fill: "#10b981", stroke: "#34d399" },
  VIEW: { fill: "#14b8a6", stroke: "#2dd4bf" },
  MATERIALIZED_VIEW: { fill: "#f59e0b", stroke: "#fbbf24" },
  STREAMING_TABLE: { fill: "#f43f5e", stroke: "#fb7185" },
  VOLUME: { fill: "#8b5cf6", stroke: "#a78bfa" },
  PATH: { fill: "#f97316", stroke: "#fb923c" },
};

interface Props {
  onSelectTable: (fqdn: string) => void;
}

function DonutChart({ data }: { data: { label: string; value: number; color: string }[] }) {
  const total = data.reduce((s, d) => s + d.value, 0);
  if (total === 0) return null;
  const radius = 40;
  const circumference = 2 * Math.PI * radius;
  let offset = 0;

  return (
    <svg viewBox="0 0 100 100" className="w-32 h-32">
      {data.map((d, i) => {
        const pct = d.value / total;
        const dashLength = pct * circumference;
        const dashOffset = -offset * circumference;
        offset += pct;
        return (
          <circle
            key={i}
            cx="50" cy="50" r={radius}
            fill="none"
            stroke={d.color}
            strokeWidth="12"
            strokeDasharray={`${dashLength} ${circumference - dashLength}`}
            strokeDashoffset={dashOffset}
            strokeLinecap="round"
            className="transition-all duration-700"
          />
        );
      })}
      <text x="50" y="46" textAnchor="middle" className="fill-slate-200 text-[14px] font-semibold">{total}</text>
      <text x="50" y="58" textAnchor="middle" className="fill-slate-500 text-[7px]">tables</text>
    </svg>
  );
}

function LandingPage({ onSelectTable }: Props) {
  const { allTables, allTablesLoading } = useLineageStore();
  const [filter, setFilter] = useState("");
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());

  // Compute stats
  const stats = useMemo(() => {
    const catalogs = new Set<string>();
    const schemas = new Set<string>();
    const types: Record<string, number> = {};
    const catalogCounts: Record<string, number> = {};

    for (const t of allTables) {
      catalogs.add(t.catalog);
      schemas.add(`${t.catalog}.${t.schema}`);
      types[t.table_type] = (types[t.table_type] || 0) + 1;
      catalogCounts[t.catalog] = (catalogCounts[t.catalog] || 0) + 1;
    }

    return {
      totalTables: allTables.length,
      catalogCount: catalogs.size,
      schemaCount: schemas.size,
      types,
      catalogCounts,
    };
  }, [allTables]);

  // Group tables by catalog.schema
  const grouped = useMemo(() => {
    const q = filter.toLowerCase();
    const filtered = q
      ? allTables.filter((t) => t.fqdn.toLowerCase().includes(q) || t.name.toLowerCase().includes(q))
      : allTables;

    const groups: Record<string, TableSearchItem[]> = {};
    for (const t of filtered) {
      const key = `${t.catalog}.${t.schema}`;
      if (!groups[key]) groups[key] = [];
      groups[key].push(t);
    }
    // Sort groups by key, tables within group by name
    const sorted = Object.entries(groups).sort(([a], [b]) => a.localeCompare(b));
    for (const [, tables] of sorted) {
      tables.sort((a, b) => a.name.localeCompare(b.name));
    }
    return sorted;
  }, [allTables, filter]);

  const toggleSchema = useCallback((key: string) => {
    setExpandedSchemas((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);

  const handleSelect = useCallback(
    (fqdn: string) => {
      onSelectTable(fqdn);
    },
    [onSelectTable]
  );

  // Donut chart data
  const donutData = useMemo(() => {
    return Object.entries(stats.types).map(([type, count]) => ({
      label: type === "MATERIALIZED_VIEW" ? "Mat. View" : type,
      value: count,
      color: TYPE_CHART_COLORS[type]?.fill || "#6366f1",
    }));
  }, [stats.types]);

  // Bar chart: top catalogs
  const barData = useMemo(() => {
    return Object.entries(stats.catalogCounts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 6);
  }, [stats.catalogCounts]);

  const maxBarValue = barData.length > 0 ? barData[0][1] : 1;

  // Loading state
  if (allTablesLoading) {
    return (
      <div className="h-screen w-screen flex flex-col items-center justify-center bg-surface">
        <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_center,rgba(99,102,241,0.06)_0%,transparent_70%)]" />
        <div className="relative z-10 flex flex-col items-center gap-4">
          <div className="w-14 h-14 rounded-2xl bg-gradient-to-br from-accent to-purple-500 flex items-center justify-center shadow-[0_0_40px_rgba(99,102,241,0.25)]">
            <GitBranch size={26} className="text-white" />
          </div>
          <Loader2 size={24} className="text-accent animate-spin" />
          <p className="text-sm text-slate-500">Loading tables...</p>
        </div>
      </div>
    );
  }

  // Error / empty state
  if (allTables.length === 0) {
    return (
      <div className="h-screen w-screen flex flex-col items-center justify-center bg-surface">
        <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_center,rgba(99,102,241,0.06)_0%,transparent_70%)]" />
        <div className="relative z-10 flex flex-col items-center gap-4">
          <div className="w-14 h-14 rounded-2xl bg-gradient-to-br from-accent to-purple-500 flex items-center justify-center shadow-[0_0_40px_rgba(99,102,241,0.25)]">
            <GitBranch size={26} className="text-white" />
          </div>
          <p className="text-sm text-slate-500">Unable to load table index</p>
          <button
            onClick={() => {
              useLineageStore.getState().setAllTablesLoading(true);
              api.getTables()
                .then((r) => useLineageStore.getState().setAllTables(r.tables))
                .catch(() => useLineageStore.getState().setAllTablesLoading(false));
            }}
            className="flex items-center gap-2 px-4 py-2 rounded-lg bg-accent/10 hover:bg-accent/20 border border-accent/20 text-accent-light text-[12px] font-medium transition-all duration-200"
          >
            <RefreshCw size={13} />
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="h-screen w-screen flex flex-col bg-surface overflow-hidden">
      <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_top,rgba(99,102,241,0.04)_0%,transparent_50%)]" />

      {/* Header */}
      <motion.div
        initial={{ opacity: 0, y: -10 }}
        animate={{ opacity: 1, y: 0 }}
        className="relative z-10 flex items-center gap-4 px-8 py-5 border-b border-white/[0.06]"
      >
        <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-accent to-purple-500 flex items-center justify-center shadow-[0_0_30px_rgba(99,102,241,0.2)]">
          <GitBranch size={20} className="text-white" />
        </div>
        <div>
          <h1 className="text-lg font-semibold text-white tracking-tight">Lineage Explorer</h1>
          <p className="text-[11px] text-slate-500">Click any table to explore its data lineage</p>
        </div>

        {/* Search in header */}
        <div className="ml-auto flex items-center gap-3 w-80">
          <div className="relative flex-1">
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
      </motion.div>

      <div className="relative z-10 flex-1 flex overflow-hidden">
        {/* Left: Stats + Charts */}
        <motion.div
          initial={{ opacity: 0, x: -20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: 0.1 }}
          className="w-80 flex-shrink-0 border-r border-white/[0.06] overflow-y-auto scrollbar-thin p-5 space-y-5"
        >
          {/* Stat cards */}
          <div className="grid grid-cols-2 gap-3">
            {[
              { label: "Tables", value: stats.totalTables, icon: Database, color: "text-indigo-400" },
              { label: "Catalogs", value: stats.catalogCount, icon: FolderOpen, color: "text-emerald-400" },
              { label: "Schemas", value: stats.schemaCount, icon: Layers, color: "text-amber-400" },
              { label: "Types", value: Object.keys(stats.types).length, icon: PieChart, color: "text-rose-400" },
            ].map((s) => (
              <div key={s.label} className="bg-surface-50/60 border border-white/[0.04] rounded-xl p-3">
                <div className="flex items-center gap-2 mb-1.5">
                  <s.icon size={13} className={s.color} />
                  <span className="text-[10px] text-slate-500 uppercase tracking-wider">{s.label}</span>
                </div>
                <div className="text-xl font-semibold text-slate-100">{s.value.toLocaleString()}</div>
              </div>
            ))}
          </div>

          {/* Type distribution donut */}
          <div className="bg-surface-50/60 border border-white/[0.04] rounded-xl p-4">
            <div className="flex items-center gap-2 mb-3">
              <PieChart size={13} className="text-slate-400" />
              <span className="text-[11px] text-slate-400 font-medium">Table Types</span>
            </div>
            <div className="flex items-center justify-center">
              <DonutChart data={donutData} />
            </div>
            <div className="mt-3 space-y-1.5">
              {donutData.map((d) => (
                <div key={d.label} className="flex items-center gap-2 text-[11px]">
                  <div className="w-2.5 h-2.5 rounded-sm" style={{ backgroundColor: d.color }} />
                  <span className="text-slate-400 flex-1">{d.label}</span>
                  <span className="text-slate-300 font-mono">{d.value}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Catalog bar chart */}
          <div className="bg-surface-50/60 border border-white/[0.04] rounded-xl p-4">
            <div className="flex items-center gap-2 mb-3">
              <BarChart3 size={13} className="text-slate-400" />
              <span className="text-[11px] text-slate-400 font-medium">Tables per Catalog</span>
            </div>
            <div className="space-y-2.5">
              {barData.map(([catalog, count], i) => (
                <div key={catalog}>
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-[11px] text-slate-400 font-mono truncate max-w-[180px]">{catalog}</span>
                    <span className="text-[11px] text-slate-300 font-mono">{count}</span>
                  </div>
                  <div className="h-2 bg-white/[0.04] rounded-full overflow-hidden">
                    <motion.div
                      initial={{ width: 0 }}
                      animate={{ width: `${(count / maxBarValue) * 100}%` }}
                      transition={{ duration: 0.6, delay: i * 0.1 }}
                      className={`h-full rounded-full ${CATALOG_COLORS[i % CATALOG_COLORS.length]}`}
                    />
                  </div>
                </div>
              ))}
            </div>
          </div>
        </motion.div>

        {/* Right: Table listing */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.2 }}
          className="flex-1 overflow-y-auto scrollbar-thin"
        >
          {grouped.length === 0 ? (
            <div className="flex items-center justify-center h-full">
              <p className="text-sm text-slate-500">No tables matching &ldquo;{filter}&rdquo;</p>
            </div>
          ) : (
            <div className="p-4 space-y-1">
              {grouped.map(([schemaKey, tables]) => {
                const isExpanded = expandedSchemas.has(schemaKey) || filter.length > 0;
                return (
                  <div key={schemaKey} className="border border-white/[0.04] rounded-xl overflow-hidden bg-surface-50/40">
                    {/* Schema header */}
                    <button
                      onClick={() => toggleSchema(schemaKey)}
                      className="w-full flex items-center gap-3 px-4 py-3 hover:bg-white/[0.02] transition-colors"
                    >
                      <ChevronRight
                        size={14}
                        className={`text-slate-500 transition-transform duration-200 ${isExpanded ? "rotate-90" : ""}`}
                      />
                      <FolderOpen size={14} className="text-indigo-400" />
                      <span className="font-mono text-[13px] text-slate-300">{schemaKey}</span>
                      <span className="ml-auto text-[11px] text-slate-600 font-mono">{tables.length} tables</span>
                    </button>

                    {/* Table rows */}
                    {isExpanded && (
                      <div className="border-t border-white/[0.04]">
                        {tables.map((t) => {
                          const Icon = typeIcons[t.table_type] || Database;
                          const color = typeColors[t.table_type] || "text-blue-400";
                          const bgColor = typeBgColors[t.table_type] || "bg-blue-500/10 text-blue-400";
                          return (
                            <button
                              key={t.fqdn}
                              onClick={() => handleSelect(t.fqdn)}
                              className="w-full flex items-center gap-3 px-4 py-2.5 pl-11 hover:bg-accent/[0.06] transition-colors group border-b border-white/[0.02] last:border-b-0"
                            >
                              <Icon size={13} className={color} />
                              <span className="font-mono text-[12px] text-slate-300 group-hover:text-accent-light transition-colors truncate">
                                {t.fqdn}
                              </span>
                              <span className={`ml-auto text-[9px] font-medium tracking-wider uppercase px-1.5 py-0.5 rounded ${bgColor}`}>
                                {t.table_type === "MATERIALIZED_VIEW" ? "MV" : t.table_type}
                              </span>
                              <ChevronRight size={12} className="text-slate-600 group-hover:text-accent-light transition-colors" />
                            </button>
                          );
                        })}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </motion.div>
      </div>
    </div>
  );
}

export default memo(LandingPage);
