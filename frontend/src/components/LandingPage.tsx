import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Search, GitBranch, Database, Eye, Layers, Loader2, RefreshCw } from "lucide-react";
import { useLineageStore } from "../store/lineageStore";
import { api } from "../api/client";
import type { TableSearchItem } from "../api/client";

const typeIcons: Record<string, typeof Database> = {
  MANAGED: Database,
  TABLE: Database,
  EXTERNAL: Database,
  VIEW: Eye,
  MATERIALIZED_VIEW: Layers,
};

const typeColors: Record<string, string> = {
  MANAGED: "text-blue-400",
  TABLE: "text-blue-400",
  EXTERNAL: "text-emerald-400",
  VIEW: "text-emerald-400",
  MATERIALIZED_VIEW: "text-amber-400",
};

interface Props {
  onSelectTable: (fqdn: string) => void;
}

function LandingPage({ onSelectTable }: Props) {
  const { allTables, allTablesLoading } = useLineageStore();
  const [query, setQuery] = useState("");
  const [activeIndex, setActiveIndex] = useState(0);
  const [isFocused, setIsFocused] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLDivElement>(null);

  // Client-side fuzzy filter: match anywhere in name or fqdn
  const filtered = useMemo(() => {
    if (!query.trim()) return [];
    const q = query.toLowerCase();
    const results: (TableSearchItem & { matchScore: number })[] = [];
    for (const t of allTables) {
      const nameIdx = t.name.toLowerCase().indexOf(q);
      const fqdnIdx = t.fqdn.toLowerCase().indexOf(q);
      if (nameIdx >= 0 || fqdnIdx >= 0) {
        // Prefer name match, then start-of-name, then fqdn match
        const score = nameIdx === 0 ? 0 : nameIdx > 0 ? 1 : 2;
        results.push({ ...t, matchScore: score });
      }
    }
    results.sort((a, b) => a.matchScore - b.matchScore || a.name.localeCompare(b.name));
    return results.slice(0, 50); // cap at 50 results for performance
  }, [query, allTables]);

  const showDropdown = isFocused && query.trim().length > 0;

  // Reset active index when results change
  useEffect(() => {
    setActiveIndex(0);
  }, [filtered.length]);

  // Scroll active item into view
  useEffect(() => {
    if (!listRef.current) return;
    const activeEl = listRef.current.querySelector(`[data-index="${activeIndex}"]`);
    activeEl?.scrollIntoView({ block: "nearest" });
  }, [activeIndex]);

  const handleSelect = useCallback(
    (fqdn: string) => {
      setQuery("");
      onSelectTable(fqdn);
    },
    [onSelectTable]
  );

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (!showDropdown) return;
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setActiveIndex((i) => Math.min(i + 1, filtered.length - 1));
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        setActiveIndex((i) => Math.max(i - 1, 0));
      } else if (e.key === "Enter" && filtered[activeIndex]) {
        e.preventDefault();
        handleSelect(filtered[activeIndex].fqdn);
      } else if (e.key === "Escape") {
        inputRef.current?.blur();
      }
    },
    [showDropdown, filtered, activeIndex, handleSelect]
  );

  // Highlight matching text
  const highlightMatch = (text: string, q: string) => {
    if (!q) return text;
    const idx = text.toLowerCase().indexOf(q.toLowerCase());
    if (idx < 0) return text;
    return (
      <>
        {text.slice(0, idx)}
        <span className="text-accent-light">{text.slice(idx, idx + q.length)}</span>
        {text.slice(idx + q.length)}
      </>
    );
  };

  return (
    <div className="h-screen w-screen flex flex-col items-center justify-center bg-surface overflow-hidden relative">
      {/* Subtle radial gradient background */}
      <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_center,rgba(99,102,241,0.06)_0%,transparent_70%)]" />

      {/* Content */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5, ease: "easeOut" }}
        className="relative z-10 flex flex-col items-center w-full max-w-[640px] px-6"
      >
        {/* Logo */}
        <motion.div
          initial={{ scale: 0.8, opacity: 0 }}
          animate={{ scale: 1, opacity: 1 }}
          transition={{ duration: 0.4, delay: 0.1 }}
          className="mb-8"
        >
          <div className="w-14 h-14 rounded-2xl bg-gradient-to-br from-accent to-purple-500 flex items-center justify-center shadow-[0_0_40px_rgba(99,102,241,0.25)]">
            <GitBranch size={26} className="text-white" />
          </div>
        </motion.div>

        {/* Title */}
        <motion.h1
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.2 }}
          className="text-[28px] font-semibold text-white tracking-tight mb-2"
        >
          Lineage Explorer
        </motion.h1>
        <motion.p
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.3 }}
          className="text-[14px] text-slate-500 mb-10 tracking-wide"
        >
          Search for a table to explore its data lineage
        </motion.p>

        {/* Search box */}
        <motion.div
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.35 }}
          className="w-full relative"
        >
          <div
            className={`
              relative flex items-center gap-3 px-5 py-4
              bg-surface-50/80 backdrop-blur-md
              border rounded-2xl
              transition-all duration-300
              ${isFocused
                ? "border-accent/40 shadow-[0_0_30px_rgba(99,102,241,0.12)]"
                : "border-white/[0.06] hover:border-white/[0.12]"
              }
              ${showDropdown && filtered.length > 0 ? "rounded-b-none border-b-transparent" : ""}
            `}
          >
            {allTablesLoading ? (
              <Loader2 size={18} className="text-slate-500 animate-spin flex-shrink-0" />
            ) : (
              <Search size={18} className={`flex-shrink-0 transition-colors duration-200 ${isFocused ? "text-accent" : "text-slate-500"}`} />
            )}
            <input
              ref={inputRef}
              type="text"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onFocus={() => setIsFocused(true)}
              onBlur={() => setTimeout(() => setIsFocused(false), 200)}
              onKeyDown={handleKeyDown}
              placeholder={allTablesLoading ? "Loading tables..." : "Search tables by name..."}
              disabled={allTablesLoading}
              className="flex-1 bg-transparent text-[15px] text-slate-100 placeholder:text-slate-600 outline-none font-mono tracking-tight"
              autoFocus
            />
            {query && (
              <button
                onMouseDown={(e) => { e.preventDefault(); setQuery(""); inputRef.current?.focus(); }}
                className="text-slate-600 hover:text-slate-400 transition-colors"
              >
                <span className="text-[11px] font-mono bg-surface-200 px-2 py-0.5 rounded-md border border-white/[0.06]">ESC</span>
              </button>
            )}
          </div>

          {/* Dropdown results */}
          <AnimatePresence>
            {showDropdown && (
              <motion.div
                initial={{ opacity: 0, height: 0 }}
                animate={{ opacity: 1, height: "auto" }}
                exit={{ opacity: 0, height: 0 }}
                transition={{ duration: 0.15 }}
                className="absolute top-full left-0 right-0 z-50 overflow-hidden"
              >
                <div
                  ref={listRef}
                  className="
                    bg-surface-50/95 backdrop-blur-xl
                    border border-t-0 border-accent/40
                    rounded-b-2xl
                    shadow-[0_20px_60px_rgba(0,0,0,0.5)]
                    max-h-[360px] overflow-y-auto
                    scrollbar-thin
                  "
                >
                  {filtered.length === 0 ? (
                    <div className="px-5 py-8 text-center">
                      <div className="text-[13px] text-slate-500">No tables matching &ldquo;{query}&rdquo;</div>
                      <div className="text-[11px] text-slate-600 mt-1">Try a different search term</div>
                    </div>
                  ) : (
                    <>
                      <div className="px-5 py-2 text-[10px] text-slate-600 font-medium tracking-wider uppercase border-b border-white/[0.04]">
                        {filtered.length}{filtered.length === 50 ? "+" : ""} results
                      </div>
                      {filtered.map((item, idx) => {
                        const Icon = typeIcons[item.table_type] || Database;
                        const color = typeColors[item.table_type] || "text-blue-400";
                        const isActive = idx === activeIndex;
                        return (
                          <button
                            key={item.fqdn}
                            data-index={idx}
                            onMouseDown={(e) => { e.preventDefault(); handleSelect(item.fqdn); }}
                            onMouseEnter={() => setActiveIndex(idx)}
                            className={`
                              w-full flex items-start gap-3.5 px-5 py-3
                              transition-colors duration-100 text-left
                              border-b border-white/[0.02] last:border-b-0
                              ${isActive
                                ? "bg-accent/[0.08]"
                                : "hover:bg-white/[0.03]"
                              }
                            `}
                          >
                            <div className={`mt-0.5 p-1.5 rounded-lg ${isActive ? "bg-accent/15" : "bg-white/[0.04]"}`}>
                              <Icon size={14} className={color} />
                            </div>
                            <div className="flex-1 min-w-0">
                              <div className="font-mono text-[14px] text-slate-200 truncate leading-tight">
                                {highlightMatch(item.name, query)}
                              </div>
                              <div className="font-mono text-[11px] text-slate-500 truncate mt-0.5 leading-tight">
                                {highlightMatch(item.fqdn, query)}
                              </div>
                            </div>
                            <span className={`
                              text-[9px] font-medium tracking-wider uppercase mt-1 px-1.5 py-0.5 rounded
                              ${item.table_type === "VIEW" || item.table_type === "MATERIALIZED_VIEW"
                                ? "bg-emerald-500/10 text-emerald-500/70"
                                : "bg-blue-500/10 text-blue-500/70"
                              }
                            `}>
                              {item.table_type === "MATERIALIZED_VIEW" ? "MV" : item.table_type}
                            </span>
                          </button>
                        );
                      })}
                    </>
                  )}
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        </motion.div>

        {/* Hint text */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.5 }}
          className="mt-6 flex items-center gap-4 text-[11px] text-slate-600"
        >
          <span className="flex items-center gap-1.5">
            <kbd className="bg-surface-200 px-1.5 py-0.5 rounded text-[9px] font-mono border border-white/[0.06]">&uarr;</kbd>
            <kbd className="bg-surface-200 px-1.5 py-0.5 rounded text-[9px] font-mono border border-white/[0.06]">&darr;</kbd>
            navigate
          </span>
          <span className="flex items-center gap-1.5">
            <kbd className="bg-surface-200 px-1.5 py-0.5 rounded text-[9px] font-mono border border-white/[0.06]">Enter</kbd>
            select
          </span>
        </motion.div>

        {/* Stats / Error state */}
        {!allTablesLoading && allTables.length > 0 && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.6 }}
            className="mt-4 text-[10px] text-slate-700 font-mono"
          >
            {allTables.length.toLocaleString()} tables indexed across your catalogs
          </motion.div>
        )}
        {!allTablesLoading && allTables.length === 0 && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            className="mt-6 flex flex-col items-center gap-3"
          >
            <div className="text-[12px] text-slate-500">Unable to load table index</div>
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
          </motion.div>
        )}
      </motion.div>
    </div>
  );
}

export default memo(LandingPage);
