import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Search, Clock, Database, Eye, Layers, Zap, FolderOpen, HardDrive, CornerDownLeft } from "lucide-react";
import { useLineageStore } from "../../store/lineageStore";
import { useRecents } from "../../hooks/useRecents";
import type { TableSearchItem } from "../../api/client";

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

const MAX_MATCHES = 12;

interface Props {
  onSelectTable: (fqdn: string) => void;
}

function highlight(text: string, query: string) {
  if (!query) return text;
  const idx = text.toLowerCase().indexOf(query.toLowerCase());
  if (idx < 0) return text;
  return (
    <>
      {text.slice(0, idx)}
      <span className="text-accent-light font-medium">{text.slice(idx, idx + query.length)}</span>
      {text.slice(idx + query.length)}
    </>
  );
}

function GlobalSearch({ onSelectTable }: Props) {
  const open = useLineageStore((s) => s.globalSearchOpen);
  const setOpen = useLineageStore((s) => s.setGlobalSearchOpen);
  const allTables = useLineageStore((s) => s.allTables);
  const { recents } = useRecents();

  const [query, setQuery] = useState("");
  const [activeIdx, setActiveIdx] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLDivElement>(null);

  // Cmd+K toggle from anywhere
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        setOpen(!open);
      }
      if (open && e.key === "Escape") {
        setOpen(false);
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [open, setOpen]);

  // Focus input on open, reset state on close
  useEffect(() => {
    if (open) {
      setTimeout(() => inputRef.current?.focus(), 50);
    } else {
      setQuery("");
      setActiveIdx(0);
    }
  }, [open]);

  // Build the active table index from recents
  const recentItems = useMemo(() => {
    const byFqdn = new Map(allTables.map((t) => [t.fqdn, t]));
    return recents
      .map((fqdn) => byFqdn.get(fqdn))
      .filter((t): t is TableSearchItem => Boolean(t))
      .slice(0, 5);
  }, [recents, allTables]);

  // Compute matches
  const matches = useMemo(() => {
    if (!query.trim()) return [] as TableSearchItem[];
    const q = query.toLowerCase();
    const scored: { item: TableSearchItem; score: number }[] = [];
    for (const t of allTables) {
      const name = t.name.toLowerCase();
      const fqdn = t.fqdn.toLowerCase();
      let score = -1;
      if (name === q) score = 1000;
      else if (name.startsWith(q)) score = 500 + (1 - name.length / 100);
      else if (name.includes(q)) score = 200 + (1 - name.length / 100);
      else if (fqdn.includes(q)) score = 100;
      if (score > 0) scored.push({ item: t, score });
    }
    scored.sort((a, b) => b.score - a.score);
    return scored.slice(0, MAX_MATCHES).map((s) => s.item);
  }, [allTables, query]);

  // Flat list for keyboard nav: recents first (when no query), then matches
  const flat = useMemo<TableSearchItem[]>(() => {
    if (query.trim()) return matches;
    return recentItems;
  }, [recentItems, matches, query]);

  // Keep activeIdx in range
  useEffect(() => {
    setActiveIdx(0);
  }, [query]);

  useEffect(() => {
    if (activeIdx >= flat.length) setActiveIdx(Math.max(0, flat.length - 1));
  }, [activeIdx, flat.length]);

  const handleSelect = useCallback(
    (fqdn: string) => {
      setOpen(false);
      onSelectTable(fqdn);
    },
    [setOpen, onSelectTable]
  );

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setActiveIdx((i) => Math.min(flat.length - 1, i + 1));
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        setActiveIdx((i) => Math.max(0, i - 1));
      } else if (e.key === "Enter") {
        e.preventDefault();
        const item = flat[activeIdx];
        if (item) handleSelect(item.fqdn);
      }
    },
    [flat, activeIdx, handleSelect]
  );

  // Scroll active item into view
  useEffect(() => {
    const el = listRef.current?.querySelector(`[data-idx="${activeIdx}"]`);
    if (el instanceof HTMLElement) el.scrollIntoView({ block: "nearest" });
  }, [activeIdx]);

  const totalMatchCount = useMemo(() => {
    if (!query.trim()) return 0;
    const q = query.toLowerCase();
    return allTables.filter((t) => t.name.toLowerCase().includes(q) || t.fqdn.toLowerCase().includes(q)).length;
  }, [allTables, query]);

  const renderRow = (t: TableSearchItem, idx: number, section: "recent" | "match") => {
    const Icon = typeIcons[t.table_type] || Database;
    const color = typeColors[t.table_type] || "text-blue-400";
    const isActive = idx === activeIdx;
    return (
      <button
        key={t.fqdn}
        data-idx={idx}
        onMouseEnter={() => setActiveIdx(idx)}
        onClick={() => handleSelect(t.fqdn)}
        className={`w-full flex items-center gap-3 px-4 py-2.5 text-left transition-colors ${
          isActive ? "bg-accent/15" : "hover:bg-white/[0.04]"
        }`}
      >
        <Icon size={13} className={color} />
        <div className="flex-1 min-w-0">
          <div className="font-mono text-[13px] text-slate-200 truncate">
            {highlight(t.fqdn, query)}
          </div>
        </div>
        {section === "recent" && <Clock size={11} className="text-slate-600" />}
        <span className="text-[9px] font-medium tracking-wider uppercase text-slate-500">
          {t.table_type === "MATERIALIZED_VIEW" ? "MV" : t.table_type}
        </span>
        {isActive && <CornerDownLeft size={11} className="text-accent-light" />}
      </button>
    );
  };

  return (
    <AnimatePresence>
      {open && (
        <>
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 bg-black/60 backdrop-blur-sm z-[9998]"
            onClick={() => setOpen(false)}
          />
          <motion.div
            initial={{ opacity: 0, scale: 0.97, y: -8 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.97, y: -8 }}
            transition={{ duration: 0.15 }}
            className="fixed top-[28%] left-1/2 -translate-x-1/2 z-[9999] w-[640px] max-w-[90vw]"
          >
            <div className="bg-surface-100/95 backdrop-blur-xl border border-white/[0.08] rounded-2xl overflow-hidden shadow-[0_20px_60px_rgba(0,0,0,0.5)]">
              {/* Input */}
              <div className="flex items-center gap-3 px-4 py-3 border-b border-white/[0.06]">
                <Search size={16} className="text-slate-500" />
                <input
                  ref={inputRef}
                  type="text"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  onKeyDown={handleKeyDown}
                  placeholder="Search tables across all catalogs..."
                  className="flex-1 bg-transparent text-[14px] text-slate-100 placeholder:text-slate-600 outline-none font-mono"
                />
                <kbd className="text-[10px] text-slate-600 bg-surface-200 px-1.5 py-0.5 rounded font-mono">ESC</kbd>
              </div>

              {/* Results */}
              <div ref={listRef} className="max-h-[400px] overflow-y-auto scrollbar-thin">
                {!query.trim() && recentItems.length > 0 && (
                  <>
                    <div className="px-4 pt-3 pb-1.5 text-[10px] uppercase tracking-wider text-slate-600 font-medium">
                      Recent
                    </div>
                    {recentItems.map((t, i) => renderRow(t, i, "recent"))}
                  </>
                )}

                {!query.trim() && recentItems.length === 0 && (
                  <div className="px-4 py-10 text-center text-[13px] text-slate-600">
                    Start typing to search across {allTables.length.toLocaleString()} tables.
                  </div>
                )}

                {query.trim() && matches.length === 0 && (
                  <div className="px-4 py-10 text-center text-[13px] text-slate-600">
                    No tables matching &ldquo;{query}&rdquo;
                  </div>
                )}

                {query.trim() && matches.length > 0 && (
                  <>
                    <div className="flex items-center justify-between px-4 pt-3 pb-1.5">
                      <span className="text-[10px] uppercase tracking-wider text-slate-600 font-medium">Matches</span>
                      {totalMatchCount > MAX_MATCHES && (
                        <span className="text-[10px] text-slate-600 font-mono">
                          showing {matches.length} of {totalMatchCount}
                        </span>
                      )}
                    </div>
                    {matches.map((t, i) => renderRow(t, i, "match"))}
                  </>
                )}
              </div>

              {/* Footer */}
              <div className="flex items-center gap-4 px-4 py-2 border-t border-white/[0.06] text-[10px] text-slate-600 font-mono">
                <span className="flex items-center gap-1.5">
                  <kbd className="bg-surface-200 px-1 py-0.5 rounded">↑↓</kbd> navigate
                </span>
                <span className="flex items-center gap-1.5">
                  <kbd className="bg-surface-200 px-1 py-0.5 rounded">↵</kbd> open lineage
                </span>
                <span className="flex items-center gap-1.5">
                  <kbd className="bg-surface-200 px-1 py-0.5 rounded">esc</kbd> close
                </span>
              </div>
            </div>
          </motion.div>
        </>
      )}
    </AnimatePresence>
  );
}

export default memo(GlobalSearch);
