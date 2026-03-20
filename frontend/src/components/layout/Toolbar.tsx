import { memo, useCallback, useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { GitBranch, Search, ChevronDown, Columns3, Zap, Info } from "lucide-react";
import { useLineageStore } from "../../store/lineageStore";
import { api, setLiveMode } from "../../api/client";

interface Props {
  onGenerate: () => void;
}

function Toolbar({ onGenerate }: Props) {
  const {
    catalog, schema, columnLineageEnabled, liveMode,
    catalogs, schemas, loading, cached, cachedAt,
    setCatalog, setSchema, setColumnLineageEnabled, setLiveMode: setStoreLiveMode,
    setCatalogs, setSchemas, setSearchOpen,
  } = useLineageStore();

  const nodes = useLineageStore((s) => s.nodes);
  const [toast, setToast] = useState<string | null>(null);

  const handleLiveModeToggle = useCallback(() => {
    const next = !liveMode;
    setStoreLiveMode(next);
    setLiveMode(next);
    if (next) {
      setToast("Live mode enabled — next refresh will query system tables directly. This may take a few seconds.");
    } else {
      setToast("Live mode disabled — data will be served from cache for faster loading.");
    }
  }, [liveMode, setStoreLiveMode]);

  // Auto-dismiss toast
  useEffect(() => {
    if (!toast) return;
    const t = setTimeout(() => setToast(null), 4000);
    return () => clearTimeout(t);
  }, [toast]);

  useEffect(() => {
    api.getCatalogs().then((r) => setCatalogs(r.catalogs)).catch(console.error);
  }, [setCatalogs]);

  useEffect(() => {
    if (catalog) {
      api.getSchemas(catalog).then((r) => setSchemas(r.schemas)).catch(console.error);
    }
  }, [catalog, setSchemas]);

  const handleGenerate = useCallback(() => {
    if (catalog && schema) onGenerate();
  }, [catalog, schema, onGenerate]);

  return (
    <>
    <motion.header
      initial={{ y: -10, opacity: 0 }}
      animate={{ y: 0, opacity: 1 }}
      transition={{ duration: 0.25 }}
      className="
        relative z-50 flex items-center gap-4 px-5 h-14
        bg-[#0D0D16]/90 backdrop-blur-xl
        border-b border-white/[0.04]
      "
    >
      {/* Logo */}
      <div className="flex items-center gap-2.5 mr-1">
        <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-accent to-purple-500 flex items-center justify-center shadow-[0_0_12px_rgba(99,102,241,0.3)]">
          <GitBranch size={16} className="text-white" />
        </div>
        <div>
          <div className="font-semibold text-[14px] text-white tracking-tight leading-none">
            Lineage Explorer
          </div>
          <div className="text-[9px] text-slate-600 tracking-wider uppercase mt-0.5">
            Unity Catalog
          </div>
        </div>
      </div>

      {/* Divider */}
      <div className="w-px h-8 bg-white/[0.06]" />

      {/* Catalog */}
      <SelectBox
        label="Catalog"
        value={catalog}
        options={catalogs}
        onChange={setCatalog}
        placeholder="Select catalog"
      />

      {/* Schema */}
      <SelectBox
        label="Schema"
        value={schema}
        options={schemas}
        onChange={setSchema}
        placeholder="Select schema"
        disabled={!catalog}
      />

      {/* Column Lineage */}
      <div className="flex items-center gap-2.5 px-3 py-1.5 rounded-lg bg-white/[0.02] border border-white/[0.04]">
        <Columns3 size={13} className="text-slate-500" />
        <span className="text-[11px] text-slate-500 font-medium">Columns</span>
        <button
          onClick={() => setColumnLineageEnabled(!columnLineageEnabled)}
          className={`
            relative w-8 h-[18px] rounded-full transition-all duration-300
            ${columnLineageEnabled
              ? "bg-gradient-to-r from-accent to-purple-500 shadow-[0_0_10px_rgba(99,102,241,0.3)]"
              : "bg-white/[0.06]"
            }
          `}
        >
          <motion.div
            animate={{ x: columnLineageEnabled ? 15 : 2 }}
            transition={{ type: "spring", stiffness: 500, damping: 30 }}
            className="absolute top-[2px] w-[14px] h-[14px] rounded-full bg-white shadow-sm"
          />
        </button>
      </div>

      {/* Live Query */}
      <div className="flex items-center gap-2.5 px-3 py-1.5 rounded-lg bg-white/[0.02] border border-white/[0.04]">
        <Zap size={13} className={liveMode ? "text-amber-400" : "text-slate-500"} />
        <span className="text-[11px] text-slate-500 font-medium">Live</span>
        <button
          onClick={handleLiveModeToggle}
          className={`
            relative w-8 h-[18px] rounded-full transition-all duration-300
            ${liveMode
              ? "bg-gradient-to-r from-amber-500 to-orange-500 shadow-[0_0_10px_rgba(245,158,11,0.3)]"
              : "bg-white/[0.06]"
            }
          `}
        >
          <motion.div
            animate={{ x: liveMode ? 15 : 2 }}
            transition={{ type: "spring", stiffness: 500, damping: 30 }}
            className="absolute top-[2px] w-[14px] h-[14px] rounded-full bg-white shadow-sm"
          />
        </button>
      </div>

      {/* Generate */}
      <button
        onClick={handleGenerate}
        disabled={!catalog || !schema || loading}
        className={`
          relative px-5 py-2 rounded-xl text-[13px] font-semibold transition-all duration-300
          ${catalog && schema && !loading
            ? "bg-gradient-to-r from-accent to-purple-500 text-white shadow-[0_0_20px_rgba(99,102,241,0.2)] hover:shadow-[0_0_30px_rgba(99,102,241,0.35)] active:scale-[0.97]"
            : "bg-white/[0.04] text-slate-600 cursor-not-allowed"
          }
        `}
      >
        {loading ? (
          <motion.span
            animate={{ opacity: [1, 0.4, 1] }}
            transition={{ duration: 1.2, repeat: Infinity }}
          >
            Loading...
          </motion.span>
        ) : (
          "Generate Lineage"
        )}
      </button>

      {/* Spacer */}
      <div className="flex-1" />

      {/* Search */}
      <button
        onClick={() => setSearchOpen(true)}
        className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-white/[0.03] hover:bg-white/[0.06] border border-white/[0.04] hover:border-white/[0.08] transition-all duration-200"
      >
        <Search size={13} className="text-slate-500" />
        <span className="text-[11px] text-slate-600 font-medium">Search</span>
        <div className="flex gap-0.5 ml-2">
          <kbd className="text-[9px] text-slate-600 bg-white/[0.04] px-1.5 py-0.5 rounded font-mono border border-white/[0.06]">
            Cmd
          </kbd>
          <kbd className="text-[9px] text-slate-600 bg-white/[0.04] px-1.5 py-0.5 rounded font-mono border border-white/[0.06]">
            K
          </kbd>
        </div>
      </button>
    </motion.header>
    {/* Cache status banner */}
    {nodes.length > 0 && (
      <div className={`
        flex items-center justify-center gap-2 px-4 py-1 text-[10px] font-medium tracking-wide
        ${liveMode
          ? "bg-amber-500/10 text-amber-400 border-b border-amber-500/20"
          : "bg-white/[0.02] text-slate-600 border-b border-white/[0.04]"
        }
      `}>
        {liveMode ? (
          <>
            <span className="relative flex h-1.5 w-1.5">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-amber-400 opacity-75" />
              <span className="relative inline-flex rounded-full h-1.5 w-1.5 bg-amber-400" />
            </span>
            LIVE MODE — Fetching fresh data from Unity Catalog system tables (may take a few seconds)
          </>
        ) : cached ? (
          <>
            <Info size={10} className="text-slate-600 flex-shrink-0" />
            Showing cached data for instant loading{cachedAt ? ` · Refreshed ${formatTimeAgo(cachedAt)}` : ""}
            {" · "}
            <button onClick={handleLiveModeToggle} className="underline underline-offset-2 hover:text-slate-400 transition-colors">
              Enable live mode for latest data
            </button>
          </>
        ) : (
          <>Data loaded from system tables</>
        )}
      </div>
    )}

    {/* Toast notification for live mode toggle */}
    <AnimatePresence>
      {toast && (
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -20 }}
          transition={{ duration: 0.3 }}
          className="fixed top-20 left-1/2 -translate-x-1/2 z-[100] flex items-center gap-2.5 px-4 py-2.5 rounded-xl bg-[#1A1A2E]/95 backdrop-blur-md border border-white/[0.08] shadow-[0_8px_32px_rgba(0,0,0,0.5)]"
        >
          <Zap size={13} className={liveMode ? "text-amber-400" : "text-slate-500"} />
          <span className="text-[12px] text-slate-300 font-medium max-w-[360px]">{toast}</span>
          <button
            onClick={() => setToast(null)}
            className="text-slate-600 hover:text-slate-400 text-[14px] ml-1 transition-colors"
          >
            &times;
          </button>
        </motion.div>
      )}
    </AnimatePresence>
    </>
  );
}

function formatTimeAgo(isoDate: string): string {
  const diff = Date.now() - new Date(isoDate).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ${mins % 60}m ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

function SelectBox({
  label,
  value,
  options,
  onChange,
  placeholder,
  disabled = false,
}: {
  label: string;
  value: string;
  options: string[];
  onChange: (v: string) => void;
  placeholder: string;
  disabled?: boolean;
}) {
  return (
    <div className="relative">
      <label className="absolute -top-1 left-3 text-[8px] text-slate-600 uppercase tracking-[0.1em] font-semibold bg-[#0D0D16] px-1 z-10">
        {label}
      </label>
      <div className="relative">
        <select
          value={value}
          onChange={(e) => onChange(e.target.value)}
          disabled={disabled}
          className={`
            appearance-none bg-white/[0.02] border border-white/[0.06]
            rounded-lg px-3.5 py-2 pr-8 text-[12px] font-mono
            min-w-[190px] outline-none
            transition-all duration-200
            ${disabled ? "opacity-30 cursor-not-allowed" : "hover:border-white/[0.12] focus:border-accent/40 focus:shadow-[0_0_12px_rgba(99,102,241,0.1)] cursor-pointer"}
            ${value ? "text-slate-100" : "text-slate-600"}
          `}
        >
          <option value="" disabled>{placeholder}</option>
          {options.map((opt) => (
            <option key={opt} value={opt} className="bg-[#14141F] text-slate-200">
              {opt}
            </option>
          ))}
        </select>
        <ChevronDown size={12} className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-600 pointer-events-none" />
      </div>
    </div>
  );
}

export default memo(Toolbar);
