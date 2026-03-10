import { memo, useCallback, useEffect } from "react";
import { motion } from "framer-motion";
import { GitBranch, Search, ChevronDown, Columns3 } from "lucide-react";
import { useLineageStore } from "../../store/lineageStore";
import { api } from "../../api/client";

interface Props {
  onGenerate: () => void;
}

function Toolbar({ onGenerate }: Props) {
  const {
    catalog, schema, columnLineageEnabled,
    catalogs, schemas, loading,
    setCatalog, setSchema, setColumnLineageEnabled,
    setCatalogs, setSchemas, setSearchOpen,
  } = useLineageStore();

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
  );
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
