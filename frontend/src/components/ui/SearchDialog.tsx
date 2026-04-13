import { memo, useCallback, useEffect, useRef } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Search, X, Database, Eye, Layers } from "lucide-react";
import { useLineageStore } from "../../store/lineageStore";

const typeIcons: Record<string, typeof Database> = {
  MANAGED: Database,
  TABLE: Database,
  EXTERNAL: Database,
  VIEW: Eye,
  MATERIALIZED_VIEW: Layers,
};

interface Props {
  onSelectNode: (nodeId: string) => void;
}

function SearchDialog({ onSelectNode }: Props) {
  const { searchOpen, setSearchOpen, searchQuery, setSearchQuery, nodes } = useLineageStore();
  const inputRef = useRef<HTMLInputElement>(null);

  const tableNodes = nodes.filter((n) => n.node_type !== "entity");
  const filtered = searchQuery
    ? tableNodes.filter((n) => n.name.toLowerCase().includes(searchQuery.toLowerCase()))
    : tableNodes;

  useEffect(() => {
    if (searchOpen) {
      setTimeout(() => inputRef.current?.focus(), 100);
    } else {
      setSearchQuery("");
    }
  }, [searchOpen, setSearchQuery]);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        setSearchOpen(!searchOpen);
      }
      if (e.key === "Escape") {
        setSearchOpen(false);
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [searchOpen, setSearchOpen]);

  const handleSelect = useCallback(
    (nodeId: string) => {
      onSelectNode(nodeId);
      setSearchOpen(false);
    },
    [onSelectNode, setSearchOpen]
  );

  return (
    <AnimatePresence>
      {searchOpen && (
        <>
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 bg-black/50 backdrop-blur-sm z-[9998]"
            onClick={() => setSearchOpen(false)}
          />
          <motion.div
            initial={{ opacity: 0, scale: 0.96, y: -10 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.96, y: -10 }}
            transition={{ duration: 0.15 }}
            className="fixed top-[20%] left-1/2 -translate-x-1/2 z-[9999] w-[460px]"
          >
            <div className="glass-tooltip rounded-2xl overflow-hidden shadow-glow-lg">
              {/* Input */}
              <div className="flex items-center gap-3 px-4 py-3 border-b border-surface-300/50">
                <Search size={16} className="text-slate-500" />
                <input
                  ref={inputRef}
                  type="text"
                  placeholder="Search tables and views..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="flex-1 bg-transparent text-[14px] text-slate-200 placeholder:text-slate-600 outline-none font-mono"
                />
                <kbd className="text-[10px] text-slate-600 bg-surface-200 px-1.5 py-0.5 rounded font-mono">ESC</kbd>
              </div>

              {/* Results */}
              <div className="max-h-[300px] overflow-y-auto py-1">
                {filtered.length === 0 ? (
                  <div className="px-4 py-8 text-center text-[13px] text-slate-600">
                    No tables found
                  </div>
                ) : (
                  filtered.map((node) => {
                    const Icon = typeIcons[node.table_type] || Database;
                    return (
                      <button
                        key={node.id}
                        onClick={() => handleSelect(node.id)}
                        className="w-full flex items-center gap-3 px-4 py-2.5 hover:bg-surface-200/50 transition-colors text-left"
                      >
                        <Icon size={14} className="text-slate-500" />
                        <span className="font-mono text-[13px] text-slate-300 flex-1 truncate">
                          {node.name}
                        </span>
                        <span className="text-[10px] text-slate-600 font-mono">
                          {node.table_type}
                        </span>
                      </button>
                    );
                  })
                )}
              </div>
            </div>
          </motion.div>
        </>
      )}
    </AnimatePresence>
  );
}

export default memo(SearchDialog);
