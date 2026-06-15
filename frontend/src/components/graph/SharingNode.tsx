import { memo } from "react";
import { Handle, Position, type NodeProps } from "reactflow";
import { motion } from "framer-motion";
import { Share2, Users, Building2 } from "lucide-react";

// Synthetic node injected by the Delta Sharing overlay. Not a lineage object —
// it represents the share boundary: a Share, a Recipient (outbound), or a
// Provider (inbound). Styled distinctly (teal, dashed) so it never reads as a
// real transform-lineage node.
export type SharingNodeData = {
  node_type: "sharing";
  sharingKind: "share" | "recipient" | "provider";
  label: string;
  sub?: string | null;
  isRevealed?: boolean;
  isDimmed?: boolean;
};

const kindConfig = {
  share: { icon: Share2, tag: "SHARE", color: "text-teal-300", border: "border-teal-400/40", bg: "from-teal-500/[0.10] to-teal-600/[0.04]", dot: "bg-teal-400" },
  recipient: { icon: Users, tag: "RECIPIENT", color: "text-sky-300", border: "border-sky-400/40", bg: "from-sky-500/[0.10] to-sky-600/[0.04]", dot: "bg-sky-400" },
  provider: { icon: Building2, tag: "PROVIDER", color: "text-violet-300", border: "border-violet-400/40", bg: "from-violet-500/[0.10] to-violet-600/[0.04]", dot: "bg-violet-400" },
} as const;

function SharingNodeComponent({ data }: NodeProps<SharingNodeData>) {
  const isRevealed = data.isRevealed ?? true;
  const isDimmed = data.isDimmed ?? false;
  const cfg = kindConfig[data.sharingKind] ?? kindConfig.share;
  const Icon = cfg.icon;

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.9 }}
      animate={{ opacity: isRevealed ? (isDimmed ? 0.15 : 1) : 0, scale: isRevealed ? 1 : 0.9 }}
      transition={{ duration: 0.25, ease: "easeOut" }}
      className="relative"
    >
      <Handle type="target" position={Position.Left} className="!w-2 !h-2 !rounded-full !border-0 !bg-teal-600/70" />
      <Handle type="source" position={Position.Right} className="!w-2 !h-2 !rounded-full !border-0 !bg-teal-600/70" />

      <div
        className={`px-3.5 py-2 rounded-xl backdrop-blur-sm border border-dashed bg-gradient-to-r ${cfg.bg} ${cfg.border} shadow-[0_2px_12px_rgba(0,0,0,0.3)] ${isDimmed ? "pointer-events-none" : ""}`}
        style={{ minWidth: 150 }}
      >
        <div className="flex items-center gap-2.5">
          <div className={`w-1.5 h-1.5 rounded-full ${cfg.dot} shadow-[0_0_6px] flex-shrink-0`} />
          <Icon size={13} className={`${cfg.color} flex-shrink-0 opacity-80`} />
          <div className="min-w-0">
            <div className={`font-mono font-medium text-[11px] ${cfg.color} truncate max-w-[160px]`} title={data.label}>
              {data.label}
            </div>
            {data.sub && (
              <div className="text-[9px] text-slate-500 truncate max-w-[160px]" title={data.sub}>{data.sub}</div>
            )}
          </div>
          <span className={`text-[8px] font-semibold tracking-wider uppercase px-1.5 py-0.5 rounded ${cfg.color} bg-white/[0.04] flex-shrink-0`}>
            {cfg.tag}
          </span>
        </div>
      </div>
    </motion.div>
  );
}

export default memo(SharingNodeComponent);
