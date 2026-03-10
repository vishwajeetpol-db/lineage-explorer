import { memo } from "react";
import { motion } from "framer-motion";

function Skeleton() {
  const skeletonNodes = Array.from({ length: 8 }, (_, i) => ({
    x: 60 + Math.floor(i / 3) * 260,
    y: 40 + (i % 3) * 90,
    w: 200 + Math.random() * 40,
  }));

  return (
    <div className="absolute inset-0 flex items-center justify-center">
      <svg width="900" height="350" className="opacity-40">
        {skeletonNodes.map((node, i) => (
          <motion.rect
            key={i}
            x={node.x}
            y={node.y}
            width={node.w}
            height={44}
            rx={12}
            className="shimmer"
            fill="#1E1E2E"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: i * 0.08 }}
          />
        ))}
        {[
          [0, 3], [1, 3], [1, 4], [2, 4], [3, 5], [4, 6], [5, 7], [6, 7],
        ].map(([from, to], i) => (
          <motion.line
            key={`e-${i}`}
            x1={skeletonNodes[from].x + skeletonNodes[from].w}
            y1={skeletonNodes[from].y + 22}
            x2={skeletonNodes[to].x}
            y2={skeletonNodes[to].y + 22}
            stroke="#1E1E2E"
            strokeWidth={2}
            initial={{ pathLength: 0 }}
            animate={{ pathLength: 1 }}
            transition={{ delay: 0.3 + i * 0.06, duration: 0.4 }}
          />
        ))}
      </svg>
      <div className="absolute text-center">
        <motion.div
          animate={{ opacity: [0.4, 1, 0.4] }}
          transition={{ duration: 2, repeat: Infinity }}
          className="text-[14px] text-slate-500 font-medium"
        >
          Loading lineage graph...
        </motion.div>
      </div>
    </div>
  );
}

export default memo(Skeleton);
