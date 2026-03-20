import { memo, useState, useEffect } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { useLineageStore } from "../../store/lineageStore";

const liveStages = [
  "Connecting to warehouse...",
  "Querying Unity Catalog system tables — this may take a moment...",
  "Building dependency graph...",
  "Almost there...",
];

const cachedStages = [
  "Checking cache...",
  "Loading cached lineage data...",
  "Rendering graph...",
];

const liveDelays = [1500, 4000, 7000];
const cachedDelays = [800, 2000];

function Skeleton() {
  const liveMode = useLineageStore((s) => s.liveMode);
  const stages = liveMode ? liveStages : cachedStages;
  const stageDelays = liveMode ? liveDelays : cachedDelays;
  const [stage, setStage] = useState(0);

  useEffect(() => {
    const timers: ReturnType<typeof setTimeout>[] = [];
    stageDelays.forEach((delay, i) => {
      timers.push(setTimeout(() => setStage(i + 1), delay));
    });
    return () => timers.forEach(clearTimeout);
  }, []);

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
        <AnimatePresence mode="wait">
          <motion.div
            key={stage}
            initial={{ opacity: 0, y: 4 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -4 }}
            transition={{ duration: 0.3 }}
            className="text-[14px] text-slate-500 font-medium"
          >
            {stages[stage]}
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  );
}

export default memo(Skeleton);
