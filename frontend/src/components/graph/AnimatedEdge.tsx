import { memo } from "react";
import { getBezierPath, type EdgeProps } from "reactflow";

function AnimatedEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
}: EdgeProps<{ isHighlighted: boolean; isDimmed: boolean; isColumnEdge: boolean }>) {
  const [edgePath] = getBezierPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    curvature: 0.25,
  });

  const isHighlighted = data?.isHighlighted ?? false;
  const isDimmed = data?.isDimmed ?? false;
  const isColumnEdge = data?.isColumnEdge ?? false;

  if (isColumnEdge) {
    return (
      <g>
        {/* Glow layer */}
        <path
          d={edgePath}
          fill="none"
          stroke="rgba(167,139,250,0.2)"
          strokeWidth={8}
          style={{ filter: "blur(6px)" }}
        />
        {/* Main line */}
        <path
          id={id}
          d={edgePath}
          fill="none"
          stroke="#A78BFA"
          strokeWidth={2}
          className="column-edge"
        />
        {/* Traveling dot */}
        <circle r={3} fill="#A78BFA" filter="drop-shadow(0 0 4px rgba(167,139,250,0.9))">
          <animateMotion dur="1.2s" repeatCount="indefinite" path={edgePath} />
        </circle>
      </g>
    );
  }

  if (isDimmed) {
    return (
      <path
        id={id}
        d={edgePath}
        fill="none"
        stroke="#1E1E2E"
        strokeWidth={1}
        style={{ opacity: 0.3, transition: "all 0.4s ease" }}
      />
    );
  }

  if (isHighlighted) {
    return (
      <g>
        {/* Glow */}
        <path
          d={edgePath}
          fill="none"
          stroke="rgba(99,102,241,0.12)"
          strokeWidth={10}
          style={{ filter: "blur(6px)" }}
        />
        {/* Main */}
        <path
          id={id}
          d={edgePath}
          fill="none"
          stroke="#6366F1"
          strokeWidth={2}
          className="animated-edge"
          style={{ transition: "all 0.4s ease" }}
        />
        {/* Traveling dot */}
        <circle r={2.5} fill="#818CF8" filter="drop-shadow(0 0 3px rgba(99,102,241,0.8))">
          <animateMotion dur="2s" repeatCount="indefinite" path={edgePath} />
        </circle>
      </g>
    );
  }

  // Default: visible but understated edge
  return (
    <path
      id={id}
      d={edgePath}
      fill="none"
      stroke="rgba(100,116,139,0.28)"
      strokeWidth={1.5}
      strokeDasharray="none"
      style={{ transition: "all 0.4s ease" }}
    />
  );
}

export default memo(AnimatedEdge);
