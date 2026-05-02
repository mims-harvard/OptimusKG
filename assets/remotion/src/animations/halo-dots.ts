import { spring } from "remotion";
import type { SpringConfig } from "./internal";

export type SweepFrom = "left" | "right" | "top" | "bottom";

export interface HaloDotsParams {
  frame: number;
  start: number;
  count: number;
  radius: number;
  fps: number;
  sweepFrom?: SweepFrom;
}

export interface HaloDot {
  x: number;
  y: number;
  scale: number;
  opacity: number;
}

const SWEEP_ANGLE: Record<SweepFrom, number> = {
  right: 0,
  bottom: Math.PI / 2,
  left: Math.PI,
  top: -Math.PI / 2,
};

const HALO_SPRING: SpringConfig = { damping: 12, stiffness: 200, mass: 1 };
const HALO_MAX_DELAY_FRAMES = 18;

export function haloDots({
  frame,
  start,
  count,
  radius,
  fps,
  sweepFrom = "left",
}: HaloDotsParams): HaloDot[] {
  const sweep = SWEEP_ANGLE[sweepFrom];
  const out: HaloDot[] = new Array(count);
  for (let i = 0; i < count; i++) {
    const a = (i / count) * Math.PI * 2;
    // Shortest signed angular distance to the sweep origin, normalised to [0,1].
    const diff = Math.atan2(Math.sin(a - sweep), Math.cos(a - sweep));
    const delay = (Math.abs(diff) / Math.PI) * HALO_MAX_DELAY_FRAMES;
    const t = spring({
      fps,
      frame: frame - start - delay,
      config: HALO_SPRING,
    });
    out[i] = {
      x: Math.cos(a) * radius,
      y: Math.sin(a) * radius,
      scale: t,
      opacity: t,
    };
  }
  return out;
}
