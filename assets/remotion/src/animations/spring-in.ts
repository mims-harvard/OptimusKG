import { spring } from "remotion";
import type { SpringConfig } from "./internal";

export interface SpringInParams {
  frame: number;
  start: number;
  fps: number;
  config?: Partial<SpringConfig>;
}

const DEFAULT_SPRING: SpringConfig = { damping: 14, stiffness: 90, mass: 1 };

export function springIn({
  frame,
  start,
  fps,
  config,
}: SpringInParams): number {
  return spring({
    fps,
    frame: frame - start,
    config: { ...DEFAULT_SPRING, ...config },
  });
}
