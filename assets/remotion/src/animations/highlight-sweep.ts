import { clamp, EASE_OUT_CUBIC } from "./internal";

export interface HighlightSweepParams {
  frame: number;
  start: number;
  duration?: number;
}

// Returns a width string (e.g. "47.3%") to drive a ::before background.
export function highlightSweep({
  frame,
  start,
  duration = 24,
}: HighlightSweepParams): string {
  return `${clamp(frame, [start, start + duration], [0, 100], EASE_OUT_CUBIC)}%`;
}
