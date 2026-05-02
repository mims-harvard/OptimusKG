import { clamp, EASE_OUT_EXPO } from "./internal";

export interface FadeRampParams {
  frame: number;
  start: number;
  duration?: number;
  from?: number;
  to?: number;
}

// Opacity ramp with a non-zero `from` so incoming text reads as
// "pre-dimmed" rather than absent.
export function fadeRamp({
  frame,
  start,
  duration = 12,
  from = 0.3,
  to = 1,
}: FadeRampParams): number {
  return clamp(frame, [start, start + duration], [from, to], EASE_OUT_EXPO);
}
