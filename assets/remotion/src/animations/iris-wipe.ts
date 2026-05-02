import { clamp, EASE_INOUT_CUBIC } from "./internal";

export interface IrisWipeParams {
  frame: number;
  start: number;
  duration?: number;
  direction?: "expand" | "contract";
}

export function irisWipe({
  frame,
  start,
  duration = 30,
  direction = "expand",
}: IrisWipeParams): string {
  const range: readonly [number, number] =
    direction === "expand" ? [0, 120] : [120, 0];
  const r = clamp(frame, [start, start + duration], range, EASE_INOUT_CUBIC);
  return `circle(${r}% at 50% 50%)`;
}
