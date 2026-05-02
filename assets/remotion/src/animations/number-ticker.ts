import { clamp, EASE_OUT_CUBIC } from "./internal";

export interface NumberTickerParams {
  frame: number;
  start: number;
  from: number;
  to: number;
  duration?: number;
  format?: (value: number) => string;
}

export function numberTicker({
  frame,
  start,
  from,
  to,
  duration = 45,
  format,
}: NumberTickerParams): string {
  const v = clamp(frame, [start, start + duration], [from, to], EASE_OUT_CUBIC);
  return format ? format(v) : Math.round(v).toLocaleString("en-US");
}
