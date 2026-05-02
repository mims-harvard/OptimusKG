import { clamp } from "./internal";

export interface LineDrawParams {
  frame: number;
  start: number;
  length: number;
  duration?: number;
}

export interface LineDrawStyle {
  strokeDasharray: number;
  strokeDashoffset: number;
}

export function lineDraw({
  frame,
  start,
  length,
  duration = 75,
}: LineDrawParams): LineDrawStyle {
  return {
    strokeDasharray: length,
    strokeDashoffset: clamp(frame, [start, start + duration], [length, 0]),
  };
}
