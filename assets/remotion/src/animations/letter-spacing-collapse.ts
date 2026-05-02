import { clamp, EASE_OUT_QUART } from "./internal";

export interface LetterSpacingCollapseParams {
  frame: number;
  start: number;
  duration?: number;
  rise?: string;
}

export interface LetterSpacingCollapseStyle {
  letterSpacing: string;
  opacity: number;
  transform: string;
}

export function letterSpacingCollapse({
  frame,
  start,
  duration = 18,
  rise = "1.5rem",
}: LetterSpacingCollapseParams): LetterSpacingCollapseStyle {
  const t = clamp(frame, [start, start + duration], [0, 1], EASE_OUT_QUART);
  const ls = 0.05 * (1 - t);
  return {
    letterSpacing: `${ls}em`,
    opacity: t,
    transform: `translateY(calc(${rise} * ${1 - t}))`,
  };
}
