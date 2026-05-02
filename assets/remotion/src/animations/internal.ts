import { Easing, interpolate, spring } from "remotion";

export const EASE_OUT_EXPO = Easing.bezier(0.16, 1, 0.3, 1);
export const EASE_OUT_QUART = Easing.out(Easing.poly(4));
export const EASE_OUT_CUBIC = Easing.out(Easing.cubic);
export const EASE_INOUT_CUBIC = Easing.inOut(Easing.cubic);

export type Easer = (t: number) => number;

// Always-clamped interpolate. Eliminates the repeated extrapolateLeft/Right
// boilerplate at every call site.
export function clamp(
  frame: number,
  range: readonly [number, number],
  output: readonly [number, number],
  easing?: Easer,
): number {
  return interpolate(frame, range, output, {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
    easing,
  });
}

export type SpringConfig = NonNullable<Parameters<typeof spring>[0]["config"]>;

export function mulberry32(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (s + 0x6d2b79f5) >>> 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
