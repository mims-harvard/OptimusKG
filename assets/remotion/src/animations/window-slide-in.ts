import { springIn } from "./spring-in";

export type SlideSide = "left" | "right" | "top" | "bottom";

export interface WindowSlideInParams {
  frame: number;
  start: number;
  fromSide: SlideSide;
  fps: number;
}

export interface WindowSlideInStyle {
  transform: string;
  opacity: number;
  boxShadow: string;
}

const SLIDE_AXIS: Record<SlideSide, { axis: "X" | "Y"; sign: -1 | 1 }> = {
  left: { axis: "X", sign: -1 },
  right: { axis: "X", sign: 1 },
  top: { axis: "Y", sign: -1 },
  bottom: { axis: "Y", sign: 1 },
};

export function windowSlideIn({
  frame,
  start,
  fromSide,
  fps,
}: WindowSlideInParams): WindowSlideInStyle {
  const t = springIn({ frame, start, fps });
  const { axis, sign } = SLIDE_AXIS[fromSide];
  const unit = axis === "X" ? "vw" : "vh";
  const offset = sign * (1 - t) * 100;
  const scale = 0.94 + 0.06 * t;
  const blur = 2.5 * t;
  return {
    transform: `translate${axis}(${offset}${unit}) scale(${scale})`,
    opacity: t,
    boxShadow: `0 ${blur / 2}rem ${blur}rem rgba(0, 0, 0, 0.18)`,
  };
}
