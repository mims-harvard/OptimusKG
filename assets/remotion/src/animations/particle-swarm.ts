import { mulberry32 } from "./internal";

export type ParticleMode = "inward" | "outward" | "orbit";

export interface ParticleSwarmParams {
  frame: number;
  start: number;
  count: number;
  seed: number;
  mode: ParticleMode;
  duration?: number;
  radius?: number;
}

export interface Particle {
  x: number;
  y: number;
  opacity: number;
  scale: number;
  rotation: number;
}

const easeOutCubic = (t: number) => 1 - (1 - t) ** 3;

export function particleSwarm({
  frame,
  start,
  count,
  seed,
  mode,
  duration = 30,
  radius = 200,
}: ParticleSwarmParams): Particle[] {
  const rand = mulberry32(seed);
  const elapsed = Math.max(0, frame - start);
  const t = easeOutCubic(Math.min(1, elapsed / duration));
  const out: Particle[] = [];
  for (let i = 0; i < count; i++) {
    const angle = (i / count) * Math.PI * 2 + rand() * 0.4;
    const scatter = 40 + rand() * 120;
    const off = 800 + rand() * 200;
    const rotation = rand() * 360;
    let x = 0;
    let y = 0;
    let opacity = t;
    let scale = 0.5 + 0.5 * t;
    if (mode === "inward") {
      const sx = Math.cos(angle) * scatter;
      const sy = Math.sin(angle) * scatter;
      x = Math.cos(angle) * off * (1 - t) + sx * t;
      y = Math.sin(angle) * off * (1 - t) + sy * t;
    } else if (mode === "outward") {
      x = Math.cos(angle) * off * t;
      y = Math.sin(angle) * off * t;
      opacity = 1 - t;
      scale = 1 - 0.5 * t;
    } else {
      x = Math.cos(angle) * radius;
      y = Math.sin(angle) * radius;
    }
    out.push({ x, y, opacity, scale, rotation });
  }
  return out;
}
