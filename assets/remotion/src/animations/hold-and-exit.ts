import { clamp } from "./internal";

export interface HoldAndExitParams {
  frame: number;
  sceneEnd: number;
  exitDuration?: number;
}

// Stays at 1 until sceneEnd - exitDuration, then fades to 0.
export function holdAndExit({
  frame,
  sceneEnd,
  exitDuration = 18,
}: HoldAndExitParams): number {
  return clamp(frame, [sceneEnd - exitDuration, sceneEnd], [1, 0]);
}
