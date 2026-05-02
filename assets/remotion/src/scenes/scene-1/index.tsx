import { Graph } from "./graph";
import { HookText } from "./hook-text";

// Scene 1. 240 frames @ 60fps (4s).
// Beats (local frames):
//   0–102    Hook text words enter via word-by-word fade-ramp.
//   60–~190  Graph nodes pop in around the text; edges draw between them.
//   ~190–222 Hold full graph.
//   222–240  Dissolve to white (Scene's default 18-frame exit).

export const Scene1: React.FC = () => (
  <>
    <Graph />
    <HookText />
  </>
);
