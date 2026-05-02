import { Heading } from "./heading";
import { Window } from "./window";

// Scene 8 — Python client. 240 frames @ 60fps (4s). Global 1530–1770.
// Beats (local frames):
//   0–~48    Heading words slide in right-to-left, centred.
//   48–90    Hold heading alone, centred.
//   90–~150  Empty window springs in from the right; heading is pushed
//            leftward in sync.
//   150–222  Hold final layout (heading on the left, window on the right).
//   222–240  Heading dissolves; window fades with the scene's exit.

export const Scene8: React.FC = () => (
  <>
    <Heading />
    <Window />
  </>
);
