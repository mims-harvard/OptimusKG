import { Heading } from "./heading";

// Scene 2 — Use cases. 240 frames @ 60fps (4s). Global 240–480.
// Beats (local frames):
//   0–18     "And use it for" enters via letter-spacing collapse.
//   18–60    Hold.
//   60–75    "Graph AI" fades in (fadeRamp + spring translateY 0.5rem).
//   90–105   "Biomedical discovery" fades in.
//   120–135  "Hypothesis generation" fades in.
//   135–222  Hold all four.
//   222–240  Dissolve to white (Scene's default 18-frame exit).

export const Scene2: React.FC = () => (
  <>
    <Heading />
  </>
);
