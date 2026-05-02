import { Figure } from "./figure";
import { Heading } from "./heading";

// Scene 4 — Multimodal description. 240 frames @ 60fps (4s). Global 600–840.
// Beats (local frames):
//   0–60     Heading enters word-by-word, vertically centred and alone.
//   60–90    Hold heading alone.
//   90–150   Figure spring-rises from below into centre, while the heading
//            simultaneously fades out (clean handoff, no overlap).
//   150–222  Hold figure centred and alone.
//   222–240  Figure dissolves to white via Scene's default 18-frame exit.

export const Scene4: React.FC = () => (
  <>
    <Heading />
    <Figure />
  </>
);
