import { Circle } from "./circle";
import { Heading } from "./heading";
import { PaperQA } from "./paper-qa";

// Scene 7 — Independent validation. 480 frames @ 60fps (8s). Global 1230–1710.
// Beats (local frames):
//   0–28     Circle scales in.
//   28–55    Dots spring around the circle (5f stagger).
//   28–150   Hold heading + circle.
//   150–180  Heading and circle fade out.
//   168–198  PaperQA3 window slides up + fades in (overlaps the fade-out).
//   198–438  Sidebar cycles Anatomy → Gene → Molecular Function → Phenotype
//            (60 frames per entity), tabs accumulate, charts crossfade.
//   438–462  Hold final state.
//   462–480  Dissolve to white (Scene's default 18-frame exit).

export const Scene7: React.FC = () => (
  <>
    <Circle />
    <Heading />
    <PaperQA />
  </>
);
