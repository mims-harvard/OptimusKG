import { Cloud } from "./cloud";
import { Heading } from "./heading";

// Scene 5 — Ontology grounded. 210 frames @ 60fps (3.5s). Global 840–1050.
// Beats (local frames):
//   0–36     Heading letter-spacing collapse, slower (longer line).
//   36–~156  60 ontology IDs swarm inward from off-canvas, per-particle
//            random delay 0–60f, spring (damping 14, stiffness 90) to
//            scattered positions around the heading.
//   156–192  Hold.
//   192–210  IDs disperse outward (mirrored config) and the heading
//            dissolves with the scene's default 18-frame exit.

export const Scene5: React.FC = () => (
  <>
    <Cloud />
    <Heading />
  </>
);
