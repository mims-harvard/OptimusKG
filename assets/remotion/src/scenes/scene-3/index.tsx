import { Introducing } from "./introducing";
import { Wordmark } from "./wordmark";

// Scene 3 — Introducing OptimusKG. 120 frames @ 60fps (2s). Global 480–600.
// Beats (local frames):
//   0–18     "Introducing" letter-spacing collapse, body size.
//   18–36    "OptimusKG" letter-spacing collapse, display size, 1.5rem rise.
//   36–102   Hold.
//   102–120  Synchronised dissolve to white (Scene's default 18-frame exit).

export const Scene3: React.FC = () => (
  <>
    <Introducing />
    <Wordmark />
  </>
);
