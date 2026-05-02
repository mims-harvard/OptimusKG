import { LogoMark } from "./logo-mark";
import { Snippet } from "./snippet";

// Scene 11 — End card. 300 frames @ 60fps (5s).
// Beats (local frames):
//   0–~50    Five logo circles assemble; four connecting lines fade in.
//            Logo is centred, alone.
//   50–90    Hold logo centred.
//   90–120   Wordmark "OptimusKG" grows in beside the logo; the logo
//            shifts left so the combined logo+wordmark stays centred.
//   120–160  Hold logo + wordmark.
//   160–184  "uv add optimuskg" snippet rises in at vertical 60%.
//   184–282  Hold full layout.
//   282–300  Dissolve to white (Scene's default 18-frame exit).

export const Scene11: React.FC = () => (
  <>
    <LogoMark />
    <Snippet />
  </>
);
