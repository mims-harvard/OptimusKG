import { Stats } from "./stats";

// Scene 6 — Stats. 180 frames @ 60fps (3s). Global 1050–1230.
// Beats (local frames):
//   0–18     All three labels fade in simultaneously.
//   18–63    Nodes counter ticks 0 → 1.2M (Easing.out cubic).
//   30–75    Edges counter ticks 0 → 8.4M (offset 12f).
//   42–87    Properties counter ticks 0 → 45M (offset 24f).
//   87–162   Hold.
//   162–180  Synchronised dissolve to white (Scene's default 18-frame exit).

export const Scene6: React.FC = () => <Stats />;
