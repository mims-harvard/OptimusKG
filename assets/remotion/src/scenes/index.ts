import type { ComponentType } from "react";
import type { SceneExit } from "../components/scene";
import { Scene1 } from "./scene-1";
import { Scene2 } from "./scene-2";
import { Scene3 } from "./scene-3";
import { Scene4 } from "./scene-4";
import { Scene5 } from "./scene-5";
import { Scene6 } from "./scene-6";
import { Scene7 } from "./scene-7";
import { Scene8 } from "./scene-8";
import { Scene9 } from "./scene-9";
import { Scene10 } from "./scene-10";
import { Scene11 } from "./scene-11";

export interface SceneSpec {
  id: string;
  name: string;
  durationInFrames: number;
  component: ComponentType;
  exit?: SceneExit;
}

export const SCENES: SceneSpec[] = [
  {
    id: "scene-1",
    name: "scene-1",
    durationInFrames: 240,
    component: Scene1,
  },
  {
    id: "scene-2",
    name: "scene-2",
    // Long enough to give all 3 use-case chips equal hold time. Math:
    // USE_CASE_START (140) + (n-1)·SLOT (2·42) + ENTER (18) + HOLD (24)
    //   + SCENE_EXIT_FRAMES (18) = 284. Rounded to 288 for a small buffer.
    durationInFrames: 288,
    component: Scene2,
  },
  {
    id: "scene-3",
    name: "scene-3",
    durationInFrames: 120,
    component: Scene3,
  },
  {
    id: "scene-4",
    name: "scene-4",
    durationInFrames: 240,
    component: Scene4,
  },
  {
    id: "scene-5",
    name: "scene-5",
    durationInFrames: 210,
    component: Scene5,
  },
  {
    id: "scene-6",
    name: "scene-6",
    // Counters finish ticking by ~frame 69 (startFrames 0/12/24 + 45f tick).
    // 120 frames gives ~33f of hold before the 18f dissolve — tighter than
    // the previous 180f which sat idle for ~1.5s.
    durationInFrames: 120,
    component: Scene6,
  },
  {
    id: "scene-7",
    name: "scene-7",
    durationInFrames: 480,
    component: Scene7,
  },
  {
    id: "scene-8",
    name: "scene-8",
    durationInFrames: 420,
    component: Scene8,
  },
  {
    id: "scene-9",
    name: "scene-9",
    durationInFrames: 120,
    component: Scene9,
  },
  {
    id: "scene-10",
    name: "scene-10",
    durationInFrames: 120,
    component: Scene10,
  },
  {
    id: "scene-11",
    name: "scene-11",
    durationInFrames: 300,
    component: Scene11,
  },
];

export interface ScenePlacement extends SceneSpec {
  from: number;
}

export const SCENE_PLACEMENTS: ScenePlacement[] = (() => {
  const placements: ScenePlacement[] = [];
  let cursor = 0;
  for (const scene of SCENES) {
    placements.push({ ...scene, from: cursor });
    cursor += scene.durationInFrames;
  }
  return placements;
})();

export const TOTAL_DURATION_IN_FRAMES = SCENES.reduce(
  (acc, s) => acc + s.durationInFrames,
  0,
);

export {
  Scene1,
  Scene2,
  Scene3,
  Scene4,
  Scene5,
  Scene6,
  Scene7,
  Scene8,
  Scene9,
  Scene10,
  Scene11,
};
