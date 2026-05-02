import { AbsoluteFill } from "remotion";
import { Scene } from "./components/scene";
import { SCENE_PLACEMENTS, TOTAL_DURATION_IN_FRAMES } from "./scenes";

export const FPS = 60;
export const WIDTH = 1920;
export const HEIGHT = 1080;
export const DURATION_IN_FRAMES = TOTAL_DURATION_IN_FRAMES;

export const Video: React.FC = () => (
  <AbsoluteFill style={{ backgroundColor: "white" }}>
    {SCENE_PLACEMENTS.map(
      ({ id, name, from, durationInFrames, exit, component: Comp }) => (
        <Scene
          durationInFrames={durationInFrames}
          exit={exit}
          from={from}
          key={id}
          name={name}
        >
          <Comp />
        </Scene>
      ),
    )}
  </AbsoluteFill>
);
