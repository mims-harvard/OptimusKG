import "./index.css";
import { Composition } from "remotion";
import { OptimusKGVideo, FPS, DURATION_IN_FRAMES, WIDTH, HEIGHT } from "./Composition";

export const RemotionRoot: React.FC = () => {
  return (
    <>
      <Composition
        id="OptimusKG"
        component={OptimusKGVideo}
        durationInFrames={DURATION_IN_FRAMES}
        fps={FPS}
        width={WIDTH}
        height={HEIGHT}
      />
    </>
  );
};
