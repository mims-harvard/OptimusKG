import "./index.css";
import { Composition } from "remotion";
import { loadFont } from "@remotion/google-fonts/Inter";
import {
  DURATION_IN_FRAMES,
  FPS,
  HEIGHT,
  OptimusKGVideo,
  WIDTH,
} from "./Composition";

loadFont("normal", { weights: ["400", "500", "700"], subsets: ["latin"] });

export const RemotionRoot: React.FC = () => (
  <Composition
    component={OptimusKGVideo}
    durationInFrames={DURATION_IN_FRAMES}
    fps={FPS}
    height={HEIGHT}
    id="OptimusKG"
    width={WIDTH}
  />
);
