import { AbsoluteFill, Audio, interpolate, staticFile } from "remotion";
import { Scene } from "./components/scene";
import { SCENE_PLACEMENTS, TOTAL_DURATION_IN_FRAMES } from "./scenes";

export const FPS = 60;
export const WIDTH = 1920;
export const HEIGHT = 1080;
export const DURATION_IN_FRAMES = TOTAL_DURATION_IN_FRAMES;

// Background music — single track playing for the whole composition.
// Volume is mixed quietly so per-scene SFX sit clearly on top.
const BG_MUSIC_VOLUME = 0.15;
// Frames over which the music fades out at the end of the composition,
// so it resolves gracefully into the final dissolve-to-white instead of
// cutting hard.
const FADE_OUT_FRAMES = 60;

export const Video: React.FC = () => (
  <AbsoluteFill style={{ backgroundColor: "white" }}>
    <Audio
      src={staticFile("sounds/song.mp3")}
      volume={(f) =>
        interpolate(
          f,
          [DURATION_IN_FRAMES - FADE_OUT_FRAMES, DURATION_IN_FRAMES],
          [BG_MUSIC_VOLUME, 0],
          { extrapolateLeft: "clamp", extrapolateRight: "clamp" },
        )
      }
    />
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
