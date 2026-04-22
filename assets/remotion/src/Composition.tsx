import { AbsoluteFill, Sequence } from "remotion";
import { BEAT_PLACEMENTS, DURATION_IN_FRAMES } from "./scenes";

export const FPS = 30;
export const WIDTH = 1920;
export const HEIGHT = 1080;
export { DURATION_IN_FRAMES };

export const OptimusKGVideo: React.FC = () => (
  <AbsoluteFill className="bg-fd-background text-fd-foreground">
    {BEAT_PLACEMENTS.map((beat) => {
      const Comp = beat.component;
      return (
        <Sequence
          durationInFrames={beat.durationInFrames}
          from={beat.from}
          key={beat.id}
          name={beat.label}
        >
          <Comp
            heroText={beat.heroText}
            layout={beat.layout}
            {...(beat.props ?? {})}
          />
        </Sequence>
      );
    })}
  </AbsoluteFill>
);
