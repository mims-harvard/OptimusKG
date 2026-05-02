import { Audio, Sequence, staticFile } from "remotion";
import { SFX, type SfxName } from "./index";

interface SfxProps {
  // Logical sound name from the SFX registry.
  sound: SfxName;
  // Frame (within the enclosing scene's local timeline) where the sound
  // should fire.
  at: number;
  // Optional override of the registry's default volume.
  volume?: number;
  // Optional override of the wrapping Sequence's duration. Defaults to
  // the registry's `frames` value.
  durationInFrames?: number;
}

// Drop one of these next to a beat to fire a sound at a specific frame:
//
//   <Sfx sound="pop" at={42} />
//   <Sfx sound="whoosh" at={90} volume={0.6} />
//
// The wrapping <Sequence> mounts the <Audio> only during its window,
// so the audio cleans up after itself and won't bleed into the next scene.
export const Sfx: React.FC<SfxProps> = ({
  sound,
  at,
  volume,
  durationInFrames,
}) => {
  const meta = SFX[sound];
  const v = volume ?? meta.volume;
  return (
    <Sequence
      durationInFrames={durationInFrames ?? meta.frames}
      from={at}
    >
      {/* Remotion's lint plugin prefers a callback so volume can be
          frame-evaluated; for a constant we just return v. */}
      <Audio src={staticFile(meta.src)} volume={() => v} />
    </Sequence>
  );
};
