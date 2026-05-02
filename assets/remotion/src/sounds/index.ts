// Sound-effect registry. Each entry maps a logical name → file path
// (relative to public/) plus default playback metadata.
//
// All audio lives in public/sounds/ as mp3. `frames` is the playback window
// the wrapping <Sequence> reserves — keep it ≥ 30 to avoid Remotion Studio's
// AudioWaveform crash on zero-width canvases. The actual file plays once
// regardless of window length.

export interface SfxMeta {
  src: string;
  frames: number;
  volume: number;
}

export const SFX = {
  // Single mechanical key click — fires per word in word-by-word reveals.
  typeKey: { src: "sounds/type-key.mp3", frames: 60, volume: 0.3 },
  // UI panel / window slide-in.
  swoosh: { src: "sounds/swoosh.mp3", frames: 60, volume: 0.4 },
  // Airy text reveal — letter-spacing collapse, hero text resolves.
  swish: { src: "sounds/swish.mp3", frames: 60, volume: 0.35 },
  // Soft "pop" for an element appearing — logo circles, dots, etc.
  pop: { src: "sounds/pop.mp3", frames: 60, volume: 0.35 },
  // Subtle UI tick — counter steps, sidebar / tab cycles.
  tick: { src: "sounds/tick.mp3", frames: 60, volume: 0.3 },
  // Soft completion chime — final stat landings, scene resolutions.
  chime: { src: "sounds/chime.mp3", frames: 90, volume: 0.4 },
} as const satisfies Record<string, SfxMeta>;

export type SfxName = keyof typeof SFX;
