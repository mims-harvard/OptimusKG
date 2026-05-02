// Sound-effect registry. Each entry maps a logical name → file path
// (relative to public/) plus default playback metadata.
//
// SFX live in public/sounds/sfx/. Background music lives at public/sounds/song.mp3
// and is wired directly in composition.tsx (not via this registry). `frames`
// is the playback window the wrapping <Sequence> reserves — keep it ≥ 30
// to avoid Remotion Studio's AudioWaveform crash on zero-width canvases;
// the actual file plays once regardless of window length.

export interface SfxMeta {
  src: string;
  frames: number;
  volume: number;
}

export const SFX = {
  // Single mechanical key click — fires per word in word-by-word reveals.
  typeKey: { src: "sounds/sfx/type-key.mp3", frames: 60, volume: 0.3 },
  // UI panel / window slide-in.
  swoosh: { src: "sounds/sfx/swoosh.mp3", frames: 60, volume: 0.4 },
  // Airy text reveal — letter-spacing collapse, hero text resolves.
  swish: { src: "sounds/sfx/swish.mp3", frames: 60, volume: 0.35 },
  // Soft "pop" for an element appearing — logo circles, dots, etc.
  pop: { src: "sounds/sfx/pop.mp3", frames: 60, volume: 0.35 },
  // Subtle UI tick — counter steps, sidebar / tab cycles.
  tick: { src: "sounds/sfx/tick.mp3", frames: 60, volume: 0.3 },
  // Soft completion chime — final stat landings, scene resolutions.
  chime: { src: "sounds/sfx/chime.mp3", frames: 90, volume: 0.4 },
} as const satisfies Record<string, SfxMeta>;

export type SfxName = keyof typeof SFX;
