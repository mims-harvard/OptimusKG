// useStaggered — per-item delay across words, particles, rows.
// Returns a clamped local frame (frame - index * base) that downstream
// primitives can feed in as `start = 0`. Pure function; the `use` prefix is
// the requested API name, not a React hook.
export function useStaggered(frame: number, index: number, base = 6): number {
  return Math.max(0, frame - index * base);
}
