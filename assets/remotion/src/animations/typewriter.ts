export interface TypewriterParams {
  frame: number;
  start: number;
  text: string;
  charsPerFrame?: number;
  pauseOnPunctuation?: number;
}

const PUNCTUATION = new Set([".", ",", ":", "—"]);

// Typewriter substring with extra pause after . , : —
export function typewriter({
  frame,
  start,
  text,
  charsPerFrame = 0.4,
  pauseOnPunctuation = 8,
}: TypewriterParams): string {
  const elapsed = frame - start;
  if (elapsed <= 0) return "";
  const frameCostPerChar = 1 / charsPerFrame;
  let acc = 0;
  let chars = 0;
  for (let i = 0; i < text.length; i++) {
    acc += frameCostPerChar;
    if (acc > elapsed) break;
    chars = i + 1;
    if (PUNCTUATION.has(text[i])) acc += pauseOnPunctuation;
  }
  return text.slice(0, chars);
}
