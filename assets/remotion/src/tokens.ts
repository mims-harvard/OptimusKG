export const fontSize = {
  caption: "1.375rem",
  body: "2rem",
  hero: "4.0rem",
  display: "5.25rem",
}

export const fontWeight = {
  regular: 400,
  medium: 500,
  semibold: 600,
} as const;

export type FontWeightToken = keyof typeof fontWeight;

export const spacing = {
  0.5: "0.5rem",
  1: "1rem",
  1.5: "1.5rem",
  2: "2rem",
  3: "3rem",
  4: "4rem",
  6: "6rem",
  8: "8rem",
} as const;

export type SpacingToken = keyof typeof spacing;

import type { CSSProperties } from "react";

// Canonical h1 styling for hero copy across scenes. Spread into the h1's
// `style` and add scene-specific overrides (opacity, transform, maxWidth) on
// top.
export const heroHeading: CSSProperties = {
  fontSize: fontSize.hero,
  fontWeight: fontWeight.semibold,
  letterSpacing: "-0.05em",
  lineHeight: 1.5,
  margin: 0,
  textAlign: "center",
};
