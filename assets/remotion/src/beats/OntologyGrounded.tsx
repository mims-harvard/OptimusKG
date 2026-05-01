import { useMemo } from "react";
import { AbsoluteFill, Easing, interpolate, useCurrentFrame } from "remotion";
import { HERO_HEADING } from "../Beat";
import type { BeatRenderProps } from "../scenes";

// Beat (180 frames):
//   0–36     Tagline fades in, centered.
//   36–120   CURIEs spawn one-by-one in row-flow positions (no overlap,
//            uniform gap between siblings within each row).
//   120–168  Hold.
//   168–180  Whole stack fades out.

export interface OntologyEntity {
  position: number;
  curie: string;
}

interface OntologyGroundedProps extends BeatRenderProps {
  entities?: OntologyEntity[];
}

const TAGLINE_FADE_IN = [0, 36] as const;
const FADE_OUT = [168, 180] as const;
const SPAWN_WINDOW = 72;
const SPAWN_FADE = 24;
const EASE = Easing.inOut(Easing.quad);

const FRAME_W = 1920;
const FRAME_H = 1080;
const SIDE_MARGIN = 16;
const ITEM_GAP = 40; // identical gap between every CURIE within a row
const ROW_H = 82;
const NUM_ROWS = 13;
const ROW_BAND_TOP = (FRAME_H - NUM_ROWS * ROW_H) / 2;
const TAGLINE_ROW = 6;
const TAGLINE_HOLE_W = 760;

// Per-character-class width estimates for Inter at 3rem / weight 500
// (slightly conservative so items don't overflow the frame edges).
const PX_DIGIT = 27;
const PX_UPPER = 33;
const PX_PUNCT = 16;

function estimateWidth(s: string): number {
  let w = 0;
  for (let i = 0; i < s.length; i++) {
    const ch = s.charCodeAt(i);
    if (ch >= 48 && ch <= 57) w += PX_DIGIT;
    else if (ch === 58 || ch === 45) w += PX_PUNCT;
    else w += PX_UPPER;
  }
  return w;
}

interface RowItem {
  curie: string;
  spawnFrame: number;
  opacityMax: number;
}

interface RowSegment {
  rowIdx: number;
  side: "full" | "left" | "right";
  items: RowItem[];
}

function mulberry32(seed: number) {
  let s = seed >>> 0;
  return () => {
    s = (s + 0x6d2b79f5) >>> 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// Consume CURIEs from `pool` (without replacement) until the row segment
// is full. Pool is mutated — once empty, no more rows get items.
function fillSegment(maxWidth: number, pool: string[]): string[] {
  const items: string[] = [];
  let used = 0;
  while (pool.length > 0) {
    const isFirst = items.length === 0;
    const remaining = maxWidth - used - (isFirst ? 0 : ITEM_GAP);
    if (remaining <= 0) break;
    // First item in the (shuffled) pool that fits.
    const fitIdx = pool.findIndex((c) => estimateWidth(c) <= remaining);
    if (fitIdx === -1) break;
    const curie = pool.splice(fitIdx, 1)[0];
    items.push(curie);
    used += estimateWidth(curie) + (isFirst ? 0 : ITEM_GAP);
  }
  return items;
}

function generateRows(curies: string[]): RowSegment[] {
  if (curies.length === 0) return [];
  const rand = mulberry32(13);

  // Shuffled pool — each CURIE is consumed at most once.
  const pool = curies.slice();
  for (let i = pool.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [pool[i], pool[j]] = [pool[j], pool[i]];
  }

  const segments: RowSegment[] = [];
  const cx = FRAME_W / 2;
  const fullWidth = FRAME_W - 2 * SIDE_MARGIN;
  const halfWidth = cx - TAGLINE_HOLE_W / 2 - SIDE_MARGIN;

  for (let r = 0; r < NUM_ROWS; r++) {
    if (r === TAGLINE_ROW) {
      segments.push({
        rowIdx: r,
        side: "left",
        items: fillSegment(halfWidth, pool).map((c) => ({
          curie: c,
          spawnFrame: 0,
          opacityMax: 0,
        })),
      });
      segments.push({
        rowIdx: r,
        side: "right",
        items: fillSegment(halfWidth, pool).map((c) => ({
          curie: c,
          spawnFrame: 0,
          opacityMax: 0,
        })),
      });
    } else {
      segments.push({
        rowIdx: r,
        side: "full",
        items: fillSegment(fullWidth, pool).map((c) => ({
          curie: c,
          spawnFrame: 0,
          opacityMax: 0,
        })),
      });
    }
  }

  // Assign spawn metadata (random order across all items).
  const flat: RowItem[] = segments.flatMap((s) => s.items);
  const order = flat.map((_, i) => i);
  for (let i = order.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [order[i], order[j]] = [order[j], order[i]];
  }
  for (let k = 0; k < order.length; k++) {
    flat[order[k]].spawnFrame =
      36 + Math.floor((k / Math.max(1, order.length)) * SPAWN_WINDOW);
    flat[order[k]].opacityMax = 0.5 + rand() * 0.2;
  }

  return segments;
}

export const OntologyGrounded: React.FC<OntologyGroundedProps> = ({
  heroText,
  entities = [],
}) => {
  const frame = useCurrentFrame();
  const tagline = (heroText as string) ?? "Every entity is ontology grounded.";

  const segments = useMemo(
    () => generateRows(entities.map((e) => e.curie)),
    [entities],
  );

  const taglineOpacity = interpolate(frame, TAGLINE_FADE_IN, [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
    easing: EASE,
  });

  const stackOpacity = interpolate(frame, FADE_OUT, [1, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  return (
    <AbsoluteFill
      className="bg-fd-background"
      style={{ opacity: stackOpacity, overflow: "hidden" }}
    >
      {segments.map((seg, i) => {
        const top = ROW_BAND_TOP + seg.rowIdx * ROW_H;
        const isTaglineRow = seg.rowIdx === TAGLINE_ROW;
        const sideStyle: React.CSSProperties =
          seg.side === "full"
            ? { left: SIDE_MARGIN, right: SIDE_MARGIN, justifyContent: "center" }
            : seg.side === "left"
              ? {
                  left: SIDE_MARGIN,
                  width: FRAME_W / 2 - TAGLINE_HOLE_W / 2 - SIDE_MARGIN,
                  justifyContent: "flex-start",
                }
              : {
                  right: SIDE_MARGIN,
                  width: FRAME_W / 2 - TAGLINE_HOLE_W / 2 - SIDE_MARGIN,
                  justifyContent: "flex-end",
                };
        return (
          <div
            key={`${seg.rowIdx}-${seg.side}-${i}`}
            style={{
              position: "absolute",
              top,
              height: ROW_H,
              display: "flex",
              alignItems: "center",
              gap: ITEM_GAP,
              ...sideStyle,
            }}
          >
            {seg.items.map((it, j) => {
              const opacity = interpolate(
                frame,
                [it.spawnFrame, it.spawnFrame + SPAWN_FADE],
                [0, it.opacityMax],
                {
                  extrapolateLeft: "clamp",
                  extrapolateRight: "clamp",
                  easing: EASE,
                },
              );
              const lift = interpolate(
                frame,
                [it.spawnFrame, it.spawnFrame + SPAWN_FADE],
                [6, 0],
                {
                  extrapolateLeft: "clamp",
                  extrapolateRight: "clamp",
                  easing: EASE,
                },
              );
              return (
                <span
                  key={j}
                  style={{
                    ...HERO_HEADING,
                    lineHeight: 1,
                    color: "var(--color-fd-muted-foreground)",
                    whiteSpace: "nowrap",
                    opacity,
                    transform: `translateY(${lift}px)`,
                  }}
                >
                  {it.curie}
                </span>
              );
            })}
            {/* Suppress unused-row-warning for tagline row container without items. */}
            {isTaglineRow && seg.items.length === 0 ? <span /> : null}
          </div>
        );
      })}

      <div
        style={{
          position: "absolute",
          top: "50%",
          left: "50%",
          transform: "translate(-50%, -50%)",
          textAlign: "center",
          opacity: taglineOpacity,
        }}
      >
        <div
          className="text-fd-foreground"
          style={{ ...HERO_HEADING, whiteSpace: "nowrap" }}
        >
          {tagline}
        </div>
      </div>
    </AbsoluteFill>
  );
};
