import { useMemo } from "react";
import { AbsoluteFill, useCurrentFrame } from "remotion";
import { fadeRamp } from "../../animations";
import { mulberry32 } from "../../animations/internal";
import { Sfx } from "../../sounds/sfx";
import { fontWeight } from "../../tokens";

// Beat: ontology-ID cloud. CURIEs sit fixed at row-flow positions around
// the heading; each one fades in at its own random delay (no movement).
//
// Placement adapted from the reference at:
//   https://github.com/mims-harvard/OptimusKG/blob/remotion/assets/remotion/src/beats/OntologyGrounded.tsx
// (rows + tagline-hole packing). Frames/colors are scene-5-specific.

// Curated CURIE pool spanning the ontologies used in OptimusKG.
const IDS_POOL: string[] = [
  // Anatomy (UBERON)
  "UBERON:0002107",
  "UBERON:0000955",
  "UBERON:0000948",
  "UBERON:0002048",
  "UBERON:0002113",
  "UBERON:0000945",
  "UBERON:0001264",
  "UBERON:0002385",
  // Biological Process (GO)
  "GO:0008152",
  "GO:0006915",
  "GO:0007049",
  "GO:0006955",
  "GO:0051301",
  "GO:0006954",
  "GO:0023052",
  "GO:0006351",
  // Cellular Component (GO)
  "GO:0005634",
  "GO:0005739",
  "GO:0005886",
  "GO:0005783",
  "GO:0005840",
  "GO:0005737",
  "GO:0005794",
  "GO:0005764",
  // Disease (MONDO)
  "MONDO:0005148",
  "MONDO:0007254",
  "MONDO:0005575",
  "MONDO:0004979",
  "MONDO:0004975",
  "MONDO:0005180",
  "MONDO:0005027",
  "MONDO:0007915",
  // Drug (CHEBI)
  "CHEBI:15365",
  "CHEBI:27732",
  "CHEBI:45783",
  "CHEBI:6904",
  "CHEBI:41774",
  "CHEBI:6801",
  "CHEBI:5855",
  "CHEBI:46195",
  // Exposure (ECTO)
  "ECTO:0000907",
  "ECTO:0009017",
  "ECTO:1000022",
  "ECTO:0000009",
  "ECTO:0000110",
  "ECTO:0000191",
  "ECTO:0001090",
  "ECTO:0005015",
  // Gene (ENSG)
  "ENSG00000163513",
  "ENSG00000141510",
  "ENSG00000139618",
  "ENSG00000146648",
  "ENSG00000136997",
  "ENSG00000157764",
  "ENSG00000133703",
  "ENSG00000130203",
  // Pathway (REACTOME)
  "REACT:R-HSA-162582",
  "REACT:R-HSA-1640170",
  "REACT:R-HSA-168256",
  "REACT:R-HSA-1430728",
  "REACT:R-HSA-74160",
  "REACT:R-HSA-73894",
  "REACT:R-HSA-5357801",
  "REACT:R-HSA-382551",
  // Phenotype (HP)
  "HP:0001250",
  "HP:0001263",
  "HP:0001508",
  "HP:0000252",
  "HP:0001252",
  "HP:0001249",
  "HP:0000256",
  "HP:0002376",
];

const SEED = 13;

const ENTER_START = 36;
const ENTER_MAX_DELAY = 90;
const ENTER_DURATION = 18;

// Row layout — sized to fill the full canvas height. Rows are packed wider
// than the canvas so items extend past both side edges and read as cropped
// (the AbsoluteFill clips them via overflow:hidden).
const FRAME_W = 1920;
const FRAME_H = 1080;
const ROW_OVERFLOW = 280; // pixels each row extends past each side edge
const ITEM_GAP = 48;
const NUM_ROWS = 13;
const ROW_H = Math.floor(FRAME_H / NUM_ROWS); // 83px
const ROW_BAND_TOP = (FRAME_H - NUM_ROWS * ROW_H) / 2;
const TAGLINE_ROW = Math.floor(NUM_ROWS / 2); // 6, middle row
const TAGLINE_HOLE_W = 1500;

// 2.5rem (≈40px) monospace.
const ID_FONT_SIZE = "2.5rem";
const PX_DIGIT = 24;
const PX_UPPER = 26;
const PX_PUNCT = 13;

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
  text: string;
  // Fixed row-flow position, canvas pixel coords.
  x: number;
  y: number;
  // Per-item entry delay (frames after ENTER_START). Randomised order.
  delay: number;
  // Final on-screen alpha (slightly varied for depth).
  opacityMax: number;
}

// Consume CURIEs from `pool` (without replacement) until the row width is
// exhausted. The first item that doesn't fit terminates the row.
function fillSegment(maxWidth: number, pool: string[]): string[] {
  const items: string[] = [];
  let used = 0;
  while (pool.length > 0) {
    const isFirst = items.length === 0;
    const remaining = maxWidth - used - (isFirst ? 0 : ITEM_GAP);
    if (remaining <= 0) break;
    const fitIdx = pool.findIndex((c) => estimateWidth(c) <= remaining);
    if (fitIdx === -1) break;
    const curie = pool.splice(fitIdx, 1)[0];
    items.push(curie);
    used += estimateWidth(curie) + (isFirst ? 0 : ITEM_GAP);
  }
  return items;
}

interface RowSegment {
  rowIdx: number;
  side: "full" | "left" | "right";
  texts: string[];
}

function buildDeck(rand: () => number, copies: number): string[] {
  const deck: string[] = [];
  for (let c = 0; c < copies; c++) {
    const slice = IDS_POOL.slice();
    for (let i = slice.length - 1; i > 0; i--) {
      const j = Math.floor(rand() * (i + 1));
      [slice[i], slice[j]] = [slice[j], slice[i]];
    }
    deck.push(...slice);
  }
  return deck;
}

function generateRowSegments(rand: () => number): RowSegment[] {
  // Repeated, per-copy-shuffled deck — guarantees enough items to fill all
  // rows. Repetition is fine for a backdrop effect.
  const pool = buildDeck(rand, 10);

  const segments: RowSegment[] = [];
  const cx = FRAME_W / 2;
  // Wider than the canvas so items reach past the edges and crop.
  const fullWidth = FRAME_W + 2 * ROW_OVERFLOW;
  const halfWidth = cx - TAGLINE_HOLE_W / 2 + ROW_OVERFLOW;

  for (let r = 0; r < NUM_ROWS; r++) {
    if (r === TAGLINE_ROW) {
      segments.push({ rowIdx: r, side: "left", texts: fillSegment(halfWidth, pool) });
      segments.push({ rowIdx: r, side: "right", texts: fillSegment(halfWidth, pool) });
    } else {
      segments.push({ rowIdx: r, side: "full", texts: fillSegment(fullWidth, pool) });
    }
  }
  return segments;
}

// Convert row segments into laid-out items with absolute pixel coords plus
// per-item entry metadata.
function layout(rand: () => number): RowItem[] {
  const segments = generateRowSegments(rand);
  const items: RowItem[] = [];
  const cx = FRAME_W / 2;

  for (const seg of segments) {
    const totalW =
      seg.texts.reduce((acc, t) => acc + estimateWidth(t), 0) +
      Math.max(0, seg.texts.length - 1) * ITEM_GAP;

    let segLeft: number;
    if (seg.side === "full") {
      // Centred row; row width usually exceeds FRAME_W so it overflows
      // past both edges and gets cropped by overflow:hidden.
      segLeft = cx - totalW / 2;
    } else if (seg.side === "left") {
      // Right edge anchored at the tagline-hole boundary; left edge can
      // extend past the canvas's left side.
      segLeft = cx - TAGLINE_HOLE_W / 2 - totalW;
    } else {
      // Left edge anchored at the tagline-hole boundary; right edge can
      // extend past the canvas's right side.
      segLeft = cx + TAGLINE_HOLE_W / 2;
    }

    const y = ROW_BAND_TOP + seg.rowIdx * ROW_H + ROW_H / 2;
    let x = segLeft;
    for (const text of seg.texts) {
      const w = estimateWidth(text);
      items.push({
        text,
        x: x + w / 2,
        y,
        delay: 0, // assigned in a second pass for global random ordering
        opacityMax: 0.55 + rand() * 0.2,
      });
      x += w + ITEM_GAP;
    }
  }

  // Assign spawn delays in a randomised order across all items so the
  // reveal order is decorrelated from on-screen position.
  const order = items.map((_, i) => i);
  for (let i = order.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [order[i], order[j]] = [order[j], order[i]];
  }
  for (let k = 0; k < order.length; k++) {
    items[order[k]].delay = Math.floor(
      (k / Math.max(1, order.length - 1)) * ENTER_MAX_DELAY,
    );
  }

  return items;
}

export const Cloud: React.FC = () => {
  const frame = useCurrentFrame();

  const items = useMemo(() => layout(mulberry32(SEED)), []);

  return (
    <AbsoluteFill style={{ overflow: "hidden", pointerEvents: "none" }}>
      {/* Fan-out swoosh — scene-5 duration 210 → exit starts at 192. */}
      <Sfx at={192} sound="swoosh" />
      {items.map((p, i) => {
        const t = fadeRamp({
          frame,
          start: ENTER_START + p.delay,
          duration: ENTER_DURATION,
          from: 0,
          to: 1,
        });
        const opacity = p.opacityMax * t;
        return (
          <span
            key={i}
            style={{
              color: "#64748b",
              fontFamily:
                "ui-monospace, SFMono-Regular, 'SF Mono', Menlo, Consolas, monospace",
              fontSize: ID_FONT_SIZE,
              fontWeight: fontWeight.regular,
              left: p.x,
              opacity,
              position: "absolute",
              top: p.y,
              transform: "translate(-50%, -50%)",
              whiteSpace: "nowrap",
            }}
          >
            {p.text}
          </span>
        );
      })}
    </AbsoluteFill>
  );
};
