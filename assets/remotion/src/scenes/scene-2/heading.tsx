import { AbsoluteFill, Easing, interpolate, useCurrentFrame } from "remotion";
import { heroHeading } from "../../tokens";

// Beat: heading "And use it to do research in".
// Phase 1 (0–~48):  each word slides in from the right (right → left), 6f stagger.
// Phase 2 (60–~110): first 5 words ("And use it to do") fade out left → right.
// Phase 3 (110–134): the whole h1 translates left so "research in" lands at
//                    screen centre. The faded leading words still occupy
//                    layout space, so this shift is what visually re-centres
//                    the tail.
// 134+:             hold.

const TEXT = "And use it to do research in";
const WORDS = TEXT.split(" ");
// Words 0..LEAD_COUNT-1 fade out in phase 2.
const LEAD_COUNT = 5;

const ENTRY_START = 0;
const ENTRY_WORD_DELAY = 6;
const ENTRY_DURATION = 12;
// rem of horizontal travel for each word's slide-in (positive = from the right).
const ENTRY_DISTANCE_REM = 2;

const EXIT_START = 60;
const EXIT_WORD_DELAY = 8;
const EXIT_DURATION = 14;

const SHIFT_START = 110;
const SHIFT_DURATION = 24;
// The h1's natural centre includes the (initially invisible) chip slot on
// the right — without compensation the text alone reads as off-centre to
// the left at frame 0. INITIAL_SHIFT_REM nudges the h1 right by ~half the
// slot width so "And use it to do research in" reads centred initially;
// SHIFT_REM then animates left so "research in <chip>" reads centred at
// the end. Tune both by eye for the longest chip in USE_CASES.
const INITIAL_SHIFT_REM = 25;
const SHIFT_REM = -15;

// Use-case chips cycle in the slot after "research in". Each rises in from
// below; when the next one's turn comes the previous one vanishes upward
// (its exit overlaps the next one's entry, so the swap reads as one motion).
// The widest chip determines the slot width — the hidden reservation span
// inside the container holds the layout open so swaps don't reflow.
interface UseCase {
  text: string;
  bg: string;
  color: string;
}

const USE_CASES: UseCase[] = [
  { text: "graph-based machine learning", bg: "#dbeafe", color: "#1d4ed8" }, // blue
  { text: "knowledge-grounded retrieval", bg: "#fef3c7", color: "#b45309" }, // amber
  { text: "biomedical hypothesis generation", bg: "#d1fae5", color: "#047857" }, // emerald
];
const WIDEST_USE_CASE = USE_CASES.reduce((a, b) =>
  a.text.length >= b.text.length ? a : b,
).text;
const USE_CASE_START = SHIFT_START + SHIFT_DURATION + 6;
const USE_CASE_ENTER = 18;
const USE_CASE_HOLD = 24;
const USE_CASE_EXIT = 18;
const USE_CASE_RISE_REM = 1.5;
// Slot frames between consecutive chip entry-starts. Equal to ENTER + HOLD
// so the next chip starts entering at the same frame the current starts
// exiting — they cross in opposite directions for a clean swap.
const USE_CASE_SLOT_FRAMES = USE_CASE_ENTER + USE_CASE_HOLD;

const EASE = Easing.bezier(0.16, 1, 0.3, 1);

const ramp = (frame: number, start: number, duration: number) =>
  interpolate(frame, [start, start + duration], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
    easing: EASE,
  });

export const Heading: React.FC = () => {
  const frame = useCurrentFrame();

  const shift = interpolate(
    frame,
    [SHIFT_START, SHIFT_START + SHIFT_DURATION],
    [INITIAL_SHIFT_REM, SHIFT_REM],
    {
      extrapolateLeft: "clamp",
      extrapolateRight: "clamp",
      easing: EASE,
    },
  );


  return (
    <AbsoluteFill
      style={{
        alignItems: "center",
        display: "flex",
        justifyContent: "center",
        paddingInline: "8rem",
      }}
    >
      <h1
        style={{
          ...heroHeading,
          transform: `translateX(${shift}rem)`,
          whiteSpace: "nowrap",
        }}
      >
        {WORDS.map((word, i) => {
          const entryProgress = ramp(
            frame,
            ENTRY_START + i * ENTRY_WORD_DELAY,
            ENTRY_DURATION,
          );
          const exitProgress =
            i < LEAD_COUNT
              ? ramp(frame, EXIT_START + i * EXIT_WORD_DELAY, EXIT_DURATION)
              : 0;

          const opacity = entryProgress * (1 - exitProgress);
          const tx = ENTRY_DISTANCE_REM * (1 - entryProgress);

          return (
            <span
              key={i}
              style={{
                display: "inline-block",
                opacity,
                transform: `translateX(${tx}rem)`,
                whiteSpace: "pre",
              }}
            >
              {word}
              {i < WORDS.length - 1 ? " " : ""}
            </span>
          );
        })}
        <span
          style={{
            display: "inline-grid",
            gridTemplateAreas: '"slot"',
            marginInlineStart: "0.4em",
            placeItems: "center",
            verticalAlign: "baseline",
          }}
        >
          {/* Width reservation — sized to the widest chip so swaps don't reflow. */}
          <span
            aria-hidden
            style={{
              borderRadius: "0.75rem",
              gridArea: "slot",
              paddingBlock: "0.15em",
              paddingInline: "0.5em",
              visibility: "hidden",
              whiteSpace: "pre",
            }}
          >
            {WIDEST_USE_CASE}
          </span>
          {USE_CASES.map((uc, i) => {
            const enterStart = USE_CASE_START + i * USE_CASE_SLOT_FRAMES;
            const exitStart = enterStart + USE_CASE_ENTER + USE_CASE_HOLD;
            const isLast = i === USE_CASES.length - 1;

            const enterProgress = ramp(frame, enterStart, USE_CASE_ENTER);
            const exitProgress = isLast
              ? 0
              : ramp(frame, exitStart, USE_CASE_EXIT);

            const opacity = enterProgress * (1 - exitProgress);
            // Below baseline while entering, above baseline while exiting.
            const ty =
              USE_CASE_RISE_REM * (1 - enterProgress) -
              USE_CASE_RISE_REM * exitProgress;

            return (
              <span
                key={i}
                style={{
                  backgroundColor: uc.bg,
                  borderRadius: "0.75rem",
                  color: uc.color,
                  gridArea: "slot",
                  opacity,
                  paddingBlock: "0.15em",
                  paddingInline: "0.5em",
                  transform: `translateY(${ty}rem)`,
                  whiteSpace: "pre",
                }}
              >
                {uc.text}
              </span>
            );
          })}
        </span>
      </h1>
    </AbsoluteFill>
  );
};
