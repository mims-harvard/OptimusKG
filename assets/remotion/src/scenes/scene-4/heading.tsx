import { AbsoluteFill, Easing, interpolate, useCurrentFrame } from "remotion";
import { fadeRamp, useStaggered } from "../../animations";
import { heroHeading } from "../../tokens";

// Beat: heading "A multimodal knowledge graph across biomedical domains".
// Centred, alone. Fades out as the figure rises into centre — no push-up,
// just a clean handoff from text to figure.
//
// Phase 1 (0–~60):    word-by-word fade-ramp (6f stagger, 12f ramp).
// Phase 2 (60–90):    hold centred.
// Phase 3 (90–150):   fade out (synced with the figure's rise).

const TEXT = "A multimodal knowledge graph across biomedical domains";
const WORDS = TEXT.split(" ");
const HEADING_START = 0;
const WORD_DELAY = 6;

const FADE_OUT_START = 90;
const FADE_OUT_DURATION = 60;

const EASE = Easing.out(Easing.cubic);

export const Heading: React.FC = () => {
  const frame = useCurrentFrame();
  const elapsed = frame - HEADING_START;

  const wrapperOpacity = interpolate(
    frame,
    [FADE_OUT_START, FADE_OUT_START + FADE_OUT_DURATION],
    [1, 0],
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
          maxWidth: "80rem",
          opacity: wrapperOpacity,
        }}
      >
        {WORDS.map((word, i) => {
          // useStaggered is a pure helper, not a React hook — safe in a loop.
          // eslint-disable-next-line react-hooks/rules-of-hooks
          const local = useStaggered(elapsed, i, WORD_DELAY);
          const opacity = fadeRamp({ frame: local, start: 0, from: 0 });
          return (
            <span
              key={i}
              style={{
                display: "inline-block",
                opacity,
                whiteSpace: "pre",
              }}
            >
              {word}
              {i < WORDS.length - 1 ? " " : ""}
            </span>
          );
        })}
      </h1>
    </AbsoluteFill>
  );
};
