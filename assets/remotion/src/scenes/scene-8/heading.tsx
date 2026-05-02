import { AbsoluteFill, useCurrentFrame, useVideoConfig } from "remotion";
import { WordByWordText } from "../../components/word-by-word-text";
import { springIn } from "../../animations";
import { Sfx } from "../../sounds/sfx";
import { heroHeading } from "../../tokens";
import { WINDOW_START } from "./window";

// Beat: heading centred. Words enter right-to-left with opacity 0 → 1, with
// a soft click on every word's reveal. While the empty window slides in
// from the right (starting at WINDOW_START) the heading is "pushed"
// leftwards in sync. The Scene wrapper handles the dissolve at the end.

const TEXT = "Usable in a single line of code";
const HEADING_START = 0;
const WORD_DELAY = 6;
const WORDS = TEXT.split(" ");

const PUSH_REM = -22; // final translateX of the heading

export const Heading: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  // Heading shifts left in sync with the window's spring-in (same start
  // frame; same spring config so they appear coupled).
  const pushT = springIn({
    frame,
    start: WINDOW_START,
    fps,
    config: { damping: 22, stiffness: 110, mass: 1 },
  });
  const pushX = PUSH_REM * pushT;

  return (
    <AbsoluteFill
      style={{
        alignItems: "center",
        display: "flex",
        // Same row-direction fix as window.tsx so that `justifyContent`
        // affects the horizontal axis.
        flexDirection: "row",
        justifyContent: "center",
        paddingInline: "8rem",
      }}
    >
      <h1
        style={{
          ...heroHeading,
          fontSize: "3.75rem",
          maxWidth: "60rem",
          transform: `translateX(${pushX}rem)`,
        }}
      >
        <WordByWordText
          distance="2rem"
          motion="slide-left"
          startFrame={HEADING_START}
          text={TEXT}
          wordDelay={WORD_DELAY}
        />
      </h1>

      {WORDS.map((_, i) => (
        <Sfx at={HEADING_START + i * WORD_DELAY} key={i} sound="typeKey" />
      ))}
    </AbsoluteFill>
  );
};
