import { AbsoluteFill } from "remotion";
import { WordByWordText } from "../../components/word-by-word-text";
import { heroHeading } from "../../tokens";

// Beat: heading centred on one line. Words rise from below with opacity 0 → 1
// (motion="rise"), 6-frame stagger between words.

export const Heading: React.FC = () => (
  <AbsoluteFill
    style={{
      alignItems: "center",
      display: "flex",
      flexDirection: "row",
      justifyContent: "center",
      paddingInline: "4rem",
    }}
  >
    <h1 style={{ ...heroHeading, whiteSpace: "nowrap" }}>
      <WordByWordText
        distance="1.5rem"
        motion="rise"
        startFrame={0}
        text="An open science, research initiative"
        wordDelay={6}
      />
    </h1>
  </AbsoluteFill>
);
