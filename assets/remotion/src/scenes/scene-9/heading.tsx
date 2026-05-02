import { AbsoluteFill } from "remotion";
import { WordByWordText } from "../../components/word-by-word-text";
import { Sfx } from "../../sounds/sfx";
import { heroHeading } from "../../tokens";

// Beat: heading centred on one line. Words rise from below with opacity 0 → 1
// (motion="rise"), 6-frame stagger between words, with a soft click on
// every word's reveal.

const TEXT = "An open science, research initiative";
const HEADING_START = 0;
const WORD_DELAY = 6;
const WORDS = TEXT.split(" ");

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
