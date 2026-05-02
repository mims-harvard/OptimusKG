import { AbsoluteFill } from "remotion";
import { WordByWordText } from "../../components/word-by-word-text";
import { Sfx } from "../../sounds/sfx";
import { heroHeading } from "../../tokens";

// Beat: hero hook copy. Local 0–102.
// Words enter via word-by-word fade-ramp, 6f stagger, 12f ramp each, with
// a soft click on every word's reveal.

const TEXT = "What if you could trust your knowledge graph?";
const HOOK_START = 0;
const WORD_DELAY = 6;
const WORDS = TEXT.split(" ");

export const HookText: React.FC = () => (
  <AbsoluteFill
    style={{
      alignItems: "center",
      display: "flex",
      justifyContent: "center",
      paddingInline: "8rem",
    }}
  >
    <h1 style={{ ...heroHeading, maxWidth: "50rem" }}>
      <WordByWordText
        distance="1.5rem"
        motion="rise"
        startFrame={HOOK_START}
        text={TEXT}
        wordDelay={WORD_DELAY}
      />
    </h1>

    {WORDS.map((_, i) => (
      <Sfx
        at={HOOK_START + i * WORD_DELAY}
        key={i}
        sound="typeKey"
      />
    ))}
  </AbsoluteFill>
);
