import { AbsoluteFill } from "remotion";
import { WordByWordText } from "../../components/word-by-word-text";
import { heroHeading } from "../../tokens";

// Beat: hero hook copy. Local 0–102.
// 9 words enter via word-by-word fade-ramp, 6f stagger, 12f ramp each.

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
        startFrame={0}
        text="What if you could trust your knowledge graph?"
        wordDelay={6}
      />
    </h1>
  </AbsoluteFill>
);
