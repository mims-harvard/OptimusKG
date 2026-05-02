import { AbsoluteFill, useCurrentFrame } from "remotion";
import { letterSpacingCollapse } from "../../animations";
import { heroHeading } from "../../tokens";

// Beat: heading "Where every entity is ontology grounded".
// Centred. Letter-spacing collapse runs slower (36f) since the line is long.
// Uses the shared heroHeading token so typography matches the other scenes.

export const Heading: React.FC = () => {
  const frame = useCurrentFrame();
  const collapse = letterSpacingCollapse({ frame, start: 0, duration: 36 });

  return (
    <AbsoluteFill
      style={{
        alignItems: "center",
        display: "flex",
        justifyContent: "center",
        paddingInline: "8rem",
      }}
    >
      <h1 style={{ ...heroHeading, ...collapse }}>
        Where every entity is ontology grounded
      </h1>
    </AbsoluteFill>
  );
};
