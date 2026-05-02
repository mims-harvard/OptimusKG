import { AbsoluteFill } from "remotion";
import { StatTicker } from "../../components/stat-ticker";
import { Sfx } from "../../sounds/sfx";
import { spacing } from "../../tokens";

// Beat: three stat lines stacked vertically and centred. All three rows fade
// in together; counters then tick up with a 12-frame cascade.

export const Stats: React.FC = () => (
  <AbsoluteFill
    style={{
      alignItems: "center",
      display: "flex",
      flexDirection: "column",
      gap: spacing[2],
      justifyContent: "center",
      paddingInline: "8rem",
    }}
  >
    <StatTicker
      bg="#dbeafe"
      color="#1d4ed8"
      label="nodes"
      startFrame={0}
      from={127}
      value={190_531}
    />
    <StatTicker
      bg="#fef3c7"
      color="#b45309"
      label="edges"
      startFrame={12}
      from={2976}
      value={21_813_816}
    />
    <StatTicker
      bg="#d1fae5"
      color="#047857"
      label="properties"
      startFrame={24}
      from={79321}
      value={110_276_843}
    />

    {/* Tick when each counter starts ticking up. */}
    <Sfx at={0} sound="tick" />
    <Sfx at={12} sound="tick" />
    <Sfx at={24} sound="tick" />
    {/* Soft chime once the last counter (startFrame 24 + 45f duration) lands. */}
    <Sfx at={70} sound="chime" />
  </AbsoluteFill>
);
