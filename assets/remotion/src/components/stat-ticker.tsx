import { useCurrentFrame } from "remotion";
import { fadeRamp, numberTicker } from "../animations";
import { fontSize, fontWeight } from "../tokens";

export type StatFormat = "comma" | "compact";

const FORMATTERS: Record<StatFormat, (v: number) => string> = {
  comma: (v) => Math.round(v).toLocaleString("en-US"),
  compact: (v) =>
    Intl.NumberFormat("en-US", { notation: "compact" }).format(Math.round(v)),
};

interface StatTickerProps {
  label: string;
  value: number;
  // Frame at which the counter starts ticking up.
  startFrame: number;
  // Frame at which the whole row begins to fade in. Defaults to 0.
  labelStart?: number;
  // Frames over which the row fades in.
  labelDuration?: number;
  // Frames over which the counter ticks from `from` → `value`.
  duration?: number;
  format?: StatFormat;
  // Counter starts at this value rather than 0 so the initial display has
  // visual weight. Defaults to ~50% of `value`.
  from?: number;
  // Number-chip background and text color.
  bg?: string;
  color?: string;
}

export const StatTicker: React.FC<StatTickerProps> = ({
  label,
  value,
  startFrame,
  labelStart = 0,
  labelDuration = 18,
  duration = 45,
  format = "comma",
  from,
  bg = "#f1f5f9",
  color = "#0f172a",
}) => {
  const frame = useCurrentFrame();
  const startValue = from ?? Math.floor(value * 0.1);

  const opacity = fadeRamp({
    frame,
    start: labelStart,
    duration: labelDuration,
    from: 0,
    to: 1,
  });

  const number = numberTicker({
    frame,
    start: startFrame,
    from: startValue,
    to: value,
    duration,
    format: FORMATTERS[format],
  });

  return (
    <div
      style={{
        alignItems: "baseline",
        display: "flex",
        fontSize: fontSize.display,
        fontWeight: fontWeight.semibold,
        gap: "0.4em",
        letterSpacing: "-0.03em",
        lineHeight: 1.1,
        opacity,
        whiteSpace: "nowrap",
      }}
    >
      <span
        style={{
          backgroundColor: bg,
          borderRadius: "0.75rem",
          color,
          display: "inline-block",
          fontFeatureSettings: "'tnum'",
          fontVariantNumeric: "tabular-nums",
          paddingBlock: "0.1em",
          paddingInline: "0.5em",
        }}
      >
        {number}
      </span>
      <span>{label}</span>
    </div>
  );
};
