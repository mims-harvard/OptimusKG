export const Logo: React.FC<{ size?: number }> = ({ size = 22 }) => (
  <span className="flex items-center gap-1">
    <svg
      aria-hidden="true"
      fill="none"
      height={size}
      viewBox="0 0 256 256"
      width={size}
      xmlns="http://www.w3.org/2000/svg"
    >
      <g
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="16"
      >
        <circle cx="128" cy="128" r="24" />
        <circle cx="96" cy="56" r="24" />
        <circle cx="200" cy="104" r="24" />
        <circle cx="200" cy="184" r="24" />
        <circle cx="56" cy="192" r="24" />
        <line x1="118.25" x2="105.75" y1="106.07" y2="77.93" />
        <line x1="177.23" x2="150.77" y1="111.59" y2="120.41" />
        <line x1="181.06" x2="146.94" y1="169.27" y2="142.73" />
        <line x1="110.06" x2="73.94" y1="143.94" y2="176.06" />
      </g>
    </svg>
    <span
      className="whitespace-nowrap font-normal"
      style={{ fontSize: size * 0.8, lineHeight: "1", letterSpacing: "-0.015em" }}
    >
      OptimusKG
    </span>
  </span>
);
