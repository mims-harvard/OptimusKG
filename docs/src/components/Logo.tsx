export function Logo() {
  return (
    <span className="flex items-center gap-1">
      <svg
        width="22"
        height="22"
        viewBox="0 0 256 256"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
        aria-hidden="true"
      >
        <circle cx="128" cy="128" r="24" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="16" />
        <circle cx="96" cy="56" r="24" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="16" />
        <circle cx="200" cy="104" r="24" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="16" />
        <circle cx="200" cy="184" r="24" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="16" />
        <circle cx="56" cy="192" r="24" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="16" />
        <line x1="118.25" y1="106.07" x2="105.75" y2="77.93" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="16" />
        <line x1="177.23" y1="111.59" x2="150.77" y2="120.41" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="16" />
        <line x1="181.06" y1="169.27" x2="146.94" y2="142.73" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="16" />
        <line x1="110.06" y1="143.94" x2="73.94" y2="176.06" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="16" />
      </svg>
      <span
        className="font-normal whitespace-nowrap"
        style={{ fontSize: "1rem", lineHeight: "1", letterSpacing: "-0.015em" }}
      >
        OptimusKG
      </span>
    </span>
  );
}
