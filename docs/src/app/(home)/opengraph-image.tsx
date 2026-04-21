import { ImageResponse } from "@takumi-rs/image-response";

export const contentType = "image/png";
export const size = { width: 1200, height: 630 };

async function loadInter(weight: number): Promise<ArrayBuffer> {
  const res = await fetch(
    `https://cdn.jsdelivr.net/fontsource/fonts/inter@latest/latin-${weight}-normal.ttf`
  );
  if (!res.ok) {
    throw new Error(`Failed to load Inter ${weight}: ${res.status}`);
  }
  return res.arrayBuffer();
}

export default async function HomeOpengraphImage() {
  const [regular, semibold] = await Promise.all([loadInter(400), loadInter(600)]);

  return new ImageResponse(
    (
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          width: "100%",
          height: "100%",
          backgroundColor: "#ffffff",
          padding: "80px",
          fontFamily: "Inter",
          color: "#0a0a0a",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: "18px",
          }}
        >
          <svg
            fill="none"
            height="56"
            viewBox="0 0 256 256"
            width="56"
            xmlns="http://www.w3.org/2000/svg"
          >
            <circle
              cx="128"
              cy="128"
              r="24"
              stroke="#0a0a0a"
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth="16"
            />
            <circle
              cx="96"
              cy="56"
              r="24"
              stroke="#0a0a0a"
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth="16"
            />
            <circle
              cx="200"
              cy="104"
              r="24"
              stroke="#0a0a0a"
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth="16"
            />
            <circle
              cx="200"
              cy="184"
              r="24"
              stroke="#0a0a0a"
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth="16"
            />
            <circle
              cx="56"
              cy="192"
              r="24"
              stroke="#0a0a0a"
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth="16"
            />
            <line
              stroke="#0a0a0a"
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth="16"
              x1="118.25"
              x2="105.75"
              y1="106.07"
              y2="77.93"
            />
            <line
              stroke="#0a0a0a"
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth="16"
              x1="177.23"
              x2="150.77"
              y1="111.59"
              y2="120.41"
            />
            <line
              stroke="#0a0a0a"
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth="16"
              x1="181.06"
              x2="146.94"
              y1="169.27"
              y2="142.73"
            />
            <line
              stroke="#0a0a0a"
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth="16"
              x1="110.06"
              x2="73.94"
              y1="143.94"
              y2="176.06"
            />
          </svg>
          <span
            style={{
              fontSize: "44px",
              fontWeight: 400,
              letterSpacing: "-0.015em",
            }}
          >
            OptimusKG
          </span>
        </div>

        <div style={{ flex: 1 }} />

        <div
          style={{
            display: "flex",
            flexDirection: "column",
            fontSize: "72px",
            fontWeight: 600,
            lineHeight: 1.1,
            letterSpacing: "-0.02em",
          }}
        >
          <span>Unifying biomedical knowledge</span>
          <span>in a modern multimodal graph</span>
        </div>
      </div>
    ),
    {
      ...size,
      fonts: [
        { name: "Inter", data: regular, weight: 400, style: "normal" },
        { name: "Inter", data: semibold, weight: 600, style: "normal" },
      ],
    }
  );
}
