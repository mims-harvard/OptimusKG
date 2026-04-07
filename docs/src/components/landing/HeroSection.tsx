import { DownloadButton } from "./DownloadButton";

// ─── App window sub-components ───────────────────────────────────────────────

const TASK_ROWS = [
  { label: "Build Landing Page",        time: "now",  sub: "Done. Fonts preload in the head, c…", active: false },
  { label: "Analyze Tab vs Agent Usa…", time: "now",  sub: "All set! We now track focus share, …",  active: true  },
  { label: "Plan Mission Control",      time: "now",  sub: "+20 -3 · Drafted implementation st…",  active: false },
  { label: "PyTorch MNIST Experiments", time: "10m",  sub: "PyTorch MNIST Experiments",            active: false },
  { label: "Set up Cursor Rules for D…",time: "30m",  sub: "Set up Cursor Rules for Dashboard",    active: false },
  { label: "Bioinformatics Tools",      time: "45m",  sub: "+135 -21 · Bioinformatics Tools",       active: false },
];

function TaskSidebar() {
  return (
    <div
      className="flex flex-col h-full"
      style={{ width: "13.75rem", borderRight: "1px solid rgba(38,37,30,0.1)", background: "#f2f1ed" }}
    >
      {/* Section header */}
      <div className="px-[0.703rem] pt-[0.293rem] pb-[0.211rem] shrink-0">
        <p
          className="uppercase font-normal"
          style={{ fontSize: "0.644rem", lineHeight: "0.82rem", letterSpacing: "0.003rem", color: "rgba(38,37,30,0.6)" }}
        >
          Ready for Review <span style={{ color: "rgba(38,37,30,0.5)" }}>6</span>
        </p>
      </div>
      {/* Task rows */}
      <div className="flex flex-col">
        {TASK_ROWS.map((row) => (
          <div
            key={row.label}
            className="flex gap-[0.469rem] items-start py-[0.586rem] shrink-0"
            style={{
              paddingLeft: "0.828rem",
              paddingRight: "0.703rem",
              background: row.active ? "#ebeae5" : "transparent",
            }}
          >
            {/* Checkmark icon placeholder */}
            <div
              className="shrink-0 rounded-full border"
              style={{
                width: "0.9375rem",
                height: "0.9375rem",
                borderColor: "rgba(38,37,30,0.2)",
                marginTop: "0.1rem",
              }}
            />
            <div className="flex flex-col gap-px min-w-0 flex-1">
              <div className="flex justify-between items-start gap-px">
                <p
                  className="truncate font-normal"
                  style={{
                    fontSize: "0.663rem",
                    lineHeight: "0.9375rem",
                    color: row.active ? "#26251e" : "rgba(38,37,30,0.6)",
                  }}
                >
                  {row.label}
                </p>
                <p
                  className="shrink-0 font-normal"
                  style={{ fontSize: "0.594rem", lineHeight: "0.82rem", color: "rgba(38,37,30,0.4)", letterSpacing: "0.003rem" }}
                >
                  {row.time}
                </p>
              </div>
              <p
                className="truncate font-normal"
                style={{ fontSize: "0.613rem", lineHeight: "0.82rem", color: "rgba(38,37,30,0.4)", letterSpacing: "0.003rem" }}
              >
                {row.sub}
              </p>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

const CHAT_MESSAGES = [
  {
    role: "user",
    text: "Help me understand how teams split their focus between the tab view and the agents panel across our workspaces.",
  },
  {
    role: "assistant",
    thought: "7s",
    reviewed: "workspace usage exports and historical enga…",
    content:
      "I'll build an analytics toolkit that highlights tab versus agent adoption and switching behavior:",
    files: [{ name: "summary.py", added: "+150", removed: "-0" }],
  },
  {
    role: "assistant",
    content: "Next I'll normalize the interaction labels and add session tagging so product can filter specific cohorts:",
    files: [{ name: "segmentation.py", added: "+94", removed: "-0" }],
  },
  {
    role: "assistant",
    content: "Time to stitch everything together with a report helper that combines the key metrics:",
    files: [{ name: "report.py", added: "+40", removed: "-0" }],
  },
  {
    role: "assistant",
    content: "I'll add tests to lock in the behaviour using a representative sample from last week's export:",
    files: [{ name: "test_usage.py", added: "+90", removed: "" }],
  },
  {
    role: "assistant",
    content:
      "All set! We now track focus share, switching rates, and rolling engagement so PMs can compare tab-first and agent-first workflows in seconds.",
    files: [],
  },
];

function ChatPanel() {
  return (
    <div
      className="flex flex-col h-full"
      style={{ width: "21.25rem", borderRight: "1px solid rgba(38,37,30,0.1)", background: "#f2f1ed" }}
    >
      {/* Chat title bar */}
      <div
        className="flex items-center px-[0.703rem] shrink-0"
        style={{ height: "1.875rem", borderBottom: "1px solid rgba(38,37,30,0.1)" }}
      >
        <p className="font-normal truncate" style={{ fontSize: "0.65rem", lineHeight: "0.9375rem", color: "#26251e" }}>
          Analyze Tab vs Agent Usage Patterns
        </p>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-hidden px-[0.938rem] pt-[4rem] relative">
        <div className="flex flex-col gap-[0.5rem]">
          {/* User message */}
          <div
            className="rounded-[0.469rem] px-[0.531rem] py-[0.414rem]"
            style={{ background: "#f7f7f4", border: "1px solid rgba(38,37,30,0.1)" }}
          >
            <p className="font-normal" style={{ fontSize: "0.763rem", lineHeight: "1.083rem", color: "#26251e" }}>
              Help me understand how teams split their focus between the tab view and the agents panel across our workspaces.
            </p>
          </div>

          {/* Assistant messages */}
          {[
            { text: "I'll build an analytics toolkit that highlights tab versus agent adoption and switching behavior:", file: "summary.py +150 -0" },
            { text: "Next I'll normalize the interaction labels and add session tagging so product can filter specific cohorts:", file: "segmentation.py +94 -0" },
            { text: "Time to stitch everything together with a report helper that combines the key metrics:", file: "report.py +40 -0" },
          ].map((msg, i) => (
            <div key={i} className="flex flex-col gap-[0.281rem]">
              <p className="font-normal" style={{ fontSize: "0.75rem", lineHeight: "1.083rem", color: "#26251e" }}>
                {msg.text}
              </p>
              <div
                className="rounded-[0.469rem] px-[0.531rem] py-[0.469rem]"
                style={{ background: "#f7f7f4", border: "1px solid rgba(38,37,30,0.1)" }}
              >
                <p className="font-normal" style={{ fontSize: "0.694rem", lineHeight: "0.9375rem", color: "#26251e" }}>
                  {msg.file}
                </p>
              </div>
            </div>
          ))}

          <p className="font-normal" style={{ fontSize: "0.75rem", lineHeight: "1.083rem", color: "#26251e" }}>
            All set! We now track focus share, switching rates, and rolling engagement so PMs can compare tab-first and agent-first workflows in seconds.
          </p>
        </div>
      </div>

      {/* Input */}
      <div className="px-[0.703rem] pb-[0.703rem] shrink-0">
        <div
          className="overflow-hidden rounded-[0.469rem]"
          style={{ border: "1px solid rgba(38,37,30,0.1)" }}
        >
          <div className="px-[0.469rem] pt-[0.469rem] pb-[0.234rem]">
            <p className="font-normal" style={{ fontSize: "0.763rem", lineHeight: "1.083rem", color: "rgba(38,37,30,0.5)" }}>
              Plan, search, build anything...
            </p>
          </div>
          <div className="flex items-center justify-between px-[0.469rem] pb-[0.469rem]">
            <div
              className="flex items-center gap-[0.234rem] rounded-full px-[0.469rem] py-[0.176rem]"
              style={{ background: "#e6e5e0" }}
            >
              <p className="font-normal" style={{ fontSize: "0.594rem", lineHeight: "0.82rem", color: "rgba(38,37,30,0.6)", letterSpacing: "0.003rem" }}>
                Agent
              </p>
            </div>
            <div
              className="flex items-center justify-center rounded-full"
              style={{ width: "1.172rem", height: "1.172rem", background: "#e1e0db" }}
            />
          </div>
        </div>
      </div>
    </div>
  );
}

function CodePanel() {
  return (
    <div className="flex flex-col h-full flex-1 overflow-hidden" style={{ background: "#f7f7f4" }}>
      {/* Tab bar */}
      <div
        className="flex items-end shrink-0 overflow-hidden"
        style={{ height: "1.785rem", borderBottom: "1px solid rgba(38,37,30,0.1)", background: "#f2f1ed" }}
      >
        {/* Active tab */}
        <div
          className="flex items-center gap-[0.469rem] px-[0.703rem]"
          style={{
            height: "100%",
            background: "#f7f7f4",
            borderRight: "1px solid rgba(38,37,30,0.1)",
          }}
        >
          <p className="font-normal" style={{ fontSize: "0.65rem", lineHeight: "0.9375rem", color: "#26251e" }}>
            summary.py
          </p>
        </div>
        {/* Inactive tabs */}
        {["report.py", "test_usage.py"].map((tab) => (
          <div
            key={tab}
            className="flex items-center gap-[0.469rem] px-[0.703rem]"
            style={{
              height: "100%",
              background: "#f2f1ed",
              borderRight: "1px solid rgba(38,37,30,0.1)",
            }}
          >
            <p className="font-normal" style={{ fontSize: "0.638rem", lineHeight: "0.9375rem", color: "rgba(38,37,30,0.6)" }}>
              {tab}
            </p>
          </div>
        ))}
        <div
          className="flex-1 self-stretch"
          style={{ borderBottom: "1px solid rgba(38,37,30,0.1)" }}
        />
      </div>

      {/* Code content area */}
      <div className="flex-1 px-3 pt-3">
        <div className="flex flex-col gap-1">
          {[
            { tokens: [{ t: "def ", c: "#7c3aed" }, { t: "focus_share", c: "#1e40af" }, { t: "(", c: "#26251e" }, { t: "df", c: "#26251e" }, { t: "):", c: "#26251e" }] },
            { tokens: [{ t: "    ", c: "" }, { t: '"""', c: "#1f8a65" }, { t: "Compute per-user focus share.", c: "#1f8a65" }, { t: '"""', c: "#1f8a65" }] },
            { tokens: [{ t: "    tab_time ", c: "#26251e" }, { t: "=", c: "#7c3aed" }, { t: " df[", c: "#26251e" }, { t: '"tab"', c: "#1f8a65" }, { t: "].sum()", c: "#26251e" }] },
            { tokens: [{ t: "    agent_time ", c: "#26251e" }, { t: "=", c: "#7c3aed" }, { t: " df[", c: "#26251e" }, { t: '"agent"', c: "#1f8a65" }, { t: "].sum()", c: "#26251e" }] },
            { tokens: [{ t: "    total ", c: "#26251e" }, { t: "=", c: "#7c3aed" }, { t: " tab_time ", c: "#26251e" }, { t: "+", c: "#7c3aed" }, { t: " agent_time", c: "#26251e" }] },
            { tokens: [{ t: "    ", c: "" }, { t: "return", c: "#7c3aed" }, { t: " tab_time ", c: "#26251e" }, { t: "/", c: "#7c3aed" }, { t: " total", c: "#26251e" }] },
            { tokens: [] },
            { tokens: [{ t: "def ", c: "#7c3aed" }, { t: "switching_rate", c: "#1e40af" }, { t: "(", c: "#26251e" }, { t: "events", c: "#26251e" }, { t: "):", c: "#26251e" }] },
            { tokens: [{ t: "    ", c: "" }, { t: '"""', c: "#1f8a65" }, { t: "Count tab↔agent transitions.", c: "#1f8a65" }, { t: '"""', c: "#1f8a65" }] },
            { tokens: [{ t: "    transitions ", c: "#26251e" }, { t: "=", c: "#7c3aed" }, { t: " 0", c: "#b45309" }] },
          ].map((line, i) => (
            <div key={i} className="flex items-center" style={{ height: "0.875rem" }}>
              <span
                className="select-none mr-3 text-right shrink-0"
                style={{ fontSize: "0.625rem", lineHeight: "0.875rem", color: "rgba(38,37,30,0.3)", width: "1rem" }}
              >
                {i + 1}
              </span>
              <span className="font-mono" style={{ fontSize: "0.625rem", lineHeight: "0.875rem" }}>
                {line.tokens.map((tok, j) => (
                  <span key={j} style={{ color: tok.c || "transparent" }}>{tok.t}</span>
                ))}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function DemoWindow() {
  return (
    <div
      className="absolute left-[2rem] top-[2rem] md:top-[4.375rem] lg:left-[8.125rem] lg:top-[2rem] 2xl:hidden flex flex-col overflow-hidden rounded-[0.625rem] bg-[#f2f1ed]"
      style={{
        width: "65rem",
        height: "38.5rem",
        boxShadow:
          "0px 28px 70px rgba(0,0,0,0.14), 0px 14px 32px rgba(0,0,0,0.1), 0px 0px 0px 1px rgba(38,37,30,0.1)",
      }}
    >
      {/* Chrome bar */}
      <div
        className="flex items-center justify-between shrink-0 px-[0.469rem]"
        style={{ height: "1.75rem", borderBottom: "1px solid rgba(38,37,30,0.1)" }}
      >
        <div className="flex items-center" style={{ gap: "5.6px" }}>
          {[0, 1, 2].map((i) => (
            <div
              key={i}
              className="rounded-full bg-[rgba(38,37,30,0.2)]"
              style={{ width: "9.38px", height: "9.38px" }}
            />
          ))}
        </div>
        {/* Spacer */}
        <div style={{ width: "1.406rem" }} />
      </div>

      {/* Panels */}
      <div className="flex flex-1 overflow-hidden">
        <TaskSidebar />
        <ChatPanel />
        <CodePanel />
      </div>
    </div>
  );
}

// ─── Main export ──────────────────────────────────────────────────────────────

const MEDIA_BG =
  "linear-gradient(rgba(38,37,30,0.05),rgba(38,37,30,0.05)), linear-gradient(#f2f1ed,#f2f1ed)";

export function HeroSection() {
  return (
    <section
      className="bg-[#f7f7f4] pt-[6.498rem] pb-[3.9375rem] lg:pt-[6.953rem] lg:pb-[4.199rem]"
    >
      <div className="max-w-[81.25rem] mx-auto px-[1.172rem] lg:px-[1.25rem]">
        <div className="flex flex-col gap-[3.281rem] lg:gap-[3.5rem]">

          {/* ── Text + CTA ── */}
          <div className="flex flex-col gap-[1.3125rem] lg:gap-[1.399rem]">
            <h1
              className="text-[#26251e] font-normal lg:text-[1.55rem] lg:leading-[2.031rem] lg:tracking-[-0.020rem]"
              style={{ fontSize: "1.456rem", lineHeight: "1.904rem", letterSpacing: "-0.019rem" }}
            >
              <span className="block">Unifying biomedical knowledge</span>
              <span className="block">in a modern multimodal graph</span>
            </h1>

            {/* Desktop/tablet CTA */}
            <DownloadButton
              className="hidden md:inline-flex self-start items-center bg-[#26251e] text-[#f7f7f4] font-normal rounded-full"
              style={{
                fontSize: "0.894rem",
                lineHeight: "1rem",
                paddingBlock: "0.864rem",
                paddingInline: "1.4125rem",
              }}
            />

            {/* Mobile-only CTA */}
            <DownloadButton
              className="md:hidden inline-flex self-start items-center bg-[#26251e] text-[#f7f7f4] font-normal rounded-full"
              style={{
                fontSize: "0.894rem",
                lineHeight: "0.9375rem",
                paddingBlock: "0.915rem",
                paddingInline: "1.422rem",
              }}
            />
          </div>

          {/* ── Media ── */}
          <div
            className="relative h-[42.5rem] md:h-[48.75rem] lg:h-[42.5rem] 2xl:h-[48.75rem] overflow-hidden rounded-[0.25rem]"
            style={{ backgroundImage: MEDIA_BG }}
          >
            {/* Landscape background */}
            <img
              src="/hero-bg.webp"
              alt=""
              className="absolute left-0 right-0 w-full object-cover pointer-events-none"
              style={{ height: "111.11%", top: "-5.56%" }}
            />

            {/* Demo window */}
            <DemoWindow />

            {/* Border overlay */}
            <div className="absolute inset-0 rounded-[0.25rem] border border-[rgba(38,37,30,0.03)] pointer-events-none" />
          </div>

        </div>
      </div>
    </section>
  );
}
