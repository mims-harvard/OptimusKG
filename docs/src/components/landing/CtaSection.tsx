export function CtaSection() {
  return (
    <section
      id="download"
      className="bg-[#f7f7f4] py-[5.25rem] pb-[8.4rem] border-t border-[rgba(38,37,30,0.08)]"
    >
      <div className="max-w-[810px] mx-auto px-6 flex flex-col items-center gap-[1.4rem] text-center">
        <h2 className="text-[4.1875rem] leading-[1.18] tracking-[-0.032em] font-normal text-[#26251e] whitespace-nowrap">
          Try Cursor now.
        </h2>
        <a
          href="https://api2.cursor.sh/updates/download/golden/win32-x64-user/cursor/3.0"
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1.5 bg-[#26251e] text-[#f7f7f4] text-[0.95625rem] px-[22.6px] pt-[13.48px] pb-[13.8px] rounded-full hover:opacity-80 transition-opacity"
        >
          Download for Windows ↓
        </a>
      </div>
    </section>
  );
}
