"use client";

import { type CSSProperties, useEffect, useRef, useState } from "react";

import { cn } from "@/lib/cn";

import MacOSDock, { type DockApp } from "./MacOSDock";
import { type EditorTab, TabbedEditor } from "./TabbedEditor";

type SizeState = "normal" | "maximized";
type VisibilityState = "visible" | "minimized" | "closed";

const DEFAULT_DOCK_APPS: DockApp[] = [
  {
    id: "finder",
    name: "Finder",
    icon: "https://cdn.jim-nielsen.com/macos/1024/finder-2021-09-10.png?rf=1024",
  },
  {
    id: "calculator",
    name: "Calculator",
    icon: "https://cdn.jim-nielsen.com/macos/1024/calculator-2021-04-29.png?rf=1024",
  },
  {
    id: "terminal",
    name: "Terminal",
    icon: "https://cdn.jim-nielsen.com/macos/1024/terminal-2021-06-03.png?rf=1024",
  },
  {
    id: "mail",
    name: "Mail",
    icon: "https://cdn.jim-nielsen.com/macos/1024/mail-2021-05-25.png?rf=1024",
  },
  {
    id: "notes",
    name: "Notes",
    icon: "https://cdn.jim-nielsen.com/macos/1024/notes-2021-05-25.png?rf=1024",
  },
  {
    id: "safari",
    name: "Safari",
    icon: "https://cdn.jim-nielsen.com/macos/1024/safari-2021-06-02.png?rf=1024",
  },
  {
    id: "photos",
    name: "Photos",
    icon: "https://cdn.jim-nielsen.com/macos/1024/photos-2021-05-28.png?rf=1024",
  },
  {
    id: "music",
    name: "Music",
    icon: "https://cdn.jim-nielsen.com/macos/1024/music-2021-05-25.png?rf=1024",
  },
  {
    id: "calendar",
    name: "Calendar",
    icon: "https://cdn.jim-nielsen.com/macos/1024/calendar-2021-04-29.png?rf=1024",
  },
];

export function HeroWindowShell({
  title,
  tabs,
  contentBg,
  normalStyle,
  appId,
  appName,
  appIcon,
}: {
  title?: string;
  tabs: EditorTab[];
  contentBg?: string;
  normalStyle: CSSProperties;
  appId: string;
  appName: string;
  appIcon: string;
}) {
  const [sizeState, setSizeState] = useState<SizeState>("normal");
  const [visibility, setVisibility] = useState<VisibilityState>("visible");
  const [sessionId, setSessionId] = useState(0);
  const [parentSize, setParentSize] = useState<{ w: number; h: number } | null>(
    null
  );
  const [windowSize, setWindowSize] = useState<{ w: number; h: number } | null>(
    null
  );
  const frameRef = useRef<HTMLDivElement>(null);
  const sizerRef = useRef<HTMLDivElement>(null);

  const isMaximized = sizeState === "maximized";
  const isHidden = visibility !== "visible";

  useEffect(() => {
    if (!(frameRef.current && sizerRef.current)) return;
    const frameEl = frameRef.current;
    const sizerEl = sizerRef.current;
    const ro = new ResizeObserver(() => {
      setParentSize({ w: frameEl.clientWidth, h: frameEl.clientHeight });
      setWindowSize({ w: sizerEl.offsetWidth, h: sizerEl.offsetHeight });
    });
    ro.observe(frameEl);
    ro.observe(sizerEl);
    setParentSize({ w: frameEl.clientWidth, h: frameEl.clientHeight });
    setWindowSize({ w: sizerEl.offsetWidth, h: sizerEl.offsetHeight });
    return () => ro.disconnect();
  }, []);

  const reopen = () => {
    if (visibility === "closed") {
      setSessionId((id) => id + 1);
      setSizeState("normal");
    }
    setVisibility("visible");
  };

  let wrapperStyle: CSSProperties;
  if (parentSize && windowSize && !isMaximized) {
    const offsetX = Math.max(0, (parentSize.w - windowSize.w) / 2);
    const offsetY = Math.max(0, (parentSize.h - windowSize.h) / 2);
    wrapperStyle = {
      top: offsetY,
      left: offsetX,
      width: windowSize.w,
      height: windowSize.h,
    };
  } else if (parentSize && isMaximized) {
    wrapperStyle = {
      top: 0,
      left: 0,
      width: parentSize.w,
      height: parentSize.h,
    };
  } else {
    // First render (no measurements yet): fall back to normalStyle via flex-centered wrapper.
    wrapperStyle = { top: "50%", left: "50%", translate: "-50% -50%" };
  }

  return (
    <div className="absolute inset-0" ref={frameRef}>
      {/* Hidden sizer — measures what the window would be at normal size */}
      <div
        aria-hidden="true"
        className="invisible absolute"
        ref={sizerRef}
        style={normalStyle}
      />

      <div
        aria-hidden={isHidden}
        className={cn(
          "absolute origin-bottom transition-all duration-300 ease-out",
          isHidden &&
            "pointer-events-none translate-y-[40%] scale-[0.6] opacity-0"
        )}
        style={wrapperStyle}
      >
        <TabbedEditor
          chromeOverlay={isMaximized}
          className={cn(
            "h-full w-full transition-[border-radius] duration-300 ease-out",
            isMaximized && "rounded-none"
          )}
          contentBg={contentBg}
          isMaximized={isMaximized}
          key={sessionId}
          onClose={() => setVisibility("closed")}
          onMaximize={() =>
            setSizeState((s) => (s === "maximized" ? "normal" : "maximized"))
          }
          onMinimize={() => setVisibility("minimized")}
          tabs={tabs}
          title={title}
        />
      </div>

      <div
        aria-hidden={!isHidden}
        className={cn(
          "absolute inset-x-0 bottom-4 z-20 flex justify-center transition-opacity duration-300",
          isHidden ? "opacity-100" : "pointer-events-none opacity-0"
        )}
      >
        <MacOSDock
          apps={[{ id: appId, name: appName, icon: appIcon }, ...DEFAULT_DOCK_APPS]}
          onAppClick={(clickedId) => {
            if (clickedId === appId) {
              reopen();
            }
          }}
          openApps={visibility === "minimized" ? [appId] : []}
        />
      </div>
    </div>
  );
}
