"use client";

import { type CSSProperties, useState } from "react";

import { cn } from "@/lib/cn";

import MacOSDock, { type DockApp } from "./components/mac-os-dock";
import { type EditorTab, TabbedEditor } from "./components/tabbed-editor";

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

  const isMaximized = sizeState === "maximized";
  const isHidden = visibility !== "visible";

  const reopen = () => {
    if (visibility === "closed") {
      setSessionId((id) => id + 1);
      setSizeState("normal");
    }
    setVisibility("visible");
  };

  const normalW = normalStyle.width;
  const normalH = normalStyle.height;
  const wrapperStyle: CSSProperties = isMaximized
    ? { top: 0, right: 0, bottom: 0, left: 0 }
    : {
        top: `calc((100% - (${normalH})) / 2)`,
        bottom: `calc((100% - (${normalH})) / 2)`,
        left: `calc((100% - (${normalW})) / 2)`,
        right: `calc((100% - (${normalW})) / 2)`,
      };

  return (
    <div className="absolute inset-0">
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
