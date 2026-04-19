"use client";

import { type CSSProperties, useState } from "react";

import { cn } from "@/lib/cn";

import MacOSDock, { type DockApp } from "./MacOSDock";
import { type EditorTab, TabbedEditor } from "./TabbedEditor";

type WindowState = "normal" | "maximized" | "minimized" | "closed";

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
  const [state, setState] = useState<WindowState>("normal");
  const [sessionId, setSessionId] = useState(0);
  const isHidden = state === "minimized" || state === "closed";

  const reopen = () => {
    if (state === "closed") {
      setSessionId((id) => id + 1);
    }
    setState("normal");
  };

  return (
    <div className="absolute inset-0 flex items-center justify-center">
      <div
        aria-hidden={isHidden}
        className={cn(
          "origin-bottom transition-[opacity,scale,translate] duration-300 ease-out",
          state === "maximized" && "absolute inset-0",
          isHidden &&
            "pointer-events-none translate-y-[40%] scale-[0.6] opacity-0"
        )}
        style={state === "maximized" ? undefined : normalStyle}
      >
        <TabbedEditor
          className={cn(
            "h-full w-full",
            state === "maximized" && "rounded-none"
          )}
          contentBg={contentBg}
          key={sessionId}
          onClose={() => setState("closed")}
          onMaximize={() =>
            setState((s) => (s === "maximized" ? "normal" : "maximized"))
          }
          onMinimize={() => setState("minimized")}
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
          openApps={state === "minimized" ? [appId] : []}
        />
      </div>
    </div>
  );
}
