"use client";

import {
  type CSSProperties,
  type ReactNode,
  createContext,
  useContext,
  useEffect,
  useState,
} from "react";

import { cn } from "@/lib/cn";

import { EditorWindow } from "./components/editor-window";
import MacOSDock, { type DockApp } from "./components/mac-os-dock";

type SizeState = "normal" | "maximized";
type VisibilityState = "visible" | "minimized" | "closed";

const DEFAULT_DOCK_APPS: DockApp[] = [
  { id: "finder", name: "Finder", icon: "/dock/finder.png" },
  { id: "calculator", name: "Calculator", icon: "/dock/calculator.png" },
  { id: "terminal", name: "Terminal", icon: "/dock/terminal.png" },
  { id: "mail", name: "Mail", icon: "/dock/mail.png" },
  { id: "notes", name: "Notes", icon: "/dock/notes.png" },
  { id: "safari", name: "Safari", icon: "/dock/safari.png" },
  { id: "photos", name: "Photos", icon: "/dock/photos.png" },
  { id: "music", name: "Music", icon: "/dock/music.png" },
  { id: "calendar", name: "Calendar", icon: "/dock/calendar.png" },
];

const MOBILE_HIDDEN_APP_IDS = new Set([
  "calculator",
  "terminal",
  "photos",
  "music",
]);

export type WindowControls = {
  close: () => void;
  minimize: () => void;
  toggleMaximize: () => void;
  isMaximized: boolean;
};

const WindowContext = createContext<WindowControls | null>(null);

export function useMaximizableWindow(): WindowControls | null {
  return useContext(WindowContext);
}

type Props = {
  title?: string;
  normalStyle: CSSProperties;
  appId: string;
  appName: string;
  appIcon: string;
  children: ReactNode;
};

export function MaximizableWindow({
  title,
  children,
  normalStyle,
  appId,
  appName,
  appIcon,
}: Props) {
  const [sizeState, setSizeState] = useState<SizeState>("normal");
  const [visibility, setVisibility] = useState<VisibilityState>("visible");
  const [sessionId, setSessionId] = useState(0);
  const [isMobile, setIsMobile] = useState(false);

  useEffect(() => {
    const mediaQuery = window.matchMedia("(max-width: 767px)");
    const update = () => setIsMobile(mediaQuery.matches);
    update();
    mediaQuery.addEventListener("change", update);
    return () => mediaQuery.removeEventListener("change", update);
  }, []);

  const dockApps: DockApp[] = [
    ...(isMobile
      ? DEFAULT_DOCK_APPS.filter((app) => !MOBILE_HIDDEN_APP_IDS.has(app.id))
      : DEFAULT_DOCK_APPS),
    { id: appId, name: appName, icon: appIcon },
  ];

  const isMaximized = sizeState === "maximized";
  const isHidden = visibility !== "visible";

  const close = () => setVisibility("closed");
  const minimize = () => {
    if (!isMaximized) {
      setVisibility("minimized");
    }
  };
  const toggleMaximize = () =>
    setSizeState((s) => (s === "maximized" ? "normal" : "maximized"));

  const reopen = () => {
    if (visibility === "closed") {
      setSessionId((id) => id + 1);
      setSizeState("normal");
    }
    setVisibility("visible");
  };

  const controls: WindowControls = {
    close,
    minimize,
    toggleMaximize,
    isMaximized,
  };

  return (
    <WindowContext.Provider value={controls}>
      <div className="absolute inset-0">
        <div className="pointer-events-none absolute inset-0 flex items-center justify-center">
          <div
            aria-hidden={isHidden}
            className={cn(
              "pointer-events-auto origin-bottom transition-all duration-300 ease-out",
              isHidden &&
                "pointer-events-none translate-y-[40%] scale-[0.6] opacity-0",
              isMaximized && "h-full w-full"
            )}
            key={sessionId}
            style={isMaximized ? undefined : normalStyle}
          >
            <EditorWindow
              canMinimize={!isMaximized}
              chromeOverlay={isMaximized}
              className={cn(
                "h-full w-full transition-[border-radius] duration-300 ease-out",
                isMaximized && "rounded-none"
              )}
              isMaximized={isMaximized}
              onClose={close}
              onMaximize={toggleMaximize}
              onMinimize={minimize}
              title={title}
            >
              {children}
            </EditorWindow>
          </div>
        </div>

        <div
          aria-hidden={!isHidden}
          className={cn(
            "absolute inset-x-0 bottom-4 z-20 flex justify-center transition-opacity duration-300",
            isHidden ? "opacity-100" : "pointer-events-none opacity-0"
          )}
        >
          <MacOSDock
            apps={dockApps}
            onAppClick={(clickedId) => {
              if (clickedId === appId) {
                reopen();
              }
            }}
            openApps={visibility === "minimized" ? [appId] : []}
          />
        </div>
      </div>
    </WindowContext.Provider>
  );
}
