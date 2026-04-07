"use client";

import { useEffect, useState } from "react";

type OS = "Windows" | "macOS" | "Linux" | "iOS" | "Android" | null;

function detectOS(): OS {
  const ua = navigator.userAgent;
  if (/android/i.test(ua)) return "Android";
  if (/iphone|ipad|ipod/i.test(ua)) return "iOS";
  if (/win/i.test(ua)) return "Windows";
  if (/mac/i.test(ua)) return "macOS";
  if (/linux/i.test(ua)) return "Linux";
  return null;
}

export function DownloadButton({ className, style }: { className?: string; style?: React.CSSProperties }) {
  const [os, setOS] = useState<OS>(null);

  useEffect(() => {
    setOS(detectOS());
  }, []);

  const label = os ? `Download for ${os} ⤓` : "Download ⤓";

  return (
    <a
      href="#"
      className={className}
      style={style}
    >
      {label}
    </a>
  );
}
