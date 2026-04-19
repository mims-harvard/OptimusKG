import type { Metadata } from "next";

import { RootProvider } from "fumadocs-ui/provider/next";
import "./global.css";

import { Inter } from "next/font/google";

import { Analytics } from "@vercel/analytics/next";

export const metadata: Metadata = {
  title: {
    default: "OptimusKG",
    template: "OptimusKG · %s",
  },
};

const inter = Inter({
  subsets: ["latin"],
});

export default function Layout({ children }: LayoutProps<"/">) {
  return (
    <html className={inter.className} lang="en" suppressHydrationWarning>
      <body className="flex min-h-screen flex-col">
        <RootProvider>{children}</RootProvider>
        <Analytics />
      </body>
    </html>
  );
}
