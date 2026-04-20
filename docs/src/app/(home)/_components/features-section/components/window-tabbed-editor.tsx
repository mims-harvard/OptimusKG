"use client";

import { forwardRef } from "react";

import { useMaximizableWindow } from "./maximizable-window";
import {
  type EditorTab,
  TabbedEditor,
  type TabbedEditorHandle,
} from "./tabbed-editor";

export const WindowTabbedEditor = forwardRef<
  TabbedEditorHandle,
  {
    tabs: EditorTab[];
    contentBg?: string;
  }
>(function WindowTabbedEditor({ tabs, contentBg }, ref) {
  const window = useMaximizableWindow();
  return (
    <TabbedEditor
      contentBg={contentBg}
      onLastTabClose={window?.close}
      ref={ref}
      tabs={tabs}
    />
  );
});
