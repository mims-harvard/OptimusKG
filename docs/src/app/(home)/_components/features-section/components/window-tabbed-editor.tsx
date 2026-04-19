"use client";

import { useMaximizableWindow } from "./maximizable-window";
import { type EditorTab, TabbedEditor } from "./tabbed-editor";

export function WindowTabbedEditor({
  tabs,
  contentBg,
}: {
  tabs: EditorTab[];
  contentBg?: string;
}) {
  const window = useMaximizableWindow();
  return (
    <TabbedEditor
      contentBg={contentBg}
      onLastTabClose={window?.close}
      tabs={tabs}
    />
  );
}
