"use client";

import { useRef } from "react";

import {
  hotkeysCoreFeature,
  selectionFeature,
  syncDataLoaderFeature,
} from "@headless-tree/core";
import { useTree } from "@headless-tree/react";

import { Tree, TreeItem, TreeItemLabel } from "@/app/(home)/_components/tree";

import {
  type EditorTab,
  type TabbedEditorHandle,
} from "../../../tabbed-editor";
import { WindowTabbedEditor } from "../../../window-tabbed-editor";
import { ThemedSvgTabContent } from "./components/themed-svg-tab-content";

type Validation = {
  id: string;
  fileName: string;
  tabName: string;
  lightSrc: string;
  darkSrc: string;
  alt: string;
};

export const VALIDATIONS: Validation[] = (
  [
    { id: "anatomy", label: "Anatomy" },
    { id: "biological-process", label: "Biological Process" },
    { id: "cellular-component", label: "Cellular Component" },
    { id: "disease", label: "Disease" },
    { id: "drug", label: "Drug" },
    { id: "exposure", label: "Exposure" },
    { id: "gene", label: "Gene" },
    { id: "molecular-function", label: "Molecular Function" },
    { id: "pathway", label: "Pathway" },
    { id: "phenotype", label: "Phenotype" },
  ] as const
).map(({ id, label }) => ({
  id,
  fileName: `${id}.svg`,
  tabName: `${label} Validation`,
  lightSrc: `/features/${id}-light.svg`,
  darkSrc: `/features/${id}-dark.svg`,
  alt: `${label} validation bar chart`,
}));

const DEFAULT_OPEN_IDS = ["molecular-function", "phenotype"];

type TreeNode = {
  name: string;
  children?: string[];
};

const TREE_DATA: Record<string, TreeNode> = {
  root: { name: "validations", children: ["validations-folder"] },
  "validations-folder": {
    name: "validations",
    children: VALIDATIONS.map((v) => v.id),
  },
  ...Object.fromEntries(
    VALIDATIONS.map((v) => [v.id, { name: v.fileName } satisfies TreeNode])
  ),
};

function validationToTab(v: Validation): EditorTab {
  return {
    name: v.tabName,
    content: (
      <ThemedSvgTabContent
        alt={v.alt}
        darkSrc={v.darkSrc}
        lightSrc={v.lightSrc}
      />
    ),
  };
}

export function ValidationsEditor() {
  const editorRef = useRef<TabbedEditorHandle>(null);

  function openValidation(id: string) {
    const validation = VALIDATIONS.find((v) => v.id === id);
    if (!validation) {
      return;
    }
    editorRef.current?.openTab(validationToTab(validation));
  }

  const tree = useTree<TreeNode>({
    initialState: {
      expandedItems: ["validations-folder"],
    },
    rootItemId: "root",
    getItemName: (item) => item.getItemData().name,
    isItemFolder: (item) => Boolean(item.getItemData().children),
    onPrimaryAction: (item) => {
      if (!item.isFolder()) {
        openValidation(item.getId());
      }
    },
    dataLoader: {
      getItem: (id) => TREE_DATA[id],
      getChildren: (id) => TREE_DATA[id]?.children ?? [],
    },
    indent: 14,
    features: [syncDataLoaderFeature, selectionFeature, hotkeysCoreFeature],
  });

  const defaultTabs = VALIDATIONS.filter((v) =>
    DEFAULT_OPEN_IDS.includes(v.id)
  ).map(validationToTab);

  return (
    <div className="flex h-full w-full">
      <aside className="flex w-48 shrink-0 flex-col border-fd-border border-r bg-fd-card">
        <div className="min-h-0 flex-1 overflow-y-auto p-1.5">
          <Tree indent={14} tree={tree}>
            {tree.getItems().map((item) => (
              <TreeItem item={item} key={item.getId()}>
                <TreeItemLabel className="px-2 py-0.5 text-xs">
                  {item.getItemName()}
                </TreeItemLabel>
              </TreeItem>
            ))}
          </Tree>
        </div>
      </aside>
      <div className="min-w-0 flex-1">
        <WindowTabbedEditor ref={editorRef} tabs={defaultTabs} />
      </div>
    </div>
  );
}
