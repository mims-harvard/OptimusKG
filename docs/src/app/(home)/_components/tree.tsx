"use client";

import type { ItemInstance } from "@headless-tree/core";
import { ChevronDownIcon } from "lucide-react";
import { Slot } from "radix-ui";
import * as React from "react";

import { cn } from "@/lib/cn";

// biome-ignore lint/suspicious/noExplicitAny: upstream originui signature
type TreeContextValue<T = any> = {
  indent: number;
  currentItem?: ItemInstance<T>;
  // biome-ignore lint/suspicious/noExplicitAny: upstream originui signature
  tree?: any;
};

const TreeContext = React.createContext<TreeContextValue>({
  indent: 20,
  currentItem: undefined,
  tree: undefined,
});

// biome-ignore lint/suspicious/noExplicitAny: upstream originui signature
function useTreeContext<T = any>() {
  return React.useContext(TreeContext) as TreeContextValue<T>;
}

type TreeProps = React.HTMLAttributes<HTMLDivElement> & {
  indent?: number;
  // biome-ignore lint/suspicious/noExplicitAny: upstream originui signature
  tree?: any;
};

function Tree({ indent = 20, tree, className, ...props }: TreeProps) {
  const containerProps =
    tree && typeof tree.getContainerProps === "function"
      ? tree.getContainerProps()
      : {};
  const mergedProps = { ...props, ...containerProps };

  const { style: propStyle, ...otherProps } = mergedProps;

  const mergedStyle = {
    ...propStyle,
    "--tree-indent": `${indent}px`,
  } as React.CSSProperties;

  return (
    <TreeContext.Provider value={{ indent, tree }}>
      <div
        data-slot="tree"
        style={mergedStyle}
        className={cn("flex flex-col", className)}
        {...otherProps}
      />
    </TreeContext.Provider>
  );
}

// biome-ignore lint/suspicious/noExplicitAny: upstream originui signature
type TreeItemProps<T = any> = React.HTMLAttributes<HTMLButtonElement> & {
  item: ItemInstance<T>;
  indent?: number;
  asChild?: boolean;
};

// biome-ignore lint/suspicious/noExplicitAny: upstream originui signature
function TreeItem<T = any>({
  item,
  className,
  asChild,
  children,
  ...props
}: Omit<TreeItemProps<T>, "indent">) {
  const { indent } = useTreeContext<T>();

  const itemProps = typeof item.getProps === "function" ? item.getProps() : {};
  const mergedProps = { ...props, ...itemProps };

  const { style: propStyle, ...otherProps } = mergedProps;

  const mergedStyle = {
    ...propStyle,
    "--tree-padding": `${item.getItemMeta().level * indent}px`,
  } as React.CSSProperties;

  const Comp = asChild ? Slot.Root : "button";

  return (
    <TreeContext.Provider value={{ indent, currentItem: item }}>
      <Comp
        data-slot="tree-item"
        style={mergedStyle}
        className={cn(
          "z-10 block w-full cursor-pointer text-left outline-hidden select-none focus:z-20 data-[disabled]:pointer-events-none data-[disabled]:opacity-50",
          className
        )}
        data-focus={
          typeof item.isFocused === "function"
            ? item.isFocused() || false
            : undefined
        }
        data-folder={
          typeof item.isFolder === "function"
            ? item.isFolder() || false
            : undefined
        }
        data-selected={
          typeof item.isSelected === "function"
            ? item.isSelected() || false
            : undefined
        }
        data-drag-target={
          typeof item.isDragTarget === "function"
            ? item.isDragTarget() || false
            : undefined
        }
        data-search-match={
          typeof item.isMatchingSearch === "function"
            ? item.isMatchingSearch() || false
            : undefined
        }
        aria-expanded={item.isExpanded()}
        {...otherProps}
      >
        {children}
      </Comp>
    </TreeContext.Provider>
  );
}

// biome-ignore lint/suspicious/noExplicitAny: upstream originui signature
type TreeItemLabelProps<T = any> = React.HTMLAttributes<HTMLSpanElement> & {
  item?: ItemInstance<T>;
};

// biome-ignore lint/suspicious/noExplicitAny: upstream originui signature
function TreeItemLabel<T = any>({
  item: propItem,
  children,
  className,
  ...props
}: TreeItemLabelProps<T>) {
  const { currentItem } = useTreeContext<T>();
  const item = propItem || currentItem;

  if (!item) {
    console.warn("TreeItemLabel: No item provided via props or context");
    return null;
  }

  return (
    <span
      data-slot="tree-item-label"
      className={cn(
        "flex items-center gap-1 ps-(--tree-padding) py-[3px] pe-2 text-[13px] leading-[18px] transition-colors",
        "text-fd-foreground/85",
        "hover:bg-[color-mix(in_srgb,var(--color-fd-foreground)_6%,transparent)]",
        "in-data-[selected=true]:bg-[color-mix(in_srgb,var(--color-fd-foreground)_10%,transparent)]",
        "in-data-[selected=true]:text-fd-foreground",
        "in-focus-visible:ring-fd-ring/50 in-focus-visible:ring-inset in-focus-visible:ring-1",
        "not-in-data-[folder=true]:ps-[calc(var(--tree-padding)+1rem)]",
        "[&_svg]:pointer-events-none [&_svg]:shrink-0",
        className
      )}
      {...props}
    >
      {item.isFolder() && (
        <ChevronDownIcon
          className="size-3.5 text-fd-muted-foreground transition-transform duration-150 in-aria-[expanded=false]:-rotate-90"
          strokeWidth={2}
        />
      )}
      {children ||
        (typeof item.getItemName === "function" ? item.getItemName() : null)}
    </span>
  );
}

function TreeDragLine({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) {
  const { tree } = useTreeContext();

  if (!tree || typeof tree.getDragLineStyle !== "function") {
    console.warn(
      "TreeDragLine: No tree provided via context or tree does not have getDragLineStyle method"
    );
    return null;
  }

  const dragLine = tree.getDragLineStyle();
  return (
    <div
      style={dragLine}
      className={cn(
        "bg-primary before:bg-background before:border-primary absolute z-30 -mt-px h-0.5 w-[unset] before:absolute before:-top-[3px] before:left-0 before:size-2 before:rounded-full before:border-2",
        className
      )}
      {...props}
    />
  );
}

export { Tree, TreeItem, TreeItemLabel, TreeDragLine };
