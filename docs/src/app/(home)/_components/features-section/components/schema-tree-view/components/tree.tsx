"use client";

import {
  type HTMLAttributes,
  type ReactNode,
  createContext,
  useCallback,
  useContext,
  useId,
  useState,
} from "react";

import { ChevronRight, File, Folder, FolderOpen } from "lucide-react";

import { cn } from "@/lib/cn";

type TreeContextType = {
  expandedIds: Set<string>;
  selectedIds: string[];
  toggleExpanded: (nodeId: string) => void;
  handleSelection: (nodeId: string, ctrlKey: boolean) => void;
  showLines?: boolean;
  showIcons?: boolean;
  selectable?: boolean;
  multiSelect?: boolean;
  indent?: number;
};

const TreeContext = createContext<TreeContextType | undefined>(undefined);

function useTree() {
  const ctx = useContext(TreeContext);
  if (!ctx) {
    throw new Error("Tree components must be used within a TreeProvider");
  }
  return ctx;
}

type TreeNodeContextType = {
  nodeId: string;
  level: number;
  isLast: boolean;
  parentPath: boolean[];
};

const TreeNodeContext = createContext<TreeNodeContextType | undefined>(undefined);

function useTreeNode() {
  const ctx = useContext(TreeNodeContext);
  if (!ctx) {
    throw new Error("TreeNode components must be used within a TreeNode");
  }
  return ctx;
}

export type TreeProviderProps = {
  children: ReactNode;
  defaultExpandedIds?: string[];
  showLines?: boolean;
  showIcons?: boolean;
  selectable?: boolean;
  multiSelect?: boolean;
  selectedIds?: string[];
  onSelectionChange?: (selectedIds: string[]) => void;
  indent?: number;
  className?: string;
};

export function TreeProvider({
  children,
  defaultExpandedIds = [],
  showLines = true,
  showIcons = true,
  selectable = true,
  multiSelect = false,
  selectedIds,
  onSelectionChange,
  indent = 20,
  className,
}: TreeProviderProps) {
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set(defaultExpandedIds));
  const [internalSelected, setInternalSelected] = useState<string[]>(selectedIds ?? []);

  const isControlled = selectedIds !== undefined && onSelectionChange !== undefined;
  const current = isControlled ? selectedIds : internalSelected;

  const toggleExpanded = useCallback((nodeId: string) => {
    setExpandedIds((prev) => {
      const next = new Set(prev);
      if (next.has(nodeId)) {
        next.delete(nodeId);
      } else {
        next.add(nodeId);
      }
      return next;
    });
  }, []);

  const handleSelection = useCallback(
    (nodeId: string, ctrlKey = false) => {
      if (!selectable) {
        return;
      }
      let next: string[];
      if (multiSelect && ctrlKey) {
        next = current.includes(nodeId)
          ? current.filter((id) => id !== nodeId)
          : [...current, nodeId];
      } else {
        next = current.includes(nodeId) ? [] : [nodeId];
      }
      if (isControlled) {
        onSelectionChange?.(next);
      } else {
        setInternalSelected(next);
      }
    },
    [selectable, multiSelect, current, isControlled, onSelectionChange],
  );

  return (
    <TreeContext.Provider
      value={{
        expandedIds,
        selectedIds: current,
        toggleExpanded,
        handleSelection,
        showLines,
        showIcons,
        selectable,
        multiSelect,
        indent,
      }}
    >
      <div className={cn("w-full", className)}>{children}</div>
    </TreeContext.Provider>
  );
}

export type TreeViewProps = HTMLAttributes<HTMLDivElement>;

export function TreeView({ className, children, ...props }: TreeViewProps) {
  return (
    <div className={cn("p-2", className)} {...props}>
      {children}
    </div>
  );
}

export type TreeNodeProps = HTMLAttributes<HTMLDivElement> & {
  nodeId?: string;
  level?: number;
  isLast?: boolean;
  parentPath?: boolean[];
};

export function TreeNode({
  nodeId: providedId,
  level = 0,
  isLast = false,
  parentPath = [],
  children,
  className,
  ...props
}: TreeNodeProps) {
  const generatedId = useId();
  const nodeId = providedId ?? generatedId;

  const currentPath = level === 0 ? [] : [...parentPath];
  if (level > 0 && parentPath.length < level - 1) {
    while (currentPath.length < level - 1) {
      currentPath.push(false);
    }
  }
  if (level > 0) {
    currentPath[level - 1] = isLast;
  }

  return (
    <TreeNodeContext.Provider value={{ nodeId, level, isLast, parentPath: currentPath }}>
      <div className={cn("select-none", className)} {...props}>
        {children}
      </div>
    </TreeNodeContext.Provider>
  );
}

export type TreeNodeTriggerProps = HTMLAttributes<HTMLDivElement> & {
  hasChildren?: boolean;
};

export function TreeNodeTrigger({
  children,
  className,
  hasChildren = false,
  onClick,
  ...props
}: TreeNodeTriggerProps) {
  const { selectedIds, toggleExpanded, handleSelection, indent, expandedIds, showLines } =
    useTree();
  const { nodeId, level } = useTreeNode();
  const isSelected = selectedIds.includes(nodeId);
  const isExpanded = expandedIds.has(nodeId);

  return (
    <div
      className={cn(
        "group relative flex cursor-pointer items-center rounded-md px-4 py-1.5 transition-colors duration-200",
        "hover:bg-[var(--l-bg)]",
        isSelected && "bg-[var(--l-bg)]",
        className,
      )}
      onClick={(e) => {
        toggleExpanded(nodeId);
        handleSelection(nodeId, e.ctrlKey || e.metaKey);
        onClick?.(e);
      }}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          toggleExpanded(nodeId);
          handleSelection(nodeId, e.ctrlKey || e.metaKey);
        }
      }}
      role="button"
      style={{ paddingLeft: level * (indent ?? 0) + 8 }}
      tabIndex={0}
      {...props}
    >
      <TreeLines />
      {children}
    </div>
  );
}

export function TreeLines() {
  const { showLines, indent } = useTree();
  const { level, isLast, parentPath } = useTreeNode();

  if (!showLines || level === 0) {
    return null;
  }

  return (
    <div className="pointer-events-none absolute top-0 bottom-0 left-1.5">
      {Array.from(
        { length: level },
        (_, index) =>
          !(index === level - 1 && isLast) && (
            <div
              className="absolute border-[var(--l-border)] border-l"
              data-parent-path={JSON.stringify(parentPath)}
              key={`tree-line-${index}`}
              style={{
                left: index * (indent ?? 0) + 12,
                top: 0,
                bottom: "-1px",
              }}
            />
          ),
      )}

      <div
        className="absolute top-1/2 border-[var(--l-border)] border-t"
        style={{
          left: (level - 1) * (indent ?? 0) + 12,
          width: (indent ?? 0) - 12,
          transform: "translateY(-1px)",
        }}
      />

      {isLast && (
        <div
          className="absolute border-[var(--l-border)] border-l"
          style={{
            left: (level - 1) * (indent ?? 0) + 12,
            top: 0,
            height: "calc(50%)",
          }}
        />
      )}
    </div>
  );
}

export type TreeNodeContentProps = HTMLAttributes<HTMLDivElement> & {
  hasChildren?: boolean;
};

export function TreeNodeContent({
  children,
  hasChildren = false,
  className,
  ...props
}: TreeNodeContentProps) {
  const { expandedIds } = useTree();
  const { nodeId } = useTreeNode();
  const isExpanded = expandedIds.has(nodeId);

  if (!(hasChildren && isExpanded)) {
    return null;
  }

  return (
    <div className={className} {...props}>
      {children}
    </div>
  );
}

export type TreeExpanderProps = HTMLAttributes<HTMLDivElement> & {
  hasChildren?: boolean;
};

export function TreeExpander({
  hasChildren = false,
  className,
  onClick,
  ...props
}: TreeExpanderProps) {
  const { expandedIds, toggleExpanded } = useTree();
  const { nodeId } = useTreeNode();
  const isExpanded = expandedIds.has(nodeId);

  if (!hasChildren) {
    return <div className="mr-1 h-4 w-4" />;
  }

  return (
    <div
      className={cn(
        "mx-0.5 flex h-4 w-4 cursor-pointer items-center justify-center transition-transform duration-200",
        isExpanded && "rotate-90",
        className,
      )}
      onClick={(e) => {
        e.stopPropagation();
        toggleExpanded(nodeId);
        onClick?.(e);
      }}
      {...props}
    >
      <ChevronRight className="h-3 w-3 text-[var(--l-ink-muted)]" />
    </div>
  );
}

export type TreeIconProps = HTMLAttributes<HTMLDivElement> & {
  icon?: ReactNode;
  hasChildren?: boolean;
};

export function TreeIcon({ icon, hasChildren = false, className, ...props }: TreeIconProps) {
  const { showIcons, expandedIds } = useTree();
  const { nodeId } = useTreeNode();
  const isExpanded = expandedIds.has(nodeId);

  if (!showIcons) {
    return null;
  }

  let defaultIcon: ReactNode;
  if (hasChildren) {
    defaultIcon = isExpanded ? <FolderOpen className="h-4 w-4" /> : <Folder className="h-4 w-4" />;
  } else {
    defaultIcon = <File className="h-4 w-4" />;
  }

  return (
    <div
      className={cn(
        "mr-2 flex h-4 w-4 items-center justify-center text-[var(--l-ink-muted)]",
        className,
      )}
      {...props}
    >
      {icon ?? defaultIcon}
    </div>
  );
}

export type TreeLabelProps = HTMLAttributes<HTMLSpanElement>;

export function TreeLabel({ className, ...props }: TreeLabelProps) {
  return <span className={cn("flex-1 truncate text-sm", className)} {...props} />;
}
