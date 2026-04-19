"use client";

import type { SchemaField } from "@/components/schema-tree";
import {
  TreeExpander,
  TreeNode,
  TreeNodeContent,
  TreeNodeTrigger,
  TreeProvider,
  TreeView,
} from "./components/tree";

function collectStructIds(fields: SchemaField[], prefix = ""): string[] {
  const ids: string[] = [];
  fields.forEach((field, i) => {
    const id = `${prefix}${i}`;
    if (field.children?.length) {
      ids.push(id);
      ids.push(...collectStructIds(field.children, `${id}.`));
    }
  });
  return ids;
}

function SchemaRow({
  field,
  index,
  total,
  parentPath,
  level,
  idPrefix,
}: {
  field: SchemaField;
  index: number;
  total: number;
  parentPath: boolean[];
  level: number;
  idPrefix: string;
}) {
  const hasChildren = Boolean(field.children?.length);
  const isLast = index === total - 1;
  const nodeId = `${idPrefix}${index}`;

  return (
    <TreeNode isLast={isLast} level={level} nodeId={nodeId} parentPath={parentPath}>
      <TreeNodeTrigger hasChildren={hasChildren}>
        <TreeExpander hasChildren={hasChildren} />
        <div className="flex min-w-0 flex-1 items-center gap-2">
          <span className="shrink-0 truncate font-mono text-sm text-[var(--l-ink)]">
            {field.name}
          </span>
          <span className="shrink-0 rounded border border-[var(--l-border)] bg-[var(--l-bg)] px-1.5 py-px font-mono text-xs text-[var(--l-ink-muted)]">
            {field.type}
          </span>
          {field.description && (
            <span className="truncate text-xs text-[var(--l-ink-muted)]">
              {field.description}
            </span>
          )}
        </div>
      </TreeNodeTrigger>
      {hasChildren && (
        <TreeNodeContent hasChildren>
          {field.children?.map((child, i, arr) => (
            <SchemaRow
              field={child}
              idPrefix={`${nodeId}.`}
              index={i}
              key={`${nodeId}.${child.name}`}
              level={level + 1}
              parentPath={[...parentPath, isLast]}
              total={arr.length}
            />
          ))}
        </TreeNodeContent>
      )}
    </TreeNode>
  );
}

export function SchemaTreeView({
  fields,
  defaultExpanded = true,
}: {
  fields: SchemaField[];
  defaultExpanded?: boolean;
}) {
  const defaultExpandedIds = defaultExpanded ? collectStructIds(fields) : [];

  return (
    <TreeProvider
      defaultExpandedIds={defaultExpandedIds}
      indent={16}
      selectable={false}
      showIcons={false}
      showLines
    >
      <TreeView className="p-2">
        {fields.map((field, i, arr) => (
          <SchemaRow
            field={field}
            idPrefix=""
            index={i}
            key={field.name}
            level={0}
            parentPath={[]}
            total={arr.length}
          />
        ))}
      </TreeView>
    </TreeProvider>
  );
}
