"use client";

import {
  TreeExpander,
  TreeNode,
  TreeNodeContent,
  TreeNodeTrigger,
  TreeProvider,
  TreeView,
} from "./components/tree";

export type SchemaField = {
  name: string;
  type: string;
  description?: string;
  children?: SchemaField[];
};

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
    <TreeNode
      isLast={isLast}
      level={level}
      nodeId={nodeId}
      parentPath={parentPath}
    >
      <TreeNodeTrigger hasChildren={hasChildren}>
        <TreeExpander hasChildren={hasChildren} />
        <div className="flex flex-1 items-center gap-2 whitespace-nowrap">
          <span className="shrink-0 font-mono text-fd-foreground text-sm">
            {field.name}
          </span>
          <span className="shrink-0 rounded-[1px] border border-fd-border bg-fd-muted/30 px-1.5 py-px font-mono text-fd-muted-foreground text-xs">
            {field.type}
          </span>
          {field.description && (
            <span className="text-fd-muted-foreground text-xs">
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
    <div className="overflow-x-auto">
      <TreeProvider
        defaultExpandedIds={defaultExpandedIds}
        indent={24}
        selectable={false}
        showIcons={false}
        showLines
      >
        <TreeView className="w-fit min-w-full p-2 pb-8">
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
    </div>
  );
}
