import {
  type SchemaField,
  SchemaTreeView,
} from "@/components/schema-tree-view";

export function DocsSchemaTree({
  fields,
  defaultExpanded,
}: {
  fields: SchemaField[];
  defaultExpanded?: boolean;
}) {
  return (
    <div className="not-prose my-6 rounded-[1px] border border-fd-border">
      <SchemaTreeView defaultExpanded={defaultExpanded} fields={fields} />
    </div>
  );
}
