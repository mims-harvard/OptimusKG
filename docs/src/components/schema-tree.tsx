export type SchemaField = {
  name: string;
  type: string;
  description?: string;
  children?: SchemaField[];
};

function SchemaRow({
  field,
  isLast,
  ancestorLines,
}: {
  field: SchemaField;
  isLast: boolean;
  ancestorLines: boolean[]; // true = │ continuation, false = blank
}) {
  const prefix = ancestorLines.map((open) => (open ? "│   " : "    ")).join("");
  const connector = isLast ? "└── " : "├── ";
  const childAncestorLines = [...ancestorLines, !isLast];

  return (
    <>
      <tr className="border-fd-border/40 border-b last:border-0">
        <td className="px-4 py-1 align-top font-mono text-sm">
          {/* white-space: pre preserves the spaces in the prefix */}
          <span
            className="select-none text-fd-muted-foreground"
            style={{ whiteSpace: "pre" }}
          >
            {prefix}
            {connector}
          </span>
          <span className="font-medium text-fd-foreground">{field.name}</span>
        </td>
        <td className="whitespace-nowrap px-4 py-1 align-top font-mono text-fd-primary text-xs">
          {field.type}
        </td>
        <td className="px-4 py-1 align-top text-fd-muted-foreground text-xs">
          {field.description ?? ""}
        </td>
      </tr>
      {field.children?.map((child, i, arr) => (
        <SchemaRow
          ancestorLines={childAncestorLines}
          field={child}
          isLast={i === arr.length - 1}
          key={`${child.name}-${child.type}`}
        />
      ))}
    </>
  );
}

export function SchemaTree({ fields }: { fields: SchemaField[] }) {
  return (
    <div className="not-prose my-6 overflow-hidden rounded-[1px] border border-fd-border">
      <table className="w-full border-collapse bg-fd-card text-left">
        <thead>
          <tr className="border-fd-border border-b bg-fd-muted/30">
            <th className="w-64 px-4 py-2 font-semibold text-fd-muted-foreground text-xs">
              Column
            </th>
            <th className="w-36 px-4 py-2 font-semibold text-fd-muted-foreground text-xs">
              Data Type
            </th>
            <th className="px-4 py-2 font-semibold text-fd-muted-foreground text-xs">
              Description
            </th>
          </tr>
        </thead>
        <tbody>
          {fields.map((field, i, arr) => (
            <SchemaRow
              ancestorLines={[]}
              field={field}
              isLast={i === arr.length - 1}
              key={`${field.name}-${field.type}`}
            />
          ))}
        </tbody>
      </table>
    </div>
  );
}
