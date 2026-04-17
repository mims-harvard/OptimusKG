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
  const prefix = ancestorLines.map(open => (open ? '│   ' : '    ')).join('');
  const connector = isLast ? '└── ' : '├── ';
  const childAncestorLines = [...ancestorLines, !isLast];

  return (
    <>
      <tr className="border-b border-fd-border/40 last:border-0">
        <td className="py-1 px-4 font-mono text-sm align-top">
          {/* white-space: pre preserves the spaces in the prefix */}
          <span className="select-none text-fd-muted-foreground" style={{ whiteSpace: 'pre' }}>
            {prefix}{connector}
          </span>
          <span className="font-medium text-fd-foreground">{field.name}</span>
        </td>
        <td className="py-1 px-4 font-mono text-xs text-fd-primary align-top whitespace-nowrap">
          {field.type}
        </td>
        <td className="py-1 px-4 text-xs text-fd-muted-foreground align-top">
          {field.description ?? ''}
        </td>
      </tr>
      {field.children?.map((child, i, arr) => (
        <SchemaRow
          key={i}
          field={child}
          isLast={i === arr.length - 1}
          ancestorLines={childAncestorLines}
        />
      ))}
    </>
  );
}

export function SchemaTree({ fields }: { fields: SchemaField[] }) {
  return (
    <div className="not-prose my-6 rounded-lg border border-fd-border overflow-hidden">
      <div className="flex items-center border-b border-fd-border bg-fd-muted/50 px-4 py-2">
        <div className="flex-1 border-t border-fd-border/60" />
        <span className="px-3 text-xs font-sans font-semibold text-fd-muted-foreground tracking-widest uppercase">
          Schema Tree
        </span>
        <div className="flex-1 border-t border-fd-border/60" />
      </div>
      <table className="w-full text-left border-collapse bg-fd-card">
        <thead>
          <tr className="border-b border-fd-border bg-fd-muted/30">
            <th className="py-2 px-4 text-xs font-semibold text-fd-muted-foreground w-64">Column</th>
            <th className="py-2 px-4 text-xs font-semibold text-fd-muted-foreground w-36">Data Type</th>
            <th className="py-2 px-4 text-xs font-semibold text-fd-muted-foreground">Description</th>
          </tr>
        </thead>
        <tbody>
          {fields.map((field, i, arr) => (
            <SchemaRow
              key={i}
              field={field}
              isLast={i === arr.length - 1}
              ancestorLines={[]}
            />
          ))}
        </tbody>
      </table>
    </div>
  );
}
