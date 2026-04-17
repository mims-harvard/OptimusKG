export type SchemaField = {
  name: string;
  type: string;
  description?: string;
  children?: SchemaField[];
};

function SchemaRow({ field, depth }: { field: SchemaField; depth: number }) {
  const treePrefix = '|─'.repeat(depth) + '|─';

  return (
    <>
      <tr className="border-b border-fd-border/50 last:border-0">
        <td className="py-1.5 px-4 font-mono text-sm whitespace-nowrap">
          <span className="select-none text-fd-muted-foreground">{treePrefix}</span>
          <span className="font-medium"> {field.name}</span>
        </td>
        <td className="py-1.5 px-4 font-mono text-sm text-fd-primary whitespace-nowrap">
          {field.type}
        </td>
        <td className="py-1.5 px-4 text-sm text-fd-muted-foreground">
          {field.description ?? ''}
        </td>
      </tr>
      {field.children?.map((child, i) => (
        <SchemaRow key={i} field={child} depth={depth + 1} />
      ))}
    </>
  );
}

export function SchemaTree({ fields }: { fields: SchemaField[] }) {
  return (
    <div className="not-prose rounded-lg border border-fd-border overflow-x-auto my-6">
      <table className="w-full text-left border-collapse">
        <thead>
          <tr className="border-b border-fd-border bg-fd-muted/50">
            <th className="py-2.5 px-4 text-sm font-semibold w-72">Column</th>
            <th className="py-2.5 px-4 text-sm font-semibold w-36">Data Type</th>
            <th className="py-2.5 px-4 text-sm font-semibold">Description</th>
          </tr>
        </thead>
        <tbody>
          {fields.map((field, i) => (
            <SchemaRow key={i} field={field} depth={0} />
          ))}
        </tbody>
      </table>
    </div>
  );
}
