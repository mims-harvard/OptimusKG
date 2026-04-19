export type SchemaField = {
  name: string;
  type: string;
  description?: string;
  children?: SchemaField[];
};
