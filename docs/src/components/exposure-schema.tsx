import { SchemaTree, type SchemaField } from './schema-tree';

const fields: SchemaField[] = [
  { name: 'id', type: 'String', description: 'Exposure identifier (e.g. CTD:D001564)' },
  { name: 'label', type: 'String', description: 'Node type abbreviation (EXP)' },
  {
    name: 'properties',
    type: 'Struct',
    description: 'Exposure-specific properties',
    children: [
      { name: 'name', type: 'String', description: 'Exposure name' },
      { name: 'source_categories', type: 'List[String]', description: 'Exposure source categories (e.g. chemical, biological)' },
      { name: 'source_details', type: 'String', description: 'Additional source details' },
      {
        name: 'sources',
        type: 'Struct',
        description: 'Provenance of this node',
        children: [
          { name: 'direct', type: 'List[String]', description: 'Datasets that directly contributed this entity' },
          { name: 'indirect', type: 'List[String]', description: 'Datasets that referenced this entity' },
        ],
      },
    ],
  },
];

export function ExposureSchema() {
  return <SchemaTree fields={fields} />;
}
