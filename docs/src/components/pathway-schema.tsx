import { SchemaTree, type SchemaField } from './schema-tree';

const fields: SchemaField[] = [
  { name: 'id', type: 'String', description: 'Reactome pathway ID (e.g. R-HSA-109582)' },
  { name: 'label', type: 'String', description: 'Node type abbreviation (PWY)' },
  {
    name: 'properties',
    type: 'Struct',
    description: 'Pathway-specific properties',
    children: [
      { name: 'name', type: 'String', description: 'Pathway name' },
      { name: 'species', type: 'String', description: 'Species name (e.g. Homo sapiens)' },
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

export function PathwaySchema() {
  return <SchemaTree fields={fields} />;
}
