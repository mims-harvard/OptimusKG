/**
 * Shared building blocks for edge schema components.
 */
import type { SchemaField } from './schema-tree';

export const sourcesField: SchemaField = {
  name: 'sources',
  type: 'Struct',
  description: 'Provenance of this edge',
  children: [
    { name: 'direct', type: 'List[String]', description: 'Datasets that directly contributed this relationship' },
    { name: 'indirect', type: 'List[String]', description: 'Datasets that referenced this relationship' },
  ],
};

export function baseEdgeFields(
  label: string,
  relations: string | string[],
  undirected: boolean,
  propertyChildren: SchemaField[],
): SchemaField[] {
  const relationStr = Array.isArray(relations) ? relations.join(', ') : relations;
  return [
    { name: 'from', type: 'String', description: 'Source node ID (CURIE format)' },
    { name: 'to', type: 'String', description: 'Target node ID (CURIE format)' },
    { name: 'label', type: 'String', description: `Edge type label (${label})` },
    { name: 'relation', type: 'String', description: 'Relation type' },
    { name: 'undirected', type: 'Boolean', description: undirected ? 'True' : 'False' },
    {
      name: 'properties',
      type: 'Struct',
      description: 'Edge-specific properties',
      children: propertyChildren,
    },
  ];
}
