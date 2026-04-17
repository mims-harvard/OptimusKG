/**
 * Anatomy, Biological Process, Cellular Component, and Molecular Function
 * all share the same parquet schema. This file exports one component per node
 * type, all backed by the same field definition.
 */
import { SchemaTree, type SchemaField } from './schema-tree';

const fields = (idExample: string, labelAbbrev: string): SchemaField[] => [
  { name: 'id', type: 'String', description: `Node identifier in CURIE format (e.g. ${idExample})` },
  { name: 'label', type: 'String', description: `Node type abbreviation (${labelAbbrev})` },
  {
    name: 'properties',
    type: 'Struct',
    description: 'Entity-specific properties',
    children: [
      { name: 'name', type: 'String', description: 'Entity name' },
      { name: 'definition', type: 'String', description: 'Ontology definition' },
      { name: 'xrefs', type: 'List[String]', description: 'Cross-references to external databases' },
      { name: 'synonyms', type: 'List[String]', description: 'Synonym labels' },
      {
        name: 'ontology',
        type: 'Struct',
        description: 'Source ontology metadata',
        children: [
          { name: 'title', type: 'String', description: 'Ontology title' },
          { name: 'description', type: 'String', description: 'Ontology description' },
          { name: 'license', type: 'String', description: 'Ontology license' },
          { name: 'version', type: 'String', description: 'Ontology version' },
        ],
      },
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

export function AnatomySchema() {
  return <SchemaTree fields={fields('UBERON:0000948', 'ANA')} />;
}

export function BiologicalProcessSchema() {
  return <SchemaTree fields={fields('GO:0006915', 'BPO')} />;
}

export function CellularComponentSchema() {
  return <SchemaTree fields={fields('GO:0005737', 'CCO')} />;
}

export function MolecularFunctionSchema() {
  return <SchemaTree fields={fields('GO:0003677', 'MFN')} />;
}
