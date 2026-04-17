import { SchemaTree, type SchemaField } from './schema-tree';

const nodeFields: SchemaField[] = [
  { name: 'id',         type: 'String',  description: 'Globally unique node identifier in CURIE format (e.g. ENSG00000139618)' },
  { name: 'label',      type: 'String',  description: 'Node type 3-letter abbreviation (e.g. GEN, DRG)' },
  { name: 'properties', type: 'String',  description: 'JSON-encoded type-specific properties. Expanded to a native Struct in per-type parquet files.' },
];

const edgeFields: SchemaField[] = [
  { name: 'from',       type: 'String',  description: 'Source node identifier in CURIE format' },
  { name: 'to',         type: 'String',  description: 'Target node identifier in CURIE format' },
  { name: 'label',      type: 'String',  description: 'Edge type label (e.g. DIS-GEN)' },
  { name: 'relation',   type: 'String',  description: 'Relation type (e.g. ASSOCIATED_WITH)' },
  { name: 'undirected', type: 'Boolean', description: 'True if the edge has no intrinsic directionality' },
  { name: 'properties', type: 'String',  description: 'JSON-encoded edge-specific properties. Expanded to a native Struct in per-type parquet files.' },
];

export function BaseNodeSchema() {
  return <SchemaTree fields={nodeFields} />;
}

export function BaseEdgeSchema() {
  return <SchemaTree fields={edgeFields} />;
}
