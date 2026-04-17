/**
 * GO-annotation edge types: BPO-GEN, CCO-GEN, MFN-GEN.
 * All three share the same property schema (GO evidence, gene product, ECO IDs).
 */
import { SchemaTree } from './schema-tree';
import { baseEdgeFields, sourcesField } from './edge-schema-shared';

const goAnnotationProperties = [
  { name: 'evidence', type: 'List[String]', description: 'GO evidence codes (e.g. IDA, IMP, TAS)' },
  { name: 'gene_product', type: 'List[String]', description: 'Gene product IDs annotated to this term' },
  { name: 'eco_ids', type: 'List[String]', description: 'Evidence & Conclusion Ontology (ECO) IDs' },
  sourcesField,
];

export function BpoGenEdge() {
  return <SchemaTree fields={baseEdgeFields('BPO-GEN', 'INTERACTS_WITH', true, goAnnotationProperties)} />;
}
export function CcoGenEdge() {
  return <SchemaTree fields={baseEdgeFields('CCO-GEN', 'INTERACTS_WITH', true, goAnnotationProperties)} />;
}
export function MfnGenEdge() {
  return <SchemaTree fields={baseEdgeFields('MFN-GEN', 'INTERACTS_WITH', true, goAnnotationProperties)} />;
}
