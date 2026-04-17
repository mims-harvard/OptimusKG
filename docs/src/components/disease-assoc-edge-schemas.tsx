/**
 * Disease/Phenotype → Gene association edges (DIS-GEN, PHE-GEN).
 * Both share the same DisGeNET-derived property schema.
 */
import { SchemaTree } from './schema-tree';
import { baseEdgeFields, sourcesField } from './edge-schema-shared';

const assocProperties = [
  { name: 'evidence_score', type: 'Float64', description: 'Aggregated association evidence score' },
  { name: 'evidence_count', type: 'Int64', description: 'Number of evidence items supporting the association' },
  { name: 'evidence_index', type: 'Float64', description: 'Combined evidence index (Open Targets)' },
  { name: 'disease_specificity_index', type: 'Float64', description: 'DSI — specificity of the gene to this disease' },
  { name: 'disease_pleiotropy_index', type: 'Float64', description: 'DPI — number of disease classes the gene is associated with' },
  { name: 'disgenet_score', type: 'Float64', description: 'DisGeNET gene–disease association score' },
  { name: 'year_initial', type: 'String', description: 'Year of the earliest supporting publication' },
  { name: 'year_final', type: 'String', description: 'Year of the most recent supporting publication' },
  { name: 'number_of_pmids', type: 'Int16', description: 'Number of supporting PubMed publications' },
  { name: 'number_of_snps', type: 'Int16', description: 'Number of supporting SNPs (GWAS evidence)' },
  sourcesField,
];

export function DisGenEdge() {
  return <SchemaTree fields={baseEdgeFields('DIS-GEN', 'ASSOCIATED_WITH', true, assocProperties)} />;
}
export function PheGenEdge() {
  return <SchemaTree fields={baseEdgeFields('PHE-GEN', 'ASSOCIATED_WITH', false, assocProperties)} />;
}
