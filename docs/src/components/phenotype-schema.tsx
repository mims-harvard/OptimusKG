import { SchemaTree, type SchemaField } from './schema-tree';

const ontologyField: SchemaField = {
  name: 'ontology',
  type: 'Struct',
  description: 'Source ontology metadata',
  children: [
    { name: 'title', type: 'String', description: 'Ontology title' },
    { name: 'description', type: 'String', description: 'Ontology description' },
    { name: 'license', type: 'String', description: 'Ontology license' },
    { name: 'version', type: 'String', description: 'Ontology version' },
  ],
};

const sourcesField: SchemaField = {
  name: 'sources',
  type: 'Struct',
  description: 'Provenance of this node',
  children: [
    { name: 'direct', type: 'List[String]', description: 'Datasets that directly contributed this entity' },
    { name: 'indirect', type: 'List[String]', description: 'Datasets that referenced this entity' },
  ],
};

const fields: SchemaField[] = [
  { name: 'id', type: 'String', description: 'Node identifier in CURIE format (e.g. HP:0001250)' },
  { name: 'label', type: 'String', description: 'Node type abbreviation (PHE)' },
  {
    name: 'properties',
    type: 'Struct',
    description: 'Phenotype-specific properties',
    children: [
      { name: 'name', type: 'String', description: 'Phenotype name' },
      { name: 'description', type: 'String', description: 'Phenotype description' },
      { name: 'code', type: 'String', description: 'Primary ontology code' },
      { name: 'type', type: 'String', description: 'Phenotype type classification' },
      { name: 'xrefs', type: 'List[String]', description: 'Cross-references to external databases' },
      { name: 'parents', type: 'List[String]', description: 'Parent phenotype terms in the ontology hierarchy' },
      { name: 'children', type: 'List[String]', description: 'Child phenotype terms in the ontology hierarchy' },
      { name: 'ancestors', type: 'List[String]', description: 'All ancestor terms (transitive parents)' },
      { name: 'descendants', type: 'List[String]', description: 'All descendant terms (transitive children)' },
      { name: 'exact_synonyms', type: 'List[String]', description: 'Exact synonym labels' },
      { name: 'related_synonyms', type: 'List[String]', description: 'Related synonym labels' },
      { name: 'narrow_synonyms', type: 'List[String]', description: 'Narrow synonym labels' },
      { name: 'broad_synonyms', type: 'List[String]', description: 'Broad synonym labels' },
      { name: 'obsolete_terms', type: 'List[String]', description: 'Deprecated ontology terms' },
      { name: 'obsolete_xrefs', type: 'List[String]', description: 'Deprecated cross-references' },
      { name: 'concept_ids', type: 'List[String]', description: 'UMLS concept IDs' },
      { name: 'concept_names', type: 'List[String]', description: 'UMLS concept names' },
      { name: 'umls_cui', type: 'String', description: 'Primary UMLS Concept Unique Identifier' },
      { name: 'snomed_full_names', type: 'List[String]', description: 'SNOMED CT full concept names' },
      { name: 'snomed_concept_ids', type: 'List[String]', description: 'SNOMED CT concept IDs' },
      { name: 'cui_semantic_type', type: 'String', description: 'UMLS semantic type of the primary CUI' },
      ontologyField,
      sourcesField,
    ],
  },
];

export function PhenotypeSchema() {
  return <SchemaTree fields={fields} />;
}
