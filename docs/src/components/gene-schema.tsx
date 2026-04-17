import { SchemaTree, type SchemaField } from './schema-tree';

const fields: SchemaField[] = [
  { name: 'id', type: 'String', description: 'Node identifier in CURIE format (e.g. ENSG00000141510)' },
  { name: 'label', type: 'String', description: 'Node type abbreviation (GEN)' },
  {
    name: 'properties',
    type: 'Struct',
    description: 'Gene-specific properties',
    children: [
      { name: 'symbol', type: 'String', description: 'Official HGNC gene symbol (e.g. TP53)' },
      { name: 'name', type: 'String', description: 'Full gene name' },
      { name: 'biotype', type: 'String', description: 'Gene biotype (e.g. protein_coding, lncRNA)' },
      {
        name: 'genomic_location',
        type: 'Struct',
        description: 'Chromosomal coordinates',
        children: [
          { name: 'chromosome', type: 'String', description: 'Chromosome name' },
          { name: 'start', type: 'Int64', description: 'Start position (0-based)' },
          { name: 'end', type: 'Int64', description: 'End position' },
          { name: 'strand', type: 'Int32', description: 'Strand (+1 forward, -1 reverse)' },
        ],
      },
      { name: 'transcription_start_site', type: 'Int64', description: 'Transcription start site position' },
      {
        name: 'canonical_transcript',
        type: 'Struct',
        description: 'Canonical transcript details',
        children: [
          { name: 'id', type: 'String', description: 'Ensembl transcript ID' },
          { name: 'chromosome', type: 'String', description: 'Chromosome name' },
          { name: 'start', type: 'Int64', description: 'Start position' },
          { name: 'end', type: 'Int64', description: 'End position' },
          { name: 'strand', type: 'String', description: 'Strand' },
        ],
      },
      { name: 'canonical_exons', type: 'List[String]', description: 'Canonical exon coordinates' },
      { name: 'transcript_ids', type: 'List[String]', description: 'All associated Ensembl transcript IDs' },
      { name: 'alternative_genes', type: 'List[String]', description: 'Alternative gene entries at the same locus' },
      { name: 'function_descriptions', type: 'List[String]', description: 'Functional descriptions' },
      {
        name: 'synonyms',
        type: 'List[Struct]',
        description: 'General gene synonyms',
        children: [
          { name: 'label', type: 'String', description: 'Synonym label' },
          { name: 'source', type: 'String', description: 'Source database' },
        ],
      },
      {
        name: 'symbol_synonyms',
        type: 'List[Struct]',
        description: 'Alternative gene symbols',
        children: [
          { name: 'label', type: 'String', description: 'Symbol label' },
          { name: 'source', type: 'String', description: 'Source database' },
        ],
      },
      {
        name: 'name_synonyms',
        type: 'List[Struct]',
        description: 'Alternative gene names',
        children: [
          { name: 'label', type: 'String', description: 'Name label' },
          { name: 'source', type: 'String', description: 'Source database' },
        ],
      },
      {
        name: 'obsolete_symbols',
        type: 'List[Struct]',
        description: 'Deprecated gene symbols',
        children: [
          { name: 'label', type: 'String', description: 'Symbol label' },
          { name: 'source', type: 'String', description: 'Source database' },
        ],
      },
      {
        name: 'obsolete_names',
        type: 'List[Struct]',
        description: 'Deprecated gene names',
        children: [
          { name: 'label', type: 'String', description: 'Name label' },
          { name: 'source', type: 'String', description: 'Source database' },
        ],
      },
      {
        name: 'subcellular_locations',
        type: 'List[Struct]',
        description: 'Subcellular localization annotations',
        children: [
          { name: 'location', type: 'String', description: 'Location name' },
          { name: 'source', type: 'String', description: 'Source database' },
          { name: 'term_sl', type: 'String', description: 'Subcellular location ontology term' },
          { name: 'label_sl', type: 'String', description: 'Subcellular location label' },
        ],
      },
      {
        name: 'target_class',
        type: 'List[Struct]',
        description: 'Drug target class classification',
        children: [
          { name: 'id', type: 'Int64', description: 'Target class ID' },
          { name: 'label', type: 'String', description: 'Target class label' },
          { name: 'level', type: 'String', description: 'Hierarchy level' },
        ],
      },
      {
        name: 'target_enabling_package',
        type: 'Struct',
        description: 'Target enabling package annotation',
        children: [
          { name: 'target_from_source_id', type: 'String', description: 'Source target ID' },
          { name: 'description', type: 'String', description: 'Package description' },
          { name: 'therapeutic_area', type: 'String', description: 'Therapeutic area' },
          { name: 'url', type: 'String', description: 'Reference URL' },
        ],
      },
      {
        name: 'tractability',
        type: 'List[Struct]',
        description: 'Drug tractability assessments per modality',
        children: [
          { name: 'modality', type: 'String', description: 'Drug modality (e.g. sm, ab, pr)' },
          { name: 'id', type: 'String', description: 'Tractability category ID' },
          { name: 'value', type: 'Boolean', description: 'Tractability assessment value' },
        ],
      },
      {
        name: 'constraint_scores',
        type: 'List[Struct]',
        description: 'Evolutionary constraint scores (e.g. pLI, LOEUF)',
        children: [
          { name: 'constraint_type', type: 'String', description: 'Score type (e.g. lof, mis)' },
          { name: 'score', type: 'Float32', description: 'Constraint score' },
          { name: 'exp', type: 'Float32', description: 'Expected variant count' },
          { name: 'obs', type: 'Int32', description: 'Observed variant count' },
          { name: 'oe', type: 'Float32', description: 'Observed/expected ratio' },
          { name: 'oe_lower', type: 'Float32', description: 'O/E 90% CI lower bound' },
          { name: 'oe_upper', type: 'Float32', description: 'O/E 90% CI upper bound' },
          { name: 'upper_rank', type: 'Int32', description: 'Upper rank (gnomAD)' },
          { name: 'upper_bin', type: 'Int32', description: 'Upper bin (10-bin)' },
          { name: 'upper_bin6', type: 'Int32', description: 'Upper bin (6-bin)' },
        ],
      },
      {
        name: 'hallmarks_attributes',
        type: 'List[Struct]',
        description: 'Cancer hallmark attributes (Cancer Gene Census)',
        children: [
          { name: 'pmid', type: 'Int64', description: 'PubMed ID of supporting reference' },
          { name: 'description', type: 'String', description: 'Hallmark description' },
          { name: 'attribute_name', type: 'String', description: 'Attribute name' },
        ],
      },
      {
        name: 'cancer_hallmarks',
        type: 'List[Struct]',
        description: 'Associated cancer hallmarks',
        children: [
          { name: 'pmid', type: 'Int64', description: 'PubMed ID of supporting reference' },
          { name: 'description', type: 'String', description: 'Hallmark description' },
          { name: 'impact', type: 'String', description: 'Functional impact (promotes/suppresses)' },
          { name: 'label', type: 'String', description: 'Hallmark label' },
        ],
      },
      {
        name: 'associated_proteins',
        type: 'List[Struct]',
        description: 'Associated UniProt protein entries',
        children: [
          { name: 'id', type: 'String', description: 'UniProt accession' },
          { name: 'source', type: 'String', description: 'Source database' },
        ],
      },
      {
        name: 'xrefs',
        type: 'List[Struct]',
        description: 'Cross-references to external databases',
        children: [
          { name: 'id', type: 'String', description: 'External identifier' },
          { name: 'source', type: 'String', description: 'Database name' },
        ],
      },
      {
        name: 'chemical_probes',
        type: 'List[Struct]',
        description: 'Chemical probe annotations (Probes & Drugs)',
      },
      {
        name: 'homologues',
        type: 'List[Struct]',
        description: 'Ortholog and paralog information',
        children: [
          { name: 'species_id', type: 'String', description: 'NCBI taxonomy ID' },
          { name: 'species_name', type: 'String', description: 'Species name' },
          { name: 'homology_type', type: 'String', description: 'Homology type (ortholog/paralog)' },
          { name: 'target_gene_id', type: 'String', description: 'Target gene identifier' },
          { name: 'is_high_confidence', type: 'String', description: 'High-confidence flag' },
          { name: 'target_gene_symbol', type: 'String', description: 'Target gene symbol' },
          { name: 'query_percentage_identity', type: 'Float64', description: 'Query % sequence identity' },
          { name: 'target_percentage_identity', type: 'Float64', description: 'Target % sequence identity' },
          { name: 'priority', type: 'Int32', description: 'Priority rank' },
        ],
      },
      {
        name: 'safety_liabilities',
        type: 'List[Struct]',
        description: 'Safety liability annotations (OpenTargets)',
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

export function GeneSchema() {
  return <SchemaTree fields={fields} />;
}
