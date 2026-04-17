/**
 * Remaining edge types: ANA-GEN, DIS-PHE, and all EXP-* edges.
 */
import { SchemaTree } from './schema-tree';
import { baseEdgeFields, sourcesField } from './edge-schema-shared';

// ANA-GEN — Bgee gene expression
export function AnaGenEdge() {
  return (
    <SchemaTree
      fields={baseEdgeFields(
        'ANA-GEN',
        ['EXPRESSION_PRESENT', 'EXPRESSION_ABSENT'],
        true,
        [
          { name: 'expression_rank', type: 'Int32', description: 'Bgee expression rank score (lower = higher expression)' },
          { name: 'call_quality', type: 'String', description: 'Expression call quality (gold/silver)' },
          sourcesField,
        ],
      )}
    />
  );
}

// DIS-PHE — HPO disease–phenotype annotations
export function DisPheEdge() {
  return (
    <SchemaTree
      fields={baseEdgeFields(
        'DIS-PHE',
        'PHENOTYPE_PRESENT',
        true,
        [
          { name: 'aspect', type: 'List[String]', description: 'HPO annotation aspect (P=phenotypic, I=inheritance, etc.)' },
          { name: 'evidence_type', type: 'List[String]', description: 'Evidence type codes (e.g. IEA, PCS, TAS)' },
          { name: 'frequency', type: 'List[String]', description: 'Phenotype frequency annotations' },
          { name: 'onset', type: 'List[String]', description: 'Age of onset annotations' },
          { name: 'modifiers', type: 'List[String]', description: 'Clinical modifier annotations' },
          { name: 'sexes', type: 'List[String]', description: 'Sex-specific annotations' },
          { name: 'qualifier_not', type: 'Boolean', description: 'True if phenotype is explicitly absent' },
          { name: 'bio_curation', type: 'List[String]', description: 'Biocuration provenance entries' },
          { name: 'references', type: 'List[String]', description: 'Supporting publication or database references' },
          sourcesField,
        ],
      )}
    />
  );
}

// Shared CTD exposure properties
const exposureProperties = [
  { name: 'evidence_count', type: 'UInt32', description: 'Number of evidence entries' },
  { name: 'number_of_receptors', type: 'Int64', description: 'Number of receptor/study participants' },
  { name: 'receptors', type: 'List[String]', description: 'Receptor identifiers (e.g. cell line, organism)' },
  { name: 'receptor_notes', type: 'List[String]', description: 'Free-text notes on receptors' },
  { name: 'smoking_statuses', type: 'List[String]', description: 'Smoking status of study subjects' },
  { name: 'sexes', type: 'List[String]', description: 'Sex of study subjects' },
  { name: 'races', type: 'List[String]', description: 'Race/ethnicity of study subjects' },
  { name: 'methods', type: 'List[String]', description: 'Measurement methods used' },
  { name: 'mediums', type: 'List[String]', description: 'Biological mediums measured (e.g. blood, urine)' },
  { name: 'detection_limit', type: 'List[String]', description: 'Lower limit of detection values' },
  { name: 'detection_limit_uom', type: 'List[String]', description: 'Units of detection limit values' },
  { name: 'detection_frequency', type: 'List[String]', description: 'Detection frequency values' },
  { name: 'age_entries', type: 'UInt32', description: 'Number of age-stratified entries' },
  { name: 'age_range_values', type: 'List[String]', description: 'Age range values for subjects' },
  { name: 'age_mean_values', type: 'List[String]', description: 'Mean age values' },
  { name: 'age_median_values', type: 'List[String]', description: 'Median age values' },
  { name: 'age_point_values', type: 'List[String]', description: 'Point age values' },
  { name: 'age_open_range_values', type: 'List[String]', description: 'Open-ended age range values' },
  { name: 'study_countries', type: 'List[String]', description: 'Countries where studies were conducted' },
  { name: 'states_or_provinces', type: 'List[String]', description: 'States or provinces of study' },
  { name: 'city_town_region_areas', type: 'List[String]', description: 'City/town/region of study' },
  { name: 'outcome_relationships', type: 'List[String]', description: 'Observed outcome relationships' },
  { name: 'exposure_event_notes', type: 'List[String]', description: 'Notes on the exposure event' },
  { name: 'exposure_outcome_notes', type: 'List[String]', description: 'Notes on the exposure outcome' },
  { name: 'references', type: 'List[String]', description: 'Supporting literature references' },
  { name: 'associated_study_titles', type: 'List[String]', description: 'Titles of associated studies' },
  { name: 'enrollment_start_years', type: 'List[String]', description: 'Study enrollment start years' },
  { name: 'enrollment_end_years', type: 'List[String]', description: 'Study enrollment end years' },
  { name: 'study_factors', type: 'List[String]', description: 'Study design factors' },
  { name: 'assay_notes', type: 'List[String]', description: 'Notes on the assay used' },
  sourcesField,
];

export function ExpBpoEdge() {
  return <SchemaTree fields={baseEdgeFields('EXP-BPO', 'INTERACTS_WITH', true, exposureProperties)} />;
}
export function ExpCcoEdge() {
  return <SchemaTree fields={baseEdgeFields('EXP-CCO', 'INTERACTS_WITH', true, exposureProperties)} />;
}
export function ExpDisEdge() {
  return <SchemaTree fields={baseEdgeFields('EXP-DIS', 'LINKED_TO', false, exposureProperties)} />;
}
export function ExpExpEdge() {
  return <SchemaTree fields={baseEdgeFields('EXP-EXP', 'PARENT', false, exposureProperties)} />;
}
export function ExpGenEdge() {
  return <SchemaTree fields={baseEdgeFields('EXP-GEN', 'INTERACTS_WITH', false, exposureProperties)} />;
}
export function ExpMfnEdge() {
  return <SchemaTree fields={baseEdgeFields('EXP-MFN', 'INTERACTS_WITH', true, exposureProperties)} />;
}
