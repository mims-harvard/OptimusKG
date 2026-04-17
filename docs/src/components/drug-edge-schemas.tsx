/**
 * Drug-related edge types: DRG-DIS, DRG-DRG, DRG-GEN, DRG-PHE.
 */
import { SchemaTree } from './schema-tree';
import { baseEdgeFields, sourcesField } from './edge-schema-shared';

// DRG-DIS
const drugDiseaseProperties = [
  { name: 'highest_clinical_trial_phase', type: 'Float64', description: 'Highest clinical trial phase for this indication' },
  { name: 'structure_id', type: 'String', description: 'DrugCentral structure ID' },
  { name: 'drug_disease_id', type: 'String', description: 'DrugCentral drug–disease identifier' },
  { name: 'reference_ids', type: 'List[String]', description: 'Supporting reference identifiers' },
  sourcesField,
];

export function DrgDisEdge() {
  return (
    <SchemaTree
      fields={baseEdgeFields(
        'DRG-DIS',
        ['INDICATION', 'CONTRAINDICATION', 'OFF_LABEL_USE'],
        true,
        drugDiseaseProperties,
      )}
    />
  );
}

// DRG-DRG
export function DrgDrgEdge() {
  return (
    <SchemaTree
      fields={baseEdgeFields(
        'DRG-DRG',
        ['SYNERGISTIC_INTERACTION', 'PARENT'],
        false,
        [
          { name: 'interaction_description', type: 'String', description: 'Description of the drug–drug interaction' },
          sourcesField,
        ],
      )}
    />
  );
}

// DRG-GEN
const drugGeneRelations = [
  'ACTIVATOR', 'AGONIST', 'ALLOSTERIC_ANTAGONIST', 'ANTAGONIST',
  'BINDING_AGENT', 'BLOCKER', 'CARRIER', 'DEGRADER', 'ENZYME',
  'INHIBITOR', 'INVERSE_AGONIST', 'MODULATOR', 'NEGATIVE_ALLOSTERIC_MODULATOR',
  'NEGATIVE_MODULATOR', 'OPENER', 'PARTIAL_AGONIST', 'POSITIVE_ALLOSTERIC_MODULATOR',
  'POSITIVE_MODULATOR', 'RELEASING_AGENT', 'STABILISER', 'SUBSTRATE',
  'TARGET', 'TRANSPORTER',
];

export function DrgGenEdge() {
  return (
    <SchemaTree
      fields={baseEdgeFields(
        'DRG-GEN',
        drugGeneRelations,
        false,
        [
          { name: 'mechanisms_of_action', type: 'List[String]', description: 'Mechanism of action descriptions' },
          { name: 'source_ids', type: 'List[String]', description: 'Source-specific interaction identifiers' },
          { name: 'source_urls', type: 'List[String]', description: 'URLs to source evidence records' },
          sourcesField,
        ],
      )}
    />
  );
}

// DRG-PHE
export function DrgPheEdge() {
  return (
    <SchemaTree
      fields={baseEdgeFields(
        'DRG-PHE',
        ['ADVERSE_DRUG_REACTION', 'ASSOCIATED_WITH', 'CONTRAINDICATION', 'INDICATION', 'OFF_LABEL_USE'],
        true,
        [
          { name: 'highest_clinical_trial_phase', type: 'Float64', description: 'Highest clinical trial phase' },
          { name: 'structure_id', type: 'String', description: 'DrugCentral structure ID' },
          { name: 'drug_disease_id', type: 'String', description: 'DrugCentral drug–disease identifier' },
          { name: 'reference_ids', type: 'List[String]', description: 'Supporting reference identifiers' },
          sourcesField,
        ],
      )}
    />
  );
}
